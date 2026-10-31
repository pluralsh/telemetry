//! Namespace-scoped views over a shared shard database.
//!
//! A shard is one SlateDB holding many namespaces. Each namespace is a
//! complete vector database (centroid tree, posting lists, id dictionary,
//! metadata and FTS indexes) whose keys carry an 8-byte namespace id right
//! after the 3-byte segment routing prefix:
//!
//! ```text
//! logical:  [subsystem | segment | version | tag | ...]
//! physical: [subsystem | segment | version | namespace id (8) | tag | ...]
//! ```
//!
//! Keeping the routing prefix first leaves the segment extractor (and so the
//! per-segment compaction layout) unchanged, while every namespace occupies a
//! contiguous key range inside each segment. That contiguity is what lets a
//! shard split with SlateDB projected clones: a namespace-id range maps to one
//! key range per segment.
//!
//! [`NamespacedStorage`] rewrites keys in both directions so the unmodified
//! single-collection code (`VectorDb`, the query engine, the indexer) runs
//! against one namespace of a shared shard.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use common::storage::{
    CheckpointInfo, MergeRecordOp, PutRecordOp, Record, RecordOp, StorageIterator, StorageSnapshot,
    WriteOptions, WriteResult,
};
use common::{BytesRange, Storage, StorageError, StorageRead, StorageResult};

use crate::storage::segment_extractor::ROUTING_PREFIX_LEN;

/// Length of the namespace id embedded in physical keys.
pub const NAMESPACE_ID_LEN: usize = 8;

/// Fixed-width namespace identifier embedded in every physical key.
///
/// Derived from the namespace name by hashing, so it is stable across
/// processes and shard moves; the catalog rejects the (vanishingly rare)
/// collision of two names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub [u8; NAMESPACE_ID_LEN]);

impl NamespaceId {
    /// The id for namespace `name`.
    pub fn for_name(name: &str) -> Self {
        let hash = blake3::hash(name.as_bytes());
        let mut id = [0u8; NAMESPACE_ID_LEN];
        id.copy_from_slice(&hash.as_bytes()[..NAMESPACE_ID_LEN]);
        // An all-0xFF id has no successor, which range mapping relies on.
        if id == [0xFF; NAMESPACE_ID_LEN] {
            id[NAMESPACE_ID_LEN - 1] = 0xFE;
        }
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.0)
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value.to_be_bytes())
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The next id in key order. Ids never reach `u64::MAX` (see
    /// [`Self::for_name`]), so this always exists for real namespaces.
    fn successor(&self) -> Self {
        Self::from_u64(self.as_u64() + 1)
    }
}

/// Inserts `ns` into a logical key after its routing prefix.
pub(crate) fn to_physical(ns: NamespaceId, logical: &[u8]) -> Bytes {
    assert!(
        logical.len() >= ROUTING_PREFIX_LEN,
        "vector key shorter than its routing prefix: {logical:02x?}"
    );
    let mut key = BytesMut::with_capacity(logical.len() + NAMESPACE_ID_LEN);
    key.extend_from_slice(&logical[..ROUTING_PREFIX_LEN]);
    key.extend_from_slice(&ns.0);
    key.extend_from_slice(&logical[ROUTING_PREFIX_LEN..]);
    key.freeze()
}

/// Splits a physical key into its namespace id and logical key.
pub(crate) fn to_logical(physical: &[u8]) -> Option<(NamespaceId, Bytes)> {
    if physical.len() < ROUTING_PREFIX_LEN + NAMESPACE_ID_LEN {
        return None;
    }
    let mut id = [0u8; NAMESPACE_ID_LEN];
    id.copy_from_slice(&physical[ROUTING_PREFIX_LEN..ROUTING_PREFIX_LEN + NAMESPACE_ID_LEN]);
    let mut key = BytesMut::with_capacity(physical.len() - NAMESPACE_ID_LEN);
    key.extend_from_slice(&physical[..ROUTING_PREFIX_LEN]);
    key.extend_from_slice(&physical[ROUTING_PREFIX_LEN + NAMESPACE_ID_LEN..]);
    Some((NamespaceId(id), key.freeze()))
}

/// The physical key range of namespaces `[lo, hi)` within the segment named
/// by `routing_prefix`; `None` bounds extend to the segment edges.
pub(crate) fn namespace_range_in_segment(
    routing_prefix: &[u8],
    lo: Option<NamespaceId>,
    hi: Option<NamespaceId>,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let at = |ns: NamespaceId| -> Bytes {
        let mut key = BytesMut::with_capacity(routing_prefix.len() + NAMESPACE_ID_LEN);
        key.extend_from_slice(routing_prefix);
        key.extend_from_slice(&ns.0);
        key.freeze()
    };
    let start = match lo {
        Some(ns) => Bound::Included(at(ns)),
        None => Bound::Unbounded,
    };
    let end = match hi {
        Some(ns) => Bound::Excluded(at(ns)),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Maps a logical scan range of one namespace to its physical range.
///
/// Vector scans always stay within one segment and use bounds of at least
/// the routing prefix plus record tag. A shorter (or unbounded) end bound is
/// capped at the end of the namespace in the start bound's segment, which is
/// exact for every range that stays inside one segment.
fn physical_range(ns: NamespaceId, range: &BytesRange) -> StorageResult<BytesRange> {
    let start_prefix = match &range.start {
        Bound::Included(k) | Bound::Excluded(k) if k.len() >= ROUTING_PREFIX_LEN => {
            k.slice(..ROUTING_PREFIX_LEN)
        }
        _ => {
            return Err(StorageError::Internal(format!(
                "namespaced scan needs a start bound with a routing prefix, got {range:?}"
            )));
        }
    };
    let map = |k: &Bytes| -> Bytes { to_physical(ns, k) };
    let start = match &range.start {
        Bound::Included(k) => Bound::Included(map(k)),
        Bound::Excluded(k) => Bound::Excluded(map(k)),
        Bound::Unbounded => unreachable!("checked above"),
    };
    let namespace_end = || {
        let mut key = BytesMut::with_capacity(ROUTING_PREFIX_LEN + NAMESPACE_ID_LEN);
        key.extend_from_slice(&start_prefix);
        key.extend_from_slice(&ns.successor().0);
        Bound::Excluded(key.freeze())
    };
    let end = match &range.end {
        Bound::Included(k)
            if k.len() > ROUTING_PREFIX_LEN && k[..ROUTING_PREFIX_LEN] == start_prefix[..] =>
        {
            Bound::Included(map(k))
        }
        Bound::Excluded(k)
            if k.len() > ROUTING_PREFIX_LEN && k[..ROUTING_PREFIX_LEN] == start_prefix[..] =>
        {
            Bound::Excluded(map(k))
        }
        _ => namespace_end(),
    };
    Ok(BytesRange::new(start, end))
}

fn strip_record(ns: NamespaceId, record: Record) -> StorageResult<Record> {
    match to_logical(&record.key) {
        Some((id, key)) if id == ns => Ok(Record::new(key, record.value)),
        _ => Err(StorageError::Internal(format!(
            "key outside namespace {}: {:02x?}",
            ns.to_hex(),
            record.key
        ))),
    }
}

fn map_op(ns: NamespaceId, op: RecordOp) -> RecordOp {
    match op {
        RecordOp::Put(put) => RecordOp::Put(map_put(ns, put)),
        RecordOp::Merge(merge) => RecordOp::Merge(map_merge(ns, merge)),
        RecordOp::Delete(key) => RecordOp::Delete(to_physical(ns, &key)),
    }
}

fn map_put(ns: NamespaceId, mut put: PutRecordOp) -> PutRecordOp {
    put.record.key = to_physical(ns, &put.record.key);
    put
}

fn map_merge(ns: NamespaceId, mut merge: MergeRecordOp) -> MergeRecordOp {
    merge.record.key = to_physical(ns, &merge.record.key);
    merge
}

struct NamespacedIterator {
    ns: NamespaceId,
    inner: Box<dyn StorageIterator + Send + 'static>,
}

#[async_trait]
impl StorageIterator for NamespacedIterator {
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.inner.next().await? {
            Some(record) => Ok(Some(strip_record(self.ns, record)?)),
            None => Ok(None),
        }
    }
}

async fn namespaced_get(
    reader: &dyn StorageRead,
    ns: NamespaceId,
    key: Bytes,
) -> StorageResult<Option<Record>> {
    match reader.get(to_physical(ns, &key)).await? {
        Some(record) => Ok(Some(strip_record(ns, record)?)),
        None => Ok(None),
    }
}

async fn namespaced_scan_iter(
    reader: &dyn StorageRead,
    ns: NamespaceId,
    range: BytesRange,
) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
    let inner = reader.scan_iter(physical_range(ns, &range)?).await?;
    Ok(Box::new(NamespacedIterator { ns, inner }))
}

/// One namespace of a shard's storage.
///
/// Reads, writes and snapshots are confined to the namespace's keys. The
/// shard owns the underlying database: [`StorageRead::close`] is a no-op, and
/// flushes/checkpoints act on the whole shard.
pub(crate) struct NamespacedStorage {
    inner: Arc<dyn Storage>,
    ns: NamespaceId,
}

impl NamespacedStorage {
    pub(crate) fn new(inner: Arc<dyn Storage>, ns: NamespaceId) -> Self {
        Self { inner, ns }
    }
}

#[async_trait]
impl StorageRead for NamespacedStorage {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        namespaced_get(self.inner.as_ref(), self.ns, key).await
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        namespaced_scan_iter(self.inner.as_ref(), self.ns, range).await
    }
}

#[async_trait]
impl Storage for NamespacedStorage {
    async fn apply_with_options(
        &self,
        ops: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let ops = ops.into_iter().map(|op| map_op(self.ns, op)).collect();
        self.inner.apply_with_options(ops, options).await
    }

    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let records = records.into_iter().map(|r| map_put(self.ns, r)).collect();
        self.inner.put_with_options(records, options).await
    }

    async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let records = records.into_iter().map(|r| map_merge(self.ns, r)).collect();
        self.inner.merge_with_options(records, options).await
    }

    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        Ok(Arc::new(NamespacedSnapshot {
            inner: self.inner.snapshot().await?,
            ns: self.ns,
        }))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    async fn flush_memtable(&self) -> StorageResult<()> {
        self.inner.flush_memtable().await
    }

    fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe_durable()
    }

    async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        self.inner.create_checkpoint().await
    }
}

/// One namespace of a read-only shard (a reader following the writer).
pub(crate) struct NamespacedRead {
    inner: Arc<dyn StorageRead>,
    ns: NamespaceId,
}

impl NamespacedRead {
    pub(crate) fn new(inner: Arc<dyn StorageRead>, ns: NamespaceId) -> Self {
        Self { inner, ns }
    }
}

#[async_trait]
impl StorageRead for NamespacedRead {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        namespaced_get(self.inner.as_ref(), self.ns, key).await
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        namespaced_scan_iter(self.inner.as_ref(), self.ns, range).await
    }
}

struct NamespacedSnapshot {
    inner: Arc<dyn StorageSnapshot>,
    ns: NamespaceId,
}

#[async_trait]
impl StorageRead for NamespacedSnapshot {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        namespaced_get(self.inner.as_ref(), self.ns, key).await
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        namespaced_scan_iter(self.inner.as_ref(), self.ns, range).await
    }
}

impl StorageSnapshot for NamespacedSnapshot {}

/// Deletes every key of namespace `ns` from `storage`, segment by segment.
pub(crate) async fn delete_namespace_keys(
    storage: &Arc<dyn Storage>,
    ns: NamespaceId,
) -> StorageResult<u64> {
    const BATCH: usize = 1024;
    let mut deleted = 0u64;
    for segment in [
        crate::serde::Segment::Default,
        crate::serde::Segment::Ann,
        crate::serde::Segment::Fts,
    ] {
        let prefix = crate::storage::segment_extractor::segment_routing_prefix(segment);
        let (start, end) = namespace_range_in_segment(&prefix, Some(ns), Some(ns.successor()));
        let mut iter = storage.scan_iter(BytesRange::new(start, end)).await?;
        let mut batch = Vec::with_capacity(BATCH);
        while let Some(record) = iter.next().await? {
            batch.push(RecordOp::Delete(record.key));
            if batch.len() == BATCH {
                deleted += batch.len() as u64;
                storage.apply(std::mem::take(&mut batch)).await?;
            }
        }
        if !batch.is_empty() {
            deleted += batch.len() as u64;
            storage.apply(batch).await?;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::key::{IdDictionaryKey, MetadataIndexKey};
    use crate::serde::{FieldValue, RecordType};
    use common::storage::in_memory::InMemoryStorage;

    fn ns(name: &str) -> NamespaceId {
        NamespaceId::for_name(name)
    }

    #[test]
    fn should_round_trip_physical_keys() {
        let logical = IdDictionaryKey::new("doc-1").encode();
        let physical = to_physical(ns("a"), &logical);
        assert_eq!(physical.len(), logical.len() + NAMESPACE_ID_LEN);
        assert_eq!(
            &physical[..ROUTING_PREFIX_LEN],
            &logical[..ROUTING_PREFIX_LEN]
        );
        assert_eq!(to_logical(&physical), Some((ns("a"), logical)));
    }

    #[tokio::test]
    async fn should_isolate_namespaces_on_shared_storage() {
        // given - two namespaces over one storage, each writing the same keys
        let shared: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let a = NamespacedStorage::new(shared.clone(), ns("a"));
        let b = NamespacedStorage::new(shared.clone(), ns("b"));
        for (storage, value) in [(&a, "from-a"), (&b, "from-b")] {
            storage
                .put(vec![PutRecordOp::new(Record::new(
                    IdDictionaryKey::new("doc-1").encode(),
                    Bytes::from(value),
                ))])
                .await
                .unwrap();
            storage
                .put(vec![PutRecordOp::new(Record::new(
                    MetadataIndexKey::new("k", FieldValue::Int64(1)).encode(),
                    Bytes::from(value),
                ))])
                .await
                .unwrap();
        }

        // when
        let got_a = a.get(IdDictionaryKey::new("doc-1").encode()).await.unwrap();
        let mut prefix = BytesMut::new();
        RecordType::IdDictionary.write_prefix(&mut prefix);
        let scanned_b = b.scan(BytesRange::prefix(prefix.freeze())).await.unwrap();
        let snapshot_b = b.snapshot().await.unwrap();
        let index_b = snapshot_b
            .scan(MetadataIndexKey::field_range("k"))
            .await
            .unwrap();

        // then - each namespace sees only its own records, under logical keys
        assert_eq!(got_a.unwrap().value, Bytes::from("from-a"));
        assert_eq!(scanned_b.len(), 1);
        assert_eq!(scanned_b[0].key, IdDictionaryKey::new("doc-1").encode());
        assert_eq!(scanned_b[0].value, Bytes::from("from-b"));
        assert_eq!(index_b.len(), 1);
        assert_eq!(index_b[0].value, Bytes::from("from-b"));
    }

    #[tokio::test]
    async fn should_delete_only_one_namespace() {
        let shared: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let a = NamespacedStorage::new(shared.clone(), ns("a"));
        let b = NamespacedStorage::new(shared.clone(), ns("b"));
        for storage in [&a, &b] {
            storage
                .put(vec![PutRecordOp::new(Record::new(
                    IdDictionaryKey::new("doc-1").encode(),
                    Bytes::from("v"),
                ))])
                .await
                .unwrap();
        }

        let deleted = delete_namespace_keys(&shared, ns("a")).await.unwrap();

        assert_eq!(deleted, 1);
        let key = IdDictionaryKey::new("doc-1").encode();
        assert!(a.get(key.clone()).await.unwrap().is_none());
        assert!(b.get(key).await.unwrap().is_some());
    }
}
