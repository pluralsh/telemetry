//! Namespace metadata, kept in the shard that holds the namespace.
//!
//! Each namespace stores its [`NamespaceMeta`] as one JSON record inside its
//! own key range, at the logical key `[subsystem | Default | version | tag]`
//! where the tag is the collection-meta record type with reserved bits `1`
//! (so it sorts next to, and never collides with, the collection meta
//! record). Because the record lives in the namespace's range, it moves with
//! the namespace when the shard splits, and the shard alone answers which
//! namespaces it holds.

use std::ops::Bound;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use common::serde::record_tag::RecordTag;
use common::storage::{PutRecordOp, Record, RecordOp, WriteOptions};
use common::{BytesRange, Storage, StorageRead};

use super::schema::NamespaceMeta;
use super::topology::NamespaceRange;
use crate::error::{Error, Result};
use crate::serde::{KEY_VERSION, RecordType, SUBSYSTEM, Segment};
use crate::storage::namespaced::{
    NamespaceId, namespace_range_in_segment, to_logical, to_physical,
};
use crate::storage::segment_extractor::segment_routing_prefix;

fn meta_key() -> Bytes {
    let mut key = BytesMut::with_capacity(4);
    key.put_u8(SUBSYSTEM);
    key.put_u8(Segment::Default.as_byte());
    key.put_u8(KEY_VERSION);
    key.put_u8(RecordTag::new(RecordType::CollectionMeta.id(), 1).as_byte());
    key.freeze()
}

fn decode(bytes: &[u8]) -> Result<NamespaceMeta> {
    serde_json::from_slice(bytes)
        .map_err(|e| Error::Encoding(format!("decode namespace record: {e}")))
}

/// The metadata of namespace `ns`, if the shard holds it.
pub(crate) async fn get(shard: &dyn StorageRead, ns: NamespaceId) -> Result<Option<NamespaceMeta>> {
    match shard.get(to_physical(ns, &meta_key())).await? {
        Some(record) => decode(&record.value).map(Some),
        None => Ok(None),
    }
}

/// Stores `meta`, durably, in the shard.
pub(crate) async fn put(shard: &Arc<dyn Storage>, meta: &NamespaceMeta) -> Result<()> {
    let value = serde_json::to_vec(meta)
        .map_err(|e| Error::Internal(format!("encode namespace record: {e}")))?;
    let ns = NamespaceId::for_name(&meta.name);
    shard
        .apply_with_options(
            vec![RecordOp::Put(PutRecordOp::new(Record::new(
                to_physical(ns, &meta_key()),
                Bytes::from(value),
            )))],
            WriteOptions {
                await_durable: true,
            },
        )
        .await?;
    Ok(())
}

/// Every namespace of the shard within `range`, in id order.
///
/// Every namespace has keys in the Default segment, so this visits each one
/// with a scan for its first Default-segment key and a point read of its
/// record, then skips past it.
pub(crate) async fn list(
    shard: &dyn StorageRead,
    range: NamespaceRange,
) -> Result<Vec<NamespaceMeta>> {
    let prefix = segment_routing_prefix(Segment::Default);
    let (start, end) = range.bounds();
    let mut cursor = start.unwrap_or(NamespaceId::from_u64(0));
    let mut namespaces = Vec::new();
    loop {
        let (lo, hi) = namespace_range_in_segment(&prefix, Some(cursor), end);
        let hi = match hi {
            Bound::Unbounded => Bound::Excluded(segment_routing_prefix(Segment::Ann)),
            bounded => bounded,
        };
        let mut iter = shard.scan_iter(BytesRange::new(lo, hi)).await?;
        let Some(record) = iter.next().await? else {
            break;
        };
        let Some((ns, _)) = to_logical(&record.key) else {
            break;
        };
        if let Some(meta) = get(shard, ns).await? {
            namespaces.push(meta);
        }
        match ns.as_u64().checked_add(1) {
            Some(next) => cursor = NamespaceId::from_u64(next),
            None => break,
        }
    }
    Ok(namespaces)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::schema::Metric;
    use crate::serde::key::IdDictionaryKey;
    use crate::storage::namespaced::NamespacedStorage;
    use common::storage::in_memory::InMemoryStorage;

    fn meta(name: &str) -> NamespaceMeta {
        NamespaceMeta {
            name: name.into(),
            id: NamespaceId::for_name(name).to_hex(),
            shard: 0,
            dimensions: 2,
            distance_metric: Metric::default(),
            schema: Default::default(),
            embedding_model: None,
            created_at_ms: 0,
        }
    }

    #[tokio::test]
    async fn should_list_namespaces_in_a_range() {
        let shard: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let names = ["a", "b", "c", "d", "e"];
        for name in names {
            put(&shard, &meta(name)).await.unwrap();
            // data keys of the namespace sort around its record
            NamespacedStorage::new(shard.clone(), NamespaceId::for_name(name))
                .put(vec![PutRecordOp::new(Record::new(
                    IdDictionaryKey::new("doc").encode(),
                    Bytes::from("v"),
                ))])
                .await
                .unwrap();
        }

        let all = list(shard.as_ref(), NamespaceRange::FULL).await.unwrap();
        let mut expected: Vec<&str> = names.to_vec();
        expected.sort_by_key(|n| NamespaceId::for_name(n));
        assert_eq!(
            all.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
            expected
        );

        let half = u64::MAX / 2;
        let low = list(
            shard.as_ref(),
            NamespaceRange {
                start: super::super::topology::HexId(0),
                end: Some(super::super::topology::HexId(half)),
            },
        )
        .await
        .unwrap();
        assert!(
            low.iter()
                .all(|m| NamespaceId::for_name(&m.name).as_u64() < half)
        );
        assert_eq!(
            low.len(),
            names
                .iter()
                .filter(|n| NamespaceId::for_name(n).as_u64() < half)
                .count()
        );
        assert_eq!(
            get(shard.as_ref(), NamespaceId::for_name("c"))
                .await
                .unwrap(),
            Some(meta("c"))
        );
    }
}
