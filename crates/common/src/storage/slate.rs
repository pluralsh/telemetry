use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::storage::sst_blocks;
use crate::storage::{MergeOptions, PutOptions};
use crate::{
    BytesRange, CheckpointInfo, ReadHints, Record, StorageError, StorageIterator, StorageRead,
    StorageResult, Ttl,
    storage::{
        MergeOperator, MergeRecordOp, PutRecordOp, RecordOp, Storage, StorageSnapshot,
        WriteOptions, WriteResult,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use slatedb::IterationOrder;
use slatedb::config::{CheckpointOptions, CheckpointScope, ReadOptions, ScanOptions};
use slatedb::manifest::VersionedManifest;
use slatedb::{
    CacheTarget, Db, DbIterator, DbReader, DbSnapshot, FilterContext,
    MergeOperator as SlateDbMergeOperator, MergeOperatorError, SstReader, WriteBatch,
    config::WriteOptions as SlateDbWriteOptions,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub mod warm;

pub use warm::SstWarmTracker;

/// Adapter that wraps our `MergeOperator` trait to implement SlateDB's `MergeOperator` trait.
///
/// This allows using our common merge operator interface with SlateDB's merge functionality.
pub struct SlateDbMergeOperatorAdapter {
    operator: Arc<dyn MergeOperator>,
}

impl SlateDbMergeOperatorAdapter {
    fn new(operator: Arc<dyn MergeOperator>) -> Self {
        Self { operator }
    }
}

impl SlateDbMergeOperator for SlateDbMergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        Ok(self.operator.merge_batch(key, existing_value, &[value]))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if operands.is_empty() && existing_value.is_none() {
            return Err(MergeOperatorError::EmptyBatch);
        }
        Ok(self.operator.merge_batch(key, existing_value, operands))
    }
}

/// Returns the default scan options used for storage scans.
fn default_scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: Default::default(),
        dirty: false,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 8,
        order: IterationOrder::Ascending,
        filter_context: None,
    }
}

fn hinted_scan_options(hints: ReadHints, filter_context: Option<FilterContext>) -> ScanOptions {
    ScanOptions {
        cache_blocks: hints.cache_blocks,
        ..default_scan_options()
    }
    .with_filter_context(filter_context)
}

fn hinted_read_options(hints: ReadHints) -> ReadOptions {
    ReadOptions {
        cache_blocks: hints.cache_blocks,
        ..ReadOptions::default()
    }
}
/// Where a [`SlateReadHandle`] reads its manifest from. Both variants expose a
/// live `manifest()`, so each count reflects the latest flushed state.
enum ManifestSource {
    /// The writer's `Db` — the manifest is always current.
    Db(Arc<Db>),
    /// A reader's `DbReader` — the manifest reflects its last poll.
    Reader(Arc<DbReader>),
}

impl ManifestSource {
    fn manifest(&self) -> VersionedManifest {
        match self {
            ManifestSource::Db(db) => db.manifest(),
            ManifestSource::Reader(reader) => reader.manifest(),
        }
    }
}

/// Self-contained slatedb resources for the SST-walk count path, returned by
/// [`StorageRead::slate_read`].
///
/// Owns a manifest source (a live `Db`/`DbReader`) plus an [`SstReader`] pinned
/// to the same path + object store. This is a deliberate, single-method leak of
/// slatedb internals: the count path needs a manifest and an SST reader,
/// neither of which belongs on the storage-neutral trait. The handle is
/// `'static`, so callers can hold it for the lifetime of a read view rather
/// than re-deriving it per call.
///
/// Each [`count_in_range`](SlateReadHandle::count_in_range) reads the manifest
/// fresh from the live source, so counts reflect the latest flushed state
/// without a second polling handle to keep in sync.
pub struct SlateReadHandle {
    source: ManifestSource,
    sst_reader: Arc<SstReader>,
}

impl SlateReadHandle {
    /// Counts physical write operations in `range` by walking the live
    /// manifest's persisted SSTs. See [`sst_blocks::count_in_range`].
    pub async fn count_in_range(
        &self,
        range: &BytesRange,
    ) -> StorageResult<sst_blocks::CountResult> {
        sst_blocks::count_in_range(&self.source.manifest(), &self.sst_reader, range).await
    }

    /// Returns the live manifest snapshot for metadata-only inspection — e.g.
    /// summarizing how data is distributed across the LSM tree. Reads no SST
    /// files; each call reflects the latest flushed state of the live source.
    pub fn manifest(&self) -> VersionedManifest {
        self.source.manifest()
    }

    /// Warms cache blocks for live SlateDB segments matching `prefixes`.
    ///
    /// Filters and SST indexes are always warmed into SlateDB's metadata cache.
    /// When `include_data` is true, every data block in the matching segment
    /// keyspace is also warmed. Metadata-only warming intentionally avoids
    /// catalog data blocks so it cannot displace payloads from the data cache.
    /// Backends without a configured cache treat warming as a no-op. With a
    /// `tracker`, SSTs it has already warmed are skipped.
    pub async fn warm_prefixes(
        &self,
        product: &'static str,
        prefixes: &[Bytes],
        include_data: bool,
        concurrency: usize,
        cancel: &CancellationToken,
        tracker: Option<&SstWarmTracker>,
    ) -> StorageResult<()> {
        let manifest = self.source.manifest();
        let work = manifest
            .segments()
            .iter()
            .filter_map(|segment| {
                let prefix = prefixes
                    .iter()
                    .find(|prefix| segment.prefix() == prefix.as_ref())?;
                let mut targets = vec![CacheTarget::Filters, CacheTarget::Index];
                if include_data {
                    targets.push(CacheTarget::data::<Bytes, _>(BytesRange::prefix(
                        prefix.clone(),
                    )));
                }
                let targets: Arc<[CacheTarget]> = targets.into();
                Some(
                    segment
                        .l0()
                        .iter()
                        .map(|view| view.sst.id)
                        .chain(
                            segment
                                .compacted()
                                .iter()
                                .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
                        )
                        .map(move |id| (id, targets.clone()))
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .collect::<Vec<_>>();

        match &self.source {
            ManifestSource::Db(db) => {
                warm::warm_ssts(
                    db.as_ref(),
                    product,
                    work,
                    include_data,
                    concurrency,
                    cancel,
                    tracker,
                )
                .await
            }
            ManifestSource::Reader(reader) => {
                warm::warm_ssts(
                    reader.as_ref(),
                    product,
                    work,
                    include_data,
                    concurrency,
                    cancel,
                    tracker,
                )
                .await
            }
        }
        .map_err(StorageError::from_storage)
    }
}

/// SlateDB-backed implementation of the Storage trait.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability.
pub struct SlateDbStorage {
    pub(super) db: Arc<Db>,
    durable_tx: watch::Sender<u64>,
    durable_bridge_abort: tokio::task::AbortHandle,
    /// SST reader for the [`StorageRead::slate_read`] count path. Built by the
    /// production constructors ([`crate::StorageBuilder`]); `None` for the bare
    /// `new` used in tests that only exercise get/scan, where `slate_read`
    /// falls back to `None`. `Arc` so the handle can outlive a borrow of `self`.
    sst_reader: Option<Arc<SstReader>>,
    /// Set by every write, cleared by the periodic memtable flush.
    unflushed: Arc<AtomicBool>,
}

/// How often a writer with new writes flushes its memtable to L0. A
/// `DbReader` replays each manifest poll's new WALs into a memtable of its
/// own and drops them only once L0 covers them, so a writer below
/// `l0_sst_size_bytes` would leave readers probing one memtable per poll
/// on every get and scan; this bounds them to about this many polls' worth.
pub const MEMTABLE_FLUSH_INTERVAL: Duration = Duration::from_secs(10);

/// Flushes `db`'s memtable to L0 every `every` while `unflushed` is set,
/// until the database closes or every strong handle is dropped.
pub fn spawn_memtable_flusher(db: Weak<Db>, unflushed: Arc<AtomicBool>, every: Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let Some(db) = db.upgrade() else { return };
            if !unflushed.swap(false, Ordering::AcqRel) {
                continue;
            }
            let flushed = db
                .flush_with_options(slatedb::config::FlushOptions {
                    flush_type: slatedb::config::FlushType::MemTable,
                })
                .await;
            match flushed {
                Ok(()) => {}
                Err(err) if matches!(err.kind(), slatedb::ErrorKind::Closed(_)) => return,
                Err(err) => {
                    unflushed.store(true, Ordering::Release);
                    tracing::warn!("periodic memtable flush failed: {err}");
                }
            }
        }
    });
}

impl SlateDbStorage {
    /// Creates a new SlateDbStorage instance wrapping the given SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        let slate_rx = db.subscribe();
        let (durable_tx, _) = watch::channel(slate_rx.borrow().durable_seq);
        let task = tokio::spawn({
            let tx = durable_tx.clone();
            async move {
                let mut slate_rx = slate_rx;
                while slate_rx.changed().await.is_ok() {
                    let durable_seq = slate_rx.borrow_and_update().durable_seq;
                    // Use send_replace rather than send: SlateDB may publish
                    // a DbStatus change during `open` (e.g. manifest write)
                    // before any consumer has subscribed, and `send` would
                    // return Err on zero receivers — killing the bridge
                    // permanently. send_replace doesn't care about receiver
                    // count, so late subscribers still get future updates.
                    tx.send_replace(durable_seq);
                }
            }
        });

        Self {
            db,
            durable_tx,
            durable_bridge_abort: task.abort_handle(),
            sst_reader: None,
            unflushed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Flushes the memtable to L0 every `every` while writes arrive; see
    /// [`MEMTABLE_FLUSH_INTERVAL`].
    pub fn with_memtable_flush(self, every: Duration) -> Self {
        spawn_memtable_flusher(Arc::downgrade(&self.db), Arc::clone(&self.unflushed), every);
        self
    }

    /// Attaches an [`SstReader`] so [`StorageRead::slate_read`] can serve the
    /// SST-walk count path. Built against the same path + object store as `db`.
    pub fn with_sst_reader(mut self, sst_reader: SstReader) -> Self {
        self.sst_reader = Some(Arc::new(sst_reader));
        self
    }

    /// Returns the underlying SlateDB instance. Useful for callers that need to access
    /// slatedb-specific features like `DbStatus`
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Creates a SlateDB `MergeOperator` from our common `MergeOperator` trait.
    ///
    /// This adapter can be used when constructing a SlateDB database with a merge operator:
    /// ```rust,ignore
    /// use common::storage::MergeOperator;
    /// use slatedb::{DbBuilder, object_store::ObjectStore};
    ///
    /// let my_merge_op: Arc<dyn MergeOperator> = Arc::new(MyMergeOperator);
    /// let slate_merge_op = SlateDbStorage::merge_operator_adapter(my_merge_op);
    ///
    /// let db = DbBuilder::new("path", object_store)
    ///     .with_merge_operator(Arc::new(slate_merge_op))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn merge_operator_adapter(operator: Arc<dyn MergeOperator>) -> SlateDbMergeOperatorAdapter {
        SlateDbMergeOperatorAdapter::new(operator)
    }
}

#[async_trait]
impl StorageRead for SlateDbStorage {
    /// Retrieves a single record by key from SlateDB.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .db
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    /// Slatedb consults its SST-level filters on `scan_prefix` but not on
    /// `scan`, so routing prefix scans through this path is what lets a
    /// configured `PrefixExtractor` (and custom `FilterPolicy`, parametrized by
    /// `filter_context`) actually skip SSTs.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .db
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get_with_options(&key, &hinted_read_options(hints))
            .await
            .map_err(StorageError::from_storage)?;
        Ok(value.map(|value| Record::new(key, value)))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter_with(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
        hints: ReadHints,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = hinted_scan_options(hints, filter_context);
        let iter = self
            .db
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.sst_reader.as_ref().map(|sst_reader| SlateReadHandle {
            // Live writer manifest: reflects everything flushed so far.
            source: ManifestSource::Db(Arc::clone(&self.db)),
            sst_reader: Arc::clone(sst_reader),
        })
    }

    async fn close(&self) -> StorageResult<()> {
        // Stop durable bridge first so no status subscriber outlives DB close.
        self.durable_bridge_abort.abort();
        // `Db::close()` drives block-cache shutdown (including flushing any
        // hybrid disk tier), so we don't manage the cache lifecycle here.
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }
}

pub(super) struct SlateDbIterator {
    iter: DbIterator,
}

#[async_trait]
impl StorageIterator for SlateDbIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.iter.next().await.map_err(StorageError::from_storage)? {
            Some(entry) => Ok(Some(Record::new(entry.key, entry.value))),
            None => Ok(None),
        }
    }
}

/// SlateDB snapshot wrapper that implements StorageSnapshot.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
pub struct SlateDbStorageSnapshot {
    snapshot: Arc<DbSnapshot>,
}

#[async_trait]
impl StorageRead for SlateDbStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .snapshot
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .snapshot
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get_with_options(&key, &hinted_read_options(hints))
            .await
            .map_err(StorageError::from_storage)?;
        Ok(value.map(|value| Record::new(key, value)))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter_with(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
        hints: ReadHints,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = hinted_scan_options(hints, filter_context);
        let iter = self
            .snapshot
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

#[async_trait]
impl StorageSnapshot for SlateDbStorageSnapshot {}

#[async_trait]
impl Storage for SlateDbStorage {
    async fn apply_with_options(
        &self,
        records: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            match op {
                RecordOp::Put(op) => {
                    batch.put_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Merge(op) => {
                    batch.merge_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Delete(key) => batch.delete(key),
            }
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        self.unflushed.store(true, Ordering::Release);
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.put_with_options(op.record.key, op.record.value, &op.options.into());
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        self.unflushed.store(true, Ordering::Release);
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    async fn merge_with_options(
        &self,
        records: Vec<MergeRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.merge_with_options(op.record.key, op.record.value, &op.options.into());
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("merge operator") || error_msg.contains("not configured") {
                    StorageError::Storage(
                        "Merge operator not configured for this database".to_string(),
                    )
                } else {
                    StorageError::from_storage(e)
                }
            })?;
        self.unflushed.store(true, Ordering::Release);
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    fn subscribe_durable(&self) -> watch::Receiver<u64> {
        self.durable_tx.subscribe()
    }

    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Arc::new(SlateDbStorageSnapshot { snapshot }))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        let result = self
            .db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(CheckpointInfo {
            id: result.id,
            manifest_id: result.manifest_id,
        })
    }
}

impl From<Ttl> for slatedb::config::Ttl {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::Default => slatedb::config::Ttl::Default,
            Ttl::NoExpiry => slatedb::config::Ttl::NoExpiry,
            Ttl::ExpireAfter(ts) => slatedb::config::Ttl::ExpireAfter(ts),
            Ttl::ExpireAt(ts) => slatedb::config::Ttl::ExpireAt(ts),
        }
    }
}

impl From<PutOptions> for slatedb::config::PutOptions {
    fn from(value: PutOptions) -> Self {
        Self {
            ttl: value.ttl.into(),
        }
    }
}

impl From<MergeOptions> for slatedb::config::MergeOptions {
    fn from(value: MergeOptions) -> Self {
        Self {
            ttl: value.ttl.into(),
        }
    }
}

/// Read-only SlateDB storage using `DbReader`.
///
/// This struct provides read-only access to a SlateDB database without fencing,
/// allowing multiple readers to coexist with a single writer.
pub struct SlateDbStorageReader {
    reader: Arc<DbReader>,
    /// SST reader for the [`StorageRead::slate_read`] count path; see
    /// [`SlateDbStorage::sst_reader`].
    sst_reader: Option<Arc<SstReader>>,
}

impl SlateDbStorageReader {
    /// Creates a new SlateDbStorageReader wrapping the given DbReader.
    pub fn new(reader: Arc<DbReader>) -> Self {
        Self {
            reader,
            sst_reader: None,
        }
    }

    /// Attaches an [`SstReader`] so [`StorageRead::slate_read`] can serve the
    /// SST-walk count path. Built against the same path + object store as the
    /// `DbReader`.
    pub fn with_sst_reader(mut self, sst_reader: SstReader) -> Self {
        self.sst_reader = Some(Arc::new(sst_reader));
        self
    }
}

#[async_trait]
impl StorageRead for SlateDbStorageReader {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .reader
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .reader
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = default_scan_options().with_filter_context(filter_context);
        let iter = self
            .reader
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        let value = self
            .reader
            .get_with_options(&key, &hinted_read_options(hints))
            .await
            .map_err(StorageError::from_storage)?;
        Ok(value.map(|value| Record::new(key, value)))
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_prefix_iter_with(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
        hints: ReadHints,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let options = hinted_scan_options(hints, filter_context);
        let iter = self
            .reader
            .scan_prefix_with_options(prefix, subrange, &options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.sst_reader.as_ref().map(|sst_reader| SlateReadHandle {
            // The DbReader's last-polled manifest. Scans on this same handle
            // read the same view, so count and scan stay consistent.
            source: ManifestSource::Reader(Arc::clone(&self.reader)),
            sst_reader: Arc::clone(sst_reader),
        })
    }

    async fn close(&self) -> StorageResult<()> {
        // `DbReader::close()` drives block-cache shutdown itself.
        self.reader
            .close()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
