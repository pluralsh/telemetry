//! Native SlateDB-backed storage for timeseries.
//!
//! [`Storage`] owns a `slatedb::Db` and implements [`Store`]: every
//! [`StorageRead`] method plus the write path (batch apply, snapshot, flush).
//! Reads are served by the private [`StorageReaderInner`], which is generic
//! over SlateDB's [`DbReadOps`] so a single implementation of the OpenTSDB
//! read methods (bucket list, forward / inverted index, series dictionary)
//! works against all three SlateDB read handles: the writer `Db` itself, a
//! point-in-time [`StorageSnapshot`] (`DbSnapshot`), and the read-only
//! [`StorageReader`] (`DbReader`). The OpenTSDB record-op builders are free
//! functions — pure encoders that touch no storage state.
//!
//! Value types (`Record`, `RecordOp`, `Ttl`, …), the error type, and the
//! `Ttl`/options → SlateDB conversions are all reused from `common::storage`;
//! only the storage *handles* are native here.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::storage::config::SlateDbStorageConfig;
use common::storage::factory::{SharedDbCache, sst_block_size};
use common::storage::metrics_recorder::MetricsRsRecorder;
use common::storage::slate::warm::{WarmDb, WarmPlan, metadata_estimate, payload_estimate};
use common::storage::slate::{MEMTABLE_FLUSH_INTERVAL, SlateDbStorage as CommonSlateDbStorage};
use common::storage::{
    CheckpointInfo, MergeOptions, MergeRecordOp, PutOptions, PutRecordOp, Record, RecordOp,
    StorageError, StorageResult, WriteOptions, WriteResult,
};
use common::{BytesRange, Ttl, create_object_store};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, DbReaderOptions, ScanOptions, Settings,
    WriteOptions as SlateDbWriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::{
    CacheTarget, Db, DbBuilder, DbCacheManagerOps, DbIterator, DbMetadataOps, DbReadOps, DbReader,
    DbSnapshot, IterationOrder, WriteBatch,
};
use tracing::info;
use uuid::Uuid;

use crate::Namespace;
use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
use crate::model::{
    HistogramSample, Label, Sample, SeriesData, SeriesFingerprint, SeriesId, TimeBucket,
    covering_bucket, visible_buckets,
};
use crate::serde::dictionary::SeriesDictionaryValue;
use crate::serde::forward_index::ForwardIndexValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::key::{
    BucketGenerationKey, ForwardIndexKey, InvertedIndexKey, SeriesDictionaryKey,
    SeriesFingerprintKey, TimeSeriesKey, decode_bucket_generation, encode_bucket_generation,
    encode_series_fingerprint,
};
use crate::serde::{TimeBucketScoped, bucket_records_range, seal_key, write_bucket_prefix};
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::storage::segment_extractor::{
    Segment, TimeseriesSegmentExtractor, bucket_span, parse_bucket, parse_segment, superseded,
    unsealed,
};
use common::storage::compaction::{MergeHold, MergeHolds, SegmentCompaction};
use common::storage::settings::{base_settings, load_settings_over};
use slatedb::CompactorBuilder;

mod reader;
mod records;

pub(crate) use reader::*;
pub(crate) use records::*;

/// Private accessor that gives the blanket [`StorageRead`] impl access to a
/// handle's [`StorageReaderInner`] without exposing the inner type outside
/// this module.
trait HasReader {
    type Db: DbReadOps + Send + Sync;
    fn reader(&self) -> &StorageReaderInner<Self::Db>;
}

/// Read operations shared by every storage handle.
///
/// Implemented (via a blanket impl forwarding to the private
/// [`StorageReaderInner`]) by [`Storage`], [`StorageSnapshot`], and
/// [`StorageReader`], so per-bucket readers and background tasks can be
/// generic over the underlying SlateDB read handle. See the methods of the
/// same names on `StorageReaderInner` for the full documentation.
#[async_trait::async_trait]
pub(crate) trait StorageRead: Send + Sync {
    /// Retrieves a single value by exact key. Returns `Ok(None)` if absent.
    async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>>;

    /// Returns an iterator over the given key range.
    async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator>;

    /// Returns an iterator over keys starting with `prefix`. Unlike
    /// [`Self::scan`], consults SST prefix filters.
    async fn scan_prefix(&self, prefix: Bytes) -> StorageResult<DbIterator>;

    /// Returns the buckets overlapping `[start_secs, end_secs]`, sorted by
    /// start time.
    async fn get_buckets_in_range(
        &self,
        namespace: &Namespace,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<TimeBucket>>;

    /// Returns the buckets overlapping any of the given disjoint ranges.
    async fn get_buckets_for_ranges(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>>;

    /// Every bucket segment of `namespace`, visible or not, and every sealed
    /// bucket.
    async fn bucket_segments(&self, namespace: &Namespace) -> (Vec<TimeBucket>, Vec<TimeBucket>);

    /// The bucket each of `buckets` is read from: itself, or the sealed
    /// bucket covering it.
    async fn resolve_buckets(
        &self,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> Vec<TimeBucket>;

    /// Loads the full forward index of `bucket`.
    async fn get_forward_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<ForwardIndex>;

    /// Loads the full inverted index of `bucket`.
    async fn get_inverted_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<InvertedIndex>;

    /// Loads only the given terms from the inverted index (legacy batch path).
    async fn get_inverted_index_terms(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex>;

    /// Fetches a single inverted-index posting for `(bucket, term)`.
    async fn get_inverted_index_term(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        term: &Label,
    ) -> crate::util::Result<Option<RoaringBitmap>>;

    /// Loads only the given series from the forward index (legacy batch path).
    async fn get_forward_index_series(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex>;

    /// Fetches a single forward-index entry for `(bucket, series_id)`.
    async fn get_forward_index_one(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>>;

    /// The highest series ID with a forward-index entry in `bucket`.
    async fn last_forward_index_series(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
    ) -> crate::util::Result<Option<SeriesId>>;

    /// Loads the series dictionary of `bucket` through `insert` and returns
    /// the maximum series ID found.
    ///
    /// `Self: Sized` keeps the trait object-safe (for [`Store`]); call this
    /// on a concrete handle.
    async fn load_series_dictionary<F>(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
        Self: Sized;

    /// Returns all values of `label_name` within `bucket`.
    async fn get_label_values(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>>;

    /// The write generation of `bucket`, or `None` if the bucket has never
    /// been flushed (or its record has expired).
    async fn get_bucket_generation(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
    ) -> crate::util::Result<Option<u64>> {
        let key = BucketGenerationKey {
            namespace: namespace.clone(),
            bucket: *bucket,
        }
        .encode();
        match self.get(key).await? {
            Some(value) => Ok(Some(decode_bucket_generation(&value)?)),
            None => Ok(None),
        }
    }

    /// [`Self::get_bucket_generation`] for each of `buckets`, in order, read
    /// from the bucket it resolves to, so a write to a rollup bucket changes
    /// the generation of every bucket it covers.
    async fn get_bucket_generations(
        &self,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> crate::util::Result<Vec<Option<u64>>> {
        let resolved = self.resolve_buckets(namespace, buckets).await;
        futures::future::try_join_all(resolved.iter().map(|bucket| async move {
            // Generations count per bucket, so tag a rollup bucket's with its
            // width: an hour's generation must change when it resolves to a
            // rollup bucket that happens to sit at the same count.
            let generation = self.get_bucket_generation(namespace, bucket).await?;
            Ok::<_, crate::error::Error>(
                generation.map(|generation| generation | (u64::from(bucket.size) << 56)),
            )
        }))
        .await
    }
}

#[async_trait::async_trait]
impl<H: HasReader + Send + Sync> StorageRead for H {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>> {
        self.reader().get(key).await
    }

    async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator> {
        self.reader().scan(range).await
    }

    async fn scan_prefix(&self, prefix: Bytes) -> StorageResult<DbIterator> {
        self.reader().scan_prefix(prefix).await
    }

    async fn get_buckets_in_range(
        &self,
        namespace: &Namespace,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<TimeBucket>> {
        self.reader()
            .get_buckets_in_range(namespace, start_secs, end_secs)
            .await
    }

    async fn get_buckets_for_ranges(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>> {
        self.reader()
            .get_buckets_for_ranges(namespace, ranges)
            .await
    }

    async fn bucket_segments(&self, namespace: &Namespace) -> (Vec<TimeBucket>, Vec<TimeBucket>) {
        self.reader().list_segments(namespace)
    }

    async fn resolve_buckets(
        &self,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> Vec<TimeBucket> {
        self.reader().resolve_buckets(namespace, buckets)
    }

    async fn get_forward_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<ForwardIndex> {
        self.reader().get_forward_index(namespace, bucket).await
    }

    async fn get_inverted_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<InvertedIndex> {
        self.reader().get_inverted_index(namespace, bucket).await
    }

    async fn get_inverted_index_terms(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex> {
        self.reader()
            .get_inverted_index_terms(namespace, bucket, terms)
            .await
    }

    async fn get_inverted_index_term(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        term: &Label,
    ) -> crate::util::Result<Option<RoaringBitmap>> {
        self.reader()
            .get_inverted_index_term(namespace, bucket, term)
            .await
    }

    async fn get_forward_index_series(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex> {
        self.reader()
            .get_forward_index_series(namespace, bucket, series_ids)
            .await
    }

    async fn get_forward_index_one(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>> {
        self.reader()
            .get_forward_index_one(namespace, bucket, series_id)
            .await
    }

    async fn last_forward_index_series(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
    ) -> crate::util::Result<Option<SeriesId>> {
        self.reader()
            .last_forward_index_series(namespace, bucket)
            .await
    }

    async fn load_series_dictionary<F>(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
    {
        self.reader()
            .load_series_dictionary(namespace, bucket, insert)
            .await
    }

    async fn get_label_values(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>> {
        self.reader()
            .get_label_values(namespace, bucket, label_name)
            .await
    }
}

/// Block-cache warming for the storage handles that own a SlateDB cache
/// manager — the writer [`Storage`] and the read-only [`StorageReader`].
///
/// This is a separate trait from [`StorageRead`] because a [`StorageSnapshot`]
/// (over a `DbSnapshot`) has no cache-manager handle and so cannot warm; the
/// blanket impl below is gated on `Db: DbMetadataOps + DbCacheManagerOps`,
/// which `Db` and `DbReader` satisfy but `DbSnapshot` does not.
pub(crate) trait WarmStorage: StorageRead {
    /// Plans warming the SSTs backing the `wanted` buckets, including the
    /// sample data blocks only when `include_samples` is set. See
    /// [`StorageReaderInner::plan_warm`].
    fn plan_warm(
        &self,
        wanted: &[(Namespace, HashSet<TimeBucket>)],
        include_samples: bool,
        plan: &mut WarmPlan,
    );
}

impl<H> WarmStorage for H
where
    H: HasReader + Send + Sync,
    H::Db: DbMetadataOps + DbCacheManagerOps + 'static,
{
    fn plan_warm(
        &self,
        wanted: &[(Namespace, HashSet<TimeBucket>)],
        include_samples: bool,
        plan: &mut WarmPlan,
    ) {
        self.reader().plan_warm(wanted, include_samples, plan);
    }
}

/// The full storage surface — every [`StorageRead`] method plus the write
/// path. This is the flusher's dependency: [`Storage`] is the only production
/// implementation, and the trait exists so tests can substitute a failing
/// implementation and verify that errors from each flush phase are
/// propagated — the concrete SlateDB writer offers no way to inject
/// per-operation faults.
#[async_trait::async_trait]
pub(crate) trait Store: StorageRead {
    /// Applies a batch of mixed operations atomically.
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult>;

    /// Creates a point-in-time snapshot for consistent reads.
    async fn snapshot(&self) -> StorageResult<StorageSnapshot>;

    /// Flushes pending writes to durable storage.
    async fn flush(&self) -> StorageResult<()>;
}

/// Source of the segment prefixes visible to a storage handle.
///
/// `Db` and `DbReader` expose a live [`slatedb::DbStatus`] (via
/// `DbMetadataOps::status`), so their listers re-read the current segment
/// list on every call. `DbSnapshot` has no status accessor; its lister
/// returns the writer's segment list captured when the snapshot was taken,
/// which matches the snapshot's point-in-time semantics.
type SegmentLister = Arc<dyn Fn() -> Vec<slatedb::SegmentPrefix> + Send + Sync>;

/// A consistent point-in-time read view of the storage, wrapping a SlateDB
/// `DbSnapshot`. Reads go through [`StorageRead`].
#[derive(Clone)]
pub(crate) struct StorageSnapshot {
    reader: StorageReaderInner<DbSnapshot>,
}

impl HasReader for StorageSnapshot {
    type Db = DbSnapshot;
    fn reader(&self) -> &StorageReaderInner<DbSnapshot> {
        &self.reader
    }
}

/// Read/write SlateDB-backed storage.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability. Reads go through
/// [`StorageRead`]; cloning is cheap (the handles are `Arc`s).
#[derive(Clone)]
pub(crate) struct Storage {
    db: Arc<Db>,
    reader: StorageReaderInner<Db>,
    /// Set by every write, cleared by the periodic memtable flush.
    unflushed: Arc<AtomicBool>,
    /// Holds on the merges of buckets a rollup copies into.
    merge_holds: MergeHolds,
}

impl HasReader for Storage {
    type Db = Db;
    fn reader(&self) -> &StorageReaderInner<Db> {
        &self.reader
    }
}

impl Storage {
    /// Opens the storage from configuration, wired with the OpenTSDB merge
    /// operator, the timeseries segment extractor, the metrics recorder, and
    /// (when configured) the foyer block cache.
    pub(crate) async fn try_new(slate_config: &SlateDbStorageConfig) -> crate::util::Result<Self> {
        let object_store = create_object_store(&slate_config.object_store)?;
        Self::try_new_with_object_store(slate_config, object_store).await
    }

    /// Like [`Self::try_new`] but over an explicit object store, so tests can
    /// share an in-memory store between a writer and a reader.
    pub(crate) async fn try_new_with_object_store(
        slate_config: &SlateDbStorageConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> crate::util::Result<Self> {
        let cache = SharedDbCache::from_slatedb_config(slate_config).await?;
        Self::try_new_with_cache(slate_config, object_store, &cache, None).await
    }

    /// Like [`Self::try_new_with_object_store`] but uses `cache` instead of
    /// building one from the config. With a `retention`, compaction drops
    /// bucket segments once everything in them has expired.
    pub(crate) async fn try_new_with_cache(
        slate_config: &SlateDbStorageConfig,
        object_store: Arc<dyn ObjectStore>,
        cache: &SharedDbCache,
        retention: Option<Duration>,
    ) -> crate::util::Result<Self> {
        let mut settings = load_settings(slate_config)?;
        info!(
            "create slatedb storage with config: {:?}, settings: {:?}",
            slate_config, settings
        );

        let adapter = CommonSlateDbStorage::merge_operator_adapter(Arc::new(OpenTsdbMergeOperator));
        let policy = SegmentCompaction::new(bucket_span)
            .with_retention(retention)
            .with_superseded(Arc::new(superseded))
            .with_unsealed(Arc::new(unsealed));
        let merge_holds = policy.holds();
        let compactor = settings.compactor_options.take().map(|options| {
            CompactorBuilder::new(slate_config.path.clone(), Arc::clone(&object_store))
                .with_options(options)
                .with_scheduler_supplier(policy.supplier(&slate_config.path))
        });
        let mut builder = DbBuilder::new(slate_config.path.clone(), object_store)
            .with_settings(settings)
            .with_sst_block_size(sst_block_size(&slate_config.disk))
            .with_merge_operator(Arc::new(adapter))
            .with_segment_extractor(TimeseriesSegmentExtractor::shared())
            .with_metrics_recorder(Arc::new(MetricsRsRecorder));

        if let Some(cache) = cache.cache() {
            builder = builder.with_db_cache(cache);
        }
        if let Some(compactor) = compactor {
            builder = builder.with_compactor_builder(compactor);
        }

        let db = Arc::new(
            builder
                .build()
                .await
                .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?,
        );
        common::storage::health::StorageHealth::global()
            .watch(slate_config.path.clone(), db.subscribe());

        let mut storage = Self::from_db(db);
        storage.merge_holds = merge_holds;
        storage.spawn_memtable_flusher(MEMTABLE_FLUSH_INTERVAL);
        Ok(storage)
    }

    fn from_db(db: Arc<Db>) -> Self {
        Self {
            db: db.clone(),
            reader: StorageReaderInner {
                db: db.clone(),
                segments: Arc::new(move || db.status().list_segments()),
            },
            unflushed: Arc::new(AtomicBool::new(false)),
            merge_holds: MergeHolds::default(),
        }
    }

    /// Flushes the memtable to L0 every `every` while writes arrive; see
    /// [`MEMTABLE_FLUSH_INTERVAL`].
    fn spawn_memtable_flusher(&self, every: Duration) {
        common::storage::slate::spawn_memtable_flusher(
            Arc::downgrade(&self.db),
            Arc::clone(&self.unflushed),
            every,
        );
    }

    // ── write path ───────────────────────────────────────────────────

    /// Applies a batch of mixed operations atomically with default options
    /// (`await_durable: false`).
    pub(crate) async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        self.apply_with_options(ops, WriteOptions::default()).await
    }

    /// Applies a batch of mixed operations atomically with custom options.
    pub(crate) async fn apply_with_options(
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
        self.write_batch(batch, options).await
    }

    /// Writes records with default options (`await_durable: false`).
    pub(crate) async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<WriteResult> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    /// Writes records with custom options controlling durability.
    pub(crate) async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.put_with_options(op.record.key, op.record.value, &op.options.into());
        }
        self.write_batch(batch, options).await
    }

    /// Merges records using the configured merge operator, default options.
    pub(crate) async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.merge_with_options(op.record.key, op.record.value, &op.options.into());
        }
        self.write_batch(batch, WriteOptions::default()).await
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
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

    // ── lifecycle ────────────────────────────────────────────────────

    /// Creates a point-in-time snapshot for consistent reads.
    ///
    /// `DbSnapshot` exposes no status, so the writer's segment list is
    /// captured here and served unchanged for the snapshot's lifetime.
    pub(crate) async fn snapshot(&self) -> StorageResult<StorageSnapshot> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        let segments = self.db.status().list_segments();
        Ok(StorageSnapshot {
            reader: StorageReaderInner {
                db: snapshot,
                segments: Arc::new(move || segments.clone()),
            },
        })
    }

    /// Flushes pending writes to durable storage.
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    /// Creates a durable checkpoint covering all data.
    pub(crate) async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
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

    /// Closes the database (which also closes the block cache).
    pub(crate) async fn close(&self) -> StorageResult<()> {
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }
}

#[cfg(any(test, feature = "bench-internals"))]
impl Storage {
    /// Live layout across segments, from the manifest:
    /// `(segments, l0_ssts, sorted_runs, run_ssts)`.
    pub(crate) fn layout(&self) -> (usize, usize, usize, usize) {
        let manifest = self.db.manifest();
        let segments = manifest.segments();
        (
            segments.len(),
            segments.iter().map(|segment| segment.l0().len()).sum(),
            segments
                .iter()
                .map(|segment| segment.compacted().len())
                .sum(),
            segments
                .iter()
                .flat_map(|segment| segment.compacted())
                .map(|run| run.sst_views.len())
                .sum(),
        )
    }
}

impl Storage {
    /// Freezes the memtable and writes it to L0, merging operands up to
    /// SlateDB's oldest live snapshot.
    pub(crate) async fn flush_memtable(&self) -> StorageResult<()> {
        self.db
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .map_err(StorageError::from_storage)
    }

    /// Every bucket segment of `namespace`, visible or not, and every
    /// sealed bucket, as of now.
    pub(crate) fn segments(&self, namespace: &Namespace) -> (Vec<TimeBucket>, Vec<TimeBucket>) {
        self.reader.list_segments(namespace)
    }

    /// Every namespace with a segment in the manifest.
    pub(crate) fn namespaces(&self) -> Vec<Namespace> {
        let manifest = self.db.manifest();
        let namespaces: HashSet<Namespace> = manifest
            .segments()
            .iter()
            .filter_map(|segment| parse_segment(segment.prefix()).map(|(namespace, _)| namespace))
            .collect();
        namespaces.into_iter().collect()
    }

    /// Estimated on-disk bytes of each bucket segment of `namespace`, from
    /// the SSTs live in the manifest. Excludes the memtable.
    pub(crate) fn bucket_bytes(&self, namespace: &Namespace) -> HashMap<TimeBucket, u64> {
        let manifest = self.db.manifest();
        manifest
            .segments()
            .iter()
            .filter_map(|segment| {
                let (segment_namespace, bucket) = parse_bucket(segment.prefix())?;
                (&segment_namespace == namespace).then(|| {
                    let l0: u64 = segment.l0().iter().map(|view| view.estimate_size()).sum();
                    let runs: u64 = segment
                        .compacted()
                        .iter()
                        .map(|run| run.estimate_size())
                        .sum();
                    (bucket, l0 + runs)
                })
            })
            .collect()
    }

    /// Holds merges of `bucket`'s segment while a rollup copies into it.
    pub(crate) fn hold_merges(&self, namespace: &Namespace, bucket: TimeBucket) -> MergeHold {
        self.merge_holds
            .copying(bucket_segment_prefix(namespace, &bucket))
    }

    /// Waits up to `wait` for `bucket`'s segment to hold everything it held
    /// at `since_ms` in one sorted run. Returns whether it does.
    pub(crate) async fn await_merge(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
        since_ms: u64,
        wait: Duration,
    ) -> bool {
        common::storage::rollup::await_merge(
            || self.db.manifest(),
            &bucket_segment_prefix(namespace, &bucket),
            since_ms,
            wait,
        )
        .await
    }

    /// Durably marks `bucket` as holding every sample of the narrower
    /// buckets it covers, making it visible to reads in their place.
    pub(crate) async fn seal(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
        ttl: Ttl,
    ) -> StorageResult<()> {
        self.put_with_options(
            vec![PutRecordOp::new_with_options(
                Record::empty(seal_key(namespace, &bucket)),
                PutOptions { ttl },
            )],
            WriteOptions {
                await_durable: true,
            },
        )
        .await?;
        Ok(())
    }
}

fn bucket_segment_prefix(namespace: &Namespace, bucket: &TimeBucket) -> bytes::Bytes {
    let mut prefix = bytes::BytesMut::new();
    write_bucket_prefix(&mut prefix, namespace, bucket);
    prefix.freeze()
}

#[cfg(test)]
impl Storage {
    /// `(recent_snapshot_min_seq, last_l0_seq)` from the manifest. The first
    /// is the merge barrier compaction uses: the oldest live snapshot's
    /// sequence at the last L0 flush, or that flush's last sequence when no
    /// snapshot was live.
    pub(crate) fn merge_barrier(&self) -> (u64, u64) {
        let manifest = self.db.manifest();
        (manifest.recent_snapshot_min_seq(), manifest.last_l0_seq())
    }
}

#[async_trait::async_trait]
impl Store for Storage {
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        Storage::apply(self, ops).await
    }

    async fn snapshot(&self) -> StorageResult<StorageSnapshot> {
        Storage::snapshot(self).await
    }

    async fn flush(&self) -> StorageResult<()> {
        Storage::flush(self).await
    }
}

/// Metrics' `l0_sst_size_bytes`. SlateDB's 64 MiB default holds a busy
/// writer's merge operands in the memtable for many minutes, and every query
/// over recent data pays to merge them; see
/// `documentation/metrics/configuration.md`.
pub(crate) const DEFAULT_L0_SST_SIZE_BYTES: usize = 16 << 20;
/// Metrics' size-tiered `min_compaction_sources`, so small L0 SSTs are
/// folded into the sorted runs sooner than with SlateDB's 4; 2 merges
/// barely fewer operands for about twice the compaction bytes.
pub(crate) const DEFAULT_MIN_COMPACTION_SOURCES: usize = 3;

/// [`base_settings`] with Metrics' overrides applied; the base layer that
/// a settings file or `SLATEDB_` environment variables override per key.
pub(crate) fn default_settings() -> Settings {
    let mut settings = Settings {
        l0_sst_size_bytes: DEFAULT_L0_SST_SIZE_BYTES,
        ..base_settings()
    };
    if let Some(compactor) = settings.compactor_options.as_mut() {
        compactor.scheduler_options.insert(
            "min_compaction_sources".to_string(),
            DEFAULT_MIN_COMPACTION_SOURCES.to_string(),
        );
    }
    settings
}

/// Loads SlateDB settings over [`default_settings`], so Metrics' tuning
/// applies wherever the user leaves a key unset.
fn load_settings(slate_config: &SlateDbStorageConfig) -> StorageResult<Settings> {
    load_settings_over(default_settings(), slate_config)
}

#[cfg(test)]
mod tests;
