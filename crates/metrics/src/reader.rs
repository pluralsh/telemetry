//! Read-only time series database access.
//!
//! This module provides [`TimeSeriesDbReader`], a read-only view of a time
//! series database. It uses SlateDB's `DbReader` under the hood, which
//! coexists with a production writer without fencing — unlike `Db::open()`,
//! which always fences the previous writer.

use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use common::storage::config::SlateDbStorageConfig;
use futures::stream::{self, StreamExt, TryStreamExt};
use moka::future::Cache;
use uuid::Uuid;

use crate::Namespace;
use crate::error::{QueryError, Result};
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::{ForwardIndexCache, MiniQueryReader, ReplicaPostings, SeriesCache};
use crate::model::{
    Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, SeriesId, TimeBucket,
};
use crate::postings_cache::PostingsCache;
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::{StorageRead, StorageReader};
use crate::tsdb::{
    TsdbReadEngine, find_label_values_in_range, find_labels_in_range, find_series_in_range,
};

// ── ReaderQueryReader ────────────────────────────────────────────────

pub(crate) const DEFAULT_CACHE_CAPACITY: u64 = 50;

/// QueryReader implementation for read-only access.
///
/// Wraps `Arc<MiniQueryReader>` per bucket (unlike `TsdbQueryReader` which
/// uses owned `MiniQueryReader`).
pub(crate) struct ReaderQueryReader {
    mini_readers: HashMap<TimeBucket, Arc<MiniQueryReader<StorageReader>>>,
}

impl ReaderQueryReader {
    fn new(readers: Vec<(TimeBucket, Arc<MiniQueryReader<StorageReader>>)>) -> Self {
        Self {
            mini_readers: readers.into_iter().collect(),
        }
    }
}

#[async_trait]
impl QueryReader for ReaderQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        Ok(self.mini_readers.keys().cloned().collect())
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index(series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index(terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.all_inverted_index().await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_values(label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::model::SeriesData> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples(series_id, metric_name, start_ms, end_ms).await
    }

    async fn samples_many(
        &self,
        bucket: &TimeBucket,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<crate::model::SeriesData>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples_many(metric_name, series_ids, start_ms, end_ms)
            .await
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index_one(series_id).await
    }

    async fn forward_index_many(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Vec<Option<crate::index::SeriesSpec>>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index_many(series_ids).await
    }

    async fn label_postings(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<(String, roaring::RoaringBitmap)>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_postings(label_name).await
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<roaring::RoaringBitmap>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index_term(term).await
    }

    async fn cached_selector(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<roaring::RoaringBitmap>> {
        self.mini_readers.get(bucket)?.cached_selector(key).await
    }

    async fn cache_selector(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
        postings: &roaring::RoaringBitmap,
    ) {
        if let Some(mini) = self.mini_readers.get(bucket) {
            mini.cache_selector(key, postings).await;
        }
    }

    async fn cached_series_set(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<[crate::model::Labels]>> {
        self.mini_readers.get(bucket)?.cached_series_set(key).await
    }

    async fn cache_series_set(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
        series: Arc<[crate::model::Labels]>,
    ) {
        if let Some(mini) = self.mini_readers.get(bucket) {
            mini.cache_series_set(key, series).await;
        }
    }

    async fn cached_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<crate::query::CachedSeriesResolution>> {
        self.mini_readers
            .get(bucket)?
            .cached_selector_resolution(key)
            .await
    }

    async fn cache_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
        resolution: Arc<crate::query::CachedSeriesResolution>,
    ) {
        if let Some(mini) = self.mini_readers.get(bucket) {
            mini.cache_selector_resolution(key, resolution).await;
        }
    }
}

// ── TimeSeriesDbReader ───────────────────────────────────────────────

/// A read-only view of a time series database.
///
/// `TimeSeriesDbReader` provides the same read API as [`TimeSeriesDb`](crate::timeseries::TimeSeriesDb)
/// but without write operations. It uses SlateDB's `DbReader`, which opens the
/// database without fencing — this means it can safely coexist with a production
/// writer on the same storage path.
///
/// # When to Use
///
/// Use `TimeSeriesDbReader` when you need read-only access to production data
/// without risking writer fencing:
///
/// - **Benchmarking**: Run queries against production data from a separate process.
/// - **Ad hoc analysis**: Investigate metrics without affecting the running service.
/// - **Testing**: Verify data written by a production writer.
///
/// # Example
///
/// ```ignore
/// use common::StorageConfig;
/// use slatedb::config::DbReaderOptions;
/// use timeseries::TimeSeriesDbReader;
///
/// let storage = StorageConfig::default();
/// let reader_options = DbReaderOptions::default();
/// let reader = TimeSeriesDbReader::open(storage, reader_options, 50).await?;
///
/// let result = reader.query("rate(http_requests_total[5m])", None).await?;
/// ```
pub struct TimeSeriesDbReader {
    storage: StorageReader,
    /// LRU cache for read-only query buckets.
    query_cache: Cache<(Namespace, TimeBucket), Arc<MiniQueryReader<StorageReader>>>,
    forward_cache: Arc<ForwardIndexCache>,
    /// Half of the reader's cache budget, in bytes.
    series_cache: Arc<SeriesCache>,
    postings_caches: dashmap::DashMap<Namespace, Arc<PostingsCache>>,
    matcher_cache_capacity_bytes: u64,
    discovery_cache: crate::discovery::MetricsDiscoveryCache,
}

impl TimeSeriesDbReader {
    /// Opens a read-only view of the time series database.
    ///
    /// Uses SlateDB's `DbReader` which polls the manifest based on `reader_options`
    /// to discover new data. Does **not** fence the existing writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    pub async fn open(
        storage_config: SlateDbStorageConfig,
        reader_options: slatedb::config::DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        Self::open_inner(storage_config, reader_options, cache_capacity, None).await
    }

    /// Opens a read-only view pinned to a specific checkpoint.
    ///
    /// Unlike [`open`](Self::open), the returned reader serves a frozen view
    /// of the database as of the checkpoint and does not advance with newer
    /// writes. The checkpoint must already exist (typically created via
    /// `TimeSeriesDb::create_checkpoint`).
    pub async fn open_at_checkpoint(
        storage_config: SlateDbStorageConfig,
        reader_options: slatedb::config::DbReaderOptions,
        cache_capacity: u64,
        checkpoint_id: Uuid,
    ) -> Result<Self> {
        Self::open_inner(
            storage_config,
            reader_options,
            cache_capacity,
            Some(checkpoint_id),
        )
        .await
    }

    async fn open_inner(
        storage_config: SlateDbStorageConfig,
        reader_options: slatedb::config::DbReaderOptions,
        cache_capacity: u64,
        checkpoint_id: Option<Uuid>,
    ) -> Result<Self> {
        let cache = common::SharedDbCache::from_slatedb_config(&storage_config).await?;
        Self::open_with_cache(
            storage_config,
            reader_options,
            cache_capacity,
            checkpoint_id,
            &cache,
        )
        .await
    }

    /// Opens a reader whose SlateDB block cache is `cache` instead of one
    /// built from `storage_config`.
    pub(crate) async fn open_with_cache(
        storage_config: SlateDbStorageConfig,
        reader_options: slatedb::config::DbReaderOptions,
        cache_capacity: u64,
        checkpoint_id: Option<Uuid>,
        cache: &common::SharedDbCache,
    ) -> Result<Self> {
        let reader = StorageReader::try_new_with_cache(
            &storage_config,
            reader_options,
            checkpoint_id,
            common::create_object_store(&storage_config.object_store)?,
            cache,
        )
        .await?;
        Ok(Self::from_storage_with_capacity(reader, cache_capacity))
    }

    /// Creates a TimeSeriesDbReader from an existing storage implementation.
    pub(crate) fn from_storage(storage: StorageReader) -> Self {
        Self::from_storage_with_capacity(storage, DEFAULT_CACHE_CAPACITY)
    }

    fn from_storage_with_capacity(storage: StorageReader, cache_capacity: u64) -> Self {
        let query_cache = Cache::builder().max_capacity(cache_capacity).build();
        Self {
            storage,
            query_cache,
            forward_cache: Arc::new(ForwardIndexCache::new(
                crate::minitsdb::DEFAULT_FORWARD_CACHE_CAPACITY_BYTES,
            )),
            series_cache: Arc::new(SeriesCache::new(cache_capacity / 2)),
            postings_caches: dashmap::DashMap::new(),
            matcher_cache_capacity_bytes: crate::config::QueryCacheConfig::default()
                .matcher_capacity_bytes,
            discovery_cache: crate::discovery::MetricsDiscoveryCache::new(),
        }
    }

    /// Sets the byte budget of each namespace's selector-matcher cache.
    pub fn with_matcher_cache_capacity(mut self, bytes: u64) -> Self {
        self.matcher_cache_capacity_bytes = bytes;
        self
    }

    /// Sets the shared forward-index and resolved-selector cache budget.
    pub fn with_forward_index_cache_capacity(mut self, bytes: u64) -> Self {
        self.forward_cache = Arc::new(ForwardIndexCache::new(bytes));
        self
    }

    /// The write generations of `buckets` in `namespace`, in order, as of
    /// the last manifest poll; `None` for a bucket never flushed.
    pub(crate) async fn bucket_generations(
        &self,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> Result<Vec<Option<u64>>> {
        self.storage
            .get_bucket_generations(namespace, buckets)
            .await
    }

    /// Returns a read handle to the underlying storage, for background tasks
    /// like the cache warmer.
    pub(crate) fn storage_read(&self) -> StorageReader {
        self.storage.clone()
    }

    pub(crate) async fn make_query_reader_for_ranges(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> Result<ReaderQueryReader> {
        ScopedReader {
            namespace,
            reader: self,
        }
        .make_query_reader_for_ranges(ranges)
        .await
    }

    /// A bucket reader for one query, built from the cached bucket reader.
    async fn bucket_reader_for_query(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniQueryReader<StorageReader>>> {
        let storage = self.storage.clone();
        let forward_cache = Arc::clone(&self.forward_cache);
        let series_cache = Arc::clone(&self.series_cache);
        let postings = self
            .postings_caches
            .entry(namespace.clone())
            .or_insert_with(|| {
                Arc::new(PostingsCache::new(None, self.matcher_cache_capacity_bytes))
            })
            .clone();
        let owned_namespace = namespace.clone();
        let cached = self
            .query_cache
            .get_with((namespace.clone(), bucket), async move {
                Arc::new(
                    MiniQueryReader::new(owned_namespace, bucket, storage)
                        .with_forward_cache(forward_cache)
                        .with_series_cache(series_cache)
                        .with_replica_postings(Arc::new(ReplicaPostings::new(postings))),
                )
            })
            .await;
        Ok(Arc::new(cached.for_query().await?))
    }

    // ── Public inherent methods ───────────────────────────────────────

    /// Evaluates an instant PromQL query at a single point in time.
    ///
    /// If `time` is `None`, the current wall-clock time is used.
    pub async fn query(
        &self,
        namespace: &Namespace,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        ScopedReader {
            namespace,
            reader: self,
        }
        .eval_query(query, time, &QueryOptions::default())
        .await
    }

    /// Evaluates a range PromQL query over a time interval.
    pub async fn query_range(
        &self,
        namespace: &Namespace,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        <ScopedReader<'_> as TsdbReadEngine>::eval_query_range(
            &ScopedReader {
                namespace,
                reader: self,
            },
            query,
            range,
            step,
            &QueryOptions::default(),
        )
        .await
    }

    /// Returns the set of label-sets matching the given series matchers.
    pub async fn series(
        &self,
        namespace: &Namespace,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        find_series_in_range(
            &ScopedReader {
                namespace,
                reader: self,
            },
            matchers,
            range,
        )
        .await
    }

    /// Returns the set of label names matching the given matchers.
    pub async fn labels(
        &self,
        namespace: &Namespace,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        if matchers.is_none_or(<[&str]>::is_empty) {
            let (start, end) = crate::util::range_bounds_to_secs(range)?;
            let buckets = self
                .storage
                .get_buckets_in_range(namespace, Some(start), Some(end))
                .await
                .map_err(QueryError::from)?;
            crate::discovery::names(
                self.storage.clone(),
                namespace,
                &buckets,
                &self.discovery_cache,
            )
            .await
            .map_err(QueryError::from)
        } else {
            find_labels_in_range(
                &ScopedReader {
                    namespace,
                    reader: self,
                },
                matchers,
                range,
            )
            .await
        }
    }

    /// Closes the underlying storage reader, flushing any caches to disk.
    pub async fn close(&self) -> Result<()> {
        self.storage.close().await?;
        Ok(())
    }

    /// Returns the set of values for a given label name.
    pub async fn label_values(
        &self,
        namespace: &Namespace,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        if matchers.is_none_or(<[&str]>::is_empty) {
            let (start, end) = crate::util::range_bounds_to_secs(range)?;
            let buckets = self
                .storage
                .get_buckets_in_range(namespace, Some(start), Some(end))
                .await
                .map_err(QueryError::from)?;
            crate::discovery::values(
                self.storage.clone(),
                namespace,
                &buckets,
                label_name,
                &self.discovery_cache,
            )
            .await
            .map_err(QueryError::from)
        } else {
            find_label_values_in_range(
                &ScopedReader {
                    namespace,
                    reader: self,
                },
                label_name,
                matchers,
                range,
            )
            .await
        }
    }

    /// Returns metric metadata from the durable discovery catalog.
    pub async fn metadata(
        &self,
        namespace: &Namespace,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let buckets = self
            .storage
            .get_buckets_in_range(namespace, None, None)
            .await
            .map_err(QueryError::from)?;
        crate::discovery::metadata(
            self.storage.clone(),
            namespace,
            &buckets,
            metric,
            &self.discovery_cache,
        )
        .await
        .map_err(QueryError::from)
    }
}

// ── TsdbReadEngine for TimeSeriesDbReader ────────────────────────────

/// Maximum number of buckets to load concurrently.
const BUCKET_LOAD_CONCURRENCY: usize = 16;

pub(crate) struct ScopedReader<'a> {
    pub(crate) namespace: &'a Namespace,
    pub(crate) reader: &'a TimeSeriesDbReader,
}

#[async_trait]
impl TsdbReadEngine for ScopedReader<'_> {
    type QR = ReaderQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<ReaderQueryReader> {
        let buckets = self
            .reader
            .storage
            .get_buckets_in_range(self.namespace, Some(start), Some(end))
            .await?;

        let readers: Vec<_> = stream::iter(buckets)
            .map(|bucket| async move {
                let mini = self
                    .reader
                    .bucket_reader_for_query(self.namespace, bucket)
                    .await?;
                Ok::<_, crate::error::Error>((bucket, mini))
            })
            .buffer_unordered(BUCKET_LOAD_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(ReaderQueryReader::new(readers))
    }

    async fn make_query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<ReaderQueryReader> {
        let buckets = {
            let _g = crate::promql::trace::Scope::enter("list_buckets");
            self.reader
                .storage
                .get_buckets_for_ranges(self.namespace, ranges)
                .await?
        };

        let readers: Vec<_> = stream::iter(buckets)
            .map(|bucket| async move {
                let _g = crate::promql::trace::Scope::enter("bucket_load");
                let mini = self
                    .reader
                    .bucket_reader_for_query(self.namespace, bucket)
                    .await?;
                Ok::<_, crate::error::Error>((bucket, mini))
            })
            .buffer_unordered(BUCKET_LOAD_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(ReaderQueryReader::new(readers))
    }
}

#[async_trait]
impl TsdbReadEngine for TimeSeriesDbReader {
    type QR = ReaderQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<Self::QR> {
        let namespace = Namespace::default();
        ScopedReader {
            namespace: &namespace,
            reader: self,
        }
        .make_query_reader(start, end)
        .await
    }

    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<Self::QR> {
        let namespace = Namespace::default();
        ScopedReader {
            namespace: &namespace,
            reader: self,
        }
        .make_query_reader_for_ranges(ranges)
        .await
    }
}

#[cfg(test)]
mod tests;
