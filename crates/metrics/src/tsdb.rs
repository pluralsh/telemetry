use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use moka::future::Cache;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};
use tokio::sync::Mutex;
use tracing::error;

use crate::Namespace;
use crate::active_series::{ActiveSeriesTracker, current_unix_minute};
use crate::error::QueryError;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::{MiniQueryReader, MiniTsdb};
use crate::model::{
    Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, Series, SeriesId,
    TimeBucket,
};
use crate::postings_cache::PostingsCache;
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::{Storage, StorageRead, StorageSnapshot};
use crate::tsdb_metrics;
use crate::util::Result;

mod discovery;
mod engine;
mod execute;
mod preload;

pub(crate) use discovery::*;
pub(crate) use engine::*;
pub(crate) use execute::*;
pub(crate) use preload::*;

#[async_trait]
pub(crate) trait TsdbReadEngine: Send + Sync {
    type QR: QueryReader + Send + Sync;

    /// Build a query reader spanning `[start, end]` (seconds).
    async fn make_query_reader(&self, start: i64, end: i64) -> Result<Self::QR>;

    /// Build a query reader spanning a set of disjoint ranges (seconds).
    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<Self::QR>;

    // ── Provided: 5 default methods written once ──

    /// Discover series matching any of the given selectors.
    async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_series(&reader, matchers).await
    }

    /// Discover label names, optionally filtered by matchers.
    async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_labels(&reader, matchers).await
    }

    /// Discover values for a specific label, optionally filtered by matchers.
    async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_label_values(&reader, label_name, matchers).await
    }

    /// Evaluate an instant PromQL query, returning a [`QueryValue`]
    /// (scalar, vector or matrix). Parses the query, lowers to the
    /// logical plan, runs the rule-based optimiser, builds a physical
    /// plan over a [`crate::promql::source_adapter::QueryReaderSource`]
    /// wrapping this engine's [`QueryReader`], drives the operator tree
    /// to completion and reshapes collected batches into a `QueryValue`.
    async fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError>
    where
        Self::QR: 'static,
    {
        let start = Instant::now();
        let result = self
            .eval_query_traced(query, time, opts, None)
            .await
            .map(|o| o.value);

        metrics::counter!(tsdb_metrics::TSDB_QUERIES, "type" => "instant").increment(1);
        metrics::histogram!(tsdb_metrics::TSDB_QUERY_DURATION_SECONDS, "type" => "instant")
            .record(start.elapsed().as_secs_f64());

        result
    }

    /// Tracing-aware variant of [`Self::eval_query`]. Pass `trace`
    /// `Some(...)` to populate [`ExecuteOutcome::trace`] on the result.
    async fn eval_query_traced(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError>
    where
        Self::QR: 'static,
    {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let mut plan_ctx = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(opts.lookback_delta),
        );
        if let Some(c) = trace {
            plan_ctx = plan_ctx.with_trace(c);
        }
        let collector = plan_ctx.trace.clone();

        let ranges = preload_ranges_for_query(query, at_ms, at_ms, opts.lookback_delta)?;
        let t0 = Instant::now();
        let build_reader = self.make_query_reader_for_ranges(&ranges);
        let reader = match collector.clone() {
            Some(c) => crate::promql::trace::with_trace(c, build_reader).await?,
            None => build_reader.await?,
        };
        if let Some(c) = collector.as_ref() {
            c.record_phase(
                crate::promql::trace::Phase::ReaderSetup,
                t0.elapsed().as_nanos() as u64,
            );
        }
        execute_query(query, reader, plan_ctx, /*is_instant=*/ true, opts).await
    }

    /// Evaluate a range PromQL query and project the resulting
    /// [`QueryValue`] onto the `Vec<RangeSample>` wire contract.
    async fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError>
    where
        Self::QR: 'static,
    {
        let start = Instant::now();
        let result = self
            .eval_query_range_traced(query, range, step, opts, None)
            .await
            .and_then(|o| query_value_to_range_samples(o.value));

        metrics::counter!(tsdb_metrics::TSDB_QUERIES, "type" => "range").increment(1);
        metrics::histogram!(tsdb_metrics::TSDB_QUERY_DURATION_SECONDS, "type" => "range")
            .record(start.elapsed().as_secs_f64());

        result
    }

    /// Like [`Self::eval_query_range`] but returns the raw [`QueryValue`]
    /// so HTTP handlers can decide whether to keep scalar/vector shapes
    /// intact.
    async fn eval_query_range_value(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError>
    where
        Self::QR: 'static,
    {
        self.eval_query_range_traced(query, range, step, opts, None)
            .await
            .map(|o| o.value)
    }

    /// Tracing-aware variant of [`Self::eval_query_range`].
    async fn eval_query_range_traced(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError>
    where
        Self::QR: 'static,
    {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }

        let mut plan_ctx = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(opts.lookback_delta),
        );
        if let Some(c) = trace {
            plan_ctx = plan_ctx.with_trace(c);
        }
        let collector = plan_ctx.trace.clone();

        let ranges = preload_ranges_for_query(query, start_ms, end_ms, opts.lookback_delta)?;
        let t0 = Instant::now();
        let build_reader = self.make_query_reader_for_ranges(&ranges);
        let reader = match collector.clone() {
            Some(c) => crate::promql::trace::with_trace(c, build_reader).await?,
            None => build_reader.await?,
        };
        if let Some(c) = collector.as_ref() {
            c.record_phase(
                crate::promql::trace::Phase::ReaderSetup,
                t0.elapsed().as_nanos() as u64,
            );
        }
        execute_query(query, reader, plan_ctx, /*is_instant=*/ false, opts).await
    }
}

/// Multi-bucket time series database.
///
/// Tsdb manages multiple MiniTsdb instances (one per time bucket) and provides
/// a unified QueryReader interface that merges results across buckets.
pub(crate) struct Tsdb {
    namespace: Namespace,
    storage: Arc<Storage>,

    /// TTI cache (15 min idle) for buckets being actively ingested into.
    /// Also used during queries so that unflushed data is visible.
    ingest_cache: Cache<TimeBucket, Arc<MiniTsdb>>,
    discovery_cache: crate::discovery::MetricsDiscoveryCache,
    /// Serializes cache misses so concurrent writes cannot construct two
    /// coordinators for the same bucket.
    bucket_creation: Mutex<()>,

    /// Retention duration plumbed into each `MiniTsdb` so the flusher can
    /// stamp records with a bucket-aligned `Ttl::ExpireAt`. `None` disables
    /// per-record expiration.
    retention: Option<Duration>,
    write_buffer: common::coordinator::WriteCoordinatorConfig,

    /// Rolling HLL ring estimating unique series seen in the last ~15 min.
    /// Updated and published to `tsdb_active_series` by the flusher.
    active_series: Arc<ActiveSeriesTracker>,

    /// Inverted-index postings shared across queries; bucket flushers
    /// invalidate it as they add series.
    postings_cache: Arc<PostingsCache>,
}

impl Tsdb {
    pub(crate) fn new(storage: Arc<Storage>) -> Self {
        Self::with_retention_scoped(Namespace::default(), storage, None)
    }

    pub(crate) fn with_retention(storage: Arc<Storage>, retention: Option<Duration>) -> Self {
        Self::with_retention_scoped(Namespace::default(), storage, retention)
    }

    pub(crate) fn with_retention_scoped(
        namespace: Namespace,
        storage: Arc<Storage>,
        retention: Option<Duration>,
    ) -> Self {
        Self::with_retention_scoped_and_buffer(
            namespace,
            storage,
            retention,
            common::coordinator::WriteCoordinatorConfig::default(),
        )
    }

    pub(crate) fn with_retention_scoped_and_buffer(
        namespace: Namespace,
        storage: Arc<Storage>,
        retention: Option<Duration>,
        write_buffer: common::coordinator::WriteCoordinatorConfig,
    ) -> Self {
        // TTI cache: 15 minute idle timeout for ingest buckets
        let ingest_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(15 * 60))
            .build();

        let active_series = Arc::new(ActiveSeriesTracker::new(current_unix_minute()));

        Self {
            namespace,
            storage,
            ingest_cache,
            discovery_cache: crate::discovery::MetricsDiscoveryCache::new(),
            bucket_creation: Mutex::new(()),
            retention,
            write_buffer,
            active_series,
            postings_cache: Arc::new(PostingsCache::new(retention)),
        }
    }

    /// Returns a read handle to the underlying storage, for background tasks
    /// like the cache warmer.
    pub(crate) fn storage_read(&self) -> Storage {
        (*self.storage).clone()
    }

    /// Get or create a MiniTsdb for ingestion into a specific bucket.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_or_create_for_ingest(
        &self,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniTsdb>> {
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        let _creation = self.bucket_creation.lock().await;
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // Load from storage and put in ingest cache while holding the
        // creation lock. This guarantees one coordinator per bucket.
        let mini = Arc::new(
            MiniTsdb::load(
                self.namespace.clone(),
                bucket,
                self.storage.clone(),
                self.retention,
                self.active_series.clone(),
                self.write_buffer.clone(),
                Some(self.postings_cache.clone()),
            )
            .await?,
        );
        self.ingest_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Create a QueryReader for a time range.
    /// For buckets in the ingest cache, uses the write coordinator's view
    /// (includes unflushed data). For all other buckets, constructs a
    /// lightweight reader directly from the storage snapshot.
    pub(crate) async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbQueryReader> {
        let read_at = self.postings_cache.read_seq();
        let snapshot = self.storage.snapshot().await?;
        let mut buckets = snapshot
            .get_buckets_in_range(&self.namespace, Some(start_secs), Some(end_secs))
            .await?;
        self.ingest_cache.run_pending_tasks().await;
        for (key, _) in self.ingest_cache.iter() {
            let bucket = *key;
            let bucket_start = i64::from(bucket.start) * 60;
            let bucket_end = bucket_start + i64::from(bucket.size_in_mins()) * 60;
            if bucket_end > start_secs && bucket_start <= end_secs && !buckets.contains(&bucket) {
                buckets.push(bucket);
            }
        }
        buckets.sort_by_key(|bucket| bucket.start);

        let readers = self.build_readers(&snapshot, read_at, buckets).await;
        Ok(TsdbQueryReader::new(readers))
    }

    /// Create a QueryReader for a set of disjoint time ranges.
    pub(crate) async fn query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<TsdbQueryReader> {
        let read_at = self.postings_cache.read_seq();
        let snapshot = {
            let _g = crate::promql::trace::Scope::enter("snapshot");
            self.storage.snapshot().await?
        };
        let mut buckets = {
            let _g = crate::promql::trace::Scope::enter("list_buckets");
            snapshot
                .get_buckets_for_ranges(&self.namespace, ranges)
                .await?
        };
        self.ingest_cache.run_pending_tasks().await;
        for (key, _) in self.ingest_cache.iter() {
            let bucket = *key;
            let bucket_start = i64::from(bucket.start) * 60;
            let bucket_end = bucket_start + i64::from(bucket.size_in_mins()) * 60;
            if ranges
                .iter()
                .any(|(start, end)| bucket_end > *start && bucket_start <= *end)
                && !buckets.contains(&bucket)
            {
                buckets.push(bucket);
            }
        }
        buckets.sort_by_key(|bucket| bucket.start);

        let readers = {
            let _g = crate::promql::trace::Scope::enter("build_readers");
            self.build_readers(&snapshot, read_at, buckets).await
        };
        Ok(TsdbQueryReader::new(readers))
    }

    /// Build a reader over `snapshot` for each bucket. `read_at` is the
    /// postings-cache sequence read before `snapshot` was taken.
    async fn build_readers(
        &self,
        snapshot: &StorageSnapshot,
        read_at: u64,
        buckets: Vec<TimeBucket>,
    ) -> Vec<(TimeBucket, MiniQueryReader<StorageSnapshot>)> {
        let mut readers = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let reader = MiniQueryReader::new(self.namespace.clone(), bucket, snapshot.clone())
                .with_postings_cache(self.postings_cache.clone(), read_at);
            readers.push((bucket, reader));
        }
        readers
    }

    /// Flush all dirty buckets to durable storage.
    ///
    /// First flushes each bucket's delta to the storage memtable in parallel,
    /// then issues a single `storage.flush()` to persist everything durably.
    pub(crate) async fn flush(&self) -> Result<()> {
        self.flush_written().await?;
        self.storage.flush().await?;
        Ok(())
    }

    /// Flush all applied deltas into SlateDB's writable state without waiting
    /// for object-store durability.
    pub(crate) async fn flush_written(&self) -> Result<()> {
        // `iter()` does not include entries whose insert is still queued
        // in moka's internal write buffer; drain it first so a bucket
        // created moments before flush isn't silently skipped.
        self.ingest_cache.run_pending_tasks().await;
        let futs: futures::stream::FuturesUnordered<_> = self
            .ingest_cache
            .iter()
            .map(|(_, mini)| async move { mini.flush_written().await })
            .collect();
        futs.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    /// Flushes pending writes and creates a durable checkpoint.
    ///
    /// The returned [`common::CheckpointInfo::id`] can be passed to
    /// [`crate::reader::TimeSeriesDbReader::open`] (via
    /// [`common::StorageReaderRuntime::with_checkpoint_id`]) to open a
    /// reader pinned to this exact view of the database.
    pub(crate) async fn create_checkpoint(&self) -> Result<common::CheckpointInfo> {
        self.flush().await?;
        Ok(self.storage.create_checkpoint().await?)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.flush().await?;
        self.storage.close().await?;
        Ok(())
    }

    /// Ingest series into the TSDB.
    /// Each series is split by time bucket based on sample timestamps.
    ///
    /// If `timeout` is provided, each bucket batch will wait up to the given
    /// duration for space in the write queue. Otherwise, writes fail
    /// immediately if the queue is full.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            series_count = series_list.len(),
            total_samples = tracing::field::Empty,
            buckets_touched = tracing::field::Empty
        )
    )]
    pub(crate) async fn ingest_samples(
        &self,
        series_list: Vec<Series>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        if !series_list.is_empty() {
            self.discovery_cache.clear();
        }
        let mut bucket_series_map: HashMap<TimeBucket, Vec<Series>> = HashMap::new();
        let mut total_samples = 0;

        // First pass: group all series by bucket
        for series in series_list {
            total_samples += series.sample_count();
            let partitions = series.partition_by(|timestamp_ms| {
                TimeBucket::round_to_hour(
                    std::time::UNIX_EPOCH + std::time::Duration::from_millis(timestamp_ms as u64),
                )
            })?;
            for (bucket, series) in partitions {
                bucket_series_map.entry(bucket).or_default().push(series);
            }
        }

        let buckets_touched = bucket_series_map.len();

        // Second pass: ingest all series for each bucket in a single batch
        for (bucket, series_list) in bucket_series_map {
            let series_count = series_list.len();
            let samples_count: usize = series_list.iter().map(Series::sample_count).sum();

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Ingesting batch into bucket"
            );

            let mini = match self.get_or_create_for_ingest(bucket).await {
                Ok(mini) => mini,
                Err(err) => {
                    error!("failed to load minitsdb: {:?}: {:?}", bucket, err);
                    return Err(err);
                }
            };
            mini.ingest_batch(&series_list, timeout).await?;

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Bucket batch ingestion completed"
            );
        }

        // Record final metrics on the main span
        tracing::Span::current().record("total_samples", total_samples);
        tracing::Span::current().record("buckets_touched", buckets_touched);

        metrics::counter!(tsdb_metrics::TSDB_SAMPLES_INGESTED).increment(total_samples as u64);

        tracing::debug!(
            total_samples = total_samples,
            buckets_touched = buckets_touched,
            "Completed ingesting all samples"
        );

        Ok(())
    }

    /// Return metadata for all (or a specific) metric.
    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let snapshot = self.storage.snapshot().await.map_err(QueryError::from)?;
        let buckets = snapshot
            .get_buckets_in_range(&self.namespace, None, None)
            .await
            .map_err(QueryError::from)?;
        crate::discovery::metadata(
            snapshot,
            &self.namespace,
            &buckets,
            metric,
            &self.discovery_cache,
        )
        .await
        .map_err(QueryError::from)
    }

    pub(crate) async fn catalog_labels(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let snapshot = self.storage.snapshot().await.map_err(QueryError::from)?;
        let buckets = snapshot
            .get_buckets_in_range(&self.namespace, Some(start_secs), Some(end_secs))
            .await
            .map_err(QueryError::from)?;
        crate::discovery::names(snapshot, &self.namespace, &buckets, &self.discovery_cache)
            .await
            .map_err(QueryError::from)
    }

    pub(crate) async fn catalog_label_values(
        &self,
        label_name: &str,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let snapshot = self.storage.snapshot().await.map_err(QueryError::from)?;
        let buckets = snapshot
            .get_buckets_in_range(&self.namespace, Some(start_secs), Some(end_secs))
            .await
            .map_err(QueryError::from)?;
        crate::discovery::values(
            snapshot,
            &self.namespace,
            &buckets,
            label_name,
            &self.discovery_cache,
        )
        .await
        .map_err(QueryError::from)
    }
}

#[async_trait]
impl TsdbReadEngine for Tsdb {
    type QR = TsdbQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<TsdbQueryReader> {
        self.query_reader(start, end).await
    }

    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<TsdbQueryReader> {
        self.query_reader_for_ranges(ranges).await
    }
}

// ── TsdbEngine: unified read/write or read-only dispatch ────────────

#[cfg(test)]
mod tests;
