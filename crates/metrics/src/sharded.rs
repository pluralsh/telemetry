use std::collections::HashSet;
use std::ops::RangeBounds;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use common::{SharedDbCache, SstWarmTracker};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use roaring::RoaringBitmap;
use sharding::{
    ReaderShardLifecycle, ShardDatabase, ShardId, ShardMap, ShardRole, ShardSet, ShardSetError,
    ShardingOptions, shard_opener,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::admission::QueryAdmission;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{SeriesId, TimeBucket};
use crate::promql::memory::QueryError as SourceQueryError;
use crate::promql::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use crate::promql::source_adapter::QueryReaderSource;
use crate::query::{CachedSeriesResolution, LimitedQueryReader, QueryLimits, QueryReader};
use crate::reader::ReaderQueryReader;
use crate::remote::{MetricsShardClient, RemoteShard, RemoteShardSource};
use crate::result_cache::{CachePlan, Generations, ResultCache, has_duplicate_labels, merge};
use crate::storage::{StorageRead, WarmStorage};
use crate::tsdb::{
    TsdbQueryReader, TsdbReadEngine, duration_to_ms, execute_query_source,
    preload_ranges_for_query, query_value_to_range_samples, system_time_to_ms,
};
use crate::{
    Config, Error, Label, Labels, MetricMetadata, Namespace, QueryCacheConfig, QueryError,
    QueryLimitsConfig, QueryValue, RangeSample, Result, Series, TimeSeriesDb, TimeSeriesDbReader,
    Visibility,
};

const SOURCE_BUCKET_BITS: u32 = 40;
const SOURCE_BUCKET_MASK: u64 = (1 << SOURCE_BUCKET_BITS) - 1;

async fn acquire_io_permit(permits: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    let start = Instant::now();
    let permit = Arc::clone(permits)
        .acquire_owned()
        .await
        .map_err(|_| ShardSetError::Closed)?;
    crate::promql::trace::record_io_concurrency_wait(start.elapsed().as_nanos() as u64);
    Ok(permit)
}

/// One storage shard, opened either as the single writer or as a reader.
pub enum MetricsShard {
    Writer(TimeSeriesDb),
    Reader(Box<TimeSeriesDbReader>),
}

#[async_trait]
impl ShardDatabase for MetricsShard {
    type Error = Error;

    async fn flush_database(&self) -> Result<()> {
        match self {
            Self::Writer(db) => db.flush().await,
            Self::Reader(_) => Ok(()),
        }
    }

    async fn close_database(self: Arc<Self>) -> Result<()> {
        match Arc::try_unwrap(self) {
            Ok(Self::Writer(db)) => db.close().await,
            Ok(Self::Reader(db)) => db.close().await,
            Err(shared) => match &*shared {
                Self::Reader(db) => db.close().await,
                Self::Writer(_) => Err(Error::Internal(
                    "shard database still has in-flight references".into(),
                )),
            },
        }
    }
}

fn shard_storage(config: &Config, shard: ShardId) -> common::storage::config::SlateDbStorageConfig {
    let mut storage = config.storage.clone();
    storage.path = ShardingOptions::shard_path(&config.storage.path, shard);
    storage
}

async fn warm_storage<S: StorageRead + WarmStorage>(
    storage: S,
    namespaces: &[Namespace],
    (start, end): (i64, i64),
    include_samples: bool,
    concurrency: usize,
    cancel: &CancellationToken,
    tracker: Option<&SstWarmTracker>,
) -> Result<()> {
    for namespace in namespaces {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let buckets = storage
            .get_buckets_in_range(namespace, Some(start), Some(end))
            .await?;
        storage
            .warm(
                namespace,
                buckets,
                include_samples,
                concurrency,
                cancel,
                tracker,
            )
            .await?;
    }
    Ok(())
}

/// Namespace-aware facade over locally owned storage shards.
///
/// Every shard shares one SlateDB block and metadata cache, so the configured
/// cache capacities bound the whole process rather than each shard.
pub struct ShardedMetrics {
    shards: Arc<ShardSet<MetricsShard>>,
    block_cache: SharedDbCache,
    result_cache: Option<ResultCache>,
    admission: QueryAdmission,
    /// Reads the shards other reader groups serve; unset outside reader
    /// groups, where every shard is open locally.
    remote: OnceLock<Arc<dyn MetricsShardClient>>,
}

pub type ShardedTimeseries = ShardedMetrics;

impl ShardedMetrics {
    async fn new(
        shards: Result<ShardSet<MetricsShard>>,
        block_cache: SharedDbCache,
        caches: &QueryCacheConfig,
        limits: &QueryLimitsConfig,
    ) -> Result<Self> {
        let shards = match shards {
            Ok(shards) => shards,
            Err(error) => {
                block_cache.close().await?;
                return Err(error);
            }
        };
        Ok(Self {
            shards: Arc::new(shards),
            block_cache,
            result_cache: caches
                .result_cache_enabled
                .then(|| ResultCache::new(caches.result_capacity_bytes)),
            admission: QueryAdmission::new(limits),
            remote: OnceLock::new(),
        })
    }

    pub async fn open_writers(
        config: Config,
        options: ShardingOptions,
        owned_shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let caches = config.query_cache;
        let limits = config.query_limits;
        let block_cache = SharedDbCache::from_slatedb_config(&config.storage).await?;
        let shard_cache = block_cache.clone();
        let opener = shard_opener(move |shard| {
            let mut config = config.clone();
            config.storage = shard_storage(&config, shard);
            let cache = shard_cache.clone();
            async move {
                TimeSeriesDb::open_with_cache(config, &cache)
                    .await
                    .map(MetricsShard::Writer)
            }
        });
        let shards = ShardSet::open(ShardRole::Writer, options, opener, owned_shards).await;
        Self::new(shards, block_cache, &caches, &limits).await
    }

    pub async fn open_readers(
        config: Config,
        options: ShardingOptions,
        local_shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        let caches = config.query_cache;
        let limits = config.query_limits;
        let matcher_capacity = caches.matcher_capacity_bytes;
        let forward_index_capacity = caches.forward_index_capacity_bytes;
        let block_cache = SharedDbCache::from_slatedb_config(&config.storage).await?;
        let shard_cache = block_cache.clone();
        let opener = shard_opener(move |shard| {
            let storage = shard_storage(&config, shard);
            let reader_options = reader_options.clone();
            let cache = shard_cache.clone();
            async move {
                TimeSeriesDbReader::open_with_cache(
                    storage,
                    reader_options,
                    cache_capacity,
                    None,
                    &cache,
                )
                .await
                .map(|db| {
                    MetricsShard::Reader(Box::new(
                        db.with_matcher_cache_capacity(matcher_capacity)
                            .with_forward_index_cache_capacity(forward_index_capacity),
                    ))
                })
            }
        });
        let shards = ShardSet::open(ShardRole::Reader, options, opener, local_shards).await;
        Self::new(shards, block_cache, &caches, &limits).await
    }

    /// The open storage shards, for ownership lifecycle management.
    pub fn shards(&self) -> &Arc<ShardSet<MetricsShard>> {
        &self.shards
    }

    /// Reads every shard outside this reader's scope through `client`.
    /// Returns false when a client was already connected.
    pub fn connect_remote_shards(&self, client: Arc<dyn MetricsShardClient>) -> bool {
        self.remote.set(client).is_ok()
    }

    /// The shards this reader reads remotely. Never includes an open shard:
    /// a local shard is always read in process.
    pub(crate) fn remote_shards(&self) -> Vec<RemoteShard> {
        let Some(client) = self.remote.get() else {
            return Vec::new();
        };
        self.shards
            .remote_shards()
            .into_iter()
            .map(|shard| RemoteShard {
                shard,
                client: Arc::clone(client),
            })
            .collect()
    }

    /// Warms recent cache blocks for every open shard and namespace, skipping
    /// SSTs `tracker` has already warmed.
    pub async fn warm_recent(
        &self,
        namespaces: &[Namespace],
        warm_range: Duration,
        include_samples: bool,
        concurrency: usize,
        cancel: &CancellationToken,
        tracker: Option<&SstWarmTracker>,
    ) -> Result<()> {
        let end = common::time::now_secs();
        let start = end.saturating_sub(i64::try_from(warm_range.as_secs()).unwrap_or(i64::MAX));
        self.shards
            .warm(concurrency, cancel, |shard, concurrency| async move {
                let range = (start, end);
                match &*shard {
                    MetricsShard::Writer(db) => {
                        let storage = db.storage_read();
                        warm_storage(
                            storage,
                            namespaces,
                            range,
                            include_samples,
                            concurrency,
                            cancel,
                            tracker,
                        )
                        .await
                    }
                    MetricsShard::Reader(db) => {
                        let storage = db.storage_read();
                        warm_storage(
                            storage,
                            namespaces,
                            range,
                            include_samples,
                            concurrency,
                            cancel,
                            tracker,
                        )
                        .await
                    }
                }
            })
            .await
    }

    pub async fn write_shard(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        series: Vec<Series>,
        visibility: Visibility,
    ) -> Result<()> {
        match &*self.shards.require(shard).await? {
            MetricsShard::Writer(db) => {
                db.write_with_visibility(namespace, series, visibility)
                    .await
            }
            MetricsShard::Reader(_) => Err(Error::InvalidInput(format!(
                "shard {} is not owned by this writer",
                shard.get()
            ))),
        }
    }

    /// Writes to locally opened shards. Every routed shard must be open, and
    /// nothing is written unless all of them are.
    pub async fn write(
        &self,
        assignment: &ShardMap,
        namespace: &Namespace,
        series: Vec<Series>,
        visibility: Visibility,
    ) -> Result<()> {
        let grouped = crate::routing::split(assignment, namespace, series);
        for shard in grouped.keys() {
            self.shards.require(*shard).await?;
        }
        let writes = grouped.into_iter().map(|(shard, batch)| async move {
            self.write_shard(namespace, shard, batch, visibility).await
        });
        futures::future::try_join_all(writes).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.shards.flush_all().await
    }

    /// Closes every shard, then the block cache they share.
    pub async fn close(&self) -> Result<()> {
        let closed = self.shards.close_all().await;
        self.block_cache.close().await?;
        closed
    }

    pub async fn query(
        &self,
        namespace: &Namespace,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        self.query_with_trace(namespace, query, time, false)
            .await
            .map(|(value, _)| value)
    }

    /// [`Self::query`], plus the per-query trace as JSON when `trace` is set.
    pub async fn query_with_trace(
        &self,
        namespace: &Namespace,
        query: &str,
        time: Option<SystemTime>,
        trace: bool,
    ) -> std::result::Result<(QueryValue, Option<serde_json::Value>), QueryError> {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let options = crate::QueryOptions::default();
        let plan = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(options.lookback_delta),
        );
        let ranges = preload_ranges_for_query(query, at_ms, at_ms, options.lookback_delta)?;
        let admitted = self.admission.admit().await?;
        let (source, plan) = self
            .traced_query_source(namespace, &ranges, &options, plan, trace)
            .await?;
        let outcome =
            execute_query_source(query, source, plan, true, admitted.reservation.clone()).await?;
        Ok((outcome.value, trace_json(outcome.trace)))
    }

    pub async fn query_range(
        &self,
        namespace: &Namespace,
        query: &str,
        range: impl RangeBounds<SystemTime> + Clone + Send,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        self.query_range_with_trace(namespace, query, range, step, false)
            .await
            .map(|(samples, _)| samples)
    }

    /// [`Self::query_range`], plus the per-query trace as JSON when `trace`
    /// is set.
    pub async fn query_range_with_trace(
        &self,
        namespace: &Namespace,
        query: &str,
        range: impl RangeBounds<SystemTime> + Clone + Send,
        step: Duration,
        trace: bool,
    ) -> std::result::Result<(Vec<RangeSample>, Option<serde_json::Value>), QueryError> {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }
        let lookback = crate::QueryOptions::default().lookback_delta;
        let now_ms = common::time::now_ms();
        let plan = self
            .result_cache
            .as_ref()
            .filter(|_| start_ms <= end_ms && start_ms <= now_ms)
            .and_then(|cache| {
                let plan = CachePlan::new(namespace, query, start_ms, step_ms, lookback)?;
                Some((cache, plan))
            });
        let (mut series, trace_value) = match plan {
            Some((cache, plan)) => {
                self.query_range_cached(
                    cache, &plan, namespace, query, start_ms, end_ms, step_ms, now_ms, trace,
                )
                .await?
            }
            None => {
                self.query_range_uncached(namespace, query, start_ms, end_ms, step_ms, trace)
                    .await?
            }
        };
        // Prometheus sorts range results by labels; doing the same makes a
        // cached answer's order match a cold one's.
        series.sort_by(|left, right| left.labels.cmp(&right.labels));
        Ok((series, trace_value))
    }

    /// Range query served by reusing the longest still-valid prefix of
    /// cached steps and evaluating the rest. Steps after `now_ms` are
    /// evaluated but never stored, nor is anything when evaluation fails.
    #[allow(clippy::too_many_arguments)]
    async fn query_range_cached(
        &self,
        cache: &ResultCache,
        plan: &CachePlan,
        namespace: &Namespace,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        now_ms: i64,
        trace: bool,
    ) -> std::result::Result<(Vec<RangeSample>, Option<serde_json::Value>), QueryError> {
        let started = std::time::Instant::now();
        let step_count = usize::try_from((end_ms - start_ms) / step_ms + 1).unwrap_or(usize::MAX);
        let last_cacheable = start_ms + (now_ms.min(end_ms) - start_ms) / step_ms * step_ms;
        // Generations are read before evaluation: a write racing the query
        // can only make the stored generations older than what was read.
        let buckets = plan.buckets(start_ms, last_cacheable);
        let current = match self.bucket_generations(namespace, &buckets).await {
            Ok(per_shard) => Generations::new(&buckets, per_shard),
            Err(_) => {
                return self
                    .query_range_uncached(namespace, query, start_ms, end_ms, step_ms, trace)
                    .await;
            }
        };
        let lookup = cache.lookup(plan, start_ms, last_cacheable, &current).await;
        let reused = lookup.reused_steps;
        let computed = step_count - reused;
        let tail_start = start_ms + reused as i64 * step_ms;
        let head = ResultCache::reused(&lookup, start_ms, tail_start - step_ms);

        let (series, trace_value) = if computed == 0 {
            let trace_value = trace.then(|| {
                serde_json::json!({
                    "totalMs": started.elapsed().as_secs_f64() * 1000.0,
                    "phases": [],
                    "operators": [],
                })
            });
            (head, trace_value)
        } else {
            let (tail, trace_value) = self
                .query_range_uncached(namespace, query, tail_start, end_ms, step_ms, trace)
                .await?;
            if has_duplicate_labels(&tail) || has_duplicate_labels(&head) {
                cache.record(0, step_count);
                if reused == 0 {
                    return Ok((tail, trace_value));
                }
                return self
                    .query_range_uncached(namespace, query, start_ms, end_ms, step_ms, trace)
                    .await;
            }
            (merge(head, tail), trace_value)
        };

        cache.record(reused, computed);
        if computed > 0 {
            cache
                .insert(plan, start_ms, last_cacheable, &series, current)
                .await;
        }
        let trace_value = trace_value.map(|mut value| {
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "resultCache".to_string(),
                    serde_json::json!({ "reusedSteps": reused, "computedSteps": computed }),
                );
            }
            value
        });
        Ok((series, trace_value))
    }

    async fn query_range_uncached(
        &self,
        namespace: &Namespace,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        trace: bool,
    ) -> std::result::Result<(Vec<RangeSample>, Option<serde_json::Value>), QueryError> {
        let options = crate::QueryOptions::default();
        let plan = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(options.lookback_delta),
        );
        let ranges = preload_ranges_for_query(query, start_ms, end_ms, options.lookback_delta)?;
        let admitted = self.admission.admit().await?;
        let (source, plan) = self
            .traced_query_source(namespace, &ranges, &options, plan, trace)
            .await?;
        let outcome =
            execute_query_source(query, source, plan, false, admitted.reservation.clone()).await?;
        Ok((
            query_value_to_range_samples(outcome.value)?,
            trace_json(outcome.trace),
        ))
    }

    /// `(reused, computed)` range-query steps since open, when the result
    /// cache is enabled.
    #[cfg(test)]
    pub(crate) fn result_cache_steps(&self) -> Option<(u64, u64)> {
        self.result_cache.as_ref().map(ResultCache::step_counts)
    }

    /// The query source, with reader setup recorded into a fresh trace
    /// collector that `plan` then carries when `trace` is set.
    async fn traced_query_source(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
        options: &crate::QueryOptions,
        plan: crate::promql::plan::LoweringContext,
        trace: bool,
    ) -> std::result::Result<
        (
            Arc<MultiShardSeriesSource>,
            crate::promql::plan::LoweringContext,
        ),
        QueryError,
    > {
        use crate::promql::trace::{Phase, TraceCollector, with_trace};
        if !trace {
            let source = self.query_source(namespace, ranges, options).await?;
            return Ok((Arc::new(source), plan));
        }
        let collector = TraceCollector::new();
        let started = std::time::Instant::now();
        let source = with_trace(
            collector.clone(),
            self.query_source(namespace, ranges, options),
        )
        .await?;
        collector.record_phase(Phase::ReaderSetup, started.elapsed().as_nanos() as u64);
        Ok((Arc::new(source), plan.with_trace(collector)))
    }

    pub async fn series(
        &self,
        namespace: &Namespace,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let handles = self.shards.databases().await;
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let range = start..=end;
        let remote = self.remote_shards();
        let remote_range = remote_range(&remote, range.clone())?;
        let width = (handles.len() + remote.len()).max(1);
        let mut pending: Vec<BoxFuture<'_, std::result::Result<Vec<Labels>, QueryError>>> =
            Vec::with_capacity(width);
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(
                async move { reader.series(namespace, matchers, range, permits).await }.boxed(),
            );
        }
        for shard in &remote {
            pending.push(shard.series(namespace, matchers, remote_range).boxed());
        }
        let results = stream::iter(pending)
            .buffered(width)
            .try_collect::<Vec<_>>()
            .await?;
        let mut unique = HashSet::new();
        for labels in results {
            unique.extend(labels);
        }
        let mut labels: Vec<_> = unique.into_iter().collect();
        labels.sort_unstable();
        Ok(labels)
    }

    pub async fn labels(
        &self,
        namespace: &Namespace,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let handles = self.shards.databases().await;
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let range = start..=end;
        let remote = self.remote_shards();
        let remote_range = remote_range(&remote, range.clone())?;
        let width = (handles.len() + remote.len()).max(1);
        let mut pending: Vec<BoxFuture<'_, std::result::Result<Vec<String>, QueryError>>> =
            Vec::with_capacity(width);
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(
                async move { reader.labels(namespace, matchers, range, permits).await }.boxed(),
            );
        }
        for shard in &remote {
            pending.push(shard.labels(namespace, matchers, remote_range).boxed());
        }
        let results = stream::iter(pending)
            .buffered(width)
            .try_collect::<Vec<_>>()
            .await?;
        let mut unique = HashSet::new();
        for labels in results {
            unique.extend(labels);
        }
        let mut labels: Vec<_> = unique.into_iter().collect();
        labels.sort();
        Ok(labels)
    }

    pub async fn label_values(
        &self,
        namespace: &Namespace,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let handles = self.shards.databases().await;
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let range = start..=end;
        let remote = self.remote_shards();
        let remote_range = remote_range(&remote, range.clone())?;
        let width = (handles.len() + remote.len()).max(1);
        let mut pending: Vec<BoxFuture<'_, std::result::Result<Vec<String>, QueryError>>> =
            Vec::with_capacity(width);
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(
                async move {
                    reader
                        .label_values(namespace, label_name, matchers, range, permits)
                        .await
                }
                .boxed(),
            );
        }
        for shard in &remote {
            pending.push(
                shard
                    .label_values(namespace, label_name, matchers, remote_range)
                    .boxed(),
            );
        }
        let results = stream::iter(pending)
            .buffered(width)
            .try_collect::<Vec<_>>()
            .await?;
        let mut unique = HashSet::new();
        for values in results {
            unique.extend(values);
        }
        let mut values: Vec<_> = unique.into_iter().collect();
        values.sort();
        Ok(values)
    }

    pub async fn metadata(
        &self,
        namespace: &Namespace,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let handles = self.shards.databases().await;
        let remote = self.remote_shards();
        let width = (handles.len() + remote.len()).max(1);
        let mut pending: Vec<BoxFuture<'_, std::result::Result<Vec<MetricMetadata>, QueryError>>> =
            Vec::with_capacity(width);
        for reader in handles {
            let permits = self.shards.io_permits();
            pending.push(
                async move {
                    let _permit = acquire_io_permit(&permits).await?;
                    reader.metadata(namespace, metric).await
                }
                .boxed(),
            );
        }
        for shard in &remote {
            pending.push(shard.metadata(namespace, metric).boxed());
        }
        let results = stream::iter(pending)
            .buffered(width)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(crate::discovery::dedup_metadata(
            results.into_iter().flatten().collect(),
        ))
    }

    /// The write generations of `buckets` in `namespace` on every open shard,
    /// sorted by shard; each shard's list follows `buckets`, with `None` for
    /// a bucket the shard has never flushed.
    pub(crate) async fn bucket_generations(
        &self,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> Result<Vec<(ShardId, Vec<Option<u64>>)>> {
        let remote = self.remote_shards();
        let local = self.shards.entries().await.into_iter().map(|(id, shard)| {
            self.shards
                .with_io(async move {
                    let generations = match &*shard {
                        MetricsShard::Writer(db) => db.bucket_generations(namespace, buckets).await,
                        MetricsShard::Reader(db) => db.bucket_generations(namespace, buckets).await,
                    }?;
                    Ok::<_, Error>((id, generations))
                })
                .boxed()
        });
        let remote = remote.iter().map(|shard| {
            async move {
                let generations = shard.bucket_generations(namespace, buckets).await?;
                Ok::<_, Error>((shard.shard, generations))
            }
            .boxed()
        });
        let mut generations = futures::future::try_join_all(local.chain(remote)).await?;
        generations.sort_unstable_by_key(|(id, _)| *id);
        Ok(generations)
    }

    async fn query_source(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
        options: &crate::QueryOptions,
    ) -> Result<MultiShardSeriesSource> {
        let entries = self.shards.entries().await;
        let readers = futures::future::try_join_all(entries.into_iter().map(|(id, shard)| {
            self.shards.with_io(async move {
                shard_query_reader(&shard, namespace, ranges)
                    .await
                    .map(|reader| (id, reader))
            })
        }))
        .await?;
        let limits = QueryLimits::new(options);
        let mut sources: Vec<(ShardId, ShardSource)> = readers
            .into_iter()
            .map(|(id, reader)| {
                let source = local_source(reader, self.shards.io_permits(), limits.clone());
                (id, ShardSource::Local(source))
            })
            .collect();
        sources.extend(self.remote_shards().into_iter().map(|shard| {
            (
                shard.shard,
                ShardSource::Remote(shard.source(namespace, ranges)),
            )
        }));
        sources.sort_unstable_by_key(|(id, _)| *id);
        Ok(MultiShardSeriesSource::new(
            sources.into_iter().map(|(_, source)| source).collect(),
        ))
    }

    /// A slot to serve one shard read to another reader group.
    pub(crate) async fn admit_shard_read(
        &self,
    ) -> std::result::Result<OwnedSemaphorePermit, QueryError> {
        self.admission.admit_shard_read().await
    }

    /// The local shard `shard`, which must be open here.
    async fn local_shard(&self, shard: ShardId) -> Result<Arc<MetricsShard>> {
        Ok(self.shards.require(shard).await?)
    }

    /// A query source over the local shard `shard` alone.
    pub(crate) async fn local_source(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> Result<QueryReaderSource<ShardSourceReader>> {
        let database = self.local_shard(shard).await?;
        let reader = self
            .shards
            .with_io(shard_query_reader(&database, namespace, ranges))
            .await?;
        Ok(local_source(
            reader,
            self.shards.io_permits(),
            QueryLimits::new(&crate::QueryOptions::default()),
        ))
    }

    pub(crate) async fn local_series(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let database = self.local_shard(shard).await?;
        database
            .series(namespace, matchers, range, self.shards.io_permits())
            .await
    }

    pub(crate) async fn local_labels(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let database = self.local_shard(shard).await?;
        database
            .labels(namespace, matchers, range, self.shards.io_permits())
            .await
    }

    pub(crate) async fn local_label_values(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let database = self.local_shard(shard).await?;
        database
            .label_values(
                namespace,
                label_name,
                matchers,
                range,
                self.shards.io_permits(),
            )
            .await
    }

    pub(crate) async fn local_metadata(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let database = self.local_shard(shard).await?;
        let _permit = acquire_io_permit(&self.shards.io_permits()).await?;
        database.metadata(namespace, metric).await
    }

    pub(crate) async fn local_bucket_generations(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        buckets: &[TimeBucket],
    ) -> Result<Vec<Option<u64>>> {
        let database = self.local_shard(shard).await?;
        self.shards
            .with_io(async move {
                match &*database {
                    MetricsShard::Writer(db) => db.bucket_generations(namespace, buckets).await,
                    MetricsShard::Reader(db) => db.bucket_generations(namespace, buckets).await,
                }
            })
            .await
    }
}

#[async_trait]
impl ReaderShardLifecycle for ShardedMetrics {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.shards.reconcile(assignment).await.map_err(Into::into)
    }
}

/// The time range remote shards read, in Unix seconds; unused (and not
/// validated) when every shard is local.
fn remote_range(
    remote: &[RemoteShard],
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<(i64, i64), QueryError> {
    if remote.is_empty() {
        return Ok((0, 0));
    }
    crate::util::range_bounds_to_secs(range)
}

async fn shard_query_reader(
    shard: &MetricsShard,
    namespace: &Namespace,
    ranges: &[(i64, i64)],
) -> Result<ShardQueryReader> {
    match shard {
        MetricsShard::Writer(db) => db
            .read_engine(namespace)
            .await
            .make_query_reader_for_ranges(ranges)
            .await
            .map(ShardQueryReader::Writer),
        MetricsShard::Reader(db) => db
            .make_query_reader_for_ranges(namespace, ranges)
            .await
            .map(ShardQueryReader::Reader),
    }
}

fn local_source(
    reader: ShardQueryReader,
    permits: Arc<Semaphore>,
    limits: QueryLimits,
) -> QueryReaderSource<ShardSourceReader> {
    QueryReaderSource::new(Arc::new(LimitedQueryReader::new(
        IoLimitedQueryReader::new(reader, permits),
        limits,
    )))
}

fn trace_json(trace: Option<crate::promql::trace::QueryTrace>) -> Option<serde_json::Value> {
    trace.and_then(|trace| serde_json::to_value(trace).ok())
}

pub(crate) enum ShardQueryReader {
    Writer(TsdbQueryReader),
    Reader(ReaderQueryReader),
}

#[async_trait]
impl QueryReader for ShardQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        match self {
            Self::Writer(reader) => reader.list_buckets().await,
            Self::Reader(reader) => reader.list_buckets().await,
        }
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        match self {
            Self::Writer(reader) => reader.forward_index(bucket, series_ids).await,
            Self::Reader(reader) => reader.forward_index(bucket, series_ids).await,
        }
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        match self {
            Self::Writer(reader) => reader.inverted_index(bucket, terms).await,
            Self::Reader(reader) => reader.inverted_index(bucket, terms).await,
        }
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        match self {
            Self::Writer(reader) => reader.all_inverted_index(bucket).await,
            Self::Reader(reader) => reader.all_inverted_index(bucket).await,
        }
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        match self {
            Self::Writer(reader) => reader.label_values(bucket, label_name).await,
            Self::Reader(reader) => reader.label_values(bucket, label_name).await,
        }
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::model::SeriesData> {
        match self {
            Self::Writer(reader) => {
                reader
                    .samples(bucket, series_id, metric_name, start_ms, end_ms)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .samples(bucket, series_id, metric_name, start_ms, end_ms)
                    .await
            }
        }
    }

    async fn samples_many(
        &self,
        bucket: &TimeBucket,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<crate::model::SeriesData>> {
        match self {
            Self::Writer(reader) => {
                reader
                    .samples_many(bucket, metric_name, series_ids, start_ms, end_ms)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .samples_many(bucket, metric_name, series_ids, start_ms, end_ms)
                    .await
            }
        }
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<SeriesSpec>> {
        match self {
            Self::Writer(reader) => reader.forward_index_one(bucket, series_id).await,
            Self::Reader(reader) => reader.forward_index_one(bucket, series_id).await,
        }
    }

    async fn forward_index_many(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Vec<Option<SeriesSpec>>> {
        match self {
            Self::Writer(reader) => reader.forward_index_many(bucket, series_ids).await,
            Self::Reader(reader) => reader.forward_index_many(bucket, series_ids).await,
        }
    }

    async fn label_postings(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<(String, RoaringBitmap)>> {
        match self {
            Self::Writer(reader) => reader.label_postings(bucket, label_name).await,
            Self::Reader(reader) => reader.label_postings(bucket, label_name).await,
        }
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<RoaringBitmap>> {
        match self {
            Self::Writer(reader) => reader.inverted_index_term(bucket, term).await,
            Self::Reader(reader) => reader.inverted_index_term(bucket, term).await,
        }
    }

    async fn cached_selector(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<RoaringBitmap>> {
        match self {
            Self::Writer(reader) => reader.cached_selector(bucket, key).await,
            Self::Reader(reader) => reader.cached_selector(bucket, key).await,
        }
    }

    async fn cache_selector(&self, bucket: &TimeBucket, key: &Arc<str>, postings: &RoaringBitmap) {
        match self {
            Self::Writer(reader) => reader.cache_selector(bucket, key, postings).await,
            Self::Reader(reader) => reader.cache_selector(bucket, key, postings).await,
        }
    }

    async fn cached_series_set(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<[Labels]>> {
        match self {
            Self::Writer(reader) => reader.cached_series_set(bucket, key).await,
            Self::Reader(reader) => reader.cached_series_set(bucket, key).await,
        }
    }

    async fn cache_series_set(&self, bucket: &TimeBucket, key: &Arc<str>, series: Arc<[Labels]>) {
        match self {
            Self::Writer(reader) => reader.cache_series_set(bucket, key, series).await,
            Self::Reader(reader) => reader.cache_series_set(bucket, key, series).await,
        }
    }

    async fn cached_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<CachedSeriesResolution>> {
        match self {
            Self::Writer(reader) => reader.cached_selector_resolution(bucket, key).await,
            Self::Reader(reader) => reader.cached_selector_resolution(bucket, key).await,
        }
    }

    async fn cache_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
        resolution: Arc<CachedSeriesResolution>,
    ) {
        match self {
            Self::Writer(reader) => {
                reader
                    .cache_selector_resolution(bucket, key, resolution)
                    .await
            }
            Self::Reader(reader) => {
                reader
                    .cache_selector_resolution(bucket, key, resolution)
                    .await
            }
        }
    }
}

pub(crate) struct IoLimitedQueryReader<R> {
    inner: R,
    permits: Arc<Semaphore>,
}

impl<R> IoLimitedQueryReader<R> {
    fn new(inner: R, permits: Arc<Semaphore>) -> Self {
        Self { inner, permits }
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        acquire_io_permit(&self.permits).await
    }
}

#[async_trait]
impl<R: QueryReader> QueryReader for IoLimitedQueryReader<R> {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        let _permit = self.acquire().await?;
        self.inner.list_buckets().await
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await?;
        self.inner.forward_index(bucket, series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await?;
        self.inner.inverted_index(bucket, terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await?;
        self.inner.all_inverted_index(bucket).await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let _permit = self.acquire().await?;
        self.inner.label_values(bucket, label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::model::SeriesData> {
        let _permit = self.acquire().await?;
        self.inner
            .samples(bucket, series_id, metric_name, start_ms, end_ms)
            .await
    }

    async fn samples_many(
        &self,
        bucket: &TimeBucket,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<crate::model::SeriesData>> {
        let _permit = self.acquire().await?;
        self.inner
            .samples_many(bucket, metric_name, series_ids, start_ms, end_ms)
            .await
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<SeriesSpec>> {
        let _permit = self.acquire().await?;
        self.inner.forward_index_one(bucket, series_id).await
    }

    async fn forward_index_many(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Vec<Option<SeriesSpec>>> {
        let _permit = self.acquire().await?;
        self.inner.forward_index_many(bucket, series_ids).await
    }

    async fn label_postings(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<(String, RoaringBitmap)>> {
        let _permit = self.acquire().await?;
        self.inner.label_postings(bucket, label_name).await
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<RoaringBitmap>> {
        let _permit = self.acquire().await?;
        self.inner.inverted_index_term(bucket, term).await
    }

    async fn cached_selector(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<RoaringBitmap>> {
        self.inner.cached_selector(bucket, key).await
    }

    async fn cache_selector(&self, bucket: &TimeBucket, key: &Arc<str>, postings: &RoaringBitmap) {
        self.inner.cache_selector(bucket, key, postings).await
    }

    async fn cached_series_set(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<[Labels]>> {
        self.inner.cached_series_set(bucket, key).await
    }

    async fn cache_series_set(&self, bucket: &TimeBucket, key: &Arc<str>, series: Arc<[Labels]>) {
        self.inner.cache_series_set(bucket, key, series).await
    }

    async fn cached_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
    ) -> Option<Arc<CachedSeriesResolution>> {
        self.inner.cached_selector_resolution(bucket, key).await
    }

    async fn cache_selector_resolution(
        &self,
        bucket: &TimeBucket,
        key: &Arc<str>,
        resolution: Arc<CachedSeriesResolution>,
    ) {
        self.inner
            .cache_selector_resolution(bucket, key, resolution)
            .await
    }
}

/// Query limits are taken before the pod-wide I/O permit so a query waiting
/// on its own cap never holds shared I/O capacity.
pub(crate) type ShardSourceReader = LimitedQueryReader<IoLimitedQueryReader<ShardQueryReader>>;

/// One shard of a query: read in process when open here, otherwise through
/// the reader group serving it.
enum ShardSource {
    Local(QueryReaderSource<ShardSourceReader>),
    Remote(RemoteShardSource),
}

impl ShardSource {
    async fn resolve_all(
        &self,
        selector: &promql_parser::parser::VectorSelector,
        time_range: TimeRange,
    ) -> Vec<std::result::Result<ResolvedSeriesChunk, SourceQueryError>> {
        match self {
            Self::Local(source) => source.resolve(selector, time_range).collect().await,
            Self::Remote(source) => source.resolve(selector, time_range).collect().await,
        }
    }

    async fn samples_all(
        &self,
        request: SamplesRequest,
    ) -> Vec<std::result::Result<SampleBatch, SourceQueryError>> {
        match self {
            Self::Local(source) => source.samples(request).collect().await,
            Self::Remote(source) => source.samples(request).collect().await,
        }
    }
}

struct MultiShardSeriesSource {
    sources: Arc<[Arc<ShardSource>]>,
}

impl MultiShardSeriesSource {
    /// Local shards share one set of query limits, so they cap the whole
    /// query's local reads.
    fn new(sources: Vec<ShardSource>) -> Self {
        Self {
            sources: sources.into_iter().map(Arc::new).collect(),
        }
    }

    fn encode_bucket(source_index: usize, bucket_id: u64) -> u64 {
        debug_assert_eq!(bucket_id & !SOURCE_BUCKET_MASK, 0);
        ((source_index as u64) << SOURCE_BUCKET_BITS) | bucket_id
    }

    fn decode_bucket(bucket_id: u64) -> (usize, u64) {
        (
            (bucket_id >> SOURCE_BUCKET_BITS) as usize,
            bucket_id & SOURCE_BUCKET_MASK,
        )
    }
}

impl SeriesSource for MultiShardSeriesSource {
    fn resolve(
        &self,
        selector: &promql_parser::parser::VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = std::result::Result<ResolvedSeriesChunk, SourceQueryError>> + Send {
        let selector = selector.clone();
        let sources: Vec<_> = self.sources.iter().cloned().enumerate().collect();
        let width = sources.len().max(1);
        stream::iter(sources.into_iter().map(move |(source_index, source)| {
            let selector = selector.clone();
            async move {
                source
                    .resolve_all(&selector, time_range)
                    .await
                    .into_iter()
                    .map(move |chunk| {
                        chunk.map(|chunk| {
                            let bucket_id = Self::encode_bucket(source_index, chunk.bucket_id);
                            ResolvedSeriesChunk {
                                bucket_id,
                                labels: chunk.labels,
                                series: chunk
                                    .series
                                    .iter()
                                    .map(|series| ResolvedSeriesRef {
                                        bucket_id,
                                        series_id: series.series_id,
                                        metric_name: Arc::clone(&series.metric_name),
                                    })
                                    .collect(),
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            }
        }))
        .buffered(width)
        .flat_map(stream::iter)
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = std::result::Result<SampleBatch, SourceQueryError>> + Send {
        let mut runs = Vec::new();
        let mut start = 0;
        while start < request.series.len() {
            let (source_index, _) = Self::decode_bucket(request.series[start].bucket_id);
            let mut end = start + 1;
            while end < request.series.len()
                && Self::decode_bucket(request.series[end].bucket_id).0 == source_index
            {
                end += 1;
            }
            runs.push((source_index, start..end));
            start = end;
        }

        let sources = Arc::clone(&self.sources);
        let width = sources.len().max(1);
        stream::iter(runs.into_iter().map(move |(source_index, range)| {
            let sources = Arc::clone(&sources);
            let series: Arc<[ResolvedSeriesRef]> = request.series[range.clone()]
                .iter()
                .map(|series| {
                    let (_, bucket_id) = Self::decode_bucket(series.bucket_id);
                    ResolvedSeriesRef {
                        bucket_id,
                        series_id: series.series_id,
                        metric_name: Arc::clone(&series.metric_name),
                    }
                })
                .collect();
            async move {
                let Some(source) = sources.get(source_index) else {
                    return vec![Err(SourceQueryError::Internal(format!(
                        "invalid shard source index {source_index}"
                    )))];
                };
                source
                    .samples_all(SamplesRequest::new(series, request.time_range))
                    .await
                    .into_iter()
                    .map(|batch| {
                        batch.map(|batch| SampleBatch {
                            series_range: (batch.series_range.start + range.start)
                                ..(batch.series_range.end + range.start),
                            samples: batch.samples,
                        })
                    })
                    .collect::<Vec<_>>()
            }
        }))
        .buffered(width)
        .flat_map(stream::iter)
    }
}

impl MetricsShard {
    async fn series<R: RangeBounds<SystemTime>>(
        &self,
        namespace: &Namespace,
        matchers: &[&str],
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => ShardQueryReader::Writer(
                db.read_engine(namespace)
                    .await
                    .make_query_reader(start, end)
                    .await?,
            ),
            Self::Reader(db) => ShardQueryReader::Reader(
                db.make_query_reader_for_ranges(namespace, &[(start, end)])
                    .await?,
            ),
        };
        crate::tsdb::discover_series(&IoLimitedQueryReader::new(reader, permits), matchers).await
    }

    async fn labels<R: RangeBounds<SystemTime>>(
        &self,
        namespace: &Namespace,
        matchers: Option<&[&str]>,
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        if matchers.is_none_or(<[&str]>::is_empty) {
            let _permit = acquire_io_permit(&permits).await?;
            return match self {
                Self::Writer(db) => db.labels(namespace, None, range).await,
                Self::Reader(db) => db.labels(namespace, None, range).await,
            };
        }
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => ShardQueryReader::Writer(
                db.read_engine(namespace)
                    .await
                    .make_query_reader(start, end)
                    .await?,
            ),
            Self::Reader(db) => ShardQueryReader::Reader(
                db.make_query_reader_for_ranges(namespace, &[(start, end)])
                    .await?,
            ),
        };
        crate::tsdb::discover_labels(&IoLimitedQueryReader::new(reader, permits), matchers).await
    }

    async fn label_values<R: RangeBounds<SystemTime>>(
        &self,
        namespace: &Namespace,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        if matchers.is_none_or(<[&str]>::is_empty) {
            let _permit = acquire_io_permit(&permits).await?;
            return match self {
                Self::Writer(db) => db.label_values(namespace, label_name, None, range).await,
                Self::Reader(db) => db.label_values(namespace, label_name, None, range).await,
            };
        }
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => ShardQueryReader::Writer(
                db.read_engine(namespace)
                    .await
                    .make_query_reader(start, end)
                    .await?,
            ),
            Self::Reader(db) => ShardQueryReader::Reader(
                db.make_query_reader_for_ranges(namespace, &[(start, end)])
                    .await?,
            ),
        };
        crate::tsdb::discover_label_values(
            &IoLimitedQueryReader::new(reader, permits),
            label_name,
            matchers,
        )
        .await
    }

    async fn metadata(
        &self,
        namespace: &Namespace,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        match self {
            Self::Writer(db) => db.metadata(namespace, metric).await,
            Self::Reader(db) => db.metadata(namespace, metric).await,
        }
    }
}

#[cfg(test)]
mod tests;
