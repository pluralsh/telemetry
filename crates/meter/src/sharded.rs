use std::collections::HashSet;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use roaring::RoaringBitmap;
use sharding::{
    ReaderShardLifecycle, ShardDatabase, ShardId, ShardMap, ShardRole, ShardSet, ShardSetError,
    ShardingOptions, shard_opener,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{SeriesId, TimeBucket};
use crate::promql::memory::QueryError as SourceQueryError;
use crate::promql::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use crate::promql::source_adapter::QueryReaderSource;
use crate::query::{LimitedQueryReader, QueryLimits, QueryReader};
use crate::reader::ReaderQueryReader;
use crate::storage::{StorageRead, WarmStorage};
use crate::tsdb::{
    TsdbQueryReader, TsdbReadEngine, duration_to_ms, execute_query_source,
    preload_ranges_for_query, query_value_to_range_samples, system_time_to_ms,
};
use crate::{
    Config, Error, Label, Labels, MetricMetadata, Namespace, QueryError, QueryValue, RangeSample,
    Result, Sample, Series, TimeSeriesDb, TimeSeriesDbReader, Visibility,
};

const SOURCE_BUCKET_BITS: u32 = 40;
const SOURCE_BUCKET_MASK: u64 = (1 << SOURCE_BUCKET_BITS) - 1;

async fn acquire_io_permit(permits: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    Arc::clone(permits)
        .acquire_owned()
        .await
        .map_err(|_| ShardSetError::Closed.into())
}

/// One storage shard, opened either as the single writer or as a reader.
pub enum MeterShard {
    Writer(TimeSeriesDb),
    Reader(Box<TimeSeriesDbReader>),
}

#[async_trait]
impl ShardDatabase for MeterShard {
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
) -> Result<()> {
    for namespace in namespaces {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let buckets = storage
            .get_buckets_in_range(namespace, Some(start), Some(end))
            .await?;
        storage
            .warm(namespace, buckets, include_samples, concurrency, cancel)
            .await?;
    }
    Ok(())
}

/// Namespace-aware facade over locally owned storage shards.
pub struct ShardedMeter {
    shards: Arc<ShardSet<MeterShard>>,
}

pub type ShardedTimeseries = ShardedMeter;

impl ShardedMeter {
    pub async fn open_writers(
        config: Config,
        options: ShardingOptions,
        owned_shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let opener = shard_opener(move |shard| {
            let mut config = config.clone();
            config.storage = shard_storage(&config, shard);
            async move { TimeSeriesDb::open(config).await.map(MeterShard::Writer) }
        });
        Ok(Self {
            shards: Arc::new(
                ShardSet::open(ShardRole::Writer, options, opener, owned_shards).await?,
            ),
        })
    }

    pub async fn open_readers(
        config: Config,
        options: ShardingOptions,
        local_shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        let opener = shard_opener(move |shard| {
            let open = TimeSeriesDbReader::open(
                shard_storage(&config, shard),
                reader_options.clone(),
                cache_capacity,
            );
            async move { open.await.map(|db| MeterShard::Reader(Box::new(db))) }
        });
        Ok(Self {
            shards: Arc::new(
                ShardSet::open(ShardRole::Reader, options, opener, local_shards).await?,
            ),
        })
    }

    /// The open storage shards, for ownership lifecycle management.
    pub fn shards(&self) -> &Arc<ShardSet<MeterShard>> {
        &self.shards
    }

    /// Warms recent cache blocks for every open shard and namespace.
    pub async fn warm_recent(
        &self,
        namespaces: &[Namespace],
        warm_range: Duration,
        include_samples: bool,
        concurrency: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let end = common::time::now_secs();
        let start = end.saturating_sub(i64::try_from(warm_range.as_secs()).unwrap_or(i64::MAX));
        self.shards
            .warm(concurrency, cancel, |shard, concurrency| async move {
                let range = (start, end);
                match &*shard {
                    MeterShard::Writer(db) => {
                        let storage = db.storage_read();
                        warm_storage(
                            storage,
                            namespaces,
                            range,
                            include_samples,
                            concurrency,
                            cancel,
                        )
                        .await
                    }
                    MeterShard::Reader(db) => {
                        let storage = db.storage_read();
                        warm_storage(
                            storage,
                            namespaces,
                            range,
                            include_samples,
                            concurrency,
                            cancel,
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
            MeterShard::Writer(db) => {
                db.write_with_visibility(namespace, series, visibility)
                    .await
            }
            MeterShard::Reader(_) => Err(Error::InvalidInput(format!(
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

    pub async fn close(&self) -> Result<()> {
        self.shards.close_all().await
    }

    pub async fn query(
        &self,
        namespace: &Namespace,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let options = crate::QueryOptions::default();
        let plan = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(options.lookback_delta),
        );
        let ranges = preload_ranges_for_query(query, at_ms, at_ms, options.lookback_delta)?;
        let source = Arc::new(self.query_source(namespace, &ranges, &options).await?);
        execute_query_source(query, source, plan, true)
            .await
            .map(|outcome| outcome.value)
    }

    pub async fn query_range(
        &self,
        namespace: &Namespace,
        query: &str,
        range: impl RangeBounds<SystemTime> + Clone + Send,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }
        let options = crate::QueryOptions::default();
        let plan = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(options.lookback_delta),
        );
        let ranges = preload_ranges_for_query(query, start_ms, end_ms, options.lookback_delta)?;
        let source = Arc::new(self.query_source(namespace, &ranges, &options).await?);
        execute_query_source(query, source, plan, false)
            .await
            .and_then(|outcome| query_value_to_range_samples(outcome.value))
    }

    pub async fn series(
        &self,
        namespace: &Namespace,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let handles = self.shards.databases().await;
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(async move { reader.series(namespace, matchers, range, permits).await });
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
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(async move { reader.labels(namespace, matchers, range, permits).await });
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
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = self.shards.io_permits();
            let range = range.clone();
            pending.push(async move {
                reader
                    .label_values(namespace, label_name, matchers, range, permits)
                    .await
            });
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
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = self.shards.io_permits();
            pending.push(async move {
                let _permit = acquire_io_permit(&permits).await?;
                reader.metadata(namespace, metric).await
            });
        }
        let results = stream::iter(pending)
            .buffered(width)
            .try_collect::<Vec<_>>()
            .await?;
        let mut entries = Vec::new();
        for shard_entries in results {
            for entry in shard_entries {
                if !entries.contains(&entry) {
                    entries.push(entry);
                }
            }
        }
        entries.sort_by(|left, right| left.metric_name.cmp(&right.metric_name));
        Ok(entries)
    }

    async fn query_source(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
        options: &crate::QueryOptions,
    ) -> Result<MultiShardSeriesSource> {
        let databases = self.shards.databases().await;
        let readers = futures::future::try_join_all(databases.into_iter().map(|shard| {
            self.shards.with_io(async move {
                match &*shard {
                    MeterShard::Writer(db) => db
                        .read_engine(namespace)
                        .await
                        .make_query_reader_for_ranges(ranges)
                        .await
                        .map(ShardQueryReader::Writer),
                    MeterShard::Reader(db) => db
                        .make_query_reader_for_ranges(namespace, ranges)
                        .await
                        .map(ShardQueryReader::Reader),
                }
            })
        }))
        .await?;
        Ok(MultiShardSeriesSource::new(
            readers,
            self.shards.io_permits(),
            QueryLimits::new(options),
        ))
    }
}

#[async_trait]
impl ReaderShardLifecycle for ShardedMeter {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.shards.reconcile(assignment).await.map_err(Into::into)
    }
}

enum ShardQueryReader {
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
    ) -> Result<Vec<Sample>> {
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
}

struct IoLimitedQueryReader<R> {
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
    ) -> Result<Vec<Sample>> {
        let _permit = self.acquire().await?;
        self.inner
            .samples(bucket, series_id, metric_name, start_ms, end_ms)
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

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<RoaringBitmap>> {
        let _permit = self.acquire().await?;
        self.inner.inverted_index_term(bucket, term).await
    }
}

/// Query limits are taken before the pod-wide I/O permit so a query waiting
/// on its own cap never holds shared I/O capacity.
type ShardSourceReader = LimitedQueryReader<IoLimitedQueryReader<ShardQueryReader>>;

struct MultiShardSeriesSource {
    sources: Arc<[Arc<QueryReaderSource<ShardSourceReader>>]>,
}

impl MultiShardSeriesSource {
    /// Query limits are shared by every shard, so they cap the whole query.
    fn new(readers: Vec<ShardQueryReader>, permits: Arc<Semaphore>, limits: QueryLimits) -> Self {
        Self {
            sources: readers
                .into_iter()
                .map(|reader| {
                    Arc::new(QueryReaderSource::new(Arc::new(LimitedQueryReader::new(
                        IoLimitedQueryReader::new(reader, Arc::clone(&permits)),
                        limits.clone(),
                    ))))
                })
                .collect(),
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
                    .resolve(&selector, time_range)
                    .map_ok(move |chunk| {
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
                    .collect::<Vec<_>>()
                    .await
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
                    .samples(SamplesRequest::new(series, request.time_range))
                    .map_ok(move |batch| SampleBatch {
                        series_range: (batch.series_range.start + range.start)
                            ..(batch.series_range.end + range.start),
                        samples: batch.samples,
                    })
                    .collect::<Vec<_>>()
                    .await
            }
        }))
        .buffered(width)
        .flat_map(stream::iter)
    }
}

impl MeterShard {
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
