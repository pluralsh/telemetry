use std::collections::{BTreeMap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use roaring::RoaringBitmap;
use sharding::{DEFAULT_IO_CONCURRENCY_MULTIPLIER, DEFAULT_VIRTUAL_SHARDS, ShardId};
use slatedb::config::DbReaderOptions;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{SeriesId, TimeBucket};
use crate::promql::memory::QueryError as SourceQueryError;
use crate::promql::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use crate::promql::source_adapter::QueryReaderSource;
use crate::query::QueryReader;
use crate::reader::ReaderQueryReader;
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

fn shard_io_semaphore(shards: usize, multiplier: u32) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        shards.max(1).saturating_mul(multiplier as usize),
    ))
}

async fn acquire_io_permit(permits: &Arc<Semaphore>) -> OwnedSemaphorePermit {
    permits
        .clone()
        .acquire_owned()
        .await
        .expect("shard I/O semaphore must remain open")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardingOptions {
    virtual_shards: u32,
    io_concurrency_multiplier: u32,
}

impl Default for ShardingOptions {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            io_concurrency_multiplier: DEFAULT_IO_CONCURRENCY_MULTIPLIER,
        }
    }
}

impl ShardingOptions {
    pub fn new(virtual_shards: u32, io_concurrency_multiplier: u32) -> Result<Self> {
        if virtual_shards == 0 {
            return Err(Error::InvalidInput(
                "virtual shard count must be greater than zero".to_string(),
            ));
        }
        if io_concurrency_multiplier == 0 {
            return Err(Error::InvalidInput(
                "I/O concurrency multiplier must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            virtual_shards,
            io_concurrency_multiplier,
        })
    }

    pub const fn virtual_shards(self) -> u32 {
        self.virtual_shards
    }

    pub const fn io_concurrency_multiplier(self) -> u32 {
        self.io_concurrency_multiplier
    }

    pub fn shard_path(self, base: &str, shard: ShardId) -> Result<String> {
        self.validate_shard(shard)?;
        Ok(format!(
            "{}/shard-{:04}",
            base.trim_end_matches('/'),
            shard.get()
        ))
    }

    pub fn route(self, namespace: &Namespace, labels: &[crate::Label]) -> ShardId {
        let mut labels: Vec<&crate::Label> = labels.iter().collect();
        if !labels.is_sorted() {
            labels.sort_unstable();
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(namespace.as_bytes());
        for label in labels {
            hasher.update(&[0]);
            hasher.update(label.name.as_bytes());
            hasher.update(&[0]);
            hasher.update(label.value.as_bytes());
        }
        let value = u64::from_be_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap());
        ShardId::new((value % u64::from(self.virtual_shards)) as u32)
    }

    fn validate_shard(self, shard: ShardId) -> Result<()> {
        if shard.get() >= self.virtual_shards {
            return Err(Error::InvalidInput(format!(
                "shard {} is outside configured virtual shard count {}",
                shard.get(),
                self.virtual_shards
            )));
        }
        Ok(())
    }
}

/// Namespace-scoped facade over locally owned immutable virtual shards.
pub struct ShardedMeter {
    namespace: Namespace,
    options: ShardingOptions,
    writers: BTreeMap<ShardId, TimeSeriesDb>,
    readers: BTreeMap<ShardId, TimeSeriesDbReader>,
    io_permits: Arc<Semaphore>,
}

pub type ShardedTimeseries = ShardedMeter;

impl ShardedMeter {
    pub async fn open_writers(
        namespace: Namespace,
        config: Config,
        options: ShardingOptions,
        owned_shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let mut writers = BTreeMap::new();
        for shard in owned_shards {
            options.validate_shard(shard)?;
            let mut shard_config = config.clone();
            shard_config.storage.path = options.shard_path(&config.storage.path, shard)?;
            writers.insert(
                shard,
                TimeSeriesDb::open(namespace.clone(), shard_config).await?,
            );
        }
        Ok(Self {
            namespace,
            options,
            io_permits: shard_io_semaphore(writers.len(), options.io_concurrency_multiplier()),
            writers,
            readers: BTreeMap::new(),
        })
    }

    pub async fn open_readers(
        namespace: Namespace,
        config: Config,
        options: ShardingOptions,
        local_shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        let mut readers = BTreeMap::new();
        for shard in local_shards {
            options.validate_shard(shard)?;
            let mut storage = config.storage.clone();
            storage.path = options.shard_path(&config.storage.path, shard)?;
            readers.insert(
                shard,
                TimeSeriesDbReader::open(
                    namespace.clone(),
                    storage,
                    reader_options.clone(),
                    cache_capacity,
                )
                .await?,
            );
        }
        Ok(Self {
            namespace,
            options,
            writers: BTreeMap::new(),
            io_permits: shard_io_semaphore(readers.len(), options.io_concurrency_multiplier()),
            readers,
        })
    }

    pub fn route(&self, labels: &[crate::Label]) -> ShardId {
        self.options.route(&self.namespace, labels)
    }

    pub fn group(&self, series: Vec<Series>) -> Result<BTreeMap<ShardId, Vec<Series>>> {
        let mut grouped: BTreeMap<ShardId, Vec<Series>> = BTreeMap::new();
        for item in series {
            let shard = self.route(&item.labels);
            if !self.writers.contains_key(&shard) {
                return Err(Error::InvalidInput(format!(
                    "shard {} is not owned by this writer",
                    shard.get()
                )));
            }
            grouped.entry(shard).or_default().push(item);
        }
        Ok(grouped)
    }

    pub async fn write(&self, series: Vec<Series>, visibility: Visibility) -> Result<()> {
        let writes = self
            .group(series)?
            .into_iter()
            .map(|(shard, batch)| self.writers[&shard].write_with_visibility(batch, visibility));
        futures::future::try_join_all(writes).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        futures::future::try_join_all(self.writers.values().map(|writer| writer.flush())).await?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        for (_, writer) in self.writers {
            writer.close().await?;
        }
        for (_, reader) in self.readers {
            reader.close().await?;
        }
        Ok(())
    }

    pub async fn query(
        &self,
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
        let source = Arc::new(self.query_source(&ranges).await?);
        execute_query_source(query, source, plan, true)
            .await
            .map(|outcome| outcome.value)
    }

    pub async fn query_range(
        &self,
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
        let source = Arc::new(self.query_source(&ranges).await?);
        execute_query_source(query, source, plan, false)
            .await
            .and_then(|outcome| query_value_to_range_samples(outcome.value))
    }

    pub async fn series(
        &self,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let handles = self.read_handles();
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
            let range = range.clone();
            pending.push(async move { reader.series(matchers, range, permits).await });
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
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let handles = self.read_handles();
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
            let range = range.clone();
            pending.push(async move { reader.labels(matchers, range, permits).await });
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
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime> + Clone,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let handles = self.read_handles();
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
            let range = range.clone();
            pending.push(async move {
                reader
                    .label_values(label_name, matchers, range, permits)
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
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let handles = self.read_handles();
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
            pending.push(async move {
                let _permit = acquire_io_permit(&permits).await;
                reader.metadata(metric).await
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

    fn read_handles(&self) -> Vec<ReaderHandle<'_>> {
        if self.readers.is_empty() {
            self.writers.values().map(ReaderHandle::Writer).collect()
        } else {
            self.readers.values().map(ReaderHandle::Reader).collect()
        }
    }

    async fn query_source(&self, ranges: &[(i64, i64)]) -> Result<MultiShardSeriesSource> {
        let readers = if self.readers.is_empty() {
            let width = self.writers.len().max(1);
            let mut pending = Vec::with_capacity(self.writers.len());
            for db in self.writers.values() {
                let permits = Arc::clone(&self.io_permits);
                pending.push(async move {
                    let _permit = acquire_io_permit(&permits).await;
                    db.read_engine()
                        .make_query_reader_for_ranges(ranges)
                        .await
                        .map(ShardQueryReader::Writer)
                });
            }
            stream::iter(pending)
                .buffered(width)
                .try_collect::<Vec<_>>()
                .await?
        } else {
            let width = self.readers.len().max(1);
            let mut pending = Vec::with_capacity(self.readers.len());
            for db in self.readers.values() {
                let permits = Arc::clone(&self.io_permits);
                pending.push(async move {
                    let _permit = acquire_io_permit(&permits).await;
                    db.make_query_reader_for_ranges(ranges)
                        .await
                        .map(ShardQueryReader::Reader)
                });
            }
            stream::iter(pending)
                .buffered(width)
                .try_collect::<Vec<_>>()
                .await?
        };
        Ok(MultiShardSeriesSource::new(
            readers,
            Arc::clone(&self.io_permits),
        ))
    }
}

enum ReaderHandle<'a> {
    Writer(&'a TimeSeriesDb),
    Reader(&'a TimeSeriesDbReader),
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

    async fn acquire(&self) -> OwnedSemaphorePermit {
        acquire_io_permit(&self.permits).await
    }
}

#[async_trait]
impl<R: QueryReader> QueryReader for IoLimitedQueryReader<R> {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        let _permit = self.acquire().await;
        self.inner.list_buckets().await
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await;
        self.inner.forward_index(bucket, series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await;
        self.inner.inverted_index(bucket, terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let _permit = self.acquire().await;
        self.inner.all_inverted_index(bucket).await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let _permit = self.acquire().await;
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
        let _permit = self.acquire().await;
        self.inner
            .samples(bucket, series_id, metric_name, start_ms, end_ms)
            .await
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<SeriesSpec>> {
        let _permit = self.acquire().await;
        self.inner.forward_index_one(bucket, series_id).await
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<RoaringBitmap>> {
        let _permit = self.acquire().await;
        self.inner.inverted_index_term(bucket, term).await
    }
}

struct MultiShardSeriesSource {
    sources: Arc<[Arc<QueryReaderSource<IoLimitedQueryReader<ShardQueryReader>>>]>,
}

impl MultiShardSeriesSource {
    fn new(readers: Vec<ShardQueryReader>, permits: Arc<Semaphore>) -> Self {
        Self {
            sources: readers
                .into_iter()
                .map(|reader| {
                    Arc::new(QueryReaderSource::new(Arc::new(IoLimitedQueryReader::new(
                        reader,
                        Arc::clone(&permits),
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

impl ReaderHandle<'_> {
    async fn series<R: RangeBounds<SystemTime>>(
        &self,
        matchers: &[&str],
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => {
                ShardQueryReader::Writer(db.read_engine().make_query_reader(start, end).await?)
            }
            Self::Reader(db) => ShardQueryReader::Reader(db.make_query_reader(start, end).await?),
        };
        crate::tsdb::discover_series(&IoLimitedQueryReader::new(reader, permits), matchers).await
    }

    async fn labels<R: RangeBounds<SystemTime>>(
        &self,
        matchers: Option<&[&str]>,
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => {
                ShardQueryReader::Writer(db.read_engine().make_query_reader(start, end).await?)
            }
            Self::Reader(db) => ShardQueryReader::Reader(db.make_query_reader(start, end).await?),
        };
        crate::tsdb::discover_labels(&IoLimitedQueryReader::new(reader, permits), matchers).await
    }

    async fn label_values<R: RangeBounds<SystemTime>>(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: R,
        permits: Arc<Semaphore>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_secs(range)?;
        let reader = match self {
            Self::Writer(db) => {
                ShardQueryReader::Writer(db.read_engine().make_query_reader(start, end).await?)
            }
            Self::Reader(db) => ShardQueryReader::Reader(db.make_query_reader(start, end).await?),
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
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        match self {
            Self::Writer(db) => db.metadata(metric).await,
            Self::Reader(db) => db.metadata(metric).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Label;
    use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};

    const TEST_TIME_MS: i64 = 1_700_000_060_000;

    async fn test_databases() -> (ShardedMeter, TimeSeriesDb) {
        let namespace = Namespace::new("global-query-regression").unwrap();
        let config = |path: &str| Config {
            storage: SlateDbStorageConfig {
                path: path.to_string(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            },
            ..Default::default()
        };
        let sharded = ShardedMeter::open_writers(
            namespace.clone(),
            config("sharded"),
            ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_MULTIPLIER).unwrap(),
            [ShardId::new(0), ShardId::new(1)],
        )
        .await
        .unwrap();
        let unsharded = TimeSeriesDb::open(namespace, config("unsharded"))
            .await
            .unwrap();
        (sharded, unsharded)
    }

    fn series_on_shard(
        db: &ShardedMeter,
        metric: &str,
        shard: u32,
        extra_labels: &[(&str, &str)],
        samples: Vec<Sample>,
    ) -> Series {
        for candidate in 0..10_000 {
            let mut labels = extra_labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect::<Vec<_>>();
            labels.push(Label::new("instance", format!("instance-{candidate}")));
            let series = Series::new(metric, labels, samples.clone());
            if db.route(&series.labels).get() == shard {
                return series;
            }
        }
        panic!("failed to find labels routed to shard {shard}");
    }

    fn binary_join_series(db: &ShardedMeter) -> (Series, Series) {
        for candidate in 0..10_000 {
            let instance = format!("join-{candidate}");
            let left = Series::new(
                "left_metric",
                vec![Label::new("instance", &instance)],
                vec![Sample::new(TEST_TIME_MS, 2.0)],
            );
            let right = Series::new(
                "right_metric",
                vec![Label::new("instance", &instance)],
                vec![Sample::new(TEST_TIME_MS, 3.0)],
            );
            if db.route(&left.labels) != db.route(&right.labels) {
                return (left, right);
            }
        }
        panic!("failed to find binary operands routed to different shards");
    }

    fn normalized(value: QueryValue) -> Vec<(String, Vec<(i64, u64)>)> {
        let mut result = value
            .into_matrix()
            .into_iter()
            .map(|sample| {
                (
                    format!("{:?}", sample.labels),
                    sample
                        .samples
                        .into_iter()
                        .map(|(timestamp, value)| (timestamp, value.to_bits()))
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        result.sort();
        result
    }

    async fn assert_matches_unsharded(
        sharded: &ShardedMeter,
        unsharded: &TimeSeriesDb,
        query: &str,
    ) {
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis((TEST_TIME_MS + 1_000) as u64);
        let actual = sharded.query(query, Some(time)).await.unwrap();
        let expected = unsharded.query(query, Some(time)).await.unwrap();
        assert_eq!(normalized(actual), normalized(expected), "query: {query}");
    }

    async fn write_both(sharded: &ShardedMeter, unsharded: &TimeSeriesDb, series: Vec<Series>) {
        sharded
            .write(series.clone(), Visibility::Written)
            .await
            .unwrap();
        unsharded
            .write_with_visibility(series, Visibility::Written)
            .await
            .unwrap();
    }

    #[test]
    fn routing_is_canonical_and_namespace_sensitive() {
        let options = ShardingOptions::default();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let labels = vec![Label::new("z", "1"), Label::new("a", "2")];
        let reversed = labels.iter().cloned().rev().collect::<Vec<_>>();
        assert_eq!(options.route(&a, &labels), options.route(&a, &reversed));
        assert_ne!(options.route(&a, &labels), options.route(&b, &labels));
    }

    #[test]
    fn shard_paths_are_stable() {
        assert_eq!(
            ShardingOptions::default()
                .shard_path("meter", ShardId::new(3))
                .unwrap(),
            "meter/shard-0003"
        );
        assert!(ShardingOptions::new(0, DEFAULT_IO_CONCURRENCY_MULTIPLIER).is_err());
        assert!(ShardingOptions::new(DEFAULT_VIRTUAL_SHARDS, 0).is_err());
    }

    #[test]
    fn shard_io_budget_uses_configured_multiplier() {
        assert_eq!(shard_io_semaphore(8, 4).available_permits(), 32);
        assert_eq!(shard_io_semaphore(8, 2).available_permits(), 16);
        assert_eq!(shard_io_semaphore(0, 4).available_permits(), 4);
    }

    #[tokio::test]
    async fn global_sum_matches_unsharded_database() {
        let (sharded, unsharded) = test_databases().await;
        let series = vec![
            series_on_shard(
                &sharded,
                "requests_total",
                0,
                &[],
                vec![Sample::new(TEST_TIME_MS, 2.0)],
            ),
            series_on_shard(
                &sharded,
                "requests_total",
                1,
                &[],
                vec![Sample::new(TEST_TIME_MS, 3.0)],
            ),
        ];
        write_both(&sharded, &unsharded, series).await;
        assert_matches_unsharded(&sharded, &unsharded, "sum(requests_total)").await;
    }

    #[tokio::test]
    async fn grouped_aggregation_matches_unsharded_database() {
        let (sharded, unsharded) = test_databases().await;
        let series = vec![
            series_on_shard(
                &sharded,
                "requests_total",
                0,
                &[("region", "east")],
                vec![Sample::new(TEST_TIME_MS, 2.0)],
            ),
            series_on_shard(
                &sharded,
                "requests_total",
                1,
                &[("region", "east")],
                vec![Sample::new(TEST_TIME_MS, 3.0)],
            ),
            series_on_shard(
                &sharded,
                "requests_total",
                1,
                &[("region", "west")],
                vec![Sample::new(TEST_TIME_MS, 7.0)],
            ),
        ];
        write_both(&sharded, &unsharded, series).await;
        assert_matches_unsharded(&sharded, &unsharded, "sum by (region) (requests_total)").await;
    }

    #[tokio::test]
    async fn rate_then_sum_matches_unsharded_database() {
        let (sharded, unsharded) = test_databases().await;
        let samples = |start, end| {
            vec![
                Sample::new(TEST_TIME_MS - 50_000, start),
                Sample::new(TEST_TIME_MS - 10_000, end),
            ]
        };
        let series = vec![
            series_on_shard(&sharded, "requests_total", 0, &[], samples(1.0, 5.0)),
            series_on_shard(&sharded, "requests_total", 1, &[], samples(2.0, 10.0)),
        ];
        write_both(&sharded, &unsharded, series).await;
        assert_matches_unsharded(&sharded, &unsharded, "sum(rate(requests_total[1m]))").await;
    }

    #[tokio::test]
    async fn binary_join_across_shards_matches_unsharded_database() {
        let (sharded, unsharded) = test_databases().await;
        let (left, right) = binary_join_series(&sharded);
        write_both(&sharded, &unsharded, vec![left, right]).await;
        assert_matches_unsharded(
            &sharded,
            &unsharded,
            "left_metric * on(instance) right_metric",
        )
        .await;
    }
}
