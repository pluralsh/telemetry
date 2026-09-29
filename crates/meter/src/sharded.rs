use std::collections::{BTreeMap, HashSet, btree_map::Entry};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use roaring::RoaringBitmap;
use sharding::{
    DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_VIRTUAL_SHARDS, HashRangeMap, ReaderShardLifecycle,
    ShardId, ShardMap, hash_routing_key,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

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

fn shard_io_semaphore(limit: u32) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(limit as usize))
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
    io_concurrency_limit: u32,
}

impl Default for ShardingOptions {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            io_concurrency_limit: DEFAULT_IO_CONCURRENCY_LIMIT,
        }
    }
}

impl ShardingOptions {
    pub fn new(virtual_shards: u32, io_concurrency_limit: u32) -> Result<Self> {
        if virtual_shards == 0 {
            return Err(Error::InvalidInput(
                "virtual shard count must be greater than zero".to_string(),
            ));
        }
        if io_concurrency_limit == 0 {
            return Err(Error::InvalidInput(
                "I/O concurrency limit must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            virtual_shards,
            io_concurrency_limit,
        })
    }

    pub const fn virtual_shards(self) -> u32 {
        self.virtual_shards
    }

    pub const fn io_concurrency_limit(self) -> u32 {
        self.io_concurrency_limit
    }

    pub fn shard_path(self, base: &str, shard: ShardId) -> Result<String> {
        Ok(format!(
            "{}/shard-{:04}",
            base.trim_end_matches('/'),
            shard.get()
        ))
    }

    pub fn route(
        self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        labels: &[crate::Label],
    ) -> ShardId {
        routing.route(hash_routing_key(&crate::routing::canonical_routing_key(
            namespace, labels,
        )))
    }

    fn slot_range(self, shard: ShardId) -> Result<std::ops::Range<u16>> {
        HashRangeMap::bootstrap(self.virtual_shards)
            .map_err(|error| Error::InvalidInput(error.to_string()))?
            .assignments
            .into_iter()
            .find(|assignment| assignment.shard == shard)
            .ok_or_else(|| Error::InvalidInput(format!("unknown shard {}", shard.get())))?
            .range
            .slots()
            .map_err(|error| Error::InvalidInput(error.to_string()))
    }
}

/// Namespace-aware facade over locally owned immutable virtual shards.
pub struct ShardedMeter {
    config: Config,
    options: ShardingOptions,
    writers: RwLock<BTreeMap<ShardId, Arc<TimeSeriesDb>>>,
    readers: RwLock<BTreeMap<ShardId, Arc<TimeSeriesDbReader>>>,
    reader_slots: RwLock<BTreeMap<ShardId, std::ops::Range<u16>>>,
    reader_options: Option<DbReaderOptions>,
    reader_cache_capacity: u64,
    io_permits: Arc<Semaphore>,
}

pub type ShardedTimeseries = ShardedMeter;

impl ShardedMeter {
    pub async fn open_writers(
        config: Config,
        options: ShardingOptions,
        owned_shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let routing = HashRangeMap::bootstrap(options.virtual_shards)
            .map_err(|error| Error::InvalidInput(error.to_string()))?;
        Self::open_writers_with_routing(config, options, owned_shards, &routing).await
    }

    pub async fn open_writers_with_routing(
        config: Config,
        options: ShardingOptions,
        owned_shards: impl IntoIterator<Item = ShardId>,
        routing: &HashRangeMap,
    ) -> Result<Self> {
        let mut writers = BTreeMap::new();
        for shard in owned_shards {
            let mut shard_config = config.clone();
            shard_config.storage.path = options.shard_path(&config.storage.path, shard)?;
            let slots = routing
                .assignments
                .iter()
                .find(|assignment| assignment.shard == shard)
                .ok_or_else(|| Error::InvalidInput(format!("unknown shard {}", shard.get())))?
                .range
                .slots()
                .map_err(|error| Error::InvalidInput(error.to_string()))?;
            writers.insert(
                shard,
                Arc::new(TimeSeriesDb::open_with_slots(shard_config, slots).await?),
            );
        }
        Ok(Self {
            config,
            options,
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            writers: RwLock::new(writers),
            readers: RwLock::new(BTreeMap::new()),
            reader_slots: RwLock::new(BTreeMap::new()),
            reader_options: None,
            reader_cache_capacity: 0,
        })
    }

    pub async fn open_readers(
        config: Config,
        options: ShardingOptions,
        local_shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        let routing = HashRangeMap::bootstrap(options.virtual_shards)
            .map_err(|error| Error::InvalidInput(error.to_string()))?;
        Self::open_readers_with_routing(
            config,
            options,
            local_shards,
            &routing,
            reader_options,
            cache_capacity,
        )
        .await
    }

    pub async fn open_readers_with_routing(
        config: Config,
        options: ShardingOptions,
        local_shards: impl IntoIterator<Item = ShardId>,
        routing: &HashRangeMap,
        reader_options: DbReaderOptions,
        cache_capacity: u64,
    ) -> Result<Self> {
        let mut readers = BTreeMap::new();
        let mut reader_slots = BTreeMap::new();
        for shard in local_shards {
            let mut storage = config.storage.clone();
            storage.path = options.shard_path(&config.storage.path, shard)?;
            let slots = routing
                .assignments
                .iter()
                .find(|assignment| assignment.shard == shard)
                .ok_or_else(|| Error::InvalidInput(format!("unknown shard {}", shard.get())))?
                .range
                .slots()
                .map_err(|error| Error::InvalidInput(error.to_string()))?;
            readers.insert(
                shard,
                Arc::new(
                    TimeSeriesDbReader::open_with_slots(
                        storage,
                        reader_options.clone(),
                        cache_capacity,
                        slots.clone(),
                    )
                    .await?,
                ),
            );
            reader_slots.insert(shard, slots);
        }
        Ok(Self {
            config,
            options,
            writers: RwLock::new(BTreeMap::new()),
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            readers: RwLock::new(readers),
            reader_slots: RwLock::new(reader_slots),
            reader_options: Some(reader_options),
            reader_cache_capacity: cache_capacity,
        })
    }

    pub fn route(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        labels: &[crate::Label],
    ) -> ShardId {
        self.options.route(routing, namespace, labels)
    }

    pub async fn contains_writer_shard(&self, shard: ShardId) -> bool {
        self.writers.read().await.contains_key(&shard)
    }

    pub async fn writer_shards(&self) -> Vec<ShardId> {
        self.writers.read().await.keys().copied().collect()
    }

    pub async fn reader_shard_count(&self) -> usize {
        self.readers.read().await.len()
    }

    pub async fn reader_shards(&self) -> Vec<ShardId> {
        self.readers.read().await.keys().copied().collect()
    }

    /// Opens new or resized readers before atomically publishing the new set.
    /// Readers with in-flight query references remain installed and cause a
    /// retry instead of being detached while active.
    pub async fn reconcile_reader_shards(&self, routing: &HashRangeMap) -> Result<()> {
        let reader_options = self.reader_options.as_ref().ok_or_else(|| {
            Error::InvalidInput("reader reconciliation requires a reader facade".into())
        })?;
        let desired = routing
            .assignments
            .iter()
            .map(|assignment| {
                assignment
                    .range
                    .slots()
                    .map(|slots| (assignment.shard, slots))
                    .map_err(|error| Error::InvalidInput(error.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let current_slots = self.reader_slots.read().await.clone();
        let mut opened = BTreeMap::new();
        for (&shard, slots) in &desired {
            if current_slots.get(&shard) == Some(slots) {
                continue;
            }
            let mut storage = self.config.storage.clone();
            storage.path = self.options.shard_path(&self.config.storage.path, shard)?;
            opened.insert(
                shard,
                Arc::new(
                    TimeSeriesDbReader::open_with_slots(
                        storage,
                        reader_options.clone(),
                        self.reader_cache_capacity,
                        slots.clone(),
                    )
                    .await?,
                ),
            );
        }

        let mut readers = self.readers.write().await;
        let mut slots = self.reader_slots.write().await;
        let retiring = readers
            .iter()
            .filter(|(shard, _)| desired.get(shard) != slots.get(shard))
            .map(|(shard, reader)| (*shard, Arc::strong_count(reader)))
            .collect::<Vec<_>>();
        if let Some((shard, references)) = retiring
            .iter()
            .find(|(_, references)| *references != 1)
            .copied()
        {
            drop(slots);
            drop(readers);
            for (_, reader) in opened {
                Arc::try_unwrap(reader)
                    .unwrap_or_else(|_| unreachable!("new reader has no external references"))
                    .close()
                    .await?;
            }
            return Err(Error::InvalidInput(format!(
                "shard reader {shard} still has {} in-flight references",
                references - 1
            )));
        }

        let retiring = retiring
            .into_iter()
            .filter_map(|(shard, _)| readers.remove(&shard))
            .collect::<Vec<_>>();
        for (shard, reader) in opened {
            readers.insert(shard, reader);
        }
        *slots = desired;
        drop(slots);
        drop(readers);
        for reader in retiring {
            Arc::try_unwrap(reader)
                .unwrap_or_else(|_| {
                    unreachable!("retired reader was checked for external references")
                })
                .close()
                .await?;
        }
        Ok(())
    }

    pub async fn open_writer_shard(&self, shard: ShardId) -> Result<()> {
        self.open_writer_shard_with_slots(shard, self.options.slot_range(shard)?)
            .await
    }

    pub async fn open_writer_shard_with_slots(
        &self,
        shard: ShardId,
        owned_slots: std::ops::Range<u16>,
    ) -> Result<()> {
        if self.contains_writer_shard(shard).await {
            return Ok(());
        }
        let mut config = self.config.clone();
        config.storage.path = self.options.shard_path(&config.storage.path, shard)?;
        let database = Arc::new(TimeSeriesDb::open_with_slots(config, owned_slots).await?);
        let mut writers = self.writers.write().await;
        if let Entry::Vacant(entry) = writers.entry(shard) {
            entry.insert(database);
            return Ok(());
        }
        drop(writers);
        if let Ok(database) = Arc::try_unwrap(database) {
            database.close().await?;
        }
        Ok(())
    }

    pub async fn flush_shard(&self, shard: ShardId) -> Result<()> {
        if let Some(database) = self.writers.read().await.get(&shard).cloned() {
            database.flush().await?;
        }
        Ok(())
    }

    pub async fn close_writer_shard(&self, shard: ShardId) -> Result<()> {
        if let Some(database) = self.writers.write().await.remove(&shard) {
            let database = Arc::try_unwrap(database).map_err(|_| {
                Error::InvalidInput("shard database still has in-flight references".into())
            })?;
            database.close().await?;
        }
        Ok(())
    }

    pub async fn write_shard(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        series: Vec<Series>,
        visibility: Visibility,
    ) -> Result<()> {
        let database = self
            .writers
            .read()
            .await
            .get(&shard)
            .cloned()
            .ok_or_else(|| {
                Error::InvalidInput(format!("shard {} is not owned by this writer", shard.get()))
            })?;
        database
            .write_with_visibility(namespace, series, visibility)
            .await
    }

    pub async fn group(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        series: Vec<Series>,
    ) -> Result<BTreeMap<ShardId, Vec<Series>>> {
        let mut grouped: BTreeMap<ShardId, Vec<Series>> = BTreeMap::new();
        for item in series {
            let shard = self.route(routing, namespace, &item.labels);
            if !self.writers.read().await.contains_key(&shard) {
                return Err(Error::InvalidInput(format!(
                    "shard {} is not owned by this writer",
                    shard.get()
                )));
            }
            grouped.entry(shard).or_default().push(item);
        }
        Ok(grouped)
    }

    pub async fn write(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        series: Vec<Series>,
        visibility: Visibility,
    ) -> Result<()> {
        let writes = self
            .group(routing, namespace, series)
            .await?
            .into_iter()
            .map(|(shard, batch)| async move {
                self.write_shard(namespace, shard, batch, visibility).await
            });
        futures::future::try_join_all(writes).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let writers = self
            .writers
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        futures::future::try_join_all(writers.iter().map(|writer| writer.flush())).await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let writers = std::mem::take(&mut *self.writers.write().await);
        for (_, writer) in writers {
            let writer = Arc::try_unwrap(writer).map_err(|_| {
                Error::InvalidInput("shard database still has in-flight references".into())
            })?;
            writer.close().await?;
        }
        let readers = std::mem::take(&mut *self.readers.write().await);
        for (_, reader) in readers {
            let reader = Arc::try_unwrap(reader).map_err(|_| {
                Error::InvalidInput("shard reader still has in-flight references".into())
            })?;
            reader.close().await?;
        }
        Ok(())
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
        let source = Arc::new(self.query_source(namespace, &ranges).await?);
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
        let source = Arc::new(self.query_source(namespace, &ranges).await?);
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
        let handles = self.read_handles().await;
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
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
        let handles = self.read_handles().await;
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
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
        let handles = self.read_handles().await;
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
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
        let handles = self.read_handles().await;
        let width = handles.len().max(1);
        let mut pending = Vec::with_capacity(handles.len());
        for reader in handles {
            let permits = Arc::clone(&self.io_permits);
            pending.push(async move {
                let _permit = acquire_io_permit(&permits).await;
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

    async fn read_handles(&self) -> Vec<ReaderHandle> {
        let readers = self.readers.read().await;
        if readers.is_empty() {
            self.writers
                .read()
                .await
                .values()
                .cloned()
                .map(ReaderHandle::Writer)
                .collect()
        } else {
            readers
                .values()
                .cloned()
                .map(ReaderHandle::Reader)
                .collect()
        }
    }

    async fn query_source(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> Result<MultiShardSeriesSource> {
        let reader_handles = self
            .readers
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let readers = if reader_handles.is_empty() {
            let writer_handles = self
                .writers
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let width = writer_handles.len().max(1);
            let mut pending = Vec::with_capacity(writer_handles.len());
            for db in writer_handles {
                let permits = Arc::clone(&self.io_permits);
                pending.push(async move {
                    let _permit = acquire_io_permit(&permits).await;
                    db.read_engine(namespace)
                        .await
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
            let width = reader_handles.len().max(1);
            let mut pending = Vec::with_capacity(reader_handles.len());
            for db in reader_handles {
                let permits = Arc::clone(&self.io_permits);
                pending.push(async move {
                    let _permit = acquire_io_permit(&permits).await;
                    db.make_query_reader_for_ranges(namespace, ranges)
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

#[async_trait]
impl ReaderShardLifecycle for ShardedMeter {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.reconcile_reader_shards(&assignment.routing)
            .await
            .map_err(Into::into)
    }
}

enum ReaderHandle {
    Writer(Arc<TimeSeriesDb>),
    Reader(Arc<TimeSeriesDbReader>),
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

impl ReaderHandle {
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
            let _permit = acquire_io_permit(&permits).await;
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
            let _permit = acquire_io_permit(&permits).await;
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
mod tests {
    use super::*;
    use crate::Label;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    const TEST_TIME_MS: i64 = 1_700_000_060_000;

    async fn test_databases() -> (ShardedMeter, TimeSeriesDb) {
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
            config("sharded"),
            ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
            [ShardId::new(0), ShardId::new(1)],
        )
        .await
        .unwrap();
        let unsharded = TimeSeriesDb::open(config("unsharded")).await.unwrap();
        (sharded, unsharded)
    }

    #[tokio::test]
    async fn reader_reconciliation_adds_resizes_and_safely_removes_shards() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            storage: SlateDbStorageConfig {
                path: "reader-reconcile".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: directory.path().to_string_lossy().into_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            },
            ..Default::default()
        };
        let options = ShardingOptions::new(1, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let one = HashRangeMap::bootstrap(1).unwrap();
        let two = one.grow_to(2).unwrap();

        let source = ShardedMeter::open_writers_with_routing(
            config.clone(),
            options,
            [ShardId::new(0)],
            &one,
        )
        .await
        .unwrap();
        source.close().await.unwrap();
        let target = ShardedMeter::open_writers_with_routing(
            config.clone(),
            options,
            [ShardId::new(1)],
            &two,
        )
        .await
        .unwrap();
        target.close().await.unwrap();

        let readers = ShardedMeter::open_readers_with_routing(
            config,
            options,
            [ShardId::new(0)],
            &one,
            DbReaderOptions::default(),
            17,
        )
        .await
        .unwrap();
        readers.reconcile_reader_shards(&two).await.unwrap();
        assert_eq!(
            readers.reader_shards().await,
            vec![ShardId::new(0), ShardId::new(1)]
        );
        assert_eq!(readers.reader_cache_capacity, 17);

        let active = readers.readers.read().await[&ShardId::new(1)].clone();
        let error = readers.reconcile_reader_shards(&one).await.unwrap_err();
        assert!(error.to_string().contains("in-flight references"));
        assert_eq!(
            readers.reader_shards().await,
            vec![ShardId::new(0), ShardId::new(1)]
        );
        drop(active);
        readers.reconcile_reader_shards(&one).await.unwrap();
        assert_eq!(readers.reader_shards().await, vec![ShardId::new(0)]);
        readers.close().await.unwrap();
    }

    fn series_on_shard(
        db: &ShardedMeter,
        metric: &str,
        shard: u32,
        extra_labels: &[(&str, &str)],
        samples: Vec<Sample>,
    ) -> Series {
        let routing = HashRangeMap::bootstrap(db.options.virtual_shards()).unwrap();
        for candidate in 0..10_000 {
            let mut labels = extra_labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect::<Vec<_>>();
            labels.push(Label::new("instance", format!("instance-{candidate}")));
            let series = Series::new(metric, labels, samples.clone());
            if db
                .route(
                    &routing,
                    &Namespace::new("global-query-regression").unwrap(),
                    &series.labels,
                )
                .get()
                == shard
            {
                return series;
            }
        }
        panic!("failed to find labels routed to shard {shard}");
    }

    fn binary_join_series(db: &ShardedMeter) -> (Series, Series) {
        let routing = HashRangeMap::bootstrap(db.options.virtual_shards()).unwrap();
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
            let namespace = Namespace::new("global-query-regression").unwrap();
            if db.route(&routing, &namespace, &left.labels)
                != db.route(&routing, &namespace, &right.labels)
            {
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
        let namespace = Namespace::new("global-query-regression").unwrap();
        let actual = sharded.query(&namespace, query, Some(time)).await.unwrap();
        let expected = unsharded
            .query(&namespace, query, Some(time))
            .await
            .unwrap();
        assert_eq!(normalized(actual), normalized(expected), "query: {query}");
    }

    async fn write_both(sharded: &ShardedMeter, unsharded: &TimeSeriesDb, series: Vec<Series>) {
        let routing = HashRangeMap::bootstrap(sharded.options.virtual_shards()).unwrap();
        sharded
            .write(
                &routing,
                &Namespace::new("global-query-regression").unwrap(),
                series.clone(),
                Visibility::Written,
            )
            .await
            .unwrap();
        unsharded
            .write_with_visibility(
                &Namespace::new("global-query-regression").unwrap(),
                series,
                Visibility::Written,
            )
            .await
            .unwrap();
    }

    #[test]
    fn routing_is_canonical_and_namespace_sensitive() {
        let options = ShardingOptions::default();
        let routing = HashRangeMap::bootstrap(options.virtual_shards()).unwrap();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let labels = vec![Label::new("z", "1"), Label::new("a", "2")];
        let reversed = labels.iter().cloned().rev().collect::<Vec<_>>();
        assert_eq!(
            options.route(&routing, &a, &labels),
            options.route(&routing, &a, &reversed)
        );
        assert_ne!(
            options.route(&routing, &a, &labels),
            options.route(&routing, &b, &labels)
        );
    }

    #[test]
    fn shard_paths_are_stable() {
        assert_eq!(
            ShardingOptions::default()
                .shard_path("meter", ShardId::new(3))
                .unwrap(),
            "meter/shard-0003"
        );
        assert!(ShardingOptions::new(0, DEFAULT_IO_CONCURRENCY_LIMIT).is_err());
        assert!(ShardingOptions::new(DEFAULT_VIRTUAL_SHARDS, 0).is_err());
    }

    #[test]
    fn shard_io_budget_uses_fixed_process_limit() {
        assert_eq!(shard_io_semaphore(128).available_permits(), 128);
        assert_eq!(shard_io_semaphore(32).available_permits(), 32);
    }

    #[tokio::test]
    async fn custom_routing_range_rejects_writes_for_unowned_slots() {
        let config = Config {
            storage: SlateDbStorageConfig {
                path: "custom-routing".to_string(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            },
            ..Default::default()
        };
        let routing = HashRangeMap::bootstrap(2).unwrap().grow_to(3).unwrap();
        let options = ShardingOptions::new(3, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let db =
            ShardedMeter::open_writers_with_routing(config, options, [ShardId::new(2)], &routing)
                .await
                .unwrap();
        let namespace = Namespace::new("global-query-regression").unwrap();
        let series = series_on_shard(
            &db,
            "requests_total",
            0,
            &[],
            vec![Sample::new(TEST_TIME_MS, 1.0)],
        );

        let error = db
            .write_shard(
                &namespace,
                ShardId::new(2),
                vec![series],
                Visibility::Applied,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("outside opened shard range"));
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
        let start = SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis((TEST_TIME_MS - 1_000) as u64);
        let end = SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis((TEST_TIME_MS + 1_000) as u64);
        assert_eq!(
            sharded
                .label_values(
                    &Namespace::new("global-query-regression").unwrap(),
                    "region",
                    None,
                    start..=end,
                )
                .await
                .unwrap(),
            vec!["east", "west"]
        );
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
