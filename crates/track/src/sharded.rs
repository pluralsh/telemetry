use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use sharding::{
    DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_VIRTUAL_SHARDS, HashRangeMap, ReaderShardLifecycle,
    ShardId, ShardMap,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{RwLock, Semaphore};

use crate::{
    AttributeMatcher, Config, Durability, Error, Namespace, QueryOptions, Result, Trace,
    TraceBatch, TraceDb, TraceId, TraceQlResult, WriteReport,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
            return Err(Error::Invalid(
                "virtual shard count must be greater than zero".to_owned(),
            ));
        }
        if io_concurrency_limit == 0 {
            return Err(Error::Invalid(
                "I/O concurrency limit must be greater than zero".to_owned(),
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

    pub fn route(
        self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> ShardId {
        routing.route(sharding::hash_routing_key(
            &crate::routing::canonical_routing_key(namespace, trace_id),
        ))
    }

    pub fn shard_storage(self, config: &Config, shard: ShardId) -> Result<Config> {
        let mut config = config.clone();
        config.storage = config
            .storage
            .with_path_suffix(&format!("shard-{:04}", shard.get()));
        Ok(config)
    }
}

/// A facade over independently opened fixed virtual-shard trace databases.
pub struct ShardedTrack {
    config: Config,
    options: ShardingOptions,
    shards: RwLock<BTreeMap<ShardId, Arc<TraceDb>>>,
    shard_slots: RwLock<BTreeMap<ShardId, std::ops::Range<u16>>>,
    reader_options: Option<DbReaderOptions>,
    io_permits: Arc<Semaphore>,
}

impl ShardedTrack {
    pub async fn open(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let routing = HashRangeMap::bootstrap(options.virtual_shards())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Self::open_with_routing(config, options, shards, &routing).await
    }

    pub async fn open_with_routing(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
        routing: &HashRangeMap,
    ) -> Result<Self> {
        let mut databases = BTreeMap::new();
        let mut shard_slots = BTreeMap::new();
        for shard in shards {
            let slots = slot_range(routing, shard)?;
            databases.insert(
                shard,
                Arc::new(
                    TraceDb::open_with_slots(options.shard_storage(&config, shard)?, slots.clone())
                        .await?,
                ),
            );
            shard_slots.insert(shard, slots);
        }
        Ok(Self {
            config,
            options,
            shards: RwLock::new(databases),
            shard_slots: RwLock::new(shard_slots),
            reader_options: None,
            io_permits: Arc::new(Semaphore::new(options.io_concurrency_limit() as usize)),
        })
    }

    pub async fn open_readers_with_routing(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
        routing: &HashRangeMap,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        let mut databases = BTreeMap::new();
        let mut shard_slots = BTreeMap::new();
        for shard in shards {
            let slots = slot_range(routing, shard)?;
            databases.insert(
                shard,
                Arc::new(
                    TraceDb::open_reader_with_slots(
                        options.shard_storage(&config, shard)?,
                        slots.clone(),
                        reader_options.clone(),
                    )
                    .await?,
                ),
            );
            shard_slots.insert(shard, slots);
        }
        Ok(Self {
            config,
            options,
            shards: RwLock::new(databases),
            shard_slots: RwLock::new(shard_slots),
            reader_options: Some(reader_options),
            io_permits: Arc::new(Semaphore::new(options.io_concurrency_limit() as usize)),
        })
    }

    pub fn route(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> ShardId {
        self.options.route(routing, namespace, trace_id)
    }

    pub async fn contains_shard(&self, shard: ShardId) -> bool {
        self.shards.read().await.contains_key(&shard)
    }

    pub async fn shard(&self, shard: ShardId) -> Option<Arc<TraceDb>> {
        self.shards.read().await.get(&shard).cloned()
    }

    pub async fn open_shard(&self, shard: ShardId) -> Result<()> {
        let routing = HashRangeMap::bootstrap(self.options.virtual_shards())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        self.open_shard_with_slots(shard, slot_range(&routing, shard)?)
            .await
    }

    pub async fn open_shard_with_slots(
        &self,
        shard: ShardId,
        owned_slots: std::ops::Range<u16>,
    ) -> Result<()> {
        if self.contains_shard(shard).await {
            return Ok(());
        }
        let database = TraceDb::open_with_slots(
            self.options.shard_storage(&self.config, shard)?,
            owned_slots.clone(),
        )
        .await?;
        let mut shards = self.shards.write().await;
        match shards.entry(shard) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(database));
                self.shard_slots.write().await.insert(shard, owned_slots);
                return Ok(());
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
        drop(shards);
        database.close().await?;
        Ok(())
    }

    pub async fn flush_shard(&self, shard: ShardId) -> Result<()> {
        if let Some(database) = self.shard(shard).await {
            database.flush().await?;
        }
        Ok(())
    }

    pub async fn close_shard(&self, shard: ShardId) -> Result<()> {
        let mut shards = self.shards.write().await;
        let Some(database) = shards.remove(&shard) else {
            return Ok(());
        };
        let owned_slots = self.shard_slots.write().await.remove(&shard);
        let database = match Arc::try_unwrap(database) {
            Ok(database) => database,
            Err(database) => {
                shards.insert(shard, database);
                if let Some(owned_slots) = owned_slots {
                    self.shard_slots.write().await.insert(shard, owned_slots);
                }
                return Err(Error::Invalid(
                    "shard database still has in-flight references".into(),
                ));
            }
        };
        drop(shards);
        database.close().await
    }

    pub async fn open_shard_count(&self) -> usize {
        self.shards.read().await.len()
    }

    pub async fn open_shards(&self) -> Vec<ShardId> {
        self.shards.read().await.keys().copied().collect()
    }

    pub async fn reconcile_shards(&self, routing: &HashRangeMap) -> Result<()> {
        let desired = routing
            .assignments
            .iter()
            .map(|assignment| {
                assignment
                    .range
                    .slots()
                    .map(|slots| (assignment.shard, slots))
                    .map_err(|error| Error::Invalid(error.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let current_slots = self.shard_slots.read().await.clone();
        let mut opened = BTreeMap::new();
        for (&shard, slots) in &desired {
            if current_slots.get(&shard) == Some(slots) {
                continue;
            }
            let reader_options = self.reader_options.as_ref().ok_or_else(|| {
                Error::Invalid("reader reconciliation requires a reader facade".into())
            })?;
            opened.insert(
                shard,
                Arc::new(
                    TraceDb::open_reader_with_slots(
                        self.options.shard_storage(&self.config, shard)?,
                        slots.clone(),
                        reader_options.clone(),
                    )
                    .await?,
                ),
            );
        }

        let mut shards = self.shards.write().await;
        let mut shard_slots = self.shard_slots.write().await;
        let retiring = shards
            .iter()
            .filter(|(shard, _)| desired.get(shard) != shard_slots.get(shard))
            .map(|(shard, database)| (*shard, Arc::strong_count(database)))
            .collect::<Vec<_>>();
        if let Some((shard, references)) = retiring
            .iter()
            .find(|(_, references)| *references != 1)
            .copied()
        {
            drop(shard_slots);
            drop(shards);
            for (_, database) in opened {
                Arc::try_unwrap(database)
                    .unwrap_or_else(|_| unreachable!("new shard has no external references"))
                    .close()
                    .await?;
            }
            return Err(Error::Invalid(format!(
                "shard {shard} still has {} in-flight references",
                references - 1
            )));
        }
        let retiring = retiring
            .into_iter()
            .filter_map(|(shard, _)| shards.remove(&shard))
            .collect::<Vec<_>>();
        for (shard, database) in opened {
            shards.insert(shard, database);
        }
        *shard_slots = desired;
        drop(shard_slots);
        drop(shards);
        for database in retiring {
            Arc::try_unwrap(database)
                .unwrap_or_else(|_| {
                    unreachable!("retired shard was checked for external references")
                })
                .close()
                .await?;
        }
        Ok(())
    }

    pub async fn write(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut grouped = BTreeMap::<ShardId, Vec<Trace>>::new();
        for trace in batches.into_iter().flat_map(|batch| batch.traces) {
            grouped
                .entry(self.route(routing, namespace, trace.trace_id))
                .or_default()
                .push(trace);
        }
        let mut report = WriteReport::default();
        for (shard, traces) in grouped {
            let database = self.shard(shard).await.ok_or_else(|| {
                Error::Invalid(format!("shard {} is not open on this node", shard.get()))
            })?;
            let written = database
                .write_with_durability(namespace, vec![TraceBatch::new(traces)], durability)
                .await?;
            report.traces = report.traces.saturating_add(written.traces);
            report.pages = report.pages.saturating_add(written.pages);
            report.spans = report.spans.saturating_add(written.spans);
        }
        Ok(report)
    }

    pub async fn get_trace(
        &self,
        routing: &HashRangeMap,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Result<Option<Trace>> {
        let shard = self.route(routing, namespace, trace_id);
        let database = self.shard(shard).await.ok_or_else(|| {
            Error::Invalid(format!("shard {} is not open on this node", shard.get()))
        })?;
        let _permit = self
            .io_permits
            .acquire()
            .await
            .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
        database.get_trace(namespace, trace_id).await
    }

    pub async fn search(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let databases = self.databases().await;
        let permits = Arc::clone(&self.io_permits);
        let mut results = stream::iter(databases.into_iter().map(|database| {
            let permits = Arc::clone(&permits);
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
                database.search(namespace, start_ns, end_ns, matchers).await
            }
        }))
        .buffer_unordered(self.io_permits.available_permits().max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    pub async fn scan_traces(&self, namespace: &Namespace, limit: usize) -> Result<Vec<Trace>> {
        if limit == 0 {
            return Err(Error::Invalid(
                "trace scan limit must be greater than zero".to_owned(),
            ));
        }
        // Select the globally first `limit` IDs from locators alone, then
        // load only those, so S shards never materialize S × limit traces.
        let databases = self.databases().await;
        let concurrency = self.io_permits.available_permits().max(1);
        let permits = Arc::clone(&self.io_permits);
        let mut scanned =
            stream::iter(databases.into_iter().enumerate().map(|(shard, database)| {
                let permits = Arc::clone(&permits);
                async move {
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
                    let scanned = database.scan_trace_ids(namespace, limit).await?;
                    Ok::<_, Error>((database, shard, scanned))
                }
            }))
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        let mut selected = scanned
            .iter()
            .flat_map(|(_, shard, ids)| ids.iter().map(move |id| (id.trace_id, *shard)))
            .collect::<Vec<_>>();
        selected.sort_unstable();
        selected.truncate(limit);
        let keep = selected.into_iter().collect::<HashSet<_>>();
        for (_, shard, ids) in &mut scanned {
            ids.retain(|id| keep.contains(&(id.trace_id, *shard)));
        }

        let permits = Arc::clone(&self.io_permits);
        let mut results = stream::iter(
            scanned
                .into_iter()
                .filter(|(_, _, ids)| !ids.is_empty())
                .map(|(database, _, ids)| {
                    let permits = Arc::clone(&permits);
                    async move {
                        let _permit = permits.acquire_owned().await.map_err(|_| {
                            Error::Invalid("shard I/O limiter is closed".to_owned())
                        })?;
                        database.load_scanned(namespace, ids).await
                    }
                }),
        )
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    pub async fn query_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<TraceQlResult>> {
        let databases = self.databases().await;
        let permits = Arc::clone(&self.io_permits);
        let mut results = stream::iter(databases.into_iter().map(|database| {
            let permits = Arc::clone(&permits);
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
                database
                    .query_traceql(namespace, start_ns, end_ns, source, options)
                    .await
            }
        }))
        .buffer_unordered(self.io_permits.available_permits().max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        results.sort_by_key(|result| (result.start_ns, result.trace_id));
        results.truncate(options.limit);
        Ok(results)
    }

    pub async fn flush(&self) -> Result<()> {
        for database in self.databases().await {
            database.flush().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let databases = std::mem::take(&mut *self.shards.write().await)
            .into_values()
            .collect::<Vec<_>>();
        self.shard_slots.write().await.clear();
        for database in databases {
            database.close().await?;
        }
        Ok(())
    }

    async fn databases(&self) -> Vec<Arc<TraceDb>> {
        self.shards.read().await.values().cloned().collect()
    }
}

#[async_trait]
impl ReaderShardLifecycle for ShardedTrack {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.reconcile_shards(&assignment.routing)
            .await
            .map_err(Into::into)
    }
}

fn slot_range(routing: &HashRangeMap, shard: ShardId) -> Result<std::ops::Range<u16>> {
    routing
        .assignments
        .iter()
        .find(|assignment| assignment.shard == shard)
        .ok_or_else(|| Error::Invalid(format!("unknown shard {}", shard.get())))?
        .range
        .slots()
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;

    fn trace(id: u8, timestamp: u64) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        Trace::new(
            trace_id,
            vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: vec![id; 8],
                        name: format!("span-{id}"),
                        start_time_unix_nano: timestamp,
                        end_time_unix_nano: timestamp + 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        )
        .unwrap()
    }

    fn config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            ..Config::default()
        }
    }

    #[test]
    fn routing_is_stable_and_namespace_scoped() {
        let options = ShardingOptions::new(64, 4).unwrap();
        let routing = HashRangeMap::bootstrap(options.virtual_shards()).unwrap();
        let id = TraceId::new([3; 16]).unwrap();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        assert_eq!(
            options.route(&routing, &a, id),
            options.route(&routing, &a, id)
        );
        assert_ne!(
            options.route(&routing, &a, id),
            options.route(&routing, &b, id)
        );
    }

    #[tokio::test]
    async fn writes_reads_flushes_and_manages_shards() {
        let options = ShardingOptions::new(2, 2).unwrap();
        let routing = HashRangeMap::bootstrap(options.virtual_shards()).unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let database = ShardedTrack::open(config(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
        let traces = vec![trace(1, 1), trace(2, 2)];
        database
            .write(
                &routing,
                &namespace,
                vec![TraceBatch::new(traces.clone())],
                Durability::Written,
            )
            .await
            .unwrap();
        for trace in traces {
            assert_eq!(
                database
                    .get_trace(&routing, &namespace, trace.trace_id)
                    .await
                    .unwrap(),
                Some(trace)
            );
        }
        database.flush().await.unwrap();

        let extra = ShardedTrack::open(config(), options, []).await.unwrap();
        extra.open_shard(ShardId::new(1)).await.unwrap();
        let reference = extra.shard(ShardId::new(1)).await.unwrap();
        assert!(extra.close_shard(ShardId::new(1)).await.is_err());
        drop(reference);
        extra.flush_shard(ShardId::new(1)).await.unwrap();
        extra.close_shard(ShardId::new(1)).await.unwrap();
        database.close().await.unwrap();
        extra.close().await.unwrap();
    }

    #[tokio::test]
    async fn reconciliation_adds_resizes_and_safely_removes_reader_shards() {
        let options = ShardingOptions::new(1, 2).unwrap();
        let one = HashRangeMap::bootstrap(1).unwrap();
        let two = one.grow_to(2).unwrap();
        let database = ShardedTrack::open_readers_with_routing(
            config(),
            options,
            [ShardId::new(0)],
            &one,
            DbReaderOptions::default(),
        )
        .await
        .unwrap();

        database.reconcile_shards(&two).await.unwrap();
        assert_eq!(
            database.open_shards().await,
            vec![ShardId::new(0), ShardId::new(1)]
        );
        let active = database.shard(ShardId::new(1)).await.unwrap();
        assert!(database.reconcile_shards(&one).await.is_err());
        assert_eq!(
            database.open_shards().await,
            vec![ShardId::new(0), ShardId::new(1)]
        );
        drop(active);
        database.reconcile_shards(&one).await.unwrap();
        assert_eq!(database.open_shards().await, vec![ShardId::new(0)]);
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn sharded_scan_selects_the_same_traces_as_unsharded() {
        let namespace = Namespace::new("tenant").unwrap();
        // Start times run opposite to ID order, so the selection is by ID.
        let traces = (1..=12)
            .map(|id| trace(id, 1_000 - u64::from(id)))
            .collect::<Vec<_>>();
        let oracle = TraceDb::open(config()).await.unwrap();
        oracle
            .write(&namespace, vec![TraceBatch::new(traces.clone())])
            .await
            .unwrap();
        let sharded = ShardedTrack::open(
            config(),
            ShardingOptions::new(4, 2).unwrap(),
            (0..4).map(ShardId::new),
        )
        .await
        .unwrap();
        let routing = HashRangeMap::bootstrap(4).unwrap();
        sharded
            .write(
                &routing,
                &namespace,
                vec![TraceBatch::new(traces)],
                Durability::Written,
            )
            .await
            .unwrap();
        for limit in [1, 5, 12, 20] {
            assert_eq!(
                sharded.scan_traces(&namespace, limit).await.unwrap(),
                oracle.scan_traces(&namespace, limit).await.unwrap(),
                "limit {limit}"
            );
        }
    }

    #[tokio::test]
    async fn sharded_query_matches_unsharded_oracle_with_global_limit() {
        let namespace = Namespace::new("tenant").unwrap();
        let traces = vec![trace(3, 30), trace(1, 10), trace(2, 20)];
        let oracle = TraceDb::open(config()).await.unwrap();
        oracle
            .write(&namespace, vec![TraceBatch::new(traces.clone())])
            .await
            .unwrap();
        let sharded = ShardedTrack::open(
            config(),
            ShardingOptions::new(4, 2).unwrap(),
            (0..4).map(ShardId::new),
        )
        .await
        .unwrap();
        let routing = HashRangeMap::bootstrap(4).unwrap();
        sharded
            .write(
                &routing,
                &namespace,
                vec![TraceBatch::new(traces)],
                Durability::Written,
            )
            .await
            .unwrap();
        let options = QueryOptions {
            limit: 2,
            ..QueryOptions::default()
        };
        assert_eq!(
            sharded
                .query_traceql(&namespace, 0, 100, "{}", options)
                .await
                .unwrap(),
            oracle
                .query_traceql(&namespace, 0, 100, "{}", options)
                .await
                .unwrap()
        );
        sharded.close().await.unwrap();
        oracle.close().await.unwrap();
    }
}
