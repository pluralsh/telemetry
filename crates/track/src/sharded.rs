use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use common::discovery::DiscoveryValue;
use futures::{StreamExt, TryStreamExt, stream};
use sharding::{
    DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_SHARDS, ReaderShardLifecycle, ShardId, ShardMap,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    AttributeMatcher, AttributeScope, Config, Durability, Error, Namespace, QueryOptions, Result,
    Trace, TraceBatch, TraceDb, TraceId, TraceQlResult, WriteReport,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardingOptions {
    shard_count: u32,
    io_concurrency_limit: u32,
}

impl Default for ShardingOptions {
    fn default() -> Self {
        Self {
            shard_count: DEFAULT_SHARDS,
            io_concurrency_limit: DEFAULT_IO_CONCURRENCY_LIMIT,
        }
    }
}

impl ShardingOptions {
    pub fn new(shard_count: u32, io_concurrency_limit: u32) -> Result<Self> {
        if shard_count == 0 {
            return Err(Error::Invalid(
                "shard count must be greater than zero".to_owned(),
            ));
        }
        if io_concurrency_limit == 0 {
            return Err(Error::Invalid(
                "I/O concurrency limit must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            shard_count,
            io_concurrency_limit,
        })
    }

    pub const fn shard_count(self) -> u32 {
        self.shard_count
    }

    pub const fn io_concurrency_limit(self) -> u32 {
        self.io_concurrency_limit
    }

    /// Shard owning a trace (or partial trace) whose earliest span starts
    /// at `start_ns`.
    pub fn route(
        self,
        assignment: &ShardMap,
        namespace: &Namespace,
        trace_id: TraceId,
        start_ns: u64,
    ) -> ShardId {
        assignment.route_key(
            &crate::routing::canonical_routing_key(namespace, trace_id),
            i64::try_from(start_ns).unwrap_or(i64::MAX),
        )
    }

    pub fn route_trace(
        self,
        assignment: &ShardMap,
        namespace: &Namespace,
        trace: &Trace,
    ) -> ShardId {
        self.route(
            assignment,
            namespace,
            trace.trace_id,
            trace.timestamp_range().0,
        )
    }

    /// Every shard that may hold spans of `trace_id` across routing epochs.
    pub fn trace_shards(
        self,
        assignment: &ShardMap,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Vec<ShardId> {
        assignment.shards_for_key(&crate::routing::canonical_routing_key(namespace, trace_id))
    }

    pub fn shard_storage(self, config: &Config, shard: ShardId) -> Result<Config> {
        let mut config = config.clone();
        config.storage = config
            .storage
            .with_path_suffix(&format!("shard-{:04}", shard.get()));
        Ok(config)
    }
}

/// A facade over independently opened storage-shard trace databases.
pub struct ShardedTrack {
    config: Config,
    options: ShardingOptions,
    shards: RwLock<BTreeMap<ShardId, Arc<TraceDb>>>,
    reader_options: Option<DbReaderOptions>,
    io_permits: Arc<Semaphore>,
}

impl ShardedTrack {
    /// Warms recent cache blocks for every open shard and namespace.
    pub async fn warm_recent(
        &self,
        namespaces: &[Namespace],
        warm_range: Duration,
        include_payloads: bool,
        concurrency: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let concurrency = concurrency
            .max(1)
            .min(self.options.io_concurrency_limit() as usize);
        let permit_count = u32::try_from(concurrency).unwrap_or(u32::MAX);
        let _warm_permits = tokio::select! {
            permits = self.io_permits.clone().acquire_many_owned(permit_count) => {
                permits.expect("shard I/O semaphore must remain open")
            }
            () = cancel.cancelled() => return Ok(()),
        };
        let databases = self
            .shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for database in databases {
            for namespace in namespaces {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                database
                    .warm_recent(namespace, warm_range, include_payloads, concurrency, cancel)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn open(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        let mut databases = BTreeMap::new();
        for shard in shards {
            databases.insert(
                shard,
                Arc::new(TraceDb::open(options.shard_storage(&config, shard)?).await?),
            );
        }
        Ok(Self {
            config,
            options,
            shards: RwLock::new(databases),
            reader_options: None,
            io_permits: Arc::new(Semaphore::new(options.io_concurrency_limit() as usize)),
        })
    }

    pub async fn open_readers(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        let mut databases = BTreeMap::new();
        for shard in shards {
            databases.insert(
                shard,
                Arc::new(
                    TraceDb::open_reader(
                        options.shard_storage(&config, shard)?,
                        reader_options.clone(),
                    )
                    .await?,
                ),
            );
        }
        Ok(Self {
            config,
            options,
            shards: RwLock::new(databases),
            reader_options: Some(reader_options),
            io_permits: Arc::new(Semaphore::new(options.io_concurrency_limit() as usize)),
        })
    }

    pub fn route_trace(
        &self,
        assignment: &ShardMap,
        namespace: &Namespace,
        trace: &Trace,
    ) -> ShardId {
        self.options.route_trace(assignment, namespace, trace)
    }

    pub async fn contains_shard(&self, shard: ShardId) -> bool {
        self.shards.read().await.contains_key(&shard)
    }

    pub async fn shard(&self, shard: ShardId) -> Option<Arc<TraceDb>> {
        self.shards.read().await.get(&shard).cloned()
    }

    pub async fn open_shard(&self, shard: ShardId) -> Result<()> {
        if self.contains_shard(shard).await {
            return Ok(());
        }
        let database = TraceDb::open(self.options.shard_storage(&self.config, shard)?).await?;
        let mut shards = self.shards.write().await;
        match shards.entry(shard) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(database));
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
        let database = match Arc::try_unwrap(database) {
            Ok(database) => database,
            Err(database) => {
                shards.insert(shard, database);
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

    /// Opens readers for every shard in `assignment`. Shards are never
    /// removed because routing epochs only grow.
    pub async fn reconcile_shards(&self, assignment: &ShardMap) -> Result<()> {
        let reader_options = self.reader_options.as_ref().ok_or_else(|| {
            Error::Invalid("reader reconciliation requires a reader facade".into())
        })?;
        let current = self.open_shards().await;
        let mut opened = BTreeMap::new();
        for shard in (0..assignment.shard_count).map(ShardId::new) {
            if current.contains(&shard) {
                continue;
            }
            opened.insert(
                shard,
                Arc::new(
                    TraceDb::open_reader(
                        self.options.shard_storage(&self.config, shard)?,
                        reader_options.clone(),
                    )
                    .await?,
                ),
            );
        }
        if !opened.is_empty() {
            self.shards.write().await.extend(opened);
        }
        Ok(())
    }

    pub async fn write(
        &self,
        assignment: &ShardMap,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut grouped = BTreeMap::<ShardId, Vec<Trace>>::new();
        for trace in batches.into_iter().flat_map(|batch| batch.traces) {
            grouped
                .entry(self.route_trace(assignment, namespace, &trace))
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

    /// Loads a trace from every shard it may have been routed to across
    /// routing epochs and merges the partial traces.
    pub async fn get_trace(
        &self,
        assignment: &ShardMap,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Result<Option<Trace>> {
        let mut parts = Vec::new();
        for shard in self.options.trace_shards(assignment, namespace, trace_id) {
            let database = self.shard(shard).await.ok_or_else(|| {
                Error::Invalid(format!("shard {} is not open on this node", shard.get()))
            })?;
            let _permit = self
                .io_permits
                .acquire()
                .await
                .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
            parts.extend(database.get_trace(namespace, trace_id).await?);
        }
        Ok(crate::db::merge_traces(parts).pop())
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
        if self.shards.read().await.len() > 1 {
            results = crate::db::merge_traces(results);
        }
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

    pub async fn catalog_names(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
    ) -> Result<Vec<String>> {
        let databases = self.databases().await;
        let permits = Arc::clone(&self.io_permits);
        let names = stream::iter(databases.into_iter().map(|database| {
            let permits = Arc::clone(&permits);
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
                database
                    .catalog_names(namespace, start_ns, end_ns, scope)
                    .await
            }
        }))
        .buffer_unordered(self.io_permits.available_permits().max(1))
        .try_collect::<Vec<_>>()
        .await?;
        Ok(names
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub async fn catalog_values(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
        name: &str,
    ) -> Result<Vec<DiscoveryValue>> {
        let databases = self.databases().await;
        let permits = Arc::clone(&self.io_permits);
        let values = stream::iter(databases.into_iter().map(|database| {
            let permits = Arc::clone(&permits);
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))?;
                database
                    .catalog_values(namespace, start_ns, end_ns, scope, name)
                    .await
            }
        }))
        .buffer_unordered(self.io_permits.available_permits().max(1))
        .try_collect::<Vec<_>>()
        .await?;
        Ok(values
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
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
        let shards = databases.iter().map(Arc::as_ref).collect::<Vec<_>>();
        crate::db::execute_traceql(
            &shards,
            Some(&self.io_permits),
            namespace,
            (start_ns, end_ns),
            source,
            options,
        )
        .await
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
        self.reconcile_shards(assignment).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
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
                        attributes: vec![KeyValue {
                            key: "shard".to_owned(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(i64::from(id))),
                            }),
                        }],
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

    fn assignment(shards: u32) -> ShardMap {
        ShardMap::new(
            sharding::AssignmentGeneration::new(1),
            shards,
            vec![sharding::Assignment::new(
                sharding::Owner::new("track-0", 0),
                sharding::ShardRange::within(0, shards, shards).unwrap(),
                sharding::AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    fn scaled(previous: &ShardMap, shards: u32, cutover_ns: i64) -> ShardMap {
        let mut epochs = previous.epochs.clone();
        epochs.push(sharding::RoutingEpoch {
            effective_from_ns: cutover_ns,
            routing: sharding::HashRangeMap::bootstrap(shards).unwrap(),
        });
        ShardMap::with_epochs(
            previous.generation.next(),
            epochs,
            vec![sharding::Assignment::new(
                sharding::Owner::new("track-0", 0),
                sharding::ShardRange::within(0, shards, shards).unwrap(),
                sharding::AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn routing_is_stable_and_namespace_scoped() {
        let options = ShardingOptions::new(64, 4).unwrap();
        let routing = assignment(options.shard_count());
        let id = TraceId::new([3; 16]).unwrap();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        assert_eq!(
            options.route(&routing, &a, id, 0),
            options.route(&routing, &a, id, 0)
        );
        assert_ne!(
            options.route(&routing, &a, id, 0),
            options.route(&routing, &b, id, 0)
        );
    }

    #[tokio::test]
    async fn trace_split_across_a_cutover_is_merged_on_read() {
        let namespace = Namespace::new("tenant").unwrap();
        let options = ShardingOptions::new(2, 2).unwrap();
        let database = ShardedTrack::open(config(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
        let id = (1..=u8::MAX)
            .find(|id| {
                let trace_id = TraceId::new([*id; 16]).unwrap();
                options.route(&scaled(&assignment(1), 2, 100), &namespace, trace_id, 100)
                    == ShardId::new(1)
            })
            .unwrap();
        let routing = scaled(&assignment(1), 2, 100);
        let early = trace(id, 10);
        let mut late = trace(id, 200);
        late.resource_spans[0].scope_spans[0].spans[0].span_id = vec![0xAB; 8];
        for part in [&early, &late] {
            database
                .write(
                    &routing,
                    &namespace,
                    vec![TraceBatch::new(vec![part.clone()])],
                    Durability::Written,
                )
                .await
                .unwrap();
        }
        let merged = database
            .get_trace(&routing, &namespace, early.trace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged.spans().count(), 2);
        let found = database.search(&namespace, 0, 1_000, &[]).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].spans().count(), 2);
        let counted = database
            .query_traceql(
                &namespace,
                0,
                1_000,
                "{} | count() > 1",
                QueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counted.len(), 1);
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn writes_reads_flushes_and_manages_shards() {
        let options = ShardingOptions::new(2, 2).unwrap();
        let routing = assignment(options.shard_count());
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
        assert_eq!(
            database
                .catalog_names(&namespace, 0, 10, Some(AttributeScope::Span))
                .await
                .unwrap(),
            vec!["shard"]
        );
        assert_eq!(
            database
                .catalog_values(&namespace, 0, 10, Some(AttributeScope::Span), "shard")
                .await
                .unwrap()
                .len(),
            2
        );
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
    async fn reconciliation_opens_new_reader_shards() {
        let options = ShardingOptions::new(1, 2).unwrap();
        let database = ShardedTrack::open_readers(
            config(),
            options,
            [ShardId::new(0)],
            DbReaderOptions::default(),
        )
        .await
        .unwrap();

        database
            .reconcile_shards(&scaled(&assignment(1), 2, 1_000))
            .await
            .unwrap();
        assert_eq!(
            database.open_shards().await,
            vec![ShardId::new(0), ShardId::new(1)]
        );
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
        let routing = assignment(4);
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
        let routing = assignment(4);
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

    #[tokio::test]
    async fn unscoped_numeric_equality_uses_postings_across_shards() {
        let namespace = Namespace::new("tenant").unwrap();
        let sharded = ShardedTrack::open(
            config(),
            ShardingOptions::new(4, 2).unwrap(),
            (0..4).map(ShardId::new),
        )
        .await
        .unwrap();
        sharded
            .write(
                &assignment(4),
                &namespace,
                vec![TraceBatch::new(vec![
                    trace(1, 10),
                    trace(2, 20),
                    trace(3, 30),
                ])],
                Durability::Written,
            )
            .await
            .unwrap();
        // One candidate budget: only the posting-selected trace may load.
        let options = QueryOptions {
            max_candidate_traces: 1,
            ..QueryOptions::default()
        };
        for query in ["{ .shard = 2 }", "{ .shard = 2.0 }"] {
            let results = sharded
                .query_traceql(&namespace, 0, 100, query, options)
                .await
                .unwrap();
            assert_eq!(results.len(), 1, "{query}");
            assert_eq!(results[0].trace_id, TraceId::new([2; 16]).unwrap());
        }
        assert!(
            sharded
                .query_traceql(&namespace, 0, 100, "{}", options)
                .await
                .is_err()
        );
        sharded.close().await.unwrap();
    }
}
