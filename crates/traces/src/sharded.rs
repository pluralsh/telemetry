use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use common::SharedDbCache;
use common::discovery::DiscoveryValue;
use sharding::{
    ReaderShardLifecycle, ShardDatabase, ShardId, ShardMap, ShardRole, ShardSet, ShardingOptions,
    shard_opener,
};
use slatedb::config::DbReaderOptions;
use tokio_util::sync::CancellationToken;

use crate::routing::{route_trace, trace_shards};
use crate::{
    AttributeMatcher, AttributeScope, Config, Durability, Error, Namespace, QueryOptions, Result,
    Trace, TraceBatch, TraceDb, TraceId, TraceQlResult, TraceSummary, WriteReport,
};

#[async_trait]
impl ShardDatabase for TraceDb {
    type Error = Error;

    async fn flush_database(&self) -> Result<()> {
        self.flush().await
    }

    async fn close_database(self: Arc<Self>) -> Result<()> {
        self.close().await
    }
}

fn shard_config(config: &Config, shard: ShardId) -> Config {
    let mut config = config.clone();
    config.storage = config
        .storage
        .with_path_suffix(&ShardingOptions::shard_suffix(shard));
    config
}

/// A facade over independently opened storage-shard trace databases.
///
/// Every shard shares one SlateDB block and metadata cache, so the configured
/// cache capacities bound the whole process rather than each shard.
pub struct ShardedTraces {
    shards: Arc<ShardSet<TraceDb>>,
    cache: SharedDbCache,
}

impl ShardedTraces {
    pub async fn open(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        config.validate()?;
        let cache = SharedDbCache::from_config(&config.storage).await?;
        let shard_cache = cache.clone();
        let opener = shard_opener(move |shard| {
            let config = shard_config(&config, shard);
            let cache = shard_cache.clone();
            async move { TraceDb::open_with_cache(config, &cache).await }
        });
        Self::new(
            ShardSet::open(ShardRole::Writer, options, opener, shards).await,
            cache,
        )
        .await
    }

    pub async fn open_readers(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
        let cache = SharedDbCache::from_config(&config.storage).await?;
        let shard_cache = cache.clone();
        let opener = shard_opener(move |shard| {
            let config = shard_config(&config, shard);
            let reader_options = reader_options.clone();
            let cache = shard_cache.clone();
            async move { TraceDb::open_reader_with_cache(config, reader_options, &cache).await }
        });
        Self::new(
            ShardSet::open(ShardRole::Reader, options, opener, shards).await,
            cache,
        )
        .await
    }

    async fn new(shards: Result<ShardSet<TraceDb>>, cache: SharedDbCache) -> Result<Self> {
        match shards {
            Ok(shards) => Ok(Self {
                shards: Arc::new(shards),
                cache,
            }),
            Err(error) => {
                cache.close().await?;
                Err(error)
            }
        }
    }

    /// The open storage shards, for ownership lifecycle management.
    pub fn shards(&self) -> &Arc<ShardSet<TraceDb>> {
        &self.shards
    }

    /// Warms recent cache blocks for every open shard and namespace.
    pub async fn warm_recent(
        &self,
        namespaces: &[Namespace],
        warm_range: Duration,
        include_payloads: bool,
        concurrency: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        self.shards
            .warm(concurrency, cancel, |database, concurrency| async move {
                for namespace in namespaces {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    database
                        .warm_recent(namespace, warm_range, include_payloads, concurrency, cancel)
                        .await?;
                }
                Ok(())
            })
            .await
    }

    /// Writes to shards opened locally; every routed shard must be open.
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
                .entry(route_trace(assignment, namespace, &trace))
                .or_default()
                .push(trace);
        }
        let mut report = WriteReport::default();
        for (shard, traces) in grouped {
            let written = self
                .shards
                .require(shard)
                .await?
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
        for shard in trace_shards(assignment, namespace, trace_id) {
            let database = self.shards.require(shard).await?;
            parts.extend(
                self.shards
                    .with_io(database.get_trace(namespace, trace_id))
                    .await?,
            );
        }
        Ok(crate::db::merge_traces(parts)?.pop())
    }

    pub async fn search(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let per_shard = self
            .shards
            .fan_out(|database| async move {
                database.search(namespace, start_ns, end_ns, matchers).await
            })
            .await?;
        let multiple_shards = per_shard.len() > 1;
        let mut results = per_shard.into_iter().flatten().collect::<Vec<_>>();
        if multiple_shards {
            results = crate::db::merge_traces(results)?;
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
        let mut scanned = self
            .shards
            .fan_out(|database| async move {
                let ids = database.scan_trace_ids(namespace, limit).await?;
                Ok::<_, Error>((database, ids))
            })
            .await?;

        let mut selected = scanned
            .iter()
            .enumerate()
            .flat_map(|(shard, (_, ids))| ids.iter().map(move |id| (id.trace_id, shard)))
            .collect::<Vec<_>>();
        selected.sort_unstable();
        selected.truncate(limit);
        let keep = selected.into_iter().collect::<HashSet<_>>();
        for (shard, (_, ids)) in scanned.iter_mut().enumerate() {
            ids.retain(|id| keep.contains(&(id.trace_id, shard)));
        }

        let loaded = futures::future::try_join_all(
            scanned.into_iter().filter(|(_, ids)| !ids.is_empty()).map(
                |(database, ids)| async move {
                    self.shards
                        .with_io(database.load_scanned(namespace, ids))
                        .await
                },
            ),
        )
        .await?;
        let mut results = loaded.into_iter().flatten().collect::<Vec<_>>();
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
        let names = self
            .shards
            .fan_out(|database| async move {
                database
                    .catalog_names(namespace, start_ns, end_ns, scope)
                    .await
            })
            .await?;
        Ok(sorted_union(names))
    }

    pub async fn catalog_values(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
        name: &str,
    ) -> Result<Vec<DiscoveryValue>> {
        let values = self
            .shards
            .fan_out(|database| async move {
                database
                    .catalog_values(namespace, start_ns, end_ns, scope, name)
                    .await
            })
            .await?;
        Ok(sorted_union(values))
    }

    pub async fn query_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<TraceQlResult>> {
        let databases = self.shards.databases().await;
        let shards = databases.iter().map(Arc::as_ref).collect::<Vec<_>>();
        let permits = self.shards.io_permits();
        crate::db::execute_traceql(
            &shards,
            Some(&permits),
            namespace,
            (start_ns, end_ns),
            source,
            options,
        )
        .await
    }

    /// [`Self::query_traceql`] without matched spans, as a search response
    /// lists them.
    pub async fn search_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<TraceSummary>> {
        let databases = self.shards.databases().await;
        let shards = databases.iter().map(Arc::as_ref).collect::<Vec<_>>();
        let permits = self.shards.io_permits();
        crate::db::execute_traceql(
            &shards,
            Some(&permits),
            namespace,
            (start_ns, end_ns),
            source,
            options,
        )
        .await
    }

    pub async fn flush(&self) -> Result<()> {
        self.shards.flush_all().await
    }

    /// Closes every shard, then the cache they share.
    pub async fn close(&self) -> Result<()> {
        let closed = self.shards.close_all().await;
        self.cache.close().await?;
        closed
    }
}

fn sorted_union<T: Ord>(parts: Vec<Vec<T>>) -> Vec<T> {
    parts
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[async_trait]
impl ReaderShardLifecycle for ShardedTraces {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.shards.reconcile(assignment).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;
    use crate::routing::route;

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
                sharding::Owner::new("traces-0", 0),
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
                sharding::Owner::new("traces-0", 0),
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
        assert_eq!(route(&routing, &a, id, 0), route(&routing, &a, id, 0));
        assert_ne!(route(&routing, &a, id, 0), route(&routing, &b, id, 0));
    }

    #[tokio::test]
    async fn trace_split_across_a_cutover_is_merged_on_read() {
        let namespace = Namespace::new("tenant").unwrap();
        let options = ShardingOptions::new(2, 2).unwrap();
        let database = ShardedTraces::open(config(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
        let id = (1..=u8::MAX)
            .find(|id| {
                let trace_id = TraceId::new([*id; 16]).unwrap();
                route(&scaled(&assignment(1), 2, 100), &namespace, trace_id, 100) == ShardId::new(1)
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
        let database = ShardedTraces::open(config(), options, [ShardId::new(0), ShardId::new(1)])
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

        let extra = ShardedTraces::open(config(), options, []).await.unwrap();
        extra.shards().open_shard(ShardId::new(1)).await.unwrap();
        let reference = extra.shards().get(ShardId::new(1)).await.unwrap();
        assert!(extra.shards().close_shard(ShardId::new(1)).await.is_err());
        drop(reference);
        extra.shards().flush_shard(ShardId::new(1)).await.unwrap();
        extra.shards().close_shard(ShardId::new(1)).await.unwrap();
        database.close().await.unwrap();
        extra.close().await.unwrap();
    }

    #[tokio::test]
    async fn reconciliation_opens_new_reader_shards() {
        let options = ShardingOptions::new(1, 2).unwrap();
        let database = ShardedTraces::open_readers(
            config(),
            options,
            [ShardId::new(0)],
            DbReaderOptions::default(),
        )
        .await
        .unwrap();

        database
            .reconcile_readers(&scaled(&assignment(1), 2, 1_000))
            .await
            .unwrap();
        assert_eq!(
            database.shards().ids().await,
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
        let sharded = ShardedTraces::open(
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
        let sharded = ShardedTraces::open(
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
        let sharded = ShardedTraces::open(
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
