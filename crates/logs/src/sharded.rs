use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use common::{SharedDbCache, SstWarmTracker};
use sharding::{
    ReaderShardLifecycle, ShardDatabase, ShardId, ShardMap, ShardRole, ShardSet, ShardingOptions,
    shard_opener,
};
use slatedb::config::DbReaderOptions;
use tokio_util::sync::CancellationToken;

use futures::future::{try_join, try_join_all};
use proto::logs::internal::v1 as pb;

use crate::db::BlockCache;
use crate::query::{ShardSource, query_databases, serve_shard_query};
use crate::remote::{
    LogsShardClient, RemoteShard, labels_to_wire, namespace_from_wire, query_from_wire,
};
use crate::{
    Config, Durability, Error, Labels, LogBatch, LogDb, Namespace, QueryOptions, QueryRequest,
    QueryResult, Result, WriteReport,
};

#[async_trait]
impl ShardDatabase for LogDb {
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

/// A facade over independently opened storage-shard databases.
///
/// Every shard shares one SlateDB block and metadata cache, so the configured
/// cache capacities bound the whole process rather than each shard.
pub struct ShardedLogs {
    shards: Arc<ShardSet<LogDb>>,
    cache: SharedDbCache,
    /// Reads the shards other reader groups serve.
    remote: OnceLock<Arc<dyn LogsShardClient>>,
}

impl ShardedLogs {
    pub async fn open(
        config: Config,
        options: ShardingOptions,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self> {
        config.validate()?;
        let cache = SharedDbCache::from_config(&config.storage).await?;
        let shard_cache = cache.clone();
        let blocks = BlockCache::new(config.block_cache_capacity_bytes);
        let opener = shard_opener(move |shard| {
            let config = shard_config(&config, shard);
            let cache = shard_cache.clone();
            let blocks = blocks.clone();
            async move { LogDb::open_with_cache(config, &cache, &blocks).await }
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
        let blocks = BlockCache::new(config.block_cache_capacity_bytes);
        let opener = shard_opener(move |shard| {
            let config = shard_config(&config, shard);
            let reader_options = reader_options.clone();
            let cache = shard_cache.clone();
            let blocks = blocks.clone();
            async move { LogDb::open_reader_with_cache(config, reader_options, &cache, &blocks).await }
        });
        Self::new(
            ShardSet::open(ShardRole::Reader, options, opener, shards).await,
            cache,
        )
        .await
    }

    async fn new(shards: Result<ShardSet<LogDb>>, cache: SharedDbCache) -> Result<Self> {
        match shards {
            Ok(shards) => Ok(Self {
                shards: Arc::new(shards),
                cache,
                remote: OnceLock::new(),
            }),
            Err(error) => {
                cache.close().await?;
                Err(error)
            }
        }
    }

    /// The open storage shards, for ownership lifecycle management.
    pub fn shards(&self) -> &Arc<ShardSet<LogDb>> {
        &self.shards
    }

    /// Reads every shard outside this reader's scope through `client`.
    /// Returns false when a client is already connected.
    pub fn connect_remote_shards(&self, client: Arc<dyn LogsShardClient>) -> bool {
        self.remote.set(client).is_ok()
    }

    /// The shards this reader reads remotely. Never includes an open shard:
    /// a local shard is always read in process.
    fn remote_shards(&self) -> Vec<RemoteShard> {
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
        include_payloads: bool,
        concurrency: usize,
        cancel: &CancellationToken,
        tracker: Option<&SstWarmTracker>,
    ) -> Result<()> {
        self.shards
            .warm(concurrency, cancel, |database, concurrency| async move {
                for namespace in namespaces {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    database
                        .warm_recent(
                            namespace,
                            warm_range,
                            include_payloads,
                            concurrency,
                            cancel,
                            tracker,
                        )
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
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut report = WriteReport::default();
        for (shard, batches) in crate::routing::split(assignment, namespace, batches) {
            let written = self
                .shards
                .require(shard)
                .await?
                .write_with_durability(namespace, batches, durability)
                .await?;
            report.streams = report.streams.saturating_add(written.streams);
            report.pages = report.pages.saturating_add(written.pages);
            report.rows = report.rows.saturating_add(written.rows);
        }
        Ok(report)
    }

    pub async fn query(
        &self,
        namespace: &Namespace,
        request: &QueryRequest,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        let mut sources = self
            .shards
            .entries()
            .await
            .into_iter()
            .map(|(shard, database)| (shard, ShardSource::Local(database)))
            .chain(
                self.remote_shards()
                    .into_iter()
                    .map(|remote| (remote.shard, ShardSource::Remote(remote))),
            )
            .collect::<Vec<_>>();
        sources.sort_by_key(|(shard, _)| *shard);
        query_databases(
            sources.into_iter().map(|(_, source)| source).collect(),
            namespace,
            request,
            options,
            self.shards.io_permits(),
        )
        .await
    }

    pub async fn label_names(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        let remote = self.remote_shards();
        let (mut names, remote) = try_join(
            self.local_label_names(namespace, start_ns, end_ns),
            try_join_all(
                remote
                    .iter()
                    .map(|shard| shard.label_names(namespace, start_ns, end_ns)),
            ),
        )
        .await?;
        names.extend(remote);
        Ok(sorted_union(names))
    }

    pub async fn label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        let remote = self.remote_shards();
        let (mut values, remote) = try_join(
            self.local_label_values(namespace, name, start_ns, end_ns),
            try_join_all(
                remote
                    .iter()
                    .map(|shard| shard.label_values(namespace, name, start_ns, end_ns)),
            ),
        )
        .await?;
        values.extend(remote);
        Ok(sorted_union(values))
    }

    pub async fn series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Labels>> {
        let remote = self.remote_shards();
        let (mut series, remote) = try_join(
            self.local_series(namespace, selectors, start_ns, end_ns),
            try_join_all(
                remote
                    .iter()
                    .map(|shard| shard.series(namespace, selectors, start_ns, end_ns)),
            ),
        )
        .await?;
        series.extend(remote);
        Ok(sorted_union(series))
    }

    async fn local_label_names(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Vec<String>>> {
        self.shards
            .fan_out(
                |database| async move { database.label_names(namespace, start_ns, end_ns).await },
            )
            .await
    }

    async fn local_label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Vec<String>>> {
        self.shards
            .fan_out(|database| async move {
                database
                    .label_values(namespace, name, start_ns, end_ns)
                    .await
            })
            .await
    }

    async fn local_series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Vec<Labels>>> {
        self.shards
            .fan_out(|database| async move {
                database
                    .series(namespace, selectors, start_ns, end_ns)
                    .await
            })
            .await
    }

    /// The shard a reader coordinating a query asks this one to read, which
    /// must be the shard this reader serves.
    async fn served(&self, read: Option<&pb::ShardRead>) -> Result<(Arc<LogDb>, Namespace)> {
        let (shard, namespace) = namespace_from_wire(read)?;
        if self
            .shards
            .reader_scope()
            .is_some_and(|scope| scope != shard)
        {
            return Err(Error::Unavailable(format!(
                "this reader does not serve shard {}",
                shard.get()
            )));
        }
        Ok((self.shards.require(shard).await?, namespace))
    }

    /// Runs a coordinator's query over the shard this reader serves.
    pub async fn serve_query(
        &self,
        request: &pb::QueryShardRequest,
    ) -> Result<Vec<pb::ShardOutput>> {
        let (database, namespace) = self.served(request.read.as_ref()).await?;
        let (query, options) = query_from_wire(request);
        serve_shard_query(
            &database,
            &namespace,
            &query,
            options,
            &self.shards.io_permits(),
        )
        .await
    }

    pub async fn serve_label_names(
        &self,
        request: &pb::LabelNamesRequest,
    ) -> Result<pb::StringsResponse> {
        let (database, namespace) = self.served(request.read.as_ref()).await?;
        let values = self
            .shards
            .with_io(database.label_names(&namespace, request.start_ns, request.end_ns))
            .await?;
        Ok(pb::StringsResponse { values })
    }

    pub async fn serve_label_values(
        &self,
        request: &pb::LabelValuesRequest,
    ) -> Result<pb::StringsResponse> {
        let (database, namespace) = self.served(request.read.as_ref()).await?;
        let values = self
            .shards
            .with_io(database.label_values(
                &namespace,
                &request.name,
                request.start_ns,
                request.end_ns,
            ))
            .await?;
        Ok(pb::StringsResponse { values })
    }

    pub async fn serve_series(&self, request: &pb::SeriesRequest) -> Result<pb::SeriesResponse> {
        let (database, namespace) = self.served(request.read.as_ref()).await?;
        let series = self
            .shards
            .with_io(database.series(
                &namespace,
                &request.selectors,
                request.start_ns,
                request.end_ns,
            ))
            .await?;
        Ok(pb::SeriesResponse {
            series: series.iter().map(labels_to_wire).collect(),
        })
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
impl ReaderShardLifecycle for ShardedLogs {
    async fn reconcile_readers(
        &self,
        assignment: &ShardMap,
    ) -> std::result::Result<(), sharding::BoxError> {
        self.shards.reconcile(assignment).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::{
        BlockCacheConfig, FoyerHybridCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
        SlateDbStorageConfig, StorageConfig,
    };
    use sharding::DEFAULT_IO_CONCURRENCY_LIMIT;

    use super::*;
    use crate::routing::{route, split};
    use crate::{Label, LogEntry};

    fn labels(values: &[(&str, &str)]) -> Labels {
        Labels::new(
            values
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect(),
        )
        .unwrap()
    }

    fn assignment(shards: u32) -> ShardMap {
        ShardMap::new(
            sharding::AssignmentGeneration::new(1),
            shards,
            vec![sharding::Assignment::new(
                sharding::Owner::new("logs-0", 0),
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
                sharding::Owner::new("logs-0", 0),
                sharding::ShardRange::within(0, shards, shards).unwrap(),
                sharding::AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    fn in_memory() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            ..Config::default()
        }
    }

    #[test]
    fn routing_is_canonical_stable_and_namespace_scoped() {
        let routing = assignment(64);
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let canonical = labels(&[("a", "2"), ("z", "1")]);
        let reordered = Labels::new(vec![Label::new("z", "1"), Label::new("a", "2")]).unwrap();
        assert_eq!(
            route(&routing, &a, &canonical, 0),
            route(&routing, &a, &reordered, 0)
        );
        assert_ne!(
            route(&routing, &a, &canonical, 0),
            route(&routing, &b, &canonical, 0)
        );
    }

    #[tokio::test]
    async fn writes_and_queries_across_shards_with_global_limits() {
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let routing = assignment(options.shard_count());
        let database = ShardedLogs::open(in_memory(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let mut batches = Vec::new();
        for shard in 0..2 {
            let labels = (0..10_000)
                .map(|candidate| labels(&[("app", &format!("{shard}-{candidate}"))]))
                .find(|labels| route(&routing, &namespace, labels, 0).get() == shard)
                .unwrap();
            batches.push(LogBatch::new(
                labels,
                vec![LogEntry::new(i64::from(shard) + 1, format!("line-{shard}"))],
            ));
        }
        database
            .write(&routing, &namespace, batches, Durability::Written)
            .await
            .unwrap();
        assert_eq!(
            database.label_names(&namespace, 0, 10).await.unwrap(),
            vec!["app"]
        );
        assert_eq!(
            database
                .label_values(&namespace, "app", 0, 10)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            database
                .series(&namespace, &[r#"{app=~".+"}"#.to_owned()], 0, 10)
                .await
                .unwrap()
                .len(),
            2
        );
        let result = database
            .query(
                &namespace,
                &QueryRequest::range("{app=~\".+\"}", 0, 10, 1),
                QueryOptions {
                    limit: 1,
                    direction: crate::Direction::Forward,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap();
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        assert_eq!(
            streams
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            1
        );
        let error = database
            .query(
                &namespace,
                &QueryRequest::range("{app=~\".+\"}", 0, 10, 1),
                QueryOptions {
                    max_pages: 1,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("max_pages"));
    }

    #[tokio::test]
    async fn shards_share_one_hybrid_block_cache_as_writers_and_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let config = Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logs".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: tmp.path().join("objects").to_str().unwrap().to_owned(),
                }),
                settings_path: None,
                block_cache: Some(BlockCacheConfig::FoyerHybrid(FoyerHybridCacheConfig {
                    memory_capacity: 1 << 20,
                    // Foyer's disk tier uses 16 MiB blocks, and closing the
                    // cache waits for a free block to flush into.
                    disk_capacity: 64 << 20,
                    disk_path: cache_dir.to_str().unwrap().to_owned(),
                    write_policy: Default::default(),
                    flushers: 1,
                    buffer_pool_size: None,
                    submit_queue_size_threshold: 1 << 20,
                })),
                meta_cache: None,
                disk: Default::default(),
            }),
            ..Config::default()
        };
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let shards = [ShardId::new(0), ShardId::new(1)];
        let routing = assignment(options.shard_count());
        let namespace = Namespace::new("tenant").unwrap();
        let batches = (0..2)
            .map(|shard| {
                let labels = (0..10_000)
                    .map(|candidate| labels(&[("app", &format!("{shard}-{candidate}"))]))
                    .find(|labels| route(&routing, &namespace, labels, 0).get() == shard)
                    .unwrap();
                LogBatch::new(labels, vec![LogEntry::new(i64::from(shard) + 1, "line")])
            })
            .collect();
        let request = QueryRequest::range("{app=~\".+\"}", 0, 10, 1);
        let streams = |result| match result {
            QueryResult::Streams(streams) => streams.len(),
            _ => panic!("expected streams"),
        };

        let writers = ShardedLogs::open(config.clone(), options, shards)
            .await
            .unwrap();
        writers
            .write(&routing, &namespace, batches, Durability::Written)
            .await
            .unwrap();
        writers.flush().await.unwrap();
        let written = writers
            .query(&namespace, &request, QueryOptions::default())
            .await
            .unwrap();
        assert_eq!(streams(written), 2);
        writers.close().await.unwrap();

        let readers =
            ShardedLogs::open_readers(config, options, shards, DbReaderOptions::default())
                .await
                .unwrap();
        let read = readers
            .query(&namespace, &request, QueryOptions::default())
            .await
            .unwrap();
        assert_eq!(streams(read), 2);
        readers.close().await.unwrap();
    }

    #[tokio::test]
    async fn writes_to_shards_that_are_not_open_are_rejected() {
        let routing = assignment(2);
        let database = ShardedLogs::open(
            in_memory(),
            ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
            [],
        )
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let error = database
            .write(
                &routing,
                &namespace,
                vec![LogBatch::new(
                    labels(&[("app", "a")]),
                    vec![LogEntry::new(1, "x")],
                )],
                Durability::Written,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Shard(sharding::ShardSetError::NotOpen(_))
        ));
    }

    #[tokio::test]
    async fn reconciliation_opens_new_reader_shards() {
        let options = ShardingOptions::new(1, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let database = ShardedLogs::open_readers(
            in_memory(),
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
    async fn stream_straddling_a_cutover_is_split_and_queried_back() {
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let database = ShardedLogs::open(in_memory(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let routing = scaled(&assignment(1), 2, 5);
        let stream = (0..10_000)
            .map(|candidate| labels(&[("app", &format!("split-{candidate}"))]))
            .find(|labels| route(&routing, &namespace, labels, 5).get() == 1)
            .unwrap();
        let batch = LogBatch::new(
            stream,
            vec![LogEntry::new(2, "before"), LogEntry::new(7, "after")],
        );
        assert_eq!(split(&routing, &namespace, vec![batch.clone()]).len(), 2);
        database
            .write(&routing, &namespace, vec![batch], Durability::Written)
            .await
            .unwrap();
        let QueryResult::Streams(streams) = database
            .query(
                &namespace,
                &QueryRequest::range("{app=~\"split-.+\"}", 0, 10, 1),
                QueryOptions {
                    direction: crate::Direction::Forward,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap()
        else {
            panic!("expected streams");
        };
        let lines = streams
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.clone()))
            .collect::<Vec<_>>();
        assert_eq!(lines, vec!["before", "after"]);
        assert_eq!(streams.len(), 1);
    }

    /// Serves another reader's shard in process, recording what was asked.
    struct InProcessShards {
        reader: Arc<ShardedLogs>,
        asked: std::sync::Mutex<Vec<u32>>,
    }

    impl InProcessShards {
        fn record(&self, read: Option<&pb::ShardRead>) {
            self.asked.lock().unwrap().push(read.unwrap().shard);
        }
    }

    #[async_trait]
    impl LogsShardClient for InProcessShards {
        async fn query(&self, request: pb::QueryShardRequest) -> Result<Vec<pb::ShardOutput>> {
            self.record(request.read.as_ref());
            self.reader.serve_query(&request).await
        }

        async fn label_names(&self, request: pb::LabelNamesRequest) -> Result<pb::StringsResponse> {
            self.record(request.read.as_ref());
            self.reader.serve_label_names(&request).await
        }

        async fn label_values(
            &self,
            request: pb::LabelValuesRequest,
        ) -> Result<pb::StringsResponse> {
            self.record(request.read.as_ref());
            self.reader.serve_label_values(&request).await
        }

        async fn series(&self, request: pb::SeriesRequest) -> Result<pb::SeriesResponse> {
            self.record(request.read.as_ref());
            self.reader.serve_series(&request).await
        }
    }

    #[tokio::test]
    async fn reader_groups_answer_like_a_reader_with_every_shard_open() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logs".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: tmp.path().to_str().unwrap().to_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            ..Config::default()
        };
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let both = [ShardId::new(0), ShardId::new(1)];
        let routing = assignment(options.shard_count());
        let namespace = Namespace::new("tenant").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let t0 = now - 3_600_000_000_000;
        let (start, end) = (t0, t0 + 2_000);
        let mut batches = Vec::new();
        for (app, level) in [
            ("api", "info"),
            ("api", "error"),
            ("web", "info"),
            ("db", "warn"),
        ] {
            for candidate in 0..10_000 {
                let stream = labels(&[
                    ("app", app),
                    ("level", level),
                    ("n", &candidate.to_string()),
                ]);
                if batches.len() % 2 == route(&routing, &namespace, &stream, 0).get() as usize {
                    let entries = (0..50)
                        .map(|i| {
                            LogEntry::new(
                                t0 + 1_000 + i * 7 + batches.len() as i64,
                                format!("{app} {level} line {i} status={}", 200 + i % 3),
                            )
                        })
                        .collect();
                    batches.push(LogBatch::new(stream, entries));
                    break;
                }
            }
        }
        let writers = ShardedLogs::open(config.clone(), options, both)
            .await
            .unwrap();
        writers
            .write(&routing, &namespace, batches, Durability::Written)
            .await
            .unwrap();
        writers.flush().await.unwrap();
        writers.close().await.unwrap();

        let open = |shards: Vec<ShardId>| {
            ShardedLogs::open_readers(config.clone(), options, shards, DbReaderOptions::default())
        };
        let everything = open(both.to_vec()).await.unwrap();
        let coordinator = open(vec![ShardId::new(0)]).await.unwrap();
        let peer = Arc::new(open(vec![ShardId::new(1)]).await.unwrap());
        assert!(coordinator.shards().scope_readers_to(ShardId::new(0)));
        assert!(peer.shards().scope_readers_to(ShardId::new(1)));
        let shards = Arc::new(InProcessShards {
            reader: Arc::clone(&peer),
            asked: Default::default(),
        });
        assert!(coordinator.connect_remote_shards(shards.clone()));

        let queries = [
            QueryRequest::range(r#"{app=~".+"}"#, start, end, 1),
            QueryRequest::range(r#"{app=~".+"} |= "error""#, start, end, 1),
            QueryRequest::range(r#"{app=~".+"} | logfmt | status="201""#, start, end, 1),
            QueryRequest::range(
                r#"sum by (app) (count_over_time({app=~".+"}[100ns]))"#,
                start,
                end,
                50,
            ),
            QueryRequest::range(
                r#"topk(2, sum by (level) (rate({app=~".+"}[200ns])))"#,
                start,
                end,
                100,
            ),
            QueryRequest::range(
                r#"sum by (status) (count_over_time({app=~".+"} | logfmt [500ns]))"#,
                start,
                end,
                250,
            ),
            QueryRequest::instant(
                r#"count(sum by (app) (count_over_time({app=~".+"}[2000ns])))"#,
                end,
            ),
        ];
        for request in &queries {
            for (limit, direction) in [
                (1000, crate::Direction::Forward),
                (7, crate::Direction::Backward),
            ] {
                let options = QueryOptions {
                    limit,
                    direction,
                    ..QueryOptions::default()
                };
                let expected = everything
                    .query(&namespace, request, options.clone())
                    .await
                    .unwrap();
                let actual = coordinator
                    .query(&namespace, request, options)
                    .await
                    .unwrap();
                assert_eq!(actual, expected, "{}", request.query);
                let empty = match &expected {
                    QueryResult::Streams(streams) => streams.is_empty(),
                    QueryResult::Vector(samples) => samples.is_empty(),
                    QueryResult::Matrix(series) => series.is_empty(),
                    QueryResult::Scalar(_) => false,
                };
                assert!(!empty, "{}", request.query);
            }
        }
        assert_eq!(
            coordinator
                .label_names(&namespace, start, end)
                .await
                .unwrap(),
            everything
                .label_names(&namespace, start, end)
                .await
                .unwrap(),
        );
        assert_eq!(
            coordinator
                .label_values(&namespace, "app", start, end)
                .await
                .unwrap(),
            vec!["api", "db", "web"],
        );
        let selectors = [r#"{app=~".+"}"#.to_owned()];
        assert_eq!(
            coordinator
                .series(&namespace, &selectors, start, end)
                .await
                .unwrap(),
            everything
                .series(&namespace, &selectors, start, end)
                .await
                .unwrap(),
        );
        assert!(!shards.asked.lock().unwrap().is_empty());
        assert!(shards.asked.lock().unwrap().iter().all(|shard| *shard == 1));

        let error = coordinator
            .serve_label_names(&pb::LabelNamesRequest {
                read: Some(crate::remote::shard_read(ShardId::new(1), &namespace)),
                start_ns: start,
                end_ns: end,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Unavailable(_)));
        for reader in [everything, coordinator] {
            reader.close().await.unwrap();
        }
        peer.close().await.unwrap();
    }
}
