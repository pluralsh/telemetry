use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::SharedDbCache;
use sharding::{
    ReaderShardLifecycle, ShardDatabase, ShardId, ShardMap, ShardRole, ShardSet, ShardingOptions,
    shard_opener,
};
use slatedb::config::DbReaderOptions;
use tokio_util::sync::CancellationToken;

use crate::query::query_databases;
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
        let opener = shard_opener(move |shard| {
            let config = shard_config(&config, shard);
            let cache = shard_cache.clone();
            async move { LogDb::open_with_cache(config, &cache).await }
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
            async move { LogDb::open_reader_with_cache(config, reader_options, &cache).await }
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
        query_databases(
            self.shards.databases().await,
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
        let names = self
            .shards
            .fan_out(
                |database| async move { database.label_names(namespace, start_ns, end_ns).await },
            )
            .await?;
        Ok(sorted_union(names))
    }

    pub async fn label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        let values = self
            .shards
            .fan_out(|database| async move {
                database
                    .label_values(namespace, name, start_ns, end_ns)
                    .await
            })
            .await?;
        Ok(sorted_union(values))
    }

    pub async fn series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Labels>> {
        let series = self
            .shards
            .fan_out(|database| async move {
                database
                    .series(namespace, selectors, start_ns, end_ns)
                    .await
            })
            .await?;
        Ok(sorted_union(series))
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
}
