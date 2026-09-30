use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sharding::{
    DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_SHARDS, ReaderShardLifecycle, ShardId, ShardMap,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::query::query_databases;
use crate::{
    Config, Durability, Error, Labels, LogBatch, LogDb, Namespace, QueryOptions, QueryRequest,
    QueryResult, Result, WriteReport,
};

fn shard_io_semaphore(limit: u32) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(limit as usize))
}

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

    /// Shard owning an entry of `labels` timestamped `timestamp_ns`.
    pub fn route(
        self,
        assignment: &ShardMap,
        namespace: &Namespace,
        labels: &Labels,
        timestamp_ns: i64,
    ) -> ShardId {
        assignment.route_key(
            &crate::routing::canonical_routing_key(namespace, labels),
            timestamp_ns,
        )
    }

    /// Splits `batches` by the shard owning each entry. A stream whose
    /// entries straddle a routing epoch cutover is written to both shards.
    pub fn split(
        self,
        assignment: &ShardMap,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
    ) -> BTreeMap<ShardId, Vec<LogBatch>> {
        let mut grouped: BTreeMap<ShardId, Vec<LogBatch>> = BTreeMap::new();
        for batch in batches {
            let key = crate::routing::canonical_routing_key(namespace, &batch.labels);
            if assignment.epochs.len() == 1 {
                grouped
                    .entry(assignment.route_key(&key, 0))
                    .or_default()
                    .push(batch);
                continue;
            }
            let mut by_shard: BTreeMap<ShardId, Vec<crate::LogEntry>> = BTreeMap::new();
            for entry in batch.entries {
                by_shard
                    .entry(assignment.route_key(&key, entry.timestamp_ns))
                    .or_default()
                    .push(entry);
            }
            for (shard, entries) in by_shard {
                grouped
                    .entry(shard)
                    .or_default()
                    .push(LogBatch::new(batch.labels.clone(), entries));
            }
        }
        grouped
    }

    pub fn shard_storage(self, config: &Config, shard: ShardId) -> Result<Config> {
        let mut config = config.clone();
        config.storage = config
            .storage
            .with_path_suffix(&format!("shard-{:04}", shard.get()));
        Ok(config)
    }
}

/// A facade over independently opened storage-shard databases.
pub struct ShardedLine {
    config: Config,
    options: ShardingOptions,
    shards: RwLock<BTreeMap<ShardId, Arc<LogDb>>>,
    reader_options: Option<DbReaderOptions>,
    io_permits: Arc<Semaphore>,
}

impl ShardedLine {
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
            let database = LogDb::open(options.shard_storage(&config, shard)?).await?;
            databases.insert(shard, Arc::new(database));
        }
        Ok(Self {
            config,
            options,
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            shards: RwLock::new(databases),
            reader_options: None,
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
            let database = LogDb::open_reader(
                options.shard_storage(&config, shard)?,
                reader_options.clone(),
            )
            .await?;
            databases.insert(shard, Arc::new(database));
        }
        Ok(Self {
            config,
            options,
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            shards: RwLock::new(databases),
            reader_options: Some(reader_options),
        })
    }

    pub fn route(
        &self,
        assignment: &ShardMap,
        namespace: &Namespace,
        labels: &Labels,
        timestamp_ns: i64,
    ) -> ShardId {
        self.options
            .route(assignment, namespace, labels, timestamp_ns)
    }

    pub async fn contains_shard(&self, shard: ShardId) -> bool {
        self.shards.read().await.contains_key(&shard)
    }

    pub async fn shard(&self, shard: ShardId) -> Option<Arc<LogDb>> {
        self.shards.read().await.get(&shard).cloned()
    }

    pub async fn open_shard(&self, shard: ShardId) -> Result<()> {
        if self.contains_shard(shard).await {
            return Ok(());
        }
        let database = LogDb::open(self.options.shard_storage(&self.config, shard)?).await?;
        let mut shards = self.shards.write().await;
        if shards.contains_key(&shard) {
            drop(shards);
            database.close().await?;
            return Ok(());
        }
        shards.insert(shard, Arc::new(database));
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
                    LogDb::open_reader(
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
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let grouped = self.options.split(assignment, namespace, batches);
        let mut report = WriteReport::default();
        for (shard, batches) in grouped {
            let database = self.shard(shard).await.ok_or_else(|| {
                Error::Invalid(format!("shard {} is not open on this node", shard.get()))
            })?;
            let written = database
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
        let databases = self.shards.read().await.values().cloned().collect();
        query_databases(
            databases,
            namespace,
            request,
            options,
            Arc::clone(&self.io_permits),
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
            .each_shard(|database| async move {
                database.label_names(namespace, start_ns, end_ns).await
            })
            .await?;
        Ok(names
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub async fn label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        let values = self
            .each_shard(|database| async move {
                database
                    .label_values(namespace, name, start_ns, end_ns)
                    .await
            })
            .await?;
        Ok(values
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub async fn series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Labels>> {
        let series = self
            .each_shard(|database| async move {
                database
                    .series(namespace, selectors, start_ns, end_ns)
                    .await
            })
            .await?;
        Ok(series
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    /// Runs `read` on every open shard concurrently, one I/O permit each.
    async fn each_shard<T, F, Fut>(&self, read: F) -> Result<Vec<T>>
    where
        F: Fn(Arc<LogDb>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let databases = self
            .shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        futures::future::try_join_all(databases.into_iter().map(|database| {
            let permits = Arc::clone(&self.io_permits);
            let read = &read;
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Query("global query scheduler closed".into()))?;
                read(database).await
            }
        }))
        .await
    }

    pub async fn flush(&self) -> Result<()> {
        let databases = self
            .shards
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for database in databases {
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
}

#[async_trait]
impl ReaderShardLifecycle for ShardedLine {
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

    use super::*;
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
                sharding::Owner::new("line-0", 0),
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
                sharding::Owner::new("line-0", 0),
                sharding::ShardRange::within(0, shards, shards).unwrap(),
                sharding::AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn routing_is_canonical_stable_and_namespace_scoped() {
        let options = ShardingOptions::new(64, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let routing = assignment(options.shard_count());
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let canonical = labels(&[("a", "2"), ("z", "1")]);
        let reordered = Labels::new(vec![Label::new("z", "1"), Label::new("a", "2")]).unwrap();
        assert_eq!(
            options.route(&routing, &a, &canonical, 0),
            options.route(&routing, &a, &reordered, 0)
        );
        assert_ne!(
            options.route(&routing, &a, &canonical, 0),
            options.route(&routing, &b, &canonical, 0)
        );
    }

    #[test]
    fn shard_io_budget_uses_fixed_process_limit() {
        assert_eq!(shard_io_semaphore(128).available_permits(), 128);
        assert_eq!(shard_io_semaphore(32).available_permits(), 32);
    }

    #[tokio::test]
    async fn writes_and_queries_across_shards_with_global_limits() {
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let routing = assignment(options.shard_count());
        let database = ShardedLine::open(
            Config {
                storage: StorageConfig::InMemory,
                ..Config::default()
            },
            options,
            [ShardId::new(0), ShardId::new(1)],
        )
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let mut batches = Vec::new();
        for shard in 0..2 {
            let labels = (0..10_000)
                .map(|candidate| labels(&[("app", &format!("{shard}-{candidate}"))]))
                .find(|labels| options.route(&routing, &namespace, labels, 0).get() == shard)
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
    async fn opens_and_closes_shards_dynamically_without_losing_failed_close() {
        let database = ShardedLine::open(
            Config {
                storage: StorageConfig::InMemory,
                ..Config::default()
            },
            ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
            [],
        )
        .await
        .unwrap();
        let shard = ShardId::new(1);

        database.open_shard(shard).await.unwrap();
        database.open_shard(shard).await.unwrap();
        assert_eq!(database.open_shard_count().await, 1);

        let reference = database.shard(shard).await.unwrap();
        let error = database.close_shard(shard).await.unwrap_err();
        assert!(error.to_string().contains("in-flight references"));
        assert!(database.contains_shard(shard).await);

        drop(reference);
        database.flush_shard(shard).await.unwrap();
        database.close_shard(shard).await.unwrap();
        assert!(!database.contains_shard(shard).await);
    }

    #[tokio::test]
    async fn reconciliation_opens_new_reader_shards() {
        let options = ShardingOptions::new(1, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let database = ShardedLine::open_readers(
            Config {
                storage: StorageConfig::InMemory,
                ..Config::default()
            },
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
    async fn stream_straddling_a_cutover_is_split_and_queried_back() {
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let database = ShardedLine::open(
            Config {
                storage: StorageConfig::InMemory,
                ..Config::default()
            },
            options,
            [ShardId::new(0), ShardId::new(1)],
        )
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let routing = scaled(&assignment(1), 2, 5);
        let stream = (0..10_000)
            .map(|candidate| labels(&[("app", &format!("split-{candidate}"))]))
            .find(|labels| options.route(&routing, &namespace, labels, 5).get() == 1)
            .unwrap();
        let batch = LogBatch::new(
            stream,
            vec![LogEntry::new(2, "before"), LogEntry::new(7, "after")],
        );
        assert_eq!(
            options
                .split(&routing, &namespace, vec![batch.clone()])
                .len(),
            2
        );
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
