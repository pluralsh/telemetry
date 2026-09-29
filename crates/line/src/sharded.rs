use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sharding::{
    DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_VIRTUAL_SHARDS, HashRangeMap, ReaderShardLifecycle,
    ShardId, ShardMap,
};
use slatedb::config::DbReaderOptions;
use tokio::sync::{RwLock, Semaphore};

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

    pub fn route(self, routing: &HashRangeMap, namespace: &Namespace, labels: &Labels) -> ShardId {
        routing.route(sharding::hash_routing_key(
            &crate::routing::canonical_routing_key(namespace, labels),
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

/// A facade over independently opened fixed virtual-shard databases.
pub struct ShardedLine {
    config: Config,
    options: ShardingOptions,
    shards: RwLock<BTreeMap<ShardId, Arc<LogDb>>>,
    shard_slots: RwLock<BTreeMap<ShardId, std::ops::Range<u16>>>,
    reader_options: Option<DbReaderOptions>,
    io_permits: Arc<Semaphore>,
}

impl ShardedLine {
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
            let database =
                LogDb::open_with_slots(options.shard_storage(&config, shard)?, slots.clone())
                    .await?;
            databases.insert(shard, Arc::new(database));
            shard_slots.insert(shard, slots);
        }
        Ok(Self {
            config,
            options,
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            shards: RwLock::new(databases),
            shard_slots: RwLock::new(shard_slots),
            reader_options: None,
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
            let database = LogDb::open_reader_with_slots(
                options.shard_storage(&config, shard)?,
                slots.clone(),
                reader_options.clone(),
            )
            .await?;
            databases.insert(shard, Arc::new(database));
            shard_slots.insert(shard, slots);
        }
        Ok(Self {
            config,
            options,
            io_permits: shard_io_semaphore(options.io_concurrency_limit()),
            shards: RwLock::new(databases),
            shard_slots: RwLock::new(shard_slots),
            reader_options: Some(reader_options),
        })
    }

    pub fn route(&self, routing: &HashRangeMap, namespace: &Namespace, labels: &Labels) -> ShardId {
        self.options.route(routing, namespace, labels)
    }

    pub async fn contains_shard(&self, shard: ShardId) -> bool {
        self.shards.read().await.contains_key(&shard)
    }

    pub async fn shard(&self, shard: ShardId) -> Option<Arc<LogDb>> {
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
        let database = LogDb::open_with_slots(
            self.options.shard_storage(&self.config, shard)?,
            owned_slots.clone(),
        )
        .await?;
        let mut shards = self.shards.write().await;
        if shards.contains_key(&shard) {
            drop(shards);
            database.close().await?;
            return Ok(());
        }
        shards.insert(shard, Arc::new(database));
        self.shard_slots.write().await.insert(shard, owned_slots);
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
                    LogDb::open_reader_with_slots(
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
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut grouped: BTreeMap<ShardId, Vec<LogBatch>> = BTreeMap::new();
        for batch in batches {
            grouped
                .entry(self.route(routing, namespace, &batch.labels))
                .or_default()
                .push(batch);
        }
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
        self.shard_slots.write().await.clear();
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

    #[test]
    fn routing_is_canonical_stable_and_namespace_scoped() {
        let options = ShardingOptions::new(64, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let routing = HashRangeMap::bootstrap(options.virtual_shards()).unwrap();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let canonical = labels(&[("a", "2"), ("z", "1")]);
        let reordered = Labels::new(vec![Label::new("z", "1"), Label::new("a", "2")]).unwrap();
        assert_eq!(
            options.route(&routing, &a, &canonical),
            options.route(&routing, &a, &reordered)
        );
        assert_ne!(
            options.route(&routing, &a, &canonical),
            options.route(&routing, &b, &canonical)
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
        let routing = HashRangeMap::bootstrap(options.virtual_shards()).unwrap();
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
                .find(|labels| options.route(&routing, &namespace, labels).get() == shard)
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
    async fn reconciliation_adds_resizes_and_safely_removes_reader_shards() {
        let options = ShardingOptions::new(1, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
        let one = HashRangeMap::bootstrap(1).unwrap();
        let two = one.grow_to(2).unwrap();
        let database = ShardedLine::open_readers_with_routing(
            Config {
                storage: StorageConfig::InMemory,
                ..Config::default()
            },
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
}
