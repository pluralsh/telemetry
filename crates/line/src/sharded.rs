use std::collections::BTreeMap;
use std::sync::Arc;

use sharding::{DEFAULT_IO_CONCURRENCY_MULTIPLIER, DEFAULT_VIRTUAL_SHARDS, ShardId};
use tokio::sync::{RwLock, Semaphore};

use crate::query::query_databases;
use crate::{
    Config, Durability, Error, Labels, LogBatch, LogDb, Namespace, QueryOptions, QueryRequest,
    QueryResult, Result, WriteReport,
};

fn shard_io_semaphore(shards: usize, multiplier: u32) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(
        shards.max(1).saturating_mul(multiplier as usize),
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
            return Err(Error::Invalid(
                "virtual shard count must be greater than zero".to_owned(),
            ));
        }
        if io_concurrency_multiplier == 0 {
            return Err(Error::Invalid(
                "I/O concurrency multiplier must be greater than zero".to_owned(),
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

    pub fn route(self, namespace: &Namespace, labels: &Labels) -> ShardId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(namespace.as_bytes().len() as u32).to_be_bytes());
        hasher.update(namespace.as_bytes());
        for label in labels.iter() {
            hasher.update(&(label.name.len() as u32).to_be_bytes());
            hasher.update(label.name.as_bytes());
            hasher.update(&(label.value.len() as u32).to_be_bytes());
            hasher.update(label.value.as_bytes());
        }
        let hash = hasher.finalize();
        let value = u64::from_be_bytes(hash.as_bytes()[..8].try_into().expect("eight bytes"));
        ShardId::new((value % u64::from(self.virtual_shards)) as u32)
    }

    pub fn shard_storage(self, config: &Config, shard: ShardId) -> Result<Config> {
        if shard.get() >= self.virtual_shards {
            return Err(Error::Invalid(format!(
                "shard {} is outside configured virtual shard count {}",
                shard.get(),
                self.virtual_shards
            )));
        }
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
    io_permits: Arc<Semaphore>,
}

impl ShardedLine {
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
            io_permits: shard_io_semaphore(
                options.virtual_shards() as usize,
                options.io_concurrency_multiplier(),
            ),
            shards: RwLock::new(databases),
        })
    }

    pub fn route(&self, namespace: &Namespace, labels: &Labels) -> ShardId {
        self.options.route(namespace, labels)
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

    pub async fn write(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut grouped: BTreeMap<ShardId, Vec<LogBatch>> = BTreeMap::new();
        for batch in batches {
            grouped
                .entry(self.route(namespace, &batch.labels))
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
        for database in databases {
            database.close().await?;
        }
        Ok(())
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

    #[test]
    fn routing_is_canonical_stable_and_namespace_scoped() {
        let options = ShardingOptions::new(64, DEFAULT_IO_CONCURRENCY_MULTIPLIER).unwrap();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();
        let canonical = labels(&[("a", "2"), ("z", "1")]);
        let reordered = Labels::new(vec![Label::new("z", "1"), Label::new("a", "2")]).unwrap();
        assert_eq!(options.route(&a, &canonical), options.route(&a, &reordered));
        assert_ne!(options.route(&a, &canonical), options.route(&b, &canonical));
        assert_eq!(options.route(&a, &canonical).get(), 39);
    }

    #[test]
    fn shard_io_budget_uses_configured_multiplier() {
        assert_eq!(shard_io_semaphore(8, 4).available_permits(), 32);
        assert_eq!(shard_io_semaphore(8, 2).available_permits(), 16);
        assert_eq!(shard_io_semaphore(0, 4).available_permits(), 4);
    }

    #[tokio::test]
    async fn writes_and_queries_across_shards_with_global_limits() {
        let options = ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_MULTIPLIER).unwrap();
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
                .find(|labels| options.route(&namespace, labels).get() == shard)
                .unwrap();
            batches.push(LogBatch::new(
                labels,
                vec![LogEntry::new(i64::from(shard) + 1, format!("line-{shard}"))],
            ));
        }
        database
            .write(&namespace, batches, Durability::Written)
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
            ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_MULTIPLIER).unwrap(),
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
}
