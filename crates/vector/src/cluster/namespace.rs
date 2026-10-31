//! An open namespace: a [`VectorDb`] over its shard's namespaced key range
//! on the shard's writer, or a periodically refreshed query engine over a
//! read-only shard on readers.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use common::StorageRead;

use super::ClusterConfig;
use super::directory;
use super::schema::{AttributeSchema, NamespaceMeta};
use super::shard::Shard;
use crate::Config;
use crate::db::VectorDb;
use crate::error::{Error, Result};
use crate::memory::MemoryBudget;
use crate::query_engine::{QueryEngine, QueryEngineOptions};
use crate::serde::vector_bitmap::VectorBitmap;
use crate::storage::VectorDbStorageReadExt;
use crate::storage::namespaced::NamespaceId;
use crate::write::indexer::tree::centroids::{
    LeveledCentroidIndex, StoredCentroidReader, TreeDepth,
};

pub(crate) struct Namespace {
    meta: RwLock<NamespaceMeta>,
    engine: Engine,
    /// Serializes writes, so schema inference, patches, and passage
    /// replacement see each other's effects.
    pub(crate) write_lock: tokio::sync::Mutex<()>,
}

enum Engine {
    Writer(Box<VectorDb>),
    Reader(ReadEngine),
}

impl Namespace {
    pub(crate) async fn open(
        meta: NamespaceMeta,
        shard: &Shard,
        config: &ClusterConfig,
        memory: MemoryBudget,
    ) -> Result<Self> {
        let id = NamespaceId::for_name(&meta.name);
        let engine = if shard.is_writable() {
            Engine::Writer(Box::new(
                VectorDb::open_namespace(
                    db_config(&meta, config),
                    shard.namespace_storage(id)?,
                    memory,
                )
                .await?,
            ))
        } else {
            Engine::Reader(ReadEngine {
                id,
                storage: shard.namespace_read(id),
                shard: shard.read(),
                refresh: config.reader_refresh_interval,
                pruning: config.index.query_pruning_factor,
                split_threshold_vectors: config.index.split_threshold_vectors,
                memory,
                current: tokio::sync::Mutex::new(None),
            })
        };
        Ok(Self {
            meta: RwLock::new(meta),
            engine,
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn meta(&self) -> NamespaceMeta {
        self.meta.read().expect("lock poisoned").clone()
    }

    /// The writable database; fails on readers.
    pub(crate) fn db(&self) -> Result<&VectorDb> {
        match &self.engine {
            Engine::Writer(db) => Ok(&**db),
            Engine::Reader(_) => Err(Error::Unavailable(format!(
                "namespace '{}' is read-only here",
                self.meta().name
            ))),
        }
    }

    /// A query engine over the namespace. `strong` first makes every
    /// acknowledged write visible, which only the writer can do.
    pub(crate) async fn engine(&self, strong: bool) -> Result<QueryEngine> {
        match &self.engine {
            Engine::Writer(db) => {
                if strong {
                    db.sync().await?;
                }
                Ok(db.query_engine())
            }
            Engine::Reader(reader) => {
                if strong {
                    return Err(Error::Unavailable(format!(
                        "strongly consistent reads of '{}' are served by its writer",
                        self.meta().name
                    )));
                }
                let (engine, meta) = reader.engine().await?;
                if let Some(meta) = meta {
                    *self.meta.write().expect("lock poisoned") = meta;
                }
                Ok(engine)
            }
        }
    }

    /// How many centroids the vector index has, if known.
    pub(crate) fn num_centroids(&self) -> usize {
        match &self.engine {
            Engine::Writer(db) => db.num_centroids(),
            Engine::Reader(_) => usize::MAX,
        }
    }

    /// Adds inferred attribute types, returning the updated metadata when
    /// anything changed.
    pub(crate) fn add_inferred(
        &self,
        inferred: &BTreeMap<String, AttributeSchema>,
    ) -> Option<NamespaceMeta> {
        let mut meta = self.meta.write().expect("lock poisoned");
        let mut changed = false;
        for (name, schema) in inferred {
            if !meta.schema.contains_key(name) {
                meta.schema.insert(name.clone(), schema.clone());
                changed = true;
            }
        }
        changed.then(|| meta.clone())
    }

    pub(crate) fn set_meta(&self, meta: NamespaceMeta) {
        *self.meta.write().expect("lock poisoned") = meta;
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        match &self.engine {
            Engine::Writer(db) => db.flush().await,
            Engine::Reader(_) => Ok(()),
        }
    }

    pub(crate) async fn close(self: Arc<Self>) -> Result<()> {
        match Arc::try_unwrap(self) {
            Ok(namespace) => match namespace.engine {
                Engine::Writer(db) => (*db).close().await,
                Engine::Reader(_) => Ok(()),
            },
            Err(namespace) => {
                namespace.flush().await?;
                Err(Error::Conflict(format!(
                    "namespace '{}' is still in use",
                    namespace.meta().name
                )))
            }
        }
    }
}

/// The read side of a namespace on a read-only shard. The writer publishes
/// a new centroid tree and deletions bitmap as it indexes, so the engine is
/// rebuilt from storage once it is older than `refresh`.
struct ReadEngine {
    id: NamespaceId,
    storage: Arc<dyn StorageRead>,
    shard: Arc<dyn StorageRead>,
    refresh: Duration,
    pruning: Option<f32>,
    split_threshold_vectors: usize,
    memory: MemoryBudget,
    current: tokio::sync::Mutex<Option<Loaded>>,
}

struct Loaded {
    at: Instant,
    options: QueryEngineOptions,
    centroids: Arc<LeveledCentroidIndex<'static>>,
    deletions: Arc<VectorBitmap>,
}

impl ReadEngine {
    /// The current engine, and the namespace's metadata when it was
    /// reloaded.
    async fn engine(&self) -> Result<(QueryEngine, Option<NamespaceMeta>)> {
        let mut current = self.current.lock().await;
        let mut reloaded = None;
        if current
            .as_ref()
            .is_none_or(|l| l.at.elapsed() >= self.refresh)
        {
            let meta = directory::get(self.shard.as_ref(), self.id)
                .await?
                .ok_or_else(|| {
                    Error::NotFound(format!("namespace {} not found", self.id.to_hex()))
                })?;
            *current = Some(self.load(&meta).await?);
            reloaded = Some(meta);
        }
        let loaded = current.as_ref().expect("loaded above");
        let engine = QueryEngine::new(
            loaded.options.clone(),
            loaded.centroids.clone(),
            self.storage.clone(),
            loaded.deletions.clone(),
        )
        .with_memory(self.memory.clone());
        Ok((engine, reloaded))
    }

    async fn load(&self, meta: &NamespaceMeta) -> Result<Loaded> {
        let options = QueryEngineOptions {
            dimensions: meta.dimensions,
            distance_metric: meta.distance_metric.distance_metric(),
            query_pruning_factor: self.pruning,
            split_threshold_vectors: self.split_threshold_vectors,
        };
        let depth = self
            .storage
            .get_centroids_meta()
            .await?
            .map(|m| m.depth)
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "namespace '{}' has no index on this reader yet",
                    meta.name
                ))
            })?;
        let reader = Arc::new(StoredCentroidReader::new(
            meta.dimensions as usize,
            self.storage.clone(),
            0,
        ));
        let centroids = Arc::new(LeveledCentroidIndex::new(
            TreeDepth::of(depth),
            options.distance_metric,
            reader,
        ));
        let deletions = Arc::new(self.storage.get_deletions().await?);
        Ok(Loaded {
            at: Instant::now(),
            options,
            centroids,
            deletions,
        })
    }
}

fn db_config(meta: &NamespaceMeta, config: &ClusterConfig) -> Config {
    Config {
        storage: config.storage.clone(),
        dimensions: meta.dimensions,
        distance_metric: meta.distance_metric.distance_metric(),
        flush_interval: config.flush_interval,
        split_threshold_vectors: config.index.split_threshold_vectors,
        merge_threshold_vectors: config.index.merge_threshold_vectors,
        split_search_neighbourhood: config.index.split_search_neighbourhood,
        query_pruning_factor: config.index.query_pruning_factor,
        metadata_fields: meta.text_fields(),
        delete_compaction_threshold: config.delete_compaction_threshold,
        fts_stats_poll_interval: config.fts_stats_poll_interval,
        index_all_attributes: true,
    }
}
