//! Shards: one database holding many namespaces, and how they split.
//!
//! A shard is a single SlateDB (or in-memory store) at
//! `{root}/shards/{id:06}`. Namespaces live in it under their namespace-id
//! key prefix (see [`namespaced`](crate::storage::namespaced)). A writer
//! opens a shard read-write; readers open it read-only and follow the
//! writer's manifest and WAL.
//!
//! A shard splits into two children with SlateDB projected clones: after
//! a checkpoint of the parent, each child is created as a zero-copy clone
//! whose per-segment projection keeps one contiguous namespace-id range.
//! The children reference the parent's SSTs, so the parent is retired, not
//! deleted.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::storage::config::SlateDbStorageConfig;
use common::storage::factory::{StorageReaderRuntime, create_storage_read};
use common::storage::slate::SlateDbStorage;
use common::storage::{Record, RecordOp};
use common::{
    BytesRange, SharedDbCache, Storage, StorageBuilder, StorageConfig, StorageRead,
    StorageSemantics,
};
use slatedb::admin::{AdminBuilder, CloneSourceSpec};
use slatedb::object_store::ObjectStore;
use tokio::task::JoinHandle;
use tracing::debug;
use uuid::Uuid;

use super::ClusterConfig;
use super::topology::NamespaceRange;
use crate::error::{Error, Result};
use crate::memory::{CompactionBudget, MemoryBudget};
use crate::storage::VectorDbStorageReadExt;
use crate::storage::compaction_scheduler::FtsDeleteTracker;
use crate::storage::merge_operator::VectorDbMergeOperator;
use crate::storage::namespaced::{
    NamespaceId, NamespacedRead, NamespacedStorage, namespace_range_in_segment, to_logical,
};
use crate::storage::segment_extractor::VectorSegmentExtractor;

/// Where shard databases live.
pub(crate) enum ShardBackend {
    /// Process-local stores, kept across shard close/reopen.
    InMemory {
        stores: Mutex<HashMap<u32, Arc<dyn Storage>>>,
    },
    SlateDb {
        root: Box<SlateDbStorageConfig>,
        object_store: Arc<dyn ObjectStore>,
        cache: SharedDbCache,
        compaction: CompactionBudget,
    },
}

impl ShardBackend {
    pub(crate) async fn new(storage: &StorageConfig, memory: &MemoryBudget) -> Result<Self> {
        Ok(match storage {
            StorageConfig::InMemory => ShardBackend::InMemory {
                stores: Mutex::new(HashMap::new()),
            },
            StorageConfig::SlateDb(root) => ShardBackend::SlateDb {
                root: Box::new(root.clone()),
                object_store: common::create_object_store(&root.object_store)?,
                cache: SharedDbCache::from_config(storage).await?,
                compaction: memory.compaction().clone(),
            },
        })
    }

    /// Opens the catalog database, at `{root}/catalog`.
    pub(crate) async fn open_catalog(&self) -> Result<Arc<dyn Storage>> {
        let builder = match self {
            ShardBackend::InMemory { .. } => StorageBuilder::new(&StorageConfig::InMemory).await?,
            ShardBackend::SlateDb { root, cache, .. } => StorageBuilder::with_cache(
                &StorageConfig::SlateDb(SlateDbStorageConfig {
                    path: format!("{}/catalog", root.path),
                    ..root.as_ref().clone()
                }),
                cache,
            )?,
        };
        Ok(builder.build().await?)
    }

    /// A checkpoint of shard `shard` to clone from; `None` for in-memory
    /// shards, which split by copying.
    pub(crate) async fn checkpoint(&self, shard: &Shard) -> Result<Option<Uuid>> {
        match self {
            ShardBackend::InMemory { .. } => Ok(None),
            ShardBackend::SlateDb { .. } => {
                Ok(Some(shard.writable()?.create_checkpoint().await?.id))
            }
        }
    }

    /// Forgets a split shard. In-memory shards are dropped; SlateDB shards
    /// stay, since their children's clones reference the parent's SSTs.
    pub(crate) fn retire(&self, id: u32) {
        if let ShardBackend::InMemory { stores } = self {
            stores.lock().expect("lock poisoned").remove(&id);
        }
    }

    fn shard_path(root: &SlateDbStorageConfig, id: u32) -> String {
        format!("{}/shards/{id:06}", root.path)
    }

    async fn open_storage(
        &self,
        id: u32,
        tracker: Arc<FtsDeleteTracker>,
        config: &ClusterConfig,
    ) -> Result<Arc<dyn Storage>> {
        match self {
            ShardBackend::InMemory { stores } => {
                if let Some(storage) = stores.lock().expect("lock poisoned").get(&id) {
                    return Ok(storage.clone());
                }
                let builder = crate::storage::new_storage_builder(&StorageConfig::InMemory).await?;
                let storage = crate::storage::build_shard_storage(
                    builder,
                    tracker,
                    config.delete_compaction_threshold,
                    None,
                )
                .await?;
                stores
                    .lock()
                    .expect("lock poisoned")
                    .insert(id, storage.clone());
                Ok(storage)
            }
            ShardBackend::SlateDb {
                root,
                cache,
                compaction,
                ..
            } => {
                let shard_config = StorageConfig::SlateDb(SlateDbStorageConfig {
                    path: Self::shard_path(root, id),
                    ..root.as_ref().clone()
                });
                let builder = crate::storage::new_storage_builder_with_cache(&shard_config, cache)?;
                crate::storage::build_shard_storage(
                    builder,
                    tracker,
                    config.delete_compaction_threshold,
                    Some(compaction.clone()),
                )
                .await
            }
        }
    }

    /// Opens shard `id` read-only, following its writer every
    /// `poll_interval`. In-memory shards can only be read in the process
    /// that wrote them.
    async fn open_read_storage(
        &self,
        id: u32,
        poll_interval: Duration,
    ) -> Result<Arc<dyn StorageRead>> {
        match self {
            ShardBackend::InMemory { stores } => {
                let store = stores.lock().expect("lock poisoned").get(&id).cloned();
                match store {
                    Some(storage) => Ok(storage),
                    None => Err(Error::Unavailable(format!(
                        "in-memory shard {id} is not open in this process"
                    ))),
                }
            }
            ShardBackend::SlateDb {
                root,
                object_store,
                cache,
                ..
            } => {
                let shard_config = StorageConfig::SlateDb(SlateDbStorageConfig {
                    path: Self::shard_path(root, id),
                    ..root.as_ref().clone()
                });
                let runtime = StorageReaderRuntime::new()
                    .with_shared_cache(cache)
                    .with_object_store(object_store.clone());
                let semantics = StorageSemantics::new()
                    .with_merge_operator(Arc::new(VectorDbMergeOperator::namespaced()))
                    .with_segment_extractor(VectorSegmentExtractor::shared());
                let options = slatedb::config::DbReaderOptions {
                    manifest_poll_interval: poll_interval,
                    ..Default::default()
                };
                Ok(create_storage_read(&shard_config, runtime, semantics, options).await?)
            }
        }
    }

    /// Creates shard `target` holding the namespaces in `range` of shard
    /// `source`. `checkpoint` is a checkpoint of the source taken after its
    /// last write.
    pub(crate) async fn create_split_child(
        &self,
        source: &Shard,
        checkpoint: Uuid,
        target: u32,
        range: NamespaceRange,
    ) -> Result<()> {
        let (lo, hi) = range.bounds();
        match self {
            ShardBackend::SlateDb {
                root, object_store, ..
            } => {
                let admin =
                    AdminBuilder::new(Self::shard_path(root, target), object_store.clone()).build();
                admin
                    .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
                        Self::shard_path(root, source.id),
                        checkpoint,
                    ))
                    .with_segment_projection(move |prefix: &[u8]| {
                        if prefix.is_empty() {
                            (Bound::Unbounded, Bound::Unbounded)
                        } else {
                            namespace_range_in_segment(prefix, lo, hi)
                        }
                    })
                    .build()
                    .await
                    .map_err(|e| {
                        Error::Storage(format!(
                            "clone shard {} into shard {target}: {e}",
                            source.id
                        ))
                    })
            }
            ShardBackend::InMemory { stores } => {
                // No clones in memory: copy the namespace range's records.
                let builder = crate::storage::new_storage_builder(&StorageConfig::InMemory).await?;
                let target_storage = crate::storage::build_shard_storage(
                    builder,
                    FtsDeleteTracker::shared(),
                    0.2,
                    None,
                )
                .await?;
                let mut iter = source.read().scan_iter(BytesRange::unbounded()).await?;
                let mut batch = Vec::new();
                while let Some(record) = iter.next().await? {
                    let Some((ns, _)) = to_logical(&record.key) else {
                        continue;
                    };
                    if !range.contains(ns) {
                        continue;
                    }
                    batch.push(RecordOp::Put(Record::new(record.key, record.value).into()));
                    if batch.len() >= 1024 {
                        target_storage.apply(std::mem::take(&mut batch)).await?;
                    }
                }
                if !batch.is_empty() {
                    target_storage.apply(batch).await?;
                }
                stores
                    .lock()
                    .expect("lock poisoned")
                    .insert(target, target_storage);
                Ok(())
            }
        }
    }

    pub(crate) async fn close(&self) -> Result<()> {
        if let ShardBackend::SlateDb { cache, .. } = self {
            cache.close().await?;
        }
        Ok(())
    }
}

enum ShardStorage {
    Write(Arc<dyn Storage>),
    Read(Arc<dyn StorageRead>),
}

/// An open shard.
pub(crate) struct Shard {
    pub(crate) id: u32,
    /// The namespace ids the shard serves.
    pub(crate) range: NamespaceRange,
    storage: ShardStorage,
    namespaces: Arc<Mutex<HashSet<NamespaceId>>>,
    poller: Option<JoinHandle<()>>,
}

impl Shard {
    /// Opens shard `id` read-write.
    pub(crate) async fn open(
        id: u32,
        range: NamespaceRange,
        backend: &ShardBackend,
        config: &ClusterConfig,
    ) -> Result<Self> {
        let tracker = FtsDeleteTracker::shared();
        let storage = backend.open_storage(id, tracker.clone(), config).await?;
        let ids = super::directory::list(storage.as_ref(), range)
            .await?
            .iter()
            .map(|meta| NamespaceId::for_name(&meta.name))
            .collect();
        let namespaces = Arc::new(Mutex::new(ids));
        let poller = spawn_field_stats_poller(
            storage.clone(),
            namespaces.clone(),
            tracker,
            config.fts_stats_poll_interval,
        );
        Ok(Self {
            id,
            range,
            storage: ShardStorage::Write(storage),
            namespaces,
            poller,
        })
    }

    /// Opens shard `id` read-only.
    pub(crate) async fn open_read(
        id: u32,
        range: NamespaceRange,
        backend: &ShardBackend,
        config: &ClusterConfig,
    ) -> Result<Self> {
        let storage = backend
            .open_read_storage(id, config.reader_refresh_interval)
            .await?;
        Ok(Self {
            id,
            range,
            storage: ShardStorage::Read(storage),
            namespaces: Arc::default(),
            poller: None,
        })
    }

    pub(crate) fn is_writable(&self) -> bool {
        matches!(self.storage, ShardStorage::Write(_))
    }

    /// The shard database, for reads.
    pub(crate) fn read(&self) -> Arc<dyn StorageRead> {
        match &self.storage {
            ShardStorage::Write(storage) => storage.clone(),
            ShardStorage::Read(storage) => storage.clone(),
        }
    }

    /// The shard database, for writes; fails on read-only shards.
    pub(crate) fn writable(&self) -> Result<&Arc<dyn Storage>> {
        match &self.storage {
            ShardStorage::Write(storage) => Ok(storage),
            ShardStorage::Read(_) => Err(Error::Unavailable(format!(
                "shard {} is open read-only here",
                self.id
            ))),
        }
    }

    /// A read-write storage view confined to namespace `ns`.
    pub(crate) fn namespace_storage(&self, ns: NamespaceId) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(NamespacedStorage::new(
            self.writable()?.clone(),
            ns,
        )))
    }

    /// A read-only storage view confined to namespace `ns`.
    pub(crate) fn namespace_read(&self, ns: NamespaceId) -> Arc<dyn StorageRead> {
        Arc::new(NamespacedRead::new(self.read(), ns))
    }

    pub(crate) fn add_namespace(&self, ns: NamespaceId) {
        self.namespaces.lock().expect("lock poisoned").insert(ns);
    }

    pub(crate) fn remove_namespace(&self, ns: NamespaceId) {
        self.namespaces.lock().expect("lock poisoned").remove(&ns);
    }

    /// Flushes (when writable) and closes the shard database. Namespaces
    /// must be closed first.
    pub(crate) async fn close(&self) -> Result<()> {
        if let Some(poller) = &self.poller {
            poller.abort();
        }
        match &self.storage {
            ShardStorage::Write(storage) => {
                storage.flush().await?;
                storage.close().await?;
            }
            ShardStorage::Read(storage) => storage.close().await?,
        }
        Ok(())
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        if let Some(poller) = &self.poller {
            poller.abort();
        }
    }
}

/// Feeds the shard's FTS compaction scheduler with the per-field delete
/// pressure of every namespace in the shard, keyed `"{ns id}/{field}"`.
/// Only SlateDB shards compact, so in-memory shards get no poller.
fn spawn_field_stats_poller(
    storage: Arc<dyn Storage>,
    namespaces: Arc<Mutex<HashSet<NamespaceId>>>,
    tracker: Arc<FtsDeleteTracker>,
    interval: std::time::Duration,
) -> Option<JoinHandle<()>> {
    let db = (storage.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SlateDbStorage>()?
        .db()
        .clone();
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let manifest_id = db.manifest().id();
            let ids: Vec<NamespaceId> = namespaces
                .lock()
                .expect("lock poisoned")
                .iter()
                .copied()
                .collect();
            let mut fields = HashMap::new();
            let mut failed = false;
            for ns in ids {
                let view = NamespacedStorage::new(storage.clone(), ns);
                match view.scan_field_stats().await {
                    Ok(stats) => {
                        for (field, value) in stats {
                            fields.insert(
                                format!("{}/{field}", ns.to_hex()),
                                (value.count, value.deletes),
                            );
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "shard FTS field-stats poll failed; retrying next tick");
                        failed = true;
                        break;
                    }
                }
            }
            if !failed {
                tracker.update_stats(manifest_id, fields);
            }
        }
    }))
}

/// Where a shard holding `range` and the namespaces `ids` splits: at the
/// median namespace id, so `[.., split)` and `[split, ..)` each get about
/// half, or with fewer than two namespaces, the middle of the range.
pub(crate) fn split_point(range: NamespaceRange, mut ids: Vec<NamespaceId>) -> u64 {
    if ids.len() < 2 {
        return range.midpoint();
    }
    ids.sort();
    ids[ids.len() / 2].as_u64()
}
