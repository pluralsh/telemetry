//! A multi-namespace, sharded vector database.
//!
//! A [`VectorCluster`] holds any number of *namespaces*, each an
//! independent collection of rows (id, vector, attributes) with its own
//! dimensions, distance metric, schema, and index. Namespaces live in
//! *shards*, each one SlateDB database holding its namespaces under disjoint
//! key prefixes.
//!
//! A namespace's shard follows from its name: the name hashes to a 64-bit
//! namespace id, and each active shard holds one contiguous range of ids
//! (see [`topology`]). Each namespace's metadata is stored inside its shard,
//! so nothing else tracks where namespaces are.
//!
//! Every writer owns one shard. When a writer joins, the topology plans a
//! split of an existing shard for it, and that shard's owner splits its
//! range at the median namespace id with SlateDB projected clones: no data
//! is copied, each namespace moves whole to one of the two children, and the
//! new writer takes the upper child.
//!
//! # Roles
//!
//! - [`Role::Standalone`]: one process serves the one shard, keeping the
//!   topology in a catalog database next to it.
//! - [`Role::Writer`]: one of several writer replicas. The topology names an
//!   owner for each shard; the owner opens it read-write while it holds the
//!   shard's lease, and splits it when the topology plans a split.
//! - [`Role::Reader`]: opens shards read-only (one, by default) and serves
//!   eventually consistent queries from them, following the writer.
//!
//! [`VectorCluster::place`] says whether this process can run an operation;
//! when it can't, the HTTP layer forwards it to the process that can.

pub mod embedding;
pub mod query;
pub mod schema;
pub mod store;
pub mod topology;
pub mod write;

#[cfg(feature = "kubernetes")]
pub mod kubernetes;

mod directory;
mod leases;
mod namespace;
mod shard;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::StorageConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub use embedding::{Embedder, EmbeddingConfig, EmbeddingProvider, InputKind};
pub use leases::{MemoryLeases, NoLeases, ShardLeases};
pub use query::{MultiQueryRequest, MultiQueryResponse, QueryRequest, QueryResponse};
pub use schema::{AttributeSchema, AttributeType, Metric, NamespaceMeta};
pub use store::{MemoryTopologyStore, TopologyStore};
pub use topology::{HexId, NamespaceRange, ShardMeta, ShardState, Topology};
pub use write::{DocumentInput, WriteRequest, WriteResponse};

use namespace::Namespace;
use shard::{Shard, ShardBackend, split_point};
use store::CatalogTopologyStore;

use crate::error::{Error, Result};
use crate::memory::{MemoryBudget, MemoryConfig};
use crate::model::{default_delete_compaction_threshold, default_fts_stats_poll_interval};
use crate::storage::namespaced::{NamespaceId, delete_namespace_keys};

/// Longest namespace name.
pub const MAX_NAMESPACE_NAME_LEN: usize = 128;

/// The identity of a standalone process, and owner of all its shards.
pub const STANDALONE_IDENTITY: &str = "standalone";

/// The part a process plays in the cluster. See the module docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Standalone,
    Writer,
    Reader,
}

/// Configuration of a [`VectorCluster`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Where the catalog (`{path}/catalog`) and shards
    /// (`{path}/shards/{id}`) live.
    pub storage: StorageConfig,
    #[serde(default)]
    pub role: Role,
    /// Readers: the one shard to serve. Without it a reader serves every
    /// active shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reader_shard: Option<u32>,
    /// How often readers pick up the writer's changes, in seconds.
    #[serde(
        default = "default_reader_refresh_interval",
        with = "crate::model::duration_secs"
    )]
    pub reader_refresh_interval: Duration,
    /// How often a writer or reader renews its shard leases and checks
    /// that it serves the shards the topology gives it, in seconds.
    #[serde(
        default = "default_reconcile_interval",
        with = "crate::model::duration_secs"
    )]
    pub reconcile_interval: Duration,
    /// How each namespace flushes buffered writes, in seconds.
    #[serde(
        default = "default_flush_interval",
        with = "crate::model::duration_secs"
    )]
    pub flush_interval: Duration,
    #[serde(default)]
    pub index: IndexConfig,
    /// See [`Config::delete_compaction_threshold`](crate::Config).
    #[serde(default = "default_delete_compaction_threshold")]
    pub delete_compaction_threshold: f64,
    /// See [`Config::fts_stats_poll_interval`](crate::Config).
    #[serde(
        default = "default_fts_stats_poll_interval",
        with = "crate::model::duration_secs"
    )]
    pub fts_stats_poll_interval: Duration,
    /// Where writer and reader replicas run, for the writer and reader
    /// roles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubernetesSettings>,
    /// The model that embeds namespaces' `embed` attributes and query
    /// texts. Every writer and reader needs the same one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<EmbeddingConfig>,
    /// The process's query, write and compaction memory budgets; see
    /// [`memory`](crate::memory).
    #[serde(default)]
    pub memory: MemoryConfig,
}

/// Vector index tuning shared by every namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexConfig {
    pub split_threshold_vectors: usize,
    pub merge_threshold_vectors: usize,
    pub split_search_neighbourhood: usize,
    pub query_pruning_factor: Option<f32>,
}

impl Default for IndexConfig {
    fn default() -> Self {
        let config = crate::Config::default();
        Self {
            split_threshold_vectors: config.split_threshold_vectors,
            merge_threshold_vectors: config.merge_threshold_vectors,
            split_search_neighbourhood: config.split_search_neighbourhood,
            query_pruning_factor: config.query_pruning_factor,
        }
    }
}

/// The Kubernetes objects of a cluster named `name`. Resource names derive
/// from it the way the operator creates them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesSettings {
    /// Name of the `Vector` resource.
    pub name: String,
    /// Kubernetes namespace of the cluster; `POD_NAMESPACE` overrides it.
    #[serde(default = "default_kubernetes_namespace")]
    pub namespace: String,
    /// HTTP port of every replica.
    #[serde(default = "default_http_port")]
    pub port: u16,
    /// Lifetime of shard and coordinator leases, in seconds.
    #[serde(
        default = "default_lease_duration",
        with = "crate::model::duration_secs"
    )]
    pub lease_duration: Duration,
    /// Whether readers run as one group per active shard, each behind its
    /// own headless Service, rather than one group serving every shard.
    #[serde(default = "default_reader_groups")]
    pub reader_groups: bool,
}

impl KubernetesSettings {
    /// StatefulSet of the writer replicas.
    pub fn writer_stateful_set(&self) -> String {
        format!("{}-writer", self.name)
    }

    /// Headless Service giving each writer replica a DNS name.
    pub fn writer_headless_service(&self) -> String {
        format!("{}-writer-headless", self.name)
    }

    /// Headless Service of the readers serving `shard`, which resolves to
    /// their ready replicas.
    pub fn reader_service(&self, shard: u32) -> String {
        if self.reader_groups {
            format!("{}-reader-{shard}-headless", self.name)
        } else {
            format!("{}-reader-headless", self.name)
        }
    }

    /// The `VectorTopology` resource.
    pub fn topology_resource(&self) -> String {
        format!("{}-topology", self.name)
    }

    /// Address of writer replica `owner`.
    pub fn writer_address(&self, owner: &str) -> String {
        format!(
            "{owner}.{}.{}.svc:{}",
            self.writer_headless_service(),
            self.namespace,
            self.port
        )
    }

    /// Address of the readers serving `shard`.
    pub fn reader_address(&self, shard: u32) -> String {
        format!(
            "{}.{}.svc:{}",
            self.reader_service(shard),
            self.namespace,
            self.port
        )
    }
}

fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_reader_refresh_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_reconcile_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_kubernetes_namespace() -> String {
    "default".into()
}

fn default_http_port() -> u16 {
    8080
}

fn default_reader_groups() -> bool {
    true
}

fn default_lease_duration() -> Duration {
    Duration::from_secs(15)
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::InMemory,
            role: Role::default(),
            reader_shard: None,
            reader_refresh_interval: default_reader_refresh_interval(),
            reconcile_interval: default_reconcile_interval(),
            flush_interval: default_flush_interval(),
            index: IndexConfig::default(),
            delete_compaction_threshold: default_delete_compaction_threshold(),
            fts_stats_poll_interval: default_fts_stats_poll_interval(),
            kubernetes: None,
            embedding: None,
            memory: MemoryConfig::default(),
        }
    }
}

/// What a writer or reader process shares with the rest of the cluster.
pub struct ClusterRuntime {
    /// This process's name in the topology: a writer's owner name.
    pub identity: String,
    pub topology: Arc<dyn TopologyStore>,
    pub leases: Arc<dyn ShardLeases>,
}

/// What a namespace operation needs from the process running it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// The shard's writer.
    Write,
    /// Every acknowledged write: the shard's writer.
    StrongRead,
    /// Any open copy of the shard, writer or reader.
    EventualRead,
}

/// Where a namespace operation can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    Local,
    /// Elsewhere: on the shard's owner, or for eventual reads, its readers.
    Remote {
        shard: u32,
        owner: Option<String>,
    },
}

/// Body of a namespace creation.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// Length of the namespace's vectors. Optional when an attribute
    /// embeds: the embedding model's then.
    #[serde(default)]
    pub dimensions: Option<u16>,
    #[serde(default)]
    pub distance_metric: Metric,
    #[serde(default)]
    pub schema: BTreeMap<String, AttributeSchema>,
}

/// A shard and the namespaces it holds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    #[serde(flatten)]
    pub meta: ShardMeta,
    pub namespaces: Vec<String>,
}

#[derive(Default)]
struct State {
    shards: HashMap<u32, Arc<Shard>>,
    namespaces: HashMap<String, Arc<Namespace>>,
}

impl State {
    /// Closes the open namespaces of shard `id`.
    async fn close_namespaces(&mut self, shard: &Shard) -> Result<()> {
        let names: Vec<String> = self
            .namespaces
            .keys()
            .filter(|name| shard.range.contains(NamespaceId::for_name(name)))
            .cloned()
            .collect();
        for name in names {
            if let Some(namespace) = self.namespaces.remove(&name) {
                namespace.close().await?;
            }
        }
        Ok(())
    }
}

/// A sharded, multi-namespace vector database. See the module docs.
pub struct VectorCluster {
    config: ClusterConfig,
    identity: String,
    backend: ShardBackend,
    /// Every shard and namespace of the process draws on it.
    memory: MemoryBudget,
    store: Arc<dyn TopologyStore>,
    leases: Arc<dyn ShardLeases>,
    topology: watch::Receiver<Option<Topology>>,
    /// Held shared by namespace operations and exclusively by changes that
    /// close and reopen namespace or shard databases (shard moves, splits,
    /// deletes, full-text schema changes).
    lock: tokio::sync::RwLock<()>,
    state: tokio::sync::Mutex<State>,
    cancel: CancellationToken,
    embedder: Option<Arc<Embedder>>,
    declared: std::sync::RwLock<Arc<DeclaredSchemas>>,
    declared_changed: Arc<tokio::sync::Notify>,
}

/// Attribute schemas declared for namespaces in the server config, by
/// namespace name.
pub type DeclaredSchemas = BTreeMap<String, BTreeMap<String, AttributeSchema>>;

impl VectorCluster {
    /// Opens a standalone cluster, creating its shard on first open.
    pub async fn open(config: ClusterConfig) -> Result<Self> {
        if config.role != Role::Standalone {
            return Err(Error::InvalidInput(format!(
                "the {:?} role needs a cluster runtime; see VectorCluster::open_with",
                config.role
            )));
        }
        let memory = MemoryBudget::new(&config.memory);
        let backend = ShardBackend::new(&config.storage, &memory).await?;
        let store = Arc::new(CatalogTopologyStore::open(backend.open_catalog().await?).await?);
        let runtime = ClusterRuntime {
            identity: STANDALONE_IDENTITY.into(),
            topology: store,
            leases: Arc::new(NoLeases),
        };
        Self::open_parts(config, backend, memory, runtime).await
    }

    /// Opens a writer or reader replica. It serves nothing until the
    /// topology exists and gives it shards; call
    /// [`Self::spawn_reconciler`] to follow topology changes.
    pub async fn open_with(config: ClusterConfig, runtime: ClusterRuntime) -> Result<Self> {
        let memory = MemoryBudget::new(&config.memory);
        let backend = ShardBackend::new(&config.storage, &memory).await?;
        Self::open_parts(config, backend, memory, runtime).await
    }

    async fn open_parts(
        config: ClusterConfig,
        backend: ShardBackend,
        memory: MemoryBudget,
        runtime: ClusterRuntime,
    ) -> Result<Self> {
        if config.role == Role::Standalone && runtime.topology.load().await?.is_none() {
            let initial = Topology::initial(std::slice::from_ref(&runtime.identity), now_ms());
            runtime.topology.publish(None, &initial).await?;
        }
        let embedder = match &config.embedding {
            Some(embedding) => Some(Arc::new(Embedder::new(embedding)?)),
            None => None,
        };
        let cluster = Self {
            embedder,
            identity: runtime.identity,
            topology: runtime.topology.watch(),
            store: runtime.topology,
            leases: runtime.leases,
            config,
            backend,
            memory,
            lock: tokio::sync::RwLock::new(()),
            state: tokio::sync::Mutex::new(State::default()),
            cancel: CancellationToken::new(),
            declared: std::sync::RwLock::new(Arc::default()),
            declared_changed: Arc::default(),
        };
        cluster.reconcile().await?;
        Ok(cluster)
    }

    /// Embeds with `embedder` instead of the configured model.
    pub fn with_embedder(mut self, embedder: Arc<Embedder>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// The model embedding `embed` attributes and query texts, if any.
    pub fn embedder(&self) -> Option<&Arc<Embedder>> {
        self.embedder.as_ref()
    }

    pub fn role(&self) -> Role {
        self.config.role
    }

    /// Whether a query asking for `consistency` is strong. Strong queries
    /// sync the namespace's writer first, so only a standalone process,
    /// which writes every namespace, serves them (and by default).
    fn is_strong(&self, consistency: Option<query::Consistency>) -> Result<bool> {
        let standalone = self.config.role == Role::Standalone;
        match consistency.map(|c| c.level) {
            None => Ok(standalone),
            Some(query::ConsistencyLevel::Eventual) => Ok(false),
            Some(query::ConsistencyLevel::Strong) if standalone => Ok(true),
            Some(query::ConsistencyLevel::Strong) => Err(Error::InvalidInput(
                "strong consistency is only available in standalone deployments; \
                 sharded clusters serve eventually consistent queries"
                    .into(),
            )),
        }
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// The latest topology seen.
    pub fn topology(&self) -> Option<Topology> {
        self.topology.borrow().clone()
    }

    fn current(&self) -> Result<Topology> {
        self.topology()
            .ok_or_else(|| Error::Unavailable("the cluster topology is not published yet".into()))
    }

    /// Whether this process should have `shard` open.
    fn serves(&self, shard: &ShardMeta) -> bool {
        shard.state == ShardState::Active
            && match self.config.role {
                Role::Standalone => true,
                Role::Writer => shard.owner.as_deref() == Some(self.identity.as_str()),
                Role::Reader => self.config.reader_shard.is_none_or(|id| id == shard.id),
            }
    }

    /// Where an operation on namespace `name` needing `need` can run.
    pub async fn place(&self, name: &str, need: Need) -> Result<Placement> {
        let topology = self.current()?;
        let shard = topology
            .shard_for(NamespaceId::for_name(name))
            .ok_or_else(|| Error::Internal(format!("no shard holds namespace '{name}'")))?;
        Ok(self.place_shard(shard, need).await)
    }

    /// Where an operation on shard `shard` needing `need` can run.
    pub async fn place_shard(&self, shard: &ShardMeta, need: Need) -> Placement {
        let state = self.state.lock().await;
        let local = match (state.shards.get(&shard.id), need) {
            (Some(open), Need::Write | Need::StrongRead) => open.is_writable(),
            (Some(_), Need::EventualRead) => true,
            (None, _) => false,
        };
        if local {
            Placement::Local
        } else {
            Placement::Remote {
                shard: shard.id,
                owner: shard.owner.clone(),
            }
        }
    }

    /// Brings the open shards in line with the topology: renews the leases
    /// of open shards, closes shards this process no longer serves (or whose
    /// lease it lost), opens the shards it should serve, and splits the
    /// shard it owns when the topology plans a split of it.
    pub async fn reconcile(&self) -> Result<()> {
        self.reconcile_shards().await?;
        self.split_planned().await
    }

    async fn reconcile_shards(&self) -> Result<()> {
        let Some(topology) = self.topology() else {
            return Ok(());
        };
        let desired: BTreeMap<u32, NamespaceRange> = topology
            .active()
            .filter(|s| self.serves(s))
            .map(|s| (s.id, s.range))
            .collect();
        let open: Vec<u32> = self.state.lock().await.shards.keys().copied().collect();
        let writer = self.config.role == Role::Writer;

        let mut lost = Vec::new();
        if writer {
            for id in &open {
                match self.leases.renew(&self.identity, *id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(shard = id, "lost the shard lease; closing the shard");
                        lost.push(*id);
                    }
                    Err(e) => tracing::warn!(shard = id, error = %e, "shard lease renewal failed"),
                }
            }
        }
        let to_close: Vec<u32> = open
            .iter()
            .copied()
            .filter(|id| !desired.contains_key(id) || lost.contains(id))
            .collect();
        let mut to_open = Vec::new();
        for (id, range) in &desired {
            if open.contains(id) {
                continue;
            }
            if writer && !self.leases.acquire(&self.identity, *id).await? {
                tracing::debug!(shard = id, "shard lease is held elsewhere; retrying later");
                continue;
            }
            to_open.push((*id, *range));
        }
        if to_close.is_empty() && to_open.is_empty() {
            return Ok(());
        }

        let _exclusive = self.lock.write().await;
        let mut state = self.state.lock().await;
        // A split may have finished while waiting for the lock.
        let topology = self.current()?;
        let still_desired = |id: u32| topology.shard(id).is_some_and(|s| self.serves(s));
        for id in to_close {
            if still_desired(id) && !lost.contains(&id) {
                continue;
            }
            let Some(shard) = state.shards.remove(&id) else {
                continue;
            };
            if let Err(e) = state.close_namespaces(&shard).await {
                tracing::warn!(shard = id, error = %e, "closing the shard's namespaces failed");
            }
            if let Err(e) = shard.close().await {
                tracing::warn!(shard = id, error = %e, "closing the shard failed");
            }
            if writer && !lost.contains(&id) {
                self.leases.release(&self.identity, id).await?;
            }
            if topology
                .shard(id)
                .is_some_and(|s| s.state == ShardState::Retired)
            {
                self.backend.retire(id);
            }
            tracing::info!(shard = id, "closed shard");
        }
        for (id, range) in to_open {
            if state.shards.contains_key(&id) {
                continue;
            }
            if !still_desired(id) {
                if writer {
                    self.leases.release(&self.identity, id).await?;
                }
                continue;
            }
            match self.open_shard(id, range).await {
                Ok(shard) => {
                    state.shards.insert(id, Arc::new(shard));
                    tracing::info!(shard = id, role = ?self.config.role, "opened shard");
                }
                Err(e) => {
                    tracing::warn!(shard = id, error = %e, "opening the shard failed; retrying later");
                    if writer {
                        self.leases.release(&self.identity, id).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn open_shard(&self, id: u32, range: NamespaceRange) -> Result<Shard> {
        match self.config.role {
            Role::Reader => Shard::open_read(id, range, &self.backend, &self.config).await,
            Role::Standalone | Role::Writer => {
                Shard::open(id, range, &self.backend, &self.config).await
            }
        }
    }

    /// Runs [`Self::reconcile`] on every topology change and every
    /// `reconcile_interval`, until the cluster closes.
    pub fn spawn_reconciler(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let cluster = Arc::downgrade(self);
        let mut changes = self.topology.clone();
        let interval = self.config.reconcile_interval;
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            loop {
                {
                    let Some(cluster) = cluster.upgrade() else {
                        return;
                    };
                    if let Err(e) = cluster.reconcile().await {
                        tracing::warn!(error = %e, "shard reconciliation failed");
                    }
                }
                tokio::select! {
                    () = cancel.cancelled() => return,
                    changed = changes.changed() => {
                        if changed.is_err() {
                            tokio::time::sleep(interval).await;
                        }
                    }
                    () = tokio::time::sleep(interval) => {}
                }
            }
        })
    }

    /// Replaces the declared schemas. Namespaces created from then on start
    /// with theirs, and [`Self::apply_declared_schemas`] merges them into
    /// existing namespaces.
    pub fn declare_schemas(&self, schemas: DeclaredSchemas) {
        let mut declared = self.declared.write().expect("lock poisoned");
        if **declared != schemas {
            *declared = Arc::new(schemas);
            self.declared_changed.notify_one();
        }
    }

    fn declared_schema(&self, name: &str) -> BTreeMap<String, AttributeSchema> {
        let declared = self.declared.read().expect("lock poisoned");
        declared.get(name).cloned().unwrap_or_default()
    }

    /// Merges each declared schema into its namespace, if the namespace
    /// exists and this process writes its shard, and returns the namespaces
    /// whose schema can't take it. A namespace whose schema already holds
    /// every declared entry is left alone.
    pub async fn apply_declared_schemas(&self) -> Vec<(String, Error)> {
        let declared = Arc::clone(&self.declared.read().expect("lock poisoned"));
        let mut failed = Vec::new();
        for (name, schema) in declared.iter() {
            let Ok((shard, _)) = self.local_shard(name, true).await else {
                continue;
            };
            let current = match Self::stored_meta(&shard, name).await {
                Ok(Some(meta)) => meta,
                Ok(None) => continue,
                Err(e) => {
                    failed.push((name.clone(), e));
                    continue;
                }
            };
            let mut merged = current.clone();
            if let Err(e) = merged.merge_schema(schema) {
                failed.push((name.clone(), e));
                continue;
            }
            if merged.schema == current.schema {
                continue;
            }
            match self.update_schema(name, schema).await {
                Ok(_) => tracing::info!(namespace = %name, "applied the declared schema"),
                Err(e) => failed.push((name.clone(), e)),
            }
        }
        failed
    }

    /// Runs [`Self::apply_declared_schemas`] whenever the declared schemas
    /// change and every `reconcile_interval`, until the cluster closes.
    /// Each failure is logged once until its error changes.
    pub fn spawn_schema_applier(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let cluster = Arc::downgrade(self);
        let changed = Arc::clone(&self.declared_changed);
        let interval = self.config.reconcile_interval;
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut logged: HashMap<String, String> = HashMap::new();
            loop {
                {
                    let Some(cluster) = cluster.upgrade() else {
                        return;
                    };
                    let failed = cluster.apply_declared_schemas().await;
                    logged.retain(|name, _| failed.iter().any(|(n, _)| n == name));
                    for (name, error) in failed {
                        let error = error.to_string();
                        if logged.get(&name) != Some(&error) {
                            tracing::warn!(namespace = %name, %error, "the declared schema can't be applied");
                            logged.insert(name, error);
                        }
                    }
                }
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = changed.notified() => {}
                    () = tokio::time::sleep(interval) => {}
                }
            }
        })
    }

    /// The open shard holding namespace `name`, and the namespace's id.
    async fn local_shard(&self, name: &str, writable: bool) -> Result<(Arc<Shard>, NamespaceId)> {
        let id = NamespaceId::for_name(name);
        let topology = self.current()?;
        let meta = topology
            .shard_for(id)
            .ok_or_else(|| Error::Internal(format!("no shard holds namespace '{name}'")))?;
        let shard = self
            .state
            .lock()
            .await
            .shards
            .get(&meta.id)
            .cloned()
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "shard {} of namespace '{name}' is not open here",
                    meta.id
                ))
            })?;
        if writable && !shard.is_writable() {
            return Err(Error::Unavailable(format!(
                "shard {} of namespace '{name}' is read-only here",
                meta.id
            )));
        }
        Ok((shard, id))
    }

    /// The stored metadata of `name`, from its shard.
    async fn stored_meta(shard: &Shard, name: &str) -> Result<Option<NamespaceMeta>> {
        let meta = directory::get(shard.read().as_ref(), NamespaceId::for_name(name)).await?;
        Ok(meta.filter(|m| m.name == name).map(|mut m| {
            m.shard = shard.id;
            m
        }))
    }

    /// The namespaces of every shard open here, by name.
    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceMeta>> {
        let shards: Vec<u32> = self.state.lock().await.shards.keys().copied().collect();
        let mut namespaces = Vec::new();
        for id in shards {
            namespaces.extend(self.shard_namespaces(id).await?);
        }
        namespaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(namespaces)
    }

    /// The namespaces of shard `id`, which must be open here.
    pub async fn shard_namespaces(&self, id: u32) -> Result<Vec<NamespaceMeta>> {
        let shard = self
            .state
            .lock()
            .await
            .shards
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Unavailable(format!("shard {id} is not open here")))?;
        let mut namespaces = directory::list(shard.read().as_ref(), shard.range).await?;
        for meta in &mut namespaces {
            meta.shard = id;
        }
        Ok(namespaces)
    }

    pub async fn namespace(&self, name: &str) -> Result<NamespaceMeta> {
        let (shard, _) = self.local_shard(name, false).await?;
        Self::stored_meta(&shard, name)
            .await?
            .ok_or_else(|| not_found(name))
    }

    /// Creates a namespace in the shard holding its id.
    pub async fn create_namespace(
        &self,
        name: &str,
        request: CreateNamespaceRequest,
    ) -> Result<NamespaceMeta> {
        validate_namespace_name(name)?;
        let declared = self.declared_schema(name);
        let embeds = request
            .schema
            .values()
            .chain(declared.values())
            .any(|s| s.embed);
        let dimensions = match request.dimensions {
            Some(0) => return Err(Error::InvalidInput("dimensions must be positive".into())),
            Some(dimensions) => dimensions,
            None => match (&self.embedder, embeds) {
                (Some(embedder), true) => embedder.dimensions().await?,
                _ => {
                    return Err(Error::InvalidInput(
                        "dimensions is required unless an attribute embeds".into(),
                    ));
                }
            },
        };
        self.probe_embedder(&request.schema).await?;
        self.probe_embedder(&declared).await?;
        let _shared = self.lock.read().await;
        let (shard, id) = self.local_shard(name, true).await?;
        // Serializes creates with each other.
        let _state = self.state.lock().await;
        if let Some(existing) = directory::get(shard.read().as_ref(), id).await? {
            return Err(Error::Conflict(if existing.name == name {
                format!("namespace '{name}' already exists")
            } else {
                format!(
                    "namespace '{name}' hashes to the id of '{}'; choose another name",
                    existing.name
                )
            }));
        }
        let mut meta = NamespaceMeta {
            name: name.to_string(),
            id: id.to_hex(),
            shard: shard.id,
            dimensions,
            distance_metric: request.distance_metric,
            schema: BTreeMap::new(),
            embedding_model: None,
            created_at_ms: now_ms(),
        };
        meta.merge_schema(&declared)?;
        meta.merge_schema(&request.schema)?;
        embedding::pin(&mut meta, self.embedder.as_deref()).await?;
        directory::put(shard.writable()?, &meta).await?;
        shard.add_namespace(id);
        Ok(meta)
    }

    /// Adds or updates attribute schema entries. Types can't change;
    /// toggling `full_text_search` affects rows written afterwards.
    pub async fn update_schema(
        &self,
        name: &str,
        updates: &BTreeMap<String, AttributeSchema>,
    ) -> Result<NamespaceMeta> {
        self.probe_embedder(updates).await?;
        let _exclusive = self.lock.write().await;
        let (shard, _) = self.local_shard(name, true).await?;
        let mut state = self.state.lock().await;
        let current = match state.namespaces.get(name) {
            Some(namespace) => namespace.meta(),
            None => Self::stored_meta(&shard, name)
                .await?
                .ok_or_else(|| not_found(name))?,
        };
        let mut updated = current.clone();
        let text_changed = updated.merge_schema(updates)?;
        embedding::pin(&mut updated, self.embedder.as_deref()).await?;
        if updated == current {
            return Ok(current);
        }
        directory::put(shard.writable()?, &updated).await?;
        if text_changed {
            // Full-text fields are fixed when a namespace database opens.
            if let Some(namespace) = state.namespaces.remove(name) {
                namespace.close().await?;
            }
        } else if let Some(namespace) = state.namespaces.get(name) {
            namespace.set_meta(updated.clone());
        }
        Ok(updated)
    }

    /// Learns the embedding model's dimensions before a schema that embeds
    /// is applied, so no provider request runs under the cluster locks.
    async fn probe_embedder(&self, schema: &BTreeMap<String, AttributeSchema>) -> Result<()> {
        if let Some(embedder) = &self.embedder
            && schema.values().any(|s| s.embed)
        {
            embedder.dimensions().await?;
        }
        Ok(())
    }

    /// Deletes a namespace and all of its rows.
    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        let _exclusive = self.lock.write().await;
        let (shard, id) = self.local_shard(name, true).await?;
        let mut state = self.state.lock().await;
        if Self::stored_meta(&shard, name).await?.is_none() {
            return Err(not_found(name));
        }
        if let Some(namespace) = state.namespaces.remove(name) {
            namespace.close().await?;
        }
        let storage = shard.writable()?;
        delete_namespace_keys(storage, id).await?;
        storage.flush().await?;
        shard.remove_namespace(id);
        Ok(())
    }

    /// Applies a write, creating the namespace if it doesn't exist and the
    /// write carries a vector or a schema with an attribute to embed.
    pub async fn write(&self, name: &str, mut request: WriteRequest) -> Result<WriteResponse> {
        let schema = request.schema.take();
        let (shard, _) = self.local_shard(name, true).await?;
        if Self::stored_meta(&shard, name).await?.is_none() {
            let embeds = schema.as_ref().is_some_and(|s| s.values().any(|a| a.embed))
                || self.declared_schema(name).values().any(|a| a.embed);
            let dimensions = match request.first_dimensions() {
                Some(dimensions) => Some(u16::try_from(dimensions).map_err(|_| {
                    Error::InvalidInput(format!("{dimensions} dimensions is too many"))
                })?),
                None if embeds => None,
                None => return Err(not_found(name)),
            };
            let create = CreateNamespaceRequest {
                dimensions,
                distance_metric: request.distance_metric.unwrap_or_default(),
                schema: schema.clone().unwrap_or_default(),
            };
            match self.create_namespace(name, create).await {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e),
            }
        }
        if let Some(schema) = &schema {
            self.update_schema(name, schema).await?;
        }

        let _shared = self.lock.read().await;
        let (shard, namespace) = self.open_namespace(name, true).await?;
        if let Some(metric) = request.distance_metric
            && metric != namespace.meta().distance_metric
        {
            return Err(Error::InvalidInput(format!(
                "namespace '{name}' uses {:?}, not {metric:?}",
                namespace.meta().distance_metric
            )));
        }
        write::prefetch_embeddings(&request, &namespace.meta(), self.embedder.as_deref()).await?;
        let _write = namespace.write_lock.lock().await;
        let (response, inferred) =
            write::write(&namespace, request, self.embedder.as_deref()).await?;
        if let Some(meta) = namespace.add_inferred(&inferred) {
            directory::put(shard.writable()?, &meta).await?;
        }
        Ok(response)
    }

    pub async fn query(&self, name: &str, request: QueryRequest) -> Result<QueryResponse> {
        let strong = self.is_strong(request.consistency)?;
        let _shared = self.lock.read().await;
        let (_, namespace) = self.open_namespace(name, false).await?;
        query::query(&namespace, request, strong, self.embedder.as_deref()).await
    }

    pub async fn multi_query(
        &self,
        name: &str,
        request: MultiQueryRequest,
    ) -> Result<MultiQueryResponse> {
        let strong = self.is_strong(request.consistency)?;
        let _shared = self.lock.read().await;
        let (_, namespace) = self.open_namespace(name, false).await?;
        query::multi_query(&namespace, request, strong, self.embedder.as_deref()).await
    }

    /// One row with every attribute, including its vector.
    pub async fn get_row(&self, name: &str, id: &str) -> Result<Option<Map<String, Value>>> {
        let _shared = self.lock.read().await;
        let (shard, namespace) = self.open_namespace(name, false).await?;
        let Some(row) = namespace.engine(shard.is_writable()).await?.get(id).await? else {
            return Ok(None);
        };
        let mut out = Map::new();
        out.insert(schema::ID_KEY.into(), Value::String(row.id));
        for attribute in row.attributes {
            out.insert(attribute.name, schema::attribute_to_json(&attribute.value));
        }
        Ok(Some(out))
    }

    /// Every shard, active and retired, with the namespaces of those open
    /// here.
    pub async fn list_shards(&self) -> Result<Vec<ShardInfo>> {
        let topology = self.current()?;
        let open: Vec<u32> = self.state.lock().await.shards.keys().copied().collect();
        let mut shards = Vec::new();
        for meta in topology.shards {
            let namespaces = if open.contains(&meta.id) {
                self.shard_namespaces(meta.id)
                    .await?
                    .into_iter()
                    .map(|n| n.name)
                    .collect()
            } else {
                Vec::new()
            };
            shards.push(ShardInfo { meta, namespaces });
        }
        Ok(shards)
    }

    /// Splits the open shards whose split the topology plans.
    async fn split_planned(&self) -> Result<()> {
        let Some(topology) = self.topology() else {
            return Ok(());
        };
        let writable: Vec<u32> = self
            .state
            .lock()
            .await
            .shards
            .iter()
            .filter(|(_, shard)| shard.is_writable())
            .map(|(id, _)| *id)
            .collect();
        for shard in topology.active() {
            if shard.split_to.is_none() || !writable.contains(&shard.id) || !self.serves(shard) {
                continue;
            }
            match self.split_shard(shard).await {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Splits `planned`, an active shard owned here, into two new shards,
    /// each taking the namespace ids on one side of the median of its
    /// namespaces. The source shard is retired; this writer keeps the lower
    /// child, and the writer the split was planned for takes the upper one.
    ///
    /// Blocks every namespace operation here for the duration; on SlateDB
    /// the split clones the shard from a checkpoint without copying data.
    async fn split_shard(&self, planned: &ShardMeta) -> Result<Topology> {
        let id = planned.id;
        let _exclusive = self.lock.write().await;
        let mut state = self.state.lock().await;
        let topology = self.current()?;
        let parent = match topology.shard(id) {
            Some(meta)
                if meta.state == ShardState::Active
                    && meta.owner == planned.owner
                    && meta.split_to == planned.split_to =>
            {
                meta.clone()
            }
            _ => {
                return Err(Error::Conflict(format!(
                    "the split of shard {id} is no longer planned"
                )));
            }
        };
        let receiver = parent.split_to.clone().unwrap_or_default();
        let source = state
            .shards
            .get(&id)
            .filter(|s| s.is_writable())
            .cloned()
            .ok_or_else(|| Error::Unavailable(format!("shard {id} is not open for writes here")))?;
        let members = directory::list(source.read().as_ref(), source.range).await?;
        let at = split_point(
            source.range,
            members
                .iter()
                .map(|n| NamespaceId::for_name(&n.name))
                .collect(),
        );

        let left = self.reserve_shard_ids().await?;
        let right = left + 1;
        state.close_namespaces(&source).await?;
        source.writable()?.flush().await?;
        let checkpoint = self.backend.checkpoint(&source).await?.unwrap_or_default();
        let (low, high) = (
            NamespaceRange {
                start: parent.range.start,
                end: Some(HexId(at)),
            },
            NamespaceRange {
                start: HexId(at),
                end: parent.range.end,
            },
        );
        self.backend
            .create_split_child(&source, checkpoint, left, low)
            .await?;
        self.backend
            .create_split_child(&source, checkpoint, right, high)
            .await?;

        let split = self.publish_split(&parent, left, right, at).await?;
        state.shards.remove(&id);
        source.close().await?;
        if self.config.role == Role::Writer {
            self.leases.release(&self.identity, id).await?;
        }
        self.backend.retire(id);

        // The receiver opens the upper child once it sees the topology.
        if self.config.role == Role::Writer && !self.leases.acquire(&self.identity, left).await? {
            return Err(Error::Internal(format!(
                "the lease of new shard {left} is held elsewhere"
            )));
        }
        let shard = Arc::new(self.open_shard(left, low).await?);
        state.shards.insert(left, shard);
        tracing::info!(source = id, left, right, receiver = %receiver, at = %HexId(at), "split shard for a new writer");
        Ok(split)
    }

    /// Allocates two shard ids in the topology.
    async fn reserve_shard_ids(&self) -> Result<u32> {
        for _ in 0..TOPOLOGY_CAS_ATTEMPTS {
            let current = self.load_topology().await?;
            let (next, first) = current.reserve_ids(2);
            if self.store.publish(Some(current.generation), &next).await? {
                return Ok(first);
            }
        }
        Err(Error::Unavailable(
            "the topology kept changing while allocating shard ids".into(),
        ))
    }

    /// Replaces `parent` with its children in the topology, as long as it
    /// is still active, owned the same way, and to be split for the same
    /// writer.
    async fn publish_split(
        &self,
        parent: &ShardMeta,
        left: u32,
        right: u32,
        at: u64,
    ) -> Result<Topology> {
        for _ in 0..TOPOLOGY_CAS_ATTEMPTS {
            let current = self.load_topology().await?;
            match current.shard(parent.id) {
                Some(s)
                    if s.state == ShardState::Active
                        && s.owner == parent.owner
                        && s.split_to == parent.split_to => {}
                _ => {
                    return Err(Error::Conflict(format!(
                        "shard {} moved while it was splitting",
                        parent.id
                    )));
                }
            }
            let next = current.split(parent.id, left, right, at, now_ms())?;
            if self.store.publish(Some(current.generation), &next).await? {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "the topology kept changing while publishing a split".into(),
        ))
    }

    async fn load_topology(&self) -> Result<Topology> {
        self.store
            .load()
            .await?
            .ok_or_else(|| Error::Unavailable("the cluster topology is not published yet".into()))
    }

    /// Flushes every open namespace to durable storage.
    pub async fn flush(&self) -> Result<()> {
        let namespaces: Vec<Arc<Namespace>> = self
            .state
            .lock()
            .await
            .namespaces
            .values()
            .cloned()
            .collect();
        for namespace in namespaces {
            namespace.flush().await?;
        }
        Ok(())
    }

    /// Flushes and closes every namespace and shard, releasing their
    /// leases, and the topology store.
    pub async fn close(&self) -> Result<()> {
        self.cancel.cancel();
        let _exclusive = self.lock.write().await;
        let mut state = self.state.lock().await;
        for (_, namespace) in state.namespaces.drain() {
            namespace.close().await?;
        }
        for (id, shard) in state.shards.drain() {
            shard.close().await?;
            if self.config.role == Role::Writer {
                self.leases.release(&self.identity, id).await?;
            }
        }
        self.store.close().await?;
        self.backend.close().await
    }

    /// The open namespace `name` and its shard, opening the namespace if
    /// needed. Callers hold the shared lock.
    async fn open_namespace(
        &self,
        name: &str,
        writable: bool,
    ) -> Result<(Arc<Shard>, Arc<Namespace>)> {
        let (shard, _) = self.local_shard(name, writable).await?;
        let mut state = self.state.lock().await;
        if let Some(namespace) = state.namespaces.get(name) {
            return Ok((shard, namespace.clone()));
        }
        let meta = Self::stored_meta(&shard, name)
            .await?
            .ok_or_else(|| not_found(name))?;
        let namespace =
            Arc::new(Namespace::open(meta, &shard, &self.config, self.memory.clone()).await?);
        state.namespaces.insert(name.to_string(), namespace.clone());
        Ok((shard, namespace))
    }
}

const TOPOLOGY_CAS_ATTEMPTS: usize = 16;

fn not_found(name: &str) -> Error {
    Error::NotFound(format!("namespace '{name}' not found"))
}

/// Namespace names are 1-128 characters of `[A-Za-z0-9._-]`.
pub fn validate_namespace_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAMESPACE_NAME_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "invalid namespace name '{name}': use 1-{MAX_NAMESPACE_NAME_LEN} characters \
             of A-Z, a-z, 0-9, '-', '_', '.'"
        )))
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
