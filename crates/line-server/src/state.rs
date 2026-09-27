use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt, stream};
use line::{Durability as LineDurability, LogBatch, Namespace, ShardedLine, ShardingOptions};
use meter_server::auth::JwtAuthenticator;
use proto::line::internal::v1::internal_writer_client::InternalWriterClient;
use sharding::{
    Assignment, AssignmentGeneration, AssignmentState, Owner, ShardId, ShardMap, ShardRange,
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{Request, metadata::MetadataValue};

#[cfg(feature = "kubernetes")]
use sharding::{
    AssignmentStore, BoxError, OwnershipManager, OwnershipManagerConfig, ShardLifecycle,
    balanced_contiguous,
    kubernetes::{
        KubernetesAssignmentStore, KubernetesConfig, KubernetesCoordinatorElection,
        KubernetesLeaseBackend, StatefulSetMembership, run_kubernetes_coordinator,
    },
};

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend, StaticOwner},
    http::ApiError,
    internal_writer::to_proto_request,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<ShardedLine>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) namespaces: Arc<HashMap<String, NamespaceConfig>>,
    pub(crate) assignment: Arc<tokio::sync::RwLock<ShardMap>>,
    pub(crate) local_owner: String,
    pub(crate) completed_requests: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub(crate) draining_shards: Arc<tokio::sync::RwLock<HashSet<ShardId>>>,
    remote_limit: Arc<Semaphore>,
    query_cache: Arc<tokio::sync::Mutex<QueryCache>>,
    dirty: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    cancellation: CancellationToken,
    tasks: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
}

#[derive(Default)]
struct QueryCache {
    values: HashMap<String, serde_json::Value>,
    order: VecDeque<String>,
}

impl AppState {
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let jwt = match config.auth.jwt.clone() {
            Some(config) => Some(JwtAuthenticator::open(config).await?),
            None => None,
        };
        let cancellation = CancellationToken::new();
        #[cfg(feature = "kubernetes")]
        let mut kubernetes_runtime = None;
        let (local_owner, assignment) = match &config.sharding.kind {
            #[cfg(feature = "kubernetes")]
            ShardingBackend::Kubernetes(settings) => {
                let client = kube::Client::try_default().await?;
                let kube_config = kubernetes_config(settings);
                let store = KubernetesAssignmentStore::new(
                    client.clone(),
                    &kube_config,
                    cancellation.clone(),
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                let assignment = if let Some(current) = store
                    .load()
                    .await
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?
                {
                    current
                } else {
                    let identity = kubernetes_owner_id(settings);
                    loop {
                        if let Some(current) = store
                            .load()
                            .await
                            .map_err(|error| anyhow::anyhow!(error.to_string()))?
                        {
                            break current;
                        }
                        let election =
                            KubernetesCoordinatorElection::new(client.clone(), &kube_config);
                        if election.try_acquire(&identity).await? {
                            let owners = StatefulSetMembership::new(client.clone(), &kube_config)
                                .owners()
                                .await?;
                            let initial = balanced_contiguous(
                                AssignmentGeneration::new(1),
                                config.sharding.virtual_shards,
                                &owners,
                                None,
                            )?;
                            match store.publish(initial.clone()).await {
                                Ok(()) => break initial,
                                Err(error) => {
                                    tracing::warn!(%error, "initial shard assignment raced; retrying");
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                };
                let local = kubernetes_owner_id(settings);
                let leases = Arc::new(KubernetesLeaseBackend::new(
                    client,
                    &kube_config,
                    cancellation.clone(),
                ));
                kubernetes_runtime = Some((
                    store,
                    leases,
                    settings.renew_interval_seconds,
                    kube_config,
                    local.clone(),
                ));
                (local, assignment)
            }
            _ => assignment_for(&config)?,
        };
        let shards: Vec<ShardId> = match config.mode {
            ServerMode::Standalone | ServerMode::Reader => (0..config.sharding.virtual_shards)
                .map(ShardId::new)
                .collect(),
            ServerMode::Writer => {
                if matches!(config.sharding.kind, ShardingBackend::Kubernetes(_)) {
                    Vec::new()
                } else {
                    (0..config.sharding.virtual_shards)
                        .map(ShardId::new)
                        .filter(|shard| {
                            assignment
                                .owner_of(*shard)
                                .is_some_and(|owner| owner.id == local_owner)
                        })
                        .collect()
                }
            }
        };
        let db = Arc::new(
            ShardedLine::open(
                config.line_config(),
                ShardingOptions::new(
                    config.sharding.virtual_shards,
                    config.sharding.io_concurrency_multiplier,
                )?,
                shards,
            )
            .await?,
        );
        let namespaces = config
            .namespaces
            .iter()
            .cloned()
            .map(|namespace| (namespace.name.clone(), namespace))
            .collect();
        let state = Self {
            remote_limit: Arc::new(Semaphore::new(config.write.remote_concurrency)),
            config: Arc::new(config),
            db,
            jwt,
            namespaces: Arc::new(namespaces),
            assignment: Arc::new(tokio::sync::RwLock::new(assignment)),
            local_owner,
            completed_requests: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            draining_shards: Arc::new(tokio::sync::RwLock::new(HashSet::new())),
            query_cache: Arc::new(tokio::sync::Mutex::new(QueryCache::default())),
            dirty: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        };
        #[cfg(feature = "kubernetes")]
        if let Some((store, leases, renew_interval_seconds, kube_config, identity)) =
            kubernetes_runtime
            && state.config.mode != ServerMode::Reader
        {
            let lifecycle = Arc::new(LineShardLifecycle {
                db: Arc::clone(&state.db),
                draining_shards: Arc::clone(&state.draining_shards),
            });
            let manager = OwnershipManager::new(
                state.local_owner.clone(),
                OwnershipManagerConfig {
                    renew_interval: Duration::from_secs(renew_interval_seconds),
                    lease_duration: kube_config.lease_duration,
                },
                Arc::clone(&store),
                leases,
                lifecycle,
            );
            let manager_cancel = state.cancellation.clone();
            let manager_task = tokio::spawn(async move {
                if let Err(error) = manager.run(manager_cancel).await {
                    tracing::error!(%error, "Kubernetes ownership manager stopped");
                }
            });
            let assignment = Arc::clone(&state.assignment);
            let watcher_cancel = state.cancellation.clone();
            let mut updates = store.watch();
            let watcher_task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = watcher_cancel.cancelled() => return,
                        changed = updates.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            let next = { updates.borrow_and_update().clone() };
                            if let Some(next) = next {
                                *assignment.write().await = next;
                            }
                        }
                    }
                }
            });
            let coordinator_cancel = state.cancellation.clone();
            let virtual_shards = state.config.sharding.virtual_shards;
            let coordinator_task = tokio::spawn(async move {
                run_kubernetes_coordinator(
                    store,
                    kube_config,
                    identity,
                    virtual_shards,
                    coordinator_cancel,
                )
                .await;
            });
            state
                .tasks
                .lock()
                .await
                .extend([manager_task, watcher_task, coordinator_task]);
        }
        state.start_visibility_task().await;
        Ok(state)
    }

    pub async fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            return false;
        }
        match self.config.mode {
            ServerMode::Standalone | ServerMode::Reader => {
                self.db.open_shard_count().await == self.config.sharding.virtual_shards as usize
            }
            ServerMode::Writer => {
                let assignment = self.assignment.read().await;
                let expected = (0..assignment.virtual_shards)
                    .map(ShardId::new)
                    .filter(|shard| {
                        assignment
                            .owner_of(*shard)
                            .is_some_and(|owner| owner.id == self.local_owner)
                    })
                    .collect::<Vec<_>>();
                !expected.is_empty() && self.db.open_shards().await == expected
            }
        }
    }

    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_visibility(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub(crate) async fn cached_query(&self, key: &str) -> Option<serde_json::Value> {
        if self.config.retention_seconds.is_some() {
            return None;
        }
        self.query_cache.lock().await.values.get(key).cloned()
    }

    pub(crate) async fn cache_query(&self, key: String, value: serde_json::Value) {
        let capacity = self.config.cache.query_entries;
        if capacity == 0 || self.config.retention_seconds.is_some() {
            return;
        }
        let mut cache = self.query_cache.lock().await;
        if !cache.values.contains_key(&key) {
            cache.order.push_back(key.clone());
        }
        cache.values.insert(key, value);
        while cache.values.len() > capacity {
            if let Some(oldest) = cache.order.pop_front() {
                cache.values.remove(&oldest);
            }
        }
    }

    pub(crate) async fn invalidate_queries(&self) {
        let mut cache = self.query_cache.lock().await;
        cache.values.clear();
        cache.order.clear();
    }

    pub(crate) async fn route_write(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
        request_id: String,
    ) -> Result<(), ApiError> {
        let options = ShardingOptions::new(
            self.config.sharding.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )
        .map_err(ApiError::internal)?;
        let assignment = self.assignment.read().await.clone();
        let mut groups: HashMap<(Owner, ShardId), Vec<LogBatch>> = HashMap::new();
        for batch in batches {
            let shard = options.route(namespace, &batch.labels);
            let owner = assignment
                .owner_of(shard)
                .cloned()
                .ok_or_else(|| ApiError::unavailable("shard has no active owner"))?;
            groups.entry((owner, shard)).or_default().push(batch);
        }
        let results = stream::iter(groups.into_iter().map(|((owner, shard), batches)| {
            let state = self.clone();
            let namespace = namespace.clone();
            let request_id = format!("{request_id}-{}", shard.get());
            async move {
                let _permit = state
                    .remote_limit
                    .acquire()
                    .await
                    .map_err(|_| ApiError::unavailable("server is shutting down"))?;
                if owner.id == state.local_owner {
                    state.write_local(&namespace, shard, batches).await
                } else {
                    state
                        .write_remote(
                            &namespace,
                            owner,
                            shard,
                            batches,
                            assignment.generation.get(),
                            request_id,
                        )
                        .await
                }
            }
        }))
        .buffer_unordered(self.config.write.remote_concurrency)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    pub(crate) async fn write_local(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        batches: Vec<LogBatch>,
    ) -> Result<(), ApiError> {
        if self.draining_shards.read().await.contains(&shard) {
            return Err(ApiError::unavailable("local shard is draining"));
        }
        let database = self
            .db
            .shard(shard)
            .await
            .ok_or_else(|| ApiError::unavailable("local shard is not open"))?;
        database
            .write_with_durability(namespace, batches, durability(self.config.write.durability))
            .await
            .map_err(ApiError::internal)?;
        self.mark_dirty();
        self.invalidate_queries().await;
        Ok(())
    }

    async fn write_remote(
        &self,
        namespace: &Namespace,
        mut owner: Owner,
        shard: ShardId,
        batches: Vec<LogBatch>,
        mut generation: u64,
        request_id: String,
    ) -> Result<(), ApiError> {
        for attempt in 0..=self.config.write.remote_retries {
            let endpoint = owner_endpoint(&self.config, &owner)?;
            let mut client = InternalWriterClient::connect(format!("http://{endpoint}"))
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let mut request = Request::new(to_proto_request(
                namespace,
                shard,
                generation,
                &request_id,
                self.config.write.durability,
                batches.clone(),
            ));
            if let Some(secret) = &self.config.auth.internal {
                request.metadata_mut().insert(
                    "authorization",
                    MetadataValue::try_from(format!(
                        "Bearer {}",
                        secret.expose().map_err(ApiError::internal)?
                    ))
                    .map_err(ApiError::internal)?,
                );
            }
            match client.write(request).await {
                Ok(_) => return Ok(()),
                Err(status)
                    if status.code() == tonic::Code::FailedPrecondition
                        && attempt < self.config.write.remote_retries =>
                {
                    let assignment = self.assignment.read().await;
                    generation = assignment.generation.get();
                    owner = assignment
                        .owner_of(shard)
                        .cloned()
                        .ok_or_else(|| ApiError::unavailable("shard owner disappeared"))?;
                }
                Err(status) => return Err(ApiError::unavailable(status.to_string())),
            }
        }
        Err(ApiError::unavailable("remote write retries exhausted"))
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.ready.store(false, Ordering::Release);
        self.cancellation.cancel();
        for task in self.tasks.lock().await.drain(..) {
            task.await?;
        }
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
    }

    async fn start_visibility_task(&self) {
        let seconds = self.config.visibility_interval_seconds;
        if seconds == 0 {
            return;
        }
        let state = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(seconds));
            interval.tick().await;
            loop {
                tokio::select! {
                    () = state.cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if state.dirty.swap(false, Ordering::AcqRel) {
                            if let Err(error) = state.db.flush().await {
                                state.ready.store(false, Ordering::Release);
                                state.dirty.store(true, Ordering::Release);
                                tracing::error!(%error, "Line visibility flush failed");
                            } else {
                                state.invalidate_queries().await;
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().await.push(task);
    }
}

#[cfg(feature = "kubernetes")]
struct LineShardLifecycle {
    db: Arc<ShardedLine>,
    draining_shards: Arc<tokio::sync::RwLock<HashSet<ShardId>>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for LineShardLifecycle {
    async fn open(
        &self,
        shard: ShardId,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        self.db.open_shard(shard).await?;
        self.draining_shards.write().await.remove(&shard);
        Ok(())
    }

    async fn drain(&self, shard: ShardId) -> Result<(), BoxError> {
        self.draining_shards.write().await.insert(shard);
        Ok(())
    }

    async fn flush(&self, shard: ShardId) -> Result<(), BoxError> {
        self.db.flush_shard(shard).await?;
        Ok(())
    }

    async fn close(&self, shard: ShardId) -> Result<(), BoxError> {
        self.db.close_shard(shard).await?;
        Ok(())
    }
}

#[cfg(feature = "kubernetes")]
fn kubernetes_owner_id(config: &crate::config::KubernetesShardingConfig) -> String {
    std::env::var("POD_NAME").unwrap_or_else(|_| {
        let ordinal = std::env::var("POD_ORDINAL").unwrap_or_else(|_| "0".to_owned());
        format!("{}-{ordinal}", config.stateful_set)
    })
}

#[cfg(feature = "kubernetes")]
fn kubernetes_config(config: &crate::config::KubernetesShardingConfig) -> KubernetesConfig {
    KubernetesConfig {
        namespace: config.namespace.clone(),
        stateful_set: config.stateful_set.clone(),
        headless_service: config.headless_service.clone(),
        owner_port: config.owner_port,
        assignment_config_map: config.assignment_config_map.clone(),
        coordinator_lease: config.coordinator_lease.clone(),
        shard_lease_prefix: config.shard_lease_prefix.clone(),
        lease_duration: Duration::from_secs(config.lease_duration_seconds),
    }
}

fn durability(value: Durability) -> LineDurability {
    match value {
        Durability::Applied => LineDurability::Applied,
        Durability::Written => LineDurability::Written,
        Durability::Durable => LineDurability::Durable,
    }
}

pub(crate) fn assignment_for(config: &Config) -> anyhow::Result<(String, ShardMap)> {
    let (local, owners) = match &config.sharding.kind {
        ShardingBackend::Standalone => (
            "standalone".to_owned(),
            vec![StaticOwner {
                id: "standalone".to_owned(),
                ordinal: 0,
                endpoint: config.listeners.grpc.to_string(),
                start_shard: 0,
                end_shard: config.sharding.virtual_shards,
            }],
        ),
        ShardingBackend::Static { owner_id, owners } => (owner_id.clone(), owners.clone()),
        ShardingBackend::Kubernetes(kubernetes) => {
            let ordinal = std::env::var("POD_NAME")
                .ok()
                .and_then(|name| name.rsplit_once('-')?.1.parse().ok())
                .unwrap_or(0);
            let id = format!("{}-{ordinal}", kubernetes.stateful_set);
            (
                id.clone(),
                vec![StaticOwner {
                    id,
                    ordinal,
                    endpoint: format!(
                        "{}-{ordinal}.{}.{}.svc:{}",
                        kubernetes.stateful_set,
                        kubernetes.headless_service,
                        kubernetes.namespace,
                        kubernetes.owner_port
                    ),
                    start_shard: 0,
                    end_shard: config.sharding.virtual_shards,
                }],
            )
        }
    };
    let assignments = owners
        .into_iter()
        .map(|owner| {
            Ok(Assignment::new(
                Owner::new(owner.id, owner.ordinal),
                ShardRange::within(
                    owner.start_shard,
                    owner.end_shard,
                    config.sharding.virtual_shards,
                )?,
                AssignmentState::Active,
            ))
        })
        .collect::<Result<Vec<_>, sharding::ModelError>>()?;
    Ok((
        local,
        ShardMap::new(
            AssignmentGeneration::new(1),
            config.sharding.virtual_shards,
            assignments,
        )?,
    ))
}

fn owner_endpoint(config: &Config, owner: &Owner) -> Result<String, ApiError> {
    match &config.sharding.kind {
        ShardingBackend::Standalone => Ok(config.listeners.grpc.to_string()),
        ShardingBackend::Static { owners, .. } => owners
            .iter()
            .find(|candidate| candidate.id == owner.id)
            .map(|candidate| candidate.endpoint.clone())
            .ok_or_else(|| ApiError::unavailable("owner endpoint is unknown")),
        ShardingBackend::Kubernetes(kubernetes) => Ok(format!(
            "{}-{}.{}.{}.svc:{}",
            kubernetes.stateful_set,
            owner.ordinal,
            kubernetes.headless_service,
            kubernetes.namespace,
            kubernetes.owner_port
        )),
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use line::{Label, Labels, LogEntry, QueryOptions, QueryRequest, QueryResult};
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    use super::*;
    use crate::{config::ShardingConfig, grpc_service};

    fn config(owner_id: &str, endpoint: String) -> Config {
        Config {
            mode: ServerMode::Writer,
            storage: StorageConfig::InMemory,
            sharding: ShardingConfig {
                virtual_shards: 2,
                io_concurrency_multiplier: 4,
                kind: ShardingBackend::Static {
                    owner_id: owner_id.into(),
                    owners: vec![
                        StaticOwner {
                            id: "writer-a".into(),
                            ordinal: 0,
                            endpoint: "127.0.0.1:1".into(),
                            start_shard: 0,
                            end_shard: 1,
                        },
                        StaticOwner {
                            id: "writer-b".into(),
                            ordinal: 1,
                            endpoint,
                            start_shard: 1,
                            end_shard: 2,
                        },
                    ],
                },
            },
            namespaces: vec![NamespaceConfig {
                name: "tenant".into(),
                auth: Default::default(),
            }],
            ..Config::default()
        }
    }

    fn batch_on_shard(namespace: &Namespace, shard: u32) -> LogBatch {
        let options = ShardingOptions::new(2, 4).unwrap();
        let labels = (0..10_000)
            .map(|candidate| {
                Labels::new(vec![Label::new("app", format!("api-{candidate}"))]).unwrap()
            })
            .find(|labels| options.route(namespace, labels).get() == shard)
            .unwrap();
        LogBatch::new(labels, vec![LogEntry::new(1, "forwarded")])
    }

    fn with_generation(map: &ShardMap, generation: u64) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(generation),
            map.virtual_shards,
            map.assignments.clone(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn retention_disables_response_cache_that_could_resurrect_rows() {
        let state = AppState::open(Config {
            storage: StorageConfig::InMemory,
            retention_seconds: Some(60),
            ..Config::default()
        })
        .await
        .unwrap();
        state
            .cache_query("query".into(), serde_json::json!({"cached": true}))
            .await;
        assert_eq!(state.cached_query("query").await, None);
        assert!(state.query_cache.lock().await.values.is_empty());
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn local_write_rejects_a_draining_shard() {
        let state = AppState::open(Config {
            storage: StorageConfig::InMemory,
            namespaces: vec![NamespaceConfig {
                name: "tenant".into(),
                auth: Default::default(),
            }],
            ..Config::default()
        })
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let batch = batch_on_shard(&namespace, 1);
        let shard = state.db.route(&namespace, &batch.labels);
        state.draining_shards.write().await.insert(shard);

        assert!(
            state
                .write_local(&namespace, shard, vec![batch])
                .await
                .is_err()
        );
        state.shutdown().await.unwrap();
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn line_lifecycle_opens_drains_flushes_and_closes_a_shard() {
        let db = Arc::new(
            ShardedLine::open(
                line::Config {
                    storage: StorageConfig::InMemory,
                    ..line::Config::default()
                },
                ShardingOptions::new(2, 4).unwrap(),
                [],
            )
            .await
            .unwrap(),
        );
        let draining = Arc::new(tokio::sync::RwLock::new(HashSet::new()));
        let lifecycle = LineShardLifecycle {
            db: Arc::clone(&db),
            draining_shards: Arc::clone(&draining),
        };
        let shard = ShardId::new(1);

        lifecycle
            .open(shard, AssignmentGeneration::new(1))
            .await
            .unwrap();
        assert!(db.contains_shard(shard).await);
        lifecycle.drain(shard).await.unwrap();
        assert!(draining.read().await.contains(&shard));
        lifecycle.flush(shard).await.unwrap();
        lifecycle.close(shard).await.unwrap();
        assert!(!db.contains_shard(shard).await);
    }

    #[tokio::test]
    async fn remote_forwarding_retries_stale_generation_idempotently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let writer_b = AppState::open(config("writer-b", endpoint.to_string()))
            .await
            .unwrap();
        let current = writer_b.assignment.read().await.clone();
        *writer_b.assignment.write().await = with_generation(&current, 2);
        let cancel = CancellationToken::new();
        let server = tokio::spawn(
            Server::builder()
                .add_service(grpc_service(writer_b.clone()))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    cancel.clone().cancelled_owned(),
                ),
        );

        let writer_a = AppState::open(config("writer-a", endpoint.to_string()))
            .await
            .unwrap();
        let current = writer_a.assignment.read().await.clone();
        *writer_a.assignment.write().await = with_generation(&current, 2);
        let namespace = Namespace::new("tenant").unwrap();
        let batch = batch_on_shard(&namespace, 1);
        writer_a
            .write_remote(
                &namespace,
                Owner::new("writer-b", 1),
                ShardId::new(1),
                vec![batch],
                1,
                "retry-request".into(),
            )
            .await
            .unwrap();

        let result = writer_b
            .db
            .query(
                &namespace,
                &QueryRequest::range(r#"{app=~".+"}"#, 0, 2, 1),
                QueryOptions::default(),
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

        cancel.cancel();
        server.await.unwrap().unwrap();
        writer_a.shutdown().await.unwrap();
        writer_b.shutdown().await.unwrap();
    }
}
