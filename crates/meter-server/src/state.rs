use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{StreamExt, stream};
use meter::{Namespace, Series, ShardedMeter, ShardingOptions, TimeSeriesDb, Visibility};
use proto::meter::internal::v1::internal_writer_client::InternalWriterClient;
use sharding::{
    Assignment, AssignmentGeneration, AssignmentState, Owner, ShardId, ShardMap, ShardRange,
};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::{Request, metadata::MetadataValue};

#[cfg(feature = "kubernetes")]
use sharding::{
    AssignmentStore, BoxError, OwnershipManager, OwnershipManagerConfig, ShardLifecycle,
    balanced_contiguous,
    kubernetes::{
        KubernetesAssignmentStore, KubernetesConfig, KubernetesCoordinatorElection,
        KubernetesLeaseBackend, StatefulSetMembership,
    },
};

use crate::{
    auth::JwtAuthenticator,
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend, StaticOwner},
    http::ApiError,
    internal_writer::to_proto_request,
};

type Writers = HashMap<String, BTreeMap<ShardId, Arc<TimeSeriesDb>>>;
type Readers = HashMap<String, Arc<ShardedMeter>>;

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) writers: Arc<RwLock<Writers>>,
    pub(crate) readers: Arc<RwLock<Readers>>,
    pub(crate) assignment: Arc<RwLock<ShardMap>>,
    pub(crate) local_owner: String,
    pub(crate) remote_limit: Arc<Semaphore>,
    pub(crate) completed_requests: Arc<Mutex<HashSet<String>>>,
    pub(crate) draining_shards: Arc<RwLock<HashSet<ShardId>>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) background_tasks: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    pub(crate) flush_runs: Arc<AtomicU64>,
}

impl AppState {
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let jwt = match config.auth.jwt.clone() {
            Some(jwt) => Some(JwtAuthenticator::open(jwt).await?),
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
                        tokio::time::sleep(kube_config.watch_poll_interval).await;
                    }
                };
                let local = kubernetes_owner_id(settings);
                let leases = Arc::new(KubernetesLeaseBackend::new(client, &kube_config));
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
        let options = ShardingOptions::new(
            config.sharding.virtual_shards,
            config.sharding.io_concurrency_multiplier,
        )?;
        let mut writers = HashMap::new();
        let mut readers = HashMap::new();
        for namespace_config in &config.namespaces {
            let namespace = Namespace::new(&namespace_config.name)?;
            let namespace_config_base = meter_config_for_namespace(&config, &namespace);
            if config.mode == ServerMode::Standalone {
                let database = ShardedMeter::open_writers(
                    namespace,
                    namespace_config_base,
                    options,
                    (0..config.sharding.virtual_shards).map(ShardId::new),
                )
                .await?;
                writers.insert(namespace_config.name.clone(), BTreeMap::new());
                readers.insert(namespace_config.name.clone(), Arc::new(database));
                continue;
            }
            if config.mode != ServerMode::Reader
                && !matches!(config.sharding.kind, ShardingBackend::Kubernetes(_))
            {
                let mut shards = BTreeMap::new();
                for shard in 0..config.sharding.virtual_shards {
                    let shard = ShardId::new(shard);
                    if assignment
                        .owner_of(shard)
                        .is_some_and(|owner| owner.id == local_owner)
                    {
                        let mut meter_config = namespace_config_base.clone();
                        meter_config.storage.path =
                            options.shard_path(&meter_config.storage.path, shard)?;
                        shards.insert(
                            shard,
                            Arc::new(TimeSeriesDb::open(namespace.clone(), meter_config).await?),
                        );
                    }
                }
                writers.insert(namespace_config.name.clone(), shards);
            }
            if config.mode != ServerMode::Writer {
                readers.insert(
                    namespace_config.name.clone(),
                    Arc::new(
                        ShardedMeter::open_readers(
                            namespace,
                            namespace_config_base,
                            options,
                            (0..config.sharding.virtual_shards).map(ShardId::new),
                            slatedb::config::DbReaderOptions {
                                skip_wal_replay: false,
                                ..slatedb::config::DbReaderOptions::default()
                            },
                            config.reader_cache_capacity,
                        )
                        .await?,
                    ),
                );
            }
        }
        let state = Self {
            remote_limit: Arc::new(Semaphore::new(config.write.remote_concurrency)),
            config: Arc::new(config),
            jwt,
            writers: Arc::new(RwLock::new(writers)),
            readers: Arc::new(RwLock::new(readers)),
            assignment: Arc::new(RwLock::new(assignment)),
            local_owner,
            completed_requests: Arc::new(Mutex::new(HashSet::new())),
            draining_shards: Arc::new(RwLock::new(HashSet::new())),
            cancellation,
            background_tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            flush_runs: Arc::new(AtomicU64::new(0)),
        };
        #[cfg(feature = "kubernetes")]
        if let Some((store, leases, renew_interval_seconds, kube_config, identity)) =
            kubernetes_runtime
            && state.config.mode != ServerMode::Reader
        {
            let lifecycle = Arc::new(MeterShardLifecycle {
                config: Arc::clone(&state.config),
                writers: Arc::clone(&state.writers),
                draining_shards: Arc::clone(&state.draining_shards),
            });
            let manager = OwnershipManager::new(
                state.local_owner.clone(),
                OwnershipManagerConfig {
                    renew_interval: Duration::from_secs(renew_interval_seconds),
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
            let coordinator_store = Arc::clone(&store);
            let coordinator_cancel = state.cancellation.clone();
            let virtual_shards = state.config.sharding.virtual_shards;
            let coordinator_task = tokio::spawn(async move {
                run_kubernetes_coordinator(
                    coordinator_store,
                    kube_config,
                    identity,
                    virtual_shards,
                    coordinator_cancel,
                )
                .await;
            });
            state.background_tasks.lock().await.extend([
                manager_task,
                watcher_task,
                coordinator_task,
            ]);
        }
        if state.config.mode != ServerMode::Reader && state.config.write.flush_interval_seconds > 0
        {
            let flush_state = state.clone();
            let flush_cancel = state.cancellation.clone();
            let interval = Duration::from_secs(state.config.write.flush_interval_seconds);
            let flush_task = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticker.tick().await;
                loop {
                    tokio::select! {
                        () = flush_cancel.cancelled() => return,
                        _ = ticker.tick() => {
                            if let Err(error) = flush_state.flush_active_writers().await {
                                tracing::error!(%error, "periodic durable flush failed");
                            }
                        }
                    }
                }
            });
            state.background_tasks.lock().await.push(flush_task);
        }
        Ok(state)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        let tasks = std::mem::take(&mut *self.background_tasks.lock().await);
        for task in tasks {
            task.await?;
        }
        let writers = {
            let mut guard = self.writers.write().await;
            std::mem::take(&mut *guard)
        };
        for (_, shards) in writers {
            for (_, db) in shards {
                db.flush().await?;
                if let Ok(db) = Arc::try_unwrap(db) {
                    db.close().await?;
                }
            }
        }
        let readers = {
            let mut guard = self.readers.write().await;
            std::mem::take(&mut *guard)
        };
        for (_, reader) in readers {
            if let Ok(reader) = Arc::try_unwrap(reader) {
                reader.close().await?;
            }
        }
        Ok(())
    }

    pub(crate) fn namespace(&self, name: &str) -> Option<&NamespaceConfig> {
        self.config
            .namespaces
            .iter()
            .find(|namespace| namespace.name == name)
    }

    pub(crate) async fn is_ready(&self) -> bool {
        match self.config.mode {
            ServerMode::Reader | ServerMode::Standalone => {
                self.readers.read().await.len() == self.config.namespaces.len()
            }
            ServerMode::Writer => {
                let assignment = self.assignment.read().await;
                let expected = (0..assignment.virtual_shards)
                    .filter(|shard| {
                        assignment
                            .owner_of(ShardId::new(*shard))
                            .is_some_and(|owner| owner.id == self.local_owner)
                    })
                    .count();
                let writers = self.writers.read().await;
                expected > 0
                    && self.config.namespaces.iter().all(|namespace| {
                        writers
                            .get(&namespace.name)
                            .is_some_and(|shards| shards.len() == expected)
                    })
            }
        }
    }

    async fn flush_active_writers(&self) -> anyhow::Result<()> {
        self.flush_runs.fetch_add(1, Ordering::Relaxed);
        let databases = {
            let writers = self.writers.read().await;
            writers
                .values()
                .flat_map(|shards| shards.values().cloned())
                .collect::<Vec<_>>()
        };
        for database in databases {
            database.flush().await?;
        }
        if self.config.mode == ServerMode::Standalone {
            let databases = self
                .readers
                .read()
                .await
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for database in databases {
                database.flush().await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn route_write(
        &self,
        namespace: &str,
        series: Vec<Series>,
        durability: Durability,
        request_id: String,
    ) -> Result<(), ApiError> {
        let meter_namespace =
            Namespace::new(namespace).map_err(|error| ApiError::bad_request(error.to_string()))?;
        let options = ShardingOptions::new(
            self.config.sharding.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )
        .map_err(ApiError::internal)?;
        let assignment = self.assignment.read().await.clone();
        let mut groups: HashMap<(Owner, ShardId), Vec<Series>> = HashMap::new();
        for item in series {
            let shard = options.route(&meter_namespace, &item.labels);
            let owner = assignment
                .owner_of(shard)
                .cloned()
                .ok_or_else(|| ApiError::unavailable("shard has no active owner"))?;
            groups.entry((owner, shard)).or_default().push(item);
        }
        let results = stream::iter(groups.into_iter().map(|((owner, shard), batch)| {
            let state = self.clone();
            let namespace = namespace.to_owned();
            let request_id = format!("{request_id}-{}", shard.get());
            async move {
                let _permit = state
                    .remote_limit
                    .acquire()
                    .await
                    .map_err(|_| ApiError::unavailable("server is shutting down"))?;
                if owner.id == state.local_owner {
                    state
                        .write_local(&namespace, shard, batch, durability)
                        .await
                } else {
                    state
                        .write_remote(
                            &namespace,
                            owner,
                            shard,
                            batch,
                            durability,
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
        namespace: &str,
        shard: ShardId,
        series: Vec<Series>,
        durability: Durability,
    ) -> Result<(), ApiError> {
        let draining = self.draining_shards.read().await;
        if draining.contains(&shard) {
            return Err(ApiError::unavailable("local shard is draining"));
        }
        let writers = self.writers.read().await;
        if let Some(db) = writers.get(namespace).and_then(|shards| shards.get(&shard)) {
            return db
                .write_with_visibility(series, visibility(durability))
                .await
                .map_err(ApiError::internal);
        }
        drop(writers);
        if self.config.mode == ServerMode::Standalone {
            return self
                .readers
                .read()
                .await
                .get(namespace)
                .ok_or_else(|| ApiError::unavailable("standalone database is not open"))?
                .write(series, visibility(durability))
                .await
                .map_err(ApiError::internal);
        }
        drop(draining);
        Err(ApiError::unavailable("local shard is not open"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_remote(
        &self,
        namespace: &str,
        mut owner: Owner,
        shard: ShardId,
        series: Vec<Series>,
        durability: Durability,
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
                durability,
                series.clone(),
            ));
            if let Some(secret) = &self.config.auth.internal {
                let value = format!("Bearer {}", secret.expose().map_err(ApiError::internal)?);
                request.metadata_mut().insert(
                    "authorization",
                    MetadataValue::try_from(value).map_err(ApiError::internal)?,
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
}

#[cfg(feature = "kubernetes")]
struct MeterShardLifecycle {
    config: Arc<Config>,
    writers: Arc<RwLock<Writers>>,
    draining_shards: Arc<RwLock<HashSet<ShardId>>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for MeterShardLifecycle {
    async fn open(
        &self,
        range: ShardRange,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        let options = ShardingOptions::new(
            self.config.sharding.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )?;
        {
            let mut draining = self.draining_shards.write().await;
            for shard in range.start().get()..range.end().get() {
                draining.remove(&ShardId::new(shard));
            }
        }
        for namespace_config in &self.config.namespaces {
            let namespace = Namespace::new(&namespace_config.name)?;
            for shard_id in range.start().get()..range.end().get() {
                let shard = ShardId::new(shard_id);
                if self
                    .writers
                    .read()
                    .await
                    .get(&namespace_config.name)
                    .is_some_and(|shards| shards.contains_key(&shard))
                {
                    continue;
                }
                let mut config = meter_config_for_namespace(&self.config, &namespace);
                config.storage.path = options.shard_path(&config.storage.path, shard)?;
                let database = Arc::new(TimeSeriesDb::open(namespace.clone(), config).await?);
                self.writers
                    .write()
                    .await
                    .entry(namespace_config.name.clone())
                    .or_default()
                    .insert(shard, database);
            }
        }
        Ok(())
    }

    async fn drain(&self, range: ShardRange) -> Result<(), BoxError> {
        let mut draining = self.draining_shards.write().await;
        for shard in range.start().get()..range.end().get() {
            draining.insert(ShardId::new(shard));
        }
        Ok(())
    }

    async fn flush(&self, range: ShardRange) -> Result<(), BoxError> {
        let databases = {
            let writers = self.writers.read().await;
            writers
                .values()
                .flat_map(|shards| {
                    shards
                        .iter()
                        .filter(|(shard, _)| range.contains(**shard))
                        .map(|(_, database)| Arc::clone(database))
                })
                .collect::<Vec<_>>()
        };
        for database in databases {
            database.flush().await?;
        }
        Ok(())
    }

    async fn close(&self, range: ShardRange) -> Result<(), BoxError> {
        let databases = {
            let mut writers = self.writers.write().await;
            let mut removed = Vec::new();
            for shards in writers.values_mut() {
                let ids = shards
                    .keys()
                    .copied()
                    .filter(|shard| range.contains(*shard))
                    .collect::<Vec<_>>();
                for shard in ids {
                    if let Some(database) = shards.remove(&shard) {
                        removed.push(database);
                    }
                }
            }
            removed
        };
        for database in databases {
            let database = Arc::try_unwrap(database).map_err(|_| {
                std::io::Error::other("shard database still has in-flight references")
            })?;
            database.close().await?;
        }
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
        watch_poll_interval: Duration::from_secs(config.watch_poll_interval_seconds),
    }
}

#[cfg(feature = "kubernetes")]
async fn run_kubernetes_coordinator(
    store: Arc<KubernetesAssignmentStore>,
    config: KubernetesConfig,
    identity: String,
    virtual_shards: u32,
    cancel: CancellationToken,
) {
    let client = match kube::Client::try_default().await {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "cannot start Kubernetes shard coordinator");
            return;
        }
    };
    let election = KubernetesCoordinatorElection::new(client.clone(), &config);
    let membership = StatefulSetMembership::new(client, &config);
    let renew_interval = (config.lease_duration / 3).max(Duration::from_secs(1));
    let mut ticker = tokio::time::interval(renew_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut leader = false;
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                if leader
                    && let Err(error) = election.release(&identity).await
                {
                    tracing::warn!(%error, "failed to release coordinator leadership");
                }
                return;
            }
            _ = ticker.tick() => {
                leader = if leader {
                    match election.renew(&identity).await {
                        Ok(held) => held,
                        Err(error) => {
                            tracing::warn!(%error, "failed to renew coordinator leadership");
                            false
                        }
                    }
                } else {
                    match election.try_acquire(&identity).await {
                        Ok(acquired) => acquired,
                        Err(error) => {
                            tracing::warn!(%error, "failed to acquire coordinator leadership");
                            false
                        }
                    }
                };
                if !leader {
                    continue;
                }
                let owners = match membership.owners().await {
                    Ok(owners) => owners,
                    Err(error) => {
                        tracing::warn!(%error, "failed to observe StatefulSet membership");
                        continue;
                    }
                };
                let current = match store.load().await {
                    Ok(current) => current,
                    Err(error) => {
                        tracing::warn!(%error, "failed to load current shard assignment");
                        continue;
                    }
                };
                if !membership_changed(current.as_ref(), &owners, virtual_shards) {
                    continue;
                }
                let generation = current
                    .as_ref()
                    .map_or(AssignmentGeneration::new(1), |map| map.generation.next());
                let next = match balanced_contiguous(
                    generation,
                    virtual_shards,
                    &owners,
                    current.as_ref(),
                ) {
                    Ok(next) => next,
                    Err(error) => {
                        tracing::warn!(%error, "failed to plan shard assignment");
                        continue;
                    }
                };
                if let Err(error) = store.publish(next).await {
                    tracing::warn!(%error, "failed to publish shard assignment");
                    leader = false;
                }
            }
        }
    }
}

#[cfg(feature = "kubernetes")]
pub(crate) fn membership_changed(
    current: Option<&ShardMap>,
    owners: &[Owner],
    virtual_shards: u32,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.virtual_shards != virtual_shards {
        return true;
    }
    let current_owners = current
        .assignments
        .iter()
        .map(|assignment| (&assignment.owner.id, assignment.owner.ordinal))
        .collect::<HashSet<_>>();
    let desired_owners = owners
        .iter()
        .map(|owner| (&owner.id, owner.ordinal))
        .collect::<HashSet<_>>();
    current_owners != desired_owners
}

fn meter_config(config: &Config) -> meter::Config {
    meter::Config {
        storage: config.storage.clone(),
        flush_interval: Duration::from_secs(config.write.flush_interval_seconds),
        retention: None,
    }
}

pub(crate) fn meter_config_for_namespace(config: &Config, namespace: &Namespace) -> meter::Config {
    let mut meter = meter_config(config);
    let namespace_hash = blake3::hash(namespace.as_str().as_bytes()).to_hex();
    meter.storage.path = format!(
        "{}/namespace-{namespace_hash}",
        meter.storage.path.trim_end_matches('/')
    );
    meter
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
                .and_then(|name| name.rsplit_once('-')?.1.parse::<u32>().ok())
                .unwrap_or(0);
            let id = format!("{}-{ordinal}", kubernetes.stateful_set);
            (
                id.clone(),
                vec![StaticOwner {
                    id,
                    ordinal,
                    endpoint: format!(
                        "{}-{ordinal}.{}:{}",
                        kubernetes.stateful_set, kubernetes.headless_service, kubernetes.owner_port
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
            "{}-{}.{}:{}",
            kubernetes.stateful_set,
            owner.ordinal,
            kubernetes.headless_service,
            kubernetes.owner_port
        )),
    }
}

fn visibility(durability: Durability) -> Visibility {
    match durability {
        Durability::Applied => Visibility::Applied,
        Durability::Written => Visibility::Written,
        Durability::Durable => Visibility::Durable,
    }
}
