use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt, stream};
use meter_server::auth::JwtAuthenticator;
use proto::track::internal::v1::internal_writer_client::InternalWriterClient;
use sharding::{Owner, ShardId, ShardMap, server::owned_shards};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, metadata::MetadataValue};
use track::{Namespace, ShardedTrack, ShardingOptions, Trace, TraceBatch};

#[cfg(feature = "kubernetes")]
use sharding::{
    AssignmentGeneration, BoxError, MigrationExecutionError, ShardLifecycle,
    ShardMigrationExecutor, ShardSplit, server::KubernetesRuntime,
};

use crate::{
    config::{Config, NamespaceConfig, ServerMode, ShardingBackend},
    http::ApiError,
    internal_writer::to_proto_request,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<ShardedTrack>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) namespaces: Arc<HashMap<String, NamespaceConfig>>,
    pub(crate) assignment: Arc<RwLock<ShardMap>>,
    pub(crate) local_owner: String,
    pub(crate) completed_requests: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub(crate) draining_shards: Arc<RwLock<HashSet<ShardId>>>,
    pub(crate) request_limit: Arc<Semaphore>,
    pub(crate) query_limit: Arc<Semaphore>,
    remote_limit: Arc<Semaphore>,
    dirty: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    cache_warmed: Arc<AtomicBool>,
    cancellation: CancellationToken,
    tasks: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl AppState {
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let jwt = match config.auth.jwt.clone() {
            Some(value) => Some(JwtAuthenticator::open(value).await?),
            None => None,
        };
        let cancellation = CancellationToken::new();
        #[cfg(feature = "kubernetes")]
        let mut kubernetes = None;
        let (local_owner, assignment) = match &config.sharding.kind {
            #[cfg(feature = "kubernetes")]
            ShardingBackend::Kubernetes(settings) => {
                let (runtime, assignment) =
                    KubernetesRuntime::bootstrap(settings, cancellation.clone())
                        .await
                        .map_err(|error| anyhow::anyhow!(error))?;
                let local = runtime.identity().to_owned();
                kubernetes = Some(runtime);
                (local, assignment)
            }
            #[cfg(not(feature = "kubernetes"))]
            ShardingBackend::Kubernetes(_) => {
                anyhow::bail!("Kubernetes sharding requires the kubernetes feature")
            }
            _ => assignment_for(&config)?,
        };
        let shard_count = assignment.virtual_shards;
        let shards = config
            .sharding
            .startup_shards(config.mode, &assignment, &local_owner);
        let db = Arc::new(if config.mode == ServerMode::Reader {
            ShardedTrack::open_readers_with_routing(
                config.track_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
                &assignment.routing,
                slatedb::config::DbReaderOptions {
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
            )
            .await?
        } else {
            ShardedTrack::open_with_routing(
                config.track_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
                &assignment.routing,
            )
            .await?
        });
        let namespaces = config
            .namespaces
            .iter()
            .cloned()
            .map(|value| (value.name.clone(), value))
            .collect();
        let cache_warmed = !config.cache_warmer.enabled || config.mode == ServerMode::Writer;
        let state = Self {
            request_limit: Arc::new(Semaphore::new(config.request.request_concurrency)),
            query_limit: Arc::new(Semaphore::new(config.request.query_concurrency)),
            remote_limit: Arc::new(Semaphore::new(config.write.remote_concurrency)),
            config: Arc::new(config),
            db,
            jwt,
            namespaces: Arc::new(namespaces),
            assignment: Arc::new(RwLock::new(assignment)),
            local_owner,
            completed_requests: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            draining_shards: Arc::new(RwLock::new(HashSet::new())),
            dirty: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
            cache_warmed: Arc::new(AtomicBool::new(cache_warmed)),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        };
        #[cfg(feature = "kubernetes")]
        if let Some(runtime) = kubernetes {
            if state.config.mode == ServerMode::Reader {
                let task = runtime.spawn_reader(
                    Arc::clone(&state.db),
                    Arc::clone(&state.assignment),
                    &state.cancellation,
                );
                state.tasks.lock().await.push(task);
            } else {
                let lifecycle = Arc::new(TrackShardLifecycle {
                    db: Arc::clone(&state.db),
                    draining_shards: Arc::clone(&state.draining_shards),
                    assignment: Arc::clone(&state.assignment),
                });
                let tasks = runtime.spawn(
                    lifecycle,
                    Arc::new(TrackMigrationExecutor {
                        config: state.config.track_config(),
                        options: ShardingOptions::new(
                            state.assignment.read().await.virtual_shards,
                            state.config.sharding.io_concurrency_limit,
                        )?,
                    }),
                    Arc::clone(&state.assignment),
                    &state.cancellation,
                );
                state.tasks.lock().await.extend(tasks);
            }
        }
        state.start_cache_warmer().await;
        state.start_durable_flush_task().await;
        Ok(state)
    }

    pub async fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) || !self.cache_warmed.load(Ordering::Acquire) {
            return false;
        }
        match self.config.mode {
            ServerMode::Standalone | ServerMode::Reader => {
                let expected = self
                    .assignment
                    .read()
                    .await
                    .routing
                    .assignments
                    .iter()
                    .map(|assignment| assignment.shard)
                    .collect::<Vec<_>>();
                self.db.open_shards().await == expected
            }
            ServerMode::Writer => {
                let assignment = self.assignment.read().await;
                let expected = owned_shards(&assignment, &self.local_owner).collect::<Vec<_>>();
                !expected.is_empty() && self.db.open_shards().await == expected
            }
        }
    }

    async fn start_cache_warmer(&self) {
        if self.cache_warmed.load(Ordering::Acquire) {
            return;
        }
        let namespaces = self
            .config
            .namespaces
            .iter()
            .filter_map(|namespace| Namespace::new(namespace.name.clone()).ok())
            .collect::<Vec<_>>();
        let warm_range = Duration::from_secs(self.config.cache_warmer.warm_range_seconds);
        let warm_timeout = Duration::from_secs(self.config.cache_warmer.timeout_seconds);
        let include_payloads = self.config.cache_warmer.include_payloads;
        let database = Arc::clone(&self.db);
        let cancellation = self.cancellation.clone();
        let cache_warmed = Arc::clone(&self.cache_warmed);
        let task = tokio::spawn(async move {
            match tokio::time::timeout(
                warm_timeout,
                database.warm_recent(&namespaces, warm_range, include_payloads, &cancellation),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "track cache warming failed"),
                Err(_) => tracing::warn!(
                    timeout_seconds = warm_timeout.as_secs(),
                    "track cache warming timed out"
                ),
            }
            cache_warmed.store(true, Ordering::Release);
        });
        self.tasks.lock().await.push(task);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_visibility(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub(crate) async fn route_write(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        request_id: String,
    ) -> Result<(), ApiError> {
        let assignment = self.assignment.read().await.clone();
        let options = ShardingOptions::new(
            assignment.virtual_shards,
            self.config.sharding.io_concurrency_limit,
        )
        .map_err(ApiError::internal)?;
        let mut groups = HashMap::<(Owner, ShardId), Vec<Trace>>::new();
        for trace in batches.into_iter().flat_map(|batch| batch.traces) {
            let shard = options.route(&assignment.routing, namespace, trace.trace_id);
            let owner = assignment
                .owner_of(shard)
                .cloned()
                .ok_or_else(|| ApiError::unavailable("shard has no active owner"))?;
            groups.entry((owner, shard)).or_default().push(trace);
        }
        let results = stream::iter(groups.into_iter().map(|((owner, shard), traces)| {
            let state = self.clone();
            let namespace = namespace.clone();
            let request_id = format!("{request_id}-{}", shard.get());
            async move {
                let _permit = state
                    .remote_limit
                    .acquire()
                    .await
                    .map_err(|_| ApiError::unavailable("server is shutting down"))?;
                let batches = vec![TraceBatch::new(traces)];
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
        batches: Vec<TraceBatch>,
    ) -> Result<(), ApiError> {
        // Keep the read guard for the entire write so migration and shutdown
        // can use the exclusive lock as an in-flight write barrier.
        let draining = self.draining_shards.read().await;
        if draining.contains(&shard) {
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
            .map_err(ApiError::from_track)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    async fn write_remote(
        &self,
        namespace: &Namespace,
        mut owner: Owner,
        shard: ShardId,
        batches: Vec<TraceBatch>,
        mut generation: u64,
        request_id: String,
    ) -> Result<(), ApiError> {
        for attempt in 0..=self.config.write.remote_retries {
            let endpoint = owner_endpoint(&self.config, &owner)
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let mut client = InternalWriterClient::connect(format!("http://{endpoint}"))
                .await
                .map_err(|error| ApiError::unavailable(error.to_string()))?;
            let mut request = Request::new(
                to_proto_request(
                    namespace,
                    shard,
                    generation,
                    &request_id,
                    self.config.write.durability,
                    batches.clone(),
                )
                .map_err(ApiError::internal)?,
            );
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
        let open_shards = self.db.open_shards().await;
        let mut draining = self.draining_shards.write().await;
        draining.extend(open_shards);
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
    }

    async fn start_durable_flush_task(&self) {
        let seconds = self.config.write.flush_interval_seconds;
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
                        if state.dirty.swap(false, Ordering::AcqRel)
                            && let Err(error) = state.db.flush().await
                        {
                            state.ready.store(false, Ordering::Release);
                            state.dirty.store(true, Ordering::Release);
                            tracing::error!(%error, "Track durable flush failed");
                        }
                    }
                }
            }
        });
        self.tasks.lock().await.push(task);
    }
}

#[cfg(feature = "kubernetes")]
struct TrackMigrationExecutor {
    config: track::Config,
    options: ShardingOptions,
}

#[cfg(feature = "kubernetes")]
impl TrackMigrationExecutor {
    fn slate_config(
        &self,
    ) -> Result<&common::storage::config::SlateDbStorageConfig, MigrationExecutionError> {
        match &self.config.storage {
            common::StorageConfig::SlateDb(config) => Ok(config),
            common::StorageConfig::InMemory => Err(MigrationExecutionError::fatal(
                "projected shard migration requires SlateDB storage",
            )),
        }
    }

    fn spec(
        &self,
        split: &ShardSplit,
    ) -> Result<common::storage::projected_clone::ProjectedCloneSpec, MigrationExecutionError> {
        let slots = split
            .moved_range
            .slots()
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        let source = self
            .options
            .shard_storage(&self.config, split.source_shard)
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        let target = self
            .options
            .shard_storage(&self.config, split.target_shard)
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        let common::StorageConfig::SlateDb(source) = source.storage else {
            return Err(MigrationExecutionError::fatal(
                "projected shard migration requires SlateDB storage",
            ));
        };
        let common::StorageConfig::SlateDb(target) = target.storage else {
            return Err(MigrationExecutionError::fatal(
                "projected shard migration requires SlateDB storage",
            ));
        };
        Ok(common::storage::projected_clone::ProjectedCloneSpec {
            source_path: source.path,
            target_path: target.path,
            checkpoint_name: format!(
                "telemetry-migration-{}-{}-{}-{}",
                split.source_shard.get(),
                split.target_shard.get(),
                slots.start,
                slots.end
            ),
            slot_start: slots.start,
            slot_end: slots.end,
            segment_extractor_name: track::SEGMENT_EXTRACTOR_NAME.to_owned(),
        })
    }

    fn map_error(
        error: common::storage::projected_clone::ProjectedCloneError,
    ) -> MigrationExecutionError {
        if error.is_fatal() {
            MigrationExecutionError::fatal(error.to_string())
        } else {
            MigrationExecutionError::Retryable(Box::new(error))
        }
    }
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardMigrationExecutor for TrackMigrationExecutor {
    async fn preflight_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError> {
        let object_store = common::create_object_store(&self.slate_config()?.object_store)
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        common::storage::projected_clone::preflight_projected_clone(
            &self.spec(split)?,
            object_store,
        )
        .await
        .map_err(Self::map_error)
    }

    async fn clone_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError> {
        let object_store = common::create_object_store(&self.slate_config()?.object_store)
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        common::storage::projected_clone::execute_projected_clone(&self.spec(split)?, object_store)
            .await
            .map_err(Self::map_error)
    }
}

#[cfg(feature = "kubernetes")]
struct TrackShardLifecycle {
    db: Arc<ShardedTrack>,
    draining_shards: Arc<RwLock<HashSet<ShardId>>>,
    assignment: Arc<RwLock<ShardMap>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for TrackShardLifecycle {
    async fn open(
        &self,
        shard: ShardId,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        let slots = self
            .assignment
            .read()
            .await
            .routing
            .assignments
            .iter()
            .find(|assignment| assignment.shard == shard)
            .ok_or_else(|| format!("missing routing range for shard {}", shard.get()))?
            .range
            .slots()?;
        self.db.open_shard_with_slots(shard, slots).await?;
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

pub(crate) fn assignment_for(config: &Config) -> anyhow::Result<(String, ShardMap)> {
    Ok(config.sharding.static_assignment(config.listeners.grpc)?)
}

fn owner_endpoint(config: &Config, owner: &Owner) -> anyhow::Result<String> {
    config
        .sharding
        .owner_endpoint(config.listeners.grpc, owner)
        .ok_or_else(|| anyhow::anyhow!("owner endpoint is unknown"))
}

pub(crate) fn durability(value: crate::config::Durability) -> track::Durability {
    match value {
        crate::config::Durability::Applied => track::Durability::Applied,
        crate::config::Durability::Written => track::Durability::Written,
        crate::config::Durability::Durable => track::Durability::Durable,
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;
    use crate::config::{KubernetesShardingConfig, ShardingConfig};

    fn batch_on_shard(
        namespace: &Namespace,
        routing: &sharding::HashRangeMap,
        shard: u32,
    ) -> TraceBatch {
        let options = ShardingOptions::new(2, 4).unwrap();
        let trace = (1..=u8::MAX)
            .map(|id| {
                let trace_id = track::TraceId::new([id; 16]).unwrap();
                Trace::new(
                    trace_id,
                    vec![ResourceSpans {
                        scope_spans: vec![ScopeSpans {
                            spans: vec![Span {
                                trace_id: trace_id.as_bytes().to_vec(),
                                span_id: vec![id; 8],
                                name: "admitted".into(),
                                start_time_unix_nano: 1,
                                end_time_unix_nano: 2,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                )
                .unwrap()
            })
            .find(|trace| options.route(routing, namespace, trace.trace_id).get() == shard)
            .unwrap();
        TraceBatch::new(vec![trace])
    }

    #[test]
    fn kubernetes_assignment_uses_stable_statefulset_endpoint() {
        let config = Config {
            mode: ServerMode::Writer,
            sharding: ShardingConfig {
                virtual_shards: 8,
                io_concurrency_limit: 64,
                kind: ShardingBackend::Kubernetes(KubernetesShardingConfig {
                    namespace: "observability".into(),
                    stateful_set: "track-writer".into(),
                    headless_service: "track-writer-headless".into(),
                    owner_port: 9092,
                    ..Default::default()
                }),
            },
            ..Config::default()
        };
        let (_, assignment) = assignment_for(&config).unwrap();
        let owner = assignment.owner_of(ShardId::new(0)).unwrap();
        assert_eq!(
            owner_endpoint(&config, owner).unwrap(),
            "track-writer-0.track-writer-headless.observability.svc:9092"
        );
    }

    #[tokio::test]
    async fn drain_waits_for_admitted_write_guard_and_rejects_later_writes() {
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
        let routing = state.assignment.read().await.routing.clone();
        let batch = batch_on_shard(&namespace, &routing, 1);
        let shard = state
            .db
            .route(&routing, &namespace, batch.traces[0].trace_id);

        // This is the admission guard held by write_local for the full write.
        let admitted = state.draining_shards.read().await;
        let draining = Arc::clone(&state.draining_shards);
        let drain = tokio::spawn(async move {
            draining.write().await.insert(shard);
        });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "drain passed an admitted in-flight write"
        );

        drop(admitted);
        drain.await.unwrap();
        let error = state
            .write_local(&namespace, shard, vec![batch])
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("local shard is draining"));
        state.shutdown().await.unwrap();
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn track_lifecycle_opens_drains_flushes_and_closes_shard() {
        let db = Arc::new(
            ShardedTrack::open(
                track::Config {
                    storage: StorageConfig::InMemory,
                    ..track::Config::default()
                },
                ShardingOptions::new(2, 4).unwrap(),
                [],
            )
            .await
            .unwrap(),
        );
        let draining = Arc::new(RwLock::new(HashSet::new()));
        let lifecycle = TrackShardLifecycle {
            db: Arc::clone(&db),
            draining_shards: Arc::clone(&draining),
            assignment: Arc::new(RwLock::new(assignment_for(&Config::default()).unwrap().1)),
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
}
