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
use sharding::{Owner, ShardId, ShardMap, server::owned_shards};
use tokio::{sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{Request, metadata::MetadataValue};

#[cfg(feature = "kubernetes")]
use sharding::{
    AssignmentGeneration, BoxError, MigrationExecutionError, ShardLifecycle,
    ShardMigrationExecutor, ShardSplit, server::KubernetesRuntime,
};

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend},
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
            ShardedLine::open_readers_with_routing(
                config.line_config(),
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
            ShardedLine::open_with_routing(
                config.line_config(),
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
        if let Some(runtime) = kubernetes {
            if state.config.mode == ServerMode::Reader {
                let task = runtime.spawn_reader(
                    Arc::clone(&state.db),
                    Arc::clone(&state.assignment),
                    &state.cancellation,
                );
                state.tasks.lock().await.push(task);
            } else {
                let lifecycle = Arc::new(LineShardLifecycle {
                    db: Arc::clone(&state.db),
                    draining_shards: Arc::clone(&state.draining_shards),
                    assignment: Arc::clone(&state.assignment),
                });
                let tasks = runtime.spawn(
                    lifecycle,
                    Arc::new(LineMigrationExecutor {
                        config: state.config.line_config(),
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
        state.start_durable_flush_task().await;
        Ok(state)
    }

    pub async fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) {
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
        let assignment = self.assignment.read().await.clone();
        let options = ShardingOptions::new(
            assignment.virtual_shards,
            self.config.sharding.io_concurrency_limit,
        )
        .map_err(ApiError::internal)?;
        let mut groups: HashMap<(Owner, ShardId), Vec<LogBatch>> = HashMap::new();
        for batch in batches {
            let shard = options.route(&assignment.routing, namespace, &batch.labels);
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
        // Hold the read guard until the write completes. Migration and
        // shutdown take the lock exclusively before flushing, which makes
        // their flush a barrier for every write admitted before the drain.
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
            .map_err(ApiError::from_line)?;
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
                        if state.dirty.swap(false, Ordering::AcqRel) {
                            if let Err(error) = state.db.flush().await {
                                state.ready.store(false, Ordering::Release);
                                state.dirty.store(true, Ordering::Release);
                                tracing::error!(%error, "Line durable flush failed");
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
struct LineMigrationExecutor {
    config: line::Config,
    options: ShardingOptions,
}

#[cfg(feature = "kubernetes")]
impl LineMigrationExecutor {
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
            segment_extractor_name: line::SEGMENT_EXTRACTOR_NAME.to_owned(),
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
impl ShardMigrationExecutor for LineMigrationExecutor {
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
struct LineShardLifecycle {
    db: Arc<ShardedLine>,
    draining_shards: Arc<tokio::sync::RwLock<HashSet<ShardId>>>,
    assignment: Arc<tokio::sync::RwLock<ShardMap>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for LineShardLifecycle {
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

fn durability(value: Durability) -> LineDurability {
    match value {
        Durability::Applied => LineDurability::Applied,
        Durability::Written => LineDurability::Written,
        Durability::Durable => LineDurability::Durable,
    }
}

pub(crate) fn assignment_for(config: &Config) -> anyhow::Result<(String, ShardMap)> {
    Ok(config.sharding.static_assignment(config.listeners.grpc)?)
}

fn owner_endpoint(config: &Config, owner: &Owner) -> Result<String, ApiError> {
    config
        .sharding
        .owner_endpoint(config.listeners.grpc, owner)
        .ok_or_else(|| ApiError::unavailable("owner endpoint is unknown"))
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use line::{Label, Labels, LogEntry, QueryOptions, QueryRequest, QueryResult};
    use sharding::{Assignment, AssignmentGeneration, AssignmentState, Owner, ShardRange};
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    use super::*;
    use crate::{
        config::{ShardingConfig, StaticOwner},
        grpc_service,
    };

    fn config(owner_id: &str, endpoint: String) -> Config {
        Config {
            mode: ServerMode::Writer,
            storage: StorageConfig::InMemory,
            sharding: ShardingConfig {
                virtual_shards: 2,
                io_concurrency_limit: 64,
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
        let routing = sharding::HashRangeMap::bootstrap(2).unwrap();
        let labels = (0..10_000)
            .map(|candidate| {
                Labels::new(vec![Label::new("app", format!("api-{candidate}"))]).unwrap()
            })
            .find(|labels| options.route(&routing, namespace, labels).get() == shard)
            .unwrap();
        LogBatch::new(labels, vec![LogEntry::new(1, "forwarded")])
    }

    fn with_generation(map: &ShardMap, generation: u64) -> ShardMap {
        ShardMap::with_routing(
            AssignmentGeneration::new(generation),
            map.virtual_shards,
            map.routing.clone(),
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
        let routing = state.assignment.read().await.routing.clone();
        let shard = state.db.route(&routing, &namespace, &batch.labels);
        state.draining_shards.write().await.insert(shard);

        assert!(
            state
                .write_local(&namespace, shard, vec![batch])
                .await
                .is_err()
        );
        state.shutdown().await.unwrap();
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
        let batch = batch_on_shard(&namespace, 1);
        let routing = state.assignment.read().await.routing.clone();
        let shard = state.db.route(&routing, &namespace, &batch.labels);

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
            assignment: Arc::new(tokio::sync::RwLock::new(
                ShardMap::new(
                    AssignmentGeneration::new(1),
                    2,
                    vec![Assignment::new(
                        Owner::new("test", 0),
                        ShardRange::within(0, 2, 2).unwrap(),
                        AssignmentState::Active,
                    )],
                )
                .unwrap(),
            )),
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

        writer_b.db.flush().await.unwrap();
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
