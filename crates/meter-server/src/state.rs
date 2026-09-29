use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{StreamExt, stream};
use meter::{Namespace, Series, ShardedMeter, ShardingOptions, Visibility};
use proto::meter::internal::v1::internal_writer_client::InternalWriterClient;
use sharding::{Owner, ShardId, ShardMap, server::owned_shards};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::{Request, metadata::MetadataValue};

#[cfg(feature = "kubernetes")]
use sharding::{
    AssignmentGeneration, BoxError, MigrationExecutionError, ShardLifecycle,
    ShardMigrationExecutor, ShardSplit, server::KubernetesRuntime,
};

use crate::{
    auth::JwtAuthenticator,
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend},
    http::ApiError,
    internal_writer::to_proto_request,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) writers: Option<Arc<ShardedMeter>>,
    pub(crate) readers: Option<Arc<ShardedMeter>>,
    pub(crate) assignment: Arc<RwLock<ShardMap>>,
    pub(crate) local_owner: String,
    pub(crate) remote_limit: Arc<Semaphore>,
    pub(crate) completed_requests: Arc<Mutex<HashSet<(String, String)>>>,
    pub(crate) draining_shards: Arc<RwLock<HashSet<ShardId>>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) background_tasks: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    pub(crate) flush_runs: Arc<AtomicU64>,
    pub(crate) cache_warmed: Arc<AtomicBool>,
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
        let options = ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?;
        let meter_config = meter_config(&config);
        let writer_shards = if config.mode == ServerMode::Standalone {
            (0..shard_count).map(ShardId::new).collect::<Vec<_>>()
        } else if matches!(config.sharding.kind, ShardingBackend::Kubernetes(_)) {
            Vec::new()
        } else {
            owned_shards(&assignment, &local_owner).collect()
        };
        let writers = if config.mode != ServerMode::Reader {
            Some(Arc::new(
                ShardedMeter::open_writers_with_routing(
                    meter_config.clone(),
                    options,
                    writer_shards,
                    &assignment.routing,
                )
                .await?,
            ))
        } else {
            None
        };
        let readers = if config.mode == ServerMode::Standalone {
            writers.clone()
        } else if config.mode != ServerMode::Writer {
            Some(Arc::new(
                ShardedMeter::open_readers_with_routing(
                    meter_config,
                    options,
                    (0..shard_count).map(ShardId::new),
                    &assignment.routing,
                    slatedb::config::DbReaderOptions {
                        skip_wal_replay: false,
                        ..slatedb::config::DbReaderOptions::default()
                    },
                    config.reader_cache_capacity,
                )
                .await?,
            ))
        } else {
            None
        };
        let cache_warmed = !config.cache_warmer.enabled || config.mode == ServerMode::Writer;
        let state = Self {
            remote_limit: Arc::new(Semaphore::new(config.write.remote_concurrency)),
            config: Arc::new(config),
            jwt,
            writers,
            readers,
            assignment: Arc::new(RwLock::new(assignment)),
            local_owner,
            completed_requests: Arc::new(Mutex::new(HashSet::new())),
            draining_shards: Arc::new(RwLock::new(HashSet::new())),
            cancellation,
            background_tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            flush_runs: Arc::new(AtomicU64::new(0)),
            cache_warmed: Arc::new(AtomicBool::new(cache_warmed)),
        };
        #[cfg(feature = "kubernetes")]
        if let Some(runtime) = kubernetes {
            if state.config.mode == ServerMode::Reader {
                let task = runtime.spawn_reader(
                    state
                        .readers
                        .as_ref()
                        .expect("reader mode must open sharded meter")
                        .clone(),
                    Arc::clone(&state.assignment),
                    &state.cancellation,
                );
                state.background_tasks.lock().await.push(task);
            } else {
                let lifecycle = Arc::new(MeterShardLifecycle {
                    writers: state
                        .writers
                        .as_ref()
                        .expect("writer mode must open sharded meter")
                        .clone(),
                    draining_shards: Arc::clone(&state.draining_shards),
                    assignment: Arc::clone(&state.assignment),
                });
                let tasks = runtime.spawn(
                    lifecycle,
                    Arc::new(MeterMigrationExecutor {
                        storage: state.config.storage.clone(),
                        options,
                    }),
                    Arc::clone(&state.assignment),
                    &state.cancellation,
                );
                state.background_tasks.lock().await.extend(tasks);
            }
        }
        state.start_cache_warmer().await;
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
        if let Some(writers) = &self.writers {
            writers.close().await?;
        }
        if let Some(readers) = &self.readers
            && self
                .writers
                .as_ref()
                .is_none_or(|writers| !Arc::ptr_eq(writers, readers))
        {
            readers.close().await?;
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
        if !self.cache_warmed.load(Ordering::Acquire) {
            return false;
        }
        match self.config.mode {
            ServerMode::Standalone => self.readers.is_some(),
            ServerMode::Reader => {
                let expected = self
                    .assignment
                    .read()
                    .await
                    .routing
                    .assignments
                    .iter()
                    .map(|assignment| assignment.shard)
                    .collect::<Vec<_>>();
                match &self.readers {
                    Some(readers) => readers.reader_shards().await == expected,
                    None => false,
                }
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
                    .collect::<HashSet<_>>();
                let Some(writers) = &self.writers else {
                    return false;
                };
                !expected.is_empty()
                    && writers
                        .writer_shards()
                        .await
                        .into_iter()
                        .collect::<HashSet<_>>()
                        == expected
            }
        }
    }

    async fn start_cache_warmer(&self) {
        if self.cache_warmed.load(Ordering::Acquire) {
            return;
        }
        let Some(readers) = self.readers.clone() else {
            self.cache_warmed.store(true, Ordering::Release);
            return;
        };
        let namespaces = self
            .config
            .namespaces
            .iter()
            .filter_map(|namespace| Namespace::new(namespace.name.clone()).ok())
            .collect::<Vec<_>>();
        let warm_range = Duration::from_secs(self.config.cache_warmer.warm_range_seconds);
        let warm_timeout = Duration::from_secs(self.config.cache_warmer.timeout_seconds);
        let include_payloads = self.config.cache_warmer.include_payloads;
        let cancellation = self.cancellation.clone();
        let cache_warmed = Arc::clone(&self.cache_warmed);
        let task = tokio::spawn(async move {
            match tokio::time::timeout(
                warm_timeout,
                readers.warm_recent(&namespaces, warm_range, include_payloads, &cancellation),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "meter cache warming failed"),
                Err(_) => tracing::warn!(
                    timeout_seconds = warm_timeout.as_secs(),
                    "meter cache warming timed out"
                ),
            }
            cache_warmed.store(true, Ordering::Release);
        });
        self.background_tasks.lock().await.push(task);
    }

    async fn flush_active_writers(&self) -> anyhow::Result<()> {
        self.flush_runs.fetch_add(1, Ordering::Relaxed);
        if let Some(writers) = &self.writers {
            writers.flush().await?;
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
        let assignment = self.assignment.read().await.clone();
        let options = ShardingOptions::new(
            assignment.virtual_shards,
            self.config.sharding.io_concurrency_limit,
        )
        .map_err(ApiError::internal)?;
        let mut groups: HashMap<(Owner, ShardId), Vec<Series>> = HashMap::new();
        for item in series {
            let shard = options.route(&assignment.routing, &meter_namespace, &item.labels);
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
        // Held across the write: `drain` takes this lock exclusively, so it
        // waits for in-flight writes before the shard is flushed and handed off.
        let draining = self.draining_shards.read().await;
        if draining.contains(&shard) {
            return Err(ApiError::unavailable("local shard is draining"));
        }
        if let Some(writers) = &self.writers {
            return writers
                .write_shard(
                    &Namespace::new(namespace)
                        .map_err(|error| ApiError::bad_request(error.to_string()))?,
                    shard,
                    series,
                    visibility(durability),
                )
                .await
                .map_err(ApiError::from_meter);
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
struct MeterMigrationExecutor {
    storage: common::storage::config::SlateDbStorageConfig,
    options: ShardingOptions,
}

#[cfg(feature = "kubernetes")]
impl MeterMigrationExecutor {
    fn spec(
        &self,
        split: &ShardSplit,
    ) -> Result<common::storage::projected_clone::ProjectedCloneSpec, MigrationExecutionError> {
        let slots = split
            .moved_range
            .slots()
            .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?;
        Ok(common::storage::projected_clone::ProjectedCloneSpec {
            source_path: self
                .options
                .shard_path(&self.storage.path, split.source_shard)
                .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?,
            target_path: self
                .options
                .shard_path(&self.storage.path, split.target_shard)
                .map_err(|error| MigrationExecutionError::fatal(error.to_string()))?,
            checkpoint_name: format!(
                "telemetry-migration-{}-{}-{}-{}",
                split.source_shard.get(),
                split.target_shard.get(),
                slots.start,
                slots.end
            ),
            slot_start: slots.start,
            slot_end: slots.end,
            segment_extractor_name: meter::SEGMENT_EXTRACTOR_NAME.to_owned(),
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
impl ShardMigrationExecutor for MeterMigrationExecutor {
    async fn preflight_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError> {
        let object_store =
            common::create_object_store(&self.storage.object_store).map_err(|error| {
                MigrationExecutionError::fatal(format!("object store configuration: {error}"))
            })?;
        common::storage::projected_clone::preflight_projected_clone(
            &self.spec(split)?,
            object_store,
        )
        .await
        .map_err(Self::map_error)
    }

    async fn clone_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError> {
        let object_store =
            common::create_object_store(&self.storage.object_store).map_err(|error| {
                MigrationExecutionError::fatal(format!("object store configuration: {error}"))
            })?;
        common::storage::projected_clone::execute_projected_clone(&self.spec(split)?, object_store)
            .await
            .map_err(Self::map_error)
    }
}

#[cfg(feature = "kubernetes")]
struct MeterShardLifecycle {
    writers: Arc<ShardedMeter>,
    draining_shards: Arc<RwLock<HashSet<ShardId>>>,
    assignment: Arc<RwLock<ShardMap>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for MeterShardLifecycle {
    async fn open(
        &self,
        shard: ShardId,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        {
            let mut draining = self.draining_shards.write().await;
            draining.remove(&shard);
        }
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
        self.writers
            .open_writer_shard_with_slots(shard, slots)
            .await?;
        Ok(())
    }

    async fn drain(&self, shard: ShardId) -> Result<(), BoxError> {
        self.draining_shards.write().await.insert(shard);
        Ok(())
    }

    async fn flush(&self, shard: ShardId) -> Result<(), BoxError> {
        self.writers.flush_shard(shard).await?;
        Ok(())
    }

    async fn close(&self, shard: ShardId) -> Result<(), BoxError> {
        self.writers.close_writer_shard(shard).await?;
        Ok(())
    }
}

pub(crate) fn meter_config(config: &Config) -> meter::Config {
    meter::Config {
        storage: config.storage.clone(),
        flush_interval: Duration::from_secs(config.write.flush_interval_seconds),
        retention: None,
        write_buffer: common::coordinator::WriteCoordinatorConfig {
            queue_capacity: config.write.buffer_queue_capacity,
            flush_interval: Duration::from_millis(config.write.buffer_flush_interval_milliseconds),
            flush_size_threshold: config.write.buffer_size_threshold_bytes,
        },
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

fn visibility(durability: Durability) -> Visibility {
    match durability {
        Durability::Applied => Visibility::Applied,
        Durability::Written => Visibility::Written,
        Durability::Durable => Visibility::Durable,
    }
}
