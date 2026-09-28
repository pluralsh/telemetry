use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
use sharding::{AssignmentGeneration, BoxError, ShardLifecycle, server::KubernetesRuntime};

use crate::{
    config::{Config, NamespaceConfig, ServerMode, ShardingBackend},
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
    ready: Arc<AtomicBool>,
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
                let (runtime, assignment) = KubernetesRuntime::bootstrap(
                    settings,
                    config.sharding.virtual_shards,
                    cancellation.clone(),
                )
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
        let shards = config
            .sharding
            .startup_shards(config.mode, &assignment, &local_owner);
        let db = Arc::new(
            ShardedTrack::open(
                config.track_config(),
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
            .map(|value| (value.name.clone(), value))
            .collect();
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
            ready: Arc::new(AtomicBool::new(true)),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        };
        #[cfg(feature = "kubernetes")]
        if let Some(runtime) = kubernetes
            && state.config.mode != ServerMode::Reader
        {
            let lifecycle = Arc::new(TrackShardLifecycle {
                db: Arc::clone(&state.db),
                draining_shards: Arc::clone(&state.draining_shards),
            });
            let tasks = runtime.spawn(
                lifecycle,
                Arc::clone(&state.assignment),
                &state.cancellation,
            );
            state.tasks.lock().await.extend(tasks);
        }
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
                let expected = owned_shards(&assignment, &self.local_owner).collect::<Vec<_>>();
                !expected.is_empty() && self.db.open_shards().await == expected
            }
        }
    }

    pub(crate) async fn route_write(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        request_id: String,
    ) -> anyhow::Result<()> {
        let options = ShardingOptions::new(
            self.config.sharding.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )?;
        let assignment = self.assignment.read().await.clone();
        let mut groups = HashMap::<(Owner, ShardId), Vec<Trace>>::new();
        for trace in batches.into_iter().flat_map(|batch| batch.traces) {
            let shard = options.route(namespace, trace.trace_id);
            let owner = assignment
                .owner_of(shard)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("shard has no active owner"))?;
            groups.entry((owner, shard)).or_default().push(trace);
        }
        let results = stream::iter(groups.into_iter().map(|((owner, shard), traces)| {
            let state = self.clone();
            let namespace = namespace.clone();
            let request_id = format!("{request_id}-{}", shard.get());
            async move {
                let _permit = state.remote_limit.acquire().await?;
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
    ) -> anyhow::Result<()> {
        if self.draining_shards.read().await.contains(&shard) {
            anyhow::bail!("local shard is draining");
        }
        let database = self
            .db
            .shard(shard)
            .await
            .ok_or_else(|| anyhow::anyhow!("local shard is not open"))?;
        database
            .write_with_durability(namespace, batches, durability(self.config.write.durability))
            .await?;
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
    ) -> anyhow::Result<()> {
        for attempt in 0..=self.config.write.remote_retries {
            let endpoint = owner_endpoint(&self.config, &owner)?;
            let mut client = InternalWriterClient::connect(format!("http://{endpoint}")).await?;
            let mut request = Request::new(to_proto_request(
                namespace,
                shard,
                generation,
                &request_id,
                self.config.write.durability,
                batches.clone(),
            )?);
            if let Some(secret) = &self.config.auth.internal {
                request.metadata_mut().insert(
                    "authorization",
                    MetadataValue::try_from(format!("Bearer {}", secret.expose()?))?,
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
                        .ok_or_else(|| anyhow::anyhow!("shard owner disappeared"))?;
                }
                Err(status) => return Err(status.into()),
            }
        }
        anyhow::bail!("remote write retries exhausted")
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
}

#[cfg(feature = "kubernetes")]
struct TrackShardLifecycle {
    db: Arc<ShardedTrack>,
    draining_shards: Arc<RwLock<HashSet<ShardId>>>,
}

#[cfg(feature = "kubernetes")]
#[tonic::async_trait]
impl ShardLifecycle for TrackShardLifecycle {
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
    use super::*;
    use crate::config::{KubernetesShardingConfig, ShardingConfig};

    #[test]
    fn kubernetes_assignment_uses_stable_statefulset_endpoint() {
        let config = Config {
            mode: ServerMode::Writer,
            sharding: ShardingConfig {
                virtual_shards: 8,
                io_concurrency_multiplier: 4,
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

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn track_lifecycle_opens_drains_flushes_and_closes_shard() {
        use common::storage::config::StorageConfig;

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
