use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::storage::health::StorageHealth;
use plural_traces::{Namespace, ShardedTraces, ShardingOptions, Trace, TraceBatch};
use proto::traces::internal::v1::internal_writer_client::InternalWriterClient;
use server_common::auth::JwtAuthenticator;
use server_common::health::ErrorThrottle;
use server_common::ingest::{IngestPipeline, Signal};
use server_common::internal_rpc::{self, ChannelPool};
use server_common::read_groups::ReadGroups;
use server_common::reload::{self, Live, LiveConfig};
use server_common::tasks::spawn_periodic;
use server_common::warmer::{
    WarmPass, WarmState, readiness_waits, spawn_cache_warmer, warming_enabled,
};
use sharding::{
    AssignmentGeneration, EpochPolicy, ForwardError, Owner, RouterLimits, ShardId, WriteRouter,
    server::ClusterMembership, shard_request_id,
};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{
    config::{Config, NamespaceConfig, ServerMode},
    http::ApiError,
    internal_writer::to_proto_request,
    shard_reader::GrpcTracesShards,
};

#[derive(Clone)]
pub struct AppState {
    /// The config the process started with; see `live` for reloaded settings.
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<ShardedTraces>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) live: Arc<Live<LiveConfig<NamespaceConfig>>>,
    warmer: Arc<Mutex<Option<CancellationToken>>>,
    warm_state: Arc<WarmState>,
    pub(crate) router: Arc<WriteRouter>,
    pub(crate) completed_requests: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub(crate) request_limit: Arc<Semaphore>,
    pub(crate) query_limit: Arc<Semaphore>,
    pub(crate) ingest: IngestPipeline,
    channels: Arc<ChannelPool>,
    dirty: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    cache_warmed: Arc<AtomicBool>,
    pub(crate) storage_health: StorageHealth,
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
        let ClusterMembership {
            local_owner,
            assignment,
            runtime,
        } = config
            .sharding
            .join(config.listeners.grpc, EpochPolicy::default(), &cancellation)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        let shard_count = assignment.shard_count;
        let shards = config
            .sharding
            .startup_shards(config.mode, &assignment, &local_owner);
        let db = Arc::new(if config.mode == ServerMode::Reader {
            ShardedTraces::open_readers(
                config.traces_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
                slatedb::config::DbReaderOptions {
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
            )
            .await?
        } else {
            ShardedTraces::open(
                config.traces_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
            )
            .await?
        });
        if let Some(groups) = ReadGroups::configured(config.mode, &config.sharding, &config.auth)? {
            db.shards().scope_readers_to(groups.local_shard());
            db.connect_remote_shards(Arc::new(GrpcTracesShards::new(Arc::new(groups))));
        }
        let cache_warmed =
            !readiness_waits(&config.cache_warmer, config.mode != ServerMode::Writer);
        let router = WriteRouter::new(
            local_owner,
            Arc::new(RwLock::new(assignment)),
            RouterLimits {
                remote_concurrency: config.write.remote_concurrency,
                remote_retries: config.write.remote_retries,
            },
        );
        let (ingest, ingest_tasks) = IngestPipeline::standard(
            Signal::Traces,
            config.namespaces.iter().map(|namespace| {
                (
                    namespace.name.as_str(),
                    namespace.usage_reporting_endpoint.as_deref(),
                )
            }),
            &config.usage_reporting,
            &cancellation,
        )?;
        let state = Self {
            request_limit: Arc::new(Semaphore::new(config.request.request_concurrency)),
            query_limit: Arc::new(Semaphore::new(config.request.query_concurrency)),
            live: Arc::new(Live::new(live_config(&config))),
            warmer: Arc::default(),
            warm_state: Arc::default(),
            config: Arc::new(config),
            db,
            jwt,
            router: Arc::new(router),
            completed_requests: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            ingest,
            channels: Arc::new(ChannelPool::default()),
            dirty: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
            cache_warmed: Arc::new(AtomicBool::new(cache_warmed)),
            storage_health: StorageHealth::global().clone(),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(ingest_tasks)),
        };
        let cluster_tasks = runtime.spawn(state.db.shards(), &state.router, &state.cancellation);
        state.tasks.lock().await.extend(cluster_tasks);
        state.start_cache_warmer(true).await;
        state.start_durable_flush_task().await;
        state.start_rollup_task().await;
        Ok(state)
    }

    /// The `ShardReader` service of a reader group member, which serves its
    /// shard to the other groups' coordinators.
    pub fn shard_reader_service(
        &self,
    ) -> anyhow::Result<
        Option<
            proto::traces::internal::v1::shard_reader_server::ShardReaderServer<
                crate::shard_reader::ShardReaderService,
            >,
        >,
    > {
        if self.config.mode != ServerMode::Reader || self.config.sharding.read_groups.is_none() {
            return Ok(None);
        }
        let token = self.config.auth.internal_token()?;
        Ok(Some(crate::shard_reader::shard_reader_service(
            Arc::clone(&self.db),
            token,
        )))
    }

    pub(crate) fn live(&self) -> Arc<LiveConfig<NamespaceConfig>> {
        self.live.load()
    }

    /// Applies new revisions of the config file at `path` until shutdown.
    pub async fn watch_config(&self, path: PathBuf) {
        let state = self.clone();
        let task = reload::spawn_config_watcher(
            "traces",
            path,
            reload::POLL_INTERVAL,
            self.cancellation.clone(),
            move |raw| {
                let state = state.clone();
                async move {
                    let next = Config::from_yaml(&raw).map_err(|error| error.to_string())?;
                    Ok(state.reload(&next).await)
                }
            },
        );
        self.tasks.lock().await.push(task);
    }

    /// Applies the live settings of `next` and returns the sections that
    /// still differ from the running config until a restart.
    pub(crate) async fn reload(&self, next: &Config) -> Vec<String> {
        let live = live_config(next);
        let restart_warmer = self.live().warms_differently(&live);
        self.live.store(live);
        if restart_warmer {
            self.start_cache_warmer(false).await;
        }
        reload::restart_required(self.config.as_ref(), next)
    }

    pub async fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) || !self.cache_warmed.load(Ordering::Acquire) {
            return false;
        }
        let assignment = self.router.assignment().read().await;
        self.db
            .shards()
            .serves(self.config.mode, &assignment, self.router.local_owner())
            .await
    }

    /// (Re)starts the warmer from the live config, replacing any running one.
    /// Only the first start runs the startup pass.
    async fn start_cache_warmer(&self, startup: bool) {
        if let Some(previous) = self.warmer.lock().expect("warmer lock").take() {
            previous.cancel();
        }
        let live = self.live();
        let serves_reads = self.config.mode != ServerMode::Writer;
        if !warming_enabled(&live.cache_warmer, serves_reads) {
            return;
        }
        let mut config = live.cache_warmer.clone();
        config.enabled &= startup;
        let namespaces: Arc<[Namespace]> = live
            .namespaces
            .keys()
            .filter_map(|name| Namespace::new(name.clone()).ok())
            .collect();
        let cancel = self.cancellation.child_token();
        *self.warmer.lock().expect("warmer lock") = Some(cancel.clone());
        let database = Arc::clone(&self.db);
        let task = spawn_cache_warmer(
            "traces",
            &config,
            &self.warm_state,
            cancel,
            Arc::clone(&self.cache_warmed),
            move |pass: WarmPass| {
                let database = Arc::clone(&database);
                let namespaces = Arc::clone(&namespaces);
                async move {
                    database
                        .warm_recent(
                            &namespaces,
                            pass.window,
                            pass.concurrency,
                            &pass.cancel,
                            &pass.ledger,
                            pass.shares,
                        )
                        .await
                        .map(drop)
                }
            },
        );
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
        let assignment = self.router.assignment().read().await.clone();
        let mut groups = HashMap::<ShardId, Vec<Trace>>::new();
        for trace in batches.into_iter().flat_map(|batch| batch.traces) {
            groups
                .entry(plural_traces::routing::route_trace(
                    &assignment,
                    namespace,
                    &trace,
                ))
                .or_default()
                .push(trace);
        }
        let groups = groups
            .into_iter()
            .map(|(shard, traces)| (shard, vec![TraceBatch::new(traces)]));
        self.router
            .dispatch(
                &assignment,
                groups,
                |shard, batches| async move {
                    self.write_local(
                        namespace,
                        shard,
                        batches,
                        durability(self.config.write.durability),
                    )
                    .await
                    .map_err(crate::http::traces_error)
                },
                |owner, shard, generation, batches| {
                    let request_id = shard_request_id(&request_id, shard);
                    async move {
                        self.write_remote(namespace, owner, shard, batches, generation, &request_id)
                            .await
                    }
                },
            )
            .await
    }

    /// Writes to an open local shard. Callers must hold the router's
    /// admission guard for `shard`.
    pub(crate) async fn write_local(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        batches: Vec<TraceBatch>,
        durability: plural_traces::Durability,
    ) -> Result<(), plural_traces::Error> {
        self.db
            .shards()
            .require(shard)
            .await?
            .write_with_durability(namespace, batches, durability)
            .await?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    async fn write_remote(
        &self,
        namespace: &Namespace,
        owner: Owner,
        shard: ShardId,
        batches: Vec<TraceBatch>,
        generation: AssignmentGeneration,
        request_id: &str,
    ) -> Result<(), ApiError> {
        let token = self
            .config
            .auth
            .internal_token()
            .map_err(ApiError::internal)?;
        self.router
            .forward(
                owner,
                shard,
                generation,
                &batches,
                |owner, generation, batches| {
                    let request = to_proto_request(
                        namespace,
                        shard,
                        generation.get(),
                        request_id,
                        self.config.write.durability,
                        batches.clone(),
                    );
                    let endpoint = owner_endpoint(&self.config, owner);
                    let token = token.as_deref();
                    async move {
                        let request = request
                            .map_err(|error| ForwardError::Failed(ApiError::internal(error)))?;
                        let endpoint = endpoint
                            .map_err(|error| ForwardError::Failed(ApiError::unavailable(error)))?;
                        internal_rpc::forward_write(
                            &self.channels,
                            &endpoint,
                            token,
                            request,
                            |channel, request| async move {
                                internal_writer_client(channel).write(request).await
                            },
                        )
                        .await
                    }
                },
            )
            .await
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.ready.store(false, Ordering::Release);
        self.cancellation.cancel();
        for task in self.tasks.lock().await.drain(..) {
            task.await?;
        }
        self.router
            .start_draining(self.db.shards().ids().await)
            .await;
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
    }

    /// Periodically rolls closed days of segments up into wider ones.
    async fn start_rollup_task(&self) {
        let rollup = self.config.rollup.rollup();
        if !rollup.enabled || self.config.mode == ServerMode::Reader {
            return;
        }
        let state = self.clone();
        let task = spawn_periodic(rollup.interval, self.cancellation.clone(), move || {
            let state = state.clone();
            let rollup = rollup.clone();
            async move {
                let rolled = tokio::select! {
                    () = state.cancellation.cancelled() => return,
                    rolled = state.db.rollup(&rollup) => rolled,
                };
                common::storage::rollup::record_pass("traces", rolled.as_ref().ok().copied());
                if let Err(error) = rolled {
                    tracing::warn!(%error, "Traces segment rollup failed");
                }
            }
        });
        self.tasks.lock().await.push(task);
    }

    async fn start_durable_flush_task(&self) {
        let seconds = self.config.write.flush_interval_seconds;
        if seconds == 0 {
            return;
        }
        let state = self.clone();
        let failures = Arc::new(Mutex::new(ErrorThrottle::flush()));
        let task = spawn_periodic(
            Duration::from_secs(seconds),
            self.cancellation.clone(),
            move || {
                let state = state.clone();
                let failures = Arc::clone(&failures);
                async move {
                    if !state.dirty.swap(false, Ordering::AcqRel) {
                        return;
                    }
                    let flushed = state.db.flush().await;
                    let mut failures = failures.lock().expect("flush throttle lock");
                    match flushed {
                        Ok(()) => failures.reset(),
                        Err(error) => {
                            state.ready.store(false, Ordering::Release);
                            state.dirty.store(true, Ordering::Release);
                            if let Some(suppressed) = failures.admit() {
                                tracing::error!(%error, suppressed, "Traces durable flush failed");
                            }
                        }
                    }
                }
            },
        );
        self.tasks.lock().await.push(task);
    }
}

pub(crate) fn live_config(config: &Config) -> LiveConfig<NamespaceConfig> {
    LiveConfig {
        namespaces: config
            .namespaces
            .iter()
            .map(|namespace| (namespace.name.clone(), namespace.clone()))
            .collect(),
        unauthenticated: config.auth.unauthenticated,
        global: config.auth.global.clone(),
        cache_warmer: config.cache_warmer.clone(),
    }
}

fn internal_writer_client(channel: Channel) -> InternalWriterClient<Channel> {
    InternalWriterClient::new(channel)
        .max_encoding_message_size(internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
        .max_decoding_message_size(internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
}

fn owner_endpoint(config: &Config, owner: &Owner) -> anyhow::Result<String> {
    config
        .sharding
        .owner_endpoint(config.listeners.grpc, owner)
        .ok_or_else(|| anyhow::anyhow!("owner endpoint is unknown"))
}

pub(crate) fn durability(value: crate::config::Durability) -> plural_traces::Durability {
    match value {
        crate::config::Durability::Applied => plural_traces::Durability::Applied,
        crate::config::Durability::Written => plural_traces::Durability::Written,
        crate::config::Durability::Durable => plural_traces::Durability::Durable,
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use sharding::ShardMap;

    use super::*;
    use crate::config::{KubernetesShardingConfig, ShardingBackend, ShardingConfig};

    fn batch_on_shard(namespace: &Namespace, routing: &ShardMap, shard: u32) -> TraceBatch {
        let trace = (1..=u8::MAX)
            .map(|id| {
                let trace_id = plural_traces::TraceId::new([id; 16]).unwrap();
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
            .find(|trace| {
                plural_traces::routing::route_trace(routing, namespace, trace).get() == shard
            })
            .unwrap();
        TraceBatch::new(vec![trace])
    }

    #[test]
    fn kubernetes_assignment_uses_stable_statefulset_endpoint() {
        let config = Config {
            mode: ServerMode::Writer,
            sharding: ShardingConfig {
                shards: 8,
                io_concurrency_limit: 64,
                read_groups: None,
                kind: ShardingBackend::Kubernetes(KubernetesShardingConfig {
                    namespace: "observability".into(),
                    stateful_set: "traces-writer".into(),
                    headless_service: "traces-writer-headless".into(),
                    owner_port: 9092,
                    ..Default::default()
                }),
            },
            ..Config::default()
        };
        let (_, assignment) = config
            .sharding
            .static_assignment(config.listeners.grpc)
            .unwrap();
        let owner = assignment.owner_of(ShardId::new(0)).unwrap();
        assert_eq!(
            owner_endpoint(&config, owner).unwrap(),
            "traces-writer-0.traces-writer-headless.observability.svc:9092"
        );
    }

    #[tokio::test]
    async fn drain_waits_for_admitted_write_guard_and_rejects_later_writes() {
        let state = AppState::open(Config {
            storage: StorageConfig::InMemory,
            namespaces: vec![NamespaceConfig {
                name: "tenant".into(),
                auth: Default::default(),
                usage_reporting_endpoint: None,
            }],
            ..Config::default()
        })
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let routing = state.router.assignment().read().await.clone();
        let batch = batch_on_shard(&namespace, &routing, 0);
        let shard = plural_traces::routing::route_trace(&routing, &namespace, &batch.traces[0]);

        let admitted = state.router.admit(shard).await.unwrap();
        let router = Arc::clone(&state.router);
        let drain = tokio::spawn(async move {
            router.start_draining([shard]).await;
        });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "drain passed an admitted in-flight write"
        );

        drop(admitted);
        drain.await.unwrap();
        let error = state
            .route_write(&namespace, vec![batch], "after-drain".into())
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("local shard is draining"));
        state.shutdown().await.unwrap();
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn traces_lifecycle_opens_drains_flushes_and_closes_shard() {
        use sharding::{RoutedShardLifecycle, ShardLifecycle};

        let db = Arc::new(
            ShardedTraces::open(
                plural_traces::Config {
                    storage: StorageConfig::InMemory,
                    ..plural_traces::Config::default()
                },
                ShardingOptions::new(2, 4).unwrap(),
                [],
            )
            .await
            .unwrap(),
        );
        let config = Config::default();
        let (_, assignment) = config
            .sharding
            .static_assignment(config.listeners.grpc)
            .unwrap();
        let router = Arc::new(WriteRouter::new(
            "writer",
            Arc::new(RwLock::new(assignment)),
            RouterLimits {
                remote_concurrency: 1,
                remote_retries: 0,
            },
        ));
        let lifecycle = RoutedShardLifecycle::new(Arc::clone(db.shards()), Arc::clone(&router));
        let shard = ShardId::new(1);
        lifecycle
            .open(shard, AssignmentGeneration::new(1))
            .await
            .unwrap();
        assert!(db.shards().contains(shard).await);
        lifecycle.drain(shard).await.unwrap();
        assert!(router.is_draining(shard).await);
        lifecycle.flush(shard).await.unwrap();
        lifecycle.close(shard).await.unwrap();
        assert!(!db.shards().contains(shard).await);
    }
}
