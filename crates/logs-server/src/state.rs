use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::storage::health::StorageHealth;
use plural_logs::{
    Durability as LogsDurability, LogBatch, Namespace, ShardedLogs, ShardingOptions,
};
use proto::logs::internal::v1::internal_writer_client::InternalWriterClient;
use server_common::auth::JwtAuthenticator;
use server_common::body_budget::{BodyBudget, GrpcBodyBudget};
use server_common::health::ErrorThrottle;
use server_common::ingest::{IngestPipeline, Signal};
use server_common::internal_rpc::{self, ChannelPool};
use server_common::read_groups::ReadGroups;
use server_common::reload::{self, Live, LiveConfig};
use server_common::tasks::spawn_periodic;
use server_common::warmer::{WarmPass, readiness_waits, spawn_cache_warmer, warming_enabled};
use sharding::{
    AssignmentGeneration, EpochPolicy, ForwardError, Owner, RouterLimits, ShardId, WriteRouter,
    server::ClusterMembership, shard_request_id,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode},
    http::ApiError,
    internal_writer::to_proto_request,
    shard_reader::GrpcLogsShards,
};

#[derive(Clone)]
pub struct AppState {
    /// The config the process started with; see `live` for reloaded settings.
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<ShardedLogs>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) live: Arc<Live<LiveConfig<NamespaceConfig>>>,
    warmer: Arc<Mutex<Option<CancellationToken>>>,
    pub(crate) router: Arc<WriteRouter>,
    pub(crate) completed_requests: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub(crate) ingest: IngestPipeline,
    /// Shared by every request body, HTTP and gRPC.
    pub(crate) bodies: BodyBudget,
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
            Some(config) => Some(JwtAuthenticator::open(config).await?),
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
            ShardedLogs::open_readers(
                config.logs_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
                slatedb::config::DbReaderOptions {
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
            )
            .await?
        } else {
            ShardedLogs::open(
                config.logs_config(),
                ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?,
                shards,
            )
            .await?
        });
        if let Some(groups) = ReadGroups::configured(config.mode, &config.sharding, &config.auth)? {
            db.shards().scope_readers_to(groups.local_shard());
            db.connect_remote_shards(Arc::new(GrpcLogsShards::new(Arc::new(groups))));
        }
        let cache_warmed =
            !readiness_waits(&config.cache_warmer, config.mode != ServerMode::Writer);
        let router = WriteRouter::new(
            local_owner,
            Arc::new(tokio::sync::RwLock::new(assignment)),
            RouterLimits {
                remote_concurrency: config.write.remote_concurrency,
                remote_retries: config.write.remote_retries,
            },
        );
        let (ingest, ingest_tasks) = IngestPipeline::standard(
            Signal::Logs,
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
            bodies: BodyBudget::new(config.request.max_in_flight_body_bytes),
            live: Arc::new(Live::new(live_config(&config))),
            warmer: Arc::default(),
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
            GrpcBodyBudget<
                proto::logs::internal::v1::shard_reader_server::ShardReaderServer<
                    crate::shard_reader::ShardReaderService,
                >,
            >,
        >,
    > {
        if self.config.mode != ServerMode::Reader || self.config.sharding.read_groups.is_none() {
            return Ok(None);
        }
        let token = self.config.auth.internal_token()?;
        Ok(Some(GrpcBodyBudget::new(
            crate::shard_reader::shard_reader_service(Arc::clone(&self.db), token),
            self.bodies.clone(),
        )))
    }

    pub(crate) fn live(&self) -> Arc<LiveConfig<NamespaceConfig>> {
        self.live.load()
    }

    /// Applies new revisions of the config file at `path` until shutdown.
    pub async fn watch_config(&self, path: PathBuf) {
        let state = self.clone();
        let task = reload::spawn_config_watcher(
            "logs",
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
            "logs",
            &config,
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
                            pass.metadata_budget_percent,
                        )
                        .await
                        .map(drop)
                }
            },
        );
        self.tasks.lock().await.push(task);
    }

    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_visibility(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub(crate) async fn route_write(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
        request_id: String,
    ) -> Result<(), ApiError> {
        let assignment = self.router.assignment().read().await.clone();
        let groups = plural_logs::routing::split(&assignment, namespace, batches);
        self.router
            .dispatch(
                &assignment,
                groups,
                |shard, batches| self.write_unguarded(namespace, shard, batches),
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

    pub(crate) async fn write_local(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        batches: Vec<LogBatch>,
        durability: LogsDurability,
    ) -> Result<(), plural_logs::Error> {
        let database = self.db.shards().require(shard).await?;
        database
            .write_with_durability(namespace, batches, durability)
            .await?;
        self.mark_dirty();
        Ok(())
    }

    async fn write_unguarded(
        &self,
        namespace: &Namespace,
        shard: ShardId,
        batches: Vec<LogBatch>,
    ) -> Result<(), ApiError> {
        self.write_local(
            namespace,
            shard,
            batches,
            durability(self.config.write.durability),
        )
        .await
        .map_err(crate::http::logs_error)
    }

    async fn write_remote(
        &self,
        namespace: &Namespace,
        owner: Owner,
        shard: ShardId,
        batches: Vec<LogBatch>,
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
                    let token = token.as_deref();
                    let endpoint = owner_endpoint(&self.config, owner);
                    async move {
                        let endpoint = endpoint.map_err(ForwardError::Failed)?;
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
                tokio::select! {
                    () = state.cancellation.cancelled() => {}
                    rolled = state.db.rollup(&rollup) => if let Err(error) = rolled {
                        tracing::warn!(%error, "Logs segment rollup failed");
                    },
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
                                tracing::error!(%error, suppressed, "Logs durable flush failed");
                            }
                        }
                    }
                }
            },
        );
        self.tasks.lock().await.push(task);
    }
}

pub(crate) fn durability(value: Durability) -> LogsDurability {
    match value {
        Durability::Applied => LogsDurability::Applied,
        Durability::Written => LogsDurability::Written,
        Durability::Durable => LogsDurability::Durable,
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

fn owner_endpoint(config: &Config, owner: &Owner) -> Result<String, ApiError> {
    config
        .sharding
        .owner_endpoint(config.listeners.grpc, owner)
        .ok_or_else(|| ApiError::unavailable("owner endpoint is unknown"))
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use plural_logs::{
        Label, Labels, LogEntry, QueryOptions, QueryRequest, QueryResult, routing::route,
    };
    use sharding::{Assignment, AssignmentState, ShardMap, ShardRange};
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    use super::*;
    use crate::{
        config::{ShardingBackend, ShardingConfig, StaticOwner},
        grpc_service,
    };

    fn config(owner_id: &str, endpoint: String) -> Config {
        Config {
            mode: ServerMode::Writer,
            storage: StorageConfig::InMemory,
            sharding: ShardingConfig {
                shards: 2,
                io_concurrency_limit: 64,
                read_groups: None,
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
                usage_reporting_endpoint: None,
            }],
            ..Config::default()
        }
    }

    fn two_shards() -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(1),
            2,
            vec![Assignment::new(
                Owner::new("test", 0),
                ShardRange::within(0, 2, 2).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    fn batch_on_shard(namespace: &Namespace, shard: u32) -> LogBatch {
        let routing = two_shards();
        let labels = (0..10_000)
            .map(|candidate| {
                Labels::new(vec![Label::new("app", format!("api-{candidate}"))]).unwrap()
            })
            .find(|labels| route(&routing, namespace, labels, 1).get() == shard)
            .unwrap();
        LogBatch::new(labels, vec![LogEntry::new(1, "forwarded")])
    }

    fn with_generation(map: &ShardMap, generation: u64) -> ShardMap {
        ShardMap::with_epochs(
            AssignmentGeneration::new(generation),
            map.epochs.clone(),
            map.assignments.clone(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn local_write_rejects_a_draining_shard() {
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
        let batch = batch_on_shard(&namespace, 1);
        let routing = state.router.assignment().read().await.clone();
        let shard = route(&routing, &namespace, &batch.labels, 1);
        state.router.start_draining([shard]).await;

        assert!(
            state
                .route_write(&namespace, vec![batch], "drained".into())
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
                usage_reporting_endpoint: None,
            }],
            ..Config::default()
        })
        .await
        .unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let batch = batch_on_shard(&namespace, 1);
        let routing = state.router.assignment().read().await.clone();
        let shard = route(&routing, &namespace, &batch.labels, 1);

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
    async fn logs_lifecycle_opens_drains_flushes_and_closes_a_shard() {
        use sharding::{RoutedShardLifecycle, ShardLifecycle};

        let db = Arc::new(
            ShardedLogs::open(
                plural_logs::Config {
                    storage: StorageConfig::InMemory,
                    ..plural_logs::Config::default()
                },
                ShardingOptions::new(2, 4).unwrap(),
                [],
            )
            .await
            .unwrap(),
        );
        let router = Arc::new(WriteRouter::new(
            "writer",
            Arc::new(tokio::sync::RwLock::new(two_shards())),
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

    #[tokio::test]
    async fn remote_forwarding_retries_stale_generation_idempotently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let writer_b = AppState::open(config("writer-b", endpoint.to_string()))
            .await
            .unwrap();
        let current = writer_b.router.assignment().read().await.clone();
        *writer_b.router.assignment().write().await = with_generation(&current, 2);
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
        let current = writer_a.router.assignment().read().await.clone();
        *writer_a.router.assignment().write().await = with_generation(&current, 2);
        let namespace = Namespace::new("tenant").unwrap();
        let batch = batch_on_shard(&namespace, 1);
        writer_a
            .write_remote(
                &namespace,
                Owner::new("writer-b", 1),
                ShardId::new(1),
                vec![batch],
                AssignmentGeneration::new(1),
                "retry-request",
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

    #[tokio::test]
    async fn remote_forwarding_carries_batches_above_tonic_default_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let writer_b = AppState::open(config("writer-b", endpoint.to_string()))
            .await
            .unwrap();
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
        let namespace = Namespace::new("tenant").unwrap();
        let line = "x".repeat(1024);
        let mut batch = batch_on_shard(&namespace, 1);
        // ~6 MiB of entries, above tonic's 4 MiB default message size.
        batch.entries = (0..6 * 1024)
            .map(|index| LogEntry::new(index + 1, line.as_str()))
            .collect();

        writer_a
            .write_remote(
                &namespace,
                Owner::new("writer-b", 1),
                ShardId::new(1),
                vec![batch],
                AssignmentGeneration::new(1),
                "large-request",
            )
            .await
            .unwrap();

        cancel.cancel();
        server.await.unwrap().unwrap();
        writer_a.shutdown().await.unwrap();
        writer_b.shutdown().await.unwrap();
    }
}
