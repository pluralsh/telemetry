use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use plural_logs::{
    Durability as LogsDurability, LogBatch, Namespace, ShardedLogs, ShardingOptions,
};
use proto::logs::internal::v1::internal_writer_client::InternalWriterClient;
use server_common::auth::JwtAuthenticator;
use server_common::ingest::{IngestPipeline, Signal};
use server_common::internal_rpc::{self, ChannelPool};
use server_common::warmer::{WarmPass, spawn_cache_warmer, warming_enabled};
use sharding::{
    AssignmentGeneration, Owner, RouterLimits, ShardId, ShardMap, WriteRouter,
    server::owned_shards, shard_request_id,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Request;

#[cfg(feature = "kubernetes")]
use sharding::{EpochPolicy, RoutedShardLifecycle, server::KubernetesRuntime};

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend},
    http::ApiError,
    internal_writer::to_proto_request,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<ShardedLogs>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) namespaces: Arc<HashMap<String, NamespaceConfig>>,
    pub(crate) router: Arc<WriteRouter>,
    pub(crate) completed_requests: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub(crate) ingest: IngestPipeline,
    channels: Arc<ChannelPool>,
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
            Some(config) => Some(JwtAuthenticator::open(config).await?),
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
                    EpochPolicy::default(),
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
        let namespaces = config
            .namespaces
            .iter()
            .cloned()
            .map(|namespace| (namespace.name.clone(), namespace))
            .collect();
        let cache_warmed = !config.cache_warmer.enabled || config.mode == ServerMode::Writer;
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
            config: Arc::new(config),
            db,
            jwt,
            namespaces: Arc::new(namespaces),
            router: Arc::new(router),
            completed_requests: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            ingest,
            channels: Arc::new(ChannelPool::default()),
            dirty: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
            cache_warmed: Arc::new(AtomicBool::new(cache_warmed)),
            cancellation,
            tasks: Arc::new(tokio::sync::Mutex::new(ingest_tasks)),
        };
        #[cfg(feature = "kubernetes")]
        if let Some(runtime) = kubernetes {
            if state.config.mode == ServerMode::Reader {
                let task = runtime.spawn_reader(
                    Arc::clone(&state.db),
                    Arc::clone(state.router.assignment()),
                    &state.cancellation,
                );
                state.tasks.lock().await.push(task);
            } else {
                let lifecycle = Arc::new(RoutedShardLifecycle::new(
                    Arc::clone(state.db.shards()),
                    Arc::clone(&state.router),
                ));
                let tasks = runtime.spawn(
                    lifecycle,
                    Arc::clone(state.router.assignment()),
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
                let expected = (0..self.router.assignment().read().await.shard_count)
                    .map(ShardId::new)
                    .collect::<Vec<_>>();
                self.db.shards().ids().await == expected
            }
            ServerMode::Writer => {
                let assignment = self.router.assignment().read().await;
                let expected =
                    owned_shards(&assignment, self.router.local_owner()).collect::<Vec<_>>();
                !expected.is_empty() && self.db.shards().ids().await == expected
            }
        }
    }

    async fn start_cache_warmer(&self) {
        let serves_reads = self.config.mode != ServerMode::Writer;
        if !warming_enabled(&self.config.cache_warmer, serves_reads) {
            return;
        }
        let namespaces: Arc<[Namespace]> = self
            .config
            .namespaces
            .iter()
            .filter_map(|namespace| Namespace::new(namespace.name.clone()).ok())
            .collect();
        let database = Arc::clone(&self.db);
        let task = spawn_cache_warmer(
            "logs",
            &self.config.cache_warmer,
            self.cancellation.clone(),
            Arc::clone(&self.cache_warmed),
            move |pass: WarmPass| {
                let database = Arc::clone(&database);
                let namespaces = Arc::clone(&namespaces);
                async move {
                    database
                        .warm_recent(
                            &namespaces,
                            pass.range,
                            pass.include_payloads,
                            pass.concurrency,
                            &pass.cancel,
                            pass.tracker.as_deref(),
                        )
                        .await
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
            .internal
            .as_ref()
            .map(|secret| secret.expose())
            .transpose()
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
                        let unavailable = |status: &tonic::Status| ApiError::unavailable(status);
                        let endpoint = endpoint.map_err(sharding::ForwardError::Failed)?;
                        let channel = self.channels.channel(&endpoint).map_err(|status| {
                            sharding::ForwardError::Failed(unavailable(&status))
                        })?;
                        let mut request = Request::new(request);
                        internal_rpc::authorize(&mut request, token).map_err(|status| {
                            sharding::ForwardError::Failed(unavailable(&status))
                        })?;
                        InternalWriterClient::new(channel)
                            .max_encoding_message_size(internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
                            .max_decoding_message_size(internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
                            .write(request)
                            .await
                            .map(drop)
                            .map_err(|status| internal_rpc::forward_error(&status, unavailable))
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
                            tracing::error!(%error, "Logs durable flush failed");
                        }
                    }
                }
            }
        });
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
    use plural_logs::{
        Label, Labels, LogEntry, QueryOptions, QueryRequest, QueryResult, routing::route,
    };
    use sharding::{Assignment, AssignmentState, ShardRange};
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
                shards: 2,
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
        use sharding::ShardLifecycle;

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
