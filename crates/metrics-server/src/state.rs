use std::{
    collections::HashSet,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use plural_metrics::{Namespace, Series, ShardedMetrics, ShardingOptions, Visibility};
use proto::metrics::internal::v1::internal_writer_client::InternalWriterClient;
use server_common::ingest::{IngestPipeline, Signal};
use server_common::internal_rpc::{self, ChannelPool};
use server_common::reload::{self, Live, LiveConfig};
use server_common::warmer::{WarmPass, spawn_cache_warmer, warming_enabled};
use sharding::{
    AssignmentGeneration, ForwardError, Owner, RouterLimits, ShardId, ShardMap, WriteRouter,
    server::owned_shards, shard_request_id,
};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Request;

#[cfg(feature = "kubernetes")]
use sharding::{EpochPolicy, RoutedShardLifecycle, server::KubernetesRuntime};

use server_common::auth::JwtAuthenticator;

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend},
    http::ApiError,
    internal_writer::to_proto_request,
};

#[derive(Clone)]
pub struct AppState {
    /// The config the process started with; see `live` for reloaded settings.
    pub(crate) config: Arc<Config>,
    pub(crate) live: Arc<Live<LiveConfig<NamespaceConfig>>>,
    pub(crate) warmer: Arc<Mutex<Option<CancellationToken>>>,
    pub(crate) jwt: Option<JwtAuthenticator>,
    pub(crate) writers: Option<Arc<ShardedMetrics>>,
    pub(crate) readers: Option<Arc<ShardedMetrics>>,
    pub(crate) router: Arc<WriteRouter>,
    pub(crate) channels: Arc<ChannelPool>,
    pub(crate) completed_requests: Arc<Mutex<HashSet<(String, String)>>>,
    pub(crate) ingest: IngestPipeline,
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
                    KubernetesRuntime::bootstrap(settings, epoch_policy(), cancellation.clone())
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
        let options = ShardingOptions::new(shard_count, config.sharding.io_concurrency_limit)?;
        let metrics_config = metrics_config(&config);
        let writer_shards = if config.mode == ServerMode::Standalone {
            (0..shard_count).map(ShardId::new).collect::<Vec<_>>()
        } else if matches!(config.sharding.kind, ShardingBackend::Kubernetes(_)) {
            Vec::new()
        } else {
            owned_shards(&assignment, &local_owner).collect()
        };
        let writers = if config.mode != ServerMode::Reader {
            Some(Arc::new(
                ShardedMetrics::open_writers(metrics_config.clone(), options, writer_shards)
                    .await?,
            ))
        } else {
            None
        };
        let readers = if config.mode == ServerMode::Standalone {
            writers.clone()
        } else if config.mode != ServerMode::Writer {
            Some(Arc::new(
                ShardedMetrics::open_readers(
                    metrics_config,
                    options,
                    (0..shard_count).map(ShardId::new),
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
        let router = WriteRouter::new(
            local_owner,
            Arc::new(RwLock::new(assignment)),
            RouterLimits {
                remote_concurrency: config.write.remote_concurrency,
                remote_retries: config.write.remote_retries,
            },
        );
        let (ingest, ingest_tasks) = IngestPipeline::standard(
            Signal::Metrics,
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
            live: Arc::new(Live::new(live_config(&config))),
            warmer: Arc::default(),
            config: Arc::new(config),
            jwt,
            writers,
            readers,
            router: Arc::new(router),
            channels: Arc::new(ChannelPool::default()),
            completed_requests: Arc::new(Mutex::new(HashSet::new())),
            ingest,
            cancellation,
            background_tasks: Arc::new(tokio::sync::Mutex::new(ingest_tasks)),
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
                        .expect("reader mode must open sharded metrics")
                        .clone(),
                    Arc::clone(state.router.assignment()),
                    &state.cancellation,
                );
                state.background_tasks.lock().await.push(task);
            } else {
                let writers = state
                    .writers
                    .as_ref()
                    .expect("writer mode must open sharded metrics");
                let lifecycle = Arc::new(RoutedShardLifecycle::new(
                    Arc::clone(writers.shards()),
                    Arc::clone(&state.router),
                ));
                let tasks = runtime.spawn(
                    lifecycle,
                    Arc::clone(state.router.assignment()),
                    &state.cancellation,
                );
                state.background_tasks.lock().await.extend(tasks);
            }
        }
        state.start_cache_warmer(true).await;
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
            self.router
                .start_draining(writers.shards().ids().await)
                .await;
            writers.flush().await?;
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

    pub(crate) fn live(&self) -> Arc<LiveConfig<NamespaceConfig>> {
        self.live.load()
    }

    pub(crate) fn has_namespace(&self, name: &str) -> bool {
        self.live().namespaces.contains_key(name)
    }

    /// Applies new revisions of the config file at `path` until shutdown.
    pub async fn watch_config(&self, path: PathBuf) {
        let state = self.clone();
        let task = reload::spawn_config_watcher(
            "metrics",
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
        self.background_tasks.lock().await.push(task);
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

    pub(crate) async fn is_ready(&self) -> bool {
        if !self.cache_warmed.load(Ordering::Acquire) {
            return false;
        }
        match self.config.mode {
            ServerMode::Standalone => self.readers.is_some(),
            ServerMode::Reader => {
                let expected = (0..self.router.assignment().read().await.shard_count)
                    .map(ShardId::new)
                    .collect::<Vec<_>>();
                match &self.readers {
                    Some(readers) => readers.shards().ids().await == expected,
                    None => false,
                }
            }
            ServerMode::Writer => {
                let assignment = self.router.assignment().read().await;
                let expected =
                    owned_shards(&assignment, self.router.local_owner()).collect::<Vec<_>>();
                let Some(writers) = &self.writers else {
                    return false;
                };
                !expected.is_empty() && writers.shards().ids().await == expected
            }
        }
    }

    /// (Re)starts the warmer from the live config, replacing any running one.
    /// Only the first start runs the startup pass.
    async fn start_cache_warmer(&self, startup: bool) {
        if let Some(previous) = self.warmer.lock().expect("warmer lock").take() {
            previous.cancel();
        }
        let Some(readers) = self.readers.clone() else {
            self.cache_warmed.store(true, Ordering::Release);
            return;
        };
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
        let task = spawn_cache_warmer(
            "metrics",
            &config,
            cancel,
            Arc::clone(&self.cache_warmed),
            move |pass: WarmPass| {
                let readers = Arc::clone(&readers);
                let namespaces = Arc::clone(&namespaces);
                async move {
                    readers
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
        let metrics_namespace = Namespace::new(namespace).map_err(ApiError::bad_request)?;
        let assignment = self.router.assignment().read().await.clone();
        let groups = plural_metrics::routing::split(&assignment, &metrics_namespace, series);
        self.router
            .dispatch(
                &assignment,
                groups,
                |shard, batch| {
                    let metrics_namespace = &metrics_namespace;
                    async move {
                        self.write_local(metrics_namespace, shard, batch, durability)
                            .await
                            .map_err(crate::http::metrics_error)
                    }
                },
                |owner, shard, generation, batch| {
                    let request_id = shard_request_id(&request_id, shard);
                    async move {
                        self.write_remote(
                            RemoteWrite {
                                namespace,
                                shard,
                                durability,
                                request_id: &request_id,
                            },
                            owner,
                            generation,
                            batch,
                        )
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
        series: Vec<Series>,
        durability: Durability,
    ) -> Result<(), plural_metrics::Error> {
        let Some(writers) = &self.writers else {
            return Err(sharding::ShardSetError::NotOpen(shard).into());
        };
        writers
            .write_shard(namespace, shard, series, visibility(durability))
            .await
    }

    async fn write_remote(
        &self,
        target: RemoteWrite<'_>,
        owner: Owner,
        generation: AssignmentGeneration,
        series: Vec<Series>,
    ) -> Result<(), ApiError> {
        let token = self
            .config
            .auth
            .internal
            .as_ref()
            .map(|secret| secret.expose())
            .transpose()
            .map_err(ApiError::internal)?;
        let shard = target.shard;
        self.router
            .forward(
                owner,
                shard,
                generation,
                &series,
                |owner, generation, series| {
                    let request = to_proto_request(
                        target.namespace,
                        shard,
                        generation.get(),
                        target.request_id,
                        target.durability,
                        series.clone(),
                    );
                    let endpoint = owner_endpoint(&self.config, owner);
                    let token = token.as_deref();
                    async move {
                        let unavailable = |status: &tonic::Status| ApiError::unavailable(status);
                        let endpoint = endpoint.map_err(ForwardError::Failed)?;
                        let channel = self
                            .channels
                            .channel(&endpoint)
                            .map_err(|status| ForwardError::Failed(unavailable(&status)))?;
                        let mut request = Request::new(request);
                        internal_rpc::authorize(&mut request, token)
                            .map_err(|status| ForwardError::Failed(unavailable(&status)))?;
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
}

/// Where a forwarded write goes and how it is written there.
#[derive(Clone, Copy)]
struct RemoteWrite<'a> {
    namespace: &'a str,
    shard: ShardId,
    durability: Durability,
    request_id: &'a str,
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

pub(crate) fn metrics_config(config: &Config) -> plural_metrics::Config {
    plural_metrics::Config {
        storage: config.storage.clone(),
        flush_interval: config.write.flush_interval(),
        retention: config.retention_seconds.map(Duration::from_secs),
        write_buffer: config.write.write_buffer(),
        query_cache: plural_metrics::QueryCacheConfig {
            matcher_capacity_bytes: config.matcher_cache_capacity_bytes,
            series_capacity_bytes: config.reader_cache_capacity / 2,
            forward_index_capacity_bytes: config.forward_index_cache_capacity_bytes,
            result_cache_enabled: config.result_cache.enabled,
            result_capacity_bytes: config.result_cache.capacity_bytes,
        },
    }
}

#[cfg(feature = "kubernetes")]
fn epoch_policy() -> EpochPolicy {
    EpochPolicy {
        alignment: Duration::from_secs(3600),
        ..EpochPolicy::default()
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
