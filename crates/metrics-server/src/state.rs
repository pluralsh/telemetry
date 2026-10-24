use std::{
    collections::HashSet,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use common::storage::health::StorageHealth;
use plural_metrics::{Namespace, Series, ShardedMetrics, ShardingOptions, Visibility};
use proto::metrics::internal::v1::internal_writer_client::InternalWriterClient;
use server_common::health::ErrorThrottle;
use server_common::ingest::{IngestPipeline, Signal};
use server_common::internal_rpc::{self, ChannelPool};
use server_common::reload::{self, Live, LiveConfig};
use server_common::tasks::spawn_periodic;
use server_common::warmer::{
    WarmPass, WarmState, readiness_waits, spawn_cache_warmer, warming_enabled,
};
use sharding::{
    AssignmentGeneration, EpochPolicy, ForwardError, Owner, RouterLimits, ShardId, WriteRouter,
    server::ClusterMembership, shard_request_id,
};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use server_common::auth::JwtAuthenticator;

use crate::{
    config::{Config, Durability, NamespaceConfig, ServerMode},
    http::ApiError,
    internal_writer::to_proto_request,
    shard_reader::GrpcMetricsShards,
};
use server_common::read_groups::ReadGroups;

#[derive(Clone)]
pub struct AppState {
    /// The config the process started with; see `live` for reloaded settings.
    pub(crate) config: Arc<Config>,
    pub(crate) live: Arc<Live<LiveConfig<NamespaceConfig>>>,
    pub(crate) warmer: Arc<Mutex<Option<CancellationToken>>>,
    pub(crate) warm_state: Arc<WarmState>,
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
    pub(crate) storage_health: StorageHealth,
}

impl AppState {
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let jwt = match config.auth.jwt.clone() {
            Some(jwt) => Some(JwtAuthenticator::open(jwt).await?),
            None => None,
        };
        let cancellation = CancellationToken::new();
        let ClusterMembership {
            local_owner,
            assignment,
            runtime,
        } = config
            .sharding
            .join(config.listeners.grpc, epoch_policy(), &cancellation)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        let options =
            ShardingOptions::new(assignment.shard_count, config.sharding.io_concurrency_limit)?;
        let metrics_config = metrics_config(&config);
        let shards = config
            .sharding
            .startup_shards(config.mode, &assignment, &local_owner);
        let (writers, readers) = match config.mode {
            ServerMode::Standalone | ServerMode::Writer => {
                let writers =
                    Arc::new(ShardedMetrics::open_writers(metrics_config, options, shards).await?);
                let readers = (config.mode == ServerMode::Standalone).then(|| Arc::clone(&writers));
                (Some(writers), readers)
            }
            ServerMode::Reader => {
                let readers = ShardedMetrics::open_readers(
                    metrics_config,
                    options,
                    shards,
                    config
                        .reader_disk_cache
                        .apply(slatedb::config::DbReaderOptions {
                            skip_wal_replay: false,
                            ..slatedb::config::DbReaderOptions::default()
                        }),
                    config.reader_cache_capacity,
                )
                .await?;
                (None, Some(Arc::new(readers)))
            }
        };
        if let Some(readers) = &readers
            && let Some(groups) =
                ReadGroups::configured(config.mode, &config.sharding, &config.auth)?
        {
            readers.shards().scope_readers_to(groups.local_shard());
            readers.connect_remote_shards(Arc::new(GrpcMetricsShards::new(Arc::new(groups))));
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
            warm_state: Arc::default(),
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
            storage_health: StorageHealth::global().clone(),
        };
        // The writer set in writer and standalone mode, the reader set in reader mode.
        let lifecycle_shards = state
            .writers
            .as_ref()
            .or(state.readers.as_ref())
            .expect("every server mode opens sharded metrics")
            .shards();
        let cluster_tasks = runtime.spawn(lifecycle_shards, &state.router, &state.cancellation);
        state.background_tasks.lock().await.extend(cluster_tasks);
        state.start_cache_warmer(true).await;
        state.start_durable_flush_task().await;
        Ok(state)
    }

    async fn start_durable_flush_task(&self) {
        let seconds = self.config.write.flush_interval_seconds;
        if self.config.mode == ServerMode::Reader || seconds == 0 {
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
                    let flushed = state.flush_active_writers().await;
                    let mut failures = failures.lock().expect("flush throttle lock");
                    match flushed {
                        Ok(()) => failures.reset(),
                        Err(error) => {
                            if let Some(suppressed) = failures.admit() {
                                tracing::error!(%error, suppressed, "periodic durable flush failed");
                            }
                        }
                    }
                }
            },
        );
        self.background_tasks.lock().await.push(task);
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The `ShardReader` service of a reader group member, which serves its
    /// shard to the other groups' coordinators.
    pub fn shard_reader_service(
        &self,
    ) -> anyhow::Result<
        Option<
            proto::metrics::internal::v1::shard_reader_server::ShardReaderServer<
                crate::shard_reader::ShardReaderService,
            >,
        >,
    > {
        let Some(readers) = &self.readers else {
            return Ok(None);
        };
        if self.config.mode != ServerMode::Reader || self.config.sharding.read_groups.is_none() {
            return Ok(None);
        }
        let token = self.config.auth.internal_token()?;
        Ok(Some(crate::shard_reader::shard_reader_service(
            Arc::clone(readers),
            token,
        )))
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
        let database = match self.config.mode {
            ServerMode::Standalone => return self.readers.is_some(),
            ServerMode::Reader => &self.readers,
            ServerMode::Writer => &self.writers,
        };
        let Some(database) = database else {
            return false;
        };
        let assignment = self.router.assignment().read().await;
        database
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
            &self.warm_state,
            cancel,
            Arc::clone(&self.cache_warmed),
            move |pass: WarmPass| {
                let readers = Arc::clone(&readers);
                let namespaces = Arc::clone(&namespaces);
                async move {
                    readers
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
            .internal_token()
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
        query_limits: config.query.limits(),
        rollup: config.rollup.rollup(),
    }
}

/// Aligns scale-up cutovers to the hourly buckets metrics are stored in.
fn epoch_policy() -> EpochPolicy {
    EpochPolicy {
        alignment: Duration::from_secs(3600),
        ..EpochPolicy::default()
    }
}

#[cfg(test)]
pub(crate) fn assignment_for(config: &Config) -> anyhow::Result<(String, sharding::ShardMap)> {
    Ok(config.sharding.static_assignment(config.listeners.grpc)?)
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

fn visibility(durability: Durability) -> Visibility {
    match durability {
        Durability::Applied => Visibility::Applied,
        Durability::Written => Visibility::Written,
        Durability::Durable => Visibility::Durable,
    }
}
