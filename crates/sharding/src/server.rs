//! Sharding configuration and startup shared by the Meter, Line, and Track
//! servers. Products differ only in the defaults supplied by [`Product`].

use std::{fmt, marker::PhantomData, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    Assignment, AssignmentGeneration, AssignmentState, DEFAULT_IO_CONCURRENCY_MULTIPLIER,
    DEFAULT_VIRTUAL_SHARDS, ModelError, Owner, ShardId, ShardMap, ShardRange,
};

/// Product-specific defaults for the Kubernetes sharding resources.
pub trait Product: fmt::Debug + Clone + Default + Send + Sync + 'static {
    /// Base name of the product's StatefulSet, Service, ConfigMap, and Leases.
    const NAME: &'static str;
    const OWNER_PORT: u16;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerMode {
    Writer,
    Reader,
    #[default]
    Standalone,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, bound = "P: Product")]
pub struct ShardingConfig<P: Product> {
    pub virtual_shards: u32,
    pub io_concurrency_multiplier: u32,
    #[serde(flatten)]
    pub kind: ShardingBackend<P>,
}

impl<P: Product> Default for ShardingConfig<P> {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            io_concurrency_multiplier: DEFAULT_IO_CONCURRENCY_MULTIPLIER,
            kind: ShardingBackend::Standalone,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case", bound = "P: Product")]
pub enum ShardingBackend<P: Product> {
    #[default]
    Standalone,
    Static {
        owner_id: String,
        owners: Vec<StaticOwner>,
    },
    Kubernetes(KubernetesShardingConfig<P>),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StaticOwner {
    pub id: String,
    pub ordinal: u32,
    pub endpoint: String,
    pub start_shard: u32,
    pub end_shard: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, bound = "P: Product")]
pub struct KubernetesShardingConfig<P: Product> {
    /// Database name; leases are labeled `telemetry.plural.sh/<product>=<database>`.
    pub database: String,
    pub namespace: String,
    pub stateful_set: String,
    pub headless_service: String,
    pub owner_port: u16,
    pub assignment_config_map: String,
    pub coordinator_lease: String,
    pub shard_lease_prefix: String,
    pub lease_duration_seconds: u64,
    pub renew_interval_seconds: u64,
    #[serde(skip)]
    pub product: PhantomData<P>,
}

impl<P: Product> Default for KubernetesShardingConfig<P> {
    fn default() -> Self {
        Self {
            database: P::NAME.to_owned(),
            namespace: "default".to_owned(),
            stateful_set: P::NAME.to_owned(),
            headless_service: format!("{}-headless", P::NAME),
            owner_port: P::OWNER_PORT,
            assignment_config_map: format!("{}-shard-assignments", P::NAME),
            coordinator_lease: format!("{}-shard-coordinator", P::NAME),
            shard_lease_prefix: format!("{}-shard", P::NAME),
            lease_duration_seconds: 15,
            renew_interval_seconds: 5,
            product: PhantomData,
        }
    }
}

impl<P: Product> KubernetesShardingConfig<P> {
    /// Stable per-pod DNS name of the owner with `ordinal`.
    pub fn owner_endpoint(&self, ordinal: u32) -> String {
        format!(
            "{}-{ordinal}.{}.{}.svc:{}",
            self.stateful_set, self.headless_service, self.namespace, self.owner_port
        )
    }

    /// Owner id of this pod, from `POD_NAME` or `POD_ORDINAL`.
    pub fn local_owner_id(&self) -> String {
        std::env::var("POD_NAME").unwrap_or_else(|_| {
            let ordinal = std::env::var("POD_ORDINAL").unwrap_or_else(|_| "0".to_owned());
            format!("{}-{ordinal}", self.stateful_set)
        })
    }

    #[cfg(feature = "kubernetes")]
    pub fn kubernetes_config(&self) -> crate::kubernetes::KubernetesConfig {
        crate::kubernetes::KubernetesConfig {
            database_type: P::NAME.to_owned(),
            database: self.database.clone(),
            namespace: self.namespace.clone(),
            stateful_set: self.stateful_set.clone(),
            headless_service: self.headless_service.clone(),
            owner_port: self.owner_port,
            assignment_config_map: self.assignment_config_map.clone(),
            coordinator_lease: self.coordinator_lease.clone(),
            shard_lease_prefix: self.shard_lease_prefix.clone(),
            lease_duration: std::time::Duration::from_secs(self.lease_duration_seconds),
        }
    }
}

fn is_label_value(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

impl<P: Product> ShardingConfig<P> {
    pub fn validate(&self, mode: ServerMode) -> Result<(), String> {
        if self.virtual_shards == 0 {
            return Err("sharding.virtual_shards must be greater than zero".to_owned());
        }
        if self.io_concurrency_multiplier == 0 {
            return Err("sharding.io_concurrency_multiplier must be greater than zero".to_owned());
        }
        if mode == ServerMode::Standalone && !matches!(self.kind, ShardingBackend::Standalone) {
            return Err("standalone mode requires the standalone sharding backend".to_owned());
        }
        match &self.kind {
            ShardingBackend::Standalone => {}
            ShardingBackend::Static { owner_id, owners } => {
                if !owners.iter().any(|owner| &owner.id == owner_id) {
                    return Err(format!("static owner_id {owner_id} has no owner entry"));
                }
                let mut ranges = owners
                    .iter()
                    .map(|owner| (owner.start_shard, owner.end_shard))
                    .collect::<Vec<_>>();
                ranges.sort_unstable();
                let mut expected = 0;
                for (start, end) in ranges {
                    if start != expected || end <= start || end > self.virtual_shards {
                        return Err(
                            "static owners must exactly cover all virtual shards".to_owned()
                        );
                    }
                    expected = end;
                }
                if expected != self.virtual_shards {
                    return Err("static owners must exactly cover all virtual shards".to_owned());
                }
            }
            ShardingBackend::Kubernetes(settings) => {
                if settings.lease_duration_seconds == 0
                    || settings.renew_interval_seconds == 0
                    || settings.renew_interval_seconds >= settings.lease_duration_seconds
                {
                    return Err(
                        "Kubernetes renew_interval_seconds must be positive and shorter than lease_duration_seconds"
                            .to_owned(),
                    );
                }
                if !is_label_value(&settings.database) {
                    return Err(format!(
                        "sharding.database {:?} must be a non-empty Kubernetes label value (at most 63 alphanumerics, '-', '_' or '.', starting and ending alphanumeric)",
                        settings.database
                    ));
                }
            }
        }
        Ok(())
    }

    /// Local owner id and assignment for the non-Kubernetes backends. For the
    /// Kubernetes backend this is a single-owner map for this pod; the live
    /// assignment comes from [`KubernetesRuntime::bootstrap`].
    pub fn static_assignment(&self, grpc: SocketAddr) -> Result<(String, ShardMap), ModelError> {
        let (local, owners) = match &self.kind {
            ShardingBackend::Standalone => (
                "standalone".to_owned(),
                vec![StaticOwner {
                    id: "standalone".to_owned(),
                    ordinal: 0,
                    endpoint: grpc.to_string(),
                    start_shard: 0,
                    end_shard: self.virtual_shards,
                }],
            ),
            ShardingBackend::Static { owner_id, owners } => (owner_id.clone(), owners.clone()),
            ShardingBackend::Kubernetes(kubernetes) => {
                let ordinal = std::env::var("POD_NAME")
                    .ok()
                    .and_then(|name| name.rsplit_once('-')?.1.parse().ok())
                    .unwrap_or(0);
                let id = format!("{}-{ordinal}", kubernetes.stateful_set);
                (
                    id.clone(),
                    vec![StaticOwner {
                        id,
                        ordinal,
                        endpoint: kubernetes.owner_endpoint(ordinal),
                        start_shard: 0,
                        end_shard: self.virtual_shards,
                    }],
                )
            }
        };
        let assignments = owners
            .into_iter()
            .map(|owner| {
                Ok(Assignment::new(
                    Owner::new(owner.id, owner.ordinal),
                    ShardRange::within(owner.start_shard, owner.end_shard, self.virtual_shards)?,
                    AssignmentState::Active,
                ))
            })
            .collect::<Result<Vec<_>, ModelError>>()?;
        Ok((
            local,
            ShardMap::new(
                AssignmentGeneration::new(1),
                self.virtual_shards,
                assignments,
            )?,
        ))
    }

    /// gRPC endpoint of `owner`, or `None` when a static owner is unknown.
    pub fn owner_endpoint(&self, grpc: SocketAddr, owner: &Owner) -> Option<String> {
        match &self.kind {
            ShardingBackend::Standalone => Some(grpc.to_string()),
            ShardingBackend::Static { owners, .. } => owners
                .iter()
                .find(|candidate| candidate.id == owner.id)
                .map(|candidate| candidate.endpoint.clone()),
            ShardingBackend::Kubernetes(kubernetes) => {
                Some(kubernetes.owner_endpoint(owner.ordinal))
            }
        }
    }

    /// Shards this process opens at startup. Kubernetes writers start empty
    /// and open shards as the ownership manager acquires their leases.
    pub fn startup_shards(
        &self,
        mode: ServerMode,
        assignment: &ShardMap,
        local_owner: &str,
    ) -> Vec<ShardId> {
        match mode {
            ServerMode::Standalone | ServerMode::Reader => {
                (0..self.virtual_shards).map(ShardId::new).collect()
            }
            ServerMode::Writer if matches!(self.kind, ShardingBackend::Kubernetes(_)) => Vec::new(),
            ServerMode::Writer => owned_shards(assignment, local_owner).collect(),
        }
    }
}

/// Shards `owner_id` owns in `assignment`, in ascending order.
pub fn owned_shards<'a>(
    assignment: &'a ShardMap,
    owner_id: &'a str,
) -> impl Iterator<Item = ShardId> + 'a {
    (0..assignment.virtual_shards)
        .map(ShardId::new)
        .filter(move |shard| {
            assignment
                .owner_of(*shard)
                .is_some_and(|owner| owner.id == owner_id)
        })
}

#[cfg(feature = "kubernetes")]
pub use runtime::KubernetesRuntime;

#[cfg(feature = "kubernetes")]
mod runtime {
    use std::{sync::Arc, time::Duration};

    use tokio::{sync::RwLock, task::JoinHandle};
    use tokio_util::sync::CancellationToken;

    use super::{KubernetesShardingConfig, Product};
    use crate::{
        AssignmentGeneration, AssignmentStore, BoxError, OwnershipManager, OwnershipManagerConfig,
        ShardLifecycle, ShardMap, balanced_contiguous,
        kubernetes::{
            KubernetesAssignmentStore, KubernetesConfig, KubernetesCoordinatorElection,
            KubernetesLeaseBackend, StatefulSetMembership, run_kubernetes_coordinator,
        },
    };

    /// Cluster handles a Kubernetes-sharded server keeps after startup.
    pub struct KubernetesRuntime {
        store: Arc<KubernetesAssignmentStore>,
        leases: Arc<KubernetesLeaseBackend>,
        config: KubernetesConfig,
        identity: String,
        renew_interval: Duration,
        virtual_shards: u32,
        product: &'static str,
    }

    impl KubernetesRuntime {
        /// Loads the published assignment. When none exists yet, competes for
        /// the coordinator lease and publishes a balanced initial assignment.
        pub async fn bootstrap<P: Product>(
            settings: &KubernetesShardingConfig<P>,
            virtual_shards: u32,
            cancellation: CancellationToken,
        ) -> Result<(Self, ShardMap), BoxError> {
            let client = kube::Client::try_default().await?;
            let config = settings.kubernetes_config();
            let store =
                KubernetesAssignmentStore::new(client.clone(), &config, cancellation.clone())
                    .await?;
            let identity = settings.local_owner_id();
            let assignment = loop {
                if let Some(current) = store.load().await? {
                    break current;
                }
                let election = KubernetesCoordinatorElection::new(client.clone(), &config);
                if election.try_acquire(&identity).await? {
                    let owners = StatefulSetMembership::new(client.clone(), &config)
                        .owners()
                        .await?;
                    let initial = balanced_contiguous(
                        AssignmentGeneration::new(1),
                        virtual_shards,
                        &owners,
                        None,
                    )?;
                    match store.publish(initial.clone()).await {
                        Ok(()) => break initial,
                        Err(error) => {
                            tracing::warn!(%error, product = P::NAME, "initial shard assignment raced; retrying");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            };
            let leases = Arc::new(KubernetesLeaseBackend::new(client, &config, cancellation));
            Ok((
                Self {
                    store,
                    leases,
                    config,
                    identity,
                    renew_interval: Duration::from_secs(settings.renew_interval_seconds),
                    virtual_shards,
                    product: P::NAME,
                },
                assignment,
            ))
        }

        pub fn identity(&self) -> &str {
            &self.identity
        }

        /// Spawns the shard ownership manager, the assignment watcher that
        /// keeps `assignment` current, and the coordinator loop.
        pub fn spawn<R: ShardLifecycle + 'static>(
            self,
            lifecycle: Arc<R>,
            assignment: Arc<RwLock<ShardMap>>,
            cancellation: &CancellationToken,
        ) -> [JoinHandle<()>; 3] {
            let product = self.product;
            let manager = OwnershipManager::new(
                self.identity.clone(),
                OwnershipManagerConfig {
                    renew_interval: self.renew_interval,
                    lease_duration: self.config.lease_duration,
                },
                Arc::clone(&self.store),
                self.leases,
                lifecycle,
            );
            let manager_cancel = cancellation.clone();
            let manager_task = tokio::spawn(async move {
                if let Err(error) = manager.run(manager_cancel).await {
                    tracing::error!(%error, product, "Kubernetes ownership manager stopped");
                }
            });
            let watcher_cancel = cancellation.clone();
            let mut updates = self.store.watch();
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
            let coordinator_task = tokio::spawn(run_kubernetes_coordinator(
                self.store,
                self.config,
                self.identity,
                self.virtual_shards,
                cancellation.clone(),
            ));
            [manager_task, watcher_task, coordinator_task]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Demo;

    impl Product for Demo {
        const NAME: &'static str = "demo";
        const OWNER_PORT: u16 = 9000;
    }

    fn static_config(owners: &[(u32, u32)]) -> ShardingConfig<Demo> {
        ShardingConfig {
            virtual_shards: 8,
            io_concurrency_multiplier: 4,
            kind: ShardingBackend::Static {
                owner_id: "owner-0".to_owned(),
                owners: owners
                    .iter()
                    .enumerate()
                    .map(|(ordinal, &(start_shard, end_shard))| StaticOwner {
                        id: format!("owner-{ordinal}"),
                        ordinal: ordinal as u32,
                        endpoint: format!("owner-{ordinal}:9000"),
                        start_shard,
                        end_shard,
                    })
                    .collect(),
            },
        }
    }

    #[test]
    fn config_defaults_to_eight_virtual_shards() {
        assert_eq!(ShardingConfig::<Demo>::default().virtual_shards, 8);
        let parsed: ShardingConfig<Demo> =
            serde_json::from_str(r#"{"backend":"standalone"}"#).unwrap();
        assert_eq!(parsed.virtual_shards, 8);
        assert!(matches!(parsed.kind, ShardingBackend::Standalone));
    }

    #[test]
    fn kubernetes_database_must_be_a_label_value() {
        let config = |database: &str| ShardingConfig::<Demo> {
            virtual_shards: 8,
            io_concurrency_multiplier: 4,
            kind: ShardingBackend::Kubernetes(KubernetesShardingConfig {
                database: database.to_owned(),
                ..KubernetesShardingConfig::default()
            }),
        };
        assert!(config("prod.logs-1").validate(ServerMode::Writer).is_ok());
        for invalid in ["", "-prod", "prod/logs", &"a".repeat(64)] {
            assert!(
                config(invalid).validate(ServerMode::Writer).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn kubernetes_defaults_follow_the_product_name() {
        let config = KubernetesShardingConfig::<Demo>::default();
        assert_eq!(config.database, "demo");
        assert_eq!(config.stateful_set, "demo");
        assert_eq!(config.headless_service, "demo-headless");
        assert_eq!(config.assignment_config_map, "demo-shard-assignments");
        assert_eq!(config.coordinator_lease, "demo-shard-coordinator");
        assert_eq!(config.shard_lease_prefix, "demo-shard");
        assert_eq!(config.owner_port, 9000);
        assert_eq!(
            config.owner_endpoint(2),
            "demo-2.demo-headless.default.svc:9000"
        );
    }

    #[test]
    fn deserializes_flattened_backends_with_product_defaults() {
        let config: ShardingConfig<Demo> = serde_json::from_value(serde_json::json!({
            "virtual_shards": 4,
            "backend": "kubernetes",
            "namespace": "observability",
        }))
        .unwrap();
        let ShardingBackend::Kubernetes(settings) = &config.kind else {
            panic!("expected Kubernetes backend");
        };
        assert_eq!(settings.namespace, "observability");
        assert_eq!(settings.stateful_set, "demo");
        assert_eq!(
            config.io_concurrency_multiplier,
            DEFAULT_IO_CONCURRENCY_MULTIPLIER
        );
    }

    #[test]
    fn validates_static_coverage_mode_and_lease_intervals() {
        assert!(
            static_config(&[(0, 4), (4, 8)])
                .validate(ServerMode::Writer)
                .is_ok()
        );
        assert!(
            static_config(&[(0, 4), (5, 8)])
                .validate(ServerMode::Writer)
                .is_err()
        );
        assert!(
            static_config(&[(0, 4)])
                .validate(ServerMode::Writer)
                .is_err()
        );
        assert!(
            static_config(&[(0, 8)])
                .validate(ServerMode::Standalone)
                .is_err()
        );
        let mut kubernetes = ShardingConfig::<Demo> {
            kind: ShardingBackend::Kubernetes(KubernetesShardingConfig {
                renew_interval_seconds: 15,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(kubernetes.validate(ServerMode::Writer).is_err());
        kubernetes.kind = ShardingBackend::Kubernetes(KubernetesShardingConfig::default());
        assert!(kubernetes.validate(ServerMode::Writer).is_ok());
    }

    #[test]
    fn static_assignment_routes_owners_and_startup_shards() {
        let config = static_config(&[(0, 3), (3, 8)]);
        let grpc: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let (local, assignment) = config.static_assignment(grpc).unwrap();
        assert_eq!(local, "owner-0");
        assert_eq!(
            config.startup_shards(ServerMode::Writer, &assignment, &local),
            (0..3).map(ShardId::new).collect::<Vec<_>>()
        );
        assert_eq!(
            config
                .startup_shards(ServerMode::Reader, &assignment, &local)
                .len(),
            8
        );
        let owner = assignment.owner_of(ShardId::new(5)).unwrap();
        assert_eq!(
            config.owner_endpoint(grpc, owner).as_deref(),
            Some("owner-1:9000")
        );
        assert!(
            config
                .owner_endpoint(grpc, &Owner::new("missing", 9))
                .is_none()
        );
    }
}
