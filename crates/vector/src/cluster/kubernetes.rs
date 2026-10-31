//! Kubernetes-backed topology, shard leases, and coordinator.
//!
//! - The [`Topology`] is the `spec` of a `VectorTopology` custom resource
//!   (`{name}-topology`), replaced with `resourceVersion` preconditions so
//!   every change is a compare-and-swap.
//! - Shard leases are `coordination.k8s.io` Leases
//!   (`{name}-vector-shard-{id}`), via the `sharding` crate.
//! - One writer at a time, elected with a Lease, is the coordinator: it
//!   publishes the initial topology, one shard per replica of the writer
//!   StatefulSet, and as the StatefulSet grows plans a split for each new
//!   writer once its pod is ready. Writers are never removed: their shards
//!   can't merge.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{ListParams, PostParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::WatchStreamExt;
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client};
use sharding::kubernetes::{
    KubernetesConfig, KubernetesCoordinatorElection, KubernetesLeaseBackend,
};
use sharding::{AssignmentGeneration, LeaseBackend, ShardId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::leases::{NoLeases, ShardLeases};
use super::store::TopologyStore;
use super::topology::Topology;
use super::{ClusterConfig, ClusterRuntime, KubernetesSettings, Role, now_ms};
use crate::error::{Error, Result};

const GROUP: &str = "telemetry.plural.sh";
const VERSION: &str = "v1alpha1";
const KIND: &str = "VectorTopology";
const PLURAL: &str = "vectortopologies";

fn kube_error(e: kube::Error) -> Error {
    Error::Unavailable(format!("kubernetes: {e}"))
}

fn conflict(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(response) if response.code == 409)
}

/// The settings of `config`, with `POD_NAMESPACE` applied.
pub fn settings(config: &ClusterConfig) -> Result<KubernetesSettings> {
    let mut settings = config.kubernetes.clone().ok_or_else(|| {
        Error::InvalidInput(format!(
            "the {:?} role needs a 'kubernetes' configuration section",
            config.role
        ))
    })?;
    if let Ok(namespace) = std::env::var("POD_NAMESPACE")
        && !namespace.is_empty()
    {
        settings.namespace = namespace;
    }
    Ok(settings)
}

/// The `sharding` crate's view of the writer StatefulSet.
fn sharding_config(settings: &KubernetesSettings) -> KubernetesConfig {
    KubernetesConfig {
        database_type: "vector".into(),
        database: settings.name.clone(),
        namespace: settings.namespace.clone(),
        stateful_set: settings.writer_stateful_set(),
        headless_service: settings.writer_headless_service(),
        owner_port: settings.port,
        shard_map: settings.topology_resource(),
        coordinator_lease: format!("{}-vector-coordinator", settings.name),
        shard_lease_prefix: format!("{}-vector-shard", settings.name),
        lease_duration: settings.lease_duration,
        ..KubernetesConfig::default()
    }
}

/// The runtime of a writer (`identity` is its pod name) or reader replica.
/// Writers also run the coordinator election until `cancel`.
pub async fn runtime(
    config: &ClusterConfig,
    identity: String,
    cancel: CancellationToken,
) -> Result<ClusterRuntime> {
    let settings = settings(config)?;
    // kube's rustls can't pick between the providers compiled into the
    // workspace on its own.
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    let client = Client::try_default().await.map_err(kube_error)?;
    let store = KubernetesTopologyStore::open(client.clone(), &settings, cancel.clone()).await?;
    let leases: Arc<dyn ShardLeases> = match config.role {
        Role::Writer => {
            let sharding = sharding_config(&settings);
            tokio::spawn(run_coordinator(
                store.clone(),
                KubernetesCoordinatorElection::new(client.clone(), &sharding),
                WriterMembership::new(client.clone(), &settings),
                identity.clone(),
                settings.lease_duration,
                cancel.clone(),
            ));
            Arc::new(KubernetesShardLeases(KubernetesLeaseBackend::new(
                client, &sharding, cancel,
            )))
        }
        Role::Reader => Arc::new(NoLeases),
        Role::Standalone => {
            return Err(Error::InvalidInput(
                "standalone clusters don't run on Kubernetes replicas".into(),
            ));
        }
    };
    Ok(ClusterRuntime {
        identity,
        topology: store,
        leases,
    })
}

/// Shard leases as Kubernetes Leases. Shard ids are never reused, so every
/// lease uses one generation.
pub struct KubernetesShardLeases(KubernetesLeaseBackend);

const LEASE_GENERATION: AssignmentGeneration = AssignmentGeneration::new(0);

fn lease_error(e: sharding::BoxError) -> Error {
    Error::Unavailable(format!("shard lease: {e}"))
}

#[async_trait]
impl ShardLeases for KubernetesShardLeases {
    async fn acquire(&self, owner: &str, shard: u32) -> Result<bool> {
        self.0
            .acquire(owner, ShardId::new(shard), LEASE_GENERATION)
            .await
            .map_err(lease_error)
    }

    async fn renew(&self, owner: &str, shard: u32) -> Result<bool> {
        self.0
            .renew(owner, ShardId::new(shard), LEASE_GENERATION)
            .await
            .map_err(lease_error)
    }

    async fn release(&self, owner: &str, shard: u32) -> Result<()> {
        self.0
            .release(owner, ShardId::new(shard), LEASE_GENERATION)
            .await
            .map_err(lease_error)
    }
}

fn api_resource() -> ApiResource {
    let mut resource = ApiResource::from_gvk(&GroupVersionKind::gvk(GROUP, VERSION, KIND));
    resource.plural = PLURAL.into();
    resource
}

fn decode(resource: &DynamicObject) -> Option<Topology> {
    let spec = resource.data.get("spec")?;
    match serde_json::from_value(spec.clone()) {
        Ok(topology) => Some(topology),
        Err(e) => {
            tracing::warn!(error = %e, "ignoring an undecodable VectorTopology");
            None
        }
    }
}

/// Advances `tx` to `next` unless it already has as new a generation.
fn advance(tx: &watch::Sender<Option<Topology>>, next: Topology) {
    let newer = tx
        .borrow()
        .as_ref()
        .is_none_or(|current| next.generation > current.generation);
    if newer {
        tracing::info!(generation = next.generation, "applied VectorTopology");
        tx.send_replace(Some(next));
    }
}

/// A topology stored in a `VectorTopology` resource.
pub struct KubernetesTopologyStore {
    api: Api<DynamicObject>,
    resource: ApiResource,
    name: String,
    namespace: String,
    labels: BTreeMap<String, String>,
    owner: Option<OwnerReference>,
    tx: watch::Sender<Option<Topology>>,
}

impl KubernetesTopologyStore {
    pub async fn open(
        client: Client,
        settings: &KubernetesSettings,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let resource = api_resource();
        let api = Api::namespaced_with(client.clone(), &settings.namespace, &resource);
        let stateful_sets: Api<StatefulSet> = Api::namespaced(client, &settings.namespace);
        let owner = stateful_sets
            .get_opt(&settings.writer_stateful_set())
            .await
            .map_err(kube_error)?
            .and_then(|set| set.metadata.uid)
            .map(|uid| OwnerReference {
                api_version: "apps/v1".into(),
                kind: "StatefulSet".into(),
                name: settings.writer_stateful_set(),
                uid,
                ..OwnerReference::default()
            });
        let name = settings.topology_resource();
        let initial = api
            .get_opt(&name)
            .await
            .map_err(kube_error)?
            .as_ref()
            .and_then(decode);
        let store = Arc::new(Self {
            api,
            resource,
            labels: BTreeMap::from([(format!("{GROUP}/vector"), settings.name.clone())]),
            namespace: settings.namespace.clone(),
            name,
            owner,
            tx: watch::channel(initial).0,
        });
        let watched = store.clone();
        tokio::spawn(async move {
            let selector = format!("metadata.name={}", watched.name);
            let mut events = watcher::watcher(
                watched.api.clone(),
                watcher::Config::default().fields(&selector),
            )
            .default_backoff()
            .boxed();
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    event = events.next() => match event {
                        Some(Ok(Event::Apply(resource) | Event::InitApply(resource))) => {
                            if let Some(next) = decode(&resource) {
                                advance(&watched.tx, next);
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "VectorTopology watch failed; reconnecting");
                        }
                        None => return,
                    }
                }
            }
        });
        Ok(store)
    }
}

#[async_trait]
impl TopologyStore for KubernetesTopologyStore {
    async fn load(&self) -> Result<Option<Topology>> {
        let current = self
            .api
            .get_opt(&self.name)
            .await
            .map_err(kube_error)?
            .as_ref()
            .and_then(decode);
        if let Some(topology) = &current {
            advance(&self.tx, topology.clone());
        }
        Ok(current)
    }

    async fn publish(&self, expected: Option<u64>, next: &Topology) -> Result<bool> {
        next.validate()?;
        let existing = self.api.get_opt(&self.name).await.map_err(kube_error)?;
        if existing.as_ref().and_then(decode).map(|t| t.generation) != expected {
            return Ok(false);
        }
        let mut resource = DynamicObject::new(&self.name, &self.resource);
        resource.metadata.namespace = Some(self.namespace.clone());
        resource.metadata.labels = Some(self.labels.clone());
        resource.metadata.owner_references = self.owner.clone().map(|owner| vec![owner]);
        resource.data = serde_json::json!({ "spec": next });
        let result = match existing {
            Some(existing) => {
                resource.metadata.resource_version = existing.metadata.resource_version;
                self.api
                    .replace(&self.name, &PostParams::default(), &resource)
                    .await
            }
            None => self.api.create(&PostParams::default(), &resource).await,
        };
        match result {
            Ok(_) => {
                advance(&self.tx, next.clone());
                Ok(true)
            }
            Err(e) if conflict(&e) => Ok(false),
            Err(e) => Err(kube_error(e)),
        }
    }

    fn watch(&self) -> watch::Receiver<Option<Topology>> {
        self.tx.subscribe()
    }
}

/// The writer StatefulSet's replicas, by pod name in ordinal order.
struct WriterMembership {
    stateful_sets: Api<StatefulSet>,
    pods: Api<Pod>,
    name: String,
}

/// The writers of the StatefulSet's `spec.replicas`, and those of them whose
/// pods are ready.
struct Writers {
    replicas: Vec<String>,
    ready: Vec<String>,
}

impl WriterMembership {
    fn new(client: Client, settings: &KubernetesSettings) -> Self {
        Self {
            stateful_sets: Api::namespaced(client.clone(), &settings.namespace),
            pods: Api::namespaced(client, &settings.namespace),
            name: settings.writer_stateful_set(),
        }
    }

    async fn writers(&self) -> Result<Writers> {
        let stateful_set = self
            .stateful_sets
            .get(&self.name)
            .await
            .map_err(kube_error)?;
        let spec = stateful_set.spec.unwrap_or_default();
        let count = spec.replicas.unwrap_or(1).max(0);
        let replicas: Vec<String> = (0..count).map(|i| format!("{}-{i}", self.name)).collect();
        let selector = spec
            .selector
            .match_labels
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let pods = self
            .pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(kube_error)?;
        let ready_pods: Vec<String> = pods
            .items
            .into_iter()
            .filter(|pod| pod.metadata.deletion_timestamp.is_none() && pod_ready(pod))
            .filter_map(|pod| pod.metadata.name)
            .collect();
        let ready = replicas
            .iter()
            .filter(|name| ready_pods.contains(name))
            .cloned()
            .collect();
        Ok(Writers { replicas, ready })
    }
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
}

/// While elected, publishes the initial topology and plans splits for new
/// writers.
async fn run_coordinator(
    store: Arc<KubernetesTopologyStore>,
    election: KubernetesCoordinatorElection,
    membership: WriterMembership,
    identity: String,
    lease_duration: Duration,
    cancel: CancellationToken,
) {
    let interval = (lease_duration / 3).max(Duration::from_secs(1));
    let mut leader = false;
    loop {
        let elected = if leader {
            election.renew(&identity).await
        } else {
            election.try_acquire(&identity).await
        };
        match elected {
            Ok(now) => {
                if now != leader {
                    tracing::info!(leader = now, "vector coordinator leadership changed");
                }
                leader = now;
            }
            Err(e) => {
                tracing::warn!(error = %e, "vector coordinator election failed");
                leader = false;
            }
        }
        if leader && let Err(e) = coordinate(&store, &membership).await {
            tracing::warn!(error = %e, "vector coordinator pass failed");
        }
        tokio::select! {
            () = cancel.cancelled() => {
                if leader && let Err(e) = election.release(&identity).await {
                    tracing::warn!(error = %e, "releasing vector coordinator leadership failed");
                }
                return;
            }
            () = tokio::time::sleep(interval) => {}
        }
    }
}

/// Publishes one shard per writer replica when there is no topology yet,
/// and otherwise plans a split for each ready writer without a shard.
async fn coordinate(store: &KubernetesTopologyStore, membership: &WriterMembership) -> Result<()> {
    let writers = membership.writers().await?;
    let Some(current) = store.load().await? else {
        let initial = Topology::initial(&writers.replicas, now_ms());
        if store.publish(None, &initial).await? {
            tracing::info!(
                shards = writers.replicas.len(),
                "published the initial vector topology"
            );
        }
        return Ok(());
    };
    if let Some(next) = current.plan(&writers.ready)
        && store.publish(Some(current.generation), &next).await?
    {
        let planned: Vec<String> = next
            .active()
            .filter_map(|s| s.split_to.as_ref().map(|to| format!("{} -> {to}", s.id)))
            .collect();
        tracing::info!(
            generation = next.generation,
            planned = ?planned,
            "planned vector shard splits for new writers"
        );
    }
    Ok(())
}
