//! Kubernetes-backed assignment, membership, leases, and owner resolution.
//!
//! Resource encoding and lease decisions are pure functions so they can be
//! tested without a Kubernetes cluster. Network access is confined to the
//! backend structs in this module.

use std::{
    collections::{BTreeMap, HashSet},
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        coordination::v1::{Lease, LeaseSpec},
    },
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{
    Api, Client, Error as KubeError,
    api::PostParams,
    core::{ApiResource, DynamicObject, GroupVersionKind},
    runtime::{
        WatchStreamExt,
        watcher::{self, Event},
    },
};
use tokio::{sync::watch, time};
use tokio_util::sync::CancellationToken;

use crate::{
    AssignmentGeneration, AssignmentStore, BoxError, EpochPolicy, LeaseBackend, Owner,
    OwnerResolver, ResolvedOwner, ShardId, ShardMap, balanced_contiguous,
};

const RENEWED_AT_ANNOTATION: &str = "telemetry.plural.sh/renewed-at-unix-ms";
const LEASE_GENERATION_ANNOTATION: &str = "telemetry.plural.sh/assignment-generation";
const LABEL_DOMAIN: &str = "telemetry.plural.sh";

#[derive(Debug, Clone)]
pub struct KubernetesConfig {
    /// Product name, e.g. `metrics`.
    pub database_type: String,
    /// Database (custom resource) name.
    pub database: String,
    pub namespace: String,
    pub stateful_set: String,
    pub headless_service: String,
    pub owner_port: u16,
    pub shard_map: String,
    pub coordinator_lease: String,
    pub shard_lease_prefix: String,
    pub lease_duration: Duration,
    pub epoch_policy: EpochPolicy,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            database_type: "metrics".to_owned(),
            database: "metrics".to_owned(),
            namespace: "default".to_owned(),
            stateful_set: "metrics".to_owned(),
            headless_service: "metrics-headless".to_owned(),
            owner_port: 9090,
            shard_map: "metrics-shard-map".to_owned(),
            coordinator_lease: "metrics-shard-coordinator".to_owned(),
            shard_lease_prefix: "metrics-shard".to_owned(),
            lease_duration: Duration::from_secs(15),
            epoch_policy: EpochPolicy::default(),
        }
    }
}

impl KubernetesConfig {
    /// Label carried by every Lease of this database:
    /// `telemetry.plural.sh/<database_type>=<database>`.
    pub fn lease_label(&self) -> (String, String) {
        (
            format!("{LABEL_DOMAIN}/{}", self.database_type),
            self.database.clone(),
        )
    }

    fn lease_selector(&self) -> String {
        let (key, value) = self.lease_label();
        format!("{key}={value}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseRecord {
    holder: String,
    generation: AssignmentGeneration,
    renewed_at_ms: u64,
    duration_ms: u64,
}

impl LeaseRecord {
    fn is_expired(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.renewed_at_ms) >= self.duration_ms
    }
}

fn lease_record(lease: &Lease) -> Option<LeaseRecord> {
    let annotations = lease.metadata.annotations.as_ref()?;
    let holder = lease.spec.as_ref()?.holder_identity.clone()?;
    let generation = annotations
        .get(LEASE_GENERATION_ANNOTATION)?
        .parse()
        .ok()
        .map(AssignmentGeneration::new)?;
    let renewed_at_ms = annotations.get(RENEWED_AT_ANNOTATION)?.parse().ok()?;
    let duration_ms = lease
        .spec
        .as_ref()?
        .lease_duration_seconds
        .and_then(|seconds| u64::try_from(seconds).ok())?
        .saturating_mul(1_000);
    Some(LeaseRecord {
        holder,
        generation,
        renewed_at_ms,
        duration_ms,
    })
}

fn can_acquire(
    existing: Option<&LeaseRecord>,
    owner: &str,
    generation: AssignmentGeneration,
    now_ms: u64,
) -> bool {
    existing.is_none_or(|lease| {
        lease.holder == owner && lease.generation <= generation || lease.is_expired(now_ms)
    })
}

struct LeaseMeta<'a> {
    name: &'a str,
    namespace: &'a str,
    labels: &'a BTreeMap<String, String>,
    resource_version: Option<String>,
}

fn lease_resource(
    meta: LeaseMeta<'_>,
    owner: &str,
    generation: AssignmentGeneration,
    duration: Duration,
    now_ms: u64,
) -> Lease {
    let mut annotations = BTreeMap::new();
    annotations.insert(
        LEASE_GENERATION_ANNOTATION.to_owned(),
        generation.to_string(),
    );
    annotations.insert(RENEWED_AT_ANNOTATION.to_owned(), now_ms.to_string());
    Lease {
        metadata: ObjectMeta {
            name: Some(meta.name.to_owned()),
            namespace: Some(meta.namespace.to_owned()),
            resource_version: meta.resource_version,
            labels: Some(meta.labels.clone()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(owner.to_owned()),
            lease_duration_seconds: Some(duration.as_secs().try_into().unwrap_or(i32::MAX)),
            ..LeaseSpec::default()
        }),
    }
}

fn released_lease_resource(existing: &Lease) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: existing.metadata.name.clone(),
            namespace: existing.metadata.namespace.clone(),
            resource_version: existing.metadata.resource_version.clone(),
            labels: existing.metadata.labels.clone(),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec::default()),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn conflict(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(response) if response.code == 409)
}

#[derive(Clone)]
struct KubernetesLeaseSet {
    api: Api<Lease>,
    namespace: String,
    labels: BTreeMap<String, String>,
    duration: Duration,
}

impl KubernetesLeaseSet {
    fn new(client: Client, config: &KubernetesConfig) -> Self {
        Self {
            api: Api::namespaced(client, &config.namespace),
            namespace: config.namespace.clone(),
            labels: BTreeMap::from([config.lease_label()]),
            duration: config.lease_duration,
        }
    }

    fn meta<'a>(&'a self, name: &'a str, resource_version: Option<String>) -> LeaseMeta<'a> {
        LeaseMeta {
            name,
            namespace: &self.namespace,
            labels: &self.labels,
            resource_version,
        }
    }

    async fn acquire(
        &self,
        name: &str,
        owner: &str,
        generation: AssignmentGeneration,
    ) -> Result<bool, KubeError> {
        let existing = self.api.get_opt(name).await?;
        let timestamp = now_ms();
        if !can_acquire(
            existing.as_ref().and_then(lease_record).as_ref(),
            owner,
            generation,
            timestamp,
        ) {
            return Ok(false);
        }
        let resource = lease_resource(
            self.meta(
                name,
                existing
                    .as_ref()
                    .and_then(|lease| lease.metadata.resource_version.clone()),
            ),
            owner,
            generation,
            self.duration,
            timestamp,
        );
        let result = if existing.is_some() {
            self.api
                .replace(name, &PostParams::default(), &resource)
                .await
        } else {
            self.api.create(&PostParams::default(), &resource).await
        };
        match result {
            Ok(_) => Ok(true),
            Err(error) if conflict(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn renew(
        &self,
        name: &str,
        owner: &str,
        generation: AssignmentGeneration,
    ) -> Result<bool, KubeError> {
        let Some(existing) = self.api.get_opt(name).await? else {
            return Ok(false);
        };
        let Some(record) = lease_record(&existing) else {
            return Ok(false);
        };
        if record.holder != owner || record.generation != generation {
            return Ok(false);
        }
        let resource = lease_resource(
            self.meta(name, existing.metadata.resource_version),
            owner,
            generation,
            self.duration,
            now_ms(),
        );
        match self
            .api
            .replace(name, &PostParams::default(), &resource)
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if conflict(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn release(
        &self,
        name: &str,
        owner: &str,
        generation: AssignmentGeneration,
    ) -> Result<(), KubeError> {
        let Some(existing) = self.api.get_opt(name).await? else {
            return Ok(());
        };
        if lease_record(&existing)
            .is_some_and(|record| record.holder == owner && record.generation == generation)
        {
            let released = released_lease_resource(&existing);
            match self
                .api
                .replace(name, &PostParams::default(), &released)
                .await
            {
                Ok(_) => {}
                Err(error) if conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

pub struct KubernetesCoordinatorElection {
    lease_name: String,
    leases: KubernetesLeaseSet,
}

impl KubernetesCoordinatorElection {
    pub fn new(client: Client, config: &KubernetesConfig) -> Self {
        Self {
            lease_name: config.coordinator_lease.clone(),
            leases: KubernetesLeaseSet::new(client, config),
        }
    }

    pub async fn try_acquire(&self, identity: &str) -> Result<bool, KubeError> {
        self.leases
            .acquire(&self.lease_name, identity, AssignmentGeneration::default())
            .await
    }

    pub async fn renew(&self, identity: &str) -> Result<bool, KubeError> {
        self.leases
            .renew(&self.lease_name, identity, AssignmentGeneration::default())
            .await
    }

    pub async fn release(&self, identity: &str) -> Result<(), KubeError> {
        self.leases
            .release(&self.lease_name, identity, AssignmentGeneration::default())
            .await
    }
}

pub struct KubernetesLeaseBackend {
    prefix: String,
    leases: KubernetesLeaseSet,
    release_tx: watch::Sender<u64>,
}

impl KubernetesLeaseBackend {
    pub fn new(client: Client, config: &KubernetesConfig, cancel: CancellationToken) -> Self {
        let leases = KubernetesLeaseSet::new(client, config);
        let prefix = config.shard_lease_prefix.clone();
        let (release_tx, _) = watch::channel(0);
        let watch_api = leases.api.clone();
        let watch_prefix = format!("{prefix}-");
        let watch_tx = release_tx.clone();
        let selector = config.lease_selector();
        tokio::spawn(async move {
            // The coordinator lease shares the label; the name check filters it out.
            let mut events =
                watcher::watcher(watch_api, watcher::Config::default().labels(&selector))
                    .default_backoff()
                    .boxed();
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    event = events.next() => match event {
                        Some(Ok(Event::Apply(lease) | Event::InitApply(lease)))
                            if is_released_shard_lease(&lease, &watch_prefix) =>
                        {
                            notify_lease_release(&watch_tx);
                        }
                        Some(Ok(Event::Delete(lease)))
                            if is_shard_lease(&lease, &watch_prefix) =>
                        {
                            notify_lease_release(&watch_tx);
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(%error, "Kubernetes shard lease watch failed; reconnecting");
                        }
                        None => return,
                    }
                }
            }
        });
        Self {
            prefix,
            leases,
            release_tx,
        }
    }

    pub fn lease_name(&self, shard: ShardId) -> String {
        shard_lease_name(&self.prefix, shard)
    }
}

fn is_shard_lease(lease: &Lease, prefix: &str) -> bool {
    lease.metadata.name.as_deref().is_some_and(|name| {
        name.strip_prefix(prefix)
            .is_some_and(|id| id.parse::<u32>().is_ok())
    })
}

fn is_released_shard_lease(lease: &Lease, prefix: &str) -> bool {
    is_shard_lease(lease, prefix) && lease_record(lease).is_none()
}

fn notify_lease_release(tx: &watch::Sender<u64>) {
    let next = tx.borrow().wrapping_add(1);
    tx.send_replace(next);
}

pub fn shard_lease_name(prefix: &str, shard: ShardId) -> String {
    format!("{prefix}-{:04}", shard.get())
}

#[async_trait]
impl LeaseBackend for KubernetesLeaseBackend {
    fn watch_releases(&self) -> watch::Receiver<u64> {
        self.release_tx.subscribe()
    }

    async fn acquire(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        Ok(self
            .leases
            .acquire(&self.lease_name(shard), owner_id, generation)
            .await?)
    }

    async fn renew(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        Ok(self
            .leases
            .renew(&self.lease_name(shard), owner_id, generation)
            .await?)
    }

    async fn release(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        self.leases
            .release(&self.lease_name(shard), owner_id, generation)
            .await?;
        Ok(())
    }
}

pub struct KubernetesAssignmentStore {
    api: Api<DynamicObject>,
    api_resource: ApiResource,
    namespace: String,
    name: String,
    labels: BTreeMap<String, String>,
    owner_reference: Option<OwnerReference>,
    tx: watch::Sender<Option<ShardMap>>,
}

impl KubernetesAssignmentStore {
    pub async fn new(
        client: Client,
        config: &KubernetesConfig,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, BoxError> {
        let api_resource = shard_map_api_resource();
        let api = Api::namespaced_with(client.clone(), &config.namespace, &api_resource);
        let stateful_sets: Api<StatefulSet> = Api::namespaced(client, &config.namespace);
        let owner_reference = stateful_sets
            .get(&config.stateful_set)
            .await?
            .metadata
            .uid
            .map(|uid| OwnerReference {
                api_version: "apps/v1".to_owned(),
                kind: "StatefulSet".to_owned(),
                name: config.stateful_set.clone(),
                uid,
                ..OwnerReference::default()
            });
        let initial = api
            .get_opt(&config.shard_map)
            .await?
            .and_then(|resource| decode_current(&resource));
        let (tx, _) = watch::channel(initial);
        let store = Arc::new(Self {
            api,
            api_resource,
            namespace: config.namespace.clone(),
            name: config.shard_map.clone(),
            labels: BTreeMap::from([config.lease_label()]),
            owner_reference,
            tx,
        });
        let watcher = store.clone();
        tokio::spawn(async move {
            let selector = format!("metadata.name={}", watcher.name);
            let mut events = watcher::watcher(
                watcher.api.clone(),
                watcher::Config::default().fields(&selector),
            )
            .default_backoff()
            .boxed();
            let mut relisted = None;
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    event = events.next() => {
                        match event {
                            Some(Ok(Event::Apply(resource))) => {
                                apply_watched_assignment(&watcher.tx, &resource);
                            }
                            Some(Ok(Event::Delete(_))) => {
                                tracing::warn!("Kubernetes shard assignment was deleted; retaining last known generation");
                            }
                            Some(Ok(Event::Init)) => relisted = None,
                            Some(Ok(Event::InitApply(resource))) => {
                                match decode_assignment(&resource) {
                                    Ok(next) => relisted = next,
                                    Err(error) => tracing::warn!(%error, "invalid relisted Kubernetes shard assignment"),
                                }
                            }
                            Some(Ok(Event::InitDone)) => {
                                if let Some(next) = relisted.take() {
                                    apply_assignment_update(&watcher.tx, next);
                                }
                            }
                            Some(Err(error)) => {
                                tracing::warn!(%error, "Kubernetes shard assignment watch failed; reconnecting");
                            }
                            None => return,
                        }
                    }
                }
            }
        });
        Ok(store)
    }
}

fn shard_map_api_resource() -> ApiResource {
    let mut resource = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "telemetry.plural.sh",
        "v1alpha1",
        "ShardMap",
    ));
    resource.plural = "shardmaps".to_owned();
    resource
}

fn apply_watched_assignment(tx: &watch::Sender<Option<ShardMap>>, resource: &DynamicObject) {
    match decode_assignment(resource) {
        Ok(Some(next)) => apply_assignment_update(tx, next),
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "invalid Kubernetes shard assignment"),
    }
}

fn apply_assignment_update(tx: &watch::Sender<Option<ShardMap>>, next: ShardMap) {
    let current = tx.borrow().as_ref().map(|map| map.generation);
    if current.is_none_or(|generation| next.generation > generation) {
        tracing::info!(
            generation = next.generation.get(),
            shard_count = next.shard_count,
            "applied Kubernetes ShardMap generation"
        );
        tx.send_replace(Some(next));
    }
}

#[async_trait]
impl AssignmentStore for KubernetesAssignmentStore {
    async fn load(&self) -> Result<Option<ShardMap>, BoxError> {
        match self.api.get_opt(&self.name).await? {
            Some(resource) => Ok(decode_current(&resource)),
            None => Ok(self.tx.borrow().clone()),
        }
    }

    async fn publish(&self, assignment: ShardMap) -> Result<(), BoxError> {
        let existing = self.api.get_opt(&self.name).await?;
        let current = existing.as_ref().and_then(decode_current);
        if current.is_some_and(|current| current.generation >= assignment.generation) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "assignment generation must increase",
            )
            .into());
        }
        let resource = encode_assignment(
            &self.name,
            &self.namespace,
            &self.labels,
            self.owner_reference.as_ref(),
            &self.api_resource,
            &assignment,
        )?;
        match existing {
            Some(existing) => {
                let mut replacement = resource;
                replacement.metadata.resource_version = existing.metadata.resource_version;
                self.api
                    .replace(&self.name, &PostParams::default(), &replacement)
                    .await?;
            }
            None => {
                self.api.create(&PostParams::default(), &resource).await?;
            }
        }
        self.tx.send_replace(Some(assignment));
        Ok(())
    }

    fn watch(&self) -> watch::Receiver<Option<ShardMap>> {
        self.tx.subscribe()
    }
}

fn encode_assignment(
    name: &str,
    namespace: &str,
    labels: &BTreeMap<String, String>,
    owner_reference: Option<&OwnerReference>,
    api_resource: &ApiResource,
    assignment: &ShardMap,
) -> Result<DynamicObject, serde_json::Error> {
    let mut resource = DynamicObject::new(name, api_resource);
    resource.metadata.namespace = Some(namespace.to_owned());
    resource.metadata.labels = Some(labels.clone());
    resource.metadata.owner_references = owner_reference.map(|reference| vec![reference.clone()]);
    resource.data = serde_json::json!({ "spec": assignment });
    Ok(resource)
}

/// The stored map, or `None` when it is absent or cannot be decoded (for
/// example one written by an incompatible release), so the elected
/// coordinator bootstraps a replacement instead of every replica failing.
fn decode_current(resource: &DynamicObject) -> Option<ShardMap> {
    decode_assignment(resource).unwrap_or_else(|error| {
        tracing::warn!(%error, "ignoring undecodable Kubernetes ShardMap; the coordinator will replace it");
        None
    })
}

fn decode_assignment(resource: &DynamicObject) -> Result<Option<ShardMap>, BoxError> {
    let Some(spec) = resource.data.get("spec") else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_value(spec.clone())?))
}

pub struct StatefulSetMembership {
    api: Api<StatefulSet>,
    stateful_set: String,
}

impl StatefulSetMembership {
    pub fn new(client: Client, config: &KubernetesConfig) -> Self {
        Self {
            api: Api::namespaced(client, &config.namespace),
            stateful_set: config.stateful_set.clone(),
        }
    }

    pub async fn owners(&self) -> Result<Vec<Owner>, KubeError> {
        let stateful_set = self.api.get(&self.stateful_set).await?;
        Ok(stateful_set_owners(&stateful_set, &self.stateful_set))
    }
}

fn stateful_set_owners(stateful_set: &StatefulSet, name: &str) -> Vec<Owner> {
    let replicas = stateful_set
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or(1)
        .max(0) as u32;
    (0..replicas)
        .map(|ordinal| Owner::new(format!("{name}-{ordinal}"), ordinal))
        .collect()
}

#[derive(Debug, Clone)]
pub struct KubernetesOwnerResolver {
    namespace: String,
    stateful_set: String,
    headless_service: String,
    port: u16,
}

impl KubernetesOwnerResolver {
    pub fn new(config: &KubernetesConfig) -> Self {
        Self {
            namespace: config.namespace.clone(),
            stateful_set: config.stateful_set.clone(),
            headless_service: config.headless_service.clone(),
            port: config.owner_port,
        }
    }

    pub fn endpoint(&self, owner: &Owner) -> Result<String, io::Error> {
        let expected = format!("{}-{}", self.stateful_set, owner.ordinal);
        if owner.id != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owner {} does not match StatefulSet identity {expected}",
                    owner.id
                ),
            ));
        }
        Ok(format!(
            "{}.{}.{}.svc:{}",
            owner.id, self.headless_service, self.namespace, self.port
        ))
    }
}

#[async_trait]
impl OwnerResolver for KubernetesOwnerResolver {
    async fn resolve(&self, owner: &Owner) -> Result<ResolvedOwner, BoxError> {
        Ok(ResolvedOwner {
            owner: owner.clone(),
            endpoint: self.endpoint(owner)?,
            socket_addr: None,
        })
    }
}

pub async fn run_kubernetes_coordinator(
    store: Arc<KubernetesAssignmentStore>,
    config: KubernetesConfig,
    identity: String,
    cancel: CancellationToken,
) {
    let client = match Client::try_default().await {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "cannot start Kubernetes shard coordinator");
            return;
        }
    };
    let election = KubernetesCoordinatorElection::new(client.clone(), &config);
    let coordinator_api: Api<Lease> = Api::namespaced(client.clone(), &config.namespace);
    let coordinator_selector = format!("metadata.name={}", config.coordinator_lease);
    let mut coordinator_events = watcher::watcher(
        coordinator_api,
        watcher::Config::default().fields(&coordinator_selector),
    )
    .default_backoff()
    .boxed();
    let stateful_set_api: Api<StatefulSet> = Api::namespaced(client, &config.namespace);
    let stateful_set_selector = format!("metadata.name={}", config.stateful_set);
    let mut stateful_set_events = watcher::watcher(
        stateful_set_api,
        watcher::Config::default().fields(&stateful_set_selector),
    )
    .default_backoff()
    .boxed();
    let mut assignments = store.watch();
    let renew_interval = (config.lease_duration / 3).max(Duration::from_secs(1));
    let mut ticker = time::interval(renew_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut leader = false;
    // Membership comes from the StatefulSet watch; the assignment is reloaded
    // only when membership, the assignment, or leadership changes.
    let mut owners: Option<Vec<Owner>> = None;
    let mut dirty = true;
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                if leader
                    && let Err(error) = election.release(&identity).await
                {
                    tracing::warn!(%error, "failed to release coordinator leadership");
                }
                return;
            }
            event = coordinator_events.next() => {
                match event {
                    Some(Ok(Event::Apply(lease) | Event::InitApply(lease)))
                        if lease_record(&lease).is_none() =>
                    {
                        leader = false;
                        ticker.reset_immediately();
                    }
                    Some(Ok(Event::Delete(_))) => {
                        leader = false;
                        ticker.reset_immediately();
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(%error, "Kubernetes coordinator lease watch failed; reconnecting");
                    }
                    None => {
                        tracing::error!("Kubernetes coordinator lease watch ended");
                        return;
                    }
                }
            }
            event = stateful_set_events.next() => {
                match event {
                    Some(Ok(Event::Apply(stateful_set) | Event::InitApply(stateful_set))) => {
                        let observed = stateful_set_owners(&stateful_set, &config.stateful_set);
                        if owners.as_ref() != Some(&observed) {
                            owners = Some(observed);
                            dirty = true;
                        }
                    }
                    Some(Ok(Event::Delete(_))) => {
                        tracing::warn!("writer StatefulSet was deleted; retaining last known membership");
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(%error, "Kubernetes StatefulSet watch failed; reconnecting");
                    }
                    None => {
                        tracing::error!("Kubernetes StatefulSet watch ended");
                        return;
                    }
                }
            }
            changed = assignments.changed() => {
                if changed.is_err() {
                    return;
                }
                assignments.borrow_and_update();
                dirty = true;
            }
            _ = ticker.tick() => {
                let was_leader = leader;
                leader = if leader {
                    match election.renew(&identity).await {
                        Ok(held) => held,
                        Err(error) => {
                            tracing::warn!(%error, "failed to renew coordinator leadership");
                            false
                        }
                    }
                } else {
                    match election.try_acquire(&identity).await {
                        Ok(acquired) => acquired,
                        Err(error) => {
                            tracing::warn!(%error, "failed to acquire coordinator leadership");
                            false
                        }
                    }
                };
                dirty |= leader && !was_leader;
            }
        }
        if !leader || !dirty {
            continue;
        }
        let Some(owners) = owners.as_deref() else {
            continue;
        };
        match reconcile_assignment(store.as_ref(), config.epoch_policy, owners).await {
            Reconcile::Done => dirty = false,
            Reconcile::Retry => {}
            Reconcile::LostLeadership => leader = false,
        }
    }
}

enum Reconcile {
    Done,
    /// Retried on the next renew tick.
    Retry,
    LostLeadership,
}

async fn reconcile_assignment(
    store: &KubernetesAssignmentStore,
    policy: EpochPolicy,
    owners: &[Owner],
) -> Reconcile {
    let desired_shards = match u32::try_from(owners.len()) {
        Ok(0) | Err(_) => {
            tracing::warn!(
                owners = owners.len(),
                "writer replica count cannot produce a shard assignment"
            );
            return Reconcile::Retry;
        }
        Ok(count) => count,
    };
    let current = match store.load().await {
        Ok(current) => current,
        Err(error) => {
            tracing::warn!(%error, "failed to load current shard assignment");
            return Reconcile::Retry;
        }
    };
    let planned = match plan_assignment(current.as_ref(), desired_shards, owners, policy, now_ns())
    {
        Ok(Some(next)) => next,
        Ok(None) => return Reconcile::Done,
        Err(error) => {
            tracing::warn!(%error, "failed to plan shard assignment");
            return Reconcile::Retry;
        }
    };
    match store.publish(planned).await {
        Ok(()) => Reconcile::Done,
        Err(error) => {
            tracing::warn!(%error, "failed to publish shard assignment");
            Reconcile::LostLeadership
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(i64::MAX)
}

/// Next ShardMap for `owners`, or `None` when the current one already fits.
///
/// Scale-up appends a routing epoch that takes effect at the next aligned
/// cutover; existing data never moves. Scale-down is not supported and keeps
/// the current map.
pub fn plan_assignment(
    current: Option<&ShardMap>,
    desired_shards: u32,
    owners: &[Owner],
    policy: EpochPolicy,
    now_ns: i64,
) -> Result<Option<ShardMap>, BoxError> {
    let Some(current) = current else {
        let epochs = ShardMap::initial_epochs(desired_shards)?;
        return Ok(Some(balanced_contiguous(
            AssignmentGeneration::new(1),
            epochs,
            owners,
            None,
        )?));
    };
    if desired_shards < current.shard_count {
        tracing::warn!(
            current = current.shard_count,
            desired = desired_shards,
            "writer scale-down is not supported"
        );
        return Ok(None);
    }
    if desired_shards == current.shard_count
        && !membership_changed(Some(current), owners, desired_shards)
    {
        return Ok(None);
    }
    let epochs = current.epochs_scaled_to(desired_shards, policy, now_ns)?;
    Ok(Some(balanced_contiguous(
        current.generation.next(),
        epochs,
        owners,
        Some(current),
    )?))
}

pub fn membership_changed(current: Option<&ShardMap>, owners: &[Owner], shard_count: u32) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.shard_count != shard_count {
        return true;
    }
    let current_owners = current
        .assignments
        .iter()
        .map(|assignment| (&assignment.owner.id, assignment.owner.ordinal))
        .collect::<HashSet<_>>();
    let desired_owners = owners
        .iter()
        .map(|owner| (&owner.id, owner.ordinal))
        .collect::<HashSet<_>>();
    current_owners != desired_owners
}

#[cfg(test)]
mod tests {
    use crate::{Assignment, AssignmentState, ShardRange};

    use super::*;

    fn assignment() -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(7),
            64,
            vec![Assignment::new(
                Owner::new("metrics-0", 0),
                ShardRange::within(0, 64, 64).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn config_uses_stable_metrics_dns_defaults() {
        let config = KubernetesConfig::default();
        let resolver = KubernetesOwnerResolver::new(&config);
        assert_eq!(
            resolver.endpoint(&Owner::new("metrics-0", 0)).unwrap(),
            "metrics-0.metrics-headless.default.svc:9090"
        );
        assert_eq!(config.shard_map, "metrics-shard-map");
    }

    #[test]
    fn shard_map_resource_round_trips() {
        let owners = (0..65)
            .map(|ordinal| Owner::new(format!("metrics-{ordinal}"), ordinal))
            .collect::<Vec<_>>();
        let map = plan_assignment(Some(&assignment()), 65, &owners, EpochPolicy::default(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(map.epochs.len(), 2);
        let resource = encode_assignment(
            "assignments",
            "testing",
            &BTreeMap::new(),
            None,
            &shard_map_api_resource(),
            &map,
        )
        .unwrap();
        assert_eq!(decode_assignment(&resource).unwrap(), Some(map));
        assert_eq!(resource.metadata.namespace.as_deref(), Some("testing"));
    }

    #[test]
    fn maps_from_incompatible_releases_are_treated_as_absent() {
        // A pre-epoch map after the current CRD pruned its `routing` field.
        let mut resource = DynamicObject::new("logs-writer-shard-map", &shard_map_api_resource());
        resource.data = serde_json::json!({
            "spec": {
                "generation": 1,
                "shard_count": 1,
                "assignments": [{
                    "owner": { "id": "logs-writer-0", "ordinal": 0 },
                    "range": { "start": 0, "end": 1 },
                    "state": "active"
                }]
            }
        });
        assert!(decode_assignment(&resource).is_err());
        assert_eq!(decode_current(&resource), None);
    }

    #[test]
    fn coordinator_plans_scale_up_epochs_and_ignores_scale_down() {
        let owners = |count: u32| {
            (0..count)
                .map(|ordinal| Owner::new(format!("metrics-{ordinal}"), ordinal))
                .collect::<Vec<_>>()
        };
        let policy = EpochPolicy::default();
        let initial = plan_assignment(None, 2, &owners(2), policy, 0)
            .unwrap()
            .unwrap();
        assert_eq!(initial.epochs.len(), 1);
        assert!(
            plan_assignment(Some(&initial), 2, &owners(2), policy, 0)
                .unwrap()
                .is_none()
        );
        assert!(
            plan_assignment(Some(&initial), 1, &owners(1), policy, 0)
                .unwrap()
                .is_none()
        );
        let scaled = plan_assignment(Some(&initial), 3, &owners(3), policy, 0)
            .unwrap()
            .unwrap();
        assert_eq!(scaled.generation, initial.generation.next());
        assert_eq!(scaled.shard_count, 3);
        assert_eq!(scaled.epochs[1].effective_from_ns, policy.cutover_after(0));
    }

    #[test]
    fn lease_decisions_are_generation_aware_and_expiry_aware() {
        let record = LeaseRecord {
            holder: "metrics-0".to_owned(),
            generation: AssignmentGeneration::new(4),
            renewed_at_ms: 1_000,
            duration_ms: 15_000,
        };
        assert!(!can_acquire(
            Some(&record),
            "metrics-1",
            AssignmentGeneration::new(5),
            2_000
        ));
        assert!(can_acquire(
            Some(&record),
            "metrics-0",
            AssignmentGeneration::new(5),
            2_000
        ));
        assert!(can_acquire(
            Some(&record),
            "metrics-1",
            AssignmentGeneration::new(5),
            16_000
        ));
    }

    #[test]
    fn shard_leases_are_small_and_stably_named() {
        assert_eq!(
            shard_lease_name("metrics-shard", ShardId::new(16)),
            "metrics-shard-0016"
        );
    }

    fn meta<'a>(name: &'a str, labels: &'a BTreeMap<String, String>) -> LeaseMeta<'a> {
        LeaseMeta {
            name,
            namespace: "default",
            labels,
            resource_version: None,
        }
    }

    #[test]
    fn leases_are_labeled_by_database_type_and_name_through_release() {
        let config = KubernetesConfig {
            database_type: "logs".to_owned(),
            database: "logs".to_owned(),
            ..KubernetesConfig::default()
        };
        assert_eq!(
            config.lease_label(),
            ("telemetry.plural.sh/logs".to_owned(), "logs".to_owned())
        );
        assert_eq!(config.lease_selector(), "telemetry.plural.sh/logs=logs");

        let labels = BTreeMap::from([config.lease_label()]);
        let active = lease_resource(
            meta("logs-shard-0001", &labels),
            "logs-0",
            AssignmentGeneration::new(1),
            Duration::from_secs(15),
            1_000,
        );
        assert_eq!(active.metadata.labels.as_ref(), Some(&labels));
        let released = released_lease_resource(&active);
        assert_eq!(released.metadata.labels.as_ref(), Some(&labels));
    }

    #[test]
    fn lease_watch_only_notifies_for_released_shard_leases() {
        let labels = BTreeMap::new();
        let active = lease_resource(
            meta("metrics-shard-0016", &labels),
            "metrics-0",
            AssignmentGeneration::new(1),
            Duration::from_secs(15),
            1_000,
        );
        assert!(is_shard_lease(&active, "metrics-shard-"));
        assert!(!is_released_shard_lease(&active, "metrics-shard-"));

        let released = released_lease_resource(&active);
        assert!(is_released_shard_lease(&released, "metrics-shard-"));

        let coordinator = lease_resource(
            meta("metrics-shard-coordinator", &labels),
            "metrics-0",
            AssignmentGeneration::default(),
            Duration::from_secs(15),
            1_000,
        );
        assert!(!is_shard_lease(&coordinator, "metrics-shard-"));
    }

    #[test]
    fn watched_assignments_only_advance_generations() {
        let (tx, mut rx) = watch::channel(Some(assignment()));
        let mut stale = assignment();
        stale.generation = AssignmentGeneration::new(6);
        apply_assignment_update(&tx, stale);
        assert!(!rx.has_changed().unwrap());

        let mut next = assignment();
        next.generation = AssignmentGeneration::new(8);
        apply_assignment_update(&tx, next.clone());
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update().as_ref(), Some(&next));
    }
}
