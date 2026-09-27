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
        core::v1::ConfigMap,
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    Api, Client, Error as KubeError,
    api::PostParams,
    runtime::{
        WatchStreamExt,
        watcher::{self, Event},
    },
};
use tokio::{sync::watch, time};
use tokio_util::sync::CancellationToken;

use crate::{
    AssignmentGeneration, AssignmentStore, BoxError, LeaseBackend, Owner, OwnerResolver,
    ResolvedOwner, ShardId, ShardMap, balanced_contiguous,
};

const ASSIGNMENT_KEY: &str = "assignment.json";
const GENERATION_ANNOTATION: &str = "telemetry.plural.sh/assignment-generation";
const RENEWED_AT_ANNOTATION: &str = "telemetry.plural.sh/renewed-at-unix-ms";
const LEASE_GENERATION_ANNOTATION: &str = "telemetry.plural.sh/assignment-generation";

#[derive(Debug, Clone)]
pub struct KubernetesConfig {
    pub namespace: String,
    pub stateful_set: String,
    pub headless_service: String,
    pub owner_port: u16,
    pub assignment_config_map: String,
    pub coordinator_lease: String,
    pub shard_lease_prefix: String,
    pub lease_duration: Duration,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_owned(),
            stateful_set: "meter".to_owned(),
            headless_service: "meter-headless".to_owned(),
            owner_port: 9090,
            assignment_config_map: "meter-shard-assignments".to_owned(),
            coordinator_lease: "meter-shard-coordinator".to_owned(),
            shard_lease_prefix: "meter-shard".to_owned(),
            lease_duration: Duration::from_secs(15),
        }
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

fn lease_resource(
    name: &str,
    namespace: &str,
    resource_version: Option<String>,
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
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            resource_version,
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
    duration: Duration,
}

impl KubernetesLeaseSet {
    fn new(client: Client, namespace: impl Into<String>, duration: Duration) -> Self {
        let namespace = namespace.into();
        Self {
            api: Api::namespaced(client, &namespace),
            namespace,
            duration,
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
            name,
            &self.namespace,
            existing
                .as_ref()
                .and_then(|lease| lease.metadata.resource_version.clone()),
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
            name,
            &self.namespace,
            existing.metadata.resource_version,
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
            leases: KubernetesLeaseSet::new(
                client,
                config.namespace.clone(),
                config.lease_duration,
            ),
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
        let leases =
            KubernetesLeaseSet::new(client, config.namespace.clone(), config.lease_duration);
        let prefix = config.shard_lease_prefix.clone();
        let (release_tx, _) = watch::channel(0);
        let watch_api = leases.api.clone();
        let watch_prefix = format!("{prefix}-");
        let watch_tx = release_tx.clone();
        tokio::spawn(async move {
            let mut events = watcher::watcher(watch_api, watcher::Config::default())
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
    api: Api<ConfigMap>,
    namespace: String,
    name: String,
    tx: watch::Sender<Option<ShardMap>>,
}

impl KubernetesAssignmentStore {
    pub async fn new(
        client: Client,
        config: &KubernetesConfig,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, BoxError> {
        let api = Api::namespaced(client, &config.namespace);
        let initial = match api.get_opt(&config.assignment_config_map).await? {
            Some(config_map) => decode_assignment(&config_map)?,
            None => None,
        };
        let (tx, _) = watch::channel(initial);
        let store = Arc::new(Self {
            api,
            namespace: config.namespace.clone(),
            name: config.assignment_config_map.clone(),
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
                            Some(Ok(Event::Apply(config_map))) => {
                                apply_watched_assignment(&watcher.tx, &config_map);
                            }
                            Some(Ok(Event::Delete(_))) => {
                                tracing::warn!("Kubernetes shard assignment was deleted; retaining last known generation");
                            }
                            Some(Ok(Event::Init)) => relisted = None,
                            Some(Ok(Event::InitApply(config_map))) => {
                                match decode_assignment(&config_map) {
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

fn apply_watched_assignment(tx: &watch::Sender<Option<ShardMap>>, config_map: &ConfigMap) {
    match decode_assignment(config_map) {
        Ok(Some(next)) => apply_assignment_update(tx, next),
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "invalid Kubernetes shard assignment"),
    }
}

fn apply_assignment_update(tx: &watch::Sender<Option<ShardMap>>, next: ShardMap) {
    let current = tx.borrow().as_ref().map(|map| map.generation);
    if current.is_none_or(|generation| next.generation > generation) {
        tx.send_replace(Some(next));
    }
}

#[async_trait]
impl AssignmentStore for KubernetesAssignmentStore {
    async fn load(&self) -> Result<Option<ShardMap>, BoxError> {
        match self.api.get_opt(&self.name).await? {
            Some(config_map) => decode_assignment(&config_map),
            None => Ok(self.tx.borrow().clone()),
        }
    }

    async fn publish(&self, assignment: ShardMap) -> Result<(), BoxError> {
        if self
            .load()
            .await?
            .is_some_and(|current| current.generation >= assignment.generation)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "assignment generation must increase",
            )
            .into());
        }
        let config_map = encode_assignment(&self.name, &self.namespace, &assignment)?;
        match self.api.get_opt(&self.name).await? {
            Some(existing) => {
                let mut replacement = config_map;
                replacement.metadata.resource_version = existing.metadata.resource_version;
                self.api
                    .replace(&self.name, &PostParams::default(), &replacement)
                    .await?;
            }
            None => {
                self.api.create(&PostParams::default(), &config_map).await?;
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
    assignment: &ShardMap,
) -> Result<ConfigMap, serde_json::Error> {
    let mut annotations = BTreeMap::new();
    annotations.insert(
        GENERATION_ANNOTATION.to_owned(),
        assignment.generation.to_string(),
    );
    let mut data = BTreeMap::new();
    data.insert(
        ASSIGNMENT_KEY.to_owned(),
        serde_json::to_string(assignment)?,
    );
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        },
        data: Some(data),
        ..ConfigMap::default()
    })
}

fn decode_assignment(config_map: &ConfigMap) -> Result<Option<ShardMap>, BoxError> {
    let Some(serialized) = config_map
        .data
        .as_ref()
        .and_then(|data| data.get(ASSIGNMENT_KEY))
    else {
        return Ok(None);
    };
    let assignment: ShardMap = serde_json::from_str(serialized)?;
    let stamped_generation = config_map
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(GENERATION_ANNOTATION))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing generation stamp"))?
        .parse::<u64>()?;
    if assignment.generation != AssignmentGeneration::new(stamped_generation) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "generation stamp mismatch").into());
    }
    Ok(Some(assignment))
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
        let replicas = stateful_set
            .spec
            .and_then(|spec| spec.replicas)
            .unwrap_or(1)
            .max(0) as u32;
        Ok((0..replicas)
            .map(|ordinal| Owner::new(format!("{}-{ordinal}", self.stateful_set), ordinal))
            .collect())
    }
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
    virtual_shards: u32,
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
    let membership = StatefulSetMembership::new(client, &config);
    let renew_interval = (config.lease_duration / 3).max(Duration::from_secs(1));
    let mut ticker = time::interval(renew_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut leader = false;
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
            _ = ticker.tick() => {
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
                if !leader {
                    continue;
                }
                let owners = match membership.owners().await {
                    Ok(owners) => owners,
                    Err(error) => {
                        tracing::warn!(%error, "failed to observe StatefulSet membership");
                        continue;
                    }
                };
                let current = match store.load().await {
                    Ok(current) => current,
                    Err(error) => {
                        tracing::warn!(%error, "failed to load current shard assignment");
                        continue;
                    }
                };
                if !membership_changed(current.as_ref(), &owners, virtual_shards) {
                    continue;
                }
                let generation = current
                    .as_ref()
                    .map_or(AssignmentGeneration::new(1), |map| map.generation.next());
                let next = match balanced_contiguous(
                    generation,
                    virtual_shards,
                    &owners,
                    current.as_ref(),
                ) {
                    Ok(next) => next,
                    Err(error) => {
                        tracing::warn!(%error, "failed to plan shard assignment");
                        continue;
                    }
                };
                if let Err(error) = store.publish(next).await {
                    tracing::warn!(%error, "failed to publish shard assignment");
                    leader = false;
                }
            }
        }
    }
}

pub fn membership_changed(
    current: Option<&ShardMap>,
    owners: &[Owner],
    virtual_shards: u32,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.virtual_shards != virtual_shards {
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
                Owner::new("meter-0", 0),
                ShardRange::within(0, 64, 64).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn config_uses_stable_meter_dns_defaults() {
        let config = KubernetesConfig::default();
        let resolver = KubernetesOwnerResolver::new(&config);
        assert_eq!(
            resolver.endpoint(&Owner::new("meter-0", 0)).unwrap(),
            "meter-0.meter-headless.default.svc:9090"
        );
        assert_eq!(config.assignment_config_map, "meter-shard-assignments");
    }

    #[test]
    fn config_map_round_trip_requires_matching_generation_stamp() {
        let map = assignment();
        let mut config_map = encode_assignment("assignments", "testing", &map).unwrap();
        assert_eq!(decode_assignment(&config_map).unwrap(), Some(map));
        config_map
            .metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(GENERATION_ANNOTATION.to_owned(), "8".to_owned());
        assert!(decode_assignment(&config_map).is_err());
    }

    #[test]
    fn lease_decisions_are_generation_aware_and_expiry_aware() {
        let record = LeaseRecord {
            holder: "meter-0".to_owned(),
            generation: AssignmentGeneration::new(4),
            renewed_at_ms: 1_000,
            duration_ms: 15_000,
        };
        assert!(!can_acquire(
            Some(&record),
            "meter-1",
            AssignmentGeneration::new(5),
            2_000
        ));
        assert!(can_acquire(
            Some(&record),
            "meter-0",
            AssignmentGeneration::new(5),
            2_000
        ));
        assert!(can_acquire(
            Some(&record),
            "meter-1",
            AssignmentGeneration::new(5),
            16_000
        ));
    }

    #[test]
    fn shard_leases_are_small_and_stably_named() {
        assert_eq!(
            shard_lease_name("meter-shard", ShardId::new(16)),
            "meter-shard-0016"
        );
    }

    #[test]
    fn lease_watch_only_notifies_for_released_shard_leases() {
        let active = lease_resource(
            "meter-shard-0016",
            "default",
            None,
            "meter-0",
            AssignmentGeneration::new(1),
            Duration::from_secs(15),
            1_000,
        );
        assert!(is_shard_lease(&active, "meter-shard-"));
        assert!(!is_released_shard_lease(&active, "meter-shard-"));

        let released = released_lease_resource(&active);
        assert!(is_released_shard_lease(&released, "meter-shard-"));

        let coordinator = lease_resource(
            "meter-shard-coordinator",
            "default",
            None,
            "meter-0",
            AssignmentGeneration::default(),
            Duration::from_secs(15),
            1_000,
        );
        assert!(!is_shard_lease(&coordinator, "meter-shard-"));
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
