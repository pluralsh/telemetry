//! Kubernetes-backed assignment, membership, leases, and owner resolution.
//!
//! Resource encoding and lease decisions are pure functions so they can be
//! tested without a Kubernetes cluster. Network access is confined to the
//! backend structs in this module.

use std::{
    collections::BTreeMap,
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        coordination::v1::{Lease, LeaseSpec},
        core::v1::ConfigMap,
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{Api, Client, Error as KubeError, api::PostParams};
use tokio::{sync::watch, time};
use tokio_util::sync::CancellationToken;

use crate::{
    AssignmentGeneration, AssignmentStore, BoxError, LeaseBackend, Owner, OwnerResolver,
    ResolvedOwner, ShardMap, ShardRange,
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
    pub watch_poll_interval: Duration,
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
            watch_poll_interval: Duration::from_secs(2),
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
}

impl KubernetesLeaseBackend {
    pub fn new(client: Client, config: &KubernetesConfig) -> Self {
        Self {
            prefix: config.shard_lease_prefix.clone(),
            leases: KubernetesLeaseSet::new(
                client,
                config.namespace.clone(),
                config.lease_duration,
            ),
        }
    }

    pub fn lease_name(&self, range: ShardRange) -> String {
        range_lease_name(&self.prefix, range)
    }
}

pub fn range_lease_name(prefix: &str, range: ShardRange) -> String {
    format!("{prefix}-{}-{}", range.start(), range.end())
}

#[async_trait]
impl LeaseBackend for KubernetesLeaseBackend {
    async fn acquire(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        Ok(self
            .leases
            .acquire(&self.lease_name(range), owner_id, generation)
            .await?)
    }

    async fn renew(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        Ok(self
            .leases
            .renew(&self.lease_name(range), owner_id, generation)
            .await?)
    }

    async fn release(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        self.leases
            .release(&self.lease_name(range), owner_id, generation)
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
        let interval = config.watch_poll_interval;
        tokio::spawn(async move {
            let mut poll = time::interval(interval);
            poll.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    _ = poll.tick() => {
                        match watcher.api.get_opt(&watcher.name).await {
                            Ok(Some(config_map)) => match decode_assignment(&config_map) {
                                Ok(Some(next)) => {
                                    let current = watcher.tx.borrow().as_ref().map(|map| map.generation);
                                    if current.is_none_or(|generation| next.generation > generation) {
                                        watcher.tx.send_replace(Some(next));
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => tracing::warn!(%error, "invalid Kubernetes shard assignment"),
                            },
                            Ok(None) => {}
                            Err(error) => tracing::warn!(%error, "failed to watch Kubernetes shard assignment"),
                        }
                    }
                }
            }
        });
        Ok(store)
    }
}

#[async_trait]
impl AssignmentStore for KubernetesAssignmentStore {
    async fn load(&self) -> Result<Option<ShardMap>, BoxError> {
        match self.api.get_opt(&self.name).await? {
            Some(config_map) => decode_assignment(&config_map),
            None => Ok(None),
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
    fn range_leases_are_small_and_stably_named() {
        assert_eq!(
            range_lease_name("meter-shard", ShardRange::within(16, 32, 64).unwrap()),
            "meter-shard-16-32"
        );
    }
}
