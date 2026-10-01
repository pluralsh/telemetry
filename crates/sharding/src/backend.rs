use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Mutex,
};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{
    AssignmentGeneration, AssignmentStore, BoxError, LeaseBackend, Owner, OwnerResolver,
    ResolvedOwner, ShardId, ShardMap,
};

pub struct FakeAssignmentStore {
    tx: watch::Sender<Option<ShardMap>>,
    published: Mutex<Vec<ShardMap>>,
}

impl FakeAssignmentStore {
    pub fn new(initial: Option<ShardMap>) -> Self {
        let (tx, _) = watch::channel(initial);
        Self {
            tx,
            published: Mutex::new(Vec::new()),
        }
    }

    pub fn update(&self, assignment: ShardMap) {
        self.published
            .lock()
            .expect("assignment history lock poisoned")
            .push(assignment.clone());
        self.tx.send_replace(Some(assignment));
    }

    pub fn history(&self) -> Vec<ShardMap> {
        self.published
            .lock()
            .expect("assignment history lock poisoned")
            .clone()
    }
}

#[async_trait]
impl AssignmentStore for FakeAssignmentStore {
    async fn load(&self) -> Result<Option<ShardMap>, BoxError> {
        Ok(self.tx.borrow().clone())
    }

    async fn publish(&self, assignment: ShardMap) -> Result<(), BoxError> {
        self.update(assignment);
        Ok(())
    }

    fn watch(&self) -> watch::Receiver<Option<ShardMap>> {
        self.tx.subscribe()
    }
}

pub type StandaloneAssignmentStore = FakeAssignmentStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseEvent {
    Acquired {
        owner: String,
        shard: ShardId,
        generation: AssignmentGeneration,
    },
    Renewed {
        owner: String,
        shard: ShardId,
        generation: AssignmentGeneration,
    },
    Released {
        owner: String,
        shard: ShardId,
        generation: AssignmentGeneration,
    },
}

pub struct FakeLeaseBackend {
    leases: Mutex<HashMap<ShardId, (String, AssignmentGeneration)>>,
    forced_loss: Mutex<HashSet<ShardId>>,
    events: Mutex<Vec<LeaseEvent>>,
    release_tx: watch::Sender<u64>,
}

impl Default for FakeLeaseBackend {
    fn default() -> Self {
        let (release_tx, _) = watch::channel(0);
        Self {
            leases: Mutex::new(HashMap::new()),
            forced_loss: Mutex::new(HashSet::new()),
            events: Mutex::new(Vec::new()),
            release_tx,
        }
    }
}

impl FakeLeaseBackend {
    pub fn lose(&self, shard: ShardId) {
        self.forced_loss
            .lock()
            .expect("forced-loss lock poisoned")
            .insert(shard);
        let removed = self
            .leases
            .lock()
            .expect("lease lock poisoned")
            .remove(&shard)
            .is_some();
        if removed {
            self.notify_release();
        }
    }

    pub fn holder(&self, shard: ShardId) -> Option<(String, AssignmentGeneration)> {
        self.leases
            .lock()
            .expect("lease lock poisoned")
            .get(&shard)
            .cloned()
    }

    pub fn events(&self) -> Vec<LeaseEvent> {
        self.events
            .lock()
            .expect("lease event lock poisoned")
            .clone()
    }

    fn notify_release(&self) {
        let next = self.release_tx.borrow().wrapping_add(1);
        self.release_tx.send_replace(next);
    }
}

#[async_trait]
impl LeaseBackend for FakeLeaseBackend {
    fn watch_releases(&self) -> watch::Receiver<u64> {
        self.release_tx.subscribe()
    }

    async fn acquire(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        if self
            .forced_loss
            .lock()
            .expect("forced-loss lock poisoned")
            .contains(&shard)
        {
            return Ok(false);
        }
        let mut leases = self.leases.lock().expect("lease lock poisoned");
        if let Some((holder, held_generation)) = leases.get(&shard)
            && (holder != owner_id || *held_generation > generation)
        {
            return Ok(false);
        }
        leases.insert(shard, (owner_id.to_owned(), generation));
        self.events
            .lock()
            .expect("lease event lock poisoned")
            .push(LeaseEvent::Acquired {
                owner: owner_id.to_owned(),
                shard,
                generation,
            });
        Ok(true)
    }

    async fn renew(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError> {
        if self
            .forced_loss
            .lock()
            .expect("forced-loss lock poisoned")
            .contains(&shard)
        {
            return Ok(false);
        }
        let leases = self.leases.lock().expect("lease lock poisoned");
        let owned = leases.get(&shard).is_some_and(|(holder, held_generation)| {
            holder == owner_id && *held_generation == generation
        });
        drop(leases);
        if owned {
            self.events
                .lock()
                .expect("lease event lock poisoned")
                .push(LeaseEvent::Renewed {
                    owner: owner_id.to_owned(),
                    shard,
                    generation,
                });
        }
        Ok(owned)
    }

    async fn release(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        let mut leases = self.leases.lock().expect("lease lock poisoned");
        let released = leases.get(&shard).is_some_and(|(holder, held_generation)| {
            holder == owner_id && *held_generation == generation
        });
        if released {
            leases.remove(&shard);
        }
        drop(leases);
        if released {
            self.notify_release();
        }
        self.events
            .lock()
            .expect("lease event lock poisoned")
            .push(LeaseEvent::Released {
                owner: owner_id.to_owned(),
                shard,
                generation,
            });
        Ok(())
    }
}

pub type StandaloneLeaseBackend = FakeLeaseBackend;

#[derive(Default)]
pub struct StaticOwnerResolver {
    endpoints: HashMap<String, String>,
}

impl StaticOwnerResolver {
    pub fn new(endpoints: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
        }
    }
}

#[async_trait]
impl OwnerResolver for StaticOwnerResolver {
    async fn resolve(&self, owner: &Owner) -> Result<ResolvedOwner, BoxError> {
        let endpoint = self.endpoints.get(&owner.id).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no endpoint configured for owner {}", owner.id),
            )
        })?;
        Ok(ResolvedOwner {
            owner: owner.clone(),
            socket_addr: endpoint.parse().ok(),
            endpoint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Assignment, AssignmentState, ShardMap, ShardRange};

    fn map(generation: u64) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(generation),
            64,
            vec![Assignment::new(
                Owner::new("metrics-0", 0),
                ShardRange::within(0, 64, 64).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn fake_store_watches_updates() {
        let store = FakeAssignmentStore::new(Some(map(1)));
        let mut watch = store.watch();
        store.publish(map(2)).await.unwrap();
        watch.changed().await.unwrap();
        assert_eq!(
            watch.borrow().as_ref().unwrap().generation,
            AssignmentGeneration::new(2)
        );
    }

    #[tokio::test]
    async fn static_resolver_is_deterministic() {
        let resolver =
            StaticOwnerResolver::new([("metrics-0".to_owned(), "127.0.0.1:9000".to_owned())]);
        let resolved = resolver.resolve(&Owner::new("metrics-0", 0)).await.unwrap();
        assert_eq!(resolved.endpoint, "127.0.0.1:9000");
        assert_eq!(
            resolved.socket_addr,
            Some("127.0.0.1:9000".parse().unwrap())
        );
    }
}
