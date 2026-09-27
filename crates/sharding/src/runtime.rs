use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, stream};
use tokio::{sync::watch, time};
use tokio_util::sync::CancellationToken;

use crate::{
    AssignmentGeneration, AssignmentState, AssignmentStore, LeaseBackend, ShardId, ShardLifecycle,
    ShardMap,
};

#[derive(Debug, Clone)]
pub struct OwnershipManagerConfig {
    pub renew_interval: Duration,
    pub lease_duration: Duration,
}

impl Default for OwnershipManagerConfig {
    fn default() -> Self {
        Self {
            renew_interval: Duration::from_secs(5),
            lease_duration: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnershipState {
    pub generation: Option<AssignmentGeneration>,
    pub shards: Vec<(ShardId, AssignmentState)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error("stale assignment generation {incoming}; current generation is {current}")]
    StaleGeneration {
        incoming: AssignmentGeneration,
        current: AssignmentGeneration,
    },
    #[error("assignment store failed: {0}")]
    Store(#[source] crate::BoxError),
    #[error("lease operation failed: {0}")]
    Lease(#[source] crate::BoxError),
    #[error("shard lifecycle failed: {0}")]
    Lifecycle(#[source] crate::BoxError),
}

pub struct OwnershipManager<S, L, R> {
    owner_id: String,
    config: OwnershipManagerConfig,
    store: Arc<S>,
    leases: Arc<L>,
    lifecycle: Arc<R>,
    held: HashMap<ShardId, (AssignmentGeneration, Instant)>,
    generation: Option<AssignmentGeneration>,
    latest: Option<ShardMap>,
    state_tx: watch::Sender<OwnershipState>,
}

impl<S, L, R> OwnershipManager<S, L, R>
where
    S: AssignmentStore,
    L: LeaseBackend,
    R: ShardLifecycle,
{
    pub fn new(
        owner_id: impl Into<String>,
        config: OwnershipManagerConfig,
        store: Arc<S>,
        leases: Arc<L>,
        lifecycle: Arc<R>,
    ) -> Self {
        let (state_tx, _) = watch::channel(OwnershipState::default());
        Self {
            owner_id: owner_id.into(),
            config,
            store,
            leases,
            lifecycle,
            held: HashMap::new(),
            generation: None,
            latest: None,
            state_tx,
        }
    }

    pub fn watch_state(&self) -> watch::Receiver<OwnershipState> {
        self.state_tx.subscribe()
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<(), ManagerError> {
        let mut assignments = self.store.watch();
        let mut lease_releases = self.leases.watch_releases();
        if let Some(initial) = self.store.load().await.map_err(ManagerError::Store)? {
            self.apply_assignment(initial).await?;
        }
        let mut renew = time::interval(self.config.renew_interval);
        renew.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        renew.tick().await;

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    self.shutdown().await?;
                    return Ok(());
                }
                changed = assignments.changed() => {
                    if changed.is_err() {
                        self.shutdown().await?;
                        return Ok(());
                    }
                    let next = assignments.borrow_and_update().clone();
                    if let Some(next) = next {
                        match self.apply_assignment(next).await {
                            Ok(()) => {}
                            Err(ManagerError::StaleGeneration { incoming, current }) => {
                                tracing::warn!(%incoming, %current, "ignored stale shard assignment");
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                _ = renew.tick() => self.renew().await?,
                changed = lease_releases.changed() => {
                    if changed.is_ok() {
                        lease_releases.borrow_and_update();
                        self.acquire_missing().await?;
                    }
                }
            }
        }
    }

    pub async fn apply_assignment(&mut self, map: ShardMap) -> Result<(), ManagerError> {
        if let Some(current) = self.generation
            && map.generation <= current
        {
            return Err(ManagerError::StaleGeneration {
                incoming: map.generation,
                current,
            });
        }
        self.latest = Some(map.clone());

        let desired = map
            .assignments_for(&self.owner_id)
            .filter(|assignment| {
                matches!(
                    assignment.state,
                    AssignmentState::Pending | AssignmentState::Active
                )
            })
            .flat_map(|assignment| assignment.range.start().get()..assignment.range.end().get())
            .map(ShardId::new)
            .collect::<Vec<_>>();

        let to_release = self
            .held
            .keys()
            .copied()
            .filter(|shard| !desired.contains(shard))
            .collect::<Vec<_>>();
        for shard in to_release {
            self.stop_shard(shard).await?;
        }

        for shard in desired {
            if let Some((old_generation, _)) = self.held.get(&shard).copied() {
                if old_generation != map.generation {
                    match self
                        .leases
                        .acquire(&self.owner_id, shard, map.generation)
                        .await
                    {
                        Ok(true) => {
                            self.held.insert(shard, (map.generation, Instant::now()));
                        }
                        Ok(false) => self.stop_shard(shard).await?,
                        Err(error) => {
                            tracing::warn!(
                                %shard,
                                generation = %map.generation,
                                %error,
                                "shard lease generation upgrade failed; retrying before expiry"
                            );
                        }
                    }
                }
                continue;
            }

            self.publish_state(map.generation, shard, AssignmentState::Pending);
            let acquired = match self
                .leases
                .acquire(&self.owner_id, shard, map.generation)
                .await
            {
                Ok(acquired) => acquired,
                Err(error) => {
                    tracing::warn!(
                        %shard,
                        generation = %map.generation,
                        %error,
                        "shard lease acquisition failed; retrying"
                    );
                    continue;
                }
            };
            if !acquired {
                continue;
            }
            if let Err(error) = self.lifecycle.open(shard, map.generation).await {
                self.leases
                    .release(&self.owner_id, shard, map.generation)
                    .await
                    .map_err(ManagerError::Lease)?;
                return Err(ManagerError::Lifecycle(error));
            }
            self.held.insert(shard, (map.generation, Instant::now()));
            self.publish_state(map.generation, shard, AssignmentState::Active);
        }

        self.generation = Some(map.generation);
        self.publish_snapshot();
        Ok(())
    }

    async fn renew(&mut self) -> Result<(), ManagerError> {
        let latest_generation = self.latest.as_ref().map(|map| map.generation);
        let held = self
            .held
            .iter()
            .map(|(shard, (generation, confirmed))| (*shard, *generation, *confirmed))
            .collect::<Vec<_>>();
        let renewals = stream::iter(held.into_iter().map(|(shard, generation, confirmed)| {
            let leases = Arc::clone(&self.leases);
            let owner_id = self.owner_id.clone();
            let target_generation = latest_generation
                .filter(|latest| *latest > generation)
                .unwrap_or(generation);
            async move {
                let result = if target_generation == generation {
                    leases.renew(&owner_id, shard, generation).await
                } else {
                    leases.acquire(&owner_id, shard, target_generation).await
                };
                (shard, target_generation, confirmed, result)
            }
        }))
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
        for (shard, generation, confirmed, result) in renewals {
            match result {
                Ok(true) => {
                    self.held.insert(shard, (generation, Instant::now()));
                }
                Ok(false) => {
                    tracing::warn!(%shard, %generation, "shard lease lost; draining local resources");
                    self.stop_shard(shard).await?;
                }
                Err(error) if confirmed.elapsed() < self.config.lease_duration => {
                    tracing::warn!(%shard, %generation, %error, "shard lease renewal failed; retrying before expiry");
                }
                Err(error) => {
                    tracing::error!(%shard, %generation, %error, "shard lease could not be confirmed before expiry; draining");
                    self.stop_shard(shard).await?;
                }
            }
        }
        self.acquire_missing().await
    }

    async fn acquire_missing(&mut self) -> Result<(), ManagerError> {
        let missing = self
            .latest
            .as_ref()
            .into_iter()
            .flat_map(|map| {
                map.assignments_for(&self.owner_id).flat_map(move |item| {
                    (item.range.start().get()..item.range.end().get())
                        .map(move |shard| (map, item.state, ShardId::new(shard)))
                })
            })
            .filter(|(_, state, shard)| {
                matches!(state, AssignmentState::Pending | AssignmentState::Active)
                    && !self.held.contains_key(shard)
            })
            .map(|(map, _, shard)| (map.generation, shard))
            .collect::<Vec<_>>();
        for (generation, shard) in missing {
            self.publish_state(generation, shard, AssignmentState::Pending);
            let acquired = match self.leases.acquire(&self.owner_id, shard, generation).await {
                Ok(acquired) => acquired,
                Err(error) => {
                    tracing::warn!(%shard, %generation, %error, "shard lease acquisition failed; retrying");
                    continue;
                }
            };
            if !acquired {
                continue;
            }
            if let Err(error) = self.lifecycle.open(shard, generation).await {
                self.leases
                    .release(&self.owner_id, shard, generation)
                    .await
                    .map_err(ManagerError::Lease)?;
                return Err(ManagerError::Lifecycle(error));
            }
            self.held.insert(shard, (generation, Instant::now()));
            self.publish_state(generation, shard, AssignmentState::Active);
        }
        Ok(())
    }

    async fn stop_shard(&mut self, shard: ShardId) -> Result<(), ManagerError> {
        let Some((generation, _)) = self.held.get(&shard).copied() else {
            return Ok(());
        };
        self.publish_state(generation, shard, AssignmentState::Draining);
        self.lifecycle
            .drain(shard)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.lifecycle
            .flush(shard)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.lifecycle
            .close(shard)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.leases
            .release(&self.owner_id, shard, generation)
            .await
            .map_err(ManagerError::Lease)?;
        self.held.remove(&shard);
        self.publish_state(generation, shard, AssignmentState::Released);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ManagerError> {
        let shards = self.held.keys().copied().collect::<Vec<_>>();
        for shard in shards {
            self.stop_shard(shard).await?;
        }
        Ok(())
    }

    fn publish_state(
        &self,
        generation: AssignmentGeneration,
        shard: ShardId,
        state: AssignmentState,
    ) {
        let mut snapshot = self.state_tx.borrow().clone();
        snapshot.generation = Some(generation);
        snapshot.shards.retain(|(current, _)| *current != shard);
        snapshot.shards.push((shard, state));
        snapshot.shards.sort_by_key(|(shard, _)| *shard);
        self.state_tx.send_replace(snapshot);
    }

    fn publish_snapshot(&self) {
        let mut snapshot = self.state_tx.borrow().clone();
        snapshot.generation = self.generation;
        snapshot.shards.retain(|(shard, state)| {
            self.held.contains_key(shard) || *state == AssignmentState::Released
        });
        self.state_tx.send_replace(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::{
        Assignment, AssignmentGeneration, AssignmentState, BoxError, FakeAssignmentStore,
        FakeLeaseBackend, Owner, ShardMap, ShardRange,
    };

    #[derive(Default)]
    struct RecordingLifecycle {
        events: Mutex<Vec<&'static str>>,
    }

    impl RecordingLifecycle {
        fn events(&self) -> Vec<&'static str> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ShardLifecycle for RecordingLifecycle {
        async fn open(
            &self,
            _shard: ShardId,
            _generation: AssignmentGeneration,
        ) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("open");
            Ok(())
        }

        async fn drain(&self, _shard: ShardId) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("drain");
            Ok(())
        }

        async fn flush(&self, _shard: ShardId) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("flush");
            Ok(())
        }

        async fn close(&self, _shard: ShardId) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("close");
            Ok(())
        }
    }

    #[derive(Default)]
    struct FlakyLeaseBackend {
        inner: FakeLeaseBackend,
        fail_next_renewal: AtomicBool,
    }

    #[async_trait]
    impl LeaseBackend for FlakyLeaseBackend {
        fn watch_releases(&self) -> watch::Receiver<u64> {
            self.inner.watch_releases()
        }

        async fn acquire(
            &self,
            owner_id: &str,
            shard: ShardId,
            generation: AssignmentGeneration,
        ) -> Result<bool, BoxError> {
            self.inner.acquire(owner_id, shard, generation).await
        }

        async fn renew(
            &self,
            owner_id: &str,
            shard: ShardId,
            generation: AssignmentGeneration,
        ) -> Result<bool, BoxError> {
            if self.fail_next_renewal.swap(false, Ordering::AcqRel) {
                return Err(std::io::Error::other("transient renewal failure").into());
            }
            self.inner.renew(owner_id, shard, generation).await
        }

        async fn release(
            &self,
            owner_id: &str,
            shard: ShardId,
            generation: AssignmentGeneration,
        ) -> Result<(), BoxError> {
            self.inner.release(owner_id, shard, generation).await
        }
    }

    fn map(generation: u64, owner: &str, state: AssignmentState) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(generation),
            1,
            vec![Assignment::new(
                Owner::new(owner, 0),
                ShardRange::within(0, 1, 1).unwrap(),
                state,
            )],
        )
        .unwrap()
    }

    fn manager(
        initial: Option<ShardMap>,
    ) -> (
        OwnershipManager<FakeAssignmentStore, FakeLeaseBackend, RecordingLifecycle>,
        Arc<FakeLeaseBackend>,
        Arc<RecordingLifecycle>,
    ) {
        let store = Arc::new(FakeAssignmentStore::new(initial));
        let leases = Arc::new(FakeLeaseBackend::default());
        let lifecycle = Arc::new(RecordingLifecycle::default());
        (
            OwnershipManager::new(
                "meter-0",
                OwnershipManagerConfig {
                    renew_interval: Duration::from_millis(10),
                    lease_duration: Duration::from_millis(30),
                },
                store,
                leases.clone(),
                lifecycle.clone(),
            ),
            leases,
            lifecycle,
        )
    }

    #[tokio::test]
    async fn opens_only_after_acquiring_and_closes_before_release() {
        let (mut manager, leases, lifecycle) = manager(None);
        let shard = ShardId::new(0);
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Pending))
            .await
            .unwrap();
        assert!(leases.holder(shard).is_some());
        assert_eq!(lifecycle.events(), vec!["open"]);

        manager
            .apply_assignment(map(2, "meter-0", AssignmentState::Draining))
            .await
            .unwrap();
        assert_eq!(lifecycle.events(), vec!["open", "drain", "flush", "close"]);
        assert!(leases.holder(shard).is_none());
    }

    #[tokio::test]
    async fn rejects_stale_generation() {
        let (mut manager, _, lifecycle) = manager(None);
        manager
            .apply_assignment(map(2, "meter-0", AssignmentState::Active))
            .await
            .unwrap();
        assert!(matches!(
            manager
                .apply_assignment(map(1, "meter-0", AssignmentState::Active))
                .await,
            Err(ManagerError::StaleGeneration { .. })
        ));
        assert_eq!(lifecycle.events(), vec!["open"]);
    }

    #[tokio::test]
    async fn upgrades_retained_shard_leases_without_reopening_resources() {
        let (mut manager, leases, lifecycle) = manager(None);
        let shard = ShardId::new(0);
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Active))
            .await
            .unwrap();
        manager
            .apply_assignment(map(2, "meter-0", AssignmentState::Active))
            .await
            .unwrap();

        assert_eq!(
            leases.holder(shard),
            Some(("meter-0".to_owned(), AssignmentGeneration::new(2)))
        );
        assert_eq!(lifecycle.events(), vec!["open"]);
    }

    #[tokio::test]
    async fn retries_transient_renewal_errors_before_the_lease_deadline() {
        let store = Arc::new(FakeAssignmentStore::new(None));
        let leases = Arc::new(FlakyLeaseBackend::default());
        let lifecycle = Arc::new(RecordingLifecycle::default());
        let mut manager = OwnershipManager::new(
            "meter-0",
            OwnershipManagerConfig {
                renew_interval: Duration::from_millis(10),
                lease_duration: Duration::from_secs(1),
            },
            store,
            Arc::clone(&leases),
            Arc::clone(&lifecycle),
        );
        let shard = ShardId::new(0);
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Active))
            .await
            .unwrap();

        leases.fail_next_renewal.store(true, Ordering::Release);
        manager.renew().await.unwrap();
        assert_eq!(lifecycle.events(), vec!["open"]);
        assert!(manager.held.contains_key(&shard));

        manager.renew().await.unwrap();
        assert!(leases.inner.events().iter().any(|event| {
            matches!(
                event,
                crate::LeaseEvent::Renewed {
                    shard: renewed,
                    ..
                } if *renewed == shard
            )
        }));
    }

    #[tokio::test]
    async fn lease_loss_drains_and_closes_resources() {
        let initial = map(1, "meter-0", AssignmentState::Active);
        let shard = ShardId::new(0);
        let (manager, leases, lifecycle) = manager(Some(initial));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(manager.run(cancel.clone()));

        time::sleep(Duration::from_millis(5)).await;
        leases.lose(shard);
        time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(lifecycle.events(), vec!["open", "drain", "flush", "close"]);
    }

    #[tokio::test]
    async fn cancellation_drains_owned_ranges() {
        let initial = map(1, "meter-0", AssignmentState::Active);
        let (manager, _, lifecycle) = manager(Some(initial));
        let cancel = CancellationToken::new();
        cancel.cancel();
        manager.run(cancel).await.unwrap();
        assert_eq!(lifecycle.events(), vec!["open", "drain", "flush", "close"]);
    }

    #[tokio::test]
    async fn manager_applies_fake_watch_updates() {
        let store = Arc::new(FakeAssignmentStore::new(Some(map(
            1,
            "meter-1",
            AssignmentState::Active,
        ))));
        let leases = Arc::new(FakeLeaseBackend::default());
        let lifecycle = Arc::new(RecordingLifecycle::default());
        let manager = OwnershipManager::new(
            "meter-0",
            OwnershipManagerConfig {
                renew_interval: Duration::from_secs(60),
                lease_duration: Duration::from_secs(180),
            },
            store.clone(),
            leases,
            lifecycle.clone(),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(manager.run(cancel.clone()));

        tokio::task::yield_now().await;
        store
            .publish(map(2, "meter-0", AssignmentState::Active))
            .await
            .unwrap();
        time::timeout(Duration::from_secs(1), async {
            while lifecycle.events().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(lifecycle.events(), vec!["open", "drain", "flush", "close"]);
    }

    #[tokio::test]
    async fn retries_handoff_acquisition_after_previous_owner_releases() {
        let (mut manager, leases, lifecycle) = manager(None);
        let shard = ShardId::new(0);
        let generation = AssignmentGeneration::new(1);
        assert!(
            leases
                .acquire("previous-owner", shard, generation)
                .await
                .unwrap()
        );
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Active))
            .await
            .unwrap();
        assert!(lifecycle.events().is_empty());

        leases
            .release("previous-owner", shard, generation)
            .await
            .unwrap();
        manager.renew().await.unwrap();
        assert_eq!(lifecycle.events(), vec!["open"]);
        assert_eq!(
            leases.holder(shard),
            Some(("meter-0".to_owned(), generation))
        );
    }

    #[tokio::test]
    async fn lease_release_watch_wakes_waiting_owner_immediately() {
        let generation = AssignmentGeneration::new(1);
        let shard = ShardId::new(0);
        let store = Arc::new(FakeAssignmentStore::new(Some(map(
            1,
            "meter-0",
            AssignmentState::Active,
        ))));
        let leases = Arc::new(FakeLeaseBackend::default());
        assert!(
            leases
                .acquire("previous-owner", shard, generation)
                .await
                .unwrap()
        );
        let lifecycle = Arc::new(RecordingLifecycle::default());
        let manager = OwnershipManager::new(
            "meter-0",
            OwnershipManagerConfig {
                renew_interval: Duration::from_secs(60),
                lease_duration: Duration::from_secs(180),
            },
            store,
            Arc::clone(&leases),
            Arc::clone(&lifecycle),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(manager.run(cancel.clone()));
        tokio::task::yield_now().await;
        assert!(lifecycle.events().is_empty());

        leases
            .release("previous-owner", shard, generation)
            .await
            .unwrap();
        time::timeout(Duration::from_secs(1), async {
            while lifecycle.events().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(lifecycle.events(), vec!["open"]);

        cancel.cancel();
        task.await.unwrap().unwrap();
    }
}
