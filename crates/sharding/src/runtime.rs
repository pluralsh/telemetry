use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{sync::watch, time};
use tokio_util::sync::CancellationToken;

use crate::{
    AssignmentGeneration, AssignmentState, AssignmentStore, LeaseBackend, ShardLifecycle, ShardMap,
    ShardRange,
};

#[derive(Debug, Clone)]
pub struct OwnershipManagerConfig {
    pub renew_interval: Duration,
}

impl Default for OwnershipManagerConfig {
    fn default() -> Self {
        Self {
            renew_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnershipState {
    pub generation: Option<AssignmentGeneration>,
    pub ranges: Vec<(ShardRange, AssignmentState)>,
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
    held: HashMap<ShardRange, AssignmentGeneration>,
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
            .map(|assignment| assignment.range)
            .collect::<Vec<_>>();

        let to_release = self
            .held
            .keys()
            .copied()
            .filter(|range| !desired.contains(range))
            .collect::<Vec<_>>();
        for range in to_release {
            self.stop_range(range).await?;
        }

        for range in desired {
            if let Some(old_generation) = self.held.get(&range).copied() {
                if old_generation != map.generation {
                    let acquired = self
                        .leases
                        .acquire(&self.owner_id, range, map.generation)
                        .await
                        .map_err(ManagerError::Lease)?;
                    if acquired {
                        self.held.insert(range, map.generation);
                    } else {
                        self.stop_range(range).await?;
                    }
                }
                continue;
            }

            self.publish_state(map.generation, range, AssignmentState::Pending);
            let acquired = self
                .leases
                .acquire(&self.owner_id, range, map.generation)
                .await
                .map_err(ManagerError::Lease)?;
            if !acquired {
                continue;
            }
            if let Err(error) = self.lifecycle.open(range, map.generation).await {
                self.leases
                    .release(&self.owner_id, range, map.generation)
                    .await
                    .map_err(ManagerError::Lease)?;
                return Err(ManagerError::Lifecycle(error));
            }
            self.held.insert(range, map.generation);
            self.publish_state(map.generation, range, AssignmentState::Active);
        }

        self.generation = Some(map.generation);
        self.publish_snapshot();
        Ok(())
    }

    async fn renew(&mut self) -> Result<(), ManagerError> {
        let held = self
            .held
            .iter()
            .map(|(range, generation)| (*range, *generation))
            .collect::<Vec<_>>();
        for (range, generation) in held {
            let owned = self
                .leases
                .renew(&self.owner_id, range, generation)
                .await
                .map_err(ManagerError::Lease)?;
            if !owned {
                tracing::warn!(%range, %generation, "shard lease lost; draining local resources");
                self.stop_range(range).await?;
            }
        }
        let missing = self
            .latest
            .as_ref()
            .into_iter()
            .flat_map(|map| {
                map.assignments_for(&self.owner_id)
                    .map(move |item| (map, item))
            })
            .filter(|(_, assignment)| {
                matches!(
                    assignment.state,
                    AssignmentState::Pending | AssignmentState::Active
                ) && !self.held.contains_key(&assignment.range)
            })
            .map(|(map, assignment)| (map.generation, assignment.range))
            .collect::<Vec<_>>();
        for (generation, range) in missing {
            self.publish_state(generation, range, AssignmentState::Pending);
            let acquired = self
                .leases
                .acquire(&self.owner_id, range, generation)
                .await
                .map_err(ManagerError::Lease)?;
            if !acquired {
                continue;
            }
            if let Err(error) = self.lifecycle.open(range, generation).await {
                self.leases
                    .release(&self.owner_id, range, generation)
                    .await
                    .map_err(ManagerError::Lease)?;
                return Err(ManagerError::Lifecycle(error));
            }
            self.held.insert(range, generation);
            self.publish_state(generation, range, AssignmentState::Active);
        }
        Ok(())
    }

    async fn stop_range(&mut self, range: ShardRange) -> Result<(), ManagerError> {
        let Some(generation) = self.held.get(&range).copied() else {
            return Ok(());
        };
        self.publish_state(generation, range, AssignmentState::Draining);
        self.lifecycle
            .drain(range)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.lifecycle
            .flush(range)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.lifecycle
            .close(range)
            .await
            .map_err(ManagerError::Lifecycle)?;
        self.leases
            .release(&self.owner_id, range, generation)
            .await
            .map_err(ManagerError::Lease)?;
        self.held.remove(&range);
        self.publish_state(generation, range, AssignmentState::Released);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ManagerError> {
        let ranges = self.held.keys().copied().collect::<Vec<_>>();
        for range in ranges {
            self.stop_range(range).await?;
        }
        Ok(())
    }

    fn publish_state(
        &self,
        generation: AssignmentGeneration,
        range: ShardRange,
        state: AssignmentState,
    ) {
        let mut snapshot = self.state_tx.borrow().clone();
        snapshot.generation = Some(generation);
        snapshot.ranges.retain(|(current, _)| *current != range);
        snapshot.ranges.push((range, state));
        snapshot.ranges.sort_by_key(|(range, _)| range.start());
        self.state_tx.send_replace(snapshot);
    }

    fn publish_snapshot(&self) {
        let mut snapshot = self.state_tx.borrow().clone();
        snapshot.generation = self.generation;
        snapshot.ranges.retain(|(range, state)| {
            self.held.contains_key(range) || *state == AssignmentState::Released
        });
        self.state_tx.send_replace(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::{
        Assignment, AssignmentGeneration, AssignmentState, BoxError, FakeAssignmentStore,
        FakeLeaseBackend, Owner, ShardMap,
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
            _range: ShardRange,
            _generation: AssignmentGeneration,
        ) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("open");
            Ok(())
        }

        async fn drain(&self, _range: ShardRange) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("drain");
            Ok(())
        }

        async fn flush(&self, _range: ShardRange) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("flush");
            Ok(())
        }

        async fn close(&self, _range: ShardRange) -> Result<(), BoxError> {
            self.events.lock().unwrap().push("close");
            Ok(())
        }
    }

    fn map(generation: u64, owner: &str, state: AssignmentState) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(generation),
            64,
            vec![Assignment::new(
                Owner::new(owner, 0),
                ShardRange::within(0, 64, 64).unwrap(),
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
        let range = ShardRange::within(0, 64, 64).unwrap();
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Pending))
            .await
            .unwrap();
        assert!(leases.holder(range).is_some());
        assert_eq!(lifecycle.events(), vec!["open"]);

        manager
            .apply_assignment(map(2, "meter-0", AssignmentState::Draining))
            .await
            .unwrap();
        assert_eq!(lifecycle.events(), vec!["open", "drain", "flush", "close"]);
        assert!(leases.holder(range).is_none());
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
    async fn lease_loss_drains_and_closes_resources() {
        let initial = map(1, "meter-0", AssignmentState::Active);
        let range = initial.assignments[0].range;
        let (manager, leases, lifecycle) = manager(Some(initial));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(manager.run(cancel.clone()));

        time::sleep(Duration::from_millis(5)).await;
        leases.lose(range);
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
        let range = ShardRange::within(0, 64, 64).unwrap();
        let generation = AssignmentGeneration::new(1);
        assert!(
            leases
                .acquire("previous-owner", range, generation)
                .await
                .unwrap()
        );
        manager
            .apply_assignment(map(1, "meter-0", AssignmentState::Active))
            .await
            .unwrap();
        assert!(lifecycle.events().is_empty());

        leases
            .release("previous-owner", range, generation)
            .await
            .unwrap();
        manager.renew().await.unwrap();
        assert_eq!(lifecycle.events(), vec!["open"]);
        assert_eq!(
            leases.holder(range),
            Some(("meter-0".to_owned(), generation))
        );
    }
}
