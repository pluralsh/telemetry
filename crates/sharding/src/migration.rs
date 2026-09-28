use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{
    Assignment, AssignmentState, AssignmentStore, MigrationExecutionError, MigrationPhase, Owner,
    RoutingError, ShardId, ShardMap, ShardMigration, ShardMigrationExecutor, ShardRange,
    ShardSplit,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("migration requires scale-up from {current} to a larger shard count, got {desired}")]
    NotScaleUp { current: u32, desired: u32 },
    #[error("desired shard count {desired} exceeds available writer count {owners}")]
    InsufficientOwners { desired: u32, owners: usize },
    #[error("no new writer is available for the next shard")]
    NoTargetOwner,
    #[error("source shard {0} has no active owner")]
    MissingSourceOwner(ShardId),
    #[error("ShardMap has no active migration")]
    NoMigration,
    #[error("expected migration phase {expected:?}, got {actual:?}")]
    UnexpectedPhase {
        expected: MigrationPhase,
        actual: MigrationPhase,
    },
    #[error(transparent)]
    Routing(#[from] RoutingError),
    #[error(transparent)]
    Model(#[from] crate::ModelError),
}

/// Runs the source-writer side of `Preparing` or `Cloning` once.
///
/// The assignment store's compare-and-swap publication prevents a stale
/// worker from acknowledging a superseded migration.
pub async fn prepare_owned_split(
    store: &dyn AssignmentStore,
    executor: &dyn ShardMigrationExecutor,
    owner_id: &str,
) -> Result<bool, MigrationExecutionError> {
    let Some(current) = store
        .load()
        .await
        .map_err(MigrationExecutionError::Retryable)?
    else {
        return Ok(false);
    };
    let Some(migration) = current.migration.as_ref() else {
        return Ok(false);
    };
    if !matches!(
        migration.phase,
        MigrationPhase::Preparing | MigrationPhase::Cloning
    ) || migration.split.source_owner.id != owner_id
    {
        return Ok(false);
    }

    match migration.phase {
        MigrationPhase::Preparing => executor.preflight_split(&migration.split).await?,
        MigrationPhase::Cloning => executor.clone_split(&migration.split).await?,
        _ => unreachable!("phase was checked above"),
    }

    // Reload after the potentially long clone and acknowledge only the exact
    // migration that was prepared.
    let Some(latest) = store
        .load()
        .await
        .map_err(MigrationExecutionError::Retryable)?
    else {
        return Ok(false);
    };
    if latest.generation != current.generation || latest.migration != current.migration {
        return Ok(false);
    }
    let next = match migration.phase {
        MigrationPhase::Preparing => {
            mark_prepared(&latest).map_err(MigrationExecutionError::retryable)?
        }
        MigrationPhase::Cloning => {
            mark_ready(&latest).map_err(MigrationExecutionError::retryable)?
        }
        _ => unreachable!("phase was checked above"),
    };
    store
        .publish(next)
        .await
        .map_err(MigrationExecutionError::Retryable)?;
    Ok(true)
}

/// Runs the source-side migration executor whenever the durable assignment
/// changes. Retryable storage errors are retried without changing migration
/// state; fatal preflight/clone conflicts are persisted as `Failed`.
pub async fn run_migration_worker<S, E>(
    store: Arc<S>,
    executor: Arc<E>,
    owner_id: String,
    cancel: CancellationToken,
) where
    S: AssignmentStore + 'static,
    E: ShardMigrationExecutor + 'static,
{
    let mut updates = store.watch();
    loop {
        match prepare_owned_split(store.as_ref(), executor.as_ref(), &owner_id).await {
            Ok(_) => {}
            Err(MigrationExecutionError::Fatal(error)) => {
                if let Err(publish_error) =
                    persist_owned_failure(store.as_ref(), &owner_id, error.clone()).await
                {
                    tracing::warn!(%publish_error, %error, "failed to persist fatal shard migration error");
                }
            }
            Err(MigrationExecutionError::Retryable(error)) => {
                tracing::warn!(%error, "shard migration execution failed; retrying");
            }
        }

        tokio::select! {
            () = cancel.cancelled() => return,
            changed = updates.changed() => {
                if changed.is_err() {
                    return;
                }
                updates.borrow_and_update();
            }
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }
}

async fn persist_owned_failure(
    store: &dyn AssignmentStore,
    owner_id: &str,
    error: String,
) -> Result<(), crate::BoxError> {
    let Some(current) = store.load().await? else {
        return Ok(());
    };
    let Some(migration) = current.migration.as_ref() else {
        return Ok(());
    };
    if migration.split.source_owner.id != owner_id
        || !matches!(
            migration.phase,
            MigrationPhase::Preparing | MigrationPhase::Cloning
        )
    {
        return Ok(());
    }
    store.publish(fail(&current, error)?).await
}

/// Computes the coordinator-owned transition for the current durable state.
///
/// `Preparing` is a non-mutating preflight. The clone is created only after
/// `Draining` has closed the source and released its lease.
pub fn coordinator_step(
    current: &ShardMap,
    desired_shard_count: u32,
    owners: &[Owner],
    source_lease_released: bool,
) -> Result<Option<ShardMap>, MigrationError> {
    let Some(migration) = current.migration.as_ref() else {
        return if desired_shard_count > current.virtual_shards {
            plan_next_split(current, desired_shard_count, owners).map(Some)
        } else {
            Ok(None)
        };
    };

    match migration.phase {
        MigrationPhase::Preparing | MigrationPhase::Cloning | MigrationPhase::Failed => Ok(None),
        MigrationPhase::Prepared => begin_drain(current).map(Some),
        MigrationPhase::Draining if source_lease_released => begin_clone(current).map(Some),
        MigrationPhase::Draining => Ok(None),
        MigrationPhase::Ready => cutover(current, owners).map(Some),
        MigrationPhase::Completing => complete(current).map(Some),
    }
}

pub fn plan_next_split(
    current: &ShardMap,
    desired_shard_count: u32,
    owners: &[Owner],
) -> Result<ShardMap, MigrationError> {
    if desired_shard_count <= current.virtual_shards {
        return Err(MigrationError::NotScaleUp {
            current: current.virtual_shards,
            desired: desired_shard_count,
        });
    }
    if owners.len() < desired_shard_count as usize {
        return Err(MigrationError::InsufficientOwners {
            desired: desired_shard_count,
            owners: owners.len(),
        });
    }

    let target_routing = current.routing.grow_to(current.virtual_shards + 1)?;
    let target_assignment = target_routing
        .assignments
        .iter()
        .find(|candidate| {
            !current
                .routing
                .assignments
                .iter()
                .any(|existing| existing.shard == candidate.shard)
        })
        .expect("grow_to adds exactly one shard");
    let source_shard = current.routing.route(target_assignment.range.start());
    let source_owner = current
        .owner_of(source_shard)
        .cloned()
        .ok_or(MigrationError::MissingSourceOwner(source_shard))?;
    let current_owner_ids = current
        .assignments
        .iter()
        .map(|assignment| assignment.owner.id.as_str())
        .collect::<HashSet<_>>();
    let mut available = owners
        .iter()
        .filter(|owner| !current_owner_ids.contains(owner.id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    available.sort_by_key(|owner| owner.ordinal);
    let target_owner = available
        .into_iter()
        .next()
        .ok_or(MigrationError::NoTargetOwner)?;
    let migration = ShardMigration {
        phase: MigrationPhase::Preparing,
        desired_shard_count,
        split: ShardSplit {
            source_shard,
            target_shard: target_assignment.shard,
            moved_range: target_assignment.range,
            source_owner,
            target_owner,
        },
        target_routing,
        error: None,
    };
    Ok(ShardMap::with_routing_and_migration(
        current.generation.next(),
        current.virtual_shards,
        current.routing.clone(),
        current.assignments.clone(),
        Some(migration),
    )?)
}

pub fn mark_prepared(current: &ShardMap) -> Result<ShardMap, MigrationError> {
    transition(current, MigrationPhase::Preparing, |migration| {
        migration.phase = MigrationPhase::Prepared;
    })
}

pub fn begin_drain(current: &ShardMap) -> Result<ShardMap, MigrationError> {
    let migration = migration_in_phase(current, MigrationPhase::Prepared)?;
    let source = migration.split.source_shard;
    let mut assignments = Vec::with_capacity(current.virtual_shards as usize);
    for shard in 0..current.virtual_shards {
        let shard = ShardId::new(shard);
        let owner = current
            .owner_of(shard)
            .cloned()
            .ok_or(MigrationError::MissingSourceOwner(shard))?;
        assignments.push(Assignment::new(
            owner,
            ShardRange::within(shard.get(), shard.get() + 1, current.virtual_shards)?,
            if shard == source {
                AssignmentState::Draining
            } else {
                AssignmentState::Active
            },
        ));
    }
    let mut migration = migration.clone();
    migration.phase = MigrationPhase::Draining;
    Ok(ShardMap::with_routing_and_migration(
        current.generation.next(),
        current.virtual_shards,
        current.routing.clone(),
        assignments,
        Some(migration),
    )?)
}

pub fn begin_clone(current: &ShardMap) -> Result<ShardMap, MigrationError> {
    transition(current, MigrationPhase::Draining, |migration| {
        migration.phase = MigrationPhase::Cloning;
    })
}

pub fn mark_ready(current: &ShardMap) -> Result<ShardMap, MigrationError> {
    transition(current, MigrationPhase::Cloning, |migration| {
        migration.phase = MigrationPhase::Ready;
    })
}

pub fn cutover(current: &ShardMap, owners: &[Owner]) -> Result<ShardMap, MigrationError> {
    let migration = migration_in_phase(current, MigrationPhase::Ready)?;
    let next_count = current.virtual_shards + 1;
    let mut owners = owners.to_vec();
    owners.sort_by_key(|owner| owner.ordinal);
    if owners.len() < next_count as usize {
        return Err(MigrationError::InsufficientOwners {
            desired: next_count,
            owners: owners.len(),
        });
    }
    let assignments = owners
        .into_iter()
        .take(next_count as usize)
        .enumerate()
        .map(|(shard, owner)| {
            Ok(Assignment::new(
                owner,
                ShardRange::within(shard as u32, shard as u32 + 1, next_count)?,
                AssignmentState::Active,
            ))
        })
        .collect::<Result<Vec<_>, crate::ModelError>>()?;
    let mut migration = migration.clone();
    migration.phase = MigrationPhase::Completing;
    Ok(ShardMap::with_routing_and_migration(
        current.generation.next(),
        next_count,
        migration.target_routing.clone(),
        assignments,
        Some(migration),
    )?)
}

pub fn complete(current: &ShardMap) -> Result<ShardMap, MigrationError> {
    migration_in_phase(current, MigrationPhase::Completing)?;
    Ok(ShardMap::with_routing(
        current.generation.next(),
        current.virtual_shards,
        current.routing.clone(),
        current.assignments.clone(),
    )?)
}

pub fn fail(current: &ShardMap, error: impl Into<String>) -> Result<ShardMap, MigrationError> {
    let mut migration = current
        .migration
        .clone()
        .ok_or(MigrationError::NoMigration)?;
    migration.phase = MigrationPhase::Failed;
    migration.error = Some(error.into());
    Ok(ShardMap::with_routing_and_migration(
        current.generation.next(),
        current.virtual_shards,
        current.routing.clone(),
        current.assignments.clone(),
        Some(migration),
    )?)
}

fn transition(
    current: &ShardMap,
    expected: MigrationPhase,
    update: impl FnOnce(&mut ShardMigration),
) -> Result<ShardMap, MigrationError> {
    let mut migration = migration_in_phase(current, expected)?.clone();
    update(&mut migration);
    Ok(ShardMap::with_routing_and_migration(
        current.generation.next(),
        current.virtual_shards,
        current.routing.clone(),
        current.assignments.clone(),
        Some(migration),
    )?)
}

fn migration_in_phase(
    current: &ShardMap,
    expected: MigrationPhase,
) -> Result<&ShardMigration, MigrationError> {
    let migration = current
        .migration
        .as_ref()
        .ok_or(MigrationError::NoMigration)?;
    if migration.phase != expected {
        return Err(MigrationError::UnexpectedPhase {
            expected,
            actual: migration.phase,
        });
    }
    Ok(migration)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::{AssignmentGeneration, FakeAssignmentStore, HashRangeMap};

    #[derive(Default)]
    struct RecordingExecutor {
        preflights: AtomicUsize,
        clones: AtomicUsize,
    }

    #[async_trait]
    impl ShardMigrationExecutor for RecordingExecutor {
        async fn preflight_split(
            &self,
            _split: &ShardSplit,
        ) -> Result<(), MigrationExecutionError> {
            self.preflights.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn clone_split(&self, _split: &ShardSplit) -> Result<(), MigrationExecutionError> {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn owners(count: u32) -> Vec<Owner> {
        (0..count)
            .map(|ordinal| Owner::new(format!("writer-{ordinal}"), ordinal))
            .collect()
    }

    fn initial(count: u32) -> ShardMap {
        let owners = owners(count);
        ShardMap::new(
            AssignmentGeneration::new(1),
            count,
            owners
                .into_iter()
                .enumerate()
                .map(|(shard, owner)| {
                    Assignment::new(
                        owner,
                        ShardRange::within(shard as u32, shard as u32 + 1, count).unwrap(),
                        AssignmentState::Active,
                    )
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn scale_up_runs_prepare_drain_cutover_complete() {
        let owners = owners(3);
        let original = initial(2);
        let planned = plan_next_split(&original, 3, &owners).unwrap();
        let migration = planned.migration.as_ref().unwrap();
        assert_eq!(migration.phase, MigrationPhase::Preparing);
        assert_eq!(migration.split.target_shard, ShardId::new(2));
        assert_eq!(planned.routing, original.routing);
        assert_eq!(planned.virtual_shards, 2);

        let prepared = mark_prepared(&planned).unwrap();
        let draining = begin_drain(&prepared).unwrap();
        assert_eq!(
            draining
                .assignments
                .iter()
                .find(|assignment| assignment.range.contains(migration.split.source_shard))
                .unwrap()
                .state,
            AssignmentState::Draining
        );

        let cloning = begin_clone(&draining).unwrap();
        let ready = mark_ready(&cloning).unwrap();
        let cutover = cutover(&ready, &owners).unwrap();
        assert_eq!(cutover.virtual_shards, 3);
        assert_eq!(cutover.routing.assignments.len(), 3);
        assert_eq!(
            cutover.migration.as_ref().unwrap().phase,
            MigrationPhase::Completing
        );
        let complete = complete(&cutover).unwrap();
        assert!(complete.migration.is_none());
        assert_eq!(complete.generation.get(), 8);
    }

    #[test]
    fn plans_one_split_when_replica_count_jumps_multiple_steps() {
        let map = plan_next_split(&initial(1), 3, &owners(3)).unwrap();
        assert_eq!(map.virtual_shards, 1);
        assert_eq!(
            map.migration.as_ref().unwrap().target_routing.assignments,
            HashRangeMap::bootstrap(2).unwrap().assignments
        );
        assert_eq!(map.migration.as_ref().unwrap().desired_shard_count, 3);
    }

    #[test]
    fn coordinator_grows_multiple_steps_one_cutover_at_a_time() {
        let owners = owners(3);
        let mut current = initial(1);

        for expected_count in 2..=3 {
            let expected_routing = current.routing.grow_to(expected_count).unwrap();
            let planned = coordinator_step(&current, 3, &owners, false)
                .unwrap()
                .unwrap();
            let migration = planned.migration.as_ref().unwrap();
            assert_eq!(planned.virtual_shards, expected_count - 1);
            assert_eq!(migration.desired_shard_count, 3);
            assert_eq!(
                migration.target_routing.assignments,
                expected_routing.assignments
            );

            let prepared = mark_prepared(&planned).unwrap();
            let draining = coordinator_step(&prepared, 3, &owners, false)
                .unwrap()
                .unwrap();
            assert_eq!(
                coordinator_step(&draining, 3, &owners, false).unwrap(),
                None,
                "cutover must wait for the source lease at step {expected_count}"
            );
            let cloning = coordinator_step(&draining, 3, &owners, true)
                .unwrap()
                .unwrap();
            assert_eq!(coordinator_step(&cloning, 3, &owners, true).unwrap(), None);

            let ready = mark_ready(&cloning).unwrap();
            let completing = coordinator_step(&ready, 3, &owners, false)
                .unwrap()
                .unwrap();
            assert_eq!(completing.virtual_shards, expected_count);
            current = coordinator_step(&completing, 3, &owners, false)
                .unwrap()
                .unwrap();
            assert!(current.migration.is_none());
        }

        assert_eq!(current.virtual_shards, 3);
        assert_eq!(coordinator_step(&current, 3, &owners, false).unwrap(), None);
    }

    #[test]
    fn failed_migration_keeps_live_routing_unchanged() {
        let original = initial(1);
        let planned = plan_next_split(&original, 2, &owners(2)).unwrap();
        let failed = fail(&planned, "clone failed").unwrap();
        assert_eq!(failed.routing, original.routing);
        assert_eq!(failed.virtual_shards, 1);
        assert_eq!(
            failed.migration.as_ref().unwrap().error.as_deref(),
            Some("clone failed")
        );
    }

    #[test]
    fn coordinator_waits_for_writer_and_lease_gates() {
        let owners = owners(2);
        let original = initial(1);
        let planned = coordinator_step(&original, 2, &owners, false)
            .unwrap()
            .unwrap();
        assert_eq!(coordinator_step(&planned, 2, &owners, false).unwrap(), None);

        let prepared = mark_prepared(&planned).unwrap();
        let draining = coordinator_step(&prepared, 2, &owners, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            coordinator_step(&draining, 2, &owners, false).unwrap(),
            None
        );
        let cloning = coordinator_step(&draining, 2, &owners, true)
            .unwrap()
            .unwrap();
        assert_eq!(
            cloning.migration.as_ref().unwrap().phase,
            MigrationPhase::Cloning
        );
        assert_eq!(coordinator_step(&cloning, 2, &owners, true).unwrap(), None);
        let ready = mark_ready(&cloning).unwrap();
        let completing = coordinator_step(&ready, 2, &owners, false)
            .unwrap()
            .unwrap();
        let completed = coordinator_step(&completing, 2, &owners, false)
            .unwrap()
            .unwrap();
        assert!(completed.migration.is_none());
    }

    #[tokio::test]
    async fn only_source_writer_can_acknowledge_preparation() {
        let planned = plan_next_split(&initial(1), 2, &owners(2)).unwrap();
        let store = FakeAssignmentStore::new(Some(planned));
        let executor = RecordingExecutor::default();

        assert!(
            !prepare_owned_split(&store, &executor, "writer-1")
                .await
                .unwrap()
        );
        assert!(
            prepare_owned_split(&store, &executor, "writer-0")
                .await
                .unwrap()
        );
        assert_eq!(executor.preflights.load(Ordering::Relaxed), 1);
        assert_eq!(
            store
                .load()
                .await
                .unwrap()
                .unwrap()
                .migration
                .unwrap()
                .phase,
            MigrationPhase::Prepared
        );

        let draining = begin_drain(&store.load().await.unwrap().unwrap()).unwrap();
        store.publish(draining.clone()).await.unwrap();
        let cloning = begin_clone(&draining).unwrap();
        store.publish(cloning).await.unwrap();
        assert!(
            prepare_owned_split(&store, &executor, "writer-0")
                .await
                .unwrap()
        );
        assert_eq!(executor.clones.load(Ordering::Relaxed), 1);
        assert_eq!(
            store
                .load()
                .await
                .unwrap()
                .unwrap()
                .migration
                .unwrap()
                .phase,
            MigrationPhase::Ready
        );
    }

    struct FatalExecutor;

    #[async_trait]
    impl ShardMigrationExecutor for FatalExecutor {
        async fn preflight_split(
            &self,
            _split: &ShardSplit,
        ) -> Result<(), MigrationExecutionError> {
            Err(MigrationExecutionError::fatal("target conflict"))
        }

        async fn clone_split(&self, _split: &ShardSplit) -> Result<(), MigrationExecutionError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn worker_persists_fatal_preflight_failure() {
        let store = Arc::new(FakeAssignmentStore::new(Some(
            plan_next_split(&initial(1), 2, &owners(2)).unwrap(),
        )));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_migration_worker(
            Arc::clone(&store),
            Arc::new(FatalExecutor),
            "writer-0".to_owned(),
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if store
                    .load()
                    .await
                    .unwrap()
                    .and_then(|map| map.migration)
                    .is_some_and(|migration| migration.phase == MigrationPhase::Failed)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        task.await.unwrap();
        let migration = store.load().await.unwrap().unwrap().migration.unwrap();
        assert_eq!(migration.error.as_deref(), Some("target conflict"));
    }

    #[derive(Default)]
    struct RetryOnceExecutor {
        preflights: AtomicUsize,
    }

    #[async_trait]
    impl ShardMigrationExecutor for RetryOnceExecutor {
        async fn preflight_split(
            &self,
            _split: &ShardSplit,
        ) -> Result<(), MigrationExecutionError> {
            if self.preflights.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(MigrationExecutionError::retryable(io::Error::other(
                    "temporary object-store failure",
                )));
            }
            Ok(())
        }

        async fn clone_split(&self, _split: &ShardSplit) -> Result<(), MigrationExecutionError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn worker_retries_transient_failure_without_persisting_failed_state() {
        let planned = plan_next_split(&initial(1), 2, &owners(2)).unwrap();
        let store = Arc::new(FakeAssignmentStore::new(Some(planned.clone())));
        let executor = Arc::new(RetryOnceExecutor::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_migration_worker(
            Arc::clone(&store),
            Arc::clone(&executor),
            "writer-0".to_owned(),
            cancel.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.preflights.load(Ordering::Relaxed) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .load()
                .await
                .unwrap()
                .unwrap()
                .migration
                .unwrap()
                .phase,
            MigrationPhase::Preparing
        );

        // A store notification should trigger an immediate retry instead of
        // waiting for the worker's periodic retry interval.
        store.update(planned);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if store
                    .load()
                    .await
                    .unwrap()
                    .and_then(|map| map.migration)
                    .is_some_and(|migration| migration.phase == MigrationPhase::Prepared)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        cancel.cancel();
        task.await.unwrap();
        assert_eq!(executor.preflights.load(Ordering::Relaxed), 2);
        assert!(
            store
                .history()
                .iter()
                .all(|map| map.migration.as_ref().is_none_or(|migration| {
                    migration.phase != MigrationPhase::Failed && migration.error.is_none()
                }))
        );
    }
}
