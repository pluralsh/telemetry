use std::{collections::HashSet, future::Future, sync::Arc};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use tokio::sync::{RwLock, RwLockReadGuard, Semaphore};

use crate::{
    AssignmentGeneration, BoxError, Owner, ShardDatabase, ShardId, ShardLifecycle, ShardMap,
    ShardSet,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("shard has no active owner")]
    NoOwner(ShardId),
    #[error("shard owner disappeared")]
    OwnerDisappeared(ShardId),
    #[error("local shard is draining")]
    Draining(ShardId),
    #[error("server is shutting down")]
    ShuttingDown,
}

/// The outcome of one forwarding attempt. `Stale` means the receiver
/// rejected our ownership view and the write may be retried after refresh.
#[derive(Debug)]
pub enum ForwardError<E> {
    Stale(E),
    Failed(E),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterLimits {
    /// Per-request and process-wide cap on concurrent shard writes.
    pub remote_concurrency: usize,
    /// Retries after a stale-ownership rejection.
    pub remote_retries: usize,
}

/// Proof that a local write was admitted before any drain of its shard.
pub struct LocalWriteGuard<'a> {
    _draining: RwLockReadGuard<'a, HashSet<ShardId>>,
}

/// Routes pre-split shard batches to the local writer or their remote owner.
pub struct WriteRouter {
    local_owner: String,
    assignment: Arc<RwLock<ShardMap>>,
    draining: Arc<RwLock<HashSet<ShardId>>>,
    remote_limit: Semaphore,
    limits: RouterLimits,
}

impl WriteRouter {
    pub fn new(
        local_owner: impl Into<String>,
        assignment: Arc<RwLock<ShardMap>>,
        limits: RouterLimits,
    ) -> Self {
        Self {
            local_owner: local_owner.into(),
            assignment,
            draining: Arc::new(RwLock::new(HashSet::new())),
            remote_limit: Semaphore::new(limits.remote_concurrency.max(1)),
            limits,
        }
    }

    pub fn local_owner(&self) -> &str {
        &self.local_owner
    }

    pub fn assignment(&self) -> &Arc<RwLock<ShardMap>> {
        &self.assignment
    }

    /// Shards whose local writes are rejected while ownership is handed off.
    pub fn draining(&self) -> &Arc<RwLock<HashSet<ShardId>>> {
        &self.draining
    }

    pub async fn is_draining(&self, shard: ShardId) -> bool {
        self.draining.read().await.contains(&shard)
    }

    /// Waits for in-flight local writes to `shards`, then rejects new ones.
    pub async fn start_draining(&self, shards: impl IntoIterator<Item = ShardId>) {
        self.draining.write().await.extend(shards);
    }

    pub async fn stop_draining(&self, shard: ShardId) {
        self.draining.write().await.remove(&shard);
    }

    /// Admits one local write to `shard`. Hold the guard until the write
    /// completes: a drain takes the lock exclusively, so it is a barrier for
    /// every write admitted before it.
    pub async fn admit(&self, shard: ShardId) -> Result<LocalWriteGuard<'_>, RouteError> {
        let draining = self.draining.read().await;
        if draining.contains(&shard) {
            return Err(RouteError::Draining(shard));
        }
        Ok(LocalWriteGuard {
            _draining: draining,
        })
    }

    /// Runs `write` while holding an [`admit`](Self::admit) guard for `shard`.
    pub async fn write_local<T, E, Fut>(&self, shard: ShardId, write: Fut) -> Result<T, E>
    where
        E: From<RouteError>,
        Fut: Future<Output = Result<T, E>>,
    {
        let _admitted = self.admit(shard).await?;
        write.await
    }

    /// Writes every `(shard, batch)` group through the drain barrier when it is
    /// owned locally and through `remote` otherwise, with bounded fan-out.
    pub async fn dispatch<B, E, L, LF, R, RF>(
        &self,
        assignment: &ShardMap,
        groups: impl IntoIterator<Item = (ShardId, B)>,
        local: L,
        remote: R,
    ) -> Result<(), E>
    where
        E: From<RouteError>,
        L: Fn(ShardId, B) -> LF,
        LF: Future<Output = Result<(), E>>,
        R: Fn(Owner, ShardId, AssignmentGeneration, B) -> RF,
        RF: Future<Output = Result<(), E>>,
    {
        let generation = assignment.generation;
        let targets = groups
            .into_iter()
            .map(|(shard, batch)| {
                let owner = assignment
                    .owner_of(shard)
                    .cloned()
                    .ok_or(RouteError::NoOwner(shard))?;
                Ok((owner, shard, batch))
            })
            .collect::<Result<Vec<_>, RouteError>>()?;
        let (local, remote) = (&local, &remote);
        let results = stream::iter(targets.into_iter().map(|(owner, shard, batch)| async move {
            let _permit = self
                .remote_limit
                .acquire()
                .await
                .map_err(|_| RouteError::ShuttingDown)?;
            if owner.id == self.local_owner {
                self.write_local(shard, local(shard, batch)).await
            } else {
                remote(owner, shard, generation, batch).await
            }
        }))
        .buffer_unordered(self.limits.remote_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
        results.into_iter().collect()
    }

    /// Sends `batch` to its remote owner, refreshing the owner and generation
    /// from the live assignment after each stale-ownership rejection.
    pub async fn forward<B, E, F, Fut>(
        &self,
        mut owner: Owner,
        shard: ShardId,
        mut generation: AssignmentGeneration,
        batch: &B,
        send: F,
    ) -> Result<(), E>
    where
        E: From<RouteError>,
        F: Fn(&Owner, AssignmentGeneration, &B) -> Fut,
        Fut: Future<Output = Result<(), ForwardError<E>>>,
    {
        let mut attempt = 0;
        loop {
            match send(&owner, generation, batch).await {
                Ok(()) => return Ok(()),
                Err(ForwardError::Stale(_)) if attempt < self.limits.remote_retries => {
                    attempt += 1;
                    let assignment = self.assignment.read().await;
                    generation = assignment.generation;
                    owner = assignment
                        .owner_of(shard)
                        .cloned()
                        .ok_or(RouteError::OwnerDisappeared(shard))?;
                }
                Err(ForwardError::Stale(error) | ForwardError::Failed(error)) => {
                    return Err(error);
                }
            }
        }
    }
}

/// Writer lifecycle over a [`ShardSet`] guarded by a [`WriteRouter`]'s drain
/// barrier.
pub struct RoutedShardLifecycle<D: ShardDatabase> {
    shards: Arc<ShardSet<D>>,
    router: Arc<WriteRouter>,
}

impl<D: ShardDatabase> RoutedShardLifecycle<D> {
    pub fn new(shards: Arc<ShardSet<D>>, router: Arc<WriteRouter>) -> Self {
        Self { shards, router }
    }
}

#[async_trait]
impl<D> ShardLifecycle for RoutedShardLifecycle<D>
where
    D: ShardDatabase,
    D::Error: Into<BoxError>,
{
    async fn open(
        &self,
        shard: ShardId,
        _generation: AssignmentGeneration,
    ) -> Result<(), BoxError> {
        self.shards.open_shard(shard).await.map_err(Into::into)?;
        self.router.stop_draining(shard).await;
        Ok(())
    }

    async fn drain(&self, shard: ShardId) -> Result<(), BoxError> {
        self.router.start_draining([shard]).await;
        Ok(())
    }

    async fn flush(&self, shard: ShardId) -> Result<(), BoxError> {
        self.shards.flush_shard(shard).await.map_err(Into::into)
    }

    async fn close(&self, shard: ShardId) -> Result<(), BoxError> {
        self.shards.close_shard(shard).await.map_err(Into::into)
    }
}

pub fn shard_request_id(request_id: &str, shard: ShardId) -> String {
    format!("{request_id}-{}", shard.get())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{Assignment, AssignmentState, ShardRange};

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Route(RouteError),
        Remote(&'static str),
    }

    impl From<RouteError> for TestError {
        fn from(error: RouteError) -> Self {
            Self::Route(error)
        }
    }

    fn two_owners(generation: u64) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(generation),
            2,
            vec![
                Assignment::new(
                    Owner::new("a", 0),
                    ShardRange::within(0, 1, 2).unwrap(),
                    AssignmentState::Active,
                ),
                Assignment::new(
                    Owner::new("b", 1),
                    ShardRange::within(1, 2, 2).unwrap(),
                    AssignmentState::Active,
                ),
            ],
        )
        .unwrap()
    }

    fn router(retries: usize) -> WriteRouter {
        WriteRouter::new(
            "a",
            Arc::new(RwLock::new(two_owners(2))),
            RouterLimits {
                remote_concurrency: 4,
                remote_retries: retries,
            },
        )
    }

    #[tokio::test]
    async fn dispatches_local_and_remote_groups() {
        let router = router(0);
        let local = AtomicUsize::new(0);
        let remote = AtomicUsize::new(0);
        router
            .dispatch(
                &two_owners(2),
                [(ShardId::new(0), 1), (ShardId::new(1), 2)],
                |shard, _batch: i32| {
                    assert_eq!(shard, ShardId::new(0));
                    local.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, TestError>(()) }
                },
                |owner, shard, _generation, _batch| {
                    assert_eq!((owner.id.as_str(), shard), ("b", ShardId::new(1)));
                    remote.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                },
            )
            .await
            .unwrap();
        assert_eq!(local.load(Ordering::SeqCst), 1);
        assert_eq!(remote.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn local_writes_to_draining_shards_are_rejected() {
        let router = router(0);
        router.start_draining([ShardId::new(0)]).await;
        let result = router
            .write_local(ShardId::new(0), async { Ok::<_, TestError>(()) })
            .await;
        assert_eq!(
            result,
            Err(TestError::Route(RouteError::Draining(ShardId::new(0))))
        );
    }

    #[tokio::test]
    async fn forward_retries_stale_rejections_with_refreshed_generation() {
        let router = router(1);
        let attempts = AtomicUsize::new(0);
        router
            .forward(
                Owner::new("b", 1),
                ShardId::new(1),
                AssignmentGeneration::new(1),
                &(),
                |_owner, generation, _batch| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        match (attempt, generation.get()) {
                            (0, 1) => Err(ForwardError::Stale(TestError::Remote("stale"))),
                            (1, 2) => Ok(()),
                            _ => Err(ForwardError::Failed(TestError::Remote("unexpected"))),
                        }
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn forward_returns_the_last_stale_error_once_retries_are_exhausted() {
        let router = router(0);
        let result = router
            .forward(
                Owner::new("b", 1),
                ShardId::new(1),
                AssignmentGeneration::new(1),
                &(),
                |_owner, _generation, _batch| async {
                    Err::<(), _>(ForwardError::Stale(TestError::Remote("stale")))
                },
            )
            .await;
        assert_eq!(result, Err(TestError::Remote("stale")));
    }
}
