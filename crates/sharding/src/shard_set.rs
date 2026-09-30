use std::{
    collections::{BTreeMap, btree_map::Entry},
    future::Future,
    sync::Arc,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{ShardId, ShardMap, ShardingOptions};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShardSetError {
    #[error("shard {0} is not open on this node")]
    NotOpen(ShardId),
    #[error("shard {0} database still has in-flight references")]
    InFlight(ShardId),
    #[error("reader reconciliation requires a reader facade")]
    NotReader,
    #[error("shard I/O scheduler is closed")]
    Closed,
}

/// A storage database opened for exactly one storage shard.
#[async_trait]
pub trait ShardDatabase: Send + Sync + 'static {
    type Error: From<ShardSetError> + Send + 'static;

    async fn flush_database(&self) -> Result<(), Self::Error>;

    /// Closes the database. `ShardSet::close_shard` only calls this with the
    /// last reference; `ShardSet::close_all` may pass shared references.
    async fn close_database(self: Arc<Self>) -> Result<(), Self::Error>;
}

pub type ShardOpener<D> = Arc<
    dyn Fn(ShardId) -> BoxFuture<'static, Result<D, <D as ShardDatabase>::Error>> + Send + Sync,
>;

pub fn shard_opener<D, F, Fut>(open: F) -> ShardOpener<D>
where
    D: ShardDatabase,
    F: Fn(ShardId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<D, D::Error>> + Send + 'static,
{
    Arc::new(move |shard| Box::pin(open(shard)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRole {
    Writer,
    Reader,
}

/// The storage shards opened by this process, with a shared I/O budget.
pub struct ShardSet<D: ShardDatabase> {
    role: ShardRole,
    options: ShardingOptions,
    opener: ShardOpener<D>,
    shards: RwLock<BTreeMap<ShardId, Arc<D>>>,
    io_permits: Arc<Semaphore>,
}

impl<D: ShardDatabase> ShardSet<D> {
    pub async fn open(
        role: ShardRole,
        options: ShardingOptions,
        opener: ShardOpener<D>,
        shards: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self, D::Error> {
        let mut set = Self {
            role,
            options,
            opener,
            shards: RwLock::new(BTreeMap::new()),
            io_permits: Arc::new(Semaphore::new(options.io_concurrency_limit() as usize)),
        };
        let opened = set.open_many(shards.into_iter().collect()).await?;
        *set.shards.get_mut() = opened;
        Ok(set)
    }

    async fn open_many(&self, shards: Vec<ShardId>) -> Result<BTreeMap<ShardId, Arc<D>>, D::Error> {
        stream::iter(shards)
            .map(|shard| {
                let open = (self.opener)(shard);
                async move { Ok((shard, Arc::new(open.await?))) }
            })
            .buffer_unordered(self.options.io_concurrency_limit() as usize)
            .try_collect()
            .await
    }

    pub const fn role(&self) -> ShardRole {
        self.role
    }

    pub const fn options(&self) -> ShardingOptions {
        self.options
    }

    pub fn io_permits(&self) -> Arc<Semaphore> {
        Arc::clone(&self.io_permits)
    }

    pub async fn get(&self, shard: ShardId) -> Option<Arc<D>> {
        self.shards.read().await.get(&shard).cloned()
    }

    pub async fn require(&self, shard: ShardId) -> Result<Arc<D>, ShardSetError> {
        self.get(shard).await.ok_or(ShardSetError::NotOpen(shard))
    }

    pub async fn contains(&self, shard: ShardId) -> bool {
        self.shards.read().await.contains_key(&shard)
    }

    pub async fn ids(&self) -> Vec<ShardId> {
        self.shards.read().await.keys().copied().collect()
    }

    pub async fn len(&self) -> usize {
        self.shards.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.shards.read().await.is_empty()
    }

    pub async fn databases(&self) -> Vec<Arc<D>> {
        self.shards.read().await.values().cloned().collect()
    }

    pub async fn entries(&self) -> Vec<(ShardId, Arc<D>)> {
        self.shards
            .read()
            .await
            .iter()
            .map(|(shard, database)| (*shard, Arc::clone(database)))
            .collect()
    }

    pub async fn open_shard(&self, shard: ShardId) -> Result<(), D::Error> {
        if self.contains(shard).await {
            return Ok(());
        }
        let database = Arc::new((self.opener)(shard).await?);
        let mut shards = self.shards.write().await;
        if let Entry::Vacant(entry) = shards.entry(shard) {
            entry.insert(database);
            return Ok(());
        }
        drop(shards);
        database.close_database().await
    }

    pub async fn flush_shard(&self, shard: ShardId) -> Result<(), D::Error> {
        if let Some(database) = self.get(shard).await {
            database.flush_database().await?;
        }
        Ok(())
    }

    /// Closes `shard`, keeping it open if a caller still holds a reference.
    pub async fn close_shard(&self, shard: ShardId) -> Result<(), D::Error> {
        let mut shards = self.shards.write().await;
        let Some(database) = shards.remove(&shard) else {
            return Ok(());
        };
        if Arc::strong_count(&database) > 1 {
            shards.insert(shard, database);
            return Err(ShardSetError::InFlight(shard).into());
        }
        drop(shards);
        database.close_database().await
    }

    /// Opens readers for every shard in `assignment`. Shards are never
    /// removed because routing epochs only grow.
    pub async fn reconcile(&self, assignment: &ShardMap) -> Result<(), D::Error> {
        if self.role != ShardRole::Reader {
            return Err(ShardSetError::NotReader.into());
        }
        let current = self.ids().await;
        let missing = (0..assignment.shard_count)
            .map(ShardId::new)
            .filter(|shard| !current.contains(shard))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        let opened = self.open_many(missing).await?;
        let mut duplicates = Vec::new();
        {
            let mut shards = self.shards.write().await;
            for (shard, database) in opened {
                match shards.entry(shard) {
                    Entry::Vacant(entry) => {
                        entry.insert(database);
                    }
                    Entry::Occupied(_) => duplicates.push(database),
                }
            }
        }
        for database in duplicates {
            database.close_database().await?;
        }
        Ok(())
    }

    pub async fn flush_all(&self) -> Result<(), D::Error> {
        let databases = self.databases().await;
        futures::future::try_join_all(databases.iter().map(|database| database.flush_database()))
            .await?;
        Ok(())
    }

    /// Closes every shard, returning the first failure after attempting all.
    pub async fn close_all(&self) -> Result<(), D::Error> {
        let databases = std::mem::take(&mut *self.shards.write().await);
        let mut first_error = None;
        for database in databases.into_values() {
            if let Err(error) = database.close_database().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Runs `work` while holding one permit of the process I/O budget.
    pub async fn with_io<T, E, Fut>(&self, work: Fut) -> Result<T, E>
    where
        E: From<ShardSetError>,
        Fut: Future<Output = Result<T, E>>,
    {
        let _permit = self
            .io_permits
            .acquire()
            .await
            .map_err(|_| ShardSetError::Closed)?;
        work.await
    }

    /// Runs `read` on every open shard concurrently, one I/O permit each.
    /// Results are in shard order.
    pub async fn fan_out<T, E, F, Fut>(&self, read: F) -> Result<Vec<T>, E>
    where
        E: From<ShardSetError>,
        F: Fn(Arc<D>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let databases = self.databases().await;
        futures::future::try_join_all(
            databases
                .into_iter()
                .map(|database| self.with_io(read(database))),
        )
        .await
    }

    /// Reserves up to `concurrency` I/O permits (clamped to the process
    /// budget). Returns `None` when `cancel` fires first.
    pub async fn reserve_io(
        &self,
        concurrency: usize,
        cancel: &CancellationToken,
    ) -> Result<Option<(OwnedSemaphorePermit, usize)>, ShardSetError> {
        let concurrency = concurrency.clamp(1, self.options.io_concurrency_limit() as usize);
        let count = u32::try_from(concurrency).unwrap_or(u32::MAX);
        tokio::select! {
            permits = Arc::clone(&self.io_permits).acquire_many_owned(count) => permits
                .map(|permits| Some((permits, concurrency)))
                .map_err(|_| ShardSetError::Closed),
            () = cancel.cancelled() => Ok(None),
        }
    }

    /// Warms every open shard sequentially under one reservation of up to
    /// `concurrency` I/O permits.
    pub async fn warm<E, F, Fut>(
        &self,
        concurrency: usize,
        cancel: &CancellationToken,
        warm: F,
    ) -> Result<(), E>
    where
        E: From<ShardSetError>,
        F: Fn(Arc<D>, usize) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let Some((_permits, concurrency)) = self.reserve_io(concurrency, cancel).await? else {
            return Ok(());
        };
        for database in self.databases().await {
            if cancel.is_cancelled() {
                return Ok(());
            }
            warm(database, concurrency).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{Assignment, AssignmentGeneration, AssignmentState, Owner, ShardRange};

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Shard(#[from] ShardSetError),
    }

    struct TestDb {
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ShardDatabase for TestDb {
        type Error = TestError;

        async fn flush_database(&self) -> Result<(), TestError> {
            Ok(())
        }

        async fn close_database(self: Arc<Self>) -> Result<(), TestError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn opener(closes: &Arc<AtomicUsize>) -> ShardOpener<TestDb> {
        let closes = Arc::clone(closes);
        shard_opener(move |_| {
            let closes = Arc::clone(&closes);
            async move { Ok(TestDb { closes }) }
        })
    }

    fn assignment(shards: u32) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(1),
            shards,
            vec![Assignment::new(
                Owner::new("a", 0),
                ShardRange::within(0, shards, shards).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn close_shard_keeps_referenced_databases_open() {
        let closes = Arc::new(AtomicUsize::new(0));
        let set = ShardSet::open(
            ShardRole::Writer,
            ShardingOptions::default(),
            opener(&closes),
            [],
        )
        .await
        .unwrap();
        let shard = ShardId::new(1);
        set.open_shard(shard).await.unwrap();
        set.open_shard(shard).await.unwrap();
        assert_eq!(set.len().await, 1);

        let reference = set.get(shard).await.unwrap();
        assert_eq!(
            set.close_shard(shard).await,
            Err(TestError::Shard(ShardSetError::InFlight(shard)))
        );
        assert!(set.contains(shard).await);
        drop(reference);
        set.close_shard(shard).await.unwrap();
        assert!(!set.contains(shard).await);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn only_readers_reconcile_and_they_open_every_shard() {
        let closes = Arc::new(AtomicUsize::new(0));
        let writer = ShardSet::open(
            ShardRole::Writer,
            ShardingOptions::default(),
            opener(&closes),
            [],
        )
        .await
        .unwrap();
        assert_eq!(
            writer.reconcile(&assignment(2)).await,
            Err(TestError::Shard(ShardSetError::NotReader))
        );

        let reader = ShardSet::open(
            ShardRole::Reader,
            ShardingOptions::default(),
            opener(&closes),
            [ShardId::new(0)],
        )
        .await
        .unwrap();
        reader.reconcile(&assignment(3)).await.unwrap();
        assert_eq!(
            reader.ids().await,
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)]
        );
        reader.close_all().await.unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn reserve_io_clamps_to_budget_and_honours_cancellation() {
        let closes = Arc::new(AtomicUsize::new(0));
        let set = ShardSet::open(
            ShardRole::Reader,
            ShardingOptions::new(1, 4).unwrap(),
            opener(&closes),
            [],
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let (permits, concurrency) = set.reserve_io(99, &cancel).await.unwrap().unwrap();
        assert_eq!(concurrency, 4);
        cancel.cancel();
        assert!(set.reserve_io(1, &cancel).await.unwrap().is_none());
        drop(permits);
    }
}
