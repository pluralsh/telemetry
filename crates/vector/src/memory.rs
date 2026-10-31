//! Process-wide memory budgets.
//!
//! Every shard and namespace a [`VectorCluster`](crate::VectorCluster) opens
//! draws on one [`MemoryBudget`], as they share one block cache, so the
//! memory queries, writes and compactions hold is bounded per process rather
//! than per namespace or shard:
//!
//! - **query**: the posting lists ANN queries hold while scoring them, each
//!   counted at twice its decoded size (the stored record and the decoded
//!   vectors). A list's share is taken before it is read, at the size a list
//!   reaches before it splits, and trimmed to its real size once read.
//! - **write**: the estimated bytes of writes applied to a namespace's
//!   in-memory delta but not yet flushed to storage, which covers the live
//!   deltas and the frozen ones being flushed. Each namespace flushes once its
//!   delta holds a quarter of the budget.
//! - **compaction**: the estimated working set of the SlateDB compactions
//!   every shard runs in the process: read-ahead for each input of each
//!   subcompaction, and one upload buffer per subcompaction output. A
//!   compaction waits for room before it is scheduled; one may always run
//!   when none is.
//!
//! A query or write share larger than its whole budget waits for all of it
//! rather than forever.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::error::{Error, Result};
use crate::metric_names::{
    MEMORY_BUDGET_CAPACITY_BYTES, MEMORY_BUDGET_REJECTIONS_TOTAL, MEMORY_BUDGET_USED_BYTES,
    MEMORY_BUDGET_WAITS_TOTAL,
};

const MIB: u64 = 1024 * 1024;

/// Default [`MemoryConfig::query_bytes`]: 512 MiB.
pub const DEFAULT_QUERY_BUDGET_BYTES: u64 = 512 * MIB;
/// Default [`MemoryConfig::write_bytes`]: 128 MiB.
pub const DEFAULT_WRITE_BUDGET_BYTES: u64 = 128 * MIB;
/// Default [`MemoryConfig::compaction_bytes`]: 1 GiB.
pub const DEFAULT_COMPACTION_BUDGET_BYTES: u64 = 1024 * MIB;

/// Vector's SlateDB `max_unflushed_bytes`, 128 MiB rather than SlateDB's
/// 1 GiB: the estimated size of the memtables and WAL buffer past which
/// SlateDB holds new writes until L0 flushes catch up. Readers replay the
/// same unflushed WAL, so this bounds both. It must exceed
/// `l0_sst_size_bytes`, SlateDB's 64 MiB.
pub const DEFAULT_MAX_UNFLUSHED_BYTES: usize = 128 * 1024 * 1024;

/// Byte budgets of a process's [`MemoryBudget`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    /// Bytes of posting lists every ANN query in the process holds at once.
    pub query_bytes: u64,
    /// Estimated bytes of writes applied but not yet flushed, across every
    /// namespace. A write that finds no room flushes its namespace and waits
    /// for room up to its timeout, then fails with
    /// [`Error::Backpressure`].
    pub write_bytes: u64,
    /// Estimated bytes the compactions of every shard in the process hold.
    pub compaction_bytes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            query_bytes: DEFAULT_QUERY_BUDGET_BYTES,
            write_bytes: DEFAULT_WRITE_BUDGET_BYTES,
            compaction_bytes: DEFAULT_COMPACTION_BUDGET_BYTES,
        }
    }
}

/// The process-wide budgets of [`MemoryConfig`], cloned into every shard and
/// namespace the process opens.
#[derive(Clone, Debug)]
pub(crate) struct MemoryBudget {
    query: Arc<Budget>,
    write: Arc<Budget>,
    compaction: CompactionBudget,
}

impl MemoryBudget {
    pub(crate) fn new(config: &MemoryConfig) -> Self {
        Self {
            query: Arc::new(Budget::bounded("query", config.query_bytes)),
            write: Arc::new(Budget::bounded("write", config.write_bytes)),
            compaction: CompactionBudget::new(Some(config.compaction_bytes)),
        }
    }

    /// Budgets that never wait, for databases opened outside a cluster.
    pub(crate) fn unbounded() -> Self {
        Self {
            query: Arc::new(Budget::unbounded("query")),
            write: Arc::new(Budget::unbounded("write")),
            compaction: CompactionBudget::new(None),
        }
    }

    /// Holds `bytes` of query memory until the permit drops, waiting for room.
    pub(crate) async fn query(&self, bytes: u64) -> Result<BudgetPermit> {
        self.query.acquire(bytes).await
    }

    /// Holds `bytes` of unflushed writes if they fit now.
    pub(crate) fn try_write(&self, bytes: u64) -> Result<BudgetPermit> {
        self.write.try_acquire(bytes)
    }

    /// Holds `bytes` of unflushed writes, waiting up to `wait` for room.
    pub(crate) async fn write(&self, bytes: u64, wait: Duration) -> Result<BudgetPermit> {
        tokio::time::timeout(wait, self.write.acquire(bytes))
            .await
            .map_err(|_| {
                ::metrics::counter!(MEMORY_BUDGET_REJECTIONS_TOTAL, "budget" => "write")
                    .increment(1);
                Error::Backpressure("the write budget is full; retry later".into())
            })?
    }

    /// Bytes of delta at which a namespace flushes: a quarter of the write
    /// budget, between 1 MiB and 64 MiB.
    pub(crate) fn flush_threshold_bytes(&self) -> usize {
        let quarter = self.write.capacity_bytes().map_or(u64::MAX, |c| c / 4);
        quarter.clamp(MIB, 64 * MIB) as usize
    }

    pub(crate) fn compaction(&self) -> &CompactionBudget {
        &self.compaction
    }

    #[cfg(test)]
    pub(crate) fn query_available(&self) -> u64 {
        self.query.available_bytes()
    }

    #[cfg(test)]
    pub(crate) fn write_available(&self) -> u64 {
        self.write.available_bytes()
    }
}

/// Budgets count KiB, so one acquisition's `u32` permits reach 4 TiB.
const UNIT: u64 = 1024;

#[derive(Debug)]
struct Budget {
    name: &'static str,
    /// `None` for an unbounded budget.
    semaphore: Option<Arc<Semaphore>>,
    /// In [`UNIT`]s.
    capacity: u32,
}

impl Budget {
    fn bounded(name: &'static str, bytes: u64) -> Self {
        let capacity = u32::try_from(bytes.div_ceil(UNIT))
            .unwrap_or(u32::MAX)
            .max(1);
        ::metrics::gauge!(MEMORY_BUDGET_CAPACITY_BYTES, "budget" => name)
            .set(f64::from(capacity) * UNIT as f64);
        Self {
            name,
            semaphore: Some(Arc::new(Semaphore::new(capacity as usize))),
            capacity,
        }
    }

    fn unbounded(name: &'static str) -> Self {
        Self {
            name,
            semaphore: None,
            capacity: u32::MAX,
        }
    }

    fn capacity_bytes(&self) -> Option<u64> {
        self.semaphore
            .as_ref()
            .map(|_| u64::from(self.capacity) * UNIT)
    }

    /// `bytes` in units, clamped to what one acquisition may take.
    fn weight(&self, bytes: u64) -> u32 {
        u32::try_from(bytes.div_ceil(UNIT))
            .unwrap_or(u32::MAX)
            .clamp(1, self.capacity)
    }

    async fn acquire(self: &Arc<Self>, bytes: u64) -> Result<BudgetPermit> {
        let Some(semaphore) = &self.semaphore else {
            return Ok(self.held(None));
        };
        let weight = self.weight(bytes);
        let permit = match Arc::clone(semaphore).try_acquire_many_owned(weight) {
            Ok(permit) => permit,
            Err(_) => {
                ::metrics::counter!(MEMORY_BUDGET_WAITS_TOTAL, "budget" => self.name).increment(1);
                Arc::clone(semaphore)
                    .acquire_many_owned(weight)
                    .await
                    .map_err(|_| self.closed())?
            }
        };
        Ok(self.held(Some(permit)))
    }

    fn try_acquire(self: &Arc<Self>, bytes: u64) -> Result<BudgetPermit> {
        let Some(semaphore) = &self.semaphore else {
            return Ok(self.held(None));
        };
        match Arc::clone(semaphore).try_acquire_many_owned(self.weight(bytes)) {
            Ok(permit) => Ok(self.held(Some(permit))),
            Err(TryAcquireError::NoPermits) => Err(Error::Backpressure(format!(
                "the {} budget is full; retry later",
                self.name
            ))),
            Err(TryAcquireError::Closed) => Err(self.closed()),
        }
    }

    fn closed(&self) -> Error {
        Error::Unavailable(format!("{} budget closed", self.name))
    }

    fn held(self: &Arc<Self>, permit: Option<OwnedSemaphorePermit>) -> BudgetPermit {
        self.report();
        BudgetPermit {
            permit,
            budget: Arc::clone(self),
        }
    }

    #[cfg(test)]
    fn available_bytes(&self) -> u64 {
        self.semaphore
            .as_ref()
            .map_or(u64::MAX, |s| s.available_permits() as u64 * UNIT)
    }

    fn report(&self) {
        if let Some(semaphore) = &self.semaphore {
            let used =
                u64::from(self.capacity).saturating_sub(semaphore.available_permits() as u64);
            ::metrics::gauge!(MEMORY_BUDGET_USED_BYTES, "budget" => self.name)
                .set((used * UNIT) as f64);
        }
    }
}

/// Bytes of a [`MemoryBudget`] held until this drops.
#[derive(Debug)]
pub(crate) struct BudgetPermit {
    permit: Option<OwnedSemaphorePermit>,
    budget: Arc<Budget>,
}

impl BudgetPermit {
    /// Holds `bytes` instead: returns the excess at once, or returns what it
    /// holds and waits for all of `bytes`, so two growing permits never wait
    /// on each other.
    pub(crate) async fn resize(&mut self, bytes: u64) -> Result<()> {
        let (Some(semaphore), Some(held)) = (&self.budget.semaphore, &mut self.permit) else {
            return Ok(());
        };
        let target = self.budget.weight(bytes);
        let count = held.num_permits();
        if (target as usize) < count {
            drop(held.split(count - target as usize));
        } else if target as usize > count {
            drop(self.permit.take());
            self.budget.report();
            let permit = Arc::clone(semaphore)
                .acquire_many_owned(target)
                .await
                .map_err(|_| self.budget.closed())?;
            self.permit = Some(permit);
        }
        self.budget.report();
        Ok(())
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.budget.report();
    }
}

/// The compaction budget, shared by the compaction scheduler of every shard
/// in the process. Each scheduler records the estimated bytes of its running
/// compactions on every poll, and starts new ones only while the process
/// total stays within the budget.
#[derive(Clone, Debug)]
pub(crate) struct CompactionBudget {
    inner: Arc<CompactionBudgetInner>,
}

#[derive(Debug)]
struct CompactionBudgetInner {
    capacity: Option<u64>,
    held: Mutex<HashMap<u64, u64>>,
    next_id: AtomicU64,
}

impl CompactionBudget {
    fn new(capacity: Option<u64>) -> Self {
        if let Some(capacity) = capacity {
            ::metrics::gauge!(MEMORY_BUDGET_CAPACITY_BYTES, "budget" => "compaction")
                .set(capacity as f64);
        }
        Self {
            inner: Arc::new(CompactionBudgetInner {
                capacity,
                held: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn is_bounded(&self) -> bool {
        self.inner.capacity.is_some()
    }

    /// A share for one compaction scheduler; dropping it forgets its
    /// compactions.
    pub(crate) fn share(&self) -> CompactionShare {
        CompactionShare {
            budget: self.clone(),
            id: self.inner.next_id.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// One compaction scheduler's part of a [`CompactionBudget`].
#[derive(Debug)]
pub(crate) struct CompactionShare {
    budget: CompactionBudget,
    id: u64,
}

impl CompactionShare {
    /// Records `running` estimated bytes of this scheduler's compactions and
    /// returns how many of `proposed`, in order, may start beside them and
    /// every other scheduler's. The first may start whenever nothing runs in
    /// the process, however large.
    pub(crate) fn admit(&self, running: u64, proposed: &[u64]) -> usize {
        let inner = &self.budget.inner;
        let mut held = inner.held.lock().expect("lock poisoned");
        let others: u64 = held
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(_, bytes)| *bytes)
            .sum();
        let mut mine = running;
        let mut admitted = 0;
        for &bytes in proposed {
            let total = others.saturating_add(mine);
            let fits = inner
                .capacity
                .is_none_or(|capacity| total == 0 || total.saturating_add(bytes) <= capacity);
            if !fits {
                ::metrics::counter!(MEMORY_BUDGET_WAITS_TOTAL, "budget" => "compaction")
                    .increment(1);
                break;
            }
            mine = mine.saturating_add(bytes);
            admitted += 1;
        }
        held.insert(self.id, mine);
        if inner.capacity.is_some() {
            ::metrics::gauge!(MEMORY_BUDGET_USED_BYTES, "budget" => "compaction")
                .set(others.saturating_add(mine) as f64);
        }
        admitted
    }
}

impl Drop for CompactionShare {
    fn drop(&mut self) {
        self.budget
            .inner
            .held
            .lock()
            .expect("lock poisoned")
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: u64 = 1024;

    fn budget(query_bytes: u64, write_bytes: u64, compaction_bytes: u64) -> MemoryBudget {
        MemoryBudget::new(&MemoryConfig {
            query_bytes,
            write_bytes,
            compaction_bytes,
        })
    }

    #[tokio::test]
    async fn should_share_one_budget_between_clones() {
        // given
        let first = budget(100 * KIB, 100 * KIB, 0);
        let second = first.clone();
        let held = first.query(60 * KIB).await.unwrap();
        assert_eq!(second.query_available(), 40 * KIB);

        // when
        let waiting = tokio::spawn({
            let second = second.clone();
            async move { second.query(60 * KIB).await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // then
        assert!(!waiting.is_finished(), "the shared budget has no room");
        drop(held);
        waiting.await.unwrap().unwrap();
        assert_eq!(first.query_available(), 100 * KIB);
    }

    #[tokio::test]
    async fn should_take_the_whole_budget_for_a_larger_acquisition() {
        // given
        let budget = budget(100 * KIB, 100 * KIB, 0);

        // when
        let held = budget.query(10_000 * KIB).await.unwrap();

        // then
        assert_eq!(budget.query_available(), 0);
        drop(held);
        assert_eq!(budget.query_available(), 100 * KIB);
    }

    #[tokio::test]
    async fn should_count_budgets_beyond_four_gib() {
        // given
        let large = 16 * 1024 * MIB;
        let budget = budget(large, large, large);

        // when
        let held = budget.query(8 * 1024 * MIB).await.unwrap();

        // then
        assert_eq!(budget.query_available(), 8 * 1024 * MIB);
        drop(held);
    }

    #[tokio::test]
    async fn should_reject_writes_that_do_not_fit_and_time_waiting_writes_out() {
        // given
        let budget = budget(100 * KIB, 100 * KIB, 0);
        let held = budget.try_write(80 * KIB).unwrap();

        // when / then
        assert!(matches!(
            budget.try_write(30 * KIB),
            Err(Error::Backpressure(_))
        ));
        assert!(matches!(
            budget.write(30 * KIB, Duration::from_millis(10)).await,
            Err(Error::Backpressure(_))
        ));
        budget.try_write(20 * KIB).unwrap();
        drop(held);
        assert_eq!(budget.write_available(), 100 * KIB);
    }

    #[tokio::test]
    async fn should_admit_a_waiting_write_once_room_frees() {
        // given
        let budget = budget(100 * KIB, 100 * KIB, 0);
        let held = budget.try_write(80 * KIB).unwrap();
        let waiting = tokio::spawn({
            let budget = budget.clone();
            async move {
                budget
                    .write(50 * KIB, Duration::from_secs(5))
                    .await
                    .map(drop)
            }
        });

        // when
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);

        // then
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn should_resize_a_permit_down_and_up() {
        // given
        let budget = budget(100 * KIB, 100 * KIB, 0);
        let mut permit = budget.query(60 * KIB).await.unwrap();

        // when / then
        permit.resize(10 * KIB).await.unwrap();
        assert_eq!(budget.query_available(), 90 * KIB);
        permit.resize(70 * KIB).await.unwrap();
        assert_eq!(budget.query_available(), 30 * KIB);
        drop(permit);
        assert_eq!(budget.query_available(), 100 * KIB);
    }

    #[tokio::test]
    async fn should_not_deadlock_two_permits_growing_at_once() {
        // given: two permits hold the whole budget and both need more.
        let budget = budget(100 * KIB, 100 * KIB, 0);
        let mut a = budget.query(50 * KIB).await.unwrap();
        let mut b = budget.query(50 * KIB).await.unwrap();

        // when
        let grow_a = tokio::spawn(async move {
            a.resize(80 * KIB).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
        let grow_b = tokio::spawn(async move {
            b.resize(80 * KIB).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        // then
        tokio::time::timeout(Duration::from_secs(5), async {
            grow_a.await.unwrap();
            grow_b.await.unwrap();
        })
        .await
        .expect("growing permits deadlocked");
        assert_eq!(budget.query_available(), 100 * KIB);
    }

    #[tokio::test]
    async fn should_never_wait_on_unbounded_budgets() {
        // given
        let budget = MemoryBudget::unbounded();

        // when
        let a = budget.query(u64::MAX).await.unwrap();
        let b = budget.try_write(u64::MAX).unwrap();

        // then
        assert_eq!(budget.flush_threshold_bytes(), 64 * MIB as usize);
        drop((a, b));
    }

    #[test]
    fn should_flush_at_a_quarter_of_the_write_budget_within_bounds() {
        assert_eq!(
            budget(0, 128 * MIB, 0).flush_threshold_bytes(),
            32 * MIB as usize
        );
        assert_eq!(budget(0, 2 * MIB, 0).flush_threshold_bytes(), MIB as usize);
        assert_eq!(
            budget(0, 1024 * MIB, 0).flush_threshold_bytes(),
            64 * MIB as usize
        );
    }

    #[test]
    fn should_admit_compactions_while_the_process_total_fits() {
        // given
        let budget = budget(0, 0, 100);
        let first = budget.compaction().share();
        let second = budget.compaction().share();

        // when / then: the first fits two, the second only what's left.
        assert_eq!(first.admit(0, &[40, 40, 40]), 2);
        assert_eq!(second.admit(0, &[30, 10]), 0);
        assert_eq!(second.admit(0, &[20]), 1);
        // Once the first's compactions finish, its share frees.
        assert_eq!(first.admit(0, &[]), 0);
        assert_eq!(second.admit(20, &[60, 30]), 1);
    }

    #[test]
    fn should_admit_one_oversized_compaction_when_nothing_runs() {
        // given
        let budget = budget(0, 0, 100);
        let first = budget.compaction().share();
        let second = budget.compaction().share();

        // when / then
        assert_eq!(first.admit(0, &[500, 10]), 1);
        assert_eq!(second.admit(0, &[10]), 0);
        drop(first);
        assert_eq!(second.admit(0, &[10]), 1);
    }

    #[test]
    fn should_admit_every_compaction_without_a_bound() {
        let budget = MemoryBudget::unbounded();
        let share = budget.compaction().share();
        assert!(!budget.compaction().is_bounded());
        assert_eq!(share.admit(u64::MAX, &[u64::MAX, 1]), 2);
    }
}
