//! Process-wide admission for PromQL queries.
//!
//! Every query already has its own memory cap, but without a process-wide
//! limit a dashboard opening twenty panels at once could reserve twenty caps.
//! [`QueryAdmission`] bounds both: a query waits for one of
//! [`QueryLimitsConfig::max_concurrent_queries`] slots, and its memory
//! reservation is a child of one shared
//! [`QueryLimitsConfig::memory_budget_bytes`] budget. The shard-serving side
//! of reader groups has its own slots, so a coordinator waiting on a remote
//! shard never holds up that shard's own serving.
//!
//! A query that fails because the queries running beside it hold the
//! shared budget, not because it outgrew its own cap, waits for one of
//! them to finish and runs again ([`QueryAdmission::run`]), so a burst of
//! panels queues instead of failing and being retried by the client.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::QueryError;
use crate::promql::memory::MemoryReservation;

/// Default [`QueryLimitsConfig::max_query_memory_bytes`]: 1 GiB.
pub const DEFAULT_MAX_QUERY_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;

/// Limits on the queries one process runs at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimitsConfig {
    /// Queries evaluated at once; the rest wait for a slot.
    pub max_concurrent_queries: usize,
    /// How long a query waits for a slot before failing as timed out.
    pub queue_timeout: Duration,
    /// Engine memory one query may reserve.
    pub max_query_memory_bytes: u64,
    /// Engine memory every running query together may reserve.
    pub memory_budget_bytes: u64,
    /// Shard reads served to other reader groups at once.
    pub max_concurrent_shard_reads: usize,
}

impl Default for QueryLimitsConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 32,
            queue_timeout: Duration::from_secs(30),
            max_query_memory_bytes: DEFAULT_MAX_QUERY_MEMORY_BYTES,
            memory_budget_bytes: 2 * DEFAULT_MAX_QUERY_MEMORY_BYTES,
            max_concurrent_shard_reads: 16,
        }
    }
}

pub(crate) struct QueryAdmission {
    queries: Arc<Semaphore>,
    shard_reads: Arc<Semaphore>,
    budget: MemoryReservation,
    max_query_memory: usize,
    queue_timeout: Duration,
    finished: Arc<Finished>,
}

/// Queries that have released their reservation, and a wakeup for each.
#[derive(Default)]
struct Finished {
    count: AtomicU64,
    notify: Notify,
}

/// A running query's slot and memory reservation; both return on drop.
pub(crate) struct AdmittedQuery {
    _permit: OwnedSemaphorePermit,
    pub(crate) reservation: MemoryReservation,
    finished: Arc<Finished>,
}

impl Drop for AdmittedQuery {
    fn drop(&mut self) {
        self.finished.count.fetch_add(1, Ordering::AcqRel);
        self.finished.notify.notify_waiters();
    }
}

impl QueryAdmission {
    pub(crate) fn new(config: &QueryLimitsConfig) -> Self {
        let budget = usize::try_from(config.memory_budget_bytes).unwrap_or(usize::MAX);
        let per_query = usize::try_from(config.max_query_memory_bytes).unwrap_or(usize::MAX);
        Self {
            queries: Arc::new(Semaphore::new(config.max_concurrent_queries.max(1))),
            shard_reads: Arc::new(Semaphore::new(config.max_concurrent_shard_reads.max(1))),
            budget: MemoryReservation::new(budget),
            max_query_memory: per_query.min(budget),
            queue_timeout: config.queue_timeout,
            finished: Arc::default(),
        }
    }

    /// Waits for a query slot, failing with [`QueryError::Timeout`] once the
    /// queue timeout passes.
    pub(crate) async fn admit(&self) -> Result<AdmittedQuery, QueryError> {
        self.admit_by(Instant::now() + self.queue_timeout).await
    }

    async fn admit_by(&self, deadline: Instant) -> Result<AdmittedQuery, QueryError> {
        let _permit = self.wait(&self.queries, "query", deadline).await?;
        Ok(AdmittedQuery {
            _permit,
            reservation: self.budget.child(self.max_query_memory),
            finished: Arc::clone(&self.finished),
        })
    }

    /// Runs `query` under an admitted slot and reservation. When it fails
    /// starved of the shared budget, waits for another query to finish and
    /// runs it again. Slot waits and retries share one queue timeout; past
    /// it, the last memory failure is returned.
    pub(crate) async fn run<T, F, Fut>(&self, mut query: F) -> Result<T, QueryError>
    where
        F: FnMut(MemoryReservation) -> Fut,
        Fut: Future<Output = Result<T, QueryError>>,
    {
        let deadline = Instant::now() + self.queue_timeout;
        let mut admitted = self.admit_by(deadline).await?;
        loop {
            let result = query(admitted.reservation.clone()).await;
            if result.is_ok() || !admitted.reservation.starved() {
                return result;
            }
            let ours = self.finished.count.load(Ordering::Acquire) + 1;
            drop(admitted);
            ::metrics::counter!("metrics_query_memory_waits_total").increment(1);
            let started = Instant::now();
            if !self.another_finished(ours, deadline).await {
                return result;
            }
            admitted = match self.admit_by(deadline).await {
                Ok(admitted) => admitted,
                Err(_) => return result,
            };
            ::metrics::histogram!("metrics_query_memory_wait_seconds")
                .record(started.elapsed().as_secs_f64());
        }
    }

    /// Waits until more than `seen` queries have finished, unless `deadline`
    /// passes first.
    async fn another_finished(&self, seen: u64, deadline: Instant) -> bool {
        loop {
            let notified = self.finished.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if Instant::now() >= deadline {
                return false;
            }
            if self.finished.count.load(Ordering::Acquire) > seen {
                return true;
            }
            let wait = tokio::time::timeout_at(deadline, notified);
            if wait.await.is_err() {
                return false;
            }
        }
    }

    /// Waits for a slot to serve a shard read to another reader group.
    pub(crate) async fn admit_shard_read(&self) -> Result<OwnedSemaphorePermit, QueryError> {
        let deadline = Instant::now() + self.queue_timeout;
        self.wait(&self.shard_reads, "shard_read", deadline).await
    }

    async fn wait(
        &self,
        slots: &Arc<Semaphore>,
        kind: &'static str,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, QueryError> {
        let started = Instant::now();
        let permit = tokio::time::timeout_at(deadline, Arc::clone(slots).acquire_owned())
            .await
            .map_err(|_| {
                ::metrics::counter!("metrics_query_admission_timeouts_total", "kind" => kind)
                    .increment(1);
                QueryError::Timeout
            })?
            .map_err(|_| QueryError::Execution("query admission closed".to_owned()))?;
        ::metrics::histogram!("metrics_query_admission_wait_seconds", "kind" => kind)
            .record(started.elapsed().as_secs_f64());
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(queries: usize, budget: u64, per_query: u64) -> QueryAdmission {
        QueryAdmission::new(&QueryLimitsConfig {
            max_concurrent_queries: queries,
            queue_timeout: Duration::from_millis(50),
            max_query_memory_bytes: per_query,
            memory_budget_bytes: budget,
            max_concurrent_shard_reads: 1,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn queries_past_the_limit_wait_then_time_out() {
        let admission = admission(1, 100, 100);
        let first = admission.admit().await.unwrap();
        assert!(matches!(admission.admit().await, Err(QueryError::Timeout)));
        drop(first);
        admission.admit().await.unwrap();
    }

    #[tokio::test]
    async fn running_queries_share_the_memory_budget() {
        let admission = admission(2, 100, 80);
        let first = admission.admit().await.unwrap();
        let second = admission.admit().await.unwrap();
        first.reservation.try_grow(80).unwrap();
        assert!(first.reservation.try_grow(1).is_err());
        assert!(second.reservation.try_grow(30).is_err());
        second.reservation.try_grow(20).unwrap();
        drop(first);
        second.reservation.try_grow(60).unwrap();
    }

    #[tokio::test]
    async fn a_query_starved_of_the_budget_runs_again_once_another_finishes() {
        // given: a running query holding 80 of a 100-byte budget
        let admission = Arc::new(admission(2, 100, 80));
        let first = admission.admit().await.unwrap();
        first.reservation.try_grow(80).unwrap();
        let attempts = Arc::new(AtomicU64::new(0));

        // when: a second query needing 60 runs, then the first finishes
        let second = tokio::spawn({
            let (admission, attempts) = (Arc::clone(&admission), Arc::clone(&attempts));
            async move {
                admission
                    .run(|reservation| {
                        attempts.fetch_add(1, Ordering::AcqRel);
                        async move { reservation.try_grow(60).map_err(QueryError::from) }
                    })
                    .await
            }
        });
        while attempts.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        drop(first);

        // then: it succeeds on its second attempt
        second.await.unwrap().unwrap();
        assert_eq!(attempts.load(Ordering::Acquire), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_starved_query_gives_up_at_the_queue_timeout() {
        // given: a budget held for longer than the queue timeout
        let admission = admission(2, 100, 80);
        let first = admission.admit().await.unwrap();
        first.reservation.try_grow(80).unwrap();

        // when
        let result = admission
            .run(|reservation| async move { reservation.try_grow(60).map_err(QueryError::from) })
            .await;

        // then: the memory failure comes back, not a timeout
        assert!(
            matches!(&result, Err(QueryError::Execution(message)) if message.contains("memory limit")),
            "{result:?}"
        );
        drop(first);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_waits_for_a_slot_only_until_the_queue_timeout() {
        // given: a query holding most of the budget, and a starved one
        // waiting for it
        let admission = Arc::new(admission(2, 100, 80));
        let holder = admission.admit().await.unwrap();
        holder.reservation.try_grow(80).unwrap();
        let waiter = tokio::spawn({
            let admission = Arc::clone(&admission);
            async move {
                let started = Instant::now();
                let result =
                    admission
                        .run(|reservation| async move {
                            reservation.try_grow(60).map_err(QueryError::from)
                        })
                        .await;
                (started.elapsed(), result)
            }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;

        // when: at 10ms a query finishes, but another queued first takes
        // its slot and keeps both slots busy past the timeout
        let finishing = admission.admit().await.unwrap();
        let queued = tokio::spawn({
            let admission = Arc::clone(&admission);
            async move { admission.admit().await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        drop(finishing);
        let _slot = queued.await.unwrap();

        // then: the waiter gives up at its queue timeout, with its memory
        // failure rather than a slot timeout
        let (elapsed, result) = waiter.await.unwrap();
        assert!(elapsed <= admission.queue_timeout, "{elapsed:?}");
        assert!(
            matches!(&result, Err(QueryError::Execution(message)) if message.contains("memory limit")),
            "{result:?}"
        );
        drop(holder);
    }

    #[tokio::test]
    async fn a_query_over_its_own_cap_is_not_run_again() {
        let admission = admission(2, 100, 80);
        let attempts = AtomicU64::new(0);
        let result = admission
            .run(|reservation| {
                attempts.fetch_add(1, Ordering::AcqRel);
                async move { reservation.try_grow(90).map_err(QueryError::from) }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn a_query_cap_never_exceeds_the_budget() {
        let admission = admission(1, 10, 100);
        let query = admission.admit().await.unwrap();
        assert_eq!(query.reservation.cap(), 10);
    }
}
