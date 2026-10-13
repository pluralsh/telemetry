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

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
}

/// A running query's slot and memory reservation; both return on drop.
pub(crate) struct AdmittedQuery {
    _permit: OwnedSemaphorePermit,
    pub(crate) reservation: MemoryReservation,
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
        }
    }

    /// Waits for a query slot, failing with [`QueryError::Timeout`] once the
    /// queue timeout passes.
    pub(crate) async fn admit(&self) -> Result<AdmittedQuery, QueryError> {
        let _permit = self.wait(&self.queries, "query").await?;
        Ok(AdmittedQuery {
            _permit,
            reservation: self.budget.child(self.max_query_memory),
        })
    }

    /// Waits for a slot to serve a shard read to another reader group.
    pub(crate) async fn admit_shard_read(&self) -> Result<OwnedSemaphorePermit, QueryError> {
        self.wait(&self.shard_reads, "shard_read").await
    }

    async fn wait(
        &self,
        slots: &Arc<Semaphore>,
        kind: &'static str,
    ) -> Result<OwnedSemaphorePermit, QueryError> {
        let started = Instant::now();
        let permit = tokio::time::timeout(self.queue_timeout, Arc::clone(slots).acquire_owned())
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
    async fn a_query_cap_never_exceeds_the_budget() {
        let admission = admission(1, 10, 100);
        let query = admission.admit().await.unwrap();
        assert_eq!(query.reservation.cap(), 10);
    }
}
