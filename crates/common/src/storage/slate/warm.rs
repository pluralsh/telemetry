//! Block-cache warming over SlateDB SSTs, shared by every product.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures::{StreamExt, TryStreamExt};
use slatedb::manifest::SsTableId;
use slatedb::{CacheTarget, DbCacheManagerOps};
use tokio_util::sync::CancellationToken;

/// SSTs already handed to the block cache, so periodic passes warm only files
/// that appeared since the previous pass.
///
/// One tracker may span every shard of a process: compacted SST ids are
/// ULIDs. A pass that finishes drops every id it did not see, which covers
/// both SSTs compaction removed and SSTs whose bucket left the warm window.
#[derive(Debug, Default)]
pub struct SstWarmTracker {
    state: Mutex<TrackerState>,
    armed: AtomicBool,
}

#[derive(Debug, Default)]
struct TrackerState {
    seen: HashSet<SsTableId>,
    pass: HashSet<SsTableId>,
}

impl SstWarmTracker {
    /// An unarmed tracker records the live SSTs of its first pass without
    /// warming them, so enabling continuous warming never starts with a bulk
    /// warm of the whole window; an armed one warms everything it has not seen.
    pub fn new(armed: bool) -> Self {
        Self {
            state: Mutex::default(),
            armed: AtomicBool::new(armed),
        }
    }

    /// Ends a completed pass: forgets SSTs it did not see and arms the tracker.
    pub fn finish_pass(&self) {
        let mut state = self.lock();
        let pass = std::mem::take(&mut state.pass);
        state.seen.retain(|id| pass.contains(id));
        self.armed.store(true, Ordering::Release);
    }

    /// Ends a failed or cancelled pass without forgetting SSTs it never reached.
    pub fn abandon_pass(&self) {
        self.lock().pass.clear();
    }

    /// Records `id` as live in this pass; true when it should be warmed now.
    fn claim(&self, id: SsTableId) -> bool {
        let mut state = self.lock();
        state.pass.insert(id);
        state.seen.insert(id) && self.armed.load(Ordering::Acquire)
    }

    fn forget(&self, id: SsTableId) {
        self.lock().seen.remove(&id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Warms `work` with up to `concurrency` SSTs in flight, skipping SSTs that
/// `tracker` has already warmed. SSTs are claimed only when their warm
/// starts, so a cancelled pass leaves the rest for the next one, and a failed
/// warm is forgotten so the next pass retries it.
pub async fn warm_ssts<D>(
    db: &D,
    product: &'static str,
    work: Vec<(SsTableId, Arc<[CacheTarget]>)>,
    include_payloads: bool,
    concurrency: usize,
    cancel: &CancellationToken,
    tracker: Option<&SstWarmTracker>,
) -> Result<(), slatedb::Error>
where
    D: DbCacheManagerOps + Sync + ?Sized,
{
    futures::stream::iter(work)
        .filter(|(id, _)| std::future::ready(tracker.is_none_or(|tracker| tracker.claim(*id))))
        .map(|(id, targets)| async move {
            let result = db.warm_sst(id, &targets).await;
            if result.is_err()
                && let Some(tracker) = tracker
            {
                tracker.forget(id);
            }
            metrics::counter!(
                "telemetry_cache_warmer_ssts_total",
                "product" => product,
                "status" => if result.is_ok() { "success" } else { "error" },
                "payloads" => if include_payloads { "included" } else { "excluded" }
            )
            .increment(1);
            result
        })
        .buffer_unordered(concurrency.max(1))
        .take_until(cancel.cancelled())
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> SsTableId {
        SsTableId::Wal(n)
    }

    #[test]
    fn unarmed_tracker_records_first_pass_without_warming() {
        let tracker = SstWarmTracker::new(false);
        assert!(!tracker.claim(id(1)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
    }

    #[test]
    fn finished_pass_forgets_ssts_it_did_not_see() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(tracker.claim(id(1)));
    }

    #[test]
    fn abandoned_pass_keeps_unvisited_ssts() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        assert!(tracker.claim(id(2)));
        tracker.finish_pass();

        assert!(!tracker.claim(id(1)));
        tracker.abandon_pass();

        assert!(!tracker.claim(id(1)));
        assert!(!tracker.claim(id(2)));
    }

    #[test]
    fn forgotten_sst_is_warmed_again() {
        let tracker = SstWarmTracker::new(true);
        assert!(tracker.claim(id(1)));
        tracker.forget(id(1));
        assert!(tracker.claim(id(1)));
    }
}
