//! FTS compaction *scheduling* policy for the vector subsystem (RFC-0006).
//!
//! Deletes are recorded lazily into the FTS deletions bitmap and the actual
//! cleanup of postings/stats happens during compaction (see
//! [`compaction_filter`](super::compaction_filter)). To bound the amount of
//! garbage carried in the FTS segment — and the size of the in-memory deletions
//! bitmap loaded for every query — Vector forces a *major* (full) compaction of
//! the FTS segment once any indexed field's delete fraction crosses
//! `delete_compaction_threshold` (default 20%).
//!
//! The trigger is computed per field from the persisted [`FieldStats`]
//! (`count` = total documents written to the field that are still present, i.e.
//! live + pending-deleted; `deletes` = outstanding deletes, a subset of
//! `count`): a field is over threshold when `deletes > threshold * count`. If
//! *any* indexed field is over threshold, a full FTS compaction is forced.
//!
//! [`VectorCompactionScheduler`] wraps SlateDB's size-tiered scheduler. On every
//! `propose()` it reads the in-memory [`FtsDeleteTracker`] (the trigger signal
//! must come from memory: `propose()` is synchronous and has no KV access);
//! when a field is over threshold it plans a full compaction of the FTS segment
//! via `CompactionScheduler::generate` + [`CompactionRequest::FullSegment`],
//! merging it with the inner scheduler's proposals for the *other* segments so
//! ANN/Default L0 compaction keeps flowing and writers don't stall. Otherwise it
//! delegates entirely to the inner size-tiered scheduler.
//!
//! [`VectorCompactionSchedulerSupplier`] mirrors
//! [`VectorCompactionFilterSupplier`](super::compaction_filter::VectorCompactionFilterSupplier):
//! it is handed to the storage layer and builds one [`VectorCompactionScheduler`]
//! per compactor, wrapping a fresh size-tiered scheduler built from the same
//! [`CompactorOptions`].
//!
//! [`FieldStats`]: crate::serde::field_stats::FieldStatsValue
//!
//! # Re-triggering while a full compaction is in flight
//!
//! When the policy detects an active compaction already targeting the FTS
//! segment it skips proposing a new one, to avoid log spam and redundant
//! planning. Even without that check SlateDB's own conflict checker rejects a
//! second compaction that reuses the same sources, so correctness does not
//! depend on it. Once the forced compaction applies the deletes (at the last
//! sorted run), the per-field `deletes` counters fall and the next poll clears
//! the trigger.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::memory::{CompactionBudget, CompactionShare};
use crate::serde::Segment;
use crate::storage::VectorDbStorageReadExt;
use crate::storage::segment_extractor::segment_routing_prefix;
use bytes::Bytes;
use common::Storage;
use common::storage::slate::SlateDbStorage;
use slatedb::compactor::{
    Compaction, CompactionRequest, CompactionScheduler, CompactionSchedulerSupplier,
    CompactionSpec, CompactorStateView, SizeTieredCompactionSchedulerSupplier, SourceId,
};
use slatedb::config::CompactorOptions;
use slatedb::{Db, VersionedCompactions};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// In-memory snapshot of the persisted per-field
/// [`FieldStats`](crate::serde::field_stats::FieldStatsValue), describing
/// current FTS delete pressure.
///
/// Shared as an `Arc` between the FieldStats poller (see
/// [`spawn_fts_field_stats_poller`], which refreshes it periodically from
/// storage) and the [`VectorCompactionScheduler`] (which reads it on every
/// `propose()` — `propose()` is synchronous and cannot do KV reads, so the
/// values must already be in memory). The map is replaced wholesale on each
/// poll under a short-lived lock.
#[derive(Debug)]
pub(crate) struct FtsDeleteTracker {
    /// field name -> `(count, deletes)`, where `count` is the total document
    /// count for the field (live + pending-deleted) and `deletes` is the
    /// outstanding (not-yet-compacted) delete count — a subset of `count`.
    // fields: Mutex<HashMap<String, (i64, i64)>>,
    state: Mutex<FtsDeleteTrackerState>,
}

#[derive(Debug)]
pub(crate) enum FtsDeleteTrackerState {
    Initializing,
    CompactionInProgress,
    Compacted { manifest_id: u64 },
    Accumulating(HashMap<String, (i64, i64)>),
}

impl FtsDeleteTracker {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(FtsDeleteTrackerState::Initializing),
        }
    }

    /// Returns a fresh shared tracker.
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub(crate) fn notify_compacting(&self, manifest_id: u64, in_flight: bool) {
        let mut state = self.state.lock().unwrap();
        if in_flight {
            *state = FtsDeleteTrackerState::CompactionInProgress;
        } else {
            match &*state {
                FtsDeleteTrackerState::Initializing => {
                    *state = FtsDeleteTrackerState::Compacted { manifest_id }
                }
                FtsDeleteTrackerState::CompactionInProgress => {
                    *state = FtsDeleteTrackerState::Compacted { manifest_id }
                }
                _ => {}
            }
        }
    }

    /// Replaces the snapshot with the latest per-field `(count, deletes)`.
    pub(crate) fn update_stats(
        &self,
        update_manifest_id: u64,
        fields: HashMap<String, (i64, i64)>,
    ) {
        let mut state = self.state.lock().unwrap();
        match &*state {
            FtsDeleteTrackerState::Compacted { manifest_id, .. }
                if update_manifest_id >= *manifest_id =>
            {
                *state = FtsDeleteTrackerState::Accumulating(fields);
            }
            FtsDeleteTrackerState::Accumulating(_) => {
                *state = FtsDeleteTrackerState::Accumulating(fields);
            }
            _ => {}
        }
    }

    /// Returns `true` when *any* field's outstanding-delete fraction exceeds
    /// `threshold`, i.e. `deletes > threshold * count`. `count` is the field's
    /// total document count (live + pending-deleted), so it is exactly the
    /// `deletes + freq` denominator with `freq = count - deletes` (live docs).
    /// Fields with no outstanding deletes never trigger.
    pub(crate) fn should_force_full_compaction(&self, threshold: f64) -> bool {
        let state = self.state.lock().unwrap();
        let fields = match &*state {
            FtsDeleteTrackerState::Accumulating(fields) => fields,
            _ => return false,
        };
        fields
            .values()
            .any(|&(count, deletes)| field_over_threshold(count, deletes, threshold))
    }
}

/// Pure per-field trigger test: `deletes > threshold * count`.
///
/// `count` is the field's total document count (live + pending-deleted); the
/// indexer only counts documents up and the filter retires them on delete, so
/// `deletes` is a subset of `count`. The fraction `deletes / count` is therefore
/// the share of the field's documents that are pending deletes. A field with no
/// outstanding deletes (or no documents) never triggers.
fn field_over_threshold(count: i64, deletes: i64, threshold: f64) -> bool {
    if deletes <= 0 || count <= 0 {
        return false;
    }
    (deletes as f64) > threshold * (count as f64)
}

/// Spawns a background task that periodically scans the persisted per-field
/// [`FieldStats`](crate::serde::field_stats::FieldStatsValue) from `storage` and
/// refreshes `tracker`, so the (synchronous) compaction scheduler always has an
/// up-to-date in-memory view of per-field delete pressure (RFC-0006).
///
/// Reads are best-effort: a transient error is logged and retried on the next
/// tick. The returned [`JoinHandle`] should be aborted on shutdown (the caller
/// holds it for the database's lifetime).
pub(crate) fn spawn_fts_field_stats_poller(
    storage: Arc<dyn Storage>,
    tracker: Arc<FtsDeleteTracker>,
    poll_interval: Duration,
) -> JoinHandle<()> {
    fn unwrap_slatedb(storage: &dyn Any) -> Arc<Db> {
        let Some(slatedb) = storage.downcast_ref::<SlateDbStorage>() else {
            panic!("storage is not an SlateDbStorage")
        };
        slatedb.db().clone()
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(poll_interval);
        // If a tick is delayed (e.g. a slow scan), skip the backlog rather than
        // firing a burst of catch-up reads.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let db = unwrap_slatedb(storage.as_ref());
            let manifest = db.manifest();
            match storage.scan_field_stats().await {
                Ok(stats) => {
                    let fields = stats
                        .into_iter()
                        .map(|(field, value)| (field, (value.count, value.deletes)))
                        .collect();
                    tracker.update_stats(manifest.id(), fields);
                }
                Err(e) => {
                    debug!(error = %e, "FTS field-stats poll failed; retrying next tick");
                }
            }
        }
    })
}

/// The upload buffer one subcompaction's output holds: `object_store`'s
/// `BufWriter` capacity.
const OUTPUT_BUFFER_BYTES: u64 = 10 * 1024 * 1024;

/// Estimated memory of a compaction run by this process's embedded worker:
/// read-ahead for every input of every subcompaction, and one output upload
/// buffer per subcompaction. SlateDB splits a compaction into one
/// subcompaction per `max_sst_size` of input, up to `max_subcompactions`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CompactionCost {
    read_ahead_per_source: u64,
    max_sst_size: u64,
    max_subcompactions: u64,
}

impl CompactionCost {
    /// The cost model of `options`' embedded worker; `None` when compactions
    /// run in workers outside the process.
    pub(crate) fn from_options(options: &CompactorOptions) -> Option<Self> {
        let worker = options.worker.as_ref()?;
        Some(Self {
            read_ahead_per_source: (worker.bytes_to_fetch as u64)
                .saturating_mul(worker.max_fetch_tasks.max(1) as u64),
            max_sst_size: (worker.max_sst_size as u64).max(1),
            max_subcompactions: worker.max_subcompactions.max(1) as u64,
        })
    }

    pub(crate) fn estimate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> u64 {
        let subcompactions = match input_bytes(state, spec) {
            Some(bytes) => bytes
                .div_ceil(self.max_sst_size)
                .clamp(1, self.max_subcompactions),
            None => self.max_subcompactions,
        };
        let sources = spec.sources().len() as u64;
        subcompactions.saturating_mul(
            sources
                .saturating_mul(self.read_ahead_per_source)
                .saturating_add(OUTPUT_BUFFER_BYTES),
        )
    }
}

/// Estimated bytes of `spec`'s inputs; `None` when its segment or a source is
/// not in the manifest.
fn input_bytes(state: &CompactorStateView, spec: &CompactionSpec) -> Option<u64> {
    let segment = state.manifest().segment(spec.segment())?;
    spec.sources().iter().try_fold(0u64, |total, source| {
        let bytes = match source {
            SourceId::SstView(id) => segment
                .l0()
                .iter()
                .find(|view| view.id == *id)?
                .estimate_size(),
            SourceId::SortedRun(id) => segment
                .compacted()
                .iter()
                .find(|run| run.id == *id)?
                .estimate_size(),
        };
        Some(total.saturating_add(bytes))
    })
}

/// A scheduler's share of the process's compaction budget, and how it costs
/// compactions.
struct CompactionGate {
    cost: CompactionCost,
    share: CompactionShare,
}

impl CompactionGate {
    /// The longest prefix of `specs` that fits the budget beside the
    /// compactions running in this shard and the rest of the process.
    fn admit(
        &self,
        state: &CompactorStateView,
        mut specs: Vec<CompactionSpec>,
    ) -> Vec<CompactionSpec> {
        let running = state.compactions().map_or(0, |compactions| {
            compactions
                .recent_compactions()
                .filter(|c| c.active())
                .map(|c| self.cost.estimate(state, c.spec()))
                .sum()
        });
        let proposed: Vec<u64> = specs
            .iter()
            .map(|spec| self.cost.estimate(state, spec))
            .collect();
        let admitted = self.share.admit(running, &proposed);
        if admitted < specs.len() {
            debug!(
                proposed = specs.len(),
                admitted, running, "deferring compactions until the compaction budget has room"
            );
        }
        specs.truncate(admitted);
        specs
    }
}

/// Custom compaction scheduler that forces a full FTS-segment compaction when
/// any indexed field's delete fraction crosses the configured threshold, and
/// otherwise delegates to a wrapped size-tiered scheduler. See the module docs.
/// With a compaction budget, it proposes only what fits in it.
pub(crate) struct VectorCompactionScheduler {
    inner: Box<dyn CompactionScheduler + Send + Sync>,
    tracker: Arc<FtsDeleteTracker>,
    threshold: f64,
    /// The 3-byte `[subsystem, segment, version]` routing prefix that names the
    /// FTS SlateDB segment (shared with the segment extractor).
    fts_segment_prefix: Bytes,
    gate: Option<CompactionGate>,
}

impl VectorCompactionScheduler {
    fn new(
        inner: Box<dyn CompactionScheduler + Send + Sync>,
        tracker: Arc<FtsDeleteTracker>,
        threshold: f64,
        fts_segment_prefix: Bytes,
        gate: Option<CompactionGate>,
    ) -> Self {
        Self {
            inner,
            tracker,
            threshold,
            fts_segment_prefix,
            gate,
        }
    }

    fn fts_compactions<'a>(
        &self,
        compactions: &'a VersionedCompactions,
    ) -> impl Iterator<Item = &'a Compaction> {
        compactions
            .recent_compactions()
            .filter(|c| c.active())
            .filter(|c| c.spec().segment() == &self.fts_segment_prefix)
    }

    /// Returns true if a compaction already targeting the FTS segment is active
    /// (submitted or running). Best-effort: if the compactions snapshot is
    /// absent we report `false` and let SlateDB's conflict checker dedup.
    fn fts_compaction_in_flight(&self, state: &CompactorStateView) -> bool {
        let Some(compactions) = state.compactions() else {
            return false;
        };
        self.fts_compactions(compactions).next().is_some()
    }

    /// Returns true if there are any active compactions targeting the last SR in the FTS segment
    fn fts_cleanup_compaction_in_flight(&self, state: &CompactorStateView) -> bool {
        let Some(fts_segment) = state.manifest().segment(&self.fts_segment_prefix) else {
            return false;
        };
        let Some(compactions) = state.compactions() else {
            return false;
        };
        let Some(last_sr) = fts_segment.compacted().last() else {
            return false;
        };
        self.fts_compactions(compactions).any(|c| {
            c.spec()
                .sources()
                .contains(&SourceId::SortedRun(last_sr.id))
        })
    }
}

impl CompactionScheduler for VectorCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let specs = self.plan(state);
        match &self.gate {
            Some(gate) => gate.admit(state, specs),
            None => specs,
        }
    }

    fn validate(
        &self,
        state: &CompactorStateView,
        spec: &CompactionSpec,
    ) -> Result<(), slatedb::Error> {
        self.inner.validate(state, spec)
    }
}

impl VectorCompactionScheduler {
    fn plan(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        self.tracker.notify_compacting(
            state.manifest().id(),
            self.fts_cleanup_compaction_in_flight(state),
        );

        if !self.tracker.should_force_full_compaction(self.threshold) {
            return self.inner.propose(state);
        }

        // Don't re-propose while some FTS compaction is already running — it
        // only spams logs; SlateDB would reject the duplicate sources anyway.
        if self.fts_compaction_in_flight(state) {
            debug!("FTS full compaction already in flight; delegating to size-tiered");
            // TODO: this can starve the cleanup compaction
            return self.inner.propose(state);
        }

        let request = CompactionRequest::FullSegment {
            segment: self.fts_segment_prefix.clone(),
        };
        match self.generate(state, &request) {
            Ok(fts_specs) if !fts_specs.is_empty() => {
                debug!(
                    num_specs = fts_specs.len(),
                    "forcing full FTS-segment compaction (a field's deletes are over threshold)"
                );
                // Keep the inner scheduler's work for the OTHER segments so
                // ANN/Default L0 compaction keeps flowing; drop any inner spec
                // that targets the FTS segment to avoid source conflicts with
                // the full compaction we just planned.
                let mut specs: Vec<CompactionSpec> = self
                    .inner
                    .propose(state)
                    .into_iter()
                    .filter(|spec| spec.segment() != &self.fts_segment_prefix)
                    .collect();
                specs.extend(fts_specs);
                specs
            }
            // No sorted runs to merge yet (e.g. all FTS data still in L0) — fall
            // back to size-tiered, which will keep building SRs from L0.
            Ok(_) => self.inner.propose(state),
            Err(e) => {
                warn!(
                    error = %e,
                    "failed to plan full FTS-segment compaction; delegating to size-tiered"
                );
                self.inner.propose(state)
            }
        }
    }
}

/// Per-compactor factory for [`VectorCompactionScheduler`]; mirrors
/// [`VectorCompactionFilterSupplier`](super::compaction_filter::VectorCompactionFilterSupplier).
pub(crate) struct VectorCompactionSchedulerSupplier {
    tracker: Arc<FtsDeleteTracker>,
    threshold: f64,
    fts_segment_prefix: Bytes,
    budget: Option<CompactionBudget>,
}

impl VectorCompactionSchedulerSupplier {
    /// Creates a supplier sharing `tracker`, forcing a full FTS compaction once
    /// any field's delete fraction reaches `threshold`, and keeping the
    /// compactions it schedules within `budget` when there is one.
    pub(crate) fn new(
        tracker: Arc<FtsDeleteTracker>,
        threshold: f64,
        budget: Option<CompactionBudget>,
    ) -> Self {
        Self {
            tracker,
            threshold,
            fts_segment_prefix: segment_routing_prefix(Segment::Fts),
            budget,
        }
    }

    /// Returns a shared `Arc<dyn CompactionSchedulerSupplier>` for handing to
    /// the storage builder's compactor.
    pub(crate) fn shared(
        tracker: Arc<FtsDeleteTracker>,
        threshold: f64,
        budget: Option<CompactionBudget>,
    ) -> Arc<dyn CompactionSchedulerSupplier> {
        Arc::new(Self::new(tracker, threshold, budget))
    }

    /// A gate on a bounded budget for the compactions `options`' embedded
    /// worker runs; remote workers' compactions don't use this process's
    /// memory.
    fn gate(&self, options: &CompactorOptions) -> Option<CompactionGate> {
        let budget = self.budget.as_ref().filter(|budget| budget.is_bounded())?;
        Some(CompactionGate {
            cost: CompactionCost::from_options(options)?,
            share: budget.share(),
        })
    }
}

impl CompactionSchedulerSupplier for VectorCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        // Build the wrapped size-tiered scheduler from the SAME options this
        // supplier receives, so the delegate behaves exactly as the default
        // scheduler would for non-FTS segments.
        let inner = SizeTieredCompactionSchedulerSupplier::new().compaction_scheduler(options);
        Box::new(VectorCompactionScheduler::new(
            inner,
            self.tracker.clone(),
            self.threshold,
            self.fts_segment_prefix.clone(),
            self.gate(options),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_with(fields: &[(&str, i64, i64)]) -> FtsDeleteTracker {
        let tracker = FtsDeleteTracker::new();
        tracker.notify_compacting(0, false);
        tracker.update_stats(
            1,
            fields
                .iter()
                .map(|&(f, count, deletes)| (f.to_string(), (count, deletes)))
                .collect(),
        );
        tracker
    }

    #[test]
    fn should_not_force_when_no_deletes() {
        // given - fields with live docs but no outstanding deletes
        let tracker = tracker_with(&[("body", 100, 0), ("title", 50, 0)]);

        // when / then
        assert!(!tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn should_not_force_below_threshold() {
        // given - 1 delete out of 10 total docs = 10% < 20%
        let tracker = tracker_with(&[("body", 10, 1)]);

        // when / then
        assert!(!tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn should_force_above_threshold() {
        // given - 3 deletes out of 10 total docs = 30% > 20%
        let tracker = tracker_with(&[("body", 10, 3)]);

        // when / then
        assert!(tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn should_force_when_any_field_over_threshold() {
        // given - "body" is fine (1/10 = 10%), "title" is over (1/2 = 50%)
        let tracker = tracker_with(&[("body", 10, 1), ("title", 2, 1)]);

        // when / then - any field over threshold triggers
        assert!(tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn should_not_force_on_uninitialized_tracker() {
        // given - no fields polled yet
        let tracker = FtsDeleteTracker::new();

        // when / then
        assert!(!tracker.should_force_full_compaction(0.20));
        assert!(!tracker.should_force_full_compaction(0.0));
    }

    #[test]
    fn field_over_threshold_correctly_tracks_delete_pct() {
        // deletes > pct * count, where count = total docs (deletes is a subset)
        assert!(field_over_threshold(10, 3, 0.20)); // 3 > 0.2*10
        assert!(!field_over_threshold(10, 1, 0.20)); // 1 !> 0.2*10
        assert!(!field_over_threshold(100, 0, 0.0)); // no deletes -> never
        // boundary: deletes == pct*count is NOT over (strict >)
        assert!(!field_over_threshold(10, 2, 0.20)); // 2 !> 0.2*10
    }

    /// An over-threshold per-field snapshot (3 of 10 docs deleted = 30% > 20%),
    /// used to prove an update *would* trigger a compaction if it were applied —
    /// so a test that still sees `should_force == false` proves it was dropped.
    fn over_threshold() -> HashMap<String, (i64, i64)> {
        HashMap::from([("body".to_string(), (10, 3))])
    }

    #[test]
    fn should_ignore_stats_received_while_initializing() {
        // given - a fresh tracker that has not yet observed a baseline compaction
        let tracker = FtsDeleteTracker::new();

        // when - a poll delivers over-threshold stats before initialization
        tracker.update_stats(10, over_threshold());

        // then - the update is dropped; pre-baseline stats are never acted on
        assert!(!tracker.should_force_full_compaction(0.20));
        assert!(matches!(
            *tracker.state.lock().unwrap(),
            FtsDeleteTrackerState::Initializing
        ));
    }

    #[test]
    fn should_ignore_stats_received_during_compaction() {
        // given - a tracker that has observed an in-flight FTS cleanup compaction
        let tracker = FtsDeleteTracker::new();
        tracker.notify_compacting(5, true);

        // when - a poll delivers over-threshold stats mid-compaction
        tracker.update_stats(10, over_threshold());

        // then - dropped: the running compaction is about to invalidate these
        // stats, so the scheduler must not act on them
        assert!(!tracker.should_force_full_compaction(0.20));
        assert!(matches!(
            *tracker.state.lock().unwrap(),
            FtsDeleteTrackerState::CompactionInProgress
        ));
    }

    #[test]
    fn should_reject_stats_polled_before_the_compaction_manifest() {
        // given - a tracker that observed a cleanup compaction land at manifest 5
        let tracker = FtsDeleteTracker::new();
        *tracker.state.lock().unwrap() = FtsDeleteTrackerState::Compacted { manifest_id: 5 };

        // when - a poll whose snapshot predates that manifest (id 4) arrives. Its
        // stats still reflect the pre-compaction (un-cleaned) state.
        tracker.update_stats(4, over_threshold());

        // then - rejected as stale; the tracker stays Compacted and does not act
        assert!(!tracker.should_force_full_compaction(0.20));
        assert!(matches!(
            *tracker.state.lock().unwrap(),
            FtsDeleteTrackerState::Compacted { manifest_id: 5 }
        ));
    }

    #[test]
    fn should_accept_stats_polled_at_or_after_the_compaction_manifest() {
        // given - a tracker that observed a cleanup compaction land at manifest 5
        let tracker = FtsDeleteTracker::new();
        *tracker.state.lock().unwrap() = FtsDeleteTrackerState::Compacted { manifest_id: 5 };

        // when - a poll exactly at the compaction manifest (boundary: >=) arrives
        tracker.update_stats(5, over_threshold());

        // then - accepted: these stats reflect the post-compaction manifest
        assert!(tracker.should_force_full_compaction(0.20));
        assert!(matches!(
            *tracker.state.lock().unwrap(),
            FtsDeleteTrackerState::Accumulating(_)
        ));
    }

    #[test]
    fn should_stop_using_stats_once_a_compaction_starts() {
        // given - a tracker accumulating over-threshold stats (would force now)
        let tracker = tracker_with(&[("body", 10, 3)]);
        assert!(tracker.should_force_full_compaction(0.20));

        // when - the forced cleanup compaction begins
        tracker.notify_compacting(10, true);

        // then - the accumulated stats are stale (the compaction is rewriting the
        // segment), so they are no longer used...
        assert!(!tracker.should_force_full_compaction(0.20));
        // ...and mid-compaction polls are ignored too
        tracker.update_stats(99, over_threshold());
        assert!(!tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn should_apply_fresh_stats_while_accumulating() {
        // given - an accumulating tracker currently below threshold
        let tracker = tracker_with(&[("body", 10, 1)]);
        assert!(!tracker.should_force_full_compaction(0.20));

        // when - a newer poll shows deletes climbing over threshold
        tracker.update_stats(2, over_threshold());

        // then - fresh stats are applied immediately (no manifest gate while
        // accumulating)
        assert!(tracker.should_force_full_compaction(0.20));
    }

    #[test]
    fn supplier_should_use_fts_segment_prefix() {
        // given / when
        let supplier =
            VectorCompactionSchedulerSupplier::new(FtsDeleteTracker::shared(), 0.20, None);

        // then - the named segment matches the extractor's FTS routing prefix
        assert_eq!(
            supplier.fts_segment_prefix,
            segment_routing_prefix(Segment::Fts)
        );
    }

    #[test]
    fn should_cost_compactions_from_the_embedded_worker_options() {
        // given
        let options = CompactorOptions::default();
        let worker = options.worker.clone().unwrap();

        // when
        let cost = CompactionCost::from_options(&options).unwrap();

        // then
        assert_eq!(
            cost.read_ahead_per_source,
            (worker.bytes_to_fetch * worker.max_fetch_tasks) as u64
        );
        assert_eq!(cost.max_sst_size, worker.max_sst_size as u64);
        assert_eq!(cost.max_subcompactions, worker.max_subcompactions as u64);
    }

    #[test]
    fn should_not_cost_compactions_run_by_remote_workers() {
        let options = CompactorOptions {
            worker: None,
            ..Default::default()
        };
        assert!(CompactionCost::from_options(&options).is_none());
    }

    #[test]
    fn should_gate_schedulers_only_with_a_bounded_budget_and_an_embedded_worker() {
        // given
        let bounded = crate::memory::MemoryBudget::new(&crate::memory::MemoryConfig::default());
        let unbounded = crate::memory::MemoryBudget::unbounded();
        let supplier = |budget: Option<CompactionBudget>| {
            VectorCompactionSchedulerSupplier::new(FtsDeleteTracker::shared(), 0.20, budget)
        };
        let remote = CompactorOptions {
            worker: None,
            ..Default::default()
        };
        let gated = |supplier: VectorCompactionSchedulerSupplier, options: &CompactorOptions| {
            supplier.gate(options).is_some()
        };

        // when / then
        assert!(gated(
            supplier(Some(bounded.compaction().clone())),
            &CompactorOptions::default()
        ));
        assert!(!gated(
            supplier(Some(bounded.compaction().clone())),
            &remote
        ));
        assert!(!gated(
            supplier(Some(unbounded.compaction().clone())),
            &CompactorOptions::default()
        ));
        assert!(!gated(supplier(None), &CompactorOptions::default()));
    }
}
