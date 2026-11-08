//! Compaction for time-segmented SlateDB databases.
//!
//! Every store routes each `(namespace, time segment)` into its own SlateDB
//! segment. Size-tiered compaction only merges runs of similar size, so once
//! a segment stops receiving writes it keeps whatever it had: several sorted
//! runs plus up to `min_compaction_sources - 1` stray L0 SSTs, each with its
//! own copy of most keys. Every cold read of that segment pays for each of
//! them. Expiry has the same gap: records carry a TTL that only compaction
//! enforces, and nothing compacts a segment that has stopped changing.
//!
//! [`SegmentCompaction`] wraps the size-tiered scheduler and adds, per
//! segment whose span it can read from the segment prefix:
//!
//! - **Merge**: [`SegmentCompaction::grace`] after a segment ends, merge its
//!   L0s and sorted runs into one sorted run. Late writes add L0s again,
//!   which are merged into a run of their own once they hold a share of the
//!   segment's bytes or leave it with too many sources, and its runs are
//!   merged into its largest only once the late ones hold half as much. A
//!   trickle of late writes then doesn't rewrite a large segment every few
//!   seconds, and a stream of them, such as a backlog replayed hours late,
//!   is written about four times rather than once per sixteenth it adds.
//!   SlateDB runs one compaction of a segment's L0s at a time, but one of
//!   only its runs beside it, so late L0s keep draining through a rewrite.
//! - **Cap**: until then, whenever size-tiered compaction leaves a segment
//!   more than [`SegmentCompaction::with_max_open_sources`] sources and two
//!   or more L0s, merge its L0s into a fresh run, which bounds what reads of
//!   the newest data probe. Its runs stay size-tiered compaction's: a run
//!   merge chosen by count rewrites a run that grows with the segment, once
//!   per flush. Nor L0s and runs in one merge, which would hold the
//!   segment's L0 through a rewrite of everything while flushes stall.
//! - **Hold**: a rollup copying into a segment holds its merges
//!   ([`MergeHolds`]) and then asks for one, so the segment is rewritten
//!   once rather than every time the copy adds L0s.
//! - **Drain**: once both the segment's end and its newest SST are older
//!   than the retention, drop the segment without rewriting it.
//! - **Supersede**: with a [`Superseded`] hook, drop a segment the hook
//!   reports as superseded (every record it holds is held by another
//!   segment), also without rewriting it.
//!
//! A merge reads every source at once, so it takes at most
//! [`SegmentCompaction::with_max_merge_sources`] of them: past that it
//! merges a segment's oldest L0s into a fresh run, or its newest sources
//! once it has at most one L0, and later polls merge what remains.
//!
//! Every proposal, size-tiered ones included, then passes [`admit`], which
//! holds back the ones SlateDB would reject and all but one of those that
//! finish without a worker; see there for why a second one panics SlateDB.
//! What it passes on runs only while the estimated memory of every
//! compaction of the process fits its [`CompactionMemory`].

mod admit;
mod memory;

pub use memory::{CompactionMemory, DEFAULT_COMPACTION_MEMORY_BYTES};

use memory::{Held, Inputs};

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView,
    SizeTieredCompactionSchedulerSupplier, SourceId,
};
use slatedb::config::CompactorOptions;
use slatedb::manifest::VersionedManifest;

use admit::{Rejection, Tree, admit};

/// The `[start_ms, end_ms)` span of the segment with this prefix, or `None`
/// for a segment that isn't bounded in time.
pub type SegmentSpan = fn(&[u8]) -> Option<(i64, i64)>;

/// Whether the segment with this prefix is superseded, given a test for
/// which segment prefixes are live in the manifest.
pub type Superseded = Arc<dyn Fn(&[u8], &dyn Fn(&[u8]) -> bool) -> bool + Send + Sync>;

/// Default delay between a segment ending and it being merged.
pub const DEFAULT_MERGE_GRACE: Duration = Duration::from_secs(5 * 60);
/// Default [`SegmentCompaction::with_max_open_sources`]. Size-tiered
/// compaction that keeps up leaves an hour of 10-second L0 flushes at most
/// about 10 sources; capping it at 8 costs about one more rewrite of the
/// hour, and lower caps cost several.
pub const DEFAULT_MAX_OPEN_SOURCES: usize = 8;
/// Default [`SegmentCompaction::with_max_merge_sources`]: a rollup's held
/// segment or one compaction fell behind on can have hundreds of sources,
/// each holding its own prefetch while a merge of all of them runs.
pub const DEFAULT_MAX_MERGE_SOURCES: usize = 8;
/// A merged segment's late L0s are merged into a run of their own once they
/// hold this fraction (as a divisor) of its largest source's bytes...
const LATE_L0_BYTES_DIVISOR: u64 = 16;
/// ...and the whole segment once its late L0s and runs hold this fraction,
/// so each rewrite of its largest source is paid for by late bytes of at
/// least half its size...
const LATE_BYTES_DIVISOR: u64 = 2;
/// ...or once they're past the source cap with fewer than two L0s to merge
/// and hold this fraction of it.
const LATE_CAPPED_BYTES_DIVISOR: u64 = 64;

/// Whether a rollup is copying into a segment or waiting for its merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hold {
    Copying,
    Merge,
}

/// Segments, by prefix, whose merges a rollup in this process controls:
/// none while it copies, then one regardless of thresholds.
#[derive(Clone, Debug, Default)]
pub struct MergeHolds(Arc<Mutex<HashMap<Bytes, Hold>>>);

impl MergeHolds {
    /// Holds merges of `prefix` until the guard asks for one or drops.
    pub fn copying(&self, prefix: Bytes) -> MergeHold {
        self.set(&prefix, Some(Hold::Copying));
        MergeHold {
            holds: self.clone(),
            prefix,
        }
    }

    fn get(&self, prefix: &[u8]) -> Option<Hold> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(prefix)
            .copied()
    }

    fn set(&self, prefix: &Bytes, hold: Option<Hold>) {
        let mut holds = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match hold {
            Some(hold) => holds.insert(prefix.clone(), hold),
            None => holds.remove(prefix),
        };
    }
}

/// A rollup's hold on one segment's merges; released on drop.
#[derive(Debug)]
pub struct MergeHold {
    holds: MergeHolds,
    prefix: Bytes,
}

impl MergeHold {
    /// Asks for the segment to be merged into one run.
    pub fn merge(&self) {
        self.holds.set(&self.prefix, Some(Hold::Merge));
    }
}

impl Drop for MergeHold {
    fn drop(&mut self) {
        self.holds.set(&self.prefix, None);
    }
}

/// Whether everything `manifest` held in the segment at `prefix` before
/// `since_ms` is in one source: a lone L0 SST or sorted run, or one run and
/// only L0s flushed since, which are late writes.
pub fn merged_since(manifest: &VersionedManifest, prefix: &[u8], since_ms: u64) -> bool {
    let Some(segment) = manifest
        .segments()
        .iter()
        .find(|segment| segment.prefix().as_ref() == prefix)
    else {
        return true;
    };
    segment.compacted().len() + segment.l0().len() <= 1
        || (segment.compacted().len() <= 1
            && segment
                .l0()
                .iter()
                .all(|view| view.id.timestamp_ms() >= since_ms))
}

/// Segment-aware compaction policy for one database.
#[derive(Clone)]
pub struct SegmentCompaction {
    span: SegmentSpan,
    grace: Duration,
    max_open_sources: usize,
    max_merge_sources: usize,
    retention: Option<Duration>,
    superseded: Option<Superseded>,
    unsealed: Option<Superseded>,
    holds: MergeHolds,
    memory: CompactionMemory,
}

impl std::fmt::Debug for SegmentCompaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCompaction")
            .field("grace", &self.grace)
            .field("max_open_sources", &self.max_open_sources)
            .field("max_merge_sources", &self.max_merge_sources)
            .field("memory", &self.memory.budget())
            .field("retention", &self.retention)
            .field("superseded", &self.superseded.is_some())
            .field("unsealed", &self.unsealed.is_some())
            .finish_non_exhaustive()
    }
}

impl SegmentCompaction {
    pub fn new(span: SegmentSpan) -> Self {
        Self {
            span,
            grace: DEFAULT_MERGE_GRACE,
            max_open_sources: DEFAULT_MAX_OPEN_SOURCES,
            max_merge_sources: DEFAULT_MAX_MERGE_SOURCES,
            retention: None,
            superseded: None,
            unsealed: None,
            holds: MergeHolds::default(),
            memory: CompactionMemory::default(),
        }
    }

    /// Most sources one merge reads at once.
    pub fn with_max_merge_sources(mut self, max_merge_sources: usize) -> Self {
        self.max_merge_sources = max_merge_sources.max(2);
        self
    }

    /// Runs compactions within `memory`, which the process's other databases
    /// share; otherwise within a default budget of this database's own.
    pub fn with_memory(mut self, memory: CompactionMemory) -> Self {
        self.memory = memory;
        self
    }

    /// Shrinks `options`, which the compactor of this policy's database runs
    /// with, until one merge of its most sources fits the memory budget.
    pub fn bound(&self, options: &mut CompactorOptions) {
        self.memory.bound(options, self.max_merge_sources);
    }

    /// Most sources a segment keeps before it's merged, whether or not it
    /// has ended.
    pub fn with_max_open_sources(mut self, max_open_sources: usize) -> Self {
        self.max_open_sources = max_open_sources.max(2);
        self
    }

    /// Uses `holds` for the segments rollups copy into.
    pub fn with_holds(mut self, holds: MergeHolds) -> Self {
        self.holds = holds;
        self
    }

    /// The holds rollups copying into this database's segments take.
    pub fn holds(&self) -> MergeHolds {
        self.holds.clone()
    }

    /// Drops segments `superseded` reports as superseded.
    pub fn with_superseded(mut self, superseded: Superseded) -> Self {
        self.superseded = Some(superseded);
        self
    }

    /// Merges a segment `unsealed` reports as a rollup not yet sealed only
    /// when its rollup asks. A rollup that failed or whose process restarted
    /// has no [`MergeHold`], and rewriting its partial copy would keep the
    /// segment busy while the retried copy adds L0s that only its compaction
    /// can clear, stalling every flush.
    pub fn with_unsealed(mut self, unsealed: Superseded) -> Self {
        self.unsealed = Some(unsealed);
        self
    }

    /// How long after a segment ends before it's merged into one run.
    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Drains segments whose data is older than `retention`; `None` keeps
    /// them forever.
    pub fn with_retention(mut self, retention: Option<Duration>) -> Self {
        self.retention = retention;
        self
    }

    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// Whether `spec` is relief: it reads L0s, which is what SlateDB stalls
    /// flushes on, and only those or those of a segment still taking writes,
    /// unbounded or not yet past its end and grace. A merged segment's late
    /// L0s then drain beside a rewrite of its runs rather than behind it.
    fn relief(&self, spec: &CompactionSpec, now_ms: u64) -> bool {
        if spec.is_drain() || !spec.has_l0_sources() {
            return false;
        }
        if !spec.has_sr_sources() {
            return true;
        }
        let grace_ms = i64::try_from(self.grace.as_millis()).unwrap_or(i64::MAX);
        (self.span)(spec.segment()).is_none_or(|(_, end_ms)| {
            i64::try_from(now_ms).unwrap_or(i64::MAX) < end_ms.saturating_add(grace_ms)
        })
    }

    /// The scheduler supplier for the database at `path`, which labels its
    /// metrics.
    pub fn supplier(&self, path: &str) -> Arc<dyn CompactionSchedulerSupplier> {
        Arc::new(Supplier {
            policy: self.clone(),
            path: path.to_owned(),
        })
    }
}

struct Supplier {
    policy: SegmentCompaction,
    path: String,
}

impl CompactionSchedulerSupplier for Supplier {
    fn compaction_scheduler(
        &self,
        options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(Scheduler {
            inner: SizeTieredCompactionSchedulerSupplier::new().compaction_scheduler(options),
            policy: self.policy.clone(),
            max_concurrent: options.max_concurrent_compactions,
            trivial_moves: options.enable_trivial_move,
            deferred: Mutex::default(),
            path: self.path.clone(),
            cost: memory::Cost::new(options),
            memory: self.policy.memory.holder(),
        })
    }
}

struct Scheduler {
    inner: Box<dyn CompactionScheduler + Send + Sync>,
    policy: SegmentCompaction,
    max_concurrent: usize,
    trivial_moves: bool,
    /// `None` when compactions run in another process.
    cost: Option<memory::Cost>,
    memory: memory::Holder,
    /// Segments whose proposals the last poll deferred, which go first in the
    /// next so that one kind of proposal can't hold back another for good.
    deferred: Mutex<HashSet<Bytes>>,
    path: String,
}

impl CompactionScheduler for Scheduler {
    /// A panic here would close the database, so a proposal that panics is
    /// logged and dropped, and the next poll proposes again.
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        match catch_unwind(AssertUnwindSafe(|| self.admitted(state))) {
            Ok(specs) => specs,
            Err(panic) => {
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                tracing::error!(path = %self.path, %message, "compaction proposal panicked; skipping this poll");
                self.rejected("panic");
                Vec::new()
            }
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

impl Scheduler {
    fn rejected(&self, reason: &'static str) {
        metrics::counter!(
            "storage_compaction_proposals_rejected_total",
            "path" => self.path.clone(),
            "reason" => reason
        )
        .increment(1);
    }

    /// [`Self::proposed`], less what [`admit`] holds back and what doesn't
    /// fit the memory budget beside the compactions already running.
    fn admitted(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let (mut proposed, kinds) = self.proposed(state);
        let mut deferred = self
            .deferred
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        proposed.sort_by_key(|spec| !deferred.contains(spec.segment()));
        deferred.clear();
        let manifest = state.manifest();
        let trees: Vec<Tree> =
            std::iter::once(Tree::new(Bytes::new(), manifest.l0(), manifest.compacted()))
                .chain(manifest.segments().iter().map(|segment| {
                    Tree::new(segment.prefix().clone(), segment.l0(), segment.compacted())
                }))
                .collect();
        let active: Vec<_> = state
            .compactions()
            .into_iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .filter(|compaction| compaction.active())
            .map(|compaction| (compaction.spec().clone(), compaction.status()))
            .collect();
        let (specs, rejected) = admit(&trees, &active, proposed, self.trivial_moves);
        for (spec, rejection) in rejected {
            if rejection == Rejection::Deferred {
                tracing::debug!(path = %self.path, %spec, "deferring compaction to the next poll");
                deferred.insert(spec.segment().clone());
            } else {
                tracing::warn!(path = %self.path, %spec, reason = rejection.label(), "skipping invalid compaction proposal");
            }
            self.rejected(rejection.label());
        }
        let specs = match self.cost {
            Some(cost) => {
                let now_ms = unix_ms(SystemTime::now());
                let running_specs: Vec<_> = state
                    .compactions()
                    .into_iter()
                    .flat_map(|compactions| compactions.recent_compactions())
                    .filter(|compaction| compaction.active())
                    .map(|compaction| {
                        let spec = compaction.spec();
                        let bytes = cost.of(
                            spec,
                            Some(compaction.subcompactions().len()),
                            input_bytes(manifest, spec),
                        );
                        (bytes, self.policy.relief(spec, now_ms))
                    })
                    .collect();
                let running: Held = running_specs
                    .iter()
                    .map(|(bytes, relief)| Held::default().add(*bytes, *relief))
                    .sum();
                let running_merges = running_specs
                    .iter()
                    .filter(|(bytes, relief)| *bytes > 0 && !relief)
                    .count();
                let costed = specs
                    .into_iter()
                    .map(|spec| {
                        let bytes = cost.of(&spec, None, input_bytes(manifest, &spec));
                        let relief = self.policy.relief(&spec, now_ms);
                        Costed {
                            spec,
                            bytes,
                            relief,
                        }
                    })
                    .collect();
                let others = self.memory.others();
                let held = Held {
                    total: running.total + others.total,
                    merges: running.merges + others.merges,
                };
                let memory = self.memory.memory();
                let limits = Limits {
                    budget: memory.budget(),
                    merge_budget: memory.merge_budget(),
                    merge_slots: self
                        .max_concurrent
                        .saturating_sub(1)
                        .max(1)
                        .saturating_sub(running_merges),
                };
                let (specs, over, admitted) = within_budget(costed, held, limits);
                for spec in over {
                    tracing::debug!(path = %self.path, %spec, "deferring compaction until the memory budget has room");
                    deferred.insert(spec.segment().clone());
                    self.rejected("memory");
                }
                self.memory.set(Held {
                    total: running.total + admitted.total,
                    merges: running.merges + admitted.merges,
                });
                specs
            }
            None => specs,
        };
        for spec in &specs {
            if let Some(kind) = kinds.get(spec.segment()) {
                metrics::counter!(
                    "storage_segment_compactions_total",
                    "path" => self.path.clone(),
                    "kind" => *kind
                )
                .increment(1);
            }
        }
        specs
    }

    /// The size-tiered proposals plus this policy's, and the kind of each of
    /// this policy's, by segment.
    fn proposed(
        &self,
        state: &CompactorStateView,
    ) -> (Vec<CompactionSpec>, HashMap<Bytes, &'static str>) {
        let now_ms = unix_ms(SystemTime::now());
        let mut specs = self.inner.propose(state);
        let active: Vec<_> = state
            .compactions()
            .into_iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .filter(|compaction| compaction.active())
            .collect();
        let oldest_active_ms = active
            .iter()
            .map(|compaction| compaction.id().timestamp_ms())
            .min();
        metrics::gauge!(
            "storage_compaction_oldest_active_seconds",
            "path" => self.path.clone()
        )
        .set(oldest_active_ms.map_or(0.0, |started| {
            now_ms.saturating_sub(started) as f64 / 1000.0
        }));

        let slots = self
            .max_concurrent
            .saturating_sub(active.len() + specs.len());
        let mut kinds = HashMap::new();
        if slots == 0 {
            return (specs, kinds);
        }
        let mut busy: HashMap<Bytes, Busy> = HashMap::new();
        for spec in active
            .iter()
            .map(|compaction| compaction.spec())
            .chain(specs.iter())
        {
            let segment = busy.entry(spec.segment().clone()).or_default();
            segment.l0 |= spec.is_drain() || spec.has_l0_sources();
            segment.runs |= spec.is_drain() || spec.has_sr_sources();
        }
        let manifest = state.manifest();
        let segments: Vec<SegmentState> = manifest
            .segments()
            .iter()
            .map(|segment| {
                let created = || {
                    segment
                        .l0()
                        .iter()
                        .chain(segment.compacted().iter().flat_map(|run| run.sst_views()))
                        .map(|view| view.id.timestamp_ms())
                };
                let sizes = segment
                    .l0()
                    .iter()
                    .map(|view| view.estimate_size())
                    .chain(segment.compacted().iter().map(|run| run.estimate_size()));
                SegmentState {
                    prefix: segment.prefix().clone(),
                    l0: segment.l0().iter().map(|view| view.id).collect(),
                    runs: segment.compacted().iter().map(|run| run.id).collect(),
                    newest_ms: created().max().unwrap_or(0),
                    oldest_ms: created().min().unwrap_or(0),
                    bytes: sizes.clone().sum(),
                    l0_bytes: segment.l0().iter().map(|view| view.estimate_size()).sum(),
                    largest: sizes.max().unwrap_or(0),
                }
            })
            .collect();
        let committed = manifest
            .compacted()
            .iter()
            .chain(
                manifest
                    .segments()
                    .iter()
                    .flat_map(|segment| segment.compacted()),
            )
            .map(|run| run.id);
        let in_flight = active
            .iter()
            .map(|compaction| compaction.spec())
            .chain(specs.iter())
            .filter_map(CompactionSpec::destination);
        let next_run = committed.chain(in_flight).max().map_or(0, |id| id + 1);

        for planned in plan(&self.policy, now_ms, &segments, &busy, slots, next_run) {
            let kind = match planned {
                Planned::Merge { .. } => "merge",
                Planned::Drain {
                    superseded: false, ..
                } => "drain",
                Planned::Drain {
                    superseded: true, ..
                } => "supersede",
            };
            let spec = planned.into_spec();
            kinds.insert(spec.segment().clone(), kind);
            specs.push(spec);
        }
        (specs, kinds)
    }
}

/// What the planner needs to know about one segment.
#[derive(Clone, Debug)]
struct SegmentState {
    prefix: Bytes,
    /// L0 view ids, newest first, as the manifest orders them.
    l0: Vec<ulid::Ulid>,
    /// Sorted-run ids, as the manifest orders them.
    runs: Vec<u32>,
    /// Creation time of the segment's newest SST, which is no older than
    /// any write it holds.
    newest_ms: u64,
    /// Creation time of the segment's oldest SST. Once a merge rewrites
    /// the segment every SST it has is newer than the merge's grace.
    oldest_ms: u64,
    /// Estimated bytes of all its SSTs, of its L0s and of its largest source.
    bytes: u64,
    l0_bytes: u64,
    largest: u64,
}

impl SegmentState {
    fn sources(&self) -> Vec<SourceId> {
        self.l0
            .iter()
            .map(|id| SourceId::SstView(*id))
            .chain(self.runs.iter().map(|id| SourceId::SortedRun(*id)))
            .collect()
    }
}

#[derive(Debug, PartialEq)]
enum Planned {
    Merge {
        prefix: Bytes,
        sources: Vec<SourceId>,
        destination: u32,
    },
    Drain {
        prefix: Bytes,
        sources: Vec<SourceId>,
        /// Dropped because it is superseded rather than expired.
        superseded: bool,
    },
}

impl Planned {
    fn into_spec(self) -> CompactionSpec {
        match self {
            Planned::Merge {
                prefix,
                sources,
                destination,
            } => CompactionSpec::for_segment(prefix, sources, destination),
            Planned::Drain {
                prefix, sources, ..
            } => CompactionSpec::drain_segment(prefix, sources),
        }
    }
}

/// Which of a segment's sources its active compactions take. SlateDB runs
/// one compaction of a segment's L0s at a time, but one of its runs alone
/// can run beside it.
#[derive(Clone, Copy, Default)]
struct Busy {
    l0: bool,
    runs: bool,
}

/// Plans at most `slots` drains and merges, drains first and oldest segment
/// first. `next_run` is the first sorted-run id free for a new run.
fn plan(
    policy: &SegmentCompaction,
    now_ms: u64,
    segments: &[SegmentState],
    busy: &HashMap<Bytes, Busy>,
    slots: usize,
    mut next_run: u32,
) -> Vec<Planned> {
    let now_ms = i64::try_from(now_ms).unwrap_or(i64::MAX);
    let live = |segment: &&SegmentState| !segment.l0.is_empty() || !segment.runs.is_empty();
    let present: HashSet<&[u8]> = segments
        .iter()
        .filter(live)
        .map(|segment| segment.prefix.as_ref())
        .collect();
    let is_present = |prefix: &[u8]| present.contains(prefix);
    let superseded = |segment: &SegmentState| {
        policy
            .superseded
            .as_ref()
            .is_some_and(|superseded| superseded(&segment.prefix, &is_present))
    };
    let busy = |segment: &SegmentState| busy.get(&segment.prefix).copied().unwrap_or_default();
    let mut candidates: Vec<(i64, bool, &SegmentState)> = segments
        .iter()
        .filter(|segment| {
            let busy = busy(segment);
            !busy.l0 || !busy.runs
        })
        .filter(live)
        .filter_map(|segment| {
            let end_ms = (policy.span)(&segment.prefix)?.1;
            Some((end_ms, superseded(segment), segment))
        })
        .collect();
    candidates.sort_by_key(|(end_ms, _, _)| *end_ms);

    let expired = |end_ms: i64, segment: &SegmentState| {
        policy.retention.is_some_and(|retention| {
            let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
            let newest_ms = i64::try_from(segment.newest_ms).unwrap_or(i64::MAX);
            now_ms >= end_ms.saturating_add(retention_ms)
                && now_ms >= newest_ms.saturating_add(retention_ms)
        })
    };
    let unsealed = |segment: &SegmentState| {
        policy
            .unsealed
            .as_ref()
            .is_some_and(|unsealed| unsealed(&segment.prefix, &is_present))
    };
    let grace_ms = i64::try_from(policy.grace.as_millis()).unwrap_or(i64::MAX);
    // A merged segment's late sources, once past its grace: its runs into its
    // largest once they're worth rewriting it, its L0s into a fresh run once
    // they're worth a run of their own, so neither waits on the other.
    let late = |end_ms: i64, segment: &SegmentState, busy: Busy| {
        let merged_at = i64::try_from(segment.oldest_ms).unwrap_or(i64::MAX);
        if now_ms < end_ms.saturating_add(grace_ms) || merged_at < end_ms.saturating_add(grace_ms) {
            return None;
        }
        let over_cap = segment.l0.len() + segment.runs.len() > policy.max_open_sources;
        let late_runs = segment
            .bytes
            .saturating_sub(segment.l0_bytes)
            .saturating_sub(segment.largest);
        let runs_due = segment.runs.len() >= 2
            && (late_runs.saturating_mul(LATE_BYTES_DIVISOR) >= segment.largest
                || (over_cap
                    && segment.l0.len() < 2
                    && late_runs.saturating_mul(LATE_CAPPED_BYTES_DIVISOR) >= segment.largest));
        let l0_due = segment.l0.len() >= 2
            && (over_cap
                || segment.l0_bytes.saturating_mul(LATE_L0_BYTES_DIVISOR) >= segment.largest);
        if runs_due && !busy.runs {
            Some(Merge::LateRuns)
        } else if l0_due && !busy.l0 {
            Some(Merge::LateL0s)
        } else {
            None
        }
    };
    // A segment past the source cap while it still takes writes: its L0s
    // into a fresh run. Its runs are left to size-tiered compaction, which
    // merges runs of similar size: any run merge picked by count rewrites a
    // run that grows with the segment, so an hour of writes would be
    // rewritten once per flush. And never L0s and runs at once, which would
    // hold the segment's L0 until it rewrites everything while flushes stall
    // on L0.
    let open = |segment: &SegmentState, busy: Busy| {
        (segment.l0.len() >= 2 && !busy.l0).then_some(Merge::LateL0s)
    };
    let requested = |segment: &SegmentState| policy.holds.get(&segment.prefix) == Some(Hold::Merge);
    let mergeable = |end_ms: i64, segment: &SegmentState| {
        let sources = segment.l0.len() + segment.runs.len();
        let busy = busy(segment);
        if sources <= 1 {
            return None;
        }
        let idle = !busy.l0 && !busy.runs;
        match policy.holds.get(&segment.prefix) {
            Some(Hold::Copying) => return None,
            Some(Hold::Merge) => return idle.then_some(Merge::All),
            None if unsealed(segment) => return None,
            None => {}
        }
        let merge_at = end_ms.saturating_add(grace_ms);
        if now_ms < merge_at {
            return (sources > policy.max_open_sources)
                .then(|| open(segment, busy))
                .flatten();
        }
        if !idle {
            return late(end_ms, segment, busy);
        }
        if i64::try_from(segment.oldest_ms).unwrap_or(i64::MAX) < merge_at {
            return Some(Merge::All);
        }
        late(end_ms, segment, busy)
    };

    let drains = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| {
            let busy = busy(segment);
            !busy.l0 && !busy.runs && (*superseded || expired(*end_ms, segment))
        })
        .map(|(end_ms, superseded, segment)| Planned::Drain {
            prefix: segment.prefix.clone(),
            sources: segment.sources(),
            superseded: *superseded && !expired(*end_ms, segment),
        });
    // A rollup waits on its requested merge, so those go first.
    let (requested, routine): (Vec<_>, Vec<_>) = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| !superseded && !expired(*end_ms, segment))
        .filter_map(|(end_ms, _, segment)| Some((mergeable(*end_ms, segment)?, *segment)))
        .partition(|(_, segment)| requested(segment));
    let merges = requested
        .into_iter()
        .chain(routine)
        .map(|(merge, segment)| {
            let (sources, runs) = match merge {
                Merge::All => merge_window(segment, policy.max_merge_sources),
                Merge::LateL0s => late_l0s(segment, policy.max_merge_sources),
                Merge::LateRuns => late_runs(segment, policy.max_merge_sources),
            };
            let destination = match runs.iter().min() {
                Some(id) => *id,
                None => {
                    next_run += 1;
                    next_run - 1
                }
            };
            Planned::Merge {
                prefix: segment.prefix.clone(),
                sources,
                destination,
            }
        });
    drains.chain(merges).take(slots).collect()
}

/// What a segment's next merge takes.
#[derive(Clone, Copy)]
enum Merge {
    /// Every source, within [`merge_window`].
    All,
    /// Its oldest L0s, at most `cap`, into a fresh run, leaving its runs.
    LateL0s,
    /// Its oldest runs, at most `cap`, into the oldest, leaving its L0s.
    LateRuns,
}

/// The oldest `cap` of `segment`'s L0s, which SlateDB takes as a window
/// since L0s precede runs in its newest-to-oldest order.
fn late_l0s(segment: &SegmentState, cap: usize) -> (Vec<SourceId>, &[u32]) {
    let sources = segment.sources();
    let l0 = segment.l0.len();
    (sources[l0 - l0.min(cap)..l0].to_vec(), &[])
}

/// The oldest `cap` of `segment`'s runs, which end its newest-to-oldest order.
fn late_runs(segment: &SegmentState, cap: usize) -> (Vec<SourceId>, &[u32]) {
    let sources = segment.sources();
    let runs = &segment.runs[segment.runs.len() - segment.runs.len().min(cap)..];
    (sources[sources.len() - runs.len()..].to_vec(), runs)
}

/// The sources of `segment`'s next merge, and the sorted runs among them:
/// all of them when they number at most `cap`. Otherwise a window SlateDB
/// accepts, consecutive in its newest-to-oldest order: its oldest `cap` L0s
/// while it has two or more, which go into a fresh run, then its newest
/// `cap` sources, which go into the oldest run among them. Each pass
/// rewrites only its window, so a segment of `n` sources is rewritten about
/// `log_cap(n)` times rather than read through `n` iterators at once.
fn merge_window(segment: &SegmentState, cap: usize) -> (Vec<SourceId>, &[u32]) {
    let sources = segment.sources();
    let l0 = segment.l0.len();
    if sources.len() <= cap {
        (sources, &segment.runs)
    } else if l0 >= 2 {
        (sources[l0 - l0.min(cap)..l0].to_vec(), &[])
    } else {
        (sources[..cap].to_vec(), &segment.runs[..cap - l0])
    }
}

/// A proposal, its estimated bytes, and whether it is relief.
struct Costed {
    spec: CompactionSpec,
    bytes: u64,
    relief: bool,
}

/// What [`within_budget`] admits within.
#[derive(Clone, Copy, Debug)]
struct Limits {
    budget: u64,
    merge_budget: u64,
    /// Merges that may start beside the running ones, which leave
    /// SlateDB's last compaction slot to relief.
    merge_slots: usize,
}

/// The `specs` that fit beside the `held` bytes of compactions already
/// running, in order, then the rest, and the bytes of those that fit.
/// Relief fits within the whole budget; a merge also within the merge
/// budget and a free merge slot. With nothing held the first always runs,
/// however large, so compaction never stalls on one that alone exceeds the
/// budget, and drains, which read nothing, always run.
fn within_budget(
    specs: Vec<Costed>,
    mut held: Held,
    limits: Limits,
) -> (Vec<CompactionSpec>, Vec<CompactionSpec>, Held) {
    let (mut fit, mut over, mut admitted) = (Vec::new(), Vec::new(), Held::default());
    let mut merge_slots = limits.merge_slots;
    for Costed {
        spec,
        bytes,
        relief,
    } in specs
    {
        let fits = bytes == 0
            || held.total == 0
            || (held.total.saturating_add(bytes) <= limits.budget
                && (relief
                    || (merge_slots > 0
                        && held.merges.saturating_add(bytes) <= limits.merge_budget)));
        if fits {
            if !relief && bytes > 0 {
                merge_slots = merge_slots.saturating_sub(1);
            }
            held = held.add(bytes, relief);
            admitted = admitted.add(bytes, relief);
            fit.push(spec);
        } else {
            over.push(spec);
        }
    }
    (fit, over, admitted)
}

/// Estimated sizes of `spec`'s input SSTs; `None` when its segment or a
/// source isn't in `manifest`.
fn input_bytes(manifest: &VersionedManifest, spec: &CompactionSpec) -> Option<Inputs> {
    let (l0, runs) = if spec.segment().is_empty() {
        (manifest.l0(), manifest.compacted())
    } else {
        let segment = manifest
            .segments()
            .iter()
            .find(|segment| segment.prefix() == spec.segment())?;
        (segment.l0(), segment.compacted())
    };
    spec.sources()
        .iter()
        .try_fold(Inputs::default(), |inputs, source| {
            let (bytes, largest) = match source {
                SourceId::SstView(id) => {
                    let size = l0.iter().find(|view| view.id == *id)?.estimate_size();
                    (size, size)
                }
                SourceId::SortedRun(id) => {
                    let run = runs.iter().find(|run| run.id == *id)?;
                    let largest = run.sst_views().iter().map(|view| view.estimate_size()).max();
                    (run.estimate_size(), largest.unwrap_or(0))
                }
            };
            Some(Inputs {
                total: inputs.total.saturating_add(bytes),
                largest: inputs.largest.max(largest),
            })
        })
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR_MS: i64 = 3_600_000;

    /// Prefixes are `[start hour]`; `0xff` is unbounded.
    fn span(prefix: &[u8]) -> Option<(i64, i64)> {
        let hour = i64::from(*prefix.first()?);
        (hour != 0xff).then_some((hour * HOUR_MS, (hour + 1) * HOUR_MS))
    }

    fn segment(hour: u8, l0: usize, runs: &[u32], newest_ms: u64) -> SegmentState {
        SegmentState {
            prefix: Bytes::copy_from_slice(&[hour]),
            l0: (0..l0)
                .map(|index| ulid::Ulid::from_parts(index as u64, 0))
                .collect(),
            runs: runs.to_vec(),
            newest_ms,
            oldest_ms: 0,
            bytes: (l0 + runs.len()) as u64,
            l0_bytes: l0 as u64,
            largest: 1,
        }
    }

    fn policy() -> SegmentCompaction {
        SegmentCompaction::new(span).with_grace(Duration::from_secs(30 * 60))
    }

    fn at_hour(hour: f64) -> u64 {
        (hour * HOUR_MS as f64) as u64
    }

    fn prefixes(planned: &[Planned]) -> Vec<(&'static str, u8)> {
        planned
            .iter()
            .map(|planned| match planned {
                Planned::Merge { prefix, .. } => ("merge", prefix[0]),
                Planned::Drain { prefix, .. } => ("drain", prefix[0]),
            })
            .collect()
    }

    #[test]
    fn merges_a_segment_once_its_grace_has_passed() {
        let segments = [segment(1, 2, &[4, 7], 0)];
        let none = HashMap::new();
        assert!(plan(&policy(), at_hour(2.4), &segments, &none, 4, 10).is_empty());
        assert_eq!(
            plan(&policy(), at_hour(2.5), &segments, &none, 4, 10),
            vec![Planned::Merge {
                prefix: Bytes::from_static(&[1]),
                sources: segments[0].sources(),
                destination: 4,
            }]
        );
    }

    #[test]
    fn merging_l0s_alone_takes_a_fresh_run() {
        let segments = [segment(1, 2, &[], 0), segment(2, 3, &[], 0)];
        let planned = plan(&policy(), at_hour(10.0), &segments, &HashMap::new(), 4, 10);
        let destinations: Vec<_> = planned
            .iter()
            .map(|planned| match planned {
                Planned::Merge { destination, .. } => *destination,
                Planned::Drain { .. } => unreachable!(),
            })
            .collect();
        assert_eq!(destinations, vec![10, 11]);
    }

    #[test]
    fn skips_single_run_busy_and_unbounded_segments() {
        let segments = [
            segment(1, 0, &[3], 0),
            segment(2, 1, &[], 0),
            segment(3, 2, &[5], 0),
            segment(0xff, 4, &[6], 0),
        ];
        let busy = HashMap::from([(
            Bytes::from_static(&[3]),
            Busy {
                l0: true,
                runs: true,
            },
        )]);
        assert!(plan(&policy(), at_hour(100.0), &segments, &busy, 4, 10).is_empty());
    }

    #[test]
    fn drains_only_once_both_the_segment_and_its_newest_write_expire() {
        let policy = policy().with_retention(Some(Duration::from_secs(2 * 3_600)));
        let segments = [
            segment(1, 0, &[3], at_hour(1.5)),
            segment(2, 0, &[4], at_hour(5.0)),
        ];
        let none = HashMap::new();
        // Hour 1 ends at 2h; its newest SST is from 1.5h: expires at 4h.
        assert!(plan(&policy, at_hour(3.9), &segments, &none, 4, 10).is_empty());
        assert_eq!(
            prefixes(&plan(&policy, at_hour(4.0), &segments, &none, 4, 10)),
            vec![("drain", 1)]
        );
        // Hour 2 ends at 3h but was rewritten at 5h: expires at 7h.
        assert_eq!(
            prefixes(&plan(&policy, at_hour(6.9), &segments, &none, 4, 10)),
            vec![("drain", 1)]
        );
        assert_eq!(
            prefixes(&plan(&policy, at_hour(7.0), &segments, &none, 4, 10)),
            vec![("drain", 1), ("drain", 2)]
        );
    }

    /// Prefix `[hour]` is superseded once a seal `[hour | 0x80]` is live.
    fn sealed(prefix: &[u8], present: &dyn Fn(&[u8]) -> bool) -> bool {
        prefix
            .first()
            .is_some_and(|hour| *hour < 0x80 && present(&[hour | 0x80]))
    }

    #[test]
    fn drops_superseded_segments_instead_of_merging_them() {
        let policy = policy().with_superseded(Arc::new(sealed));
        let segments = [
            segment(1, 2, &[3], 0),
            segment(2, 2, &[4], 0),
            segment(0x81, 1, &[], 0),
        ];
        let planned = plan(&policy, at_hour(10.0), &segments, &HashMap::new(), 4, 10);
        assert_eq!(prefixes(&planned), vec![("drain", 1), ("merge", 2)]);
        assert!(matches!(
            planned[0],
            Planned::Drain {
                superseded: true,
                ..
            }
        ));

        let busy = HashMap::from([(
            Bytes::from_static(&[1]),
            Busy {
                l0: true,
                runs: true,
            },
        )]);
        let planned = plan(&policy, at_hour(10.0), &segments, &busy, 4, 10);
        assert_eq!(prefixes(&planned), vec![("merge", 2)]);
    }

    #[test]
    fn drains_first_then_oldest_merges_within_slots() {
        let policy = policy().with_retention(Some(Duration::from_secs(24 * 3_600)));
        let segments = [
            segment(30, 2, &[1], at_hour(31.0)),
            segment(10, 2, &[2], at_hour(11.0)),
            segment(1, 0, &[3], at_hour(2.0)),
            segment(20, 2, &[4], at_hour(21.0)),
        ];
        assert_eq!(
            prefixes(&plan(
                &policy,
                at_hour(34.0),
                &segments,
                &HashMap::new(),
                3,
                10
            )),
            vec![("drain", 1), ("merge", 10), ("merge", 20)]
        );
    }

    #[test]
    fn merges_an_open_segment_only_past_the_source_cap() {
        let policy = policy().with_max_open_sources(8);
        let none = HashMap::new();
        let at_cap = [segment(1, 4, &[5, 4, 3, 2], 0)];
        assert!(plan(&policy, at_hour(1.5), &at_cap, &none, 4, 10).is_empty());
        // Over it, the L0s into a fresh run, leaving the runs alone.
        let over = segment(1, 5, &[5, 4, 3, 2], 0);
        let sources = over.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(1.5), &[over], &none, 4, 10)),
            vec![(sources[..5].to_vec(), 10)]
        );
        // With one L0, nothing: its runs are size-tiered compaction's.
        let runs = segment(1, 1, &[9, 8, 7, 6, 5, 4, 3, 2], 0);
        assert!(plan(&policy, at_hour(1.5), &[runs], &none, 4, 10).is_empty());
    }

    fn merged(planned: &[Planned]) -> Vec<(Vec<SourceId>, u32)> {
        planned
            .iter()
            .map(|planned| match planned {
                Planned::Merge {
                    sources,
                    destination,
                    ..
                } => (sources.clone(), *destination),
                Planned::Drain { .. } => unreachable!(),
            })
            .collect()
    }

    #[test]
    fn merges_a_held_segment_of_many_sources_a_window_at_a_time() {
        let policy = policy().with_max_merge_sources(4);
        let none = HashMap::new();
        // Ten L0s and two runs: the four oldest L0s into a fresh run.
        let many = segment(1, 10, &[9, 3], 0);
        let sources = many.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(10.0), &[many], &none, 4, 10)),
            vec![(sources[6..10].to_vec(), 10)]
        );
        // One L0 and five runs: the L0 and the three newest runs, into the
        // oldest of them.
        let runs = segment(1, 1, &[8, 7, 6, 5, 4], 0);
        let sources = runs.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(10.0), &[runs], &none, 4, 10)),
            vec![(sources[..4].to_vec(), 6)]
        );
        // Within the cap, everything into the oldest run, as before.
        let few = segment(1, 2, &[5, 4], 0);
        let sources = few.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(10.0), &[few], &none, 4, 10)),
            vec![(sources, 4)]
        );
    }

    /// Hour 1, merged past its grace into run 3 of `base` bytes, since given late
    /// L0s of `l0_bytes` and late runs `late_runs` of `run_bytes` each.
    fn merged_late(l0: usize, l0_bytes: u64, late_runs: &[u32], run_bytes: u64) -> SegmentState {
        let base = 1_600;
        SegmentState {
            oldest_ms: at_hour(2.6),
            bytes: base + l0_bytes + run_bytes * late_runs.len() as u64,
            l0_bytes,
            largest: base,
            ..segment(1, l0, &[late_runs, &[3]].concat(), at_hour(3.0))
        }
    }

    #[test]
    fn merges_late_l0s_into_a_run_of_their_own_past_a_sixteenth() {
        let none = HashMap::new();
        let below = merged_late(3, 99, &[], 0);
        assert!(plan(&policy(), at_hour(10.0), &[below], &none, 4, 10).is_empty());
        let past = merged_late(3, 100, &[7], 100);
        let sources = past.sources();
        assert_eq!(
            merged(&plan(&policy(), at_hour(10.0), &[past], &none, 4, 10)),
            vec![(sources[..3].to_vec(), 10)]
        );
    }

    #[test]
    fn rewrites_a_merged_segments_runs_once_late_runs_reach_half_of_it() {
        let none = HashMap::new();
        // Late L0s don't count, as they don't go into the rewrite.
        let below = merged_late(1, 800, &[7, 6, 5, 4], 199);
        assert!(plan(&policy(), at_hour(10.0), &[below], &none, 4, 10).is_empty());
        let half = merged_late(1, 0, &[7, 6, 5, 4], 200);
        let sources = half.sources();
        assert_eq!(
            merged(&plan(&policy(), at_hour(10.0), &[half], &none, 4, 10)),
            vec![(sources[1..].to_vec(), 3)]
        );
    }

    #[test]
    fn merges_late_l0s_and_runs_beside_each_other() {
        let late = merged_late(2, 100, &[7, 6, 5, 4], 200);
        let sources = late.sources();
        let busy = |l0, runs| HashMap::from([(late.prefix.clone(), Busy { l0, runs })]);
        let planned = |busy| {
            merged(&plan(
                &policy(),
                at_hour(10.0),
                std::slice::from_ref(&late),
                &busy,
                4,
                10,
            ))
        };
        assert_eq!(planned(busy(true, false)), vec![(sources[2..].to_vec(), 3)]);
        assert_eq!(
            planned(busy(false, true)),
            vec![(sources[..2].to_vec(), 10)]
        );
        assert!(planned(busy(true, true)).is_empty());
        // Nor beside a segment's first merge, which takes all its sources.
        let unmerged = SegmentState {
            oldest_ms: 0,
            ..late.clone()
        };
        let busy = HashMap::from([(
            late.prefix.clone(),
            Busy {
                l0: false,
                runs: true,
            },
        )]);
        assert!(plan(&policy(), at_hour(10.0), &[unmerged], &busy, 4, 10).is_empty());
    }

    #[test]
    fn bounds_a_merged_segment_past_the_source_cap_by_its_late_l0s() {
        let policy = policy().with_max_open_sources(4);
        let none = HashMap::new();
        let over = merged_late(2, 2, &[7, 6], 10);
        let sources = over.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(10.0), &[over], &none, 4, 10)),
            vec![(sources[..2].to_vec(), 10)]
        );
        // With one L0 left, its runs once their late bytes are worth it.
        let runs = merged_late(1, 1, &[7, 6, 5], 10);
        let sources = runs.sources();
        assert_eq!(
            merged(&plan(&policy, at_hour(10.0), &[runs], &none, 4, 10)),
            vec![(sources[1..].to_vec(), 3)]
        );
    }

    fn costed(run: u32, bytes: u64, relief: bool) -> Costed {
        Costed {
            spec: CompactionSpec::new(vec![SourceId::SortedRun(run)], run),
            bytes,
            relief,
        }
    }

    const LIMITS: Limits = Limits {
        budget: 100,
        merge_budget: 75,
        merge_slots: 3,
    };

    fn held(total: u64, merges: u64) -> Held {
        Held { total, merges }
    }

    #[test]
    fn admits_compactions_while_they_fit_and_one_when_nothing_runs() {
        let relief = |run| costed(run, 40, true);
        let (fit, over, admitted) =
            within_budget(vec![relief(1), relief(2), relief(3)], held(10, 0), LIMITS);
        assert_eq!((fit.len(), over.len(), admitted), (2, 1, held(80, 0)));
        // Alone, a compaction larger than the budget still runs.
        let (fit, over, admitted) =
            within_budget(vec![costed(1, 500, false), relief(2)], held(0, 0), LIMITS);
        assert_eq!((fit.len(), over.len(), admitted), (1, 1, held(500, 500)));
        // Beside running ones it waits, though drains hold nothing.
        let drain = Costed {
            spec: CompactionSpec::drain_segment(Bytes::from_static(&[1]), Vec::new()),
            bytes: 0,
            relief: false,
        };
        let (fit, over, _) = within_budget(vec![relief(1), drain], held(90, 0), LIMITS);
        assert_eq!((fit.len(), over.len()), (1, 1));
        assert!(fit[0].is_drain());
    }

    #[test]
    fn merges_leave_relief_its_share_and_a_slot() {
        // Merges stop at the merge budget, relief takes the rest.
        let (fit, over, admitted) = within_budget(
            vec![
                costed(1, 40, false),
                costed(2, 40, false),
                costed(3, 20, true),
            ],
            held(10, 10),
            LIMITS,
        );
        assert_eq!((fit.len(), over.len()), (2, 1));
        assert_eq!(admitted, held(60, 40));
        // A large merge running doesn't hold relief back.
        let (fit, _, admitted) = within_budget(vec![costed(1, 25, true)], held(75, 75), LIMITS);
        assert_eq!((fit.len(), admitted), (1, held(25, 0)));
        // Merges stop at their slots, however small.
        let limits = Limits {
            merge_slots: 1,
            ..LIMITS
        };
        let (fit, over, _) = within_budget(
            vec![costed(1, 1, false), costed(2, 1, false), costed(3, 1, true)],
            held(1, 0),
            limits,
        );
        assert_eq!((fit.len(), over.len()), (2, 1));
    }

    #[test]
    fn relief_is_l0s_alone_or_of_a_segment_still_taking_writes() {
        let policy = policy();
        let l0 = vec![SourceId::SstView(ulid::Ulid::from_parts(1, 0))];
        let runs = vec![SourceId::SortedRun(1), SourceId::SortedRun(2)];
        let both = [l0.clone(), runs.clone()].concat();
        let hour = |sources: &Vec<SourceId>| {
            CompactionSpec::for_segment(Bytes::from_static(&[1]), sources.clone(), 1)
        };
        // Hour 1 ends at 2h and its grace at 2.5h.
        assert!(policy.relief(&hour(&both), at_hour(1.5)));
        assert!(policy.relief(&hour(&both), at_hour(2.4)));
        assert!(!policy.relief(&hour(&both), at_hour(2.5)));
        assert!(policy.relief(&hour(&l0), at_hour(2.5)));
        assert!(!policy.relief(&hour(&runs), at_hour(1.5)));
        let unbounded = CompactionSpec::for_segment(Bytes::from_static(&[0xff]), l0.clone(), 1);
        assert!(policy.relief(&unbounded, at_hour(100.0)));
        let drain = CompactionSpec::drain_segment(Bytes::from_static(&[1]), l0);
        assert!(!policy.relief(&drain, at_hour(1.5)));
    }

    #[test]
    fn re_merges_late_writes_only_once_they_add_up() {
        let none = HashMap::new();
        let merged_at = at_hour(2.6);
        let late = |l0: usize, bytes: u64| SegmentState {
            oldest_ms: merged_at,
            bytes: 1_000 + bytes,
            l0_bytes: bytes,
            largest: 1_000,
            ..segment(1, l0, &[3], at_hour(5.0))
        };
        // A few small late L0s stay until they hold a sixteenth of the run.
        assert!(plan(&policy(), at_hour(5.0), &[late(3, 30)], &none, 4, 10).is_empty());
        assert_eq!(
            prefixes(&plan(&policy(), at_hour(5.0), &[late(3, 63)], &none, 4, 10)),
            vec![("merge", 1)]
        );
        // Past the source cap, any share, since they leave the run alone.
        assert_eq!(
            merged(&plan(&policy(), at_hour(5.0), &[late(8, 1)], &none, 4, 10)),
            vec![(late(8, 1).sources()[..8].to_vec(), 10)]
        );
    }

    #[test]
    fn merges_an_unsealed_rollup_only_on_request() {
        let holds = MergeHolds::default();
        let policy = policy()
            .with_holds(holds.clone())
            .with_unsealed(Arc::new(|prefix, _| prefix == [2]));
        let none = HashMap::new();
        let segments = [segment(1, 2, &[3], 0), segment(2, 2, &[4], 0)];
        // No hold, as after a failed rollup or a restart.
        assert_eq!(
            prefixes(&plan(&policy, at_hour(10.0), &segments, &none, 4, 10)),
            vec![("merge", 1)]
        );
        let hold = holds.copying(Bytes::from_static(&[2]));
        hold.merge();
        assert_eq!(
            prefixes(&plan(&policy, at_hour(10.0), &segments, &none, 4, 10)),
            vec![("merge", 2), ("merge", 1)]
        );
    }

    #[test]
    fn holds_a_copy_target_then_merges_it_first_on_request() {
        let holds = MergeHolds::default();
        let policy = policy().with_holds(holds.clone());
        let none = HashMap::new();
        let segments = [
            segment(1, 2, &[3], 0),
            SegmentState {
                oldest_ms: at_hour(9.0),
                bytes: 1_001,
                largest: 1_000,
                ..segment(2, 1, &[4], 0)
            },
        ];
        let hold = holds.copying(Bytes::from_static(&[2]));
        assert_eq!(
            prefixes(&plan(&policy, at_hour(10.0), &segments, &none, 4, 10)),
            vec![("merge", 1)]
        );
        hold.merge();
        assert_eq!(
            prefixes(&plan(&policy, at_hour(10.0), &segments, &none, 4, 10)),
            vec![("merge", 2), ("merge", 1)]
        );
        drop(hold);
        assert_eq!(
            prefixes(&plan(&policy, at_hour(10.0), &segments, &none, 4, 10)),
            vec![("merge", 1)]
        );
    }
}
