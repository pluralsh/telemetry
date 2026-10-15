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
//!   which are merged in once they hold a share of the segment's bytes or
//!   leave it with too many sources, so a trickle of late writes doesn't
//!   rewrite a large segment every few seconds.
//! - **Cap**: until then, merge a segment whenever size-tiered compaction
//!   leaves it more than [`SegmentCompaction::with_max_open_sources`]
//!   sources, which bounds what reads of the newest data probe.
//! - **Hold**: a rollup copying into a segment holds its merges
//!   ([`MergeHolds`]) and then asks for one, so the segment is rewritten
//!   once rather than every time the copy adds L0s.
//! - **Drain**: once both the segment's end and its newest SST are older
//!   than the retention, drop the segment without rewriting it.
//! - **Supersede**: with a [`Superseded`] hook, drop a segment the hook
//!   reports as superseded (every record it holds is held by another
//!   segment), also without rewriting it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView,
    SizeTieredCompactionSchedulerSupplier, SourceId,
};
use slatedb::config::CompactorOptions;
use slatedb::manifest::VersionedManifest;

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
/// A merged segment's late L0s and runs are merged in once they hold this
/// fraction (as a divisor) of its largest source's bytes...
const LATE_BYTES_DIVISOR: u64 = 16;
/// ...or once they're past the source cap and hold this fraction of them.
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
        let mut holds = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
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
    retention: Option<Duration>,
    superseded: Option<Superseded>,
    holds: MergeHolds,
}

impl std::fmt::Debug for SegmentCompaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCompaction")
            .field("grace", &self.grace)
            .field("max_open_sources", &self.max_open_sources)
            .field("retention", &self.retention)
            .field("superseded", &self.superseded.is_some())
            .finish_non_exhaustive()
    }
}

impl SegmentCompaction {
    pub fn new(span: SegmentSpan) -> Self {
        Self {
            span,
            grace: DEFAULT_MERGE_GRACE,
            max_open_sources: DEFAULT_MAX_OPEN_SOURCES,
            retention: None,
            superseded: None,
            holds: MergeHolds::default(),
        }
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
            path: self.path.clone(),
        })
    }
}

struct Scheduler {
    inner: Box<dyn CompactionScheduler + Send + Sync>,
    policy: SegmentCompaction,
    max_concurrent: usize,
    path: String,
}

impl CompactionScheduler for Scheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
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
        if slots == 0 {
            return specs;
        }
        let busy: HashSet<Bytes> = active
            .iter()
            .map(|compaction| compaction.spec())
            .chain(specs.iter())
            .map(|spec| spec.segment().clone())
            .collect();
        let manifest = state.manifest();
        let segments: Vec<SegmentState> = manifest
            .segments()
            .iter()
            .map(|segment| {
                let created = || {
                    segment
                        .l0()
                        .iter()
                        .chain(segment.compacted().iter().flat_map(|run| &run.sst_views))
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
            metrics::counter!(
                "storage_segment_compactions_total",
                "path" => self.path.clone(),
                "kind" => kind
            )
            .increment(1);
            specs.push(planned.into_spec());
        }
        specs
    }

    fn validate(
        &self,
        state: &CompactorStateView,
        spec: &CompactionSpec,
    ) -> Result<(), slatedb::Error> {
        self.inner.validate(state, spec)
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
    /// Estimated bytes of all its SSTs and of its largest source.
    bytes: u64,
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

/// Plans at most `slots` drains and merges, drains first and oldest segment
/// first. `next_run` is the first sorted-run id free for a new run.
fn plan(
    policy: &SegmentCompaction,
    now_ms: u64,
    segments: &[SegmentState],
    busy: &HashSet<Bytes>,
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
    let mut candidates: Vec<(i64, bool, &SegmentState)> = segments
        .iter()
        .filter(|segment| !busy.contains(&segment.prefix))
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
    let grace_ms = i64::try_from(policy.grace.as_millis()).unwrap_or(i64::MAX);
    let requested = |segment: &SegmentState| policy.holds.get(&segment.prefix) == Some(Hold::Merge);
    let mergeable = |end_ms: i64, segment: &SegmentState| {
        let sources = segment.l0.len() + segment.runs.len();
        if sources <= 1 {
            return false;
        }
        match policy.holds.get(&segment.prefix) {
            Some(Hold::Copying) => return false,
            Some(Hold::Merge) => return true,
            None => {}
        }
        let merge_at = end_ms.saturating_add(grace_ms);
        if now_ms < merge_at {
            return sources > policy.max_open_sources;
        }
        let rest = segment.bytes.saturating_sub(segment.largest);
        i64::try_from(segment.oldest_ms).unwrap_or(i64::MAX) < merge_at
            || rest.saturating_mul(LATE_BYTES_DIVISOR) >= segment.largest
            || (sources > policy.max_open_sources
                && rest.saturating_mul(LATE_CAPPED_BYTES_DIVISOR) >= segment.largest)
    };

    let drains = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| *superseded || expired(*end_ms, segment))
        .map(|(end_ms, superseded, segment)| Planned::Drain {
            prefix: segment.prefix.clone(),
            sources: segment.sources(),
            superseded: *superseded && !expired(*end_ms, segment),
        });
    // A rollup waits on its requested merge, so those go first.
    let (requested, routine): (Vec<_>, Vec<_>) = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| {
            !superseded && !expired(*end_ms, segment) && mergeable(*end_ms, segment)
        })
        .partition(|(_, _, segment)| requested(segment));
    let merges = requested
        .into_iter()
        .chain(routine)
        .map(|(_, _, segment)| {
            let destination = match segment.runs.iter().min() {
                Some(id) => *id,
                None => {
                    next_run += 1;
                    next_run - 1
                }
            };
            Planned::Merge {
                prefix: segment.prefix.clone(),
                sources: segment.sources(),
                destination,
            }
        });
    drains.chain(merges).take(slots).collect()
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
        let none = HashSet::new();
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
        let planned = plan(&policy(), at_hour(10.0), &segments, &HashSet::new(), 4, 10);
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
        let busy = HashSet::from([Bytes::from_static(&[3])]);
        assert!(plan(&policy(), at_hour(100.0), &segments, &busy, 4, 10).is_empty());
    }

    #[test]
    fn drains_only_once_both_the_segment_and_its_newest_write_expire() {
        let policy = policy().with_retention(Some(Duration::from_secs(2 * 3_600)));
        let segments = [
            segment(1, 0, &[3], at_hour(1.5)),
            segment(2, 0, &[4], at_hour(5.0)),
        ];
        let none = HashSet::new();
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
        let planned = plan(&policy, at_hour(10.0), &segments, &HashSet::new(), 4, 10);
        assert_eq!(prefixes(&planned), vec![("drain", 1), ("merge", 2)]);
        assert!(matches!(
            planned[0],
            Planned::Drain {
                superseded: true,
                ..
            }
        ));

        let busy = HashSet::from([Bytes::from_static(&[1])]);
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
                &HashSet::new(),
                3,
                10
            )),
            vec![("drain", 1), ("merge", 10), ("merge", 20)]
        );
    }

    #[test]
    fn merges_an_open_segment_only_past_the_source_cap() {
        let policy = policy().with_max_open_sources(8);
        let none = HashSet::new();
        let at_cap = [segment(1, 4, &[5, 4, 3, 2], 0)];
        assert!(plan(&policy, at_hour(1.5), &at_cap, &none, 4, 10).is_empty());
        let over = [segment(1, 5, &[5, 4, 3, 2], 0)];
        assert_eq!(
            prefixes(&plan(&policy, at_hour(1.5), &over, &none, 4, 10)),
            vec![("merge", 1)]
        );
    }

    #[test]
    fn re_merges_late_writes_only_once_they_add_up() {
        let none = HashSet::new();
        let merged_at = at_hour(2.6);
        let late = |l0: usize, bytes: u64| SegmentState {
            oldest_ms: merged_at,
            bytes: 1_000 + bytes,
            largest: 1_000,
            ..segment(1, l0, &[3], at_hour(5.0))
        };
        // A few small late L0s stay until they hold a sixteenth of the run.
        assert!(plan(&policy(), at_hour(5.0), &[late(3, 30)], &none, 4, 10).is_empty());
        assert_eq!(
            prefixes(&plan(&policy(), at_hour(5.0), &[late(3, 63)], &none, 4, 10)),
            vec![("merge", 1)]
        );
        // Past the source cap, a smaller share is enough.
        assert!(plan(&policy(), at_hour(5.0), &[late(8, 10)], &none, 4, 10).is_empty());
        assert_eq!(
            prefixes(&plan(&policy(), at_hour(5.0), &[late(8, 16)], &none, 4, 10)),
            vec![("merge", 1)]
        );
    }

    #[test]
    fn holds_a_copy_target_then_merges_it_first_on_request() {
        let holds = MergeHolds::default();
        let policy = policy().with_holds(holds.clone());
        let none = HashSet::new();
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
