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
//!   L0s and sorted runs into one sorted run. Late writes add L0s again, and
//!   the segment is merged again.
//! - **Drain**: once both the segment's end and its newest SST are older
//!   than the retention, drop the segment without rewriting it.
//! - **Supersede**: with a [`Superseded`] hook, drop a segment the hook
//!   reports as superseded (every record it holds is held by another
//!   segment), also without rewriting it.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView,
    SizeTieredCompactionSchedulerSupplier, SourceId,
};
use slatedb::config::CompactorOptions;

/// The `[start_ms, end_ms)` span of the segment with this prefix, or `None`
/// for a segment that isn't bounded in time.
pub type SegmentSpan = fn(&[u8]) -> Option<(i64, i64)>;

/// Whether the segment with this prefix is superseded, given a test for
/// which segment prefixes are live in the manifest.
pub type Superseded = Arc<dyn Fn(&[u8], &dyn Fn(&[u8]) -> bool) -> bool + Send + Sync>;

/// Default delay between a segment ending and it being merged.
pub const DEFAULT_MERGE_GRACE: Duration = Duration::from_secs(30 * 60);

/// Segment-aware compaction policy for one database.
#[derive(Clone)]
pub struct SegmentCompaction {
    span: SegmentSpan,
    grace: Duration,
    retention: Option<Duration>,
    superseded: Option<Superseded>,
}

impl std::fmt::Debug for SegmentCompaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCompaction")
            .field("grace", &self.grace)
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
            retention: None,
            superseded: None,
        }
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
            .map(|segment| SegmentState {
                prefix: segment.prefix().clone(),
                l0: segment.l0().iter().map(|view| view.id).collect(),
                runs: segment.compacted().iter().map(|run| run.id).collect(),
                newest_ms: segment
                    .l0()
                    .iter()
                    .chain(segment.compacted().iter().flat_map(|run| &run.sst_views))
                    .map(|view| view.id.timestamp_ms())
                    .max()
                    .unwrap_or(0),
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
    let mergeable = |end_ms: i64, segment: &SegmentState| {
        segment.l0.len() + segment.runs.len() > 1 && now_ms >= end_ms.saturating_add(grace_ms)
    };

    let drains = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| *superseded || expired(*end_ms, segment))
        .map(|(end_ms, superseded, segment)| Planned::Drain {
            prefix: segment.prefix.clone(),
            sources: segment.sources(),
            superseded: *superseded && !expired(*end_ms, segment),
        });
    let merges = candidates
        .iter()
        .filter(|(end_ms, superseded, segment)| {
            !superseded && !expired(*end_ms, segment) && mergeable(*end_ms, segment)
        })
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
}
