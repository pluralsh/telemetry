//! Vetting of proposed compactions before SlateDB sees them.
//!
//! SlateDB 0.15's coordinator validates each proposal as it arrives, and
//! applies drains and trivial moves on the spot without a worker. A proposal
//! it rejects is marked failed in the same pass. After each of those it keeps
//! only the newest finished compaction in memory. When two or more finish in
//! one pass and the `.compactions` write that ends it conflicts with a
//! worker's write, the coordinator merges the stored copy back in, and the
//! older ones come back as `Submitted` even though they were already applied.
//! The next stats tick (every 10 s) looks up their sources in the manifest and
//! panics the compactor task on the first one that's gone, closing the DB.
//!
//! So [`admit`] passes on only proposals that SlateDB will accept, and at
//! most one per pass that finishes without a worker. It defers the rest to
//! later polls.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::manifest::{SortedRun, SsTableView};
use ulid::Ulid;

/// The key range of one SST view, as SlateDB computes it to decide whether
/// a compaction's inputs are disjoint; `None` when the SST doesn't record it.
type KeyRange = Option<(Bound<Bytes>, Bound<Bytes>)>;

/// One LSM tree of the manifest: the root tree (empty prefix) or a segment.
#[derive(Clone, Debug)]
pub(super) struct Tree {
    pub(super) prefix: Bytes,
    pub(super) l0: Vec<(Ulid, KeyRange)>,
    pub(super) runs: Vec<(u32, Vec<KeyRange>)>,
}

impl Tree {
    pub(super) fn new(prefix: Bytes, l0: &VecDeque<SsTableView>, runs: &[SortedRun]) -> Self {
        Self {
            prefix,
            l0: l0.iter().map(|view| (view.id, key_range(view))).collect(),
            runs: runs
                .iter()
                .map(|run| (run.id, run.sst_views.iter().map(key_range).collect()))
                .collect(),
        }
    }

    fn has(&self, source: &SourceId) -> bool {
        match source {
            SourceId::SstView(id) => self.l0.iter().any(|(view, _)| view == id),
            SourceId::SortedRun(id) => self.runs.iter().any(|(run, _)| run == id),
        }
    }

    fn source_count(&self) -> usize {
        self.l0.len() + self.runs.len()
    }

    /// Whether SlateDB may apply a compaction of `sources` as a trivial
    /// move: when its inputs' key ranges are pairwise disjoint. Unknown
    /// ranges count as disjoint, so this never misses one.
    fn may_move_trivially(&self, sources: &[SourceId]) -> bool {
        let mut ranges = Vec::new();
        for source in sources {
            match source {
                SourceId::SstView(id) => ranges.extend(
                    self.l0
                        .iter()
                        .filter(|(view, _)| view == id)
                        .map(|(_, range)| range.clone()),
                ),
                SourceId::SortedRun(id) => ranges.extend(
                    self.runs
                        .iter()
                        .filter(|(run, _)| run == id)
                        .flat_map(|(_, views)| views.iter().cloned()),
                ),
            }
        }
        let Some(mut ranges) = ranges.into_iter().collect::<Option<Vec<_>>>() else {
            return true;
        };
        ranges.sort_by(|(left, _), (right, _)| compare_starts(left, right));
        !ranges.is_empty()
            && ranges
                .windows(2)
                .all(|pair| ends_before(&pair[0].1, &pair[1].0))
    }
}

fn key_range(view: &SsTableView) -> KeyRange {
    let info = &view.sst.info;
    let mut start = Bound::Included(info.first_entry.clone()?);
    let mut end = info
        .last_entry
        .clone()
        .map_or(Bound::Unbounded, Bound::Included);
    if let Some(visible) = view.visible_range() {
        let visible_start = visible.start_bound().cloned();
        if compare_starts(&visible_start, &start) == Ordering::Greater {
            start = visible_start;
        }
        let visible_end = visible.end_bound().cloned();
        if compare_ends(&visible_end, &end) == Ordering::Less {
            end = visible_end;
        }
    }
    Some((start, end))
}

fn compare_starts(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(l), Bound::Excluded(r)) if l == r => Ordering::Less,
        (Bound::Excluded(l), Bound::Included(r)) if l == r => Ordering::Greater,
        (Bound::Included(l) | Bound::Excluded(l), Bound::Included(r) | Bound::Excluded(r)) => {
            l.cmp(r)
        }
    }
}

fn compare_ends(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(l), Bound::Excluded(r)) if l == r => Ordering::Greater,
        (Bound::Excluded(l), Bound::Included(r)) if l == r => Ordering::Less,
        (Bound::Included(l) | Bound::Excluded(l), Bound::Included(r) | Bound::Excluded(r)) => {
            l.cmp(r)
        }
    }
}

/// Whether a range ending at `end` lies wholly before one starting at `start`.
fn ends_before(end: &Bound<Bytes>, start: &Bound<Bytes>) -> bool {
    match (end, start) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(end), Bound::Included(start)) => end < start,
        (Bound::Included(end) | Bound::Excluded(end), Bound::Excluded(start))
        | (Bound::Excluded(end), Bound::Included(start)) => end <= start,
    }
}

/// Why a proposal was held back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    /// It has no sources, repeats one, or names one the tree doesn't have.
    Sources,
    /// Its segment isn't in the manifest, or it drains the root tree.
    Segment,
    /// Another compaction already uses one of its sources or its segment.
    Busy,
    /// It drains only part of its segment.
    PartialDrain,
    /// Its destination would overwrite a run it doesn't read, or one another
    /// compaction writes.
    Destination,
    /// Another proposal in this pass already finishes without a worker.
    Deferred,
}

impl Rejection {
    pub(super) fn label(self) -> &'static str {
        match self {
            Rejection::Sources => "sources",
            Rejection::Segment => "segment",
            Rejection::Busy => "busy",
            Rejection::PartialDrain => "partial_drain",
            Rejection::Destination => "destination",
            Rejection::Deferred => "deferred",
        }
    }
}

#[derive(Default)]
struct SegmentUse {
    any: bool,
    l0: bool,
    drain: bool,
}

/// What compactions already running or admitted use.
#[derive(Default)]
struct Claims {
    sources: HashSet<(Bytes, SourceId)>,
    segments: HashMap<Bytes, SegmentUse>,
    /// Destinations being written, except the fresh ones of admitted
    /// proposals, which are renumbered.
    destinations: HashSet<u32>,
}

impl Claims {
    /// Claims what `spec` uses; `l0` whether it bars other L0 compactions of
    /// its segment.
    fn claim(&mut self, spec: &CompactionSpec, l0: bool, renumbered: bool) {
        for source in spec.sources() {
            self.sources.insert((spec.segment().clone(), *source));
        }
        let used = self.segments.entry(spec.segment().clone()).or_default();
        used.any = true;
        used.l0 |= l0 && spec.has_l0_sources();
        used.drain |= spec.is_drain();
        if !renumbered {
            self.destinations.extend(spec.destination());
        }
    }

    fn check(
        &self,
        tree: Option<&&Tree>,
        spec: &CompactionSpec,
        trivial_moves: bool,
    ) -> Result<bool, Rejection> {
        let segment = spec.segment();
        let tree = tree.ok_or(Rejection::Segment)?;
        if spec.is_drain() && segment.is_empty() {
            return Err(Rejection::Segment);
        }
        let unique: HashSet<&SourceId> = spec.sources().iter().collect();
        if spec.sources().is_empty()
            || unique.len() != spec.sources().len()
            || !spec.sources().iter().all(|source| tree.has(source))
        {
            return Err(Rejection::Sources);
        }
        let used = self.segments.get(segment);
        if spec
            .sources()
            .iter()
            .any(|source| self.sources.contains(&(segment.clone(), *source)))
            || used.is_some_and(|used| {
                used.drain || (spec.is_drain() && used.any) || (spec.has_l0_sources() && used.l0)
            })
        {
            return Err(Rejection::Busy);
        }
        if spec.is_drain() && spec.sources().len() != tree.source_count() {
            return Err(Rejection::PartialDrain);
        }
        if let Some(destination) = spec.destination() {
            let rewrites_own = spec.sources().contains(&SourceId::SortedRun(destination));
            if spec.has_sr_sources() && (!rewrites_own || self.destinations.contains(&destination))
            {
                return Err(Rejection::Destination);
            }
        }
        Ok(spec.is_drain() || (trivial_moves && tree.may_move_trivially(spec.sources())))
    }
}

/// Whether `spec` writes a fresh run, whose id [`admit`] picks.
fn writes_fresh_run(spec: &CompactionSpec) -> bool {
    spec.destination().is_some() && !spec.has_sr_sources()
}

/// The proposals SlateDB will accept, at most one of which finishes in the
/// pass that validates it, which goes first. Fresh sorted-run ids for L0-only
/// compactions are renumbered above every run committed or being written,
/// the finishing one first, so whichever order SlateDB validates them in each
/// still lands above every run committed before it.
///
/// `active` is SlateDB's active compactions. Their claims follow SlateDB's
/// validation: only `Scheduled` and `Running` ones bar other L0 compactions of
/// their segment, and a `Compacted` one whose sources are gone was already
/// applied and claims nothing. A `Submitted` one is validated in the same
/// pass as the proposals, and may finish there, so none of them may.
pub(super) fn admit(
    trees: &[Tree],
    active: &[(CompactionSpec, CompactionStatus)],
    proposed: Vec<CompactionSpec>,
    trivial_moves: bool,
) -> (Vec<CompactionSpec>, Vec<(CompactionSpec, Rejection)>) {
    let trees: HashMap<&Bytes, &Tree> = trees.iter().map(|tree| (&tree.prefix, tree)).collect();
    let mut claims = Claims::default();
    let mut finishing = false;
    for (spec, status) in active {
        let live = trees
            .get(spec.segment())
            .is_some_and(|tree| spec.sources().iter().all(|source| tree.has(source)));
        match status {
            CompactionStatus::Scheduled | CompactionStatus::Running => {
                claims.claim(spec, true, false);
            }
            CompactionStatus::Submitted => {
                finishing = true;
                if live {
                    claims.claim(spec, true, false);
                }
            }
            CompactionStatus::Compacted if live => claims.claim(spec, false, false),
            _ => {}
        }
    }

    let mut admitted: Vec<(CompactionSpec, bool)> = Vec::new();
    let mut rejected = Vec::new();
    for spec in proposed {
        match claims.check(trees.get(spec.segment()), &spec, trivial_moves) {
            Ok(true) if finishing => rejected.push((spec, Rejection::Deferred)),
            Ok(finishes) => {
                finishing |= finishes;
                claims.claim(&spec, true, writes_fresh_run(&spec));
                admitted.push((spec, finishes));
            }
            Err(rejection) => rejected.push((spec, rejection)),
        }
    }

    let committed = trees
        .values()
        .flat_map(|tree| tree.runs.iter().map(|(id, _)| *id));
    let mut fresh = committed
        .chain(claims.destinations.iter().copied())
        .max()
        .map_or(0, |id| id.saturating_add(1));
    admitted.sort_by_key(|(_, finishes)| !finishes);
    let specs = admitted
        .into_iter()
        .map(|(spec, _)| {
            if !writes_fresh_run(&spec) {
                return spec;
            }
            let destination = fresh;
            fresh = fresh.saturating_add(1);
            CompactionSpec::for_segment(
                spec.segment().clone(),
                spec.sources().to_vec(),
                destination,
            )
        })
        .collect();
    (specs, rejected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: &'static [u8], end: &'static [u8]) -> KeyRange {
        Some((
            Bound::Included(Bytes::from_static(start)),
            Bound::Included(Bytes::from_static(end)),
        ))
    }

    fn view(index: u64) -> SourceId {
        SourceId::SstView(Ulid::from_parts(index, 0))
    }

    /// A segment `[prefix]` with L0s numbered from `first_l0`, each spanning
    /// `l0_ranges`, and runs with one SST each.
    fn tree(prefix: u8, first_l0: u64, l0_ranges: &[KeyRange], runs: &[(u32, KeyRange)]) -> Tree {
        Tree {
            prefix: Bytes::copy_from_slice(&[prefix]),
            l0: l0_ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (Ulid::from_parts(first_l0 + index as u64, 0), range.clone()))
                .collect(),
            runs: runs
                .iter()
                .map(|(id, range)| (*id, vec![range.clone()]))
                .collect(),
        }
    }

    fn drain(prefix: u8, sources: Vec<SourceId>) -> CompactionSpec {
        CompactionSpec::drain_segment(Bytes::copy_from_slice(&[prefix]), sources)
    }

    fn tiered(prefix: u8, sources: Vec<SourceId>, destination: u32) -> CompactionSpec {
        CompactionSpec::for_segment(Bytes::copy_from_slice(&[prefix]), sources, destination)
    }

    fn reasons(rejected: &[(CompactionSpec, Rejection)]) -> Vec<Rejection> {
        rejected.iter().map(|(_, rejection)| *rejection).collect()
    }

    #[test]
    fn passes_one_drain_per_pass_and_defers_the_rest() {
        let trees = [
            tree(1, 0, &[range(b"a", b"b")], &[(3, range(b"a", b"z"))]),
            tree(2, 10, &[range(b"a", b"b")], &[]),
            tree(3, 20, &[], &[(4, range(b"a", b"z"))]),
        ];
        let proposed = vec![
            drain(1, vec![view(0), SourceId::SortedRun(3)]),
            drain(2, vec![view(10)]),
            drain(3, vec![SourceId::SortedRun(4)]),
        ];
        let (specs, rejected) = admit(&trees, &[], proposed.clone(), true);
        assert_eq!(specs, proposed[..1]);
        assert_eq!(
            reasons(&rejected),
            [Rejection::Deferred, Rejection::Deferred]
        );
    }

    #[test]
    fn counts_a_trivial_move_as_finishing_and_puts_it_first() {
        let trees = [
            // Disjoint L0s: a trivial move.
            tree(1, 0, &[range(b"a", b"b"), range(b"c", b"d")], &[]),
            // Overlapping L0s: a real merge, which a worker runs.
            tree(
                2,
                10,
                &[range(b"a", b"m"), range(b"c", b"z")],
                &[(7, range(b"a", b"z"))],
            ),
            tree(3, 20, &[], &[(8, range(b"a", b"z"))]),
        ];
        let proposed = vec![
            tiered(2, vec![view(10), view(11)], 99),
            tiered(1, vec![view(0), view(1)], 98),
            drain(3, vec![SourceId::SortedRun(8)]),
        ];
        let (specs, rejected) = admit(&trees, &[], proposed, true);
        // The trivial move goes first with the lowest fresh id, the merge
        // after it, and the drain waits for the next pass.
        assert_eq!(
            specs,
            [
                tiered(1, vec![view(0), view(1)], 9),
                tiered(2, vec![view(10), view(11)], 10),
            ]
        );
        assert_eq!(reasons(&rejected), [Rejection::Deferred]);

        // Without trivial moves both merges run on a worker.
        let (specs, rejected) = admit(
            &trees,
            &[],
            vec![
                tiered(1, vec![view(0), view(1)], 98),
                drain(3, vec![SourceId::SortedRun(8)]),
            ],
            false,
        );
        assert_eq!(specs.len(), 2);
        assert!(rejected.is_empty());
    }

    #[test]
    fn unknown_key_ranges_may_move_trivially() {
        let trees = [
            tree(1, 0, &[None, range(b"a", b"z")], &[]),
            tree(2, 10, &[range(b"a", b"z")], &[]),
        ];
        let proposed = vec![
            tiered(1, vec![view(0), view(1)], 5),
            drain(2, vec![view(10)]),
        ];
        let (_, rejected) = admit(&trees, &[], proposed, true);
        assert_eq!(reasons(&rejected), [Rejection::Deferred]);
    }

    #[test]
    fn rejects_proposals_slatedb_would_fail() {
        let trees = [
            tree(
                1,
                0,
                &[range(b"a", b"m"), range(b"c", b"z")],
                &[(3, range(b"a", b"z")), (5, range(b"a", b"z"))],
            ),
            tree(2, 10, &[range(b"a", b"m"), range(b"c", b"z")], &[]),
        ];
        let active = [(tiered(2, vec![view(11)], 20), CompactionStatus::Running)];
        let proposed = vec![
            // Unknown segment.
            tiered(9, vec![view(0)], 30),
            // Root drain.
            CompactionSpec::drain_segment(Bytes::new(), vec![view(0)]),
            // Missing, repeated and absent sources.
            tiered(1, vec![view(7), view(0)], 30),
            tiered(1, vec![view(0), view(0)], 30),
            tiered(1, vec![], 30),
            // L0s while another L0 compaction runs in the segment.
            tiered(2, vec![view(10)], 30),
            // Overwrites run 5, which it doesn't read.
            tiered(1, vec![SourceId::SortedRun(3)], 5),
            // Drains only some of the segment.
            drain(1, vec![view(0), view(1), SourceId::SortedRun(3)]),
        ];
        let (specs, rejected) = admit(&trees, &active, proposed, true);
        assert!(specs.is_empty(), "{specs:?}");
        assert_eq!(
            reasons(&rejected),
            [
                Rejection::Segment,
                Rejection::Segment,
                Rejection::Sources,
                Rejection::Sources,
                Rejection::Sources,
                Rejection::Busy,
                Rejection::Destination,
                Rejection::PartialDrain,
            ]
        );
    }

    #[test]
    fn keeps_one_l0_compaction_per_segment_and_drains_exclusive() {
        let overlapping = [range(b"a", b"m"), range(b"c", b"z"), range(b"b", b"y")];
        let trees = [tree(
            1,
            0,
            &overlapping,
            &[(3, range(b"a", b"z")), (4, range(b"a", b"z"))],
        )];
        let proposed = vec![
            tiered(1, vec![view(0), view(1)], 40),
            tiered(1, vec![view(2)], 41),
            // Runs alongside the L0 merge.
            tiered(1, vec![SourceId::SortedRun(4), SourceId::SortedRun(3)], 3),
            drain(
                1,
                vec![
                    view(0),
                    view(1),
                    view(2),
                    SourceId::SortedRun(3),
                    SourceId::SortedRun(4),
                ],
            ),
        ];
        let (specs, rejected) = admit(&trees, &[], proposed, true);
        assert_eq!(
            specs,
            [
                tiered(1, vec![view(0), view(1)], 5),
                tiered(1, vec![SourceId::SortedRun(4), SourceId::SortedRun(3)], 3),
            ]
        );
        assert_eq!(reasons(&rejected), [Rejection::Busy, Rejection::Busy]);
    }

    #[test]
    fn fresh_runs_land_above_runs_being_written() {
        let trees = [tree(
            1,
            0,
            &[range(b"a", b"m"), range(b"c", b"z")],
            &[(3, range(b"a", b"z"))],
        )];
        let active = [(tiered(2, vec![view(50)], 12), CompactionStatus::Scheduled)];
        let (specs, _) = admit(
            &trees,
            &active,
            vec![tiered(1, vec![view(0), view(1)], 4)],
            true,
        );
        assert_eq!(specs, [tiered(1, vec![view(0), view(1)], 13)]);
    }

    #[test]
    fn applied_compactions_claim_nothing_and_resubmitted_ones_hold_finishing_back() {
        let overlapping = [range(b"a", b"m"), range(b"c", b"z")];
        let trees = [
            tree(1, 0, &overlapping, &[]),
            tree(2, 10, &[range(b"a", b"z")], &[]),
        ];
        let merge = tiered(1, vec![view(0), view(1)], 30);
        // Already applied: its L0 is gone.
        let applied = (tiered(1, vec![view(9)], 7), CompactionStatus::Compacted);
        let (specs, rejected) = admit(
            &trees,
            std::slice::from_ref(&applied),
            vec![merge.clone()],
            true,
        );
        assert_eq!(specs.len(), 1);
        assert!(rejected.is_empty());

        // A pending commit of live sources only claims those sources.
        let pending = (tiered(1, vec![view(1)], 7), CompactionStatus::Compacted);
        let (_, rejected) = admit(&trees, &[pending], vec![merge.clone()], true);
        assert_eq!(reasons(&rejected), [Rejection::Busy]);

        // A resubmitted compaction is validated in the same pass, and may
        // finish there, so the drain waits.
        let resubmitted = (applied.0, CompactionStatus::Submitted);
        let (specs, rejected) = admit(
            &trees,
            &[resubmitted],
            vec![merge, drain(2, vec![view(10)])],
            true,
        );
        assert_eq!(specs.len(), 1);
        assert_eq!(reasons(&rejected), [Rejection::Deferred]);
    }

    #[test]
    fn visible_ranges_narrow_key_ranges() {
        let excluded = |start: &'static [u8], end: &'static [u8]| {
            Some((
                Bound::Included(Bytes::from_static(start)),
                Bound::Excluded(Bytes::from_static(end)),
            ))
        };
        // [a, c) and [c, d] are disjoint; [a, c] and [c, d] are not.
        let disjoint = tree(1, 0, &[excluded(b"a", b"c"), range(b"c", b"d")], &[]);
        assert!(disjoint.may_move_trivially(&[view(0), view(1)]));
        let touching = tree(1, 0, &[range(b"a", b"c"), range(b"c", b"d")], &[]);
        assert!(!touching.may_move_trivially(&[view(0), view(1)]));
    }
}
