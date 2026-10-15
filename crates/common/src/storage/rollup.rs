//! Rollup of closed time segments into wider ones, for stores whose keys
//! start with a [`KeyScope`].
//!
//! SlateDB compacts within a segment, and every `(namespace, time segment)`
//! is its own segment, so a day of hour segments stays 24 segments however
//! long it sits. A store rolls segments up by re-writing them into wider
//! aligned ones that stay within a size cap, in two tiers:
//!
//! - Each [`RollupConfig::window`] of the day (4 hours by default), once it
//!   has closed, into one segment of that width, or of narrower widths
//!   dividing it if that's over the cap. Queries over the last hours and
//!   day read these within minutes of the window closing.
//! - Each closed day, into the fewest aligned segments of 24, 12 or 8 hours
//!   the cap allows, made from its windows' rollups. A day too big for any
//!   of those keeps its windows. A closed day with no rollups yet (after a
//!   backfill) is instead tiled from its segments directly, so it is
//!   copied once.
//!
//! Each rollup records two markers in segments of their own:
//!
//! - **Pending**, written before the first copy. From then on the store
//!   routes writes for the covered segments to the rollup segment, and reads
//!   ignore the rollup segment.
//! - **Sealed**, written once every covered segment is copied and the
//!   rollup segment merged into one sorted run. Reads use the rollup
//!   segment in place of the segments it covers, which compaction then
//!   drains ([`superseded`]).
//!
//! A marker's scope is the rollup segment's with a flag bit set in its
//! width, so markers never share a segment with data and are found from
//! the manifest without reading any SST.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use slatedb::manifest::VersionedManifest;

use crate::namespace::Namespace;
use crate::serde::scope::{KeyScope, Segment};
use crate::storage::compaction::{Superseded, merged_since};
use crate::storage::slate::SlateReadHandle;

pub const DAY_NS: i64 = 86_400 * 1_000_000_000;
const SEALED: i64 = 1 << 62;
const PENDING: i64 = 1 << 61;
const MARKERS: i64 = SEALED | PENDING;
/// Rollup widths are a day divided by one of these.
const DAY_DIVISORS: [i64; 7] = [1, 2, 3, 4, 6, 8, 12];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollupConfig {
    pub enabled: bool,
    /// Largest rollup segment, by the estimated size of what it covers.
    pub max_segment_bytes: u64,
    /// Width of the rollups made as the day goes, a fraction of a day; the
    /// widest rollup width at most this is used.
    pub window: Duration,
    /// How long after a window or day ends before it is rolled up.
    pub delay: Duration,
    /// How often closed windows and days are looked for.
    pub interval: Duration,
    /// Copied bytes a rollup may hand the writer before waiting for them to
    /// reach storage. Larger values copy faster at the cost of memory.
    pub copy_buffer_bytes: u64,
    /// How long a copied rollup waits for its merge into one sorted run
    /// before it is sealed anyway, as it must be when no compactor runs in
    /// the writer's process.
    pub merge_wait: Duration,
}

/// Default [`RollupConfig::copy_buffer_bytes`].
pub const DEFAULT_COPY_BUFFER_BYTES: u64 = 64 << 20;
/// Default [`RollupConfig::window`].
pub const DEFAULT_WINDOW: Duration = Duration::from_secs(4 * 3_600);
/// Default [`RollupConfig::delay`]. Writes arriving after a rollup starts
/// go to the rollup, so this only spares it re-merging the stragglers.
pub const DEFAULT_DELAY: Duration = Duration::from_secs(10 * 60);
/// Default [`RollupConfig::merge_wait`].
pub const DEFAULT_MERGE_WAIT: Duration = Duration::from_secs(30 * 60);
const MERGE_POLL: Duration = Duration::from_secs(1);

/// Waits until the segment at `prefix` holds everything it held at
/// `since_ms` in one sorted run, as the `manifest` it reads shows, for at
/// most `wait`. Returns whether it does.
pub async fn await_merge(
    manifest: impl Fn() -> VersionedManifest,
    prefix: &[u8],
    since_ms: u64,
    wait: Duration,
) -> bool {
    let started = std::time::Instant::now();
    loop {
        if merged_since(&manifest(), prefix, since_ms) {
            return true;
        }
        if started.elapsed() >= wait {
            return false;
        }
        tokio::time::sleep(MERGE_POLL.min(wait)).await;
    }
}

impl Default for RollupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segment_bytes: 4 << 30,
            window: DEFAULT_WINDOW,
            delay: DEFAULT_DELAY,
            interval: Duration::from_secs(300),
            copy_buffer_bytes: DEFAULT_COPY_BUFFER_BYTES,
            merge_wait: DEFAULT_MERGE_WAIT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Marker {
    Pending,
    Sealed,
}

impl Marker {
    fn flag(self) -> i64 {
        match self {
            Marker::Pending => PENDING,
            Marker::Sealed => SEALED,
        }
    }
}

/// The scope of `marker` for the rollup segment `segment`.
pub fn marker_segment(marker: Marker, segment: Segment) -> Segment {
    Segment::new(segment.start_ns, segment.width_ns | marker.flag())
}

/// The marker and rollup segment a marker scope names, or `None` for data.
pub fn parse_marker(segment: Segment) -> Option<(Marker, Segment)> {
    let data = Segment::new(segment.start_ns, segment.width_ns & !MARKERS);
    match segment.width_ns & MARKERS {
        SEALED => Some((Marker::Sealed, data)),
        PENDING => Some((Marker::Pending, data)),
        _ => None,
    }
}

/// `segment` with any marker flag cleared: the span a marker's records live
/// as long as.
pub fn data_segment(segment: Segment) -> Segment {
    Segment::new(segment.start_ns, segment.width_ns & !MARKERS)
}

/// Key of the one record of `marker` for `segment`.
pub fn marker_key(
    scope: KeyScope,
    namespace: &Namespace,
    marker: Marker,
    segment: Segment,
) -> Bytes {
    let mut bytes = BytesMut::new();
    scope.write(&mut bytes, namespace, marker_segment(marker, segment));
    bytes.freeze()
}

/// Whether `prefix` is exactly a marker scope of `scope`, judged from its
/// width bytes alone.
fn is_marker(scope: KeyScope, prefix: &[u8]) -> bool {
    if scope.prefix_len(prefix) != Some(prefix.len()) {
        return false;
    }
    let width = &prefix[prefix.len() - 8..];
    let width = u64::from_be_bytes(width.try_into().unwrap_or([0; 8]));
    width & (MARKERS as u64) != 0
}

fn covers(wide: Segment, narrow: Segment) -> bool {
    wide != narrow
        && wide.start_ns <= narrow.start_ns
        && narrow
            .end_ns()
            .is_some_and(|end| wide.end_ns().is_some_and(|wide_end| end <= wide_end))
}

fn overlaps(a: Segment, b: Segment) -> bool {
    a.start_ns < b.start_ns.saturating_add(b.width_ns)
        && b.start_ns < a.start_ns.saturating_add(a.width_ns)
}

/// A compaction hook draining each data segment of `scope`, `base_ns` wide
/// or a narrower rollup, that a live seal of a wider rollup covers.
pub fn superseded(scope: KeyScope, base_ns: i64) -> Superseded {
    Arc::new(move |prefix, present| {
        let Ok((namespace, segment, _)) = scope.parse(prefix) else {
            return false;
        };
        if parse_marker(segment).is_some() || segment.width_ns < base_ns {
            return false;
        }
        widths(base_ns, None)
            .into_iter()
            .filter(|width| *width > segment.width_ns && width % segment.width_ns == 0)
            .any(|width| {
                let cover = Segment::containing(segment.start_ns, width);
                present(&marker_key(scope, &namespace, Marker::Sealed, cover))
            })
    })
}

/// Rollup widths for `base_ns` segments, widest first: day fractions wider
/// than and a whole multiple of `base_ns`, and dividing `period_ns` if set.
pub fn widths(base_ns: i64, period_ns: Option<i64>) -> Vec<i64> {
    if base_ns <= 0 || DAY_NS % base_ns != 0 {
        return Vec::new();
    }
    DAY_DIVISORS
        .iter()
        .map(|divisor| DAY_NS / divisor)
        .filter(|width| *width > base_ns && width % base_ns == 0)
        .filter(|width| period_ns.is_none_or(|period| period % width == 0))
        .collect()
}

/// The rollup segments of one namespace and what each segment holds, as
/// the manifest lists them.
#[derive(Clone, Debug, Default)]
pub struct Rollups {
    sealed: Vec<Segment>,
    pending: Vec<Segment>,
    /// Estimated bytes of every data segment.
    sizes: HashMap<Segment, u64>,
}

impl Rollups {
    /// `namespace`'s rollup markers from the manifest `slate` reads, without
    /// segment sizes; empty for storage without a manifest. Only marker
    /// prefixes are parsed, so this is cheap enough for every read.
    pub fn markers(
        slate: Option<&SlateReadHandle>,
        scope: KeyScope,
        namespace: &Namespace,
    ) -> Self {
        let Some(slate) = slate else {
            return Self::default();
        };
        let markers = slate.segment_prefixes(|prefix| is_marker(scope, prefix));
        let mut rollups =
            Self::from_segments(scope, namespace, markers.into_iter().map(|p| (p, 0)));
        rollups.sizes.clear();
        rollups
    }

    pub fn from_segments(
        scope: KeyScope,
        namespace: &Namespace,
        segments: impl IntoIterator<Item = (Bytes, u64)>,
    ) -> Self {
        let mut rollups = Self::default();
        for (prefix, bytes) in segments {
            let Ok((segment_namespace, segment, _)) = scope.parse(&prefix) else {
                continue;
            };
            if &segment_namespace != namespace {
                continue;
            }
            match parse_marker(segment) {
                Some((Marker::Sealed, rollup)) => rollups.sealed.push(rollup),
                Some((Marker::Pending, rollup)) => rollups.pending.push(rollup),
                None => {
                    rollups.sizes.insert(segment, bytes);
                }
            }
        }
        rollups.sealed.sort();
        rollups.pending.sort();
        rollups
    }

    /// The rollups of every namespace with a segment of `scope` in the
    /// manifest `slate` reads.
    pub fn load_all(slate: &SlateReadHandle, scope: KeyScope) -> Vec<(Namespace, Rollups)> {
        let mut by_namespace: HashMap<Namespace, Vec<(Bytes, u64)>> = HashMap::new();
        for (prefix, bytes) in slate.segment_sizes() {
            if let Ok((namespace, _, _)) = scope.parse(&prefix) {
                by_namespace
                    .entry(namespace)
                    .or_default()
                    .push((prefix, bytes));
            }
        }
        by_namespace
            .into_iter()
            .map(|(namespace, segments)| {
                let rollups = Self::from_segments(scope, &namespace, segments);
                (namespace, rollups)
            })
            .collect()
    }

    /// Rollups from their `sealed` and `pending` segments and the estimated
    /// bytes of every data segment, for stores that keep markers their own
    /// way.
    pub fn new(
        mut sealed: Vec<Segment>,
        mut pending: Vec<Segment>,
        sizes: HashMap<Segment, u64>,
    ) -> Self {
        sealed.sort();
        pending.sort();
        Self {
            sealed,
            pending,
            sizes,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sealed.is_empty() && self.pending.is_empty()
    }

    pub fn sealed(&self) -> &[Segment] {
        &self.sealed
    }

    /// Rollup segments that take writes for the segments they cover: every
    /// pending or sealed one.
    pub fn routed(&self) -> BTreeSet<Segment> {
        self.sealed.iter().chain(&self.pending).copied().collect()
    }

    /// Pending rollups not yet sealed, which a restarted rollup finishes.
    pub fn unsealed(&self) -> Vec<Segment> {
        self.pending
            .iter()
            .filter(|segment| self.sealed.binary_search(segment).is_err())
            .copied()
            .collect()
    }

    /// The sealed rollup covering `segment`.
    pub fn sealed_cover(&self, segment: Segment) -> Option<Segment> {
        self.sealed
            .iter()
            .copied()
            .filter(|wide| covers(*wide, segment))
            .max_by_key(|wide| wide.width_ns)
    }

    /// Whether reads use `segment`: neither an unsealed rollup nor covered
    /// by a sealed one.
    pub fn is_visible(&self, segment: Segment) -> bool {
        if self.pending.binary_search(&segment).is_ok()
            && self.sealed.binary_search(&segment).is_err()
        {
            return false;
        }
        self.sealed_cover(segment).is_none()
    }

    /// `segments` with each covered one replaced by its sealed rollup, in
    /// order and without repeats.
    pub fn visible(&self, segments: impl IntoIterator<Item = Segment>) -> Vec<Segment> {
        let mut visible: Vec<Segment> = Vec::new();
        for segment in segments {
            let segment = self.sealed_cover(segment).unwrap_or(segment);
            if !visible.contains(&segment) {
                visible.push(segment);
            }
        }
        visible
    }

    /// What a rollup into `wide` copies, in order: each `base_ns` segment
    /// it covers, or the widest sealed rollup narrower than `wide` covering
    /// that segment in its place.
    pub fn sources(&self, wide: Segment, base_ns: i64) -> Vec<Segment> {
        let mut sources: Vec<Segment> = Vec::new();
        let mut start_ns = wide.start_ns;
        while start_ns < wide.start_ns.saturating_add(wide.width_ns) {
            let base = Segment::new(start_ns, base_ns);
            let source = self
                .sealed
                .iter()
                .copied()
                .filter(|narrow| narrow.width_ns < wide.width_ns && covers(*narrow, base))
                .max_by_key(|narrow| narrow.width_ns)
                .unwrap_or(base);
            if sources.last() != Some(&source) {
                sources.push(source);
            }
            start_ns = start_ns.saturating_add(base_ns);
        }
        sources
    }

    /// The rollup segments to fill, in order: unsealed ones first, then,
    /// for every block (a day, or a period if narrower) not yet expired
    /// under `retention`:
    ///
    /// - once it ended `delay` before `now_ns` and holds no rollup, the
    ///   tiling of its `base_ns` segments;
    /// - otherwise the tiling of each closed window of it holding no
    ///   rollup;
    /// - and once it ended, every window's rollup is sealed and none is
    ///   left to plan, the tiling of its windows into wider rollups.
    ///
    /// Tiles with fewer than two segments to merge are skipped.
    pub fn plan(
        &self,
        base_ns: i64,
        period_ns: Option<i64>,
        config: &RollupConfig,
        retention: Option<Duration>,
        now_ns: i64,
    ) -> Vec<Segment> {
        let mut planned = self.unsealed();
        let widths = widths(base_ns, period_ns);
        let Some(&block_ns) = widths.first() else {
            return planned;
        };
        let window_ns = i64::try_from(config.window.as_nanos()).unwrap_or(i64::MAX);
        let window_ns = widths
            .iter()
            .copied()
            .find(|width| *width <= window_ns && *width < block_ns);
        let delay_ns = i64::try_from(config.delay.as_nanos()).unwrap_or(i64::MAX);
        let retention_ns = retention.map(|r| i64::try_from(r.as_nanos()).unwrap_or(i64::MAX));
        let closed = |span: Segment| {
            span.start_ns
                .saturating_add(span.width_ns)
                .saturating_add(delay_ns)
                <= now_ns
        };
        let routed = self.routed();
        let blocks: BTreeSet<i64> = self
            .sizes
            .keys()
            .filter(|segment| segment.is_bounded() && segment.width_ns < block_ns)
            .filter_map(|segment| segment.start_ns.div_euclid(block_ns).checked_mul(block_ns))
            .collect();
        for block_start in blocks {
            let block = Segment::new(block_start, block_ns);
            let block_end = block_start.saturating_add(block_ns);
            if retention_ns.is_some_and(|r| block_end.saturating_add(r) <= now_ns)
                || routed
                    .iter()
                    .any(|wide| *wide == block || covers(*wide, block))
            {
                continue;
            }
            let inside: Vec<Segment> = routed
                .iter()
                .copied()
                .filter(|wide| overlaps(*wide, block))
                .collect();
            if closed(block) && inside.is_empty() {
                planned.extend(self.tile_base(block, base_ns, &widths, config));
                continue;
            }
            let Some(window_ns) = window_ns else {
                continue;
            };
            let mut windows_left = false;
            for index in 0..block_ns / window_ns {
                let window = Segment::new(block_start + index * window_ns, window_ns);
                if !closed(window) || inside.iter().any(|wide| overlaps(*wide, window)) {
                    continue;
                }
                let tiles = self.tile_base(window, base_ns, &widths, config);
                windows_left |= !tiles.is_empty();
                planned.extend(tiles);
            }
            if closed(block)
                && !windows_left
                && inside.iter().all(|wide| {
                    wide.width_ns <= window_ns && self.sealed.binary_search(wide).is_ok()
                })
            {
                planned.extend(self.tile_windows(block, window_ns, &widths, config));
            }
        }
        planned
    }

    /// The tiling of `span`'s `base_ns` segments with `widths` dividing it.
    fn tile_base(
        &self,
        span: Segment,
        base_ns: i64,
        widths: &[i64],
        config: &RollupConfig,
    ) -> Vec<Segment> {
        let slots = usize::try_from(span.width_ns / base_ns).unwrap_or(0);
        let mut bytes = vec![0; slots];
        let mut present = vec![0; slots];
        for slot in 0..slots {
            let start = span.start_ns + i64::try_from(slot).unwrap_or(0) * base_ns;
            if let Some(size) = self.sizes.get(&Segment::new(start, base_ns)) {
                bytes[slot] = *size;
                present[slot] = 1;
            }
        }
        tiles(span, base_ns, &bytes, &present, widths, config)
    }

    /// The tiling of `block`'s `window_ns` windows, each holding what reads
    /// see there, into rollups wider than a window.
    fn tile_windows(
        &self,
        block: Segment,
        window_ns: i64,
        widths: &[i64],
        config: &RollupConfig,
    ) -> Vec<Segment> {
        let slots = usize::try_from(block.width_ns / window_ns).unwrap_or(0);
        let mut bytes = vec![0; slots];
        let mut present = vec![0; slots];
        for (segment, size) in &self.sizes {
            if !covers(block, *segment) || !self.is_visible(*segment) {
                continue;
            }
            let slot = usize::try_from((segment.start_ns - block.start_ns) / window_ns).unwrap_or(0);
            if let Some(bytes) = bytes.get_mut(slot) {
                *bytes += size;
                present[slot] += 1;
            }
        }
        let wider: Vec<i64> = widths
            .iter()
            .copied()
            .filter(|width| *width > window_ns)
            .collect();
        tiles(block, window_ns, &bytes, &present, &wider, config)
    }
}

/// The rollups tiling `span`'s `slot_ns` slots holding `bytes` and
/// `present` segments with `widths` dividing `span`, each with at least two
/// segments.
fn tiles(
    span: Segment,
    slot_ns: i64,
    bytes: &[u64],
    present: &[usize],
    widths: &[i64],
    config: &RollupConfig,
) -> Vec<Segment> {
    let slot_widths: Vec<usize> = widths
        .iter()
        .filter(|width| {
            **width <= span.width_ns && span.width_ns % **width == 0 && **width % slot_ns == 0
        })
        .map(|width| usize::try_from(width / slot_ns).unwrap_or(1))
        .filter(|slots| *slots > 1)
        .chain([1])
        .collect();
    tile(bytes, &slot_widths, config.max_segment_bytes)
        .into_iter()
        .filter(|(first, len)| *len > 1 && present[*first..first + len].iter().sum::<usize>() >= 2)
        .map(|(first, len)| {
            Segment::new(
                span.start_ns + i64::try_from(first).unwrap_or(0) * slot_ns,
                i64::try_from(len).unwrap_or(1) * slot_ns,
            )
        })
        .collect()
}

/// The fewest aligned windows, as `(first slot, slots)`, tiling slots
/// holding `bytes`, each window one of `widths` slots wide (which must
/// include 1) and at most `max_bytes` unless a slot wide. Ties go to the
/// tiling found first, whose windows are widest first.
pub fn tile(bytes: &[u64], widths: &[usize], max_bytes: u64) -> Vec<(usize, usize)> {
    let slots = bytes.len();
    let mut prefix = vec![0u64; slots + 1];
    for (index, size) in bytes.iter().enumerate() {
        prefix[index + 1] = prefix[index].saturating_add(*size);
    }
    // best[end] = (window count, last window width) tiling slots [0, end).
    let mut best: Vec<Option<(usize, usize)>> = vec![None; slots + 1];
    best[0] = Some((0, 0));
    for end in 1..=slots {
        for &width in widths {
            if width == 0 || end % width != 0 {
                continue;
            }
            let start = end - width;
            let Some((count, _)) = best[start] else {
                continue;
            };
            if width > 1 && prefix[end] - prefix[start] > max_bytes {
                continue;
            }
            if best[end].is_none_or(|(current, _)| count + 1 < current) {
                best[end] = Some((count + 1, width));
            }
        }
    }
    let mut windows = Vec::new();
    let mut end = slots;
    while end > 0 {
        let Some((_, width)) = best[end] else {
            return Vec::new();
        };
        end -= width;
        windows.push((end, width));
    }
    windows.reverse();
    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: KeyScope = KeyScope::new(0x7f, 1);
    const HOUR: i64 = DAY_NS / 24;
    /// 2024-01-01.
    const DAY: i64 = 19_723 * DAY_NS;

    fn namespace() -> Namespace {
        Namespace::new("tenant").unwrap()
    }

    fn prefix(segment: Segment) -> Bytes {
        let mut bytes = BytesMut::new();
        SCOPE.write(&mut bytes, &namespace(), segment);
        bytes.freeze()
    }

    fn hours(count: i64) -> Vec<(Bytes, u64)> {
        (0..count)
            .map(|hour| (prefix(Segment::new(DAY + hour * HOUR, HOUR)), 10))
            .collect()
    }

    fn after(day: i64) -> i64 {
        day + DAY_NS + 2 * HOUR
    }

    #[test]
    fn markers_round_trip_and_never_parse_as_data() {
        let day = Segment::new(DAY, DAY_NS);
        for marker in [Marker::Pending, Marker::Sealed] {
            let scope = marker_segment(marker, day);
            assert_eq!(parse_marker(scope), Some((marker, day)));
            assert_eq!(data_segment(scope), day);
            let key = marker_key(SCOPE, &namespace(), marker, day);
            assert_eq!(SCOPE.parse(&key).unwrap().1, scope);
            assert!(is_marker(SCOPE, &key));
        }
        assert_eq!(parse_marker(day), None);
        assert!(!is_marker(SCOPE, &prefix(day)));
    }

    #[test]
    fn widths_divide_the_day_and_the_period() {
        assert_eq!(
            widths(HOUR, None),
            [24, 12, 8, 6, 4, 3, 2].map(|hours| hours * HOUR).to_vec()
        );
        assert_eq!(
            widths(HOUR, Some(6 * HOUR)),
            vec![6 * HOUR, 3 * HOUR, 2 * HOUR]
        );
        assert!(widths(DAY_NS, None).is_empty());
        assert!(widths(7_000_000_000, None).is_empty());
    }

    fn sized(segments: &[Segment], bytes: u64) -> HashMap<Segment, u64> {
        segments.iter().map(|segment| (*segment, bytes)).collect()
    }

    fn base_hours(first: i64, count: i64) -> Vec<Segment> {
        (first..first + count)
            .map(|hour| Segment::new(DAY + hour * HOUR, HOUR))
            .collect()
    }

    fn windows(first: i64, count: i64) -> Vec<Segment> {
        (first..first + count)
            .map(|index| Segment::new(DAY + index * 4 * HOUR, 4 * HOUR))
            .collect()
    }

    #[test]
    fn plans_a_closed_day_and_finishes_unsealed_rollups_first() {
        let rollups = Rollups::from_segments(SCOPE, &namespace(), hours(24));
        let config = RollupConfig::default();
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, after(DAY)),
            vec![Segment::new(DAY, DAY_NS)]
        );
        let expired = Some(Duration::from_secs(3_600));
        assert!(
            rollups
                .plan(HOUR, None, &config, expired, after(DAY))
                .is_empty()
        );

        let pending = Segment::new(DAY - DAY_NS, DAY_NS);
        let mut segments = hours(24);
        segments.push((prefix(marker_segment(Marker::Pending, pending)), 0));
        let rollups = Rollups::from_segments(SCOPE, &namespace(), segments);
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, after(DAY)),
            vec![pending, Segment::new(DAY, DAY_NS)]
        );
    }

    #[test]
    fn splits_a_day_over_the_cap_and_skips_lone_segments() {
        let rollups = Rollups::from_segments(SCOPE, &namespace(), hours(24));
        let config = RollupConfig {
            max_segment_bytes: 120,
            ..RollupConfig::default()
        };
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, after(DAY)),
            vec![
                Segment::new(DAY, 12 * HOUR),
                Segment::new(DAY + 12 * HOUR, 12 * HOUR)
            ]
        );
        let lone = Rollups::from_segments(SCOPE, &namespace(), hours(1));
        assert!(lone.plan(HOUR, None, &config, None, after(DAY)).is_empty());
    }

    #[test]
    fn a_seal_hides_what_it_covers_and_a_pending_rollup_hides_itself() {
        let day = Segment::new(DAY, DAY_NS);
        let hour = Segment::new(DAY + HOUR, HOUR);
        let mut segments = hours(24);
        segments.push((prefix(marker_segment(Marker::Pending, day)), 0));
        let pending = Rollups::from_segments(SCOPE, &namespace(), segments.clone());
        assert!(!pending.is_visible(day));
        assert!(pending.is_visible(hour));
        assert_eq!(pending.visible([hour]), vec![hour]);
        assert_eq!(pending.routed(), BTreeSet::from([day]));

        segments.push((prefix(marker_segment(Marker::Sealed, day)), 0));
        let sealed = Rollups::from_segments(SCOPE, &namespace(), segments);
        assert!(sealed.is_visible(day));
        assert!(!sealed.is_visible(hour));
        assert_eq!(
            sealed.visible([hour, Segment::new(DAY + 2 * HOUR, HOUR)]),
            vec![day]
        );
        assert!(sealed.unsealed().is_empty());
        assert!(
            sealed
                .plan(HOUR, None, &RollupConfig::default(), None, after(DAY))
                .is_empty()
        );
    }

    #[test]
    fn ignores_unbounded_segments() {
        let mut sizes = sized(&base_hours(0, 24), 10);
        sizes.insert(Segment::unbounded(i64::MIN), 10);
        let rollups = Rollups::new(Vec::new(), Vec::new(), sizes);
        assert_eq!(
            rollups.plan(HOUR, None, &RollupConfig::default(), None, after(DAY)),
            vec![Segment::new(DAY, DAY_NS)]
        );
    }

    #[test]
    fn rolls_up_each_window_as_it_closes_while_the_day_goes() {
        let rollups = Rollups::new(Vec::new(), Vec::new(), sized(&base_hours(0, 24), 10));
        let config = RollupConfig::default();
        // At 21:00 the windows up to 20:00 have closed, and 20:00 too soon.
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, DAY + 21 * HOUR),
            windows(0, 5)
        );
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, DAY + 20 * HOUR + HOUR / 12),
            windows(0, 4)
        );
        // A window over the cap splits into halves.
        let config = RollupConfig {
            max_segment_bytes: 30,
            ..RollupConfig::default()
        };
        assert_eq!(
            rollups.plan(HOUR, None, &config, None, DAY + 5 * HOUR),
            vec![Segment::new(DAY, 2 * HOUR), Segment::new(DAY + 2 * HOUR, 2 * HOUR)]
        );
    }

    #[test]
    fn rolls_a_closed_days_windows_into_the_day_once_all_are_sealed() {
        let config = RollupConfig::default();
        let mut sizes = sized(&base_hours(0, 24), 10);
        sizes.extend(sized(&windows(0, 5), 40));
        // The last window is still to roll up, so the day waits for it.
        let partial = Rollups::new(windows(0, 5), Vec::new(), sizes.clone());
        assert_eq!(
            partial.plan(HOUR, None, &config, None, after(DAY)),
            vec![Segment::new(DAY + 20 * HOUR, 4 * HOUR)]
        );
        // Unsealed, it is only finished.
        let pending = Rollups::new(windows(0, 5), windows(5, 1), sizes.clone());
        assert_eq!(
            pending.plan(HOUR, None, &config, None, after(DAY)),
            windows(5, 1)
        );
        sizes.extend(sized(&windows(5, 1), 40));
        let sealed = Rollups::new(windows(0, 6), windows(0, 6), sizes.clone());
        assert_eq!(
            sealed.plan(HOUR, None, &config, None, after(DAY)),
            vec![Segment::new(DAY, DAY_NS)]
        );
        // Too big for a day, the windows pair up into the fewest that fit.
        let capped = RollupConfig {
            max_segment_bytes: 130,
            ..RollupConfig::default()
        };
        assert_eq!(
            sealed.plan(HOUR, None, &capped, None, after(DAY)),
            vec![Segment::new(DAY, 12 * HOUR), Segment::new(DAY + 12 * HOUR, 12 * HOUR)]
        );
        // Too big to widen at all, they stay.
        let tight = RollupConfig {
            max_segment_bytes: 50,
            ..RollupConfig::default()
        };
        assert!(sealed.plan(HOUR, None, &tight, None, after(DAY)).is_empty());
        // Once the day holds a rollup wider than a window, it's done.
        let mut rolled = windows(0, 6);
        rolled.push(Segment::new(DAY, 12 * HOUR));
        let done = Rollups::new(rolled.clone(), rolled, sizes);
        assert!(done.plan(HOUR, None, &capped, None, after(DAY)).is_empty());
    }

    #[test]
    fn copies_the_widest_sealed_rollup_in_place_of_what_it_covers() {
        let day = Segment::new(DAY, DAY_NS);
        let mut sizes = sized(&base_hours(0, 24), 10);
        sizes.extend(sized(&windows(0, 2), 40));
        let rollups = Rollups::new(windows(0, 2), vec![day], sizes);
        let mut expected = windows(0, 2);
        expected.extend(base_hours(8, 16));
        assert_eq!(rollups.sources(day, HOUR), expected);
        assert_eq!(rollups.sources(windows(1, 1)[0], HOUR), base_hours(4, 4));
    }

    #[test]
    fn compaction_drains_segments_under_a_live_seal_of_a_wider_rollup() {
        let hook = superseded(SCOPE, HOUR);
        let day = Segment::new(DAY, DAY_NS);
        let window = Segment::new(DAY + 4 * HOUR, 4 * HOUR);
        let seal = marker_key(SCOPE, &namespace(), Marker::Sealed, day);
        let window_seal = marker_key(SCOPE, &namespace(), Marker::Sealed, window);
        let live = |prefix: &[u8]| prefix == seal.as_ref();
        let window_live = |prefix: &[u8]| prefix == window_seal.as_ref();
        let none = |_: &[u8]| false;
        let hour = prefix(Segment::new(DAY + 5 * HOUR, HOUR));
        assert!(hook(&hour, &live));
        assert!(hook(&hour, &window_live));
        assert!(!hook(&hour, &none));
        assert!(hook(&prefix(window), &live));
        assert!(!hook(&prefix(window), &window_live));
        assert!(!hook(&prefix(day), &live));
        assert!(!hook(&seal, &live));
        assert!(!hook(&window_seal, &live));
        assert!(!hook(&prefix(Segment::new(DAY + DAY_NS, HOUR)), &live));
    }

    #[test]
    fn tiles_with_the_fewest_windows() {
        let widths = [24, 12, 8, 6, 4, 3, 2, 1];
        assert_eq!(tile(&[10; 24], &widths, 1_000), vec![(0, 24)]);
        assert_eq!(tile(&[10; 24], &widths, 120), vec![(0, 12), (12, 12)]);
        assert_eq!(tile(&[2_000; 24], &widths, 1_000).len(), 24);
    }

    #[test]
    fn one_hot_hour_only_narrows_its_own_window() {
        let widths = [24, 12, 8, 6, 4, 3, 2, 1];
        let mut bytes = [10; 24];
        bytes[20] = 5_000;
        let windows = tile(&bytes, &widths, 1_000);
        assert_eq!(windows.len(), 5, "{windows:?}");
        assert!(windows.contains(&(20, 1)));
        assert_eq!(windows.iter().map(|(_, len)| len).sum::<usize>(), 24);
        for (first, len) in &windows {
            assert_eq!(first % len, 0, "{windows:?}");
        }
    }
}
