//! Precise key-range record counts over a SlateDB manifest.
//!
//! Built on SlateDB RFC 0020 primitives — [`slatedb::Db::manifest`],
//! [`slatedb::SstReader`], [`slatedb::SstFile::stats`],
//! [`slatedb::SstFile::index`] — plus [`slatedb::SstFile::read_block`] for
//! per-row precision on boundary blocks.
//!
//! The primary entry point is [`count_in_range`], which walks the manifest
//! and returns the exact number of physical write operations (puts,
//! deletes, merges) recorded within a query range.
//!
//! ## Walk structure
//!
//! For each SST overlapping the query:
//!
//! 1. **Back-scan** — walk blocks high-to-low, reading each, until we find
//!    one with actual in-query rows. That "witness" block fixes
//!    [`CountResult::covered_to`] from the highest in-query row key. If the
//!    witness is the SST's last data block AND fully contained in the
//!    query, [`slatedb::SstFile::info`]'s `last_entry` is a free witness
//!    (no row read needed). Blocks scanned above the witness contributed
//!    nothing in query, so we just keep walking.
//! 2. **Forward fill** — for blocks below the witness, use the cheap stats
//!    path when the block is fully contained in the query (no per-row I/O),
//!    and read rows when the block straddles `query.start` (to filter out
//!    below-query keys).
//!
//! ## Why the back-scan
//!
//! SlateDB's index stores separators, not first keys. A separator is the
//! shortest prefix of `first_key(block i)` that is still strictly greater
//! than `last_key(block i-1)` — so `sep[i] <= first_key(block i)`, often
//! strict (e.g. for blocks ending at `k230` and starting at `k280`, the
//! separator is `k28`). A block's separator can therefore place it
//! "inside" the query while every real key in the block sits above
//! `query.end`. Picking the highest *overlapping-by-separator* block as
//! the witness can yield a block with zero in-query rows. The back-scan
//! tolerates this by continuing downward until a block actually
//! contributes.
//!
//! ## Known gap
//!
//! `SsTableView::visible_range` projection is not applied. Callers using
//! view-projected SSTs may over-count rows outside the visible range.
//! Fixable with the same `read_block` primitive (clip rows to
//! `visible_range ∩ query`) — left as follow-up.
//!
//! ## Count semantics
//!
//! Counts are **physical write operations**, not visible rows. A key
//! written twice contributes 2 to `num_puts`; a tombstone counts as a
//! delete even if its put has compacted away. This matches the LogDb
//! append-only model where physical ops equal logical records. If an
//! update or delete API is added to LogDb, revisit.

use std::collections::VecDeque;
use std::ops::Bound;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use slatedb::manifest::{SortedRun, SsTableView, VersionedManifest};
use slatedb::{RowEntry, SstReader, SstStats, ValueDeletable};

use crate::{BytesRange, StorageError, StorageResult};

/// Counts of physical write operations within a query range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockOpCounts {
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
}

impl BlockOpCounts {
    pub fn num_rows(&self) -> u64 {
        self.num_puts + self.num_deletes + self.num_merges
    }

    pub fn add(&mut self, other: BlockOpCounts) {
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_merges += other.num_merges;
    }
}

/// Distribution of a key's in-query records across the **L0 tier** of the
/// walked tree(s). L0 SSTs are not range-partitioned, so a read must consult
/// every L0 SST — `ssts_total` therefore counts all of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L0Stats {
    /// L0 SSTs in the tier — all of them, since L0 isn't range-partitioned.
    pub ssts_total: u32,
    /// Of those, how many actually held an in-query record for the key
    /// (data locality: fewer ⇒ better-localized).
    pub ssts_with_data: u32,
    /// Across the SSTs that held data, how many data blocks the in-query
    /// records span — block-level locality. Divided by `ssts_with_data` it
    /// gives the average blocks-per-SST a read must touch.
    pub blocks_with_data: u32,
    /// In-query records (physical puts) found in L0.
    pub records: u64,
}

/// Distribution of a key's in-query records across the **sorted-run tier**.
/// Sorted runs are range-partitioned, so only the SST views covering the
/// query are checked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SortedRunStats {
    /// Sorted runs overlapping the query (a read merges across these).
    pub runs: u32,
    /// SST views across those runs that cover the query.
    pub ssts_total: u32,
    /// Of those, how many actually held an in-query record for the key.
    pub ssts_with_data: u32,
    /// Across the SSTs that held data, how many data blocks the in-query
    /// records span — block-level locality.
    pub blocks_with_data: u32,
    /// In-query records (physical puts) found in the sorted runs.
    pub records: u64,
}

/// LSM data-distribution statistics for a single [`count_in_range`] walk.
///
/// A *tier* here is one of the two kinds of level a SlateDB tree holds: the
/// **L0 tier** — the freshly-flushed SSTs, which are not range-partitioned
/// and may overlap each other in key space — and the **sorted-run tier** —
/// the compacted sorted runs, each a range-partitioned, non-overlapping run
/// of SSTs. A read consults both.
///
/// These stats characterize the *shape of the data* the query touched — how
/// the key's records are split between those two tiers — rather than the
/// cost of the count that produced it. Both tiers are summed over the trees
/// the walk visited: the unsegmented default tree plus every configured
/// segment whose prefix interval overlaps `query` (see [`count_in_range`]).
/// For a query confined to one segment's prefix — the LogDb case — they
/// describe that one segment's tree.
///
/// `l0.records + sorted_runs.records` is the count contributed by persisted
/// SSTs; writes not yet flushed are accounted separately by the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalkStats {
    /// The L0 tier's contribution; see [`L0Stats`].
    pub l0: L0Stats,
    /// The sorted-run tier's contribution; see [`SortedRunStats`].
    pub sorted_runs: SortedRunStats,
}

/// Private accumulator for one tier (L0 or sorted-run — see [`WalkStats`])
/// that the walk threads through `count_view`. The public [`L0Stats`] /
/// [`SortedRunStats`] are assembled from these.
#[derive(Default)]
struct TierWalk {
    ssts_total: u32,
    ssts_with_data: u32,
    blocks_with_data: u32,
    records: u64,
}

impl TierWalk {
    /// Records one block's in-query contribution: its records, and — when it
    /// holds at least one in-query row — that it's a data-spanning block.
    fn add_block(&mut self, counts: BlockOpCounts) {
        self.records += counts.num_puts;
        if counts.num_rows() > 0 {
            self.blocks_with_data += 1;
        }
    }
}

/// Result of a [`count_in_range`] walk: aggregate counts plus a witness
/// of the highest key actually observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CountResult {
    /// Per-variant counts of physical write operations in `range`.
    pub counts: BlockOpCounts,
    /// Inclusive upper bound on the keys reflected in `counts`. If
    /// `Some(k)`, every persisted entry in `[range.start, k]` is included.
    /// `None` if no SSTs contributed to the count (no overlap, empty
    /// manifest, etc.).
    ///
    /// Callers that need exact counts including not-yet-persisted writes
    /// can combine this with a scan over `(covered_to, range.end)`.
    pub covered_to: Option<Bytes>,
    /// Per-tier record distribution for this walk; see [`WalkStats`].
    pub stats: WalkStats,
}

/// Counts every physical write operation in the manifest whose key falls
/// in `query`, and returns a witness of how far up the walk observed data.
///
/// SlateDB stores data in two places: the unsegmented *default* tree
/// ([`VersionedManifest::l0`] / [`compacted`](VersionedManifest::compacted))
/// and, when a `PrefixExtractor` is configured, one independent LSM tree per
/// *segment* ([`VersionedManifest::segments`]) — each owning the key
/// interval `[prefix, prefix++)`. With an extractor every write routes to a
/// segment and the default tree is empty by construction, so a walk that
/// only consulted the default tree would miss all the data. This walks the
/// default tree and every segment whose interval overlaps `query`; for a
/// query confined to one routing prefix (the LogDb case) that is exactly one
/// segment.
///
/// Within each tree it covers L0 SSTs and the views returned by each
/// compacted sorted run's `tables_covering_range(query)`. SSTs without a
/// stats block (predating RFC 0020) are skipped with a tracing warning, so
/// the result undercounts in that case.
pub async fn count_in_range(
    manifest: &VersionedManifest,
    sst_reader: &SstReader,
    query: &BytesRange,
) -> StorageResult<CountResult> {
    let mut result = CountResult::default();
    let mut views = Vec::new();

    // The unsegmented default tree. Empty when an extractor is configured,
    // but walking it keeps the no-extractor case correct.
    collect_tree(
        manifest.l0(),
        manifest.compacted(),
        query,
        &mut views,
        &mut result,
    );

    // Each configured segment owns a disjoint prefix interval; walk only
    // those the query touches. This is where the data lives once a segment
    // extractor routes writes away from the default tree.
    for segment in manifest.segments() {
        if ranges_overlap(query, &prefix_range(segment.prefix())) {
            collect_tree(
                segment.l0(),
                segment.compacted(),
                query,
                &mut views,
                &mut result,
            );
        }
    }

    // Each SST's walk is independent and merging is order-insensitive (sums
    // and a max), so SSTs are read concurrently.
    let walks = stream::iter(views)
        .map(|(tier, view)| async move {
            count_view(sst_reader, &view, query)
                .await
                .map(|walk| (tier, walk))
        })
        .buffer_unordered(SST_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    for (tier, walk) in walks {
        result.counts.add(walk.counts);
        if let Some(covered_to) = walk.covered_to {
            bump_covered_to(&mut result.covered_to, covered_to);
        }
        match tier {
            Tier::L0 => {
                let l0 = &mut result.stats.l0;
                l0.ssts_total += walk.tier.ssts_total;
                l0.ssts_with_data += walk.tier.ssts_with_data;
                l0.blocks_with_data += walk.tier.blocks_with_data;
                l0.records += walk.tier.records;
            }
            Tier::SortedRun => {
                let runs = &mut result.stats.sorted_runs;
                runs.ssts_total += walk.tier.ssts_total;
                runs.ssts_with_data += walk.tier.ssts_with_data;
                runs.blocks_with_data += walk.tier.blocks_with_data;
                runs.records += walk.tier.records;
            }
        }
    }
    Ok(result)
}

const SST_READ_CONCURRENCY: usize = 16;

#[derive(Clone, Copy)]
enum Tier {
    L0,
    SortedRun,
}

/// Collects the SST views of one LSM tree (a default tree or a single
/// segment's tree) that the walk must read: every L0 SST plus the covering
/// views of each sorted run. Counts overlapping runs into `result`.
fn collect_tree(
    l0: &VecDeque<SsTableView>,
    compacted: &[SortedRun],
    query: &BytesRange,
    views: &mut Vec<(Tier, SsTableView)>,
    result: &mut CountResult,
) {
    // L0: not range-partitioned, so every SST is checked.
    views.extend(l0.iter().map(|view| (Tier::L0, view.clone())));

    // Sorted runs: range-partitioned, so only the views covering the query
    // are checked. A run counts as overlapping if it yields any such view.
    for run in compacted {
        let before = views.len();
        views.extend(
            run.tables_covering_range::<BytesRange>(query.clone())
                .into_iter()
                .map(|view| (Tier::SortedRun, view.clone())),
        );
        if views.len() > before {
            result.stats.sorted_runs.runs += 1;
        }
    }
}

/// One SST's contribution to a [`count_in_range`] walk.
#[derive(Default)]
struct ViewWalk {
    counts: BlockOpCounts,
    covered_to: Option<Bytes>,
    tier: TierWalk,
}

/// The key interval a segment owns: `[prefix, prefix++)`, where `prefix++`
/// is the smallest key strictly greater than every key beginning with
/// `prefix` (increment the last non-`0xFF` byte, dropping trailing `0xFF`s).
/// An all-`0xFF` or empty prefix has no upper bound.
fn prefix_range(prefix: &[u8]) -> BytesRange {
    let start = Bound::Included(Bytes::copy_from_slice(prefix));
    let mut end = prefix.to_vec();
    loop {
        match end.last().copied() {
            None => return BytesRange::new(start, Bound::Unbounded),
            Some(0xFF) => {
                end.pop();
            }
            Some(b) => {
                let last = end.len() - 1;
                end[last] = b + 1;
                return BytesRange::new(start, Bound::Excluded(Bytes::from(end)));
            }
        }
    }
}

async fn count_view(
    sst_reader: &SstReader,
    view: &SsTableView,
    query: &BytesRange,
) -> StorageResult<ViewWalk> {
    let mut walk = ViewWalk::default();
    let sst_file = sst_reader
        .open_with_handle(view.sst.clone())
        .map_err(StorageError::from_storage)?;
    let sst_id = sst_file.id();
    // This SST belongs to the tier (its index/stats footer were read) even
    // if no block ends up overlapping the query.
    walk.tier.ssts_total += 1;

    let (stats, index) = futures::try_join!(sst_file.stats(), sst_file.index())
        .map_err(StorageError::from_storage)?;
    let Some(stats) = stats else {
        tracing::warn!(?sst_id, "SST has no stats block; skipping");
        return Ok(walk);
    };
    let last_entry = sst_file.info().last_entry.clone();

    let n = index.len();
    let overlapping: Vec<usize> = (0..n)
        .filter(|i| ranges_overlap(query, &block_key_range(&index, *i, last_entry.as_ref())))
        .collect();

    // Back-scan: walk overlapping blocks high-to-low, reading each, until
    // we find one with in-query rows (see the module-level "Why the
    // back-scan" section for the rationale). That witness pins down
    // `covered_to`; blocks above contributed nothing (counted as zero),
    // blocks below get the cheap stats path on the second pass.
    let mut witness_pos: Option<usize> = None;
    for pos in (0..overlapping.len()).rev() {
        let i = overlapping[pos];
        let key_range = block_key_range(&index, i, last_entry.as_ref());
        let is_sst_last = i + 1 == n;
        let contained = range_contains(query, &key_range);

        // Shortcut: SST's last data block is contained in the query — its
        // last_entry is a free witness (already in SsTableInfo, no read).
        if is_sst_last
            && contained
            && let Some(last) = last_entry.as_ref()
        {
            let block_counts = block_counts_from_stats(&stats, i, sst_id);
            walk.counts.add(block_counts);
            walk.tier.add_block(block_counts);
            bump_covered_to(&mut walk.covered_to, last.clone());
            witness_pos = Some(pos);
            break;
        }

        let rows = sst_file
            .read_block(i)
            .await
            .map_err(StorageError::from_storage)?;
        let (block_counts, block_max) = count_rows_in_range_with_max(&rows, query);
        if let Some(max) = block_max {
            walk.counts.add(block_counts);
            walk.tier.add_block(block_counts);
            bump_covered_to(&mut walk.covered_to, max);
            witness_pos = Some(pos);
            break;
        }
        // No in-query rows in this block; the separator was misleading.
        // Continue scanning down. We've already added zero counts.
    }

    let Some(witness_pos) = witness_pos else {
        // No SST contents in query.
        return Ok(walk);
    };
    walk.tier.ssts_with_data += 1;

    // Forward pass for blocks below the witness. Cheap stats path when
    // contained; read for boundary blocks (needed to filter rows by query).
    for &i in &overlapping[..witness_pos] {
        let key_range = block_key_range(&index, i, last_entry.as_ref());
        let block_counts = if range_contains(query, &key_range) {
            block_counts_from_stats(&stats, i, sst_id)
        } else {
            let rows = sst_file
                .read_block(i)
                .await
                .map_err(StorageError::from_storage)?;
            count_rows_in_range_with_max(&rows, query).0
        };
        walk.counts.add(block_counts);
        walk.tier.add_block(block_counts);
    }

    Ok(walk)
}

fn bump_covered_to(covered_to: &mut Option<Bytes>, candidate: Bytes) {
    match covered_to {
        None => *covered_to = Some(candidate),
        Some(existing) if *existing < candidate => *covered_to = Some(candidate),
        _ => {}
    }
}

fn block_counts_from_stats(stats: &SstStats, i: usize, sst_id: ulid::Ulid) -> BlockOpCounts {
    match stats.block_stats.get(i) {
        Some(bs) => BlockOpCounts {
            num_puts: bs.num_puts as u64,
            num_deletes: bs.num_deletes as u64,
            num_merges: bs.num_merges as u64,
        },
        None => {
            // Index and stats disagree on block count — shouldn't happen
            // for an RFC-0020 SST. Skip the block rather than crash.
            tracing::warn!(?sst_id, block_index = i, "missing block_stats entry");
            BlockOpCounts::default()
        }
    }
}

fn count_rows_in_range_with_max(
    rows: &[RowEntry],
    query: &BytesRange,
) -> (BlockOpCounts, Option<Bytes>) {
    let mut counts = BlockOpCounts::default();
    let mut max_key: Option<Bytes> = None;
    for row in rows {
        if !query.contains(&row.key) {
            continue;
        }
        match row.value {
            ValueDeletable::Value(_) => counts.num_puts += 1,
            ValueDeletable::Merge(_) => counts.num_merges += 1,
            ValueDeletable::Tombstone => counts.num_deletes += 1,
        }
        match &max_key {
            None => max_key = Some(row.key.clone()),
            Some(m) if *m < row.key => max_key = Some(row.key.clone()),
            _ => {}
        }
    }
    (counts, max_key)
}

fn block_key_range(index: &[(u64, Bytes)], i: usize, sst_last_entry: Option<&Bytes>) -> BytesRange {
    let start = Bound::Included(index[i].1.clone());
    let end = if i + 1 < index.len() {
        Bound::Excluded(index[i + 1].1.clone())
    } else {
        match sst_last_entry {
            Some(last) => Bound::Included(last.clone()),
            None => Bound::Unbounded,
        }
    };
    BytesRange::new(start, end)
}

fn ranges_overlap(a: &BytesRange, b: &BytesRange) -> bool {
    lower_lt_upper(&a.start, &b.end) && lower_lt_upper(&b.start, &a.end)
}

fn lower_lt_upper(lower: &Bound<Bytes>, upper: &Bound<Bytes>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(l), Bound::Included(u)) => l <= u,
        (Bound::Included(l), Bound::Excluded(u)) => l < u,
        (Bound::Excluded(l), Bound::Included(u)) => l < u,
        (Bound::Excluded(l), Bound::Excluded(u)) => l < u,
    }
}

fn range_contains(outer: &BytesRange, inner: &BytesRange) -> bool {
    lower_le_lower(&outer.start, &inner.start) && upper_ge_upper(&outer.end, &inner.end)
}

fn lower_le_lower(a: &Bound<Bytes>, b: &Bound<Bytes>) -> bool {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) => true,
        (_, Unbounded) => false,
        (Included(ak), Included(bk)) => ak <= bk,
        (Included(ak), Excluded(bk)) => ak <= bk,
        (Excluded(ak), Included(bk)) => ak < bk,
        (Excluded(ak), Excluded(bk)) => ak <= bk,
    }
}

fn upper_ge_upper(a: &Bound<Bytes>, b: &Bound<Bytes>) -> bool {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) => true,
        (_, Unbounded) => false,
        (Included(ak), Included(bk)) => ak >= bk,
        (Included(ak), Excluded(bk)) => ak >= bk,
        (Excluded(ak), Included(bk)) => ak > bk,
        (Excluded(ak), Excluded(bk)) => ak >= bk,
    }
}

#[cfg(test)]
mod tests;
