use super::*;
use crate::storage::MergeOperator;
use slatedb::config::{FlushOptions, FlushType, PutOptions, SstBlockSize, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbBuilder};
use std::sync::Arc;

const PATH: &str = "/test";

/// Trivial merge operator: concatenate operands in batch order.
struct ConcatMerger;

impl MergeOperator for ConcatMerger {
    fn merge_batch(&self, _key: &Bytes, existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        let mut out = existing.map(|b| b.to_vec()).unwrap_or_default();
        for op in operands {
            out.extend_from_slice(op);
        }
        Bytes::from(out)
    }
}

async fn build_db_with_entries(entries: &[(&[u8], &[u8])]) -> (Arc<Db>, Arc<InMemory>) {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = DbBuilder::new(PATH, object_store.clone())
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .build()
        .await
        .unwrap();
    for (k, v) in entries {
        db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
            .await
            .unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    (Arc::new(db), object_store)
}

fn sst_reader(object_store: Arc<InMemory>) -> SstReader {
    SstReader::new(PATH, object_store, None, None)
}

fn mk_range(lo: &[u8], hi: &[u8]) -> BytesRange {
    BytesRange::new(
        Bound::Included(Bytes::copy_from_slice(lo)),
        Bound::Excluded(Bytes::copy_from_slice(hi)),
    )
}

/// Builds ~250 entries with keys `kNNN` so an SST with 1 KiB blocks
/// produces several blocks. Each entry is ~30 bytes after encoding,
/// so ~30 entries/block → ~8+ blocks per SST.
fn many_entries(count: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("k{:03}", i).into_bytes();
            let val = format!("v{:03}aaaaaaaaaaaaaa", i).into_bytes();
            (key, val)
        })
        .collect()
}

#[tokio::test]
async fn empty_manifest_counts_zero() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = DbBuilder::new(PATH, object_store.clone())
        .build()
        .await
        .unwrap();
    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &BytesRange::unbounded(),
    )
    .await
    .unwrap();
    assert_eq!(result.counts, BlockOpCounts::default());
    assert!(result.covered_to.is_none());
}

#[tokio::test]
async fn unbounded_query_counts_every_put() {
    let entries = many_entries(250);
    let entries_ref: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let (db, object_store) = build_db_with_entries(&entries_ref).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &BytesRange::unbounded(),
    )
    .await
    .unwrap();
    assert_eq!(result.counts.num_puts, 250);
    assert_eq!(result.counts.num_deletes, 0);
    assert_eq!(result.counts.num_merges, 0);
    // Unbounded query → highest SST key is the witness. Last inserted key
    // is `k249`; the SST's last_entry is exactly that.
    assert_eq!(result.covered_to.as_deref(), Some(b"k249" as &[u8]));
}

/// The boundary-block path: query slices through interior blocks, so the
/// contained-path stats counts would over-count. Asserts the exact answer.
#[tokio::test]
async fn subrange_query_counts_exactly() {
    let entries = many_entries(250);
    let entries_ref: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let (db, object_store) = build_db_with_entries(&entries_ref).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &mk_range(b"k100", b"k150"),
    )
    .await
    .unwrap();
    assert_eq!(result.counts.num_puts, 50, "exactly the 50 keys k100..k150");
    // Boundary block at the top of query — last counted row is k149.
    assert_eq!(result.covered_to.as_deref(), Some(b"k149" as &[u8]));
}

#[tokio::test]
async fn query_outside_keyspace_counts_zero() {
    let entries = many_entries(50);
    let entries_ref: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let (db, object_store) = build_db_with_entries(&entries_ref).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &mk_range(b"z", b"z\xff"),
    )
    .await
    .unwrap();
    assert_eq!(result.counts, BlockOpCounts::default());
    assert!(result.covered_to.is_none());
}

#[tokio::test]
async fn counts_across_multiple_l0_ssts() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = DbBuilder::new(PATH, object_store.clone())
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .build()
        .await
        .unwrap();

    for i in 0u8..5 {
        db.put_with_options(
            &[b'k', i],
            &[b'v', i],
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    for i in 5u8..10 {
        db.put_with_options(
            &[b'k', i],
            &[b'v', i],
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();

    let manifest = db.manifest();
    assert!(manifest.l0().len() >= 2);

    let result = count_in_range(
        &manifest,
        &sst_reader(object_store),
        &BytesRange::unbounded(),
    )
    .await
    .unwrap();
    assert_eq!(result.counts.num_puts, 10);
    // covered_to comes from the SST with the highest last_entry — `k\x09`.
    assert_eq!(result.covered_to.as_deref(), Some(&b"k\x09"[..]));
}

/// Flushes `batches` separately so each becomes its own L0 SST, then
/// returns the DB + object store. Keys are taken verbatim from each
/// batch so callers control which SST a key lands in.
async fn build_db_with_l0_batches(batches: &[&[(&[u8], &[u8])]]) -> (Arc<Db>, Arc<InMemory>) {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = DbBuilder::new(PATH, object_store.clone())
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .build()
        .await
        .unwrap();
    for batch in batches {
        for (k, v) in *batch {
            db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
                .await
                .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    (Arc::new(db), object_store)
}

#[tokio::test]
async fn walk_stats_report_manifest_shape() {
    let a: &[(&[u8], &[u8])] = &[(b"k000", b"v0"), (b"k001", b"v1")];
    let b: &[(&[u8], &[u8])] = &[(b"k100", b"v2"), (b"k101", b"v3")];
    let (db, object_store) = build_db_with_l0_batches(&[a, b]).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &BytesRange::unbounded(),
    )
    .await
    .unwrap();

    // Two flushes, no compaction → all data in two L0 SSTs, no runs.
    assert_eq!(result.stats.l0.ssts_total, 2);
    assert_eq!(result.stats.l0.ssts_with_data, 2, "both SSTs contribute");
    assert_eq!(
        result.stats.l0.blocks_with_data, 2,
        "two small SSTs, one block of data each"
    );
    assert_eq!(result.stats.l0.records, 4, "two records per batch");
    assert_eq!(result.stats.sorted_runs, SortedRunStats::default());
}

#[tokio::test]
async fn walk_stats_check_every_l0_but_attribute_data_to_one() {
    // L0 is not range-partitioned: a query matching only the second SST
    // must still check the first to know it has nothing in range — but
    // only the second holds data.
    let a: &[(&[u8], &[u8])] = &[(b"k000", b"v0"), (b"k001", b"v1")];
    let b: &[(&[u8], &[u8])] = &[(b"k100", b"v2"), (b"k101", b"v3")];
    let (db, object_store) = build_db_with_l0_batches(&[a, b]).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &mk_range(b"k100", b"k200"),
    )
    .await
    .unwrap();

    assert_eq!(result.counts.num_puts, 2, "only the k1xx batch matches");
    assert_eq!(result.stats.l0.ssts_total, 2, "both L0 SSTs are present");
    assert_eq!(
        result.stats.l0.ssts_with_data, 1,
        "only the second SST holds in-range rows"
    );
    assert_eq!(result.stats.l0.records, 2);
}

#[tokio::test]
async fn walk_stats_attribute_records_on_boundary_slice() {
    // A subrange that slices interior blocks still attributes the exact
    // record count to the (single) L0 SST it lives in.
    let entries = many_entries(250);
    let entries_ref: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let (db, object_store) = build_db_with_entries(&entries_ref).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &mk_range(b"k100", b"k150"),
    )
    .await
    .unwrap();

    assert_eq!(result.counts.num_puts, 50);
    assert_eq!(result.stats.l0.ssts_total, 1);
    assert_eq!(result.stats.l0.ssts_with_data, 1);
    // 50 records of ~30 bytes over 1 KiB blocks span multiple blocks.
    assert!(
        result.stats.l0.blocks_with_data >= 2,
        "a 50-record slice should span several blocks, got {}",
        result.stats.l0.blocks_with_data
    );
    assert_eq!(result.stats.l0.records, 50);
    assert_eq!(result.stats.sorted_runs, SortedRunStats::default());
}

/// Routes writes into per-segment trees by a fixed-length key prefix,
/// mirroring how LogDb's `LogSegmentExtractor` partitions the keyspace.
#[derive(Debug)]
struct FixedPrefixExtractor(usize);

impl slatedb::PrefixExtractor for FixedPrefixExtractor {
    fn name(&self) -> &str {
        "test/fixed-prefix"
    }

    fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
        let len = match target {
            slatedb::PrefixTarget::Point(b) => b.as_ref().len(),
            slatedb::PrefixTarget::Prefix(b) => b.as_ref().len(),
        };
        (len >= self.0).then_some(self.0)
    }
}

/// Builds a DB whose writes are routed into per-segment trees by their
/// first `prefix_len` bytes, then flushed to L0.
async fn build_segmented_db(
    entries: &[(&[u8], &[u8])],
    prefix_len: usize,
) -> (Arc<Db>, Arc<InMemory>) {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let db = DbBuilder::new(PATH, object_store.clone())
        .with_sst_block_size(SstBlockSize::Block1Kib)
        .with_segment_extractor(Arc::new(FixedPrefixExtractor(prefix_len)))
        .build()
        .await
        .unwrap();
    for (k, v) in entries {
        db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
            .await
            .unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    (Arc::new(db), object_store)
}

#[tokio::test]
async fn walks_per_segment_trees_when_extractor_configured() {
    // Regression: with a segment extractor, writes route into per-segment
    // trees and the default tree is empty. A walk that consulted only the
    // default tree (manifest.l0()/compacted()) would count zero — this is
    // the production LogDb layout, since LogDb always installs an
    // extractor.
    let entries: &[(&[u8], &[u8])] = &[
        (b"aa-1", b"v"),
        (b"aa-2", b"v"),
        (b"aa-3", b"v"),
        (b"bb-1", b"v"),
    ];
    let (db, object_store) = build_segmented_db(entries, 2).await;
    let manifest = db.manifest();

    // Precondition: data lives in segments, not the default tree.
    assert!(
        manifest.l0().is_empty() && manifest.compacted().is_empty(),
        "default tree must be empty under a segment extractor"
    );
    assert_eq!(manifest.segments().len(), 2, "one segment per prefix");

    // A query confined to the `aa` prefix must find exactly its three
    // rows and walk only that segment's tree (not the `bb` segment).
    let result = count_in_range(
        &manifest,
        &sst_reader(object_store),
        &mk_range(b"aa", b"ab"),
    )
    .await
    .unwrap();
    assert_eq!(result.counts.num_puts, 3, "the three aa-* rows");
    assert_eq!(result.covered_to.as_deref(), Some(b"aa-3" as &[u8]));
    // The data and its stats come from the walked segment's tree, not
    // the empty default tree.
    assert!(
        result.stats.l0.ssts_total >= 1,
        "must check the aa segment's L0 SST"
    );
    assert_eq!(result.stats.l0.ssts_with_data, 1);
    assert_eq!(result.stats.l0.blocks_with_data, 1, "three rows, one block");
    assert_eq!(result.stats.l0.records, 3);
}

#[tokio::test]
async fn counts_tombstones_and_merges() {
    let object_store: Arc<InMemory> = Arc::new(InMemory::new());
    let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMerger);
    let db = DbBuilder::new(PATH, object_store.clone())
        .with_merge_operator(Arc::new(
            crate::storage::slate::SlateDbStorage::merge_operator_adapter(merge_op),
        ))
        .build()
        .await
        .unwrap();

    // Use distinct keys: slatedb collapses same-key ops in the memtable
    // before flush (put+delete -> tombstone, put+merge -> value), so we
    // need one key per ValueDeletable variant to land in the SST.
    db.put_with_options(
        b"k1",
        b"v1",
        &PutOptions::default(),
        &WriteOptions::default(),
    )
    .await
    .unwrap();
    db.put_with_options(
        b"k2",
        b"v2",
        &PutOptions::default(),
        &WriteOptions::default(),
    )
    .await
    .unwrap();
    db.delete(b"k3").await.unwrap();
    let mut batch = slatedb::WriteBatch::new();
    batch.merge(b"k4", b"operand");
    db.write_with_options(batch, &WriteOptions::default())
        .await
        .unwrap();

    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &BytesRange::unbounded(),
    )
    .await
    .unwrap();
    assert_eq!(result.counts.num_puts, 2);
    assert_eq!(result.counts.num_deletes, 1);
    assert_eq!(result.counts.num_merges, 1);
}

/// The contained-interior-at-top case: query.end falls exactly on a
/// block separator. The block just below is contained AND interior
/// (the SST has more data above). count must still produce an exact
/// answer by reading that block.
#[tokio::test]
async fn subrange_query_ending_at_block_boundary() {
    // Build a DB with enough entries that 1 KiB blocks split frequently,
    // then query `[k000, k100)` which is unlikely to align perfectly
    // with a block boundary BUT exercises the "highest-overlapping is
    // contained, SST has more data above" path for the SST containing
    // entries below k100.
    let entries = many_entries(250);
    let entries_ref: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    let (db, object_store) = build_db_with_entries(&entries_ref).await;

    let result = count_in_range(
        &db.manifest(),
        &sst_reader(object_store),
        &mk_range(b"k000", b"k100"),
    )
    .await
    .unwrap();
    // Whether the boundary lands inside a block or on a separator, the
    // contract is the same: exactly 100 puts, and the witness key is
    // the highest one observed in `[k000, k100)`.
    assert_eq!(result.counts.num_puts, 100);
    assert_eq!(result.covered_to.as_deref(), Some(b"k099" as &[u8]));
}

#[test]
fn range_contains_cases() {
    use Bound::*;
    let b = |s: &[u8]| Bytes::copy_from_slice(s);
    let r = |lo, hi| BytesRange::new(lo, hi);

    // Unbounded outer contains anything
    assert!(range_contains(
        &BytesRange::unbounded(),
        &r(Included(b(b"x")), Excluded(b(b"y"))),
    ));
    // Equal ranges contain each other
    let eq = r(Included(b(b"a")), Excluded(b(b"z")));
    assert!(range_contains(&eq, &eq));
    // Strict containment
    assert!(range_contains(
        &r(Included(b(b"a")), Excluded(b(b"z"))),
        &r(Included(b(b"c")), Excluded(b(b"d"))),
    ));
    // Inner extends below outer
    assert!(!range_contains(
        &r(Included(b(b"b")), Excluded(b(b"z"))),
        &r(Included(b(b"a")), Excluded(b(b"d"))),
    ));
    // Inner extends above outer
    assert!(!range_contains(
        &r(Included(b(b"a")), Excluded(b(b"d"))),
        &r(Included(b(b"a")), Excluded(b(b"z"))),
    ));
    // Outer Included(k) vs inner Excluded(k) — outer's lower allows points inner doesn't need
    assert!(range_contains(
        &r(Included(b(b"a")), Included(b(b"z"))),
        &r(Excluded(b(b"a")), Excluded(b(b"z"))),
    ));
    // Outer Excluded(k) vs inner Included(k) at start — outer starts past k, inner needs k
    assert!(!range_contains(
        &r(Excluded(b(b"a")), Unbounded),
        &r(Included(b(b"a")), Unbounded),
    ));
}

#[test]
fn ranges_overlap_all_combinations() {
    use Bound::*;
    let b = |s: &[u8]| Bytes::copy_from_slice(s);

    // Disjoint
    assert!(!ranges_overlap(
        &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
        &BytesRange::new(Included(b(b"c")), Excluded(b(b"d"))),
    ));
    // Touching at exclusive boundary — no overlap
    assert!(!ranges_overlap(
        &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
        &BytesRange::new(Included(b(b"b")), Excluded(b(b"c"))),
    ));
    // Overlapping
    assert!(ranges_overlap(
        &BytesRange::new(Included(b(b"a")), Excluded(b(b"c"))),
        &BytesRange::new(Included(b(b"b")), Excluded(b(b"d"))),
    ));
    // Contained
    assert!(ranges_overlap(
        &BytesRange::new(Included(b(b"a")), Excluded(b(b"z"))),
        &BytesRange::new(Included(b(b"b")), Excluded(b(b"c"))),
    ));
    // Unbounded
    assert!(ranges_overlap(
        &BytesRange::unbounded(),
        &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
    ));
    // Inclusive upper meets inclusive lower — overlap at the shared point
    assert!(ranges_overlap(
        &BytesRange::new(Included(b(b"a")), Included(b(b"b"))),
        &BytesRange::new(Included(b(b"b")), Included(b(b"c"))),
    ));
}
