// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{
    Delta, Durability as CoordinatorDurability, Flusher, WriteCoordinator, WriteCoordinatorHandle,
    WriteError, WriteHandle,
};
use common::discovery::{
    CatalogBatch, DiscoveryCache, DiscoveryValue, names as catalog_names, values as catalog_values,
};
use common::storage::compaction::{MergeHolds, SegmentCompaction};
use common::storage::rollup::Rollups;
use common::storage::slate::warm::WarmPlan;
use common::storage::{RecordOp, Storage, StorageRead, Ttl, WriteOptions};
use common::{
    BytesRange, SharedDbCache, StorageBuilder, StorageReaderRuntime, StorageSemantics, WarmWindow,
    create_storage_read,
};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use slatedb::config::DbReaderOptions;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use crate::Namespace;
use crate::analyzer::DEFAULT_ANALYZER;
use crate::codec::{
    ObjectRef, SegmentSummary, StoredObject, decode_forward_key, decode_labels, decode_object_id,
    decode_postings, decode_stream_id, dictionary_key, encode_labels, encode_object_id,
    encode_postings, encode_stream_id, field_stats_key, forward_key, forward_prefix,
    next_object_id_key, next_stream_id_key, posting_key, rollup_dictionary_key, rollup_forward_key,
    rollup_forward_prefix, rollup_next_stream_id_key, rollup_posting_key, rollup_prefix,
    segment_for, segment_prefix,
};
use crate::compaction::{Compactor, WrittenObject};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::memory::{BudgetPermit, MemoryBudget, WritePermit, Writer};
use crate::model::{
    Label, Labels, LogBatch, LogEntry, LogRow, SegmentId, StreamFingerprint, StreamId,
};
use crate::object::{BuiltObject, ObjectBuilder, ObjectLocation, ObjectProperties, object_records};
use crate::search::{
    FieldStats, IndexDelta, block_max_scores, encode_field_stats, source_matches, term_index_ops,
};

mod block_cache;
mod query;
mod rollup;
mod summary;
mod unfiltered;
mod write;

pub(crate) use block_cache::BlockCache;
#[cfg(feature = "bench-internals")]
pub(crate) use query::run_entries;
pub(crate) use query::{Boundaries, MetadataNeeds, SampleRead, ScanTargets, StreamFilter};
use unfiltered::UnfilteredReads;
use write::*;

/// Read units fetched concurrently within one query segment.
const PAGE_READ_CONCURRENCY: usize = 16;
/// Streams whose forward labels and run records are read concurrently.
const STREAM_METADATA_CONCURRENCY: usize = 32;
/// Fewest selected streams, on average, read by one range scan over their ID
/// span instead of an operation per stream: below it, both cost a few point
/// operations.
const SPAN_SCAN_MIN_STREAMS: usize = 8;
/// Narrowest gap of unselected stream IDs that may end a span, and the most
/// IDs spans may cover per selected stream; past either, unselected streams'
/// records outweigh another operation.
const SPAN_SCAN_MAX_SPREAD: usize = 16;
/// Most range scans one selection is read by.
const SPAN_SCAN_MAX_SPANS: usize = 4;
/// Segments whose stream metadata is listed concurrently by `scan_targets`.
/// A cold listing is a chain of dependent object-store reads, so a day of
/// hourly segments is listed at once; a query keeps every listing until it
/// reads it anyway, so this only adds the block reads in flight.
const SEGMENT_LIST_CONCURRENCY: usize = 32;
/// Segments a query reads at once, each fetching up to
/// [`PAGE_READ_CONCURRENCY`] units: a segment's reads are a few dependent
/// object-store round trips, which segments read together overlap, and
/// [`DECODE_BYTES_IN_FLIGHT`] bounds what they hold.
const SEGMENT_READ_CONCURRENCY: usize = 16;
/// Segments past the one being consumed whose streams an early-stopping log
/// read selects ahead. Cold, a segment's selection is a few dependent
/// object-store round trips, so selecting segment by segment costs a day's
/// read those round trips 24 times over; each selected segment holds only
/// its stream metadata and the read units [`PIPELINE_UNITS`] admits.
const SEGMENT_LOOKAHEAD: usize = 4;
/// Read units an early-stopping log read keeps in flight across segments.
const PIPELINE_UNITS: usize = 2 * PAGE_READ_CONCURRENCY;
/// Estimated decoded bytes a budget's read units may hold at once, which
/// bounds the memory concurrent segments add.
const DECODE_BYTES_IN_FLIGHT: u32 = 256 << 20;
/// Segments whose discovery catalogs are scanned concurrently.
const SEGMENT_DISCOVERY_CONCURRENCY: usize = 16;
/// Larger per-segment stream sets are recomputed rather than cached, which
/// bounds the series cache to roughly `capacity * this` label sets.
const SERIES_CACHE_MAX_STREAMS: usize = 2_048;
const WRITE_CHANNEL: &str = "write";

/// How queries list and read segments; tests and profiles override the
/// defaults to compare the alternatives.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadTuning {
    /// [`SEGMENT_LIST_CONCURRENCY`].
    pub(crate) list_concurrency: usize,
    /// [`SEGMENT_READ_CONCURRENCY`].
    pub(crate) read_concurrency: usize,
    /// Whether streams selected by exact labels are cached against those
    /// labels' postings rather than the segment's stream counter, so a cold
    /// listing reads no counter before them.
    pub(crate) postings_validate: bool,
    /// Whether the runs of streams the exact labels alone select are read
    /// while their labels are.
    pub(crate) overlap_runs: bool,
    /// [`SEGMENT_LOOKAHEAD`]; zero reads segment by segment, selecting only
    /// the next segment's streams meanwhile.
    pub(crate) segment_lookahead: usize,
    /// [`PIPELINE_UNITS`].
    pub(crate) pipeline_units: usize,
    /// Whether queries read through [`UnfilteredReads`].
    pub(crate) unfiltered_reads: bool,
    /// Whether [`UnfilteredReads`] keeps reads filtered when every SST they
    /// may consult has its filter cached.
    pub(crate) cached_filters: bool,
}

impl Default for ReadTuning {
    fn default() -> Self {
        Self {
            list_concurrency: SEGMENT_LIST_CONCURRENCY,
            read_concurrency: SEGMENT_READ_CONCURRENCY,
            postings_validate: true,
            overlap_runs: true,
            segment_lookahead: SEGMENT_LOOKAHEAD,
            pipeline_units: PIPELINE_UNITS,
            unfiltered_reads: true,
            cached_filters: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    pub streams: usize,
    pub pages: usize,
    pub rows: usize,
}

/// An applied write and the budget it holds.
struct AppliedWrite {
    handle: WriteHandle<WriteReport>,
    permit: WritePermit,
}

impl AppliedWrite {
    /// Frees the write's budget once it is in storage, or the writer has
    /// failed or stopped, so the budget covers every applied delta still in
    /// memory.
    async fn written(mut self) {
        let _ = self.handle.wait(CoordinatorDurability::Written).await;
        drop(self.permit);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

/// Pages a query may still read, shareable across the databases it spans.
#[derive(Debug)]
pub(crate) struct PageBudget {
    limit: usize,
    remaining: AtomicUsize,
    decoding: Semaphore,
    /// Taken while a unit acquires its permits, so this budget's units take
    /// both its and the process's in the order they first ask. A unit then
    /// only ever waits behind units it reads after, or other budgets', and
    /// those holding permits release them without waiting on it.
    admission: Mutex<()>,
}

/// A read unit's share of its [`PageBudget`] and of the process's
/// [`MemoryBudget`].
#[derive(Debug)]
pub(crate) struct DecodePermit<'a> {
    _budget: SemaphorePermit<'a>,
    _process: BudgetPermit,
}

impl PageBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: AtomicUsize::new(limit),
            decoding: Semaphore::new(DECODE_BYTES_IN_FLIGHT as usize),
            admission: Mutex::new(()),
        }
    }

    /// Holds `bytes` of decoding in this budget and in `memory` until the
    /// permit drops. A unit larger than either allowance waits for all of
    /// it rather than forever.
    async fn decode(&self, memory: &MemoryBudget, bytes: u64) -> Result<DecodePermit<'_>> {
        let weight = u32::try_from(bytes)
            .unwrap_or(u32::MAX)
            .clamp(1, DECODE_BYTES_IN_FLIGHT);
        let _admission = self.admission.lock().await;
        let budget = self
            .decoding
            .acquire_many(weight)
            .await
            .map_err(|_| Error::Query("decode budget closed".into()))?;
        Ok(DecodePermit {
            _budget: budget,
            _process: memory.decode(u64::from(weight)).await?,
        })
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    fn take(&self) -> Result<()> {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .map(drop)
            .map_err(|_| Error::Query(format!("query exceeded max_pages ({})", self.limit)))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct QueryEstimate {
    pub compressed_bytes: u64,
    pub lines: u64,
    /// Distinct stored objects touched by the query.
    pub pages: usize,
    /// Object block ranges planned for the selected streams. One page may
    /// require several sparse ranges.
    pub read_units: usize,
}

#[derive(Clone, Copy, Debug)]
struct PageRetention {
    physical_ttl: Ttl,
    expires_at_unix_ms: Option<u64>,
    written_at_unix_ms: u64,
}

/// Single-writer/single-node log database over the common SlateDB abstraction.
pub struct LogDb {
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    segment_ns: i64,
    /// Discovery rollup period, a whole multiple of `segment_ns`.
    rollup_ns: Option<i64>,
    write_handle: Option<WriteCoordinatorHandle<LogsWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<LogsWriteDelta, LogsFlusher>>>,
    caches: DiscoveryCaches,
    blocks: BlockCache,
    /// This database's ID in `blocks`.
    database_id: u64,
    /// The process's read and write memory budgets, shared like `blocks`.
    memory: MemoryBudget,
    /// [`SPAN_SCAN_MIN_STREAMS`], overridable so tests cover both read paths.
    span_scan_min_streams: usize,
    /// [`SPAN_SCAN_MAX_SPREAD`], overridable for profiling.
    span_scan_max_spread: usize,
    /// Whether queries depending on only some stream labels build just those
    /// from postings; off in tests and profiles comparing with full reads.
    project_labels: bool,
    /// [`ReadTuning::default`] outside tests and profiles.
    tuning: ReadTuning,
    /// Rollup segments, pending or sealed, that take the writes of the
    /// segments they cover. Ingest holds the read lock until its write is
    /// applied, so a rollup holding the write lock sees none in flight.
    rolled: tokio::sync::RwLock<HashMap<Namespace, BTreeSet<SegmentId>>>,
    /// Serializes rollup passes.
    rollup_lock: Mutex<()>,
    /// Holds on the merges of segments a rollup copies into.
    merge_holds: MergeHolds,
    retention: Option<Duration>,
    /// How long after a segment ends it is summarized: once compaction
    /// finalizes it, when late writes are rare.
    settle_after: Duration,
    /// [`summary::SUMMARY_BUCKET`], overridable so tests fit buckets to
    /// their short segments.
    summary_bucket: Duration,
    /// The current summary of each segment the last pass saw, so passes
    /// read only a segment's next object ID to find it unchanged.
    summarized: std::sync::Mutex<HashMap<(Namespace, SegmentId), SegmentSummary>>,
    /// Likewise the current summary of each rollup period.
    period_summaries:
        std::sync::Mutex<HashMap<(Namespace, SegmentId), crate::codec::PeriodSummary>>,
}

/// Where discovery reads one slice of a time range from.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Partition {
    Segment(SegmentId),
    /// The rollup of every segment in the period starting here.
    Rollup(SegmentId),
}

/// Per-partition discovery results, each tagged with the partition's stream
/// counter when it was read. Discovery only changes when a partition gains
/// a stream, and every write that allocates one rewrites the counter in the
/// same batch as its catalog terms and postings, so an entry is current
/// exactly while the counter is unchanged. This holds in reader processes,
/// which see no writes to invalidate from, and for late writes into
/// segments wall-clock time has left. Sized so a day of 60s segments fits.
struct DiscoveryCaches {
    label_names: DiscoveryCache<(Namespace, Partition), Versioned<Vec<String>>>,
    label_values: DiscoveryCache<(Namespace, Partition, String), Versioned<Vec<String>>>,
    /// Streams selected by a set of exact matchers, before the remaining
    /// selector matchers run.
    series: DiscoveryCache<SeriesKey, Versioned<Vec<Arc<Labels>>>>,
    /// A segment's streams holding every exact label, with their IDs, as
    /// queries select them: with all their labels, or only those a set of
    /// names projects; weighed by label bytes. See
    /// [`ReadTuning::postings_validate`] for when an entry is current.
    streams: moka::sync::Cache<StreamsKey, SegmentStreams>,
}

/// A partition's next stream ID, `None` while it has none (or expired).
type PartitionVersion = Option<StreamId>;
type Versioned<V> = (PartitionVersion, V);

type SeriesKey = (Namespace, Partition, Vec<Label>);
type SegmentSeries = Arc<Versioned<Vec<Arc<Labels>>>>;
type StreamsKey = (Namespace, SegmentId, Vec<Label>, Option<Arc<[String]>>);
type SegmentStreams = Arc<Versioned<Vec<(StreamId, Arc<Labels>)>>>;

/// Byte budget of [`DiscoveryCaches::streams`].
const STREAMS_CACHE_BYTES: u64 = 64 * 1024 * 1024;

impl DiscoveryCaches {
    fn new() -> Self {
        // Entries are inserted as closed, so the TTL is never consulted.
        Self {
            label_names: DiscoveryCache::new(4_096, Duration::ZERO),
            label_values: DiscoveryCache::weighted(
                16_384,
                Duration::ZERO,
                64 << 20,
                |(_, values)| common::discovery::strings_weight(values),
            ),
            series: DiscoveryCache::new(1_024, Duration::ZERO),
            streams: moka::sync::Cache::builder()
                .max_capacity(STREAMS_CACHE_BYTES)
                .weigher(|_, streams: &SegmentStreams| {
                    let bytes: usize = streams
                        .1
                        .iter()
                        .map(|(_, labels)| {
                            48 + labels
                                .iter()
                                .map(|label| 48 + label.name.len() + label.value.len())
                                .sum::<usize>()
                        })
                        .sum();
                    u32::try_from(bytes + 64).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }
}

impl LogDb {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let cache = SharedDbCache::from_config(&config.storage).await?;
        let blocks = BlockCache::new(config.block_cache_capacity_bytes);
        let memory = MemoryBudget::new(&config.memory);
        Self::open_with_cache(config, &cache, &blocks, &memory).await
    }

    /// Opens a writer that uses `cache`, `blocks` and `memory` instead of
    /// building its own.
    pub(crate) async fn open_with_cache(
        config: Config,
        cache: &SharedDbCache,
        blocks: &BlockCache,
        memory: &MemoryBudget,
    ) -> Result<Self> {
        Self::open_over_store(config, cache, blocks, memory, None).await
    }

    /// [`Self::open_with_cache`] over `object_store`, when given, instead of
    /// the configured one.
    pub(crate) async fn open_over_store(
        config: Config,
        cache: &SharedDbCache,
        blocks: &BlockCache,
        memory: &MemoryBudget,
        object_store: Option<Arc<dyn slatedb::object_store::ObjectStore>>,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = duration_ns(config.segment_duration)?;
        let merge_holds = MergeHolds::default();
        let storage = StorageBuilder::with_cache_over_store(
            &config.storage,
            cache,
            crate::memory::slatedb_settings(),
            object_store,
        )?
        .with_semantics(
            storage_semantics().with_segment_compaction(
                SegmentCompaction::new(crate::codec::segment_span)
                    .with_retention(config.retention)
                    .with_holds(merge_holds.clone())
                    .with_superseded(common::storage::rollup::superseded(
                        crate::codec::KEY_SCOPE,
                        segment_ns,
                    ))
                    .with_unsealed(common::storage::rollup::unsealed(
                        crate::codec::KEY_SCOPE,
                        segment_ns,
                    )),
            ),
        )
        .build()
        .await?;
        let storage_read = storage.clone();
        let rolled = storage_read
            .slate_read()
            .map(|slate| {
                Rollups::load_all(&slate, crate::codec::KEY_SCOPE)
                    .into_iter()
                    .map(|(namespace, rollups)| (namespace, rollups.routed()))
                    .filter(|(_, routed)| !routed.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let rollup_ns = config.discovery_rollup.map(duration_ns).transpose()?;
        let direct_writer = DirectWriter::new(
            storage_read.clone(),
            storage.clone(),
            config.clone(),
            rollup_ns,
        );
        let compactor = config.compaction.enabled.then(|| {
            Compactor::new(
                storage.clone(),
                config.compaction.clone(),
                config.page.clone(),
                segment_ns,
            )
        });
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            (),
            LogsFlusher {
                direct_writer,
                storage: storage.clone(),
                compactor,
            },
        );
        let write_handle = write_coordinator.handle(WRITE_CHANNEL);
        write_coordinator.start();
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            segment_ns,
            rollup_ns,
            write_handle: Some(write_handle),
            write_coordinator: Mutex::new(Some(write_coordinator)),
            caches: DiscoveryCaches::new(),
            blocks: blocks.clone(),
            database_id: block_cache::next_database_id(),
            memory: memory.clone(),
            span_scan_min_streams: SPAN_SCAN_MIN_STREAMS,
            span_scan_max_spread: SPAN_SCAN_MAX_SPREAD,
            project_labels: true,
            tuning: ReadTuning::default(),
            rolled: tokio::sync::RwLock::new(rolled),
            rollup_lock: Mutex::new(()),
            merge_holds,
            retention: config.retention,
            settle_after: config.compaction.finalize_after,
            summary_bucket: summary::SUMMARY_BUCKET,
            summarized: std::sync::Mutex::default(),
            period_summaries: std::sync::Mutex::default(),
        })
    }

    /// Adds to `plan` this shard's SSTs of `namespaces`' segments, hourly or
    /// rolled up, that overlap `window`, each ranked by its segment's end so
    /// the newest data warms first, with payloads for those overlapping its
    /// payload window. Filters, about 95% of Logs SST metadata, are left out
    /// when reads skip them.
    pub fn plan_warm(&self, namespaces: &[Namespace], window: WarmWindow, plan: &mut WarmPlan) {
        let Some(slate) = self.storage.slate_read() else {
            return;
        };
        let end_ns = common::time::now_ns();
        let start_ns = window.metadata_since(end_ns, 1_000_000_000);
        let payloads_since = window.payloads_since(end_ns, 1_000_000_000);
        let (recent, older): (Vec<_>, Vec<_>) = slate
            .segment_sizes()
            .into_iter()
            .filter_map(|(prefix, _)| {
                let (namespace, segment, len) = crate::codec::KEY_SCOPE.parse(&prefix).ok()?;
                let segment_end = segment.end_ns().unwrap_or(i64::MAX);
                (len == prefix.len()
                    && namespaces.contains(&namespace)
                    && segment.start_ns < end_ns
                    && segment_end > start_ns)
                    .then_some((prefix, Some(segment_end / 1_000_000)))
            })
            .partition(|(_, end_ms)| {
                payloads_since
                    .is_some_and(|since| end_ms.is_some_and(|end_ms| end_ms > since / 1_000_000))
            });
        let filters = !self.tuning.unfiltered_reads;
        slate.plan_prefix_metadata(&recent, filters, true, plan);
        slate.plan_prefix_metadata(&older, filters, false, plan);
    }

    #[cfg(test)]
    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        let blocks = BlockCache::new(config.block_cache_capacity_bytes);
        let memory = MemoryBudget::new(&config.memory);
        Self::open_reader_with_cache(
            config,
            reader_options,
            &SharedDbCache::default(),
            &blocks,
            &memory,
        )
        .await
    }

    /// Opens a reader that uses `cache`, `blocks` and `memory`; an empty
    /// `cache` falls back to the config's own cache settings.
    pub(crate) async fn open_reader_with_cache(
        config: Config,
        reader_options: DbReaderOptions,
        cache: &SharedDbCache,
        blocks: &BlockCache,
        memory: &MemoryBudget,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = duration_ns(config.segment_duration)?;
        let storage = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new().with_shared_cache(cache),
            storage_semantics(),
            reader_options,
        )
        .await?;
        Ok(Self {
            storage,
            writer: None,
            segment_ns,
            rollup_ns: config.discovery_rollup.map(duration_ns).transpose()?,
            write_handle: None,
            write_coordinator: Mutex::new(None),
            caches: DiscoveryCaches::new(),
            blocks: blocks.clone(),
            database_id: block_cache::next_database_id(),
            memory: memory.clone(),
            span_scan_min_streams: SPAN_SCAN_MIN_STREAMS,
            span_scan_max_spread: SPAN_SCAN_MAX_SPREAD,
            project_labels: true,
            tuning: ReadTuning::default(),
            rolled: tokio::sync::RwLock::new(HashMap::new()),
            rollup_lock: Mutex::new(()),
            merge_holds: MergeHolds::default(),
            retention: config.retention,
            settle_after: config.compaction.finalize_after,
            summary_bucket: summary::SUMMARY_BUCKET,
            summarized: std::sync::Mutex::default(),
            period_summaries: std::sync::Mutex::default(),
        })
    }

    /// The storage queries read.
    fn reads(&self) -> Arc<dyn StorageRead> {
        if self.tuning.unfiltered_reads {
            Arc::new(UnfilteredReads::new(
                Arc::clone(&self.storage),
                self.tuning.cached_filters,
            ))
        } else {
            Arc::clone(&self.storage)
        }
    }

    /// Segments a query reads at once.
    pub(crate) fn segment_read_concurrency(&self) -> usize {
        self.tuning.read_concurrency
    }

    fn write_handle(&self) -> Result<&WriteCoordinatorHandle<LogsWriteDelta>> {
        self.write_handle
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
    }

    fn writer(&self) -> Result<&Arc<dyn Storage>> {
        self.writer
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
    }

    /// Atomically writes all generated index and page records.
    pub async fn write(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
    ) -> Result<WriteReport> {
        self.write_with_durability(namespace, batches, Durability::Written)
            .await
    }

    pub async fn write_with_durability(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let rolled = self.rolled.read().await;
        let routed = rolled.get(namespace);
        let groups = group_by_stream(batches, self.segment_ns, |segment| {
            routed
                .and_then(|routed| {
                    routed
                        .iter()
                        .filter(|wide| {
                            wide.start_ns <= segment.start_ns
                                && segment.start_ns < wide.start_ns.saturating_add(wide.width_ns)
                        })
                        .max_by_key(|wide| wide.width_ns)
                        .copied()
                })
                .unwrap_or(segment)
        })?;
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }
        // Every flush is a subset of what the write budget holds, so a
        // request larger than a chunk of it is applied a chunk at a time. A
        // retry after a later chunk fails rewrites the earlier rows, which
        // reads deduplicate.
        let mut report = WriteReport {
            streams: groups.len(),
            ..WriteReport::default()
        };
        let mut chunks = split_groups(groups, self.memory.write_chunk_bytes()).into_iter();
        while let Some(chunk) = chunks.next() {
            let (applied, write) = self.apply_write(namespace, chunk, Vec::new(), None).await?;
            report.pages += applied.pages;
            report.rows += applied.rows;
            if chunks.len() == 0 {
                tokio::spawn(write.written());
            } else {
                // A request holds one chunk of the budget at a time.
                self.write_handle()?
                    .flush(false)
                    .await
                    .map_err(map_write_error)?;
                write.written().await;
            }
        }
        drop(rolled);

        if durability != Durability::Applied {
            self.flush_written().await?;
        }
        if durability == Durability::Durable {
            self.writer()?.flush().await?;
        }
        Ok(report)
    }

    /// Applies `groups` to the write delta, waiting up to `wait` for room
    /// in the queue, or failing at once without it.
    async fn apply_groups(
        &self,
        namespace: &Namespace,
        groups: StreamGroups,
        wait: Option<Duration>,
    ) -> Result<WriteReport> {
        self.apply_groups_with_ops(namespace, groups, Vec::new(), wait)
            .await
    }

    /// Applies `groups` and `ops` in the same coordinator delta and SlateDB
    /// write batch.
    async fn apply_groups_with_ops(
        &self,
        namespace: &Namespace,
        groups: StreamGroups,
        ops: Vec<RecordOp>,
        wait: Option<Duration>,
    ) -> Result<WriteReport> {
        let (report, write) = self.apply_write(namespace, groups, ops, wait).await?;
        tokio::spawn(write.written());
        Ok(report)
    }

    async fn apply_write(
        &self,
        namespace: &Namespace,
        groups: StreamGroups,
        ops: Vec<RecordOp>,
        wait: Option<Duration>,
    ) -> Result<(WriteReport, AppliedWrite)> {
        let write = LogsWrite {
            namespace: namespace.clone(),
            groups,
            ops,
        };
        let handle = self.write_handle()?;
        let permit = self.write_budget(handle, &write, wait).await?;
        let mut write_handle = match wait {
            Some(timeout) => handle.write_timeout(write, timeout).await,
            None => handle.try_write(write).await,
        }
        .map_err(map_write_error)?;
        let report = write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)?;
        Ok((
            report,
            AppliedWrite {
                handle: write_handle,
                permit,
            },
        ))
    }

    /// Holds `write`'s bytes in the process's write budget: a rollup copy,
    /// which has a `wait`, waits up to it for room in its half; ingest fails
    /// at once without room. Only written deltas free the budget, so a write
    /// that finds its share full first asks for the applied ones to be
    /// flushed rather than wait for the flush interval; a copy holding its
    /// whole half would otherwise wait out its timeout on its own writes.
    async fn write_budget(
        &self,
        handle: &WriteCoordinatorHandle<LogsWriteDelta>,
        write: &LogsWrite,
        wait: Option<Duration>,
    ) -> Result<WritePermit> {
        let bytes = write.estimated_bytes();
        let writer = if wait.is_some() {
            Writer::Copy
        } else {
            Writer::Ingest
        };
        if !self.memory.write_fits(writer, bytes) {
            handle.flush(false).await.map_err(map_write_error)?;
        }
        self.memory
            .write(writer, bytes, wait.unwrap_or(Duration::ZERO))
            .await
    }

    /// Writes every applied delta to storage.
    async fn flush_written(&self) -> Result<()> {
        let mut flush_handle = self
            .write_handle()?
            .flush(false)
            .await
            .map_err(map_write_error)?;
        flush_handle
            .wait(CoordinatorDurability::Written)
            .await
            .map_err(map_write_error)?;
        Ok(())
    }
}

/// The writer updates postings and search statistics as merge operands, so
/// readers need the operator too.
fn storage_semantics() -> StorageSemantics {
    StorageSemantics::new()
        .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared())
        .with_merge_operator(Arc::new(crate::merge::LogsMergeOperator))
}

fn duration_ns(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos())
        .map_err(|_| Error::Invalid("duration exceeds i64 nanoseconds".to_owned()))
}

fn unix_time_ms() -> Result<u64> {
    common::time::checked_now_ms().map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod memory_profile;
#[cfg(test)]
mod tests;
