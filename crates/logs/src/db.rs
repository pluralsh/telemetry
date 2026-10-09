// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
    WriteError,
};
use common::discovery::{
    CatalogBatch, DiscoveryCache, DiscoveryValue, names as catalog_names, values as catalog_values,
};
use common::storage::{RecordOp, Storage, StorageRead, Ttl, WriteOptions};
use common::{
    BytesRange, SharedDbCache, StorageBuilder, StorageReaderRuntime, StorageSemantics,
    create_storage_read,
};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::Namespace;
use crate::analyzer::DEFAULT_ANALYZER;
use crate::codec::{
    ObjectRef, StoredObject, decode_forward_key, decode_labels, decode_object_id, decode_postings,
    decode_stream_id, dictionary_key, encode_labels, encode_object_id, encode_postings,
    encode_stream_id, field_stats_key, forward_key, forward_prefix, next_object_id_key,
    next_stream_id_key, posting_key, rollup_dictionary_key, rollup_forward_key,
    rollup_forward_prefix, rollup_next_stream_id_key, rollup_posting_key, rollup_prefix,
    segment_for, segment_prefix,
};
use crate::compaction::{Compactor, WrittenObject};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{
    Label, Labels, LogBatch, LogEntry, LogRow, SegmentId, StreamFingerprint, StreamId,
};
use crate::object::{BuiltObject, ObjectBuilder, ObjectLocation, ObjectProperties, object_records};
use crate::search::{
    FieldStats, IndexDelta, block_max_scores, encode_field_stats, source_matches, term_index_ops,
};

mod block_cache;
mod query;
mod write;

pub(crate) use block_cache::BlockCache;
#[cfg(feature = "bench-internals")]
pub(crate) use query::run_entries;
pub(crate) use query::{Boundaries, SampleRead, ScanTargets, StreamFilter};
use write::*;

/// Read units fetched concurrently within one query segment.
const PAGE_READ_CONCURRENCY: usize = 16;
/// Streams whose forward labels and run records are read concurrently.
const STREAM_METADATA_CONCURRENCY: usize = 32;
/// Fewest selected streams read by one range scan over their ID span instead
/// of an operation per stream: below it, both cost a few point operations.
const SPAN_SCAN_MIN_STREAMS: usize = 8;
/// Widest span, relative to the selected stream count, still read by one
/// range scan; past it most scanned records would be unselected streams.
const SPAN_SCAN_MAX_SPREAD: usize = 4;
/// Segments whose stream metadata is listed concurrently by `scan_targets`.
const SEGMENT_LIST_CONCURRENCY: usize = 4;
/// Segments whose discovery catalogs are scanned concurrently.
const SEGMENT_DISCOVERY_CONCURRENCY: usize = 16;
/// Larger per-segment stream sets are recomputed rather than cached, which
/// bounds the series cache to roughly `capacity * this` label sets.
const SERIES_CACHE_MAX_STREAMS: usize = 2_048;
const WRITE_CHANNEL: &str = "write";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    pub streams: usize,
    pub pages: usize,
    pub rows: usize,
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
}

impl PageBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: AtomicUsize::new(limit),
        }
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
    /// [`SPAN_SCAN_MIN_STREAMS`], overridable so tests cover both read paths.
    span_scan_min_streams: usize,
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
    /// queries select them; weighed by label bytes.
    streams: moka::sync::Cache<StreamsKey, SegmentStreams>,
}

/// A partition's next stream ID, `None` while it has none (or expired).
type PartitionVersion = Option<StreamId>;
type Versioned<V> = (PartitionVersion, V);

type SeriesKey = (Namespace, Partition, Vec<Label>);
type SegmentSeries = Arc<Versioned<Vec<Arc<Labels>>>>;
type StreamsKey = (Namespace, SegmentId, Vec<Label>);
type SegmentStreams = Arc<Versioned<Vec<(StreamId, Arc<Labels>)>>>;

/// Byte budget of [`DiscoveryCaches::streams`].
const STREAMS_CACHE_BYTES: u64 = 64 * 1024 * 1024;

impl DiscoveryCaches {
    fn new() -> Self {
        // Entries are inserted as closed, so the TTL is never consulted.
        Self {
            label_names: DiscoveryCache::new(4_096, Duration::ZERO),
            label_values: DiscoveryCache::new(16_384, Duration::ZERO),
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
        Self::open_with_cache(config, &cache, &blocks).await
    }

    /// Opens a writer that uses `cache` and `blocks` instead of building its
    /// own.
    pub(crate) async fn open_with_cache(
        config: Config,
        cache: &SharedDbCache,
        blocks: &BlockCache,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = duration_ns(config.segment_duration)?;
        let storage = StorageBuilder::with_cache(&config.storage, cache)?
            .with_semantics(storage_semantics())
            .build()
            .await?;
        let storage_read = storage.clone();
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
            span_scan_min_streams: SPAN_SCAN_MIN_STREAMS,
        })
    }

    /// Warms SlateDB caches for recent log segments in this shard.
    pub async fn warm_recent(
        &self,
        namespace: &Namespace,
        warm_range: Duration,
        include_payloads: bool,
        concurrency: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let Some(slate) = self.storage.slate_read() else {
            return Ok(());
        };
        let end_ns = common::time::now_ns();
        let range_ns = i64::try_from(warm_range.as_nanos()).unwrap_or(i64::MAX);
        let segments = self.discovery_segments(end_ns.saturating_sub(range_ns), end_ns)?;
        // A period whose first segment is in range is warmed with it.
        let periods = self
            .rollup_periods(&segments)
            .into_iter()
            .filter(|period| segments.binary_search(period).is_err());
        let prefixes = segments
            .iter()
            .map(|segment| segment_prefix(namespace, *segment))
            .chain(periods.map(|period| rollup_prefix(namespace, period)))
            .collect::<Vec<_>>();
        slate
            .warm_prefixes("logs", &prefixes, include_payloads, concurrency, cancel)
            .await?;
        Ok(())
    }

    /// Rollup periods holding `segments`; empty without a rollup.
    fn rollup_periods<'a>(
        &self,
        segments: impl IntoIterator<Item = &'a SegmentId>,
    ) -> BTreeSet<SegmentId> {
        let Some(rollup_ns) = self.rollup_ns else {
            return BTreeSet::new();
        };
        segments
            .into_iter()
            .map(|segment| segment_for(*segment, rollup_ns))
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        let blocks = BlockCache::new(config.block_cache_capacity_bytes);
        Self::open_reader_with_cache(config, reader_options, &SharedDbCache::default(), &blocks)
            .await
    }

    /// Opens a reader that uses `cache` and `blocks`; an empty `cache` falls
    /// back to the config's own cache settings.
    pub(crate) async fn open_reader_with_cache(
        config: Config,
        reader_options: DbReaderOptions,
        cache: &SharedDbCache,
        blocks: &BlockCache,
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
            span_scan_min_streams: SPAN_SCAN_MIN_STREAMS,
        })
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
        let groups = group_by_stream(batches, self.segment_ns)?;
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }
        let write = LogsWrite {
            namespace: namespace.clone(),
            groups,
        };
        let mut write_handle = self
            .write_handle()?
            .try_write(write)
            .await
            .map_err(map_write_error)?;
        let report = write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)?;

        if durability != Durability::Applied {
            let mut flush_handle = self
                .write_handle()?
                .flush(false)
                .await
                .map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
        }
        if durability == Durability::Durable {
            self.writer()?.flush().await?;
        }
        Ok(report)
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
mod tests;
