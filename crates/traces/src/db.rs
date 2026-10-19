// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{
    Delta, Durability as CoordinatorDurability, Flusher, WriteCoordinator, WriteCoordinatorHandle,
    WriteError,
};
use common::discovery::{self, CatalogBatch, DiscoveryCache, DiscoveryValue};
use common::storage::compaction::{MergeHolds, SegmentCompaction};
use common::storage::rollup::Rollups;
use common::storage::slate::warm::WarmPlan;
use common::storage::{RecordOp, Storage, StorageRead, Ttl};
use common::{
    BytesRange, SharedDbCache, StorageBuilder, StorageReaderRuntime, StorageSemantics, WarmWindow,
    create_storage_read,
};
use futures::{StreamExt, TryStreamExt, stream};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;

use crate::codec::{
    LOCATOR_SEGMENT, PageRef, PageTrace, StoredPageMetadata, TraceHead, TraceLocator,
    continuation_key, continuation_prefix, decode_head, decode_head_trace_id, decode_indices,
    decode_locator, decode_metadata, decode_metadata_sequence, decode_posting_sequence,
    decode_posting_value, decode_sequence, encode_head, encode_indices, encode_locator,
    encode_metadata, encode_sequence, field_posting_key, field_scan_prefix, field_value_prefix,
    head_key, head_namespace_prefix, metadata_key, metadata_prefix, next_sequence_key, payload_key,
    posting_key, segment_for, segment_prefix, value_subrange,
};

/// Concurrent storage reads per query stage.
const READ_CONCURRENCY: usize = 32;
/// Traces materialized per batch, bounding how many pages are held at once.
const MATERIALIZE_BATCH: usize = 256;
/// Segments whose metadata or postings are scanned concurrently.
const SEGMENT_SCAN_CONCURRENCY: usize = 8;
/// Segments whose discovery catalogs one tag lookup reads at once.
const CATALOG_SEGMENT_CONCURRENCY: usize = READ_CONCURRENCY;

#[cfg(not(test))]
fn catalog_concurrency() -> usize {
    CATALOG_SEGMENT_CONCURRENCY
}

/// Overrides [`CATALOG_SEGMENT_CONCURRENCY`] in tests when non-zero.
#[cfg(test)]
static CATALOG_CONCURRENCY_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn catalog_concurrency() -> usize {
    match CATALOG_CONCURRENCY_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => CATALOG_SEGMENT_CONCURRENCY,
        overridden => overridden,
    }
}

/// Most candidates one TraceQL search round decides at once. Rounds double
/// up to this while they come up short of the limit, and each costs a few
/// dependent reads, so a large limit takes a few wide rounds.
const SEARCH_BATCH_MAX: usize = 4 * MATERIALIZE_BATCH;

#[cfg(not(test))]
fn search_batch_max() -> usize {
    SEARCH_BATCH_MAX
}

/// Overrides [`SEARCH_BATCH_MAX`] in tests when non-zero.
#[cfg(test)]
static SEARCH_BATCH_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn search_batch_max() -> usize {
    match SEARCH_BATCH_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => SEARCH_BATCH_MAX,
        overridden => overridden,
    }
}

/// Encoded value bytes one tag-values lookup reads, per segment and across
/// segments, so a high-cardinality attribute is never listed whole.
const CATALOG_VALUES_MAX_BYTES: usize = 8 << 20;
/// Bytes of catalog values a shard keeps cached.
const CATALOG_VALUES_CACHE_BYTES: usize = 32 << 20;
/// Most unwanted pages a metadata scan steps over between two wanted ones.
/// A point get costs about as much as reading seven adjacent small records,
/// so wider gaps are cheaper as separate reads.
const METADATA_SCAN_MAX_GAP: u64 = 6;
use crate::sidecar::PageColumns;
use crate::traceql::{
    IndexField, IndexPredicate, IndexTest, PushdownClause, TraceFilter, span_intrinsics,
};
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Config, Error, Namespace, Page, PageBuilder,
    PageConfig, QueryOptions, Result, SegmentId, Trace, TraceBatch, TraceId, TraceQlResult,
    TraceSummary,
};

mod query;
mod read_cache;
mod rollup;
mod write;

pub use read_cache::ReadCacheConfig;
use read_cache::{PostingEntry, ReadCache, SegmentPostings, TraceLocators};

pub(crate) use query::{Hit, evaluate_traceql, execute_traceql, merge_traces};
use write::*;

const WRITE_CHANNEL: &str = "write";
const TRACES_FLUSH_PAGES: &str = "traces_flush_pages";
const PARTITION_SCOPE: &str = "partition";
const PARTITION_NAME: &str = "segment";
/// `"<segment>:<last segment>"` for partitions holding traces that end after
/// their start segment, so searches can reach them from later windows.
const PARTITION_EXTENT_NAME: &str = "extent";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    /// Input traces accepted from this request.
    pub traces: usize,
    /// Reserved for API compatibility. Pages are formed across requests by
    /// the flusher and reported through `traces_flush_pages`.
    pub pages: usize,
    /// Input spans accepted from this request.
    pub spans: usize,
}

/// Readers need the merge operator too: trace heads are merge records.
fn storage_semantics() -> StorageSemantics {
    StorageSemantics::new()
        .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared())
        .with_merge_operator(Arc::new(crate::merge::TracesMergeOperator))
}

#[derive(Clone, Copy)]
struct Retention {
    physical_ttl: Ttl,
    expires_at_unix_ms: Option<u64>,
}

/// A segment catalog scan tagged with the segment's next page sequence when
/// it was taken; it is current only while that sequence is unchanged.
type SequencedCatalog<K, V> = DiscoveryCache<K, (u64, Vec<V>)>;

fn catalog_values_weight((_, values): &(u64, Vec<DiscoveryValue>)) -> usize {
    values.iter().map(DiscoveryValue::weight).sum()
}

/// Single-node OTLP trace database over the common SlateDB abstraction.
pub struct TraceDb {
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    write_handle: Option<WriteCoordinatorHandle<TraceWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<TraceWriteDelta, TraceFlusher>>>,
    segment_ns: u64,
    catalog_names_cache: SequencedCatalog<(Namespace, SegmentId, Option<AttributeScope>), String>,
    catalog_values_cache:
        SequencedCatalog<(Namespace, SegmentId, Option<AttributeScope>, String), DiscoveryValue>,
    read_cache: Arc<ReadCache>,
    /// Whether records expire, so cached postings may name pages that no
    /// longer exist.
    expiring: bool,
    /// Rollup segments, pending or sealed, that take the writes of the
    /// segments they cover. Ingest holds the read lock until its write is
    /// applied, so a rollup holding the write lock sees none in flight.
    rolled: tokio::sync::RwLock<HashMap<Namespace, BTreeSet<SegmentId>>>,
    /// Serializes rollup passes.
    rollup_lock: Mutex<()>,
    /// Holds on the merges of segments a rollup copies into.
    merge_holds: MergeHolds,
    retention: Option<Duration>,
}

impl TraceDb {
    /// Adds to `plan` this shard's SSTs of the trace segments overlapping
    /// `window`, each ranked by its segment's end, and of the locator
    /// segment, ranked by when each SST was written. Payloads are planned
    /// for the segments overlapping its payload window and the locator.
    pub async fn plan_warm(
        &self,
        namespaces: &[Namespace],
        window: WarmWindow,
        plan: &mut WarmPlan,
    ) -> Result<()> {
        let Some(slate) = self.storage.slate_read() else {
            return Ok(());
        };
        let now_ns = common::time::now_ns();
        let end_ns = now_ns.unsigned_abs();
        let start_ns = window
            .metadata_since(now_ns, 1_000_000_000)
            .max(0)
            .unsigned_abs();
        let payloads_since = window.payloads_since(now_ns, 1_000_000_000);
        let mut recent = Vec::new();
        let mut older = Vec::new();
        for namespace in namespaces {
            for segment in self.catalog_segments(namespace, start_ns, end_ns).await? {
                let segment_end_ns = segment.end_ns().unwrap_or(i64::MAX);
                let prefix = (
                    segment_prefix(namespace, segment),
                    Some(segment_end_ns / 1_000_000),
                );
                if payloads_since.is_some_and(|since| segment_end_ns > since) {
                    recent.push(prefix);
                } else {
                    older.push(prefix);
                }
            }
            let locator = (segment_prefix(namespace, LOCATOR_SEGMENT), None);
            if payloads_since.is_some() {
                recent.push(locator);
            } else {
                older.push(locator);
            }
        }
        slate.plan_prefixes(&recent, true, plan);
        slate.plan_prefixes(&older, false, plan);
        Ok(())
    }

    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let cache = SharedDbCache::from_config(&config.storage).await?;
        Self::open_with_cache(config, &cache).await
    }

    /// Opens a writer that uses `cache` instead of building its own.
    pub(crate) async fn open_with_cache(config: Config, cache: &SharedDbCache) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
        let merge_holds = MergeHolds::default();
        let storage = StorageBuilder::with_cache(&config.storage, cache)?
            .with_semantics(
                storage_semantics().with_segment_compaction(
                    SegmentCompaction::new(crate::codec::segment_span)
                        .with_retention(config.retention)
                        .with_holds(merge_holds.clone())
                        .with_superseded(common::storage::rollup::superseded(
                            crate::codec::KEY_SCOPE,
                            i64::try_from(segment_ns).unwrap_or(i64::MAX),
                        ))
                        .with_unsealed(common::storage::rollup::unsealed(
                            crate::codec::KEY_SCOPE,
                            i64::try_from(segment_ns).unwrap_or(i64::MAX),
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
        let read_cache = Arc::new(ReadCache::new(&config.read_cache, true));
        let flusher = TraceFlusher::new(
            storage.clone(),
            config.page.clone(),
            config.retention,
            segment_ns,
            read_cache.clone(),
        );
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            (),
            flusher,
        );
        let write_handle = write_coordinator.handle(WRITE_CHANNEL);
        write_coordinator.start();
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            write_handle: Some(write_handle),
            write_coordinator: Mutex::new(Some(write_coordinator)),
            segment_ns,
            catalog_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            catalog_values_cache: DiscoveryCache::weighted(
                4_096,
                Duration::from_secs(5),
                CATALOG_VALUES_CACHE_BYTES,
                catalog_values_weight,
            ),
            read_cache,
            expiring: config.retention.is_some(),
            rolled: tokio::sync::RwLock::new(rolled),
            rollup_lock: Mutex::new(()),
            merge_holds,
            retention: config.retention,
        })
    }

    #[cfg(test)]
    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        Self::open_reader_with_cache(config, reader_options, &SharedDbCache::default()).await
    }

    /// Opens a reader that uses `cache`; an empty `cache` falls back to the
    /// config's own cache settings.
    pub(crate) async fn open_reader_with_cache(
        config: Config,
        reader_options: DbReaderOptions,
        cache: &SharedDbCache,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
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
            write_handle: None,
            write_coordinator: Mutex::new(None),
            segment_ns,
            catalog_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            catalog_values_cache: DiscoveryCache::weighted(
                4_096,
                Duration::from_secs(5),
                CATALOG_VALUES_CACHE_BYTES,
                catalog_values_weight,
            ),
            read_cache: Arc::new(ReadCache::new(&config.read_cache, false)),
            expiring: config.retention.is_some(),
            rolled: tokio::sync::RwLock::new(HashMap::new()),
            rollup_lock: Mutex::new(()),
            merge_holds: MergeHolds::default(),
            retention: config.retention,
        })
    }

    fn write_handle(&self) -> Result<&WriteCoordinatorHandle<TraceWriteDelta>> {
        self.write_handle
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
    }

    pub async fn write(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
    ) -> Result<WriteReport> {
        self.write_with_durability(namespace, batches, Durability::Written)
            .await
    }

    /// Atomically publishes page metadata, payload, locator fragments, and
    /// immutable attribute posting fragments.
    pub async fn write_with_durability(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let rolled = self.rolled.read().await;
        let routed = rolled.get(namespace);
        let mut groups: BTreeMap<SegmentId, Vec<Trace>> = BTreeMap::new();
        let mut report = WriteReport::default();
        for batch in batches {
            for trace in batch.traces {
                let (min_timestamp_ns, _) = trace.timestamp_range();
                let segment = segment_for(min_timestamp_ns, self.segment_ns);
                let segment = routed
                    .and_then(|routed| {
                        routed
                            .iter()
                            .filter(|wide| {
                                wide.start_ns <= segment.start_ns
                                    && segment.start_ns
                                        < wide.start_ns.saturating_add(wide.width_ns)
                            })
                            .max_by_key(|wide| wide.width_ns)
                            .copied()
                    })
                    .unwrap_or(segment);
                report.traces += 1;
                report.spans += trace.spans().count();
                groups.entry(segment).or_default().push(trace);
            }
        }
        if groups.is_empty() {
            return Ok(report);
        }
        let report = self
            .apply_groups(namespace, groups, report, Vec::new(), None)
            .await?;
        drop(rolled);
        let handle = self.write_handle()?;
        if durability != Durability::Applied {
            let flush_storage = durability == Durability::Durable;
            let mut flush_handle = handle.flush(flush_storage).await.map_err(map_write_error)?;
            flush_handle
                .wait(if flush_storage {
                    CoordinatorDurability::Durable
                } else {
                    CoordinatorDurability::Written
                })
                .await
                .map_err(map_write_error)?;
        }
        Ok(report)
    }
}

impl TraceDb {
    /// Applies `groups` to the write delta, waiting up to `wait` for room
    /// in the queue, or failing at once without it.
    async fn apply_groups(
        &self,
        namespace: &Namespace,
        mut groups: BTreeMap<SegmentId, Vec<Trace>>,
        report: WriteReport,
        rebased: Vec<TraceId>,
        wait: Option<Duration>,
    ) -> Result<WriteReport> {
        for traces in groups.values_mut() {
            traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        }
        let write = TraceWrite {
            namespace: namespace.clone(),
            groups,
            report,
            rebased,
        };
        let handle = self.write_handle()?;
        let mut write_handle = match wait {
            Some(timeout) => handle.write_timeout(write, timeout).await,
            None => handle.try_write(write).await,
        }
        .map_err(|error| map_write_error(error.discard_inner()))?;
        write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(handle) = &self.write_handle {
            let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
        }
        if let Some(writer) = &self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(coordinator) = self.write_coordinator.lock().await.take() {
            coordinator
                .stop()
                .await
                .map_err(|error| Error::Invalid(format!("write coordinator stopped: {error}")))?;
        }
        self.storage.close().await?;
        Ok(())
    }
}

fn catalog_scope(scope: AttributeScope) -> &'static str {
    match scope {
        AttributeScope::Resource => "resource",
        AttributeScope::Span => "span",
    }
}

fn unix_time_ms() -> Result<u64> {
    common::time::checked_now_ms().map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests;
