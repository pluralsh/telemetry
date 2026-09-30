// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{
    Delta, Durability as CoordinatorDurability, Flusher, WriteCoordinator, WriteCoordinatorHandle,
    WriteError,
};
use common::discovery::{self, CatalogBatch, DiscoveryCache, DiscoveryValue};
use common::storage::{
    PutOptions, PutRecordOp, Record, RecordOp, Storage, StorageRead, StorageSnapshot, Ttl,
};
use common::{
    BytesRange, StorageBuilder, StorageReaderRuntime, StorageSemantics, create_storage_read,
};
use futures::{StreamExt, TryStreamExt, stream};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans},
};
use prost::Message;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::codec::{
    LOCATOR_SEGMENT, PageRef, PageTrace, StoredPageMetadata, TraceLocator, decode_indices,
    decode_locator, decode_locator_trace_id, decode_metadata, decode_posting_sequence,
    decode_sequence, encode_indices, encode_locator, encode_metadata, encode_sequence, locator_key,
    locator_namespace_prefix, locator_prefix, metadata_key, metadata_prefix, next_sequence_key,
    payload_key, posting_key, posting_scan_prefix, segment_for, segment_prefix,
};

/// Concurrent storage reads per query stage.
const READ_CONCURRENCY: usize = 32;
/// Traces materialized per batch, bounding how many pages are held at once.
const MATERIALIZE_BATCH: usize = 256;
/// Segments whose metadata or postings are scanned concurrently.
const SEGMENT_SCAN_CONCURRENCY: usize = 8;
use crate::traceql::PushdownClause;
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Config, Error, Namespace, Page, PageBuilder,
    PageConfig, QueryOptions, Result, SegmentId, Trace, TraceBatch, TraceId, TraceQlResult,
};

const WRITE_CHANNEL: &str = "write";
const TRACK_FLUSH_PAGES: &str = "track_flush_pages";
const PARTITION_SCOPE: &str = "partition";
const PARTITION_NAME: &str = "segment";

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
    /// the flusher and reported through `track_flush_pages`.
    pub pages: usize,
    /// Input spans accepted from this request.
    pub spans: usize,
}

#[derive(Clone, Copy)]
struct Retention {
    physical_ttl: Ttl,
    expires_at_unix_ms: Option<u64>,
}

/// Single-node OTLP trace database over the common SlateDB abstraction.
pub struct TraceDb {
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    write_handle: Option<WriteCoordinatorHandle<TraceWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<TraceWriteDelta, TraceFlusher>>>,
    segment_ns: u64,
    catalog_names_cache:
        DiscoveryCache<(Namespace, SegmentId, Option<AttributeScope>), Vec<String>>,
    catalog_values_cache:
        DiscoveryCache<(Namespace, SegmentId, Option<AttributeScope>, String), Vec<DiscoveryValue>>,
}

impl TraceDb {
    /// Warms SlateDB caches for recent trace segments in this shard.
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
        let end_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
            .unwrap_or(u64::MAX);
        let range_ns = u64::try_from(warm_range.as_nanos()).unwrap_or(u64::MAX);
        let mut prefixes = self
            .catalog_segments(namespace, end_ns.saturating_sub(range_ns), end_ns)
            .await?
            .into_iter()
            .map(|segment| segment_prefix(namespace, segment))
            .collect::<Vec<_>>();
        prefixes.push(segment_prefix(namespace, LOCATOR_SEGMENT));
        slate
            .warm_prefixes("track", &prefixes, include_payloads, concurrency, cancel)
            .await?;
        Ok(())
    }

    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        let storage_read = storage.clone();
        let initial_snapshot = storage.snapshot().await?;
        let flusher = TraceFlusher {
            storage: storage.clone(),
            page_config: config.page.clone(),
            retention: config.retention,
        };
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            initial_snapshot,
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
            catalog_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            semantics,
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
            catalog_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
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
        let mut groups: BTreeMap<SegmentId, Vec<Trace>> = BTreeMap::new();
        let mut report = WriteReport::default();
        for batch in batches {
            for trace in batch.traces {
                let (min_timestamp_ns, _) = trace.timestamp_range();
                let segment = segment_for(min_timestamp_ns, self.segment_ns);
                report.traces += 1;
                report.spans += trace.spans().count();
                groups.entry(segment).or_default().push(trace);
            }
        }
        if groups.is_empty() {
            return Ok(report);
        }
        for traces in groups.values_mut() {
            traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        }
        self.catalog_names_cache.clear();
        self.catalog_values_cache.clear();
        let write = TraceWrite {
            namespace: namespace.clone(),
            groups,
            report,
        };
        let handle = self.write_handle()?;
        let mut write_handle = handle
            .try_write(write)
            .await
            .map_err(|error| map_write_error(error.discard_inner()))?;
        let report = write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)?;
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

    /// Returns all live continuations merged into one logical trace. Exact
    /// duplicate spans in identical resource/scope context are removed while
    /// preserving every distinct OTLP batch and span.
    pub async fn get_trace(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Result<Option<Trace>> {
        let now = unix_time_ms()?;
        Ok(self.materialize(namespace, &[trace_id], now).await?.pop())
    }

    /// Exact-match scalar attribute search over an inclusive OTLP nanosecond
    /// range. Posting fragments are candidates only; decoded traces are always
    /// verified before being returned.
    pub async fn search(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        let clauses = matchers
            .iter()
            .map(|matcher| vec![matcher.clone()])
            .collect::<Vec<_>>();
        let ordered = self
            .ordered_candidates(namespace, start_ns, end_ns, &clauses, now)
            .await?;
        let mut results = Vec::with_capacity(ordered.len());
        for batch in ordered.chunks(MATERIALIZE_BATCH) {
            let mut traces = self.load_candidates(namespace, batch.to_vec()).await?;
            traces.retain(|trace| satisfies(trace, &clauses));
            results.extend(traces);
        }
        Ok(results)
    }

    /// Candidates overlapping `[start_ns, end_ns]`, in result order
    /// `(start, trace ID)`. Exact trace bounds come from locators and page
    /// metadata, so callers can stop loading payloads once they have enough.
    pub(crate) async fn ordered_candidates(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        clauses: &[PushdownClause],
        now: u64,
    ) -> Result<Vec<Located>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let candidates = self
            .candidate_ids(namespace, start_ns, end_ns, clauses, now)
            .await?;
        let mut located = Vec::with_capacity(candidates.len());
        for batch in candidates.chunks(MATERIALIZE_BATCH) {
            let batch = self.scan_locators(namespace, batch, now).await?;
            located.extend(
                self.bound(namespace, batch)
                    .await?
                    .into_iter()
                    .filter(|trace| trace.end_ns >= start_ns && trace.start_ns <= end_ns),
            );
        }
        located.sort_unstable_by_key(|trace| (trace.start_ns, trace.trace_id));
        Ok(located)
    }

    /// Loads located candidates without verifying them.
    pub(crate) async fn load_candidates(
        &self,
        namespace: &Namespace,
        located: Vec<Located>,
    ) -> Result<Vec<Trace>> {
        self.load_located(
            namespace,
            located
                .into_iter()
                .map(|trace| (trace.trace_id, trace.locators))
                .collect(),
        )
        .await
    }

    /// Candidate IDs from the live segments in range. Without clauses every
    /// trace in page metadata is a candidate; otherwise candidates union
    /// within a clause and intersect across clauses.
    async fn candidate_ids(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        clauses: &[PushdownClause],
        now: u64,
    ) -> Result<Vec<TraceId>> {
        let segments = self
            .catalog_segments(namespace, start_ns, end_ns)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        if clauses.is_empty() {
            let per_segment = stream::iter(segments)
                .map(|segment| async move {
                    let mut found = Vec::new();
                    let mut metadata = self
                        .storage
                        .scan_prefix_iter(
                            metadata_prefix(namespace, segment),
                            BytesRange::unbounded(),
                            None,
                        )
                        .await?;
                    while let Some(record) = metadata.next().await? {
                        let page_metadata = decode_metadata(&record.value)?;
                        if page_metadata.is_expired_at(now)
                            || !page_metadata.overlaps(start_ns, end_ns)
                        {
                            continue;
                        }
                        found.extend(
                            page_metadata
                                .traces
                                .iter()
                                .filter(|trace| trace.overlaps(start_ns, end_ns))
                                .map(|trace| trace.trace_id),
                        );
                    }
                    Ok::<_, Error>(found)
                })
                .buffered(SEGMENT_SCAN_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            let mut candidates = Candidates::default();
            for trace_id in per_segment.into_iter().flatten() {
                candidates.insert(trace_id);
            }
            return Ok(candidates.order);
        }

        let segments = &segments;
        let matched = futures::future::try_join_all(clauses.iter().map(|clause| async move {
            let alternatives = futures::future::try_join_all(clause.iter().map(|matcher| {
                self.posting_candidates(namespace, segments, matcher, start_ns, end_ns, now)
            }))
            .await?;
            let mut union = Candidates::default();
            for trace_id in alternatives.into_iter().flat_map(|found| found.order) {
                union.insert(trace_id);
            }
            Ok::<_, Error>(union)
        }))
        .await?;
        let mut matched = matched.into_iter();
        let mut candidates = matched.next().unwrap_or_default();
        for other in matched {
            candidates.retain_in(&other);
        }
        Ok(candidates.order)
    }

    /// Resolves one matcher's postings to trace IDs through page metadata,
    /// which carries the page directory, so no payload is fetched.
    async fn posting_candidates(
        &self,
        namespace: &Namespace,
        segments: &[SegmentId],
        matcher: &AttributeMatcher,
        start_ns: u64,
        end_ns: u64,
        now: u64,
    ) -> Result<Candidates> {
        let per_segment = stream::iter(segments.iter().copied())
            .map(|segment| async move {
                let mut postings = self
                    .storage
                    .scan_prefix_iter(
                        posting_scan_prefix(namespace, segment, matcher),
                        BytesRange::unbounded(),
                        None,
                    )
                    .await?;
                let mut pages = Vec::new();
                while let Some(record) = postings.next().await? {
                    pages.push((
                        decode_posting_sequence(&record.key)?,
                        decode_indices(&record.value)?,
                    ));
                }
                let mut pages = stream::iter(pages)
                    .map(|(sequence, indices)| async move {
                        let record = self
                            .storage
                            .get(metadata_key(namespace, segment, sequence))
                            .await?
                            .ok_or_else(|| {
                                Error::Corrupt(
                                    "attribute posting references missing metadata".to_owned(),
                                )
                            })?;
                        Ok::<_, Error>((decode_metadata(&record.value)?, indices))
                    })
                    .buffered(READ_CONCURRENCY);
                let mut found = Vec::new();
                while let Some((metadata, indices)) = pages.try_next().await? {
                    if metadata.is_expired_at(now) || !metadata.overlaps(start_ns, end_ns) {
                        continue;
                    }
                    for index in indices {
                        let trace = metadata.traces.get(index as usize).ok_or_else(|| {
                            Error::Corrupt(
                                "attribute posting trace index is out of bounds".to_owned(),
                            )
                        })?;
                        if trace.overlaps(start_ns, end_ns) {
                            found.push(trace.trace_id);
                        }
                    }
                }
                Ok::<_, Error>(found)
            })
            .buffered(SEGMENT_SCAN_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut candidates = Candidates::default();
        for trace_id in per_segment.into_iter().flatten() {
            candidates.insert(trace_id);
        }
        Ok(candidates)
    }

    /// Enumerates up to `limit` live traces by scanning locator records that
    /// actually exist. Unlike a full-range search, this does not walk every
    /// theoretical time segment between zero and `u64::MAX`.
    pub async fn scan_traces(&self, namespace: &Namespace, limit: usize) -> Result<Vec<Trace>> {
        let scanned = self.scan_trace_ids(namespace, limit).await?;
        self.load_scanned(namespace, scanned).await
    }

    /// Lists typed scalar attribute names from the partition-local discovery
    /// catalog without reading trace payloads.
    pub async fn catalog_names(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
    ) -> Result<Vec<String>> {
        let segments = self.catalog_segments(namespace, start_ns, end_ns).await?;
        let mut names = BTreeSet::new();
        for segment in segments {
            let key = (namespace.clone(), segment, scope);
            let segment_names = if let Some(names) = self.catalog_names_cache.get(&key) {
                names
            } else {
                let names = discovery::names(
                    self.storage.as_ref(),
                    &segment_prefix(namespace, segment),
                    scope.map(catalog_scope),
                )
                .await?;
                self.catalog_names_cache
                    .insert(key, names, self.is_active_segment(segment))
            };
            names.extend(segment_names.iter().cloned());
        }
        Ok(names.into_iter().collect())
    }

    /// Lists typed scalar attribute values from the partition-local discovery
    /// catalog without reading trace payloads.
    pub async fn catalog_values(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
        name: &str,
    ) -> Result<Vec<DiscoveryValue>> {
        let segments = self.catalog_segments(namespace, start_ns, end_ns).await?;
        let scopes: &[AttributeScope] = match scope {
            Some(AttributeScope::Resource) => &[AttributeScope::Resource],
            Some(AttributeScope::Span) => &[AttributeScope::Span],
            None => &[AttributeScope::Resource, AttributeScope::Span],
        };
        let mut values = BTreeSet::new();
        for segment in segments {
            let key = (namespace.clone(), segment, scope, name.to_owned());
            let segment_values = if let Some(values) = self.catalog_values_cache.get(&key) {
                values
            } else {
                let prefix = segment_prefix(namespace, segment);
                let mut found = BTreeSet::new();
                for scope in scopes {
                    found.extend(
                        discovery::values(
                            self.storage.as_ref(),
                            &prefix,
                            catalog_scope(*scope),
                            name,
                        )
                        .await?,
                    );
                }
                self.catalog_values_cache.insert(
                    key,
                    found.into_iter().collect(),
                    self.is_active_segment(segment),
                )
            };
            values.extend(segment_values.iter().cloned());
        }
        Ok(values.into_iter().collect())
    }

    fn is_active_segment(&self, segment: SegmentId) -> bool {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
            .unwrap_or(u64::MAX);
        segment_for(now_ns, self.segment_ns) == segment
    }

    /// Finds live data partitions from the compact namespace partition
    /// catalog, without trace-locator fan-out.
    async fn catalog_segments(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
    ) -> Result<BTreeSet<SegmentId>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let first = segment_for(start_ns, self.segment_ns);
        let last = segment_for(end_ns, self.segment_ns);
        let mut segments = BTreeSet::new();
        for value in discovery::values(
            self.storage.as_ref(),
            &segment_prefix(namespace, LOCATOR_SEGMENT),
            PARTITION_SCOPE,
            PARTITION_NAME,
        )
        .await?
        {
            match value {
                DiscoveryValue::Int(segment) if segment >= first && segment <= last => {
                    segments.insert(segment);
                }
                DiscoveryValue::Int(_) => {}
                _ => {
                    return Err(Error::Corrupt(
                        "track partition catalog contains a non-integer segment".to_owned(),
                    ));
                }
            }
        }
        Ok(segments)
    }

    /// The first `limit` live trace IDs in ID order, with each trace's first
    /// page. Reads locators only.
    pub(crate) async fn scan_trace_ids(
        &self,
        namespace: &Namespace,
        limit: usize,
    ) -> Result<Vec<ScannedTrace>> {
        if limit == 0 {
            return Err(Error::Invalid(
                "trace scan limit must be greater than zero".to_owned(),
            ));
        }
        let now = unix_time_ms()?;
        let mut first_pages = HashMap::new();
        let mut records = self
            .storage
            .scan_prefix_iter(
                locator_namespace_prefix(namespace),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while let Some(record) = records.next().await? {
            let locator = decode_locator(&record.value)?;
            if locator.is_expired_at(now) {
                continue;
            }
            let trace_id = decode_locator_trace_id(&record.key)?;
            first_pages
                .entry(trace_id)
                .or_insert((locator.segment, locator.page_sequence));
        }
        let mut scanned = first_pages
            .into_iter()
            .map(|(trace_id, first_page)| ScannedTrace {
                trace_id,
                first_page,
            })
            .collect::<Vec<_>>();
        scanned.sort_unstable_by_key(|trace| trace.trace_id);
        scanned.truncate(limit);
        Ok(scanned)
    }

    /// Loads traces found by [`scan_trace_ids`](Self::scan_trace_ids),
    /// ordered by start time.
    pub(crate) async fn load_scanned(
        &self,
        namespace: &Namespace,
        scanned: Vec<ScannedTrace>,
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        let candidates = order_by_first_page(
            scanned
                .into_iter()
                .map(|scanned| (scanned.trace_id, scanned.first_page))
                .collect(),
        );
        let mut traces = self.materialize(namespace, &candidates, now).await?;
        traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(traces)
    }

    /// Parses, validates, plans, and executes a non-metrics TraceQL query.
    ///
    /// Safe positive equalities are used as index candidates; the complete
    /// query is always evaluated against decoded traces.
    pub async fn query_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<TraceQlResult>> {
        execute_traceql(
            &[self],
            None,
            namespace,
            (start_ns, end_ns),
            source,
            options,
        )
        .await
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

    /// Decodes every live continuation of each trace and merges them. Work
    /// runs in bounded batches: locator scans and payload fetches are
    /// concurrent within a batch, and each page is fetched once per batch no
    /// matter how many of its traces are wanted. Callers order `trace_ids` by
    /// page so batches share pages.
    async fn materialize(
        &self,
        namespace: &Namespace,
        trace_ids: &[TraceId],
        now: u64,
    ) -> Result<Vec<Trace>> {
        let mut traces = Vec::with_capacity(trace_ids.len());
        for batch in trace_ids.chunks(MATERIALIZE_BATCH) {
            let located = self.scan_locators(namespace, batch, now).await?;
            traces.extend(self.load_located(namespace, located).await?);
        }
        Ok(traces)
    }

    /// Live locators of each trace, concurrently.
    async fn scan_locators(
        &self,
        namespace: &Namespace,
        trace_ids: &[TraceId],
        now: u64,
    ) -> Result<Vec<(TraceId, Vec<TraceLocator>)>> {
        stream::iter(trace_ids.iter().copied())
            .map(|trace_id| async move {
                let mut records = self
                    .storage
                    .scan_prefix_iter(
                        locator_prefix(namespace, trace_id),
                        BytesRange::unbounded(),
                        None,
                    )
                    .await?;
                let mut locators = Vec::new();
                while let Some(record) = records.next().await? {
                    let locator = decode_locator(&record.value)?;
                    if !locator.is_expired_at(now) {
                        locators.push(locator);
                    }
                }
                Ok::<_, Error>((trace_id, locators))
            })
            .buffered(READ_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Exact time bounds of each located trace from its pages' metadata:
    /// merging continuations only drops duplicate spans, so the merged trace
    /// spans the union of its continuations' ranges. Traces with no live
    /// continuation are dropped.
    async fn bound(
        &self,
        namespace: &Namespace,
        located: Vec<(TraceId, Vec<TraceLocator>)>,
    ) -> Result<Vec<Located>> {
        let wanted: BTreeSet<PageRef> = located
            .iter()
            .flat_map(|(_, locators)| locators.iter().map(TraceLocator::page))
            .collect();
        let metadata: HashMap<PageRef, StoredPageMetadata> = stream::iter(wanted)
            .map(|page @ (segment, sequence)| async move {
                let record = self
                    .storage
                    .get(metadata_key(namespace, segment, sequence))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("trace locator references missing metadata".to_owned())
                    })?;
                Ok::<_, Error>((page, decode_metadata(&record.value)?))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        located
            .into_iter()
            .filter(|(_, locators)| !locators.is_empty())
            .map(|(trace_id, locators)| {
                let mut start_ns = u64::MAX;
                let mut end_ns = 0;
                for locator in &locators {
                    let trace = metadata[&locator.page()]
                        .traces
                        .get(locator.trace_index as usize)
                        .filter(|trace| trace.trace_id == trace_id)
                        .ok_or_else(|| {
                            Error::Corrupt("trace locator disagrees with page metadata".to_owned())
                        })?;
                    start_ns = start_ns.min(trace.min_timestamp_ns);
                    end_ns = end_ns.max(trace.max_timestamp_ns);
                }
                Ok(Located {
                    trace_id,
                    locators,
                    start_ns,
                    end_ns,
                })
            })
            .collect()
    }

    /// Fetches each referenced page once, then decodes and merges every
    /// trace's continuations, preserving input order.
    async fn load_located(
        &self,
        namespace: &Namespace,
        located: Vec<(TraceId, Vec<TraceLocator>)>,
    ) -> Result<Vec<Trace>> {
        let wanted: BTreeSet<PageRef> = located
            .iter()
            .flat_map(|(_, locators)| locators.iter().map(TraceLocator::page))
            .collect();
        let pages: HashMap<PageRef, Page> = stream::iter(wanted)
            .map(|page @ (segment, sequence)| async move {
                let payload = self
                    .storage
                    .get(payload_key(namespace, segment, sequence))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("trace locator references a missing page".to_owned())
                    })?;
                Ok::<_, Error>((page, Page::decode(payload.value)?))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        let mut traces = Vec::with_capacity(located.len());
        for (trace_id, locators) in located {
            if locators.is_empty() {
                continue;
            }
            let continuations = locators
                .iter()
                .map(|locator| {
                    let trace =
                        pages[&locator.page()].decode_trace(locator.trace_index as usize)?;
                    if trace.trace_id != trace_id {
                        return Err(Error::Corrupt(
                            "trace locator points to a different trace".to_owned(),
                        ));
                    }
                    Ok(trace)
                })
                .collect::<Result<Vec<_>>>()?;
            traces.push(merge_continuations(trace_id, continuations)?);
        }
        Ok(traces)
    }
}

struct TraceWrite {
    namespace: Namespace,
    groups: BTreeMap<SegmentId, Vec<Trace>>,
    report: WriteReport,
}

type TraceGroup = (Namespace, SegmentId);

#[derive(Default)]
struct TraceWriteDelta {
    groups: BTreeMap<TraceGroup, Vec<Trace>>,
    estimated_size: usize,
}

impl Delta for TraceWriteDelta {
    type Context = ();
    type Write = TraceWrite;
    type Frozen = BTreeMap<TraceGroup, Vec<Trace>>;
    type FrozenView = ();
    type ApplyResult = WriteReport;
    type DeltaView = ();
    type Snapshot = Arc<dyn StorageSnapshot>;

    fn init((): Self::Context) -> Self {
        Self::default()
    }

    fn apply(&mut self, write: Self::Write) -> std::result::Result<Self::ApplyResult, String> {
        for (segment, traces) in write.groups {
            self.estimated_size = self.estimated_size.saturating_add(
                traces
                    .iter()
                    .flat_map(|trace| &trace.resource_spans)
                    .map(Message::encoded_len)
                    .sum::<usize>(),
            );
            self.groups
                .entry((write.namespace.clone(), segment))
                .or_default()
                .extend(traces);
        }
        Ok(write.report)
    }

    fn estimate_size(&self) -> usize {
        self.estimated_size
    }

    fn freeze(mut self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        for traces in self.groups.values_mut() {
            traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        }
        (self.groups, (), ())
    }

    fn reader(&self) -> Self::DeltaView {}
}

struct TraceFlusher {
    storage: Arc<dyn Storage>,
    page_config: PageConfig,
    retention: Option<Duration>,
}

#[async_trait]
impl Flusher<TraceWriteDelta> for TraceFlusher {
    async fn flush_delta(
        &mut self,
        frozen: BTreeMap<TraceGroup, Vec<Trace>>,
        _epoch_range: &Range<u64>,
    ) -> std::result::Result<Arc<dyn StorageSnapshot>, String> {
        let pages = direct_write(
            self.storage.as_ref(),
            &self.page_config,
            self.retention,
            frozen,
        )
        .await
        .map_err(|error| error.to_string())?;
        metrics::histogram!(TRACK_FLUSH_PAGES).record(pages as f64);
        self.storage
            .snapshot()
            .await
            .map_err(|error| error.to_string())
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

async fn direct_write(
    storage: &dyn Storage,
    page_config: &PageConfig,
    retention: Option<Duration>,
    groups: BTreeMap<TraceGroup, Vec<Trace>>,
) -> Result<usize> {
    if groups.is_empty() {
        return Ok(0);
    }
    let retention = retention_values(retention)?;
    let mut ops = Vec::new();
    let mut catalogs = BTreeMap::<(Namespace, SegmentId), CatalogBatch>::new();
    let mut partition_catalogs = BTreeMap::<Namespace, CatalogBatch>::new();
    let mut pages = 0usize;
    for ((namespace, segment), traces) in groups {
        partition_catalogs
            .entry(namespace.clone())
            .or_default()
            .insert(
                PARTITION_SCOPE,
                PARTITION_NAME,
                DiscoveryValue::Int(segment),
            );
        let catalog = catalogs.entry((namespace.clone(), segment)).or_default();
        let sequence_key = next_sequence_key(&namespace, segment);
        let mut sequence = storage
            .get(sequence_key.clone())
            .await?
            .map(|record| decode_sequence(&record.value))
            .transpose()?
            .unwrap_or(0);
        let mut builder = PageBuilder::new(page_config.clone())?;
        let mut cut_page = |(page, traces): (Page, Vec<Trace>)| -> Result<()> {
            append_page_ops(
                &mut ops,
                &namespace,
                PageWriteId { segment, sequence },
                &page,
                &traces,
                retention,
                catalog,
            )?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("page sequence exhausted".to_owned()))?;
            pages = pages.saturating_add(1);
            Ok(())
        };
        for trace in traces {
            if let Some(completed) = builder.append_with_traces(trace)? {
                cut_page(completed)?;
            }
        }
        if let Some(completed) = builder.finish_with_traces()? {
            cut_page(completed)?;
        }
        ops.push(put(
            sequence_key,
            encode_sequence(sequence),
            retention.physical_ttl,
        ));
    }
    for ((namespace, segment), catalog) in catalogs {
        ops.extend(catalog.into_ops(&segment_prefix(&namespace, segment), retention.physical_ttl));
    }
    for (namespace, catalog) in partition_catalogs {
        ops.extend(catalog.into_ops(
            &segment_prefix(&namespace, LOCATOR_SEGMENT),
            retention.physical_ttl,
        ));
    }
    storage.apply(ops).await?;
    Ok(pages)
}

fn retention_values(retention: Option<Duration>) -> Result<Retention> {
    let physical_ttl = retention
        .map(|duration| {
            u64::try_from(duration.as_millis())
                .map(Ttl::ExpireAfter)
                .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))
        })
        .transpose()?
        .unwrap_or(Ttl::NoExpiry);
    let expires_at_unix_ms = retention
        .map(|duration| {
            let duration = u64::try_from(duration.as_millis())
                .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))?;
            unix_time_ms()?
                .checked_add(duration)
                .ok_or_else(|| Error::Invalid("retention expiry overflows u64".to_owned()))
        })
        .transpose()?;
    Ok(Retention {
        physical_ttl,
        expires_at_unix_ms,
    })
}

fn map_write_error(error: WriteError) -> Error {
    match error {
        WriteError::Backpressure(_) | WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Unavailable("write coordinator is shut down".to_owned()),
        WriteError::ApplyError(_, message) => Error::Invalid(message),
        WriteError::FlushError(message) | WriteError::Internal(message) => {
            Error::Unavailable(message)
        }
    }
}

/// Search candidates deduplicated in discovery order, which follows page
/// order so materialization batches share pages.
#[derive(Default)]
struct Candidates {
    order: Vec<TraceId>,
    seen: HashSet<TraceId>,
}

impl Candidates {
    fn insert(&mut self, trace_id: TraceId) {
        if self.seen.insert(trace_id) {
            self.order.push(trace_id);
        }
    }

    fn retain_in(&mut self, other: &Self) {
        self.order.retain(|trace_id| other.seen.contains(trace_id));
        self.seen.retain(|trace_id| other.seen.contains(trace_id));
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScannedTrace {
    pub(crate) trace_id: TraceId,
    first_page: PageRef,
}

/// A trace's live continuations and exact time bounds, before any payload
/// is fetched.
#[derive(Clone, Debug)]
pub(crate) struct Located {
    trace_id: TraceId,
    locators: Vec<TraceLocator>,
    start_ns: u64,
    end_ns: u64,
}

fn order_by_first_page(first_pages: HashMap<TraceId, PageRef>) -> Vec<TraceId> {
    let mut ordered: Vec<_> = first_pages.into_iter().collect();
    ordered.sort_unstable_by_key(|&(trace_id, page)| (page, trace_id));
    ordered.into_iter().map(|(trace_id, _)| trace_id).collect()
}

#[derive(Clone, Copy)]
struct PageWriteId {
    segment: SegmentId,
    sequence: u64,
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    id: PageWriteId,
    page: &Page,
    traces: &[Trace],
    retention: Retention,
    catalog: &mut CatalogBatch,
) -> Result<()> {
    let directory = page.directory();
    let metadata = StoredPageMetadata {
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns: directory
            .iter()
            .map(|entry| entry.min_timestamp_ns)
            .min()
            .unwrap(),
        max_timestamp_ns: directory
            .iter()
            .map(|entry| entry.max_timestamp_ns)
            .max()
            .unwrap(),
        traces: directory
            .iter()
            .map(|entry| PageTrace {
                trace_id: entry.trace_id,
                min_timestamp_ns: entry.min_timestamp_ns,
                max_timestamp_ns: entry.max_timestamp_ns,
            })
            .collect(),
    };
    ops.push(put(
        metadata_key(namespace, id.segment, id.sequence),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, id.segment, id.sequence),
        page.bytes(),
        retention.physical_ttl,
    ));

    // Keyed by the encoded posting key, which already identifies matchers
    // exactly (scope, name, and typed value, with doubles compared by bits).
    let mut postings: HashMap<Bytes, Vec<u32>> = HashMap::new();
    for (index, (entry, trace)) in page.directory().iter().zip(traces).enumerate() {
        debug_assert_eq!(entry.trace_id, trace.trace_id);
        ops.push(put(
            locator_key(namespace, trace.trace_id, id.segment, id.sequence),
            encode_locator(&TraceLocator {
                segment: id.segment,
                page_sequence: id.sequence,
                trace_index: u32::try_from(index)
                    .map_err(|_| Error::Invalid("trace index exceeds u32".to_owned()))?,
                expires_at_unix_ms: retention.expires_at_unix_ms,
            })?,
            retention.physical_ttl,
        ));
        let mut seen = Vec::new();
        collect_trace_attributes(trace, &mut seen);
        for matcher in seen {
            catalog.insert(
                catalog_scope(matcher.scope),
                matcher.name.clone(),
                discovery_value(&matcher.value),
            );
            postings
                .entry(posting_key(namespace, id.segment, &matcher, id.sequence))
                .or_default()
                .push(index as u32);
        }
    }
    for (key, mut indices) in postings {
        indices.sort_unstable();
        indices.dedup();
        ops.push(put(key, encode_indices(&indices)?, retention.physical_ttl));
    }
    Ok(())
}

fn catalog_scope(scope: AttributeScope) -> &'static str {
    match scope {
        AttributeScope::Resource => "resource",
        AttributeScope::Span => "span",
    }
}

fn discovery_value(value: &AttributeValue) -> DiscoveryValue {
    match value {
        AttributeValue::String(value) => DiscoveryValue::String(value.clone()),
        AttributeValue::Bool(value) => DiscoveryValue::Bool(*value),
        AttributeValue::Int(value) => DiscoveryValue::Int(*value),
        AttributeValue::Double(value) => DiscoveryValue::Double(*value),
    }
}

fn collect_trace_attributes(trace: &Trace, output: &mut Vec<AttributeMatcher>) {
    for resource_spans in &trace.resource_spans {
        if let Some(resource) = &resource_spans.resource {
            collect_attributes(AttributeScope::Resource, &resource.attributes, output);
        }
        for scope_spans in &resource_spans.scope_spans {
            for span in &scope_spans.spans {
                collect_attributes(AttributeScope::Span, &span.attributes, output);
            }
        }
    }
    output.dedup_by(|left, right| {
        left.scope == right.scope && left.name == right.name && left.value.exact_eq(&right.value)
    });
}

fn collect_attributes(
    scope: AttributeScope,
    attributes: &[KeyValue],
    output: &mut Vec<AttributeMatcher>,
) {
    for attribute in attributes {
        if let Some(value) = attribute.value.as_ref().and_then(AttributeValue::from_otlp) {
            output.push(AttributeMatcher {
                scope,
                name: attribute.key.clone(),
                value,
            });
        }
    }
}

/// Whether `trace` holds at least one alternative of every clause.
fn satisfies(trace: &Trace, clauses: &[PushdownClause]) -> bool {
    clauses
        .iter()
        .all(|clause| clause.iter().any(|matcher| trace_matches(trace, matcher)))
}

/// Merges partial traces sharing a trace ID, which occur when a trace's
/// spans were routed to different shards on either side of an epoch cutover.
pub(crate) fn merge_traces(traces: Vec<Trace>) -> Vec<Trace> {
    let mut merged = BTreeMap::<TraceId, Trace>::new();
    for trace in traces {
        match merged.entry(trace.trace_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(trace);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().resource_spans.extend(trace.resource_spans);
            }
        }
    }
    merged.into_values().collect()
}

/// A trace's candidate continuations on each shard, ordered by the earliest.
struct CandidateGroup {
    start_ns: u64,
    trace_id: TraceId,
    parts: Vec<(usize, Located)>,
}

/// Executes a non-metrics TraceQL query over one or more shards.
///
/// Candidates from every shard are merged into global result order
/// `(start, trace ID)` before any payload is loaded, and a trace split across
/// shards is merged before evaluation. Payloads load in order until `limit`
/// results are found, and every loaded trace counts toward
/// `max_candidate_traces`, so shards never each load a full limit.
pub(crate) async fn execute_traceql(
    shards: &[&TraceDb],
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    (start_ns, end_ns): (u64, u64),
    source: &str,
    options: QueryOptions,
) -> Result<Vec<TraceQlResult>> {
    if options.max_candidate_traces == 0 {
        return Err(crate::traceql::QueryError::Limit(
            "max_candidate_traces must be greater than zero".to_owned(),
        )
        .into());
    }
    if options.max_spans_per_trace == 0 {
        return Err(crate::traceql::QueryError::Limit(
            "max_spans_per_trace must be greater than zero".to_owned(),
        )
        .into());
    }
    if options.max_concurrency == 0 {
        return Err(crate::traceql::QueryError::Limit(
            "max_concurrency must be greater than zero".to_owned(),
        )
        .into());
    }
    let query = crate::traceql::parse(source)?;
    if let Some(name) = query.stages.iter().find_map(|stage| match stage {
        crate::traceql::PipelineStage::Metric { name, .. } => Some(name),
        _ => None,
    }) {
        return Err(crate::traceql::QueryError::Unsupported(format!(
            "metric stage `{name}` is parsed but not executable"
        ))
        .into());
    }
    let plan = crate::traceql::plan(query)?;
    let now = unix_time_ms()?;
    let acquire = || async {
        match permits {
            Some(permits) => permits
                .acquire()
                .await
                .map(Some)
                .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned())),
            None => Ok(None),
        }
    };

    let per_shard = futures::future::try_join_all(shards.iter().map(|database| async {
        let _permit = acquire().await?;
        database
            .ordered_candidates(namespace, start_ns, end_ns, &plan.pushdown, now)
            .await
    }))
    .await?;
    let mut groups = HashMap::<TraceId, CandidateGroup>::new();
    for (shard, located) in per_shard.into_iter().enumerate() {
        for located in located {
            let group = groups
                .entry(located.trace_id)
                .or_insert_with(|| CandidateGroup {
                    start_ns: located.start_ns,
                    trace_id: located.trace_id,
                    parts: Vec::new(),
                });
            group.start_ns = group.start_ns.min(located.start_ns);
            group.parts.push((shard, located));
        }
    }
    let mut ordered = groups.into_values().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|group| (group.start_ns, group.trace_id));

    let mut results = Vec::new();
    let mut loaded = 0;
    let mut remaining = ordered.into_iter();
    while results.len() < options.limit {
        let wanted = (options.limit - results.len())
            .max(options.max_concurrency)
            .min(MATERIALIZE_BATCH);
        let batch = remaining.by_ref().take(wanted).collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        loaded += batch.len();
        if loaded > options.max_candidate_traces {
            return Err(crate::traceql::QueryError::Limit(format!(
                "{loaded} candidate traces exceeds maximum {}",
                options.max_candidate_traces
            ))
            .into());
        }
        let order = batch.iter().map(|group| group.trace_id).collect::<Vec<_>>();
        let mut by_shard = vec![Vec::new(); shards.len()];
        for group in batch {
            for (shard, located) in group.parts {
                by_shard[shard].push(located);
            }
        }
        let parts = futures::future::try_join_all(
            by_shard
                .into_iter()
                .enumerate()
                .filter(|(_, located)| !located.is_empty())
                .map(|(shard, located)| async move {
                    let _permit = acquire().await?;
                    shards[shard].load_candidates(namespace, located).await
                }),
        )
        .await?;
        let mut merged = merge_traces(parts.into_iter().flatten().collect())
            .into_iter()
            .map(|trace| (trace.trace_id, trace))
            .collect::<HashMap<_, _>>();
        let traces = order
            .into_iter()
            .filter_map(|trace_id| merged.remove(&trace_id))
            .filter(|trace| satisfies(trace, &plan.pushdown))
            .collect::<Vec<_>>();
        let mut executed = stream::iter(traces)
            .map(|trace| {
                let query = plan.query.clone();
                let max_spans = options.max_spans_per_trace;
                tokio::spawn(async move { crate::traceql::execute(&trace, &query, max_spans) })
            })
            .buffered(options.max_concurrency);
        while let Some(result) = executed.next().await {
            let result = result
                .map_err(|error| Error::Invalid(format!("TraceQL task failed: {error}")))??;
            if let Some(result) = result {
                results.push(result);
                if results.len() == options.limit {
                    break;
                }
            }
        }
    }
    Ok(results)
}

fn trace_matches(trace: &Trace, matcher: &AttributeMatcher) -> bool {
    match matcher.scope {
        AttributeScope::Resource => trace.resource_spans.iter().any(|resource_spans| {
            resource_spans.resource.as_ref().is_some_and(|resource| {
                attributes_match(&resource.attributes, &matcher.name, &matcher.value)
            })
        }),
        AttributeScope::Span => trace.resource_spans.iter().any(|resource_spans| {
            resource_spans.scope_spans.iter().any(|scope_spans| {
                scope_spans
                    .spans
                    .iter()
                    .any(|span| attributes_match(&span.attributes, &matcher.name, &matcher.value))
            })
        }),
    }
}

fn attributes_match(attributes: &[KeyValue], name: &str, value: &AttributeValue) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key == name
            && attribute
                .value
                .as_ref()
                .and_then(AttributeValue::from_otlp)
                .is_some_and(|found| found.exact_eq(value))
    })
}

/// Drops spans whose resource, scope and protobuf encoding repeat an earlier
/// span. Only spans sharing a span id can be equal, so only those are encoded.
fn merge_continuations(trace_id: TraceId, continuations: Vec<Trace>) -> Result<Trace> {
    let mut span_id_counts: HashMap<Vec<u8>, u32> = HashMap::new();
    for span in continuations
        .iter()
        .flat_map(|trace| &trace.resource_spans)
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| &scope.spans)
    {
        *span_id_counts.entry(span.span_id.clone()).or_default() += 1;
    }
    let mut seen = HashSet::new();
    let mut encoded = Vec::new();
    let mut resource_spans = Vec::new();
    for continuation in continuations {
        for mut resource in continuation.resource_spans {
            let resource_context = resource_context_bytes(&resource);
            let mut retained_scopes = Vec::new();
            for mut scope in std::mem::take(&mut resource.scope_spans) {
                let scope_context = scope_context_bytes(&scope);
                let mut retained_spans = Vec::new();
                for span in std::mem::take(&mut scope.spans) {
                    if span_id_counts
                        .get(&span.span_id)
                        .is_some_and(|count| *count > 1)
                    {
                        encoded.clear();
                        span.encode(&mut encoded).unwrap();
                        let mut fingerprint = blake3::Hasher::new();
                        for part in [&resource_context, &scope_context, &encoded] {
                            fingerprint.update(&(part.len() as u64).to_le_bytes());
                            fingerprint.update(part);
                        }
                        if !seen.insert(*fingerprint.finalize().as_bytes()) {
                            continue;
                        }
                    }
                    retained_spans.push(span);
                }
                if !retained_spans.is_empty() {
                    scope.spans = retained_spans;
                    retained_scopes.push(scope);
                }
            }
            if !retained_scopes.is_empty() {
                resource.scope_spans = retained_scopes;
                resource_spans.push(resource);
            }
        }
    }
    Trace::new(trace_id, resource_spans)
}

fn resource_context_bytes(resource_spans: &ResourceSpans) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(resource) = &resource_spans.resource {
        resource.encode(&mut bytes).unwrap();
    }
    bytes.extend_from_slice(resource_spans.schema_url.as_bytes());
    bytes
}

fn scope_context_bytes(scope_spans: &ScopeSpans) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(scope) = &scope_spans.scope {
        scope.encode(&mut bytes).unwrap();
    }
    bytes.extend_from_slice(scope_spans.schema_url.as_bytes());
    bytes
}

fn put(key: Bytes, value: Bytes, ttl: Ttl) -> RecordOp {
    RecordOp::Put(PutRecordOp::new_with_options(
        Record::new(key, value),
        PutOptions { ttl },
    ))
}

fn unix_time_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid("system clock is before the Unix epoch".to_owned()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::Invalid("Unix timestamp exceeds u64 milliseconds".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
    };
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };

    use super::*;
    use crate::PageConfig;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "track-test".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            segment_duration: Duration::from_secs(10),
            retention: Some(Duration::from_secs(60)),
            page: PageConfig {
                target_size_bytes: 64 * 1024,
                max_size_bytes: 128 * 1024,
                max_traces: 16,
            },
            write_buffer: Default::default(),
        }
    }

    fn value(value: any_value::Value) -> Option<AnyValue> {
        Some(AnyValue { value: Some(value) })
    }

    fn attr(name: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: name.to_owned(),
            value: self::value(value),
        }
    }

    fn trace(
        id: u8,
        start_ns: u64,
        name: &str,
        resource_attributes: Vec<KeyValue>,
        span_attributes: Vec<KeyValue>,
    ) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        Trace::new(
            trace_id,
            vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: [id; 8].to_vec(),
                        name: name.to_owned(),
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 100,
                        attributes: span_attributes,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stores_traces_in_pages_and_isolates_namespaces() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let tenant_a = Namespace::new("tenant-a").unwrap();
        let tenant_b = Namespace::new("tenant-b").unwrap();
        let first = trace(1, 1, "one", Vec::new(), Vec::new());
        let second = trace(2, 2, "two", Vec::new(), Vec::new());
        let report = db
            .write(
                &tenant_a,
                vec![TraceBatch::new(vec![first.clone(), second.clone()])],
            )
            .await
            .unwrap();
        assert_eq!(report.pages, 0);
        assert_eq!(report.traces, 2);
        db.write(
            &tenant_b,
            vec![TraceBatch::new(vec![trace(
                3,
                3,
                "other",
                Vec::new(),
                Vec::new(),
            )])],
        )
        .await
        .unwrap();

        assert_eq!(
            db.get_trace(&tenant_a, first.trace_id).await.unwrap(),
            Some(first)
        );
        assert!(
            db.get_trace(&tenant_b, second.trace_id)
                .await
                .unwrap()
                .is_none()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn traces_in_a_segment_share_one_page_sequence() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("segment-pages").unwrap();
        let first = trace(1, 1, "first", Vec::new(), Vec::new());
        let second = trace(2, 2, "second", Vec::new(), Vec::new());

        for trace in [&first, &second] {
            db.write(&namespace, vec![TraceBatch::new(vec![trace.clone()])])
                .await
                .unwrap();
        }

        let mut records = db
            .storage
            .scan_prefix_iter(
                metadata_prefix(&namespace, 0),
                BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        let mut sequences = Vec::new();
        while let Some(record) = records.next().await.unwrap() {
            sequences.push(crate::codec::decode_metadata_sequence(&record.key).unwrap());
        }
        assert_eq!(sequences, vec![0, 1]);
        assert_eq!(
            db.get_trace(&namespace, first.trace_id).await.unwrap(),
            Some(first)
        );
        assert_eq!(
            db.get_trace(&namespace, second.trace_id).await.unwrap(),
            Some(second)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn applied_writes_coalesce_into_pages_across_requests_in_timestamp_order() {
        let mut config = test_config();
        config.page.max_traces = 16;
        let db = TraceDb::open(config).await.unwrap();
        let namespace = Namespace::new("coalesced").unwrap();
        let mut early = trace(1, 1, "span", Vec::new(), Vec::new());
        let mut late = trace(2, 2, "span", Vec::new(), Vec::new());
        early.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = 10;
        early.resource_spans[0].scope_spans[0].spans[0].end_time_unix_nano = 11;
        late.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = 20;
        late.resource_spans[0].scope_spans[0].spans[0].end_time_unix_nano = 21;
        let late_report = db
            .write_with_durability(
                &namespace,
                vec![TraceBatch::new(vec![late.clone()])],
                Durability::Applied,
            )
            .await
            .unwrap();
        let early_report = db
            .write_with_durability(
                &namespace,
                vec![TraceBatch::new(vec![early.clone()])],
                Durability::Applied,
            )
            .await
            .unwrap();
        assert_eq!(
            late_report,
            WriteReport {
                traces: 1,
                pages: 0,
                spans: 1
            }
        );
        assert_eq!(
            early_report,
            WriteReport {
                traces: 1,
                pages: 0,
                spans: 1
            }
        );
        assert!(
            db.get_trace(&namespace, early.trace_id)
                .await
                .unwrap()
                .is_none(),
            "Applied acknowledges the in-memory delta only"
        );

        db.flush().await.unwrap();
        let mut records = db
            .storage
            .scan_prefix_iter(
                metadata_prefix(&namespace, 0),
                BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        let page = records.next().await.unwrap().unwrap();
        let metadata = decode_metadata(&page.value).unwrap();
        assert_eq!(
            metadata
                .traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<Vec<_>>(),
            vec![early.trace_id, late.trace_id]
        );
        assert!(records.next().await.unwrap().is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn written_write_flushes_delta_before_returning() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("written").unwrap();
        let original = trace(1, 1, "written", Vec::new(), Vec::new());

        db.write_with_durability(
            &namespace,
            vec![TraceBatch::new(vec![original.clone()])],
            Durability::Written,
        )
        .await
        .unwrap();

        assert_eq!(
            db.get_trace(&namespace, original.trace_id).await.unwrap(),
            Some(original)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn durable_write_is_visible_to_a_new_storage_reader() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.retention = None;
        config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "track-durable".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });
        let namespace = Namespace::new("durable").unwrap();
        let original = trace(1, 1, "durable", Vec::new(), Vec::new());
        let db = TraceDb::open(config.clone()).await.unwrap();

        db.write_with_durability(
            &namespace,
            vec![TraceBatch::new(vec![original.clone()])],
            Durability::Durable,
        )
        .await
        .unwrap();
        let reader = TraceDb::open_reader(config, DbReaderOptions::default())
            .await
            .unwrap();

        assert_eq!(
            reader
                .get_trace(&namespace, original.trace_id)
                .await
                .unwrap(),
            Some(original)
        );
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_drains_applied_delta_before_storage_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.retention = None;
        config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "track-shutdown".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });
        let namespace = Namespace::new("shutdown").unwrap();
        let original = trace(1, 1, "pending", Vec::new(), Vec::new());
        let db = TraceDb::open(config.clone()).await.unwrap();
        db.write_with_durability(
            &namespace,
            vec![TraceBatch::new(vec![original.clone()])],
            Durability::Applied,
        )
        .await
        .unwrap();

        db.close().await.unwrap();
        let reopened = TraceDb::open(config).await.unwrap();
        assert_eq!(
            reopened
                .get_trace(&namespace, original.trace_id)
                .await
                .unwrap(),
            Some(original)
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn routes_time_segments_and_finds_trace_by_id() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let early = trace(1, 1, "early", Vec::new(), Vec::new());
        let late = trace(2, 11_000_000_000, "late", Vec::new(), Vec::new());
        let report = db
            .write(
                &namespace,
                vec![TraceBatch::new(vec![early.clone(), late.clone()])],
            )
            .await
            .unwrap();
        assert_eq!(report.pages, 0);
        assert_eq!(
            db.get_trace(&namespace, late.trace_id).await.unwrap(),
            Some(late.clone())
        );
        assert_eq!(
            db.search(&namespace, 10_000_000_000, 12_000_000_000, &[])
                .await
                .unwrap(),
            vec![late.clone()]
        );
        assert_eq!(
            db.search(&namespace, 0, u64::MAX, &[]).await.unwrap(),
            vec![early, late]
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_and_continuation_writes_merge_without_losing_spans() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let first = trace(1, 10, "first", Vec::new(), Vec::new());
        let continuation = trace(1, 20, "second", Vec::new(), Vec::new());
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![first.clone(), first.clone()])],
        )
        .await
        .unwrap();
        db.write(&namespace, vec![TraceBatch::new(vec![continuation])])
            .await
            .unwrap();
        let merged = db
            .get_trace(&namespace, first.trace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged.spans().count(), 2);
        assert_eq!(
            merged
                .spans()
                .map(|span| span.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["first", "second"])
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn search_merges_continuations_across_pages_and_batches() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let count = MATERIALIZE_BATCH as u64 + 3;
        let single_span = |index: u64, span_id: u8, start_ns: u64| {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(index + 1).to_be_bytes());
            Trace::new(
                TraceId::new(id).unwrap(),
                vec![ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            trace_id: id.to_vec(),
                            span_id: vec![span_id; 8],
                            start_time_unix_nano: start_ns,
                            end_time_unix_nano: start_ns + 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            )
            .unwrap()
        };
        let traces: Vec<_> = (0..count)
            .map(|index| single_span(index, 1, 1 + index))
            .collect();
        db.write(&namespace, vec![TraceBatch::new(traces.clone())])
            .await
            .unwrap();
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![single_span(0, 2, 5)])],
        )
        .await
        .unwrap();

        let found = db.search(&namespace, 0, 1_000, &[]).await.unwrap();
        assert_eq!(found.len(), count as usize);
        let merged = found
            .iter()
            .find(|trace| trace.trace_id == traces[0].trace_id)
            .unwrap();
        assert_eq!(merged.spans().count(), 2);
        assert_eq!(
            db.scan_traces(&namespace, 2).await.unwrap().len(),
            2,
            "scan limit counts distinct traces"
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn exact_typed_search_distinguishes_values_and_intersects_matchers() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let variants = vec![
            trace(
                1,
                1,
                "string",
                vec![attr(
                    "service",
                    any_value::Value::StringValue("api".to_owned()),
                )],
                vec![attr("value", any_value::Value::StringValue("7".to_owned()))],
            ),
            trace(
                2,
                2,
                "int",
                vec![attr(
                    "service",
                    any_value::Value::StringValue("api".to_owned()),
                )],
                vec![attr("value", any_value::Value::IntValue(7))],
            ),
            trace(
                3,
                3,
                "double",
                Vec::new(),
                vec![attr("value", any_value::Value::DoubleValue(7.0))],
            ),
            trace(
                4,
                4,
                "bool",
                Vec::new(),
                vec![attr("value", any_value::Value::BoolValue(true))],
            ),
        ];
        db.write(&namespace, vec![TraceBatch::new(variants.clone())])
            .await
            .unwrap();
        for (expected, value) in [
            (variants[0].trace_id, AttributeValue::String("7".to_owned())),
            (variants[1].trace_id, AttributeValue::Int(7)),
            (variants[2].trace_id, AttributeValue::Double(7.0)),
            (variants[3].trace_id, AttributeValue::Bool(true)),
        ] {
            let found = db
                .search(
                    &namespace,
                    0,
                    10,
                    &[AttributeMatcher::new(AttributeScope::Span, "value", value).unwrap()],
                )
                .await
                .unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].trace_id, expected);
        }
        let found = db
            .search(
                &namespace,
                0,
                10,
                &[
                    AttributeMatcher::new(
                        AttributeScope::Resource,
                        "service",
                        AttributeValue::String("api".to_owned()),
                    )
                    .unwrap(),
                    AttributeMatcher::new(AttributeScope::Span, "value", AttributeValue::Int(7))
                        .unwrap(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].trace_id, variants[1].trace_id);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_deduplicates_pages_and_segments_without_payload_reads() {
        let mut config = test_config();
        config.page.max_traces = 1;
        let db = TraceDb::open(config).await.unwrap();
        let namespace = Namespace::new("catalog").unwrap();
        let traces = vec![
            trace(
                1,
                1,
                "first",
                vec![attr(
                    "service.name",
                    any_value::Value::StringValue("api".into()),
                )],
                vec![attr("code", any_value::Value::IntValue(200))],
            ),
            trace(
                2,
                2,
                "second",
                vec![attr(
                    "service.name",
                    any_value::Value::StringValue("api".into()),
                )],
                vec![attr("error", any_value::Value::BoolValue(true))],
            ),
            trace(
                3,
                11_000_000_000,
                "later",
                vec![attr(
                    "service.name",
                    any_value::Value::StringValue("worker".into()),
                )],
                vec![attr("ratio", any_value::Value::DoubleValue(0.5))],
            ),
        ];
        db.write(&namespace, vec![TraceBatch::new(traces.clone())])
            .await
            .unwrap();

        // Make every data page undecodable. Catalog reads must continue to
        // work because they only consult locators and catalog records.
        let mut corrupt_pages = Vec::new();
        for trace in &traces {
            let mut locators = db
                .storage
                .scan_prefix_iter(
                    locator_prefix(&namespace, trace.trace_id),
                    BytesRange::unbounded(),
                    None,
                )
                .await
                .unwrap();
            while let Some(record) = locators.next().await.unwrap() {
                let locator = decode_locator(&record.value).unwrap();
                corrupt_pages.push(put(
                    payload_key(&namespace, locator.segment, locator.page_sequence),
                    Bytes::from_static(b"not-a-page"),
                    Ttl::NoExpiry,
                ));
            }
        }
        db.writer
            .as_ref()
            .unwrap()
            .apply(corrupt_pages)
            .await
            .unwrap();

        assert_eq!(
            db.catalog_names(&namespace, 0, 20_000_000_000, None)
                .await
                .unwrap(),
            vec!["code", "error", "ratio", "service.name"]
        );
        assert_eq!(
            db.catalog_names(&namespace, 0, 9_999_999_999, Some(AttributeScope::Resource))
                .await
                .unwrap(),
            vec!["service.name"]
        );
        assert_eq!(
            db.catalog_values(
                &namespace,
                0,
                9_999_999_999,
                Some(AttributeScope::Resource),
                "service.name"
            )
            .await
            .unwrap(),
            vec![DiscoveryValue::String("api".into())]
        );
        assert_eq!(
            db.catalog_values(
                &namespace,
                10_000_000_000,
                20_000_000_000,
                Some(AttributeScope::Resource),
                "service.name"
            )
            .await
            .unwrap(),
            vec![DiscoveryValue::String("worker".into())]
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn unindexed_values_remain_in_payload() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let original = trace(
            1,
            1,
            "bytes",
            vec![attr(
                "opaque",
                any_value::Value::BytesValue(vec![0, 1, 2, 255]),
            )],
            Vec::new(),
        );
        db.write(&namespace, vec![TraceBatch::new(vec![original.clone()])])
            .await
            .unwrap();
        assert_eq!(
            db.get_trace(&namespace, original.trace_id).await.unwrap(),
            Some(original)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn logical_retention_hides_trace_and_search_results() {
        let mut config = test_config();
        config.retention = Some(Duration::from_millis(20));
        let db = TraceDb::open(config).await.unwrap();
        let namespace = Namespace::default();
        let trace = trace(
            1,
            1,
            "short-lived",
            Vec::new(),
            vec![attr("live", any_value::Value::BoolValue(true))],
        );
        db.write(&namespace, vec![TraceBatch::new(vec![trace.clone()])])
            .await
            .unwrap();
        assert!(
            db.get_trace(&namespace, trace.trace_id)
                .await
                .unwrap()
                .is_some()
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            db.get_trace(&namespace, trace.trace_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            db
                .search(
                    &namespace,
                    0,
                    10,
                    &[AttributeMatcher::new(
                        AttributeScope::Span,
                        "live",
                        AttributeValue::Bool(true),
                    )
                    .unwrap()],
                )
                .await
                .unwrap()
                .is_empty()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.retention = None;
        config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "track-reopen".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });
        let namespace = Namespace::default();
        let original = trace(1, 1, "persistent", Vec::new(), Vec::new());
        let db = TraceDb::open(config.clone()).await.unwrap();
        db.write(&namespace, vec![TraceBatch::new(vec![original.clone()])])
            .await
            .unwrap();
        db.close().await.unwrap();

        let reopened = TraceDb::open(config).await.unwrap();
        assert_eq!(
            reopened
                .get_trace(&namespace, original.trace_id)
                .await
                .unwrap(),
            Some(original)
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn traceql_query_executes_with_index_pushdown() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![
                trace(
                    1,
                    1,
                    "wanted",
                    vec![attr(
                        "service.name",
                        any_value::Value::StringValue("api".to_owned()),
                    )],
                    vec![attr("code", any_value::Value::IntValue(200))],
                ),
                trace(
                    2,
                    2,
                    "other",
                    vec![attr(
                        "service.name",
                        any_value::Value::StringValue("worker".to_owned()),
                    )],
                    vec![attr("code", any_value::Value::IntValue(500))],
                ),
            ])],
        )
        .await
        .unwrap();

        let results = db
            .query_traceql(
                &namespace,
                0,
                10,
                r#"{ resource."service.name" = "api" && span.code = 200 }"#,
                QueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "wanted");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn traceql_query_limit_and_order_are_deterministic() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![
                trace(2, 2, "second", Vec::new(), Vec::new()),
                trace(1, 1, "first", Vec::new(), Vec::new()),
            ])],
        )
        .await
        .unwrap();
        let results = db
            .query_traceql(
                &namespace,
                0,
                10,
                "{}",
                QueryOptions {
                    limit: 1,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_spans[0].name, "first");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn traceql_limit_stops_loading_without_changing_results() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        // Start order is the reverse of ID order, spread over several pages.
        for chunk in (1..=30u8).collect::<Vec<_>>().chunks(10) {
            db.write(
                &namespace,
                vec![TraceBatch::new(
                    chunk
                        .iter()
                        .map(|&id| trace(id, u64::from(31 - id) * 10, "span", vec![], vec![]))
                        .collect(),
                )],
            )
            .await
            .unwrap();
        }
        // A later continuation moves trace 1 from last to first.
        let mut early = trace(1, 5, "early", vec![], vec![]);
        early.resource_spans[0].scope_spans[0].spans[0].span_id = vec![0xee; 8];
        db.write(&namespace, vec![TraceBatch::new(vec![early])])
            .await
            .unwrap();
        let query = |options| db.query_traceql(&namespace, 0, 1_000, "{}", options);
        let key = |results: Vec<TraceQlResult>| {
            results
                .into_iter()
                .map(|result| (result.start_ns, result.trace_id))
                .collect::<Vec<_>>()
        };
        let all = key(query(QueryOptions::default()).await.unwrap());
        assert_eq!(all.len(), 30);
        assert_eq!(all[0], (5, TraceId::new([1; 16]).unwrap()));
        assert!(all.is_sorted());
        for limit in [1, 3, 8, 29, 30] {
            let limited = key(query(QueryOptions {
                limit,
                max_concurrency: 2,
                ..QueryOptions::default()
            })
            .await
            .unwrap());
            assert_eq!(limited, all[..limit], "limit {limit}");
        }
        // Only the first batch is loaded, so a cap below the match count holds.
        let limited = query(QueryOptions {
            limit: 3,
            max_candidate_traces: 10,
            ..QueryOptions::default()
        })
        .await
        .unwrap();
        assert_eq!(key(limited), all[..3]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn traceql_candidate_limit_is_explicit() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![
                trace(1, 1, "one", Vec::new(), Vec::new()),
                trace(2, 2, "two", Vec::new(), Vec::new()),
            ])],
        )
        .await
        .unwrap();
        let error = db
            .query_traceql(
                &namespace,
                0,
                10,
                "{}",
                QueryOptions {
                    max_candidate_traces: 1,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::TraceQl(crate::traceql::QueryError::Limit(_))
        ));
        db.close().await.unwrap();
    }
}
