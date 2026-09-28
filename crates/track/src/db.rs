// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::storage::{
    PutOptions, PutRecordOp, Record, RecordOp, Storage, StorageRead, Ttl, WriteOptions,
};
use common::{StorageBuilder, StorageReaderRuntime, StorageSemantics, create_storage_read};
use futures::{StreamExt, TryStreamExt, stream};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans},
};
use prost::Message;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;

use crate::codec::{
    PageRef, PageTrace, StoredPageMetadata, TraceLocator, decode_indices, decode_locator,
    decode_locator_trace_id, decode_metadata, decode_posting_sequence, decode_sequence,
    encode_indices, encode_locator, encode_metadata, encode_sequence, locator_key, locator_range,
    locator_slot_range, metadata_key, metadata_range, next_sequence_key, payload_key, posting_key,
    posting_range, segment_for,
};

/// Concurrent storage reads per query stage.
const READ_CONCURRENCY: usize = 32;
/// Traces materialized per batch, bounding how many pages are held at once.
const MATERIALIZE_BATCH: usize = 256;
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Config, Error, Namespace, Page, PageBuilder,
    QueryOptions, Result, SegmentId, Trace, TraceBatch, TraceId, TraceQlResult,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    pub traces: usize,
    pub pages: usize,
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
    config: Config,
    segment_ns: u64,
    owned_slots: Range<u16>,
    write_lock: Mutex<()>,
}

impl TraceDb {
    pub async fn open(config: Config) -> Result<Self> {
        Self::open_with_slots(config, 0..sharding::ROUTING_SLOT_COUNT).await
    }

    pub(crate) async fn open_with_slots(config: Config, owned_slots: Range<u16>) -> Result<Self> {
        config.validate()?;
        if owned_slots.start >= owned_slots.end || owned_slots.end > sharding::ROUTING_SLOT_COUNT {
            return Err(Error::Invalid(format!(
                "invalid owned routing slot range {owned_slots:?}"
            )));
        }
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
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            config,
            segment_ns,
            owned_slots,
            write_lock: Mutex::new(()),
        })
    }

    pub(crate) async fn open_reader_with_slots(
        config: Config,
        owned_slots: Range<u16>,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
        if owned_slots.start >= owned_slots.end || owned_slots.end > sharding::ROUTING_SLOT_COUNT {
            return Err(Error::Invalid(format!(
                "invalid owned routing slot range {owned_slots:?}"
            )));
        }
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
            config,
            segment_ns,
            owned_slots,
            write_lock: Mutex::new(()),
        })
    }

    fn writer(&self) -> Result<&dyn Storage> {
        self.writer
            .as_deref()
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
        let mut groups: BTreeMap<(SegmentId, u16), Vec<Trace>> = BTreeMap::new();
        let mut report = WriteReport::default();
        for batch in batches {
            for trace in batch.traces {
                let (min_timestamp_ns, _) = trace.timestamp_range();
                let segment = segment_for(min_timestamp_ns, self.segment_ns);
                let slot = crate::routing::routing_slot(namespace, trace.trace_id);
                if !self.owned_slots.contains(&slot) {
                    return Err(Error::Invalid(format!(
                        "routing slot {slot} is outside opened shard range {:?}",
                        self.owned_slots
                    )));
                }
                report.traces += 1;
                report.spans += trace.spans().count();
                groups.entry((segment, slot)).or_default().push(trace);
            }
        }
        if groups.is_empty() {
            return Ok(report);
        }
        for traces in groups.values_mut() {
            traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        }

        let _guard = self.write_lock.lock().await;
        let retention = Retention {
            physical_ttl: self.ttl()?,
            expires_at_unix_ms: self.logical_expiry()?,
        };
        let mut ops = Vec::new();
        for ((segment, slot), traces) in groups {
            let sequence_key = next_sequence_key(namespace, segment, slot);
            let mut sequence = self
                .storage
                .get(sequence_key.clone())
                .await?
                .map(|record| decode_sequence(&record.value))
                .transpose()?
                .unwrap_or(0);
            let mut builder = PageBuilder::new(self.config.page.clone())?;
            let mut cut_page = |(page, traces): (Page, Vec<Trace>)| -> Result<()> {
                append_page_ops(
                    &mut ops,
                    namespace,
                    PageWriteId {
                        segment,
                        slot,
                        sequence,
                    },
                    &page,
                    &traces,
                    retention,
                )?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("page sequence exhausted".to_owned()))?;
                report.pages += 1;
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
        self.writer()?
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: durability == Durability::Durable,
                },
            )
            .await?;
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
        let ordered = self
            .ordered_candidates(namespace, start_ns, end_ns, matchers, now)
            .await?;
        let mut results = Vec::with_capacity(ordered.len());
        for batch in ordered.chunks(MATERIALIZE_BATCH) {
            results.extend(
                self.load_verified(namespace, batch.to_vec(), matchers)
                    .await?,
            );
        }
        Ok(results)
    }

    /// Candidates overlapping `[start_ns, end_ns]`, in result order
    /// `(start, trace ID)`. Exact trace bounds come from locators and page
    /// metadata, so callers can stop loading payloads once they have enough.
    async fn ordered_candidates(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
        now: u64,
    ) -> Result<Vec<Located>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let candidates = self
            .candidate_ids(namespace, start_ns, end_ns, matchers, now)
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

    /// Loads located candidates in order, keeping those whose decoded spans
    /// satisfy every matcher (postings only nominate candidates).
    async fn load_verified(
        &self,
        namespace: &Namespace,
        located: Vec<Located>,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let mut traces = self
            .load_located(
                namespace,
                located
                    .into_iter()
                    .map(|trace| (trace.trace_id, trace.locators))
                    .collect(),
            )
            .await?;
        traces.retain(|trace| matchers.iter().all(|matcher| trace_matches(trace, matcher)));
        Ok(traces)
    }

    async fn candidate_ids(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
        now: u64,
    ) -> Result<Vec<TraceId>> {
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        Ok(if last_segment.saturating_sub(first_segment) > 4_096 {
            self.existing_locator_candidates(namespace, first_segment, last_segment, now)
                .await?
        } else if matchers.is_empty() {
            let mut candidates = Candidates::default();
            for segment in first_segment..=last_segment {
                for slot in self.owned_slots.clone() {
                    let mut metadata = self
                        .storage
                        .scan_iter(metadata_range(namespace, segment, slot))
                        .await?;
                    while let Some(record) = metadata.next().await? {
                        let page_metadata = decode_metadata(&record.value)?;
                        if page_metadata.is_expired_at(now)
                            || !page_metadata.overlaps(start_ns, end_ns)
                        {
                            continue;
                        }
                        for trace in &page_metadata.traces {
                            if trace.overlaps(start_ns, end_ns) {
                                candidates.insert(trace.trace_id);
                            }
                        }
                    }
                }
            }
            candidates.order
        } else {
            let mut candidates: Option<Candidates> = None;
            for matcher in matchers {
                let matched = self
                    .posting_candidates(
                        namespace,
                        first_segment..=last_segment,
                        matcher,
                        start_ns,
                        end_ns,
                        now,
                    )
                    .await?;
                candidates = Some(match candidates {
                    None => matched,
                    Some(mut candidates) => {
                        candidates.retain_in(&matched);
                        candidates
                    }
                });
            }
            candidates.unwrap_or_default().order
        })
    }

    /// Resolves one matcher's postings to trace IDs through page metadata,
    /// which carries the page directory, so no payload is fetched.
    async fn posting_candidates(
        &self,
        namespace: &Namespace,
        segments: std::ops::RangeInclusive<SegmentId>,
        matcher: &AttributeMatcher,
        start_ns: u64,
        end_ns: u64,
        now: u64,
    ) -> Result<Candidates> {
        let mut candidates = Candidates::default();
        for segment in segments {
            for slot in self.owned_slots.clone() {
                let mut postings = self
                    .storage
                    .scan_iter(posting_range(namespace, segment, slot, matcher))
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
                            .get(metadata_key(namespace, segment, slot, sequence))
                            .await?
                            .ok_or_else(|| {
                                Error::Corrupt(
                                    "attribute posting references missing metadata".to_owned(),
                                )
                            })?;
                        Ok::<_, Error>((decode_metadata(&record.value)?, indices))
                    })
                    .buffered(READ_CONCURRENCY);
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
                            candidates.insert(trace.trace_id);
                        }
                    }
                }
            }
        }
        Ok(candidates)
    }

    /// Candidate IDs from every live locator whose page lies in the segment
    /// range, for spans too wide to enumerate segment by segment.
    async fn existing_locator_candidates(
        &self,
        namespace: &Namespace,
        first_segment: SegmentId,
        last_segment: SegmentId,
        now: u64,
    ) -> Result<Vec<TraceId>> {
        let mut first_pages = HashMap::new();
        for slot in self.owned_slots.clone() {
            let mut records = self
                .storage
                .scan_iter(locator_slot_range(namespace, slot))
                .await?;
            while let Some(record) = records.next().await? {
                let locator = decode_locator(&record.value)?;
                if !locator.is_expired_at(now)
                    && locator.segment >= first_segment
                    && locator.segment <= last_segment
                {
                    let (key_slot, trace_id) = decode_locator_trace_id(&record.key)?;
                    first_pages.entry(trace_id).or_insert((
                        locator.segment,
                        key_slot,
                        locator.page_sequence,
                    ));
                }
            }
        }
        Ok(order_by_first_page(first_pages))
    }

    /// Enumerates up to `limit` live traces by scanning locator records that
    /// actually exist. Unlike a full-range search, this does not walk every
    /// theoretical time segment between zero and `u64::MAX`.
    pub async fn scan_traces(&self, namespace: &Namespace, limit: usize) -> Result<Vec<Trace>> {
        let scanned = self.scan_trace_ids(namespace, limit).await?;
        self.load_scanned(namespace, scanned).await
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
        for slot in self.owned_slots.clone() {
            let mut records = self
                .storage
                .scan_iter(locator_slot_range(namespace, slot))
                .await?;
            while let Some(record) = records.next().await? {
                let locator = decode_locator(&record.value)?;
                if locator.is_expired_at(now) {
                    continue;
                }
                let (key_slot, trace_id) = decode_locator_trace_id(&record.key)?;
                first_pages.entry(trace_id).or_insert((
                    locator.segment,
                    key_slot,
                    locator.page_sequence,
                ));
            }
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
    /// Safe positive scoped scalar equalities are used as index candidates;
    /// the complete query is always evaluated against decoded traces.
    pub async fn query_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
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
        // Candidates are already in result order `(start, trace ID)`, so the
        // first `limit` matches are the answer and later candidates are never
        // loaded.
        let ordered = self
            .ordered_candidates(namespace, start_ns, end_ns, &plan.pushdown, now)
            .await?;
        let mut results = Vec::new();
        let mut loaded = 0;
        let mut remaining = ordered.as_slice();
        while results.len() < options.limit && !remaining.is_empty() {
            let wanted = (options.limit - results.len())
                .max(options.max_concurrency)
                .min(MATERIALIZE_BATCH)
                .min(remaining.len());
            let (batch, rest) = remaining.split_at(wanted);
            remaining = rest;
            let traces = self
                .load_verified(namespace, batch.to_vec(), &plan.pushdown)
                .await?;
            loaded += traces.len();
            if loaded > options.max_candidate_traces {
                return Err(crate::traceql::QueryError::Limit(format!(
                    "{loaded} candidate traces exceeds maximum {}",
                    options.max_candidate_traces
                ))
                .into());
            }
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

    pub async fn flush(&self) -> Result<()> {
        if let Some(storage) = &self.writer {
            storage.flush().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
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
                let slot = crate::routing::routing_slot(namespace, trace_id);
                if !self.owned_slots.contains(&slot) {
                    return Err(Error::Invalid(format!(
                        "routing slot {slot} is outside opened shard range {:?}",
                        self.owned_slots
                    )));
                }
                let mut records = self
                    .storage
                    .scan_iter(locator_range(namespace, slot, trace_id))
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
            .flat_map(|(trace_id, locators)| {
                let slot = crate::routing::routing_slot(namespace, *trace_id);
                locators.iter().map(move |locator| locator.page(slot))
            })
            .collect();
        let metadata: HashMap<PageRef, StoredPageMetadata> = stream::iter(wanted)
            .map(|page @ (segment, slot, sequence)| async move {
                let record = self
                    .storage
                    .get(metadata_key(namespace, segment, slot, sequence))
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
                let slot = crate::routing::routing_slot(namespace, trace_id);
                let mut start_ns = u64::MAX;
                let mut end_ns = 0;
                for locator in &locators {
                    let trace = metadata[&locator.page(slot)]
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
            .flat_map(|(trace_id, locators)| {
                let slot = crate::routing::routing_slot(namespace, *trace_id);
                locators.iter().map(move |locator| locator.page(slot))
            })
            .collect();
        let pages: HashMap<PageRef, Page> = stream::iter(wanted)
            .map(|page @ (segment, slot, sequence)| async move {
                let payload = self
                    .storage
                    .get(payload_key(namespace, segment, slot, sequence))
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
            let slot = crate::routing::routing_slot(namespace, trace_id);
            let continuations = locators
                .iter()
                .map(|locator| {
                    let trace =
                        pages[&locator.page(slot)].decode_trace(locator.trace_index as usize)?;
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

    fn ttl(&self) -> Result<Ttl> {
        self.config
            .retention
            .map(|duration| {
                u64::try_from(duration.as_millis())
                    .map(Ttl::ExpireAfter)
                    .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))
            })
            .transpose()
            .map(|ttl| ttl.unwrap_or(Ttl::NoExpiry))
    }

    fn logical_expiry(&self) -> Result<Option<u64>> {
        self.config
            .retention
            .map(|retention| {
                let retention_ms = u64::try_from(retention.as_millis())
                    .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))?;
                unix_time_ms()?
                    .checked_add(retention_ms)
                    .ok_or_else(|| Error::Invalid("retention expiry overflows u64".to_owned()))
            })
            .transpose()
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
struct Located {
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
    slot: u16,
    sequence: u64,
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    id: PageWriteId,
    page: &Page,
    traces: &[Trace],
    retention: Retention,
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
        metadata_key(namespace, id.segment, id.slot, id.sequence),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, id.segment, id.slot, id.sequence),
        page.bytes(),
        retention.physical_ttl,
    ));

    // Keyed by the encoded posting key, which already identifies matchers
    // exactly (scope, name, and typed value, with doubles compared by bits).
    let mut postings: HashMap<Bytes, Vec<u32>> = HashMap::new();
    for (index, (entry, trace)) in page.directory().iter().zip(traces).enumerate() {
        debug_assert_eq!(entry.trace_id, trace.trace_id);
        ops.push(put(
            locator_key(namespace, id.slot, trace.trace_id, id.segment, id.sequence),
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
            postings
                .entry(posting_key(
                    namespace,
                    id.segment,
                    id.slot,
                    &matcher,
                    id.sequence,
                ))
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

fn merge_continuations(trace_id: TraceId, continuations: Vec<Trace>) -> Result<Trace> {
    let mut seen = HashSet::new();
    let mut resource_spans = Vec::new();
    for continuation in continuations {
        for mut resource in continuation.resource_spans {
            let resource_context = resource_context_bytes(&resource);
            let mut retained_scopes = Vec::new();
            for mut scope in std::mem::take(&mut resource.scope_spans) {
                let scope_context = scope_context_bytes(&scope);
                let mut retained_spans = Vec::new();
                for span in std::mem::take(&mut scope.spans) {
                    let mut fingerprint = resource_context.clone();
                    fingerprint.extend_from_slice(&scope_context);
                    span.encode(&mut fingerprint).unwrap();
                    if seen.insert(fingerprint) {
                        retained_spans.push(span);
                    }
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
    async fn stores_traces_in_slot_local_pages_and_isolates_namespaces() {
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
        let expected_pages = [
            crate::routing::routing_slot(&tenant_a, first.trace_id),
            crate::routing::routing_slot(&tenant_a, second.trace_id),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .len();
        assert_eq!(report.pages, expected_pages);
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
    async fn page_sequences_are_local_to_each_segment_slot() {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("slot-local").unwrap();
        let first = trace(1, 1, "first", Vec::new(), Vec::new());
        let first_slot = crate::routing::routing_slot(&namespace, first.trace_id);
        let second = (2..=u8::MAX)
            .map(|id| trace(id, 2, "second", Vec::new(), Vec::new()))
            .find(|trace| crate::routing::routing_slot(&namespace, trace.trace_id) != first_slot)
            .unwrap();
        let second_slot = crate::routing::routing_slot(&namespace, second.trace_id);

        db.write(
            &namespace,
            vec![TraceBatch::new(vec![first.clone(), second.clone()])],
        )
        .await
        .unwrap();

        for slot in [first_slot, second_slot] {
            let mut records = db
                .storage
                .scan_iter(metadata_range(&namespace, 0, slot))
                .await
                .unwrap();
            let record = records.next().await.unwrap().unwrap();
            assert_eq!(
                crate::codec::decode_metadata_sequence(&record.key).unwrap(),
                0
            );
            assert!(records.next().await.unwrap().is_none());
        }
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
    async fn opened_slot_range_rejects_non_authoritative_access() {
        let namespace = Namespace::new("owned-slots").unwrap();
        let trace = trace(1, 1, "outside", Vec::new(), Vec::new());
        let slot = crate::routing::routing_slot(&namespace, trace.trace_id);
        let owned = if slot == 0 { 1..2 } else { 0..1 };
        let db = TraceDb::open_with_slots(test_config(), owned)
            .await
            .unwrap();

        let write_error = db
            .write(&namespace, vec![TraceBatch::new(vec![trace.clone()])])
            .await
            .unwrap_err();
        assert!(
            write_error
                .to_string()
                .contains("outside opened shard range")
        );
        let read_error = db.get_trace(&namespace, trace.trace_id).await.unwrap_err();
        assert!(
            read_error
                .to_string()
                .contains("outside opened shard range")
        );
        db.close().await.unwrap();
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
        assert_eq!(report.pages, 2);
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
