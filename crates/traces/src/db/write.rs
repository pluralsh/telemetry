// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: segment grouping, the write-coordinator delta and page appends.

use std::collections::hash_map::Entry;
use std::time::Instant;

use super::*;

/// How long a segment's next page sequence stays in memory after its last
/// write. A later write reads it again.
const SEQUENCE_IDLE_EVICTION: Duration = Duration::from_secs(15 * 60);

pub(super) struct TraceWrite {
    pub(super) namespace: Namespace,
    pub(super) groups: BTreeMap<SegmentId, Vec<Trace>>,
    pub(super) report: WriteReport,
}

type TraceGroup = (Namespace, SegmentId);

#[derive(Default)]
pub(super) struct TraceWriteDelta {
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
    type Snapshot = ();

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

/// Writes flushed traces without reading what storage already holds.
///
/// The flusher is the database's only writer, so the page sequences it
/// allocates are authoritative in memory; a segment's is read once, the first
/// time this process writes to it. Trace heads are merge operands describing
/// only this flush's pages, so a trace's earlier pages are never looked up.
pub(super) struct TraceFlusher {
    storage: Arc<dyn Storage>,
    page_config: PageConfig,
    retention: Option<Duration>,
    segment_ns: u64,
    read_cache: Arc<ReadCache>,
    sequences: HashMap<TraceGroup, NextSequence>,
}

struct NextSequence {
    next: u64,
    touched: Instant,
}

/// This flush's head operand for each trace it writes, per namespace.
type FlushHeads = HashMap<Namespace, HashMap<TraceId, TraceHead>>;

#[async_trait]
impl Flusher<TraceWriteDelta> for TraceFlusher {
    async fn flush_delta(
        &mut self,
        frozen: BTreeMap<TraceGroup, Vec<Trace>>,
        _epoch_range: &Range<u64>,
    ) -> std::result::Result<(), String> {
        let (pages, written) = self
            .direct_write(frozen)
            .await
            .map_err(|error| error.to_string())?;
        self.read_cache.invalidate_traces(written).await;
        metrics::histogram!(TRACES_FLUSH_PAGES).record(pages as f64);
        Ok(())
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

impl TraceFlusher {
    pub(super) fn new(
        storage: Arc<dyn Storage>,
        page_config: PageConfig,
        retention: Option<Duration>,
        segment_ns: u64,
        read_cache: Arc<ReadCache>,
    ) -> Self {
        Self {
            storage,
            page_config,
            retention,
            segment_ns,
            read_cache,
            sequences: HashMap::new(),
        }
    }

    /// Reads the next page sequence of every segment in `groups` this
    /// process has not written since it last evicted it.
    async fn load_sequences(&mut self, groups: &BTreeMap<TraceGroup, Vec<Trace>>) -> Result<()> {
        let storage = self.storage.as_ref();
        let missing = groups
            .keys()
            .filter(|group| !self.sequences.contains_key(*group))
            .cloned()
            .collect::<Vec<_>>();
        let loaded = stream::iter(missing)
            .map(|group| async move {
                let next = storage
                    .get(next_sequence_key(&group.0, group.1))
                    .await?
                    .map(|record| decode_sequence(&record.value))
                    .transpose()?
                    .unwrap_or(0);
                Ok::<_, Error>((group, next))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let touched = Instant::now();
        self.sequences.extend(
            loaded
                .into_iter()
                .map(|(group, next)| (group, NextSequence { next, touched })),
        );
        Ok(())
    }

    /// Writes every group atomically, returning the pages written and every
    /// trace they hold. A failed write ends the flusher, so the sequences it
    /// advanced are never reused.
    async fn direct_write(
        &mut self,
        groups: BTreeMap<TraceGroup, Vec<Trace>>,
    ) -> Result<(usize, Vec<(Namespace, TraceId)>)> {
        if groups.is_empty() {
            return Ok((0, Vec::new()));
        }
        let retention = retention_values(self.retention)?;
        self.load_sequences(&groups).await?;
        let mut heads = FlushHeads::new();
        let mut ops = Vec::new();
        let mut catalogs = BTreeMap::<(Namespace, SegmentId), CatalogBatch>::new();
        let mut partition_catalogs = BTreeMap::<Namespace, CatalogBatch>::new();
        let mut pages = 0usize;
        for ((namespace, segment), traces) in groups {
            let state = self
                .sequences
                .get_mut(&(namespace.clone(), segment))
                .expect("load_sequences loads every written segment");
            state.touched = Instant::now();
            pages = pages.saturating_add(append_segment(
                &mut ops,
                (&namespace, segment),
                traces,
                &mut state.next,
                (&self.page_config, self.segment_ns, retention),
                (
                    partition_catalogs.entry(namespace.clone()).or_default(),
                    catalogs.entry((namespace.clone(), segment)).or_default(),
                    heads.entry(namespace.clone()).or_default(),
                ),
            )?);
        }
        for ((namespace, segment), catalog) in catalogs {
            ops.extend(
                catalog.into_ops(&segment_prefix(&namespace, segment), retention.physical_ttl),
            );
        }
        for (namespace, catalog) in partition_catalogs {
            ops.extend(catalog.into_ops(
                &segment_prefix(&namespace, LOCATOR_SEGMENT),
                retention.physical_ttl,
            ));
        }
        let mut written = Vec::new();
        for (namespace, heads) in heads {
            for (trace_id, head) in heads {
                ops.push(RecordOp::merge_with_ttl(
                    head_key(&namespace, trace_id),
                    encode_head(&head)?,
                    retention.physical_ttl,
                ));
                written.push((namespace.clone(), trace_id));
            }
        }
        self.storage.apply(ops).await?;
        self.sequences
            .retain(|_, state| state.touched.elapsed() < SEQUENCE_IDLE_EVICTION);
        Ok((pages, written))
    }
}

/// Appends one segment's pages, numbered from `sequence`, which it advances,
/// and returns how many it wrote.
fn append_segment(
    ops: &mut Vec<RecordOp>,
    (namespace, segment): (&Namespace, SegmentId),
    traces: Vec<Trace>,
    sequence: &mut u64,
    (page_config, segment_ns, retention): (&PageConfig, u64, Retention),
    (partition_catalog, catalog, heads): (
        &mut CatalogBatch,
        &mut CatalogBatch,
        &mut HashMap<TraceId, TraceHead>,
    ),
) -> Result<usize> {
    partition_catalog.insert(
        PARTITION_SCOPE,
        PARTITION_NAME,
        DiscoveryValue::Int(segment),
    );
    let last_segment = traces
        .iter()
        .map(|trace| segment_for(trace.timestamp_range().1, segment_ns))
        .max()
        .unwrap_or(segment);
    if last_segment > segment {
        partition_catalog.insert(
            PARTITION_SCOPE,
            PARTITION_EXTENT_NAME,
            DiscoveryValue::String(format!("{segment}:{last_segment}")),
        );
    }
    let mut pages = 0usize;
    let mut builder = PageBuilder::new(page_config.clone())?;
    let mut cut_page = |(page, traces): (Page, Vec<Trace>)| -> Result<()> {
        append_page_ops(
            ops,
            namespace,
            PageWriteId {
                segment,
                sequence: *sequence,
            },
            &page,
            &traces,
            retention,
            (catalog, heads),
        )?;
        *sequence = sequence
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
    ops.push(RecordOp::put_with_ttl(
        next_sequence_key(namespace, segment),
        encode_sequence(*sequence),
        retention.physical_ttl,
    ));
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

pub(super) fn map_write_error(error: WriteError) -> Error {
    match error {
        WriteError::Backpressure(_) | WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Unavailable("write coordinator is shut down".to_owned()),
        WriteError::ApplyError(_, message) => Error::Invalid(message),
        WriteError::FlushError(message) | WriteError::Internal(message) => {
            Error::Unavailable(message)
        }
    }
}

#[derive(Clone, Copy)]
struct PageWriteId {
    segment: SegmentId,
    sequence: u64,
}

/// Writes the continuation record of `trace_id` at `here` and adds the page
/// to the trace's head operand for this flush. Returns whether an earlier
/// page of this flush holds the trace.
fn record_page(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    heads: &mut HashMap<TraceId, TraceHead>,
    trace_id: TraceId,
    here: TraceLocator,
    retention: Retention,
) -> Result<bool> {
    ops.push(RecordOp::put_with_ttl(
        continuation_key(namespace, trace_id, here.segment, here.page_sequence),
        encode_locator(&here)?,
        retention.physical_ttl,
    ));
    match heads.entry(trace_id) {
        Entry::Vacant(entry) => {
            entry.insert(TraceHead {
                first: here,
                continued: false,
                pages: 1,
            });
            Ok(false)
        }
        Entry::Occupied(entry) => {
            let head = entry.into_mut();
            head.continued = true;
            head.pages = head.pages.saturating_add(1);
            Ok(true)
        }
    }
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    id: PageWriteId,
    page: &Page,
    traces: &[Trace],
    retention: Retention,
    (catalog, heads): (&mut CatalogBatch, &mut HashMap<TraceId, TraceHead>),
) -> Result<()> {
    let directory = page.directory();
    let (Some(min_timestamp_ns), Some(max_timestamp_ns)) = (
        directory.iter().map(|entry| entry.min_timestamp_ns).min(),
        directory.iter().map(|entry| entry.max_timestamp_ns).max(),
    ) else {
        return Err(Error::Invalid(
            "cannot write an empty trace page".to_owned(),
        ));
    };
    let mut page_traces = Vec::with_capacity(directory.len());
    for (index, entry) in directory.iter().enumerate() {
        let here = TraceLocator {
            segment: id.segment,
            page_sequence: id.sequence,
            trace_index: u32::try_from(index)
                .map_err(|_| Error::Invalid("trace index exceeds u32".to_owned()))?,
            expires_at_unix_ms: retention.expires_at_unix_ms,
        };
        let continued = record_page(ops, namespace, heads, entry.trace_id, here, retention)?;
        page_traces.push(PageTrace {
            trace_id: entry.trace_id,
            min_timestamp_ns: entry.min_timestamp_ns,
            max_timestamp_ns: entry.max_timestamp_ns,
            continued,
        });
    }
    let metadata = StoredPageMetadata {
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        traces: page_traces,
    };
    ops.push(RecordOp::put_with_ttl(
        metadata_key(namespace, id.segment, id.sequence),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(RecordOp::put_with_ttl(
        payload_key(namespace, id.segment, id.sequence),
        page.bytes(),
        retention.physical_ttl,
    ));

    // Keyed by the encoded posting key, which already identifies matchers
    // exactly (scope, name, and typed value, with doubles compared by bits).
    let mut postings: HashMap<Bytes, Vec<u32>> = HashMap::new();
    for (index, (entry, trace)) in page.directory().iter().zip(traces).enumerate() {
        debug_assert_eq!(entry.trace_id, trace.trace_id);
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
        let spans = trace
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans);
        for span in spans {
            for (name, value) in span_intrinsics(span) {
                postings
                    .entry(field_posting_key(
                        namespace,
                        id.segment,
                        (IndexField::Intrinsic, name, &value),
                        id.sequence,
                    ))
                    .or_default()
                    .push(index as u32);
            }
        }
    }
    for (key, mut indices) in postings {
        indices.sort_unstable();
        indices.dedup();
        ops.push(RecordOp::put_with_ttl(
            key,
            encode_indices(&indices)?,
            retention.physical_ttl,
        ));
    }
    Ok(())
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
