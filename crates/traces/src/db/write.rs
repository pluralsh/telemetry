// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: segment grouping, the write-coordinator delta and page appends.

use super::*;

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

pub(super) struct TraceFlusher {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) page_config: PageConfig,
    pub(super) retention: Option<Duration>,
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
        metrics::histogram!(TRACES_FLUSH_PAGES).record(pages as f64);
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
        ops.push(RecordOp::put_with_ttl(
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
    let (Some(min_timestamp_ns), Some(max_timestamp_ns)) = (
        directory.iter().map(|entry| entry.min_timestamp_ns).min(),
        directory.iter().map(|entry| entry.max_timestamp_ns).max(),
    ) else {
        return Err(Error::Invalid(
            "cannot write an empty trace page".to_owned(),
        ));
    };
    let metadata = StoredPageMetadata {
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        traces: directory
            .iter()
            .map(|entry| PageTrace {
                trace_id: entry.trace_id,
                min_timestamp_ns: entry.min_timestamp_ns,
                max_timestamp_ns: entry.max_timestamp_ns,
            })
            .collect(),
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
        ops.push(RecordOp::put_with_ttl(
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
