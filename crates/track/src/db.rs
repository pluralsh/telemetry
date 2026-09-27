// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::storage::{PutOptions, PutRecordOp, Record, RecordOp, Storage, Ttl, WriteOptions};
use common::{StorageBuilder, StorageSemantics};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans},
};
use prost::Message;
use tokio::sync::Mutex;

use crate::codec::{
    METADATA_VERSION, StoredPageMetadata, TraceLocator, decode_indices, decode_locator,
    decode_locator_trace_id, decode_metadata, decode_metadata_sequence, decode_posting_sequence,
    decode_sequence, encode_indices, encode_locator, encode_metadata, encode_sequence, locator_key,
    locator_namespace_range, locator_range, metadata_key, metadata_range, next_sequence_key,
    payload_key, posting_key, posting_range, segment_for,
};
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Config, Error, Namespace, Page, PageBuilder,
    QueryOptions, Result, SegmentId, Trace, TraceBatch, TraceId, TraceQlResult,
    TraceSegmentExtractor,
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
    storage: Arc<dyn Storage>,
    config: Config,
    segment_ns: u64,
    write_lock: Mutex<()>,
}

impl TraceDb {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
        let semantics =
            StorageSemantics::new().with_segment_extractor(TraceSegmentExtractor::shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        Ok(Self {
            storage,
            config,
            segment_ns,
            write_lock: Mutex::new(()),
        })
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

        let _guard = self.write_lock.lock().await;
        let retention = Retention {
            physical_ttl: self.ttl()?,
            expires_at_unix_ms: self.logical_expiry()?,
        };
        let mut ops = Vec::new();
        for (segment, traces) in groups {
            let sequence_key = next_sequence_key(namespace, segment);
            let mut sequence = self
                .storage
                .get(sequence_key.clone())
                .await?
                .map(|record| decode_sequence(&record.value))
                .transpose()?
                .unwrap_or(0);
            let mut builder = PageBuilder::new(self.config.page.clone())?;
            for trace in traces {
                if let Some(page) = builder.append(trace)? {
                    append_page_ops(&mut ops, namespace, segment, sequence, page, retention)?;
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("page sequence exhausted".to_owned()))?;
                    report.pages += 1;
                }
            }
            if let Some(page) = builder.finish()? {
                append_page_ops(&mut ops, namespace, segment, sequence, page, retention)?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("page sequence exhausted".to_owned()))?;
                report.pages += 1;
            }
            ops.push(put(
                sequence_key,
                encode_sequence(sequence),
                retention.physical_ttl,
            ));
        }
        self.storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: durability == Durability::Durable,
                },
            )
            .await?;
        if durability == Durability::Written {
            self.storage.flush().await?;
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
        let mut locators = self
            .storage
            .scan_iter(locator_range(namespace, trace_id))
            .await?;
        let mut continuations = Vec::new();
        while let Some(record) = locators.next().await? {
            let locator = decode_locator(&record.value)?;
            if locator.is_expired_at(now) {
                continue;
            }
            let Some(metadata_record) = self
                .storage
                .get(metadata_key(
                    namespace,
                    locator.segment,
                    locator.page_sequence,
                ))
                .await?
            else {
                return Err(Error::Corrupt(
                    "trace locator references missing page metadata".to_owned(),
                ));
            };
            let metadata = decode_metadata(&metadata_record.value)?;
            if metadata.is_expired_at(now) {
                continue;
            }
            let payload = self
                .storage
                .get(payload_key(
                    namespace,
                    locator.segment,
                    locator.page_sequence,
                ))
                .await?
                .ok_or_else(|| Error::Corrupt("trace page metadata has no payload".to_owned()))?;
            let page = decode_stored_page(&metadata, payload.value)?;
            let trace = page.decode_trace(locator.trace_index as usize)?;
            if trace.trace_id != trace_id {
                return Err(Error::Corrupt(
                    "trace locator points to a different trace".to_owned(),
                ));
            }
            continuations.push(trace);
        }
        if continuations.is_empty() {
            return Ok(None);
        }
        Ok(Some(merge_continuations(trace_id, continuations)?))
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
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        if last_segment.saturating_sub(first_segment) > 4_096 {
            return self
                .search_existing_locators(
                    namespace,
                    first_segment,
                    last_segment,
                    start_ns,
                    end_ns,
                    matchers,
                )
                .await;
        }
        let now = unix_time_ms()?;
        let mut candidate_ids: Option<BTreeSet<TraceId>> = None;

        if matchers.is_empty() {
            let mut ids = BTreeSet::new();
            for segment in first_segment..=last_segment {
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let page_metadata = decode_metadata(&record.value)?;
                    if page_metadata.is_expired_at(now)
                        || page_metadata.max_timestamp_ns < start_ns
                        || page_metadata.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    let sequence = decode_metadata_sequence(&record.key)?;
                    let page = self.load_page(namespace, segment, sequence).await?;
                    for entry in page.directory() {
                        if entry.max_timestamp_ns >= start_ns && entry.min_timestamp_ns <= end_ns {
                            ids.insert(entry.trace_id);
                        }
                    }
                }
            }
            candidate_ids = Some(ids);
        } else {
            for matcher in matchers {
                let mut matcher_ids = BTreeSet::new();
                for segment in first_segment..=last_segment {
                    let mut postings = self
                        .storage
                        .scan_iter(posting_range(namespace, segment, matcher))
                        .await?;
                    while let Some(record) = postings.next().await? {
                        let sequence = decode_posting_sequence(&record.key)?;
                        let Some(metadata_record) = self
                            .storage
                            .get(metadata_key(namespace, segment, sequence))
                            .await?
                        else {
                            return Err(Error::Corrupt(
                                "attribute posting references missing metadata".to_owned(),
                            ));
                        };
                        let metadata = decode_metadata(&metadata_record.value)?;
                        if metadata.is_expired_at(now)
                            || metadata.max_timestamp_ns < start_ns
                            || metadata.min_timestamp_ns > end_ns
                        {
                            continue;
                        }
                        let page = self.load_page(namespace, segment, sequence).await?;
                        for index in decode_indices(&record.value)? {
                            let entry = page.directory().get(index as usize).ok_or_else(|| {
                                Error::Corrupt(
                                    "attribute posting trace index is out of bounds".to_owned(),
                                )
                            })?;
                            if entry.max_timestamp_ns >= start_ns
                                && entry.min_timestamp_ns <= end_ns
                            {
                                matcher_ids.insert(entry.trace_id);
                            }
                        }
                    }
                }
                candidate_ids = Some(match candidate_ids {
                    None => matcher_ids,
                    Some(ids) => ids.intersection(&matcher_ids).copied().collect(),
                });
            }
        }

        let mut results = Vec::new();
        for trace_id in candidate_ids.unwrap_or_default() {
            let Some(trace) = self.get_trace(namespace, trace_id).await? else {
                continue;
            };
            let (min, max) = trace.timestamp_range();
            if max >= start_ns
                && min <= end_ns
                && matchers
                    .iter()
                    .all(|matcher| trace_matches(&trace, matcher))
            {
                results.push(trace);
            }
        }
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    async fn search_existing_locators(
        &self,
        namespace: &Namespace,
        first_segment: SegmentId,
        last_segment: SegmentId,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        let mut records = self
            .storage
            .scan_iter(locator_namespace_range(namespace))
            .await?;
        let mut ids = BTreeSet::new();
        while let Some(record) = records.next().await? {
            let locator = decode_locator(&record.value)?;
            if !locator.is_expired_at(now)
                && locator.segment >= first_segment
                && locator.segment <= last_segment
            {
                ids.insert(decode_locator_trace_id(&record.key)?);
            }
        }

        let mut results = Vec::new();
        for trace_id in ids {
            let Some(trace) = self.get_trace(namespace, trace_id).await? else {
                continue;
            };
            let (min, max) = trace.timestamp_range();
            if max >= start_ns
                && min <= end_ns
                && matchers
                    .iter()
                    .all(|matcher| trace_matches(&trace, matcher))
            {
                results.push(trace);
            }
        }
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    /// Enumerates up to `limit` live traces by scanning locator records that
    /// actually exist. Unlike a full-range search, this does not walk every
    /// theoretical time segment between zero and `u64::MAX`.
    pub async fn scan_traces(&self, namespace: &Namespace, limit: usize) -> Result<Vec<Trace>> {
        if limit == 0 {
            return Err(Error::Invalid(
                "trace scan limit must be greater than zero".to_owned(),
            ));
        }
        let now = unix_time_ms()?;
        let mut records = self
            .storage
            .scan_iter(locator_namespace_range(namespace))
            .await?;
        let mut ids = BTreeSet::new();
        while ids.len() < limit {
            let Some(record) = records.next().await? else {
                break;
            };
            if decode_locator(&record.value)?.is_expired_at(now) {
                continue;
            }
            ids.insert(decode_locator_trace_id(&record.key)?);
        }
        let mut traces = Vec::with_capacity(ids.len());
        for trace_id in ids {
            if let Some(trace) = self.get_trace(namespace, trace_id).await? {
                traces.push(trace);
            }
        }
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
        let candidates = self
            .search(namespace, start_ns, end_ns, &plan.pushdown)
            .await?;
        if candidates.len() > options.max_candidate_traces {
            return Err(crate::traceql::QueryError::Limit(format!(
                "{} candidate traces exceeds maximum {}",
                candidates.len(),
                options.max_candidate_traces
            ))
            .into());
        }
        let mut results = Vec::new();
        let mut tasks = tokio::task::JoinSet::new();
        for trace in candidates {
            while tasks.len() >= options.max_concurrency {
                let result = tasks
                    .join_next()
                    .await
                    .expect("query task set is non-empty")
                    .map_err(|error| Error::Invalid(format!("TraceQL task failed: {error}")))??;
                if let Some(result) = result {
                    results.push(result);
                }
            }
            let query = plan.query.clone();
            let max_spans = options.max_spans_per_trace;
            tasks.spawn(async move { crate::traceql::execute(&trace, &query, max_spans) });
        }
        while let Some(result) = tasks.join_next().await {
            let result = result
                .map_err(|error| Error::Invalid(format!("TraceQL task failed: {error}")))??;
            if let Some(result) = result {
                results.push(result);
            }
        }
        results.sort_by_key(|result| (result.start_ns, result.trace_id));
        results.truncate(options.limit);
        Ok(results)
    }

    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.storage.close().await?;
        Ok(())
    }

    async fn load_page(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        sequence: u64,
    ) -> Result<Page> {
        let metadata = self
            .storage
            .get(metadata_key(namespace, segment, sequence))
            .await?
            .ok_or_else(|| Error::Corrupt("trace page metadata is missing".to_owned()))?;
        let metadata = decode_metadata(&metadata.value)?;
        let payload = self
            .storage
            .get(payload_key(namespace, segment, sequence))
            .await?
            .ok_or_else(|| Error::Corrupt("trace page payload is missing".to_owned()))?;
        decode_stored_page(&metadata, payload.value)
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

fn decode_stored_page(metadata: &StoredPageMetadata, payload: Bytes) -> Result<Page> {
    if payload.len() != metadata.payload_bytes as usize {
        return Err(Error::Corrupt(
            "trace page payload length disagrees with metadata".to_owned(),
        ));
    }
    let page = Page::decode(payload)?;
    if page.directory().len() != metadata.trace_count as usize
        || page
            .directory()
            .iter()
            .map(|entry| entry.min_timestamp_ns)
            .min()
            != Some(metadata.min_timestamp_ns)
        || page
            .directory()
            .iter()
            .map(|entry| entry.max_timestamp_ns)
            .max()
            != Some(metadata.max_timestamp_ns)
    {
        return Err(Error::Corrupt(
            "trace page directory disagrees with metadata".to_owned(),
        ));
    }
    Ok(page)
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    segment: SegmentId,
    sequence: u64,
    page: Page,
    retention: Retention,
) -> Result<()> {
    let bytes = page.bytes();
    let min_timestamp_ns = page
        .directory()
        .iter()
        .map(|entry| entry.min_timestamp_ns)
        .min()
        .unwrap();
    let max_timestamp_ns = page
        .directory()
        .iter()
        .map(|entry| entry.max_timestamp_ns)
        .max()
        .unwrap();
    let metadata = StoredPageMetadata {
        version: METADATA_VERSION,
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        trace_count: u32::try_from(page.directory().len())
            .map_err(|_| Error::Invalid("page trace count exceeds u32".to_owned()))?,
        payload_bytes: u32::try_from(bytes.len())
            .map_err(|_| Error::Invalid("page payload exceeds u32".to_owned()))?,
    };
    ops.push(put(
        metadata_key(namespace, segment, sequence),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, segment, sequence),
        bytes,
        retention.physical_ttl,
    ));

    let mut postings: Vec<(AttributeMatcher, Vec<u32>)> = Vec::new();
    for (index, entry) in page.directory().iter().enumerate() {
        let trace = page.decode_trace(index)?;
        ops.push(put(
            locator_key(namespace, trace.trace_id, segment, sequence),
            encode_locator(&TraceLocator {
                version: METADATA_VERSION,
                segment,
                page_sequence: sequence,
                trace_index: u32::try_from(index)
                    .map_err(|_| Error::Invalid("trace index exceeds u32".to_owned()))?,
                expires_at_unix_ms: retention.expires_at_unix_ms,
            })?,
            retention.physical_ttl,
        ));
        let mut seen = Vec::new();
        collect_trace_attributes(&trace, &mut seen);
        for matcher in seen {
            if let Some((_, indices)) = postings.iter_mut().find(|(existing, _)| {
                existing.scope == matcher.scope
                    && existing.name == matcher.name
                    && existing.value.exact_eq(&matcher.value)
            }) {
                indices.push(index as u32);
            } else {
                postings.push((matcher, vec![index as u32]));
            }
        }
        debug_assert_eq!(entry.trace_id, trace.trace_id);
    }
    for (matcher, mut indices) in postings {
        indices.sort_unstable();
        indices.dedup();
        ops.push(put(
            posting_key(namespace, segment, &matcher, sequence),
            encode_indices(&indices)?,
            retention.physical_ttl,
        ));
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
    async fn stores_multiple_traces_per_page_and_isolates_namespaces() {
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
        assert_eq!(report.pages, 1);
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
