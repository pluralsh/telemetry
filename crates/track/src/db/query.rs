// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: candidate selection, trace loading, catalog listing and TraceQL.

use super::*;

impl TraceDb {
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
        let now_ns = common::time::now_ns().unsigned_abs();
        segment_for(now_ns, self.segment_ns) == segment
    }

    /// Finds live data partitions from the compact namespace partition
    /// catalog, without trace-locator fan-out.
    pub(super) async fn catalog_segments(
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
}

impl TraceDb {
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
    validate_options(&options)?;
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
    let per_shard = futures::future::try_join_all(shards.iter().map(|database| async {
        let _permit = acquire(permits).await?;
        database
            .ordered_candidates(namespace, start_ns, end_ns, &plan.pushdown, now)
            .await
    }))
    .await?;
    let ordered = group_candidates(per_shard);

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
        let traces = load_batch(shards, permits, namespace, batch)
            .await?
            .into_iter()
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

fn validate_options(options: &QueryOptions) -> Result<()> {
    for (name, value) in [
        ("max_candidate_traces", options.max_candidate_traces),
        ("max_spans_per_trace", options.max_spans_per_trace),
        ("max_concurrency", options.max_concurrency),
    ] {
        if value == 0 {
            return Err(crate::traceql::QueryError::Limit(format!(
                "{name} must be greater than zero"
            ))
            .into());
        }
    }
    Ok(())
}

async fn acquire(
    permits: Option<&tokio::sync::Semaphore>,
) -> Result<Option<tokio::sync::SemaphorePermit<'_>>> {
    let Some(permits) = permits else {
        return Ok(None);
    };
    permits
        .acquire()
        .await
        .map(Some)
        .map_err(|_| Error::Invalid("shard I/O limiter is closed".to_owned()))
}

/// Merges per-shard candidates by trace ID into global result order
/// `(start, trace ID)`.
fn group_candidates(per_shard: Vec<Vec<Located>>) -> Vec<CandidateGroup> {
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
    ordered
}

/// Loads one batch of candidate groups from their shards, merging traces
/// split across shards, in the batch's order.
async fn load_batch(
    shards: &[&TraceDb],
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    batch: Vec<CandidateGroup>,
) -> Result<Vec<Trace>> {
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
                let _permit = acquire(permits).await?;
                shards[shard].load_candidates(namespace, located).await
            }),
    )
    .await?;
    let mut merged = merge_traces(parts.into_iter().flatten().collect())
        .into_iter()
        .map(|trace| (trace.trace_id, trace))
        .collect::<HashMap<_, _>>();
    Ok(order
        .into_iter()
        .filter_map(|trace_id| merged.remove(&trace_id))
        .collect())
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
    let mut resource_spans = Vec::new();
    for continuation in continuations {
        for mut resource in continuation.resource_spans {
            let resource_context = resource_context_bytes(&resource);
            resource.scope_spans.retain_mut(|scope| {
                let scope_context = scope_context_bytes(scope);
                scope.spans.retain(|span| {
                    span_id_counts
                        .get(&span.span_id)
                        .is_none_or(|count| *count <= 1)
                        || seen.insert(span_fingerprint(&resource_context, &scope_context, span))
                });
                !scope.spans.is_empty()
            });
            if !resource.scope_spans.is_empty() {
                resource_spans.push(resource);
            }
        }
    }
    Trace::new(trace_id, resource_spans)
}

fn span_fingerprint(resource_context: &[u8], scope_context: &[u8], span: &Span) -> [u8; 32] {
    let encoded = span.encode_to_vec();
    let mut fingerprint = blake3::Hasher::new();
    for part in [resource_context, scope_context, &encoded] {
        fingerprint.update(&(part.len() as u64).to_le_bytes());
        fingerprint.update(part);
    }
    *fingerprint.finalize().as_bytes()
}

fn resource_context_bytes(resource_spans: &ResourceSpans) -> Vec<u8> {
    let mut bytes = resource_spans
        .resource
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    bytes.extend_from_slice(resource_spans.schema_url.as_bytes());
    bytes
}

fn scope_context_bytes(scope_spans: &ScopeSpans) -> Vec<u8> {
    let mut bytes = scope_spans
        .scope
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    bytes.extend_from_slice(scope_spans.schema_url.as_bytes());
    bytes
}
