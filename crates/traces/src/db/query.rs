// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: candidate selection, trace loading, catalog listing and TraceQL.

use super::*;

#[cfg(test)]
#[path = "profile.rs"]
mod profile;

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
            .map(|matcher| {
                vec![IndexPredicate {
                    field: match matcher.scope {
                        AttributeScope::Resource => IndexField::Resource,
                        AttributeScope::Span => IndexField::Span,
                    },
                    name: matcher.name.clone(),
                    test: IndexTest::Exact(matcher.value.clone()),
                }]
            })
            .collect::<Vec<_>>();
        let ordered = self
            .ordered_candidates(namespace, start_ns, end_ns, &clauses, now)
            .await?;
        let mut results = Vec::with_capacity(ordered.len());
        let memo = PageMemo::default();
        for batch in ordered.chunks(MATERIALIZE_BATCH) {
            let mut traces = self
                .load_candidates(namespace, batch.to_vec(), &memo)
                .await?;
            traces.retain(|trace| satisfies(trace, &clauses));
            results.extend(traces);
        }
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    /// Candidates overlapping `[start_ns, end_ns]`, in result order
    /// `(start, trace ID)`. A candidate seen on its only page is complete:
    /// its page and exact bounds come from page metadata with no locator
    /// read. Continued candidates are ordered by the bounds of the pages the
    /// index saw, and their other pages are located only when loaded.
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
        let Candidates { seen, unconfirmed } = self
            .candidate_ids(namespace, start_ns, end_ns, clauses, now)
            .await?;
        let unflagged: BTreeSet<SegmentId> = seen
            .values()
            .flatten()
            .filter(|seen| !seen.trace.continued)
            .map(|seen| seen.locator.segment)
            .collect();
        let markers = self.continued_markers(namespace, unflagged).await?;
        let mut located = seen
            .into_iter()
            .map(|(trace_id, seen)| Located::from_seen(trace_id, &seen, &markers))
            .filter(|located| {
                located.locators.is_none() || !unconfirmed.contains(&located.trace_id)
            })
            .collect::<Vec<_>>();
        located.sort_unstable_by_key(|trace| (trace.start_ns, trace.trace_id));
        Ok(located)
    }

    /// Pages and indices of traces continued after their first page, for
    /// each of `segments`.
    pub(super) async fn continued_markers(
        &self,
        namespace: &Namespace,
        segments: BTreeSet<SegmentId>,
    ) -> Result<HashSet<(SegmentId, u64, u32)>> {
        let per_segment = stream::iter(segments)
            .map(|segment| async move {
                let mut markers = self
                    .storage
                    .scan_prefix_iter(
                        marker_prefix(namespace, segment),
                        BytesRange::unbounded(),
                        None,
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(record) = markers.next().await? {
                    let (sequence, index) = decode_marker(&record.key)?;
                    found.push((segment, sequence, index));
                }
                Ok::<_, Error>(found)
            })
            .buffer_unordered(SEGMENT_SCAN_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(per_segment.into_iter().flatten().collect())
    }

    /// Loads candidates without verifying them, locating continued ones
    /// first.
    pub(crate) async fn load_candidates(
        &self,
        namespace: &Namespace,
        located: Vec<Located>,
        memo: &PageMemo,
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        let located = stream::iter(located)
            .map(|trace| async move {
                let locators = match trace.locators {
                    Some(locators) => locators,
                    None => self.continuations(namespace, trace.trace_id, now).await?,
                };
                Ok::<_, Error>((trace.trace_id, locators))
            })
            .buffered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        self.load_located(namespace, located, memo).await
    }

    /// Candidates from the live segments in range, with the continuations
    /// the index saw. Without clauses every trace in page metadata is a
    /// candidate; otherwise candidates union within a clause and come from
    /// the clause with the fewest postings, so metadata reads follow the most
    /// selective clause rather than the data in range.
    async fn candidate_ids(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        clauses: &[PushdownClause],
        now: u64,
    ) -> Result<Candidates> {
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
                        let page = (segment, decode_metadata_sequence(&record.key)?);
                        let page_metadata = decode_metadata(&record.value)?;
                        let every = 0..page_metadata.traces.len();
                        found.extend(page_candidates(
                            page,
                            &page_metadata,
                            every,
                            (start_ns, end_ns),
                            now,
                        )?);
                    }
                    Ok::<_, Error>(found)
                })
                .buffered(SEGMENT_SCAN_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            let mut candidates = Candidates::default();
            for seen in per_segment.into_iter().flatten() {
                candidates.insert(seen);
            }
            return Ok(candidates);
        }

        let segments = &segments;
        let mut postings = futures::future::try_join_all(clauses.iter().map(|clause| async move {
            let alternatives = futures::future::try_join_all(
                clause
                    .iter()
                    .map(|predicate| self.predicate_postings(namespace, segments, predicate)),
            )
            .await?;
            let mut union = ClausePostings::new();
            for (page, indices) in alternatives.into_iter().flatten() {
                union.entry(page).or_default().extend(indices);
            }
            Ok::<_, Error>(union)
        }))
        .await?;
        let driving = (0..postings.len())
            .min_by_key(|&clause| postings[clause].values().map(BTreeSet::len).sum::<usize>())
            .map(|clause| postings.swap_remove(clause))
            .unwrap_or_default();
        self.resolve_postings(namespace, driving, &postings, (start_ns, end_ns), now)
            .await
    }

    /// One predicate's postings by page. Exact predicates read one value's
    /// postings; others scan every value of the field and keep those the
    /// predicate admits.
    async fn predicate_postings(
        &self,
        namespace: &Namespace,
        segments: &[SegmentId],
        predicate: &IndexPredicate,
    ) -> Result<Vec<(PageRef, Vec<u32>)>> {
        let per_segment = stream::iter(segments.iter().copied())
            .map(|segment| async move {
                let (prefix, exact) = match &predicate.test {
                    IndexTest::Exact(value) => (
                        field_value_prefix(
                            namespace,
                            segment,
                            predicate.field,
                            &predicate.name,
                            value,
                        ),
                        true,
                    ),
                    _ => (
                        field_scan_prefix(namespace, segment, predicate.field, &predicate.name),
                        false,
                    ),
                };
                let prefix = prefix.freeze();
                let mut postings = self
                    .storage
                    .scan_prefix_iter(prefix.clone(), BytesRange::unbounded(), None)
                    .await?;
                let mut pages = BTreeMap::<u64, Vec<u32>>::new();
                while let Some(record) = postings.next().await? {
                    let sequence = if exact {
                        decode_posting_sequence(&record.key)?
                    } else {
                        let (value, sequence) = decode_posting_value(&record.key, prefix.len())?;
                        if !predicate.admits(&value) {
                            continue;
                        }
                        sequence
                    };
                    pages
                        .entry(sequence)
                        .or_default()
                        .extend(decode_indices(&record.value)?);
                }
                Ok::<_, Error>(
                    pages
                        .into_iter()
                        .map(|(sequence, indices)| ((segment, sequence), indices))
                        .collect::<Vec<_>>(),
                )
            })
            .buffered(SEGMENT_SCAN_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(per_segment.into_iter().flatten().collect())
    }

    /// Resolves the driving clause's postings to candidates through page
    /// metadata, which carries the page directory, so no payload is fetched.
    /// The other clauses are checked against their postings alone: a trace
    /// on one page must be posted by every clause at its own index there.
    /// Traces any clause misses are kept as unconfirmed, since a continued
    /// one may match on a page the driving clause did not post.
    async fn resolve_postings(
        &self,
        namespace: &Namespace,
        driving: ClausePostings,
        others: &[ClausePostings],
        (start_ns, end_ns): (u64, u64),
        now: u64,
    ) -> Result<Candidates> {
        let mut pages = stream::iter(driving)
            .map(|(page @ (segment, sequence), indices)| async move {
                let record = self
                    .storage
                    .get(metadata_key(namespace, segment, sequence))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("attribute posting references missing metadata".to_owned())
                    })?;
                Ok::<_, Error>((page, decode_metadata(&record.value)?, indices))
            })
            .buffered(READ_CONCURRENCY);
        let mut candidates = Candidates::default();
        let mut confirmed = HashSet::new();
        while let Some((page, metadata, indices)) = pages.try_next().await? {
            let indices = indices.into_iter().map(|index| index as usize);
            for seen in page_candidates(page, &metadata, indices, (start_ns, end_ns), now)? {
                let index = seen.locator.trace_index;
                if others.iter().all(|clause| {
                    clause
                        .get(&page)
                        .is_some_and(|posted| posted.contains(&index))
                }) {
                    confirmed.insert(seen.trace.trace_id);
                }
                candidates.insert(seen);
            }
        }
        candidates.unconfirmed = candidates
            .seen
            .keys()
            .filter(|trace_id| !confirmed.contains(trace_id))
            .copied()
            .collect();
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
        let prefix = segment_prefix(namespace, LOCATOR_SEGMENT);
        let (starts, extents) = futures::future::try_join(
            discovery::values(
                self.storage.as_ref(),
                &prefix,
                PARTITION_SCOPE,
                PARTITION_NAME,
            ),
            discovery::values(
                self.storage.as_ref(),
                &prefix,
                PARTITION_SCOPE,
                PARTITION_EXTENT_NAME,
            ),
        )
        .await?;
        let mut segments = BTreeSet::new();
        for value in starts {
            match value {
                DiscoveryValue::Int(segment) if segment >= first && segment <= last => {
                    segments.insert(segment);
                }
                DiscoveryValue::Int(_) => {}
                _ => {
                    return Err(Error::Corrupt(
                        "traces partition catalog contains a non-integer segment".to_owned(),
                    ));
                }
            }
        }
        for value in extents {
            let extent: Option<(SegmentId, SegmentId)> = match &value {
                DiscoveryValue::String(extent) => extent
                    .split_once(':')
                    .and_then(|(start, end)| Some((start.parse().ok()?, end.parse().ok()?))),
                _ => None,
            };
            let Some((segment, last_segment)) = extent else {
                return Err(Error::Corrupt(
                    "traces partition catalog contains a malformed segment extent".to_owned(),
                ));
            };
            if segment < first && last_segment >= first {
                segments.insert(segment);
            }
        }
        Ok(segments)
    }

    /// The first `limit` live trace IDs in ID order, with each trace's first
    /// page. Reads one head record per trace and stops at `limit`.
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
        let mut scanned = Vec::new();
        let mut records = self
            .storage
            .scan_prefix_iter(
                head_namespace_prefix(namespace),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while scanned.len() < limit
            && let Some(record) = records.next().await?
        {
            let head = decode_head(&record.value)?;
            if head.first.is_expired_at(now) {
                continue;
            }
            scanned.push(ScannedTrace {
                trace_id: decode_head_trace_id(&record.key)?,
                first_page: head.first.page(),
            });
        }
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
    /// runs in bounded batches: locator reads and payload fetches are
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
        let memo = PageMemo::default();
        for batch in trace_ids.chunks(MATERIALIZE_BATCH) {
            let located = stream::iter(batch.iter().copied())
                .map(|trace_id| async move {
                    Ok::<_, Error>((trace_id, self.locate(namespace, trace_id, now).await?))
                })
                .buffered(READ_CONCURRENCY)
                .try_collect()
                .await?;
            traces.extend(self.load_located(namespace, located, &memo).await?);
        }
        Ok(traces)
    }

    /// Live pages of one trace: a point get of its head, plus a scan of its
    /// continuations only when it spans several pages.
    pub(super) async fn locate(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Vec<TraceLocator>> {
        let Some(head) = self
            .storage
            .get(head_key(namespace, trace_id))
            .await?
            .map(|record| decode_head(&record.value))
            .transpose()?
        else {
            return Ok(Vec::new());
        };
        if head.first.is_expired_at(now) {
            return Ok(Vec::new());
        }
        if !head.continued {
            return Ok(vec![head.first]);
        }
        self.continuations(namespace, trace_id, now).await
    }

    /// Live pages of a trace known to span several pages.
    async fn continuations(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Vec<TraceLocator>> {
        let mut records = self
            .storage
            .scan_prefix_iter(
                continuation_prefix(namespace, trace_id),
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
        Ok(locators)
    }

    /// Fetches each referenced page once per query, then decodes and merges
    /// every trace's continuations, preserving input order.
    async fn load_located(
        &self,
        namespace: &Namespace,
        located: Vec<(TraceId, Vec<TraceLocator>)>,
        memo: &PageMemo,
    ) -> Result<Vec<Trace>> {
        let wanted: BTreeSet<PageRef> = located
            .iter()
            .flat_map(|(_, locators)| locators.iter().map(TraceLocator::page))
            .collect();
        let pages: HashMap<PageRef, Arc<Page>> = stream::iter(wanted)
            .map(|page @ (segment, sequence)| async move {
                if let Some(found) = memo.get(page) {
                    return Ok((page, found));
                }
                let payload = self
                    .storage
                    .get(payload_key(namespace, segment, sequence))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("trace locator references a missing page".to_owned())
                    })?;
                let decoded = Arc::new(Page::decode(payload.value)?);
                memo.insert(page, &decoded);
                Ok::<_, Error>((page, decoded))
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

/// One continuation of a candidate, as page metadata describes it.
#[derive(Clone, Copy, Debug)]
struct Seen {
    locator: TraceLocator,
    trace: PageTrace,
}

impl Seen {
    fn new(
        (segment, page_sequence): PageRef,
        index: usize,
        metadata: &StoredPageMetadata,
        trace: PageTrace,
    ) -> Result<Self> {
        Ok(Self {
            locator: TraceLocator {
                segment,
                page_sequence,
                trace_index: u32::try_from(index)
                    .map_err(|_| Error::Corrupt("trace index exceeds u32".to_owned()))?,
                expires_at_unix_ms: metadata.expires_at_unix_ms,
            },
            trace,
        })
    }
}

/// The traces at `indices` of one page that overlap the range; none when the
/// page has expired or lies outside it.
fn page_candidates(
    page: PageRef,
    metadata: &StoredPageMetadata,
    indices: impl IntoIterator<Item = usize>,
    (start_ns, end_ns): (u64, u64),
    now: u64,
) -> Result<Vec<Seen>> {
    if metadata.is_expired_at(now) || !metadata.overlaps(start_ns, end_ns) {
        return Ok(Vec::new());
    }
    let mut found = Vec::new();
    for index in indices {
        let trace = metadata.traces.get(index).ok_or_else(|| {
            Error::Corrupt("attribute posting trace index is out of bounds".to_owned())
        })?;
        if trace.overlaps(start_ns, end_ns) {
            found.push(Seen::new(page, index, metadata, *trace)?);
        }
    }
    Ok(found)
}

/// Search candidates, each with the distinct continuations the index saw.
#[derive(Default)]
struct Candidates {
    seen: HashMap<TraceId, Vec<Seen>>,
    /// Traces some clause did not post where the index saw them. Only a
    /// continued trace can still match.
    unconfirmed: HashSet<TraceId>,
}

/// Trace indices one clause posts on each page, unioned over its
/// alternatives.
type ClausePostings = BTreeMap<PageRef, BTreeSet<u32>>;

impl Candidates {
    fn insert(&mut self, seen: Seen) {
        let known = self.seen.entry(seen.trace.trace_id).or_default();
        if known
            .iter()
            .all(|known| known.locator.page() != seen.locator.page())
        {
            known.push(seen);
        }
    }
}

/// Decoded pages kept for the rest of a query, so later load batches that
/// touch the same pages do not fetch them again. Candidates load in start
/// order, not page order, so without it every batch refetches most pages.
/// Pages past the byte bound are fetched per batch instead.
#[derive(Default)]
pub(crate) struct PageMemo(std::sync::Mutex<(HashMap<PageRef, Arc<Page>>, usize)>);

impl PageMemo {
    const MAX_BYTES: usize = 64 << 20;

    fn get(&self, page: PageRef) -> Option<Arc<Page>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
            .get(&page)
            .cloned()
    }

    fn insert(&self, page: PageRef, decoded: &Arc<Page>) {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (pages, bytes) = &mut *guard;
        let len = decoded.bytes().len();
        if *bytes + len <= Self::MAX_BYTES && pages.insert(page, Arc::clone(decoded)).is_none() {
            *bytes += len;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScannedTrace {
    pub(crate) trace_id: TraceId,
    first_page: PageRef,
}

/// A search candidate before any payload is fetched.
#[derive(Clone, Debug)]
pub(crate) struct Located {
    trace_id: TraceId,
    /// The trace's only page, when the index saw it there and it was never
    /// continued. `None` means its pages are located at load time.
    locators: Option<Vec<TraceLocator>>,
    /// Exact for complete traces. For continued traces, the earliest start
    /// among the continuations the index saw, never before the true start.
    start_ns: u64,
}

impl Located {
    fn from_seen(
        trace_id: TraceId,
        seen: &[Seen],
        markers: &HashSet<(SegmentId, u64, u32)>,
    ) -> Self {
        let complete = matches!(seen, [only] if !only.trace.continued
        && !markers.contains(&(
            only.locator.segment,
            only.locator.page_sequence,
            only.locator.trace_index,
        )));
        Self {
            trace_id,
            locators: complete.then(|| vec![seen[0].locator]),
            start_ns: seen
                .iter()
                .map(|seen| seen.trace.min_timestamp_ns)
                .min()
                .unwrap_or(0),
        }
    }
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
/// A span re-sent across the cutover lands on both shards, so parts are
/// deduplicated like continuations within one shard.
pub(crate) fn merge_traces(traces: Vec<Trace>) -> Result<Vec<Trace>> {
    let mut parts = BTreeMap::<TraceId, Vec<Trace>>::new();
    for trace in traces {
        parts.entry(trace.trace_id).or_default().push(trace);
    }
    parts
        .into_iter()
        .map(|(trace_id, mut parts)| match parts.len() {
            1 => Ok(parts.remove(0)),
            _ => merge_continuations(trace_id, parts),
        })
        .collect()
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
    let mut previous = 0;
    let memos = shards
        .iter()
        .map(|_| PageMemo::default())
        .collect::<Vec<_>>();
    let mut remaining = ordered.into_iter();
    while results.len() < options.limit {
        // Low-hit queries would otherwise walk their candidates in many small
        // sequential round trips, so batches double while they come up short.
        let wanted = (options.limit - results.len())
            .max(options.max_concurrency)
            .max(previous * 2)
            .min(MATERIALIZE_BATCH);
        previous = wanted;
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
        let traces = load_batch((shards, &memos), permits, namespace, batch)
            .await?
            .into_iter()
            .filter(|trace| satisfies(trace, &plan.pushdown))
            .collect::<Vec<_>>();
        // One task per chunk: evaluation takes microseconds per trace, so a
        // task per trace would mostly measure scheduling.
        let chunk_len = traces.len().div_ceil(options.max_concurrency).max(1);
        let mut chunks = Vec::new();
        let mut traces = traces.into_iter().peekable();
        while traces.peek().is_some() {
            chunks.push(traces.by_ref().take(chunk_len).collect::<Vec<_>>());
        }
        let mut executed = stream::iter(chunks)
            .map(|chunk| {
                let query = plan.query.clone();
                let max_spans = options.max_spans_per_trace;
                tokio::spawn(async move {
                    chunk
                        .iter()
                        .map(|trace| crate::traceql::execute(trace, &query, max_spans))
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
            })
            .buffered(options.max_concurrency);
        'chunks: while let Some(chunk) = executed.next().await {
            let chunk = chunk
                .map_err(|error| Error::Invalid(format!("TraceQL task failed: {error}")))??;
            for result in chunk.into_iter().flatten() {
                results.push(result);
                if results.len() == options.limit {
                    break 'chunks;
                }
            }
        }
    }
    // Continued candidates are ordered by the pages the index saw; their
    // exact start is only known once loaded.
    results.sort_by_key(|result| (result.start_ns, result.trace_id));
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
    (shards, memos): (&[&TraceDb], &[PageMemo]),
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
                shards[shard]
                    .load_candidates(namespace, located, &memos[shard])
                    .await
            }),
    )
    .await?;
    let mut merged = merge_traces(parts.into_iter().flatten().collect())?
        .into_iter()
        .map(|trace| (trace.trace_id, trace))
        .collect::<HashMap<_, _>>();
    Ok(order
        .into_iter()
        .filter_map(|trace_id| merged.remove(&trace_id))
        .collect())
}

/// Whether `trace` holds a value the index would have posted for
/// `predicate`.
fn trace_matches(trace: &Trace, predicate: &IndexPredicate) -> bool {
    let spans = || {
        trace
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
    };
    match predicate.field {
        IndexField::Resource => trace.resource_spans.iter().any(|resource_spans| {
            resource_spans
                .resource
                .as_ref()
                .is_some_and(|resource| attributes_match(&resource.attributes, predicate))
        }),
        IndexField::Span => spans().any(|span| attributes_match(&span.attributes, predicate)),
        IndexField::Intrinsic => spans().any(|span| {
            span_intrinsics(span)
                .iter()
                .any(|(name, value)| *name == predicate.name && predicate.admits(value))
        }),
    }
}

fn attributes_match(attributes: &[KeyValue], predicate: &IndexPredicate) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key == predicate.name
            && attribute
                .value
                .as_ref()
                .and_then(AttributeValue::from_otlp)
                .is_some_and(|found| predicate.admits(&found))
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
