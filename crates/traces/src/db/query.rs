// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: candidate selection, trace loading, catalog listing and TraceQL.

use bytes::{BufMut, BytesMut};

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
                .load_candidates(namespace, batch.to_vec(), &memo, None)
                .await?;
            traces.retain(|trace| satisfies(trace, &clauses));
            results.extend(traces);
        }
        results.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        Ok(results)
    }

    /// Candidates overlapping `[start_ns, end_ns]`, in result order
    /// `(start, trace ID)`, by the earliest bounds of the pages the index
    /// saw; their pages are located only when loaded.
    ///
    /// A candidate some clause missed can only match through pages the
    /// index did not see. One seen on a single page that no earlier page of
    /// its flush precedes may have pages from other flushes, which only its
    /// head counts, so its head decides whether it is kept.
    pub(crate) async fn ordered_candidates(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        clauses: &[PushdownClause],
        now: u64,
    ) -> Result<Vec<Located>> {
        self.ordered_candidates_for(namespace, (start_ns, end_ns), clauses, now, false)
            .await
    }

    /// [`Self::ordered_candidates`]; for an [`existential`] query, only
    /// those some clause-confirmed part can match, without reading heads.
    ///
    /// [`existential`]: crate::traceql::existential
    pub(crate) async fn ordered_candidates_for(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (u64, u64),
        clauses: &[PushdownClause],
        now: u64,
        existential: bool,
    ) -> Result<Vec<Located>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let Candidates { seen, unconfirmed } = self
            .candidate_ids(namespace, start_ns, end_ns, clauses, now)
            .await?;
        let candidates = seen
            .into_iter()
            .filter(|(trace_id, _)| !existential || !unconfirmed.contains(trace_id))
            .map(|(trace_id, seen)| {
                let single = matches!(seen.as_slice(), [only] if !only.trace.continued);
                (Located::from_seen(trace_id, &seen), single)
            })
            .collect::<Vec<_>>();
        let mut located = stream::iter(candidates)
            .map(|(located, single)| {
                let unconfirmed = &unconfirmed;
                async move {
                    if !single || !unconfirmed.contains(&located.trace_id) {
                        return Ok::<_, Error>(Some(located));
                    }
                    let pages = self.page_count(namespace, located.trace_id, now).await?;
                    Ok(pages.is_some_and(|pages| pages > 1).then_some(located))
                }
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_filter_map(|located| async move { Ok(located) })
            .try_collect::<Vec<_>>()
            .await?;
        located.sort_unstable_by_key(|trace| (trace.start_ns, trace.trace_id));
        Ok(located)
    }

    /// Pages written for `trace_id`, from cached locators the local writer
    /// keeps current or else its head; `None` when it has no live head.
    async fn page_count(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Option<u32>> {
        if let Some(cached) = self.read_cache.current_locators(namespace, trace_id).await {
            return Ok(Some(cached.pages));
        }
        Ok(self
            .live_head(namespace, trace_id, now)
            .await?
            .map(|head| head.pages))
    }

    /// Loads candidates without verifying them, locating their pages first.
    /// With `prune`, candidates it rules out are skipped undecoded.
    pub(crate) async fn load_candidates(
        &self,
        namespace: &Namespace,
        located: Vec<Located>,
        memo: &PageMemo,
        prune: Option<Prune<'_>>,
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        // Head keys follow trace IDs, so reading them in ID order keeps
        // neighbouring gets on the same blocks.
        let mut ids = located
            .iter()
            .map(|trace| trace.trace_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        let mut found = stream::iter(ids)
            .map(|trace_id| async move {
                let locators = self.cached_locate(namespace, trace_id, now).await?;
                Ok::<_, Error>((trace_id, locators))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect::<HashMap<_, _>>()
            .await?;
        let located = located
            .into_iter()
            .filter_map(|trace| Some((trace.trace_id, found.remove(&trace.trace_id)?)))
            .collect();
        self.load_located(namespace, located, memo, prune).await
    }

    /// Loads the parts of candidates the index saw, without reading their
    /// heads; a part need not be its whole trace.
    pub(crate) async fn load_parts(
        &self,
        namespace: &Namespace,
        located: Vec<Located>,
        memo: &PageMemo,
        prune: Option<Prune<'_>>,
    ) -> Result<Vec<Trace>> {
        let now = unix_time_ms()?;
        let located = located
            .into_iter()
            .map(|trace| (trace.trace_id, live_locators(&trace.seen, now)))
            .collect();
        self.load_located(namespace, located, memo, prune).await
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

        let segments = &self.written_segments(namespace, segments).await?;
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

    /// One predicate's postings by page, over segments paired with their
    /// next page sequence. Exact predicates read one value's postings;
    /// others scan the values of the field in the predicate's value ranges,
    /// or every value without any, and keep those the predicate admits.
    async fn predicate_postings(
        &self,
        namespace: &Namespace,
        segments: &[(SegmentId, u64)],
        predicate: &IndexPredicate,
    ) -> Result<Vec<(PageRef, Vec<u32>)>> {
        let ranges = predicate.value_ranges();
        let ranges = &ranges;
        let per_segment = stream::iter(segments.iter().copied())
            .map(|(segment, through)| async move {
                let field = || {
                    field_scan_prefix(namespace, segment, predicate.field, &predicate.name).freeze()
                };
                let scans = match (&predicate.test, ranges) {
                    (IndexTest::Exact(value), _) => vec![PostingScan {
                        prefix: field_value_prefix(
                            namespace,
                            segment,
                            predicate.field,
                            &predicate.name,
                            value,
                        )
                        .freeze(),
                        subrange: None,
                        exact: true,
                    }],
                    (_, Some(ranges)) => ranges
                        .iter()
                        .map(|(low, high)| PostingScan {
                            prefix: field(),
                            subrange: Some(value_subrange(low, high)),
                            exact: false,
                        })
                        .collect(),
                    (_, None) => vec![PostingScan {
                        prefix: field(),
                        subrange: None,
                        exact: false,
                    }],
                };
                let scanned = futures::future::try_join_all(
                    scans
                        .into_iter()
                        .map(|scan| self.segment_postings(scan, through)),
                )
                .await?;
                let mut pages = BTreeMap::<u64, Vec<u32>>::new();
                for entry in scanned.iter().flat_map(|postings| &postings.entries) {
                    if entry
                        .value
                        .as_ref()
                        .is_some_and(|value| !predicate.admits(value))
                    {
                        continue;
                    }
                    pages
                        .entry(entry.sequence)
                        .or_default()
                        .extend_from_slice(&entry.indices);
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

    /// The posting records `scan` selects of the pages below `through`, a
    /// segment's next page sequence as read by this query. Cached postings
    /// older than it are extended by scanning only newer pages when the
    /// prefix fixes a value, whose keys then end in the page sequence, and
    /// rescanned otherwise.
    async fn segment_postings(
        &self,
        scan: PostingScan,
        through: u64,
    ) -> Result<Arc<SegmentPostings>> {
        let key = scan.cache_key();
        let cached = self.read_cache.postings(&key).await;
        if let Some(cached) = &cached
            && cached.through >= through
        {
            return Ok(Arc::clone(cached));
        }
        let (from, mut entries) = match cached {
            Some(cached) if scan.exact => (cached.through, Arc::unwrap_or_clone(cached).entries),
            _ => (0, Vec::new()),
        };
        let range = match scan.subrange {
            _ if from > 0 => BytesRange::new(
                std::ops::Bound::Included(Bytes::copy_from_slice(&from.to_be_bytes())),
                std::ops::Bound::Unbounded,
            ),
            Some((low, high)) => BytesRange::new(
                std::ops::Bound::Included(low),
                std::ops::Bound::Included(high),
            ),
            None => BytesRange::unbounded(),
        };
        let prefix = scan.prefix;
        let mut records = self
            .storage
            .scan_prefix_iter(prefix.clone(), range, None)
            .await?;
        while let Some(record) = records.next().await? {
            let (value, sequence) = if scan.exact {
                (None, decode_posting_sequence(&record.key)?)
            } else {
                let (value, sequence) = decode_posting_value(&record.key, prefix.len())?;
                (Some(value), sequence)
            };
            // Pages published after `through` was read are left to a later
            // query, so the cached records cover exactly the pages below it.
            if sequence >= through {
                continue;
            }
            entries.push(PostingEntry {
                value,
                sequence,
                indices: decode_indices(&record.value)?.into_boxed_slice(),
            });
        }
        let postings = Arc::new(SegmentPostings { through, entries });
        if through > 0 {
            self.read_cache.insert_postings(key, &postings).await;
        }
        Ok(postings)
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
        let mut runs = stream::iter(metadata_runs(driving))
            .map(|run| self.run_metadata(namespace, run))
            .buffered(READ_CONCURRENCY);
        let mut candidates = Candidates::default();
        let mut confirmed = HashSet::new();
        while let Some(run) = runs.try_next().await? {
            for (page, metadata, indices) in run {
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
        }
        candidates.unconfirmed = candidates
            .seen
            .keys()
            .filter(|trace_id| !confirmed.contains(trace_id))
            .copied()
            .collect();
        Ok(candidates)
    }

    /// Metadata of a run's pages, in order, from the read cache where held.
    /// Expired pages may be missing when records expire.
    async fn run_metadata(
        &self,
        namespace: &Namespace,
        run: Vec<(PageRef, BTreeSet<u32>)>,
    ) -> Result<Vec<(PageRef, Arc<StoredPageMetadata>, BTreeSet<u32>)>> {
        let mut found = Vec::with_capacity(run.len());
        let mut uncached = Vec::new();
        for (page, indices) in run {
            match self.read_cache.metadata(namespace, page).await {
                Some(metadata) => found.push((page, metadata, indices)),
                None => uncached.push((page, indices)),
            }
        }
        if uncached.is_empty() {
            return Ok(found);
        }
        for (page, metadata, indices) in self.fetch_run_metadata(namespace, uncached).await? {
            let metadata = Arc::new(metadata);
            self.read_cache
                .insert_metadata(namespace, page, &metadata)
                .await;
            found.push((page, metadata, indices));
        }
        found.sort_unstable_by_key(|(page, ..)| *page);
        Ok(found)
    }

    /// Metadata of a run's pages from storage, in order: a point get for a
    /// lone page, otherwise one scan from its first page to its last.
    async fn fetch_run_metadata(
        &self,
        namespace: &Namespace,
        run: Vec<(PageRef, BTreeSet<u32>)>,
    ) -> Result<Vec<(PageRef, StoredPageMetadata, BTreeSet<u32>)>> {
        let missing = || Error::Corrupt("attribute posting references missing metadata".to_owned());
        let (Some(&((segment, first), _)), Some(&((_, last), _))) = (run.first(), run.last())
        else {
            return Ok(Vec::new());
        };
        if first == last {
            let record = self
                .storage
                .get(metadata_key(namespace, segment, first))
                .await?;
            let (page, indices) = run.into_iter().next().expect("run is not empty");
            return match record {
                Some(record) => Ok(vec![(page, decode_metadata(&record.value)?, indices)]),
                None if self.expiring => Ok(Vec::new()),
                None => Err(missing()),
            };
        }
        let bound = |sequence: u64| {
            std::ops::Bound::Included(Bytes::copy_from_slice(&sequence.to_be_bytes()))
        };
        let mut records = self
            .storage
            .scan_prefix_iter(
                metadata_prefix(namespace, segment),
                BytesRange::new(bound(first), bound(last)),
                None,
            )
            .await?;
        let mut wanted = run.into_iter().peekable();
        let mut found = Vec::with_capacity(wanted.len());
        let mut record = records.next().await?;
        while let Some(&((_, next), _)) = wanted.peek() {
            let Some(current) = &record else {
                if !self.expiring {
                    return Err(missing());
                }
                break;
            };
            let sequence = decode_metadata_sequence(&current.key)?;
            if sequence < next {
                record = records.next().await?;
                continue;
            }
            let (page, indices) = wanted.next().expect("peeked");
            if sequence > next {
                if !self.expiring {
                    return Err(missing());
                }
                continue;
            }
            found.push((page, decode_metadata(&current.value)?, indices));
            record = records.next().await?;
        }
        Ok(found)
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
        for (segment, through) in self.written_segments(namespace, segments).await? {
            let key = (namespace.clone(), segment, scope);
            let segment_names = match self.catalog_names_cache.get(&key) {
                Some(cached) if cached.0 == through => cached,
                _ => {
                    let names = discovery::names(
                        self.storage.as_ref(),
                        &segment_prefix(namespace, segment),
                        scope.map(catalog_scope),
                    )
                    .await?;
                    self.catalog_names_cache
                        .insert(key, (through, names), false)
                }
            };
            names.extend(segment_names.1.iter().cloned());
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
        for (segment, through) in self.written_segments(namespace, segments).await? {
            let key = (namespace.clone(), segment, scope, name.to_owned());
            let segment_values = match self.catalog_values_cache.get(&key) {
                Some(cached) if cached.0 == through => cached,
                _ => {
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
                        (through, found.into_iter().collect()),
                        false,
                    )
                }
            };
            values.extend(segment_values.1.iter().cloned());
        }
        Ok(values.into_iter().collect())
    }

    /// The segments that hold pages, with each one's next page sequence.
    /// Every write to a segment advances its sequence in the batch that
    /// writes its pages and catalog terms, so a catalog scan taken after
    /// reading the sequence covers at least every write the sequence counts.
    /// Traces land in the segment of their earliest span, so a segment keeps
    /// taking writes long after wall-clock time has left it.
    async fn written_segments(
        &self,
        namespace: &Namespace,
        segments: impl IntoIterator<Item = SegmentId>,
    ) -> Result<Vec<(SegmentId, u64)>> {
        stream::iter(segments)
            .map(|segment| async move {
                let through = self
                    .storage
                    .get(next_sequence_key(namespace, segment))
                    .await?
                    .map(|record| decode_sequence(&record.value))
                    .transpose()?
                    .unwrap_or(0);
                Ok::<_, Error>((segment, through))
            })
            .buffered(READ_CONCURRENCY)
            .try_filter(|&(_, through)| std::future::ready(through > 0))
            .try_collect()
            .await
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

    /// [`Self::query_traceql`] without matched spans, as a search response
    /// lists them; page columns stand in for payloads where they can.
    pub async fn search_traceql(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        source: &str,
        options: QueryOptions,
    ) -> Result<Vec<TraceSummary>> {
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
            traces.extend(self.load_located(namespace, located, &memo, None).await?);
        }
        Ok(traces)
    }

    /// `trace_id`'s head, unless it is missing or expired.
    async fn live_head(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Option<TraceHead>> {
        Ok(self
            .storage
            .get(head_key(namespace, trace_id))
            .await?
            .map(|record| decode_head(&record.value))
            .transpose()?
            .filter(|head| !head.first.is_expired_at(now)))
    }

    /// Live pages of one trace: a point get of its head, plus a scan of its
    /// continuations only when it spans several pages.
    pub(super) async fn locate(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Vec<TraceLocator>> {
        let epoch = self.read_cache.locator_epoch();
        let Some(head) = self.live_head(namespace, trace_id, now).await? else {
            return Ok(Vec::new());
        };
        if !head.continued {
            self.read_cache
                .insert_locators(
                    namespace,
                    trace_id,
                    Arc::new(TraceLocators {
                        pages: head.pages,
                        locators: vec![head.first],
                    }),
                    epoch,
                )
                .await;
            return Ok(vec![head.first]);
        }
        self.head_continuations(namespace, trace_id, head.pages, now)
            .await
    }

    /// [`Self::locate`], answered without the head from locators the local
    /// writer keeps current.
    pub(super) async fn cached_locate(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        now: u64,
    ) -> Result<Vec<TraceLocator>> {
        if let Some(cached) = self.read_cache.current_locators(namespace, trace_id).await {
            return Ok(live_locators(&cached.locators, now));
        }
        self.locate(namespace, trace_id, now).await
    }

    /// Live pages of a continued trace whose head counts `pages`, from the
    /// read cache when it holds them at that count.
    async fn head_continuations(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        pages: u32,
        now: u64,
    ) -> Result<Vec<TraceLocator>> {
        if let Some(cached) = self.read_cache.locators(namespace, trace_id, pages).await {
            return Ok(live_locators(&cached.locators, now));
        }
        let epoch = self.read_cache.locator_epoch();
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
            locators.push(decode_locator(&record.value)?);
        }
        let found = live_locators(&locators, now);
        self.read_cache
            .insert_locators(
                namespace,
                trace_id,
                Arc::new(TraceLocators { pages, locators }),
                epoch,
            )
            .await;
        Ok(found)
    }

    /// Each of `wanted`, from the query's memo, the read cache or storage.
    async fn fetch_pages(
        &self,
        namespace: &Namespace,
        wanted: BTreeSet<PageRef>,
        memo: &PageMemo,
    ) -> Result<HashMap<PageRef, Arc<Page>>> {
        stream::iter(wanted)
            .map(|page @ (segment, sequence)| async move {
                if let Some(found) = memo.get(page) {
                    return Ok((page, found));
                }
                if let Some(found) = self.read_cache.page(namespace, page).await {
                    memo.insert(page, &found);
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
                self.read_cache.insert_page(namespace, page, &decoded).await;
                Ok::<_, Error>((page, decoded))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await
    }

    /// The candidates some part the index saw matches, decided from page
    /// columns without decoding payloads; `None` when a page's columns
    /// cannot evaluate `decider`.
    pub(crate) async fn match_parts(
        &self,
        namespace: &Namespace,
        located: &[Located],
        memo: &PageMemo,
        decider: &crate::traceql::columns::Decider,
    ) -> Result<Option<HashSet<TraceId>>> {
        let now = unix_time_ms()?;
        let live = located
            .iter()
            .map(|trace| (trace.trace_id, live_locators(&trace.seen, now)))
            .collect::<Vec<_>>();
        let pages = self
            .fetch_pages(
                namespace,
                live.iter()
                    .flat_map(|(_, locators)| locators.iter().map(TraceLocator::page))
                    .collect(),
                memo,
            )
            .await?;
        let mut decided = HashMap::<PageRef, Vec<bool>>::new();
        let mut matched = HashSet::new();
        for (trace_id, locators) in live {
            for locator in locators {
                let page = &pages[&locator.page()];
                let index = locator.trace_index as usize;
                if page
                    .directory()
                    .get(index)
                    .is_none_or(|entry| entry.trace_id != trace_id)
                {
                    return Err(Error::Corrupt(
                        "trace locator points to a different trace".to_owned(),
                    ));
                }
                let traces = match decided.entry(locator.page()) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        match page.columns()?.decide(decider)? {
                            Some(traces) => entry.insert(traces),
                            None => return Ok(None),
                        }
                    }
                };
                if traces[index] {
                    matched.insert(trace_id);
                    break;
                }
            }
        }
        Ok(Some(matched))
    }

    /// Summaries, from page columns, of those `trace_ids` wholly on one live
    /// page whose columns can tell their root, within `max_spans`; each must
    /// match the query `decider` decides exactly.
    pub(crate) async fn summarize(
        &self,
        namespace: &Namespace,
        mut trace_ids: Vec<TraceId>,
        memo: &PageMemo,
        decider: &crate::traceql::columns::Decider,
        max_spans: usize,
    ) -> Result<Vec<(TraceId, TraceSummary)>> {
        let now = unix_time_ms()?;
        trace_ids.sort_unstable();
        trace_ids.dedup();
        let located = stream::iter(trace_ids)
            .map(|trace_id| async move {
                Ok::<_, Error>((
                    trace_id,
                    self.cached_locate(namespace, trace_id, now).await?,
                ))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut by_page = BTreeMap::<PageRef, Vec<(TraceId, usize)>>::new();
        for (trace_id, locators) in located {
            if let [locator] = locators.as_slice() {
                by_page
                    .entry(locator.page())
                    .or_default()
                    .push((trace_id, locator.trace_index as usize));
            }
        }
        let pages = self
            .fetch_pages(namespace, by_page.keys().copied().collect(), memo)
            .await?;
        let mut summaries = Vec::new();
        for (page, traces) in by_page {
            let page = &pages[&page];
            for &(trace_id, index) in &traces {
                if page
                    .directory()
                    .get(index)
                    .is_none_or(|entry| entry.trace_id != trace_id)
                {
                    return Err(Error::Corrupt(
                        "trace locator points to a different trace".to_owned(),
                    ));
                }
            }
            let columns = page.columns()?;
            let indexes = traces.iter().map(|&(_, index)| index).collect::<Vec<_>>();
            let Some(found) = columns.summaries(&indexes, decider)? else {
                continue;
            };
            for ((trace_id, index), summary) in traces.into_iter().zip(found) {
                let Some(summary) = summary else {
                    continue;
                };
                if columns.trace_spans(index).len() > max_spans || summary.matched == 0 {
                    continue;
                }
                let entry = &page.directory()[index];
                summaries.push((
                    trace_id,
                    TraceSummary {
                        trace_id,
                        start_ns: entry.min_timestamp_ns,
                        end_ns: entry.max_timestamp_ns,
                        root_service_name: summary.root_service_name,
                        root_span_name: summary.root_span_name,
                        matched_spans: summary.matched,
                    },
                ));
            }
        }
        Ok(summaries)
    }

    /// Fetches each referenced page once per query, then decodes and merges
    /// every trace's continuations, preserving input order.
    async fn load_located(
        &self,
        namespace: &Namespace,
        located: Vec<(TraceId, Vec<TraceLocator>)>,
        memo: &PageMemo,
        prune: Option<Prune<'_>>,
    ) -> Result<Vec<Trace>> {
        let pages = self
            .fetch_pages(
                namespace,
                located
                    .iter()
                    .flat_map(|(_, locators)| locators.iter().map(TraceLocator::page))
                    .collect(),
                memo,
            )
            .await?;
        let mut traces = Vec::with_capacity(located.len());
        let mut page_matches = HashMap::new();
        for (trace_id, locators) in located {
            if locators.is_empty() {
                continue;
            }
            if let Some(prune) = prune
                && prune.ruled_out(trace_id, &locators, &pages, &mut page_matches)?
            {
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

/// The posting records under `prefix`, or only those within its inclusive
/// `subrange`. `exact` prefixes fix a value, so their keys end in the page
/// sequence.
struct PostingScan {
    prefix: Bytes,
    subrange: Option<(Bytes, Bytes)>,
    exact: bool,
}

impl PostingScan {
    fn cache_key(&self) -> Bytes {
        let Some((low, high)) = &self.subrange else {
            return self.prefix.clone();
        };
        // Within a field prefix every posting key continues with a value
        // type tag, never `0xff`, so bounded scans get keys of their own.
        let mut key = BytesMut::with_capacity(self.prefix.len() + 1 + low.len() + high.len());
        key.extend_from_slice(&self.prefix);
        key.put_u8(0xff);
        key.extend_from_slice(low);
        key.extend_from_slice(high);
        key.freeze()
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

/// Splits posted pages, in key order, into runs within one segment whose
/// wanted pages are at most [`METADATA_SCAN_MAX_GAP`] apart.
fn metadata_runs(postings: ClausePostings) -> Vec<Vec<(PageRef, BTreeSet<u32>)>> {
    let mut runs: Vec<Vec<(PageRef, BTreeSet<u32>)>> = Vec::new();
    for (page @ (segment, sequence), indices) in postings {
        match runs.last_mut() {
            Some(run)
                if run.last().is_some_and(|&((last_segment, last), _)| {
                    last_segment == segment && sequence - last <= METADATA_SCAN_MAX_GAP + 1
                }) =>
            {
                run.push((page, indices));
            }
            _ => runs.push(vec![(page, indices)]),
        }
    }
    runs
}

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

/// Per trace of a page, whether some span satisfies each prefilter leaf.
type PageMatches = HashMap<PageRef, (Arc<PageColumns>, Vec<Vec<bool>>)>;

/// Skips TraceQL candidates that page columns prove the query cannot return.
#[derive(Clone, Copy)]
pub(crate) struct Prune<'a> {
    filter: &'a TraceFilter,
    max_spans: usize,
    /// Candidates held entirely by the shard being loaded. A trace split
    /// across shards is merged before evaluation, so a part alone cannot
    /// rule it out.
    complete: &'a HashSet<TraceId>,
}

impl<'a> Prune<'a> {
    #[cfg(test)]
    pub(crate) fn new(
        filter: &'a TraceFilter,
        max_spans: usize,
        complete: &'a HashSet<TraceId>,
    ) -> Self {
        Self {
            filter,
            max_spans,
            complete,
        }
    }

    /// Whether `trace_id` certainly yields no result. The filter's leaves are
    /// each satisfied by the merged trace when satisfied on any continuation,
    /// since merging only drops exact duplicate spans. Traces that would exceed the span
    /// limit, or whose locators disagree with their page, are left to full
    /// evaluation so they fail exactly as before.
    fn ruled_out(
        &self,
        trace_id: TraceId,
        locators: &[TraceLocator],
        pages: &HashMap<PageRef, Arc<Page>>,
        page_matches: &mut PageMatches,
    ) -> Result<bool> {
        if !self.complete.contains(&trace_id) {
            return Ok(false);
        }
        let mut satisfied = vec![false; self.filter.leaves().len()];
        let mut spans = 0_usize;
        for locator in locators {
            let page = &pages[&locator.page()];
            let index = locator.trace_index as usize;
            if page
                .directory()
                .get(index)
                .is_none_or(|entry| entry.trace_id != trace_id)
            {
                return Ok(false);
            }
            let (columns, exists) = match page_matches.entry(locator.page()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let columns = page.columns()?;
                    let exists = columns.exists(self.filter)?;
                    entry.insert((columns, exists))
                }
            };
            spans = spans.saturating_add(columns.trace_spans(index).len());
            for (satisfied, leaf) in satisfied.iter_mut().zip(exists.iter()) {
                *satisfied |= leaf[index];
            }
        }
        Ok(spans <= self.max_spans && !self.filter.matches(&satisfied))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScannedTrace {
    pub(crate) trace_id: TraceId,
    first_page: PageRef,
}

/// A search candidate before any payload is fetched; its pages are located
/// at load time.
#[derive(Clone, Debug)]
pub(crate) struct Located {
    pub(super) trace_id: TraceId,
    /// The earliest start among the pages the index saw, never before the
    /// true start, and exact when they are all of the trace's pages.
    start_ns: u64,
    /// Where the index saw the trace, in page order.
    seen: Vec<TraceLocator>,
}

impl Located {
    fn from_seen(trace_id: TraceId, seen: &[Seen]) -> Self {
        let mut locators = seen.iter().map(|seen| seen.locator).collect::<Vec<_>>();
        locators.sort_unstable_by_key(TraceLocator::page);
        Self {
            trace_id,
            start_ns: seen
                .iter()
                .map(|seen| seen.trace.min_timestamp_ns)
                .min()
                .unwrap_or(0),
            seen: locators,
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
#[derive(Clone)]
struct CandidateGroup {
    start_ns: u64,
    trace_id: TraceId,
    parts: Vec<(usize, Located)>,
}

/// What a TraceQL search returns per matching trace.
pub(crate) trait Hit: Sized + Send + 'static {
    /// Whether page columns can stand in for a decoded trace.
    const SUMMARIES: bool;

    fn evaluate(
        trace: &Trace,
        compiled: &crate::traceql::CompiledQuery,
        max_spans: usize,
    ) -> std::result::Result<Option<Self>, crate::traceql::QueryError>;

    fn from_columns(summary: TraceSummary) -> Option<Self>;

    fn order(&self) -> (u64, TraceId);
}

impl Hit for TraceQlResult {
    const SUMMARIES: bool = false;

    fn evaluate(
        trace: &Trace,
        compiled: &crate::traceql::CompiledQuery,
        max_spans: usize,
    ) -> std::result::Result<Option<Self>, crate::traceql::QueryError> {
        crate::traceql::execute_compiled(trace, compiled, max_spans)
    }

    fn from_columns(_summary: TraceSummary) -> Option<Self> {
        None
    }

    fn order(&self) -> (u64, TraceId) {
        (self.start_ns, self.trace_id)
    }
}

impl Hit for TraceSummary {
    const SUMMARIES: bool = true;

    fn evaluate(
        trace: &Trace,
        compiled: &crate::traceql::CompiledQuery,
        max_spans: usize,
    ) -> std::result::Result<Option<Self>, crate::traceql::QueryError> {
        crate::traceql::summarize_compiled(trace, compiled, max_spans)
    }

    fn from_columns(summary: TraceSummary) -> Option<Self> {
        Some(summary)
    }

    fn order(&self) -> (u64, TraceId) {
        (self.start_ns, self.trace_id)
    }
}

/// Executes a non-metrics TraceQL query over one or more shards.
///
/// Candidates from every shard are merged into global result order
/// `(start, trace ID)` before any payload is loaded, and a trace split across
/// shards is merged before evaluation. Payloads load in order until `limit`
/// results are found, and every loaded trace counts toward
/// `max_candidate_traces`, so shards never each load a full limit.
pub(crate) async fn execute_traceql<T: Hit>(
    shards: &[&TraceDb],
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    (start_ns, end_ns): (u64, u64),
    source: &str,
    options: QueryOptions,
) -> Result<Vec<T>> {
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
    let prefilter = crate::traceql::prefilter(&plan.query);
    let compiled = Arc::new(crate::traceql::CompiledQuery::new(plan.query.clone()));
    let existential = crate::traceql::existential(&plan.query);
    let decider = crate::traceql::columns::decider(&plan.query);
    let now = unix_time_ms()?;
    let per_shard = futures::future::try_join_all(shards.iter().map(|database| async {
        let _permit = acquire(permits).await?;
        database
            .ordered_candidates_for(
                namespace,
                (start_ns, end_ns),
                &plan.pushdown,
                now,
                existential,
            )
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
        let prune = prefilter
            .as_ref()
            .map(|filter| (filter, options.max_spans_per_trace));
        let sources = (shards, memos.as_slice());
        let needed = options.limit - results.len();
        if !existential {
            let traces = load_batch(sources, permits, namespace, batch, prune, false).await?;
            results.extend(evaluate_batch::<T>(traces, &plan, &compiled, &options, needed).await?);
            continue;
        }
        // A part that matches proves its trace does, so only matches read
        // heads, to load and evaluate their whole traces. Parts are decoded
        // only when their columns cannot decide them.
        let decided = match &decider {
            Some(decider) => match_batch(sources, permits, namespace, &batch, decider).await?,
            None => None,
        };
        let columnar = decided.is_some();
        let matched = match decided {
            Some(decided) => batch
                .iter()
                .map(|group| group.trace_id)
                .filter(|trace_id| decided.contains(trace_id))
                .take(needed)
                .collect::<HashSet<_>>(),
            None => {
                let parts =
                    load_batch(sources, permits, namespace, batch.clone(), prune, true).await?;
                evaluate_batch::<TraceSummary>(parts, &plan, &compiled, &options, needed)
                    .await?
                    .into_iter()
                    .map(|summary| summary.trace_id)
                    .collect()
            }
        };
        let mut batch = batch
            .into_iter()
            .filter(|group| matched.contains(&group.trace_id))
            .collect::<Vec<_>>();
        if T::SUMMARIES
            && columnar
            && let Some(decider) = &decider
        {
            let summaries = summarize_batch(
                sources,
                permits,
                namespace,
                &batch,
                decider,
                options.max_spans_per_trace,
            )
            .await?;
            batch.retain(|group| !summaries.contains_key(&group.trace_id));
            results.extend(summaries.into_values().filter_map(T::from_columns));
        }
        let needed = options.limit - results.len();
        let traces = load_batch(sources, permits, namespace, batch, None, false).await?;
        results.extend(evaluate_batch::<T>(traces, &plan, &compiled, &options, needed).await?);
    }
    // Continued candidates are ordered by the pages the index saw; their
    // exact start is only known once loaded.
    results.sort_by_key(T::order);
    Ok(results)
}

/// The first `wanted` results of `traces` that satisfy the plan's pushdown
/// and the query, in order.
async fn evaluate_batch<T: Hit>(
    traces: Vec<Trace>,
    plan: &crate::traceql::QueryPlan,
    compiled: &Arc<crate::traceql::CompiledQuery>,
    options: &QueryOptions,
    wanted: usize,
) -> Result<Vec<T>> {
    let traces = traces
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
            let query = Arc::clone(compiled);
            let max_spans = options.max_spans_per_trace;
            tokio::spawn(async move {
                chunk
                    .iter()
                    .map(|trace| T::evaluate(trace, &query, max_spans))
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
        })
        .buffered(options.max_concurrency);
    let mut results = Vec::new();
    while results.len() < wanted
        && let Some(chunk) = executed.next().await
    {
        let chunk =
            chunk.map_err(|error| Error::Invalid(format!("TraceQL task failed: {error}")))??;
        results.extend(chunk.into_iter().flatten().take(wanted - results.len()));
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
/// split across shards, in the batch's order: whole traces, or with `parts`
/// only the parts the index saw. With a prefilter and span limit,
/// candidates the prefilter rules out are skipped.
async fn load_batch(
    (shards, memos): (&[&TraceDb], &[PageMemo]),
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    batch: Vec<CandidateGroup>,
    prefilter: Option<(&TraceFilter, usize)>,
    parts: bool,
) -> Result<Vec<Trace>> {
    let order = batch.iter().map(|group| group.trace_id).collect::<Vec<_>>();
    // A part decides only for itself, so ruling one out never loses a match.
    let complete = batch
        .iter()
        .filter(|group| parts || group.parts.len() == 1)
        .map(|group| group.trace_id)
        .collect::<HashSet<_>>();
    let prune = prefilter.map(|(filter, max_spans)| Prune {
        filter,
        max_spans,
        complete: &complete,
    });
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
                let (database, memo) = (shards[shard], &memos[shard]);
                if parts {
                    database.load_parts(namespace, located, memo, prune).await
                } else {
                    database
                        .load_candidates(namespace, located, memo, prune)
                        .await
                }
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

/// The candidates of `batch` that some shard's parts match, decided from
/// page columns; `None` when some page's columns cannot decide.
async fn match_batch(
    (shards, memos): (&[&TraceDb], &[PageMemo]),
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    batch: &[CandidateGroup],
    decider: &crate::traceql::columns::Decider,
) -> Result<Option<HashSet<TraceId>>> {
    let mut by_shard = vec![Vec::new(); shards.len()];
    for group in batch {
        for (shard, located) in &group.parts {
            by_shard[*shard].push(located.clone());
        }
    }
    let decided = futures::future::try_join_all(
        by_shard
            .into_iter()
            .enumerate()
            .filter(|(_, located)| !located.is_empty())
            .map(|(shard, located)| async move {
                let _permit = acquire(permits).await?;
                shards[shard]
                    .match_parts(namespace, &located, &memos[shard], decider)
                    .await
            }),
    )
    .await?;
    Ok(decided
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .map(|decided| decided.into_iter().flatten().collect()))
}

/// Summaries of the matches in `batch` that page columns can give: those on
/// one shard and one live page, within `max_spans`.
async fn summarize_batch(
    (shards, memos): (&[&TraceDb], &[PageMemo]),
    permits: Option<&tokio::sync::Semaphore>,
    namespace: &Namespace,
    batch: &[CandidateGroup],
    decider: &crate::traceql::columns::Decider,
    max_spans: usize,
) -> Result<HashMap<TraceId, TraceSummary>> {
    let mut by_shard = vec![Vec::new(); shards.len()];
    for group in batch {
        if let [(shard, located)] = group.parts.as_slice() {
            by_shard[*shard].push(located.trace_id);
        }
    }
    let summaries = futures::future::try_join_all(
        by_shard
            .into_iter()
            .enumerate()
            .filter(|(_, trace_ids)| !trace_ids.is_empty())
            .map(|(shard, trace_ids)| async move {
                let _permit = acquire(permits).await?;
                shards[shard]
                    .summarize(namespace, trace_ids, &memos[shard], decider, max_spans)
                    .await
            }),
    )
    .await?;
    Ok(summaries.into_iter().flatten().collect())
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
fn live_locators(locators: &[TraceLocator], now: u64) -> Vec<TraceLocator> {
    locators
        .iter()
        .filter(|locator| !locator.is_expired_at(now))
        .copied()
        .collect()
}

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
