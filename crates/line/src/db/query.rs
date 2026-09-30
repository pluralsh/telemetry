// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: stream selection, page scans and ordered row merging.

use std::collections::binary_heap::PeekMut;

use super::*;

impl LogDb {
    /// Returns stream-label names present in every segment touched by the
    /// inclusive time range.
    pub async fn label_names(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        self.validate_discovery_range(start_ns, end_ns)?;
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            let key = (namespace.clone(), segment);
            let names = if let Some(names) = self.label_names_cache.get(&key) {
                names
            } else {
                let names = catalog_names(
                    self.storage.as_ref(),
                    &segment_prefix(namespace, segment),
                    Some(""),
                )
                .await?;
                self.label_names_cache
                    .insert(key, names, self.is_active_segment(segment))
            };
            result.extend(names.iter().cloned());
        }
        Ok(result.into_iter().collect())
    }

    /// Returns values for one stream label across every touched time segment.
    pub async fn label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        self.validate_discovery_range(start_ns, end_ns)?;
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            let key = (namespace.clone(), segment, name.to_owned());
            let values = if let Some(values) = self.label_values_cache.get(&key) {
                values
            } else {
                let mut strings = Vec::new();
                for value in catalog_values(
                    self.storage.as_ref(),
                    &segment_prefix(namespace, segment),
                    "",
                    name,
                )
                .await?
                {
                    match value {
                        DiscoveryValue::String(value) => strings.push(value),
                        _ => {
                            return Err(Error::Corrupt(
                                "stream-label catalog contains a non-string value".into(),
                            ));
                        }
                    }
                }
                self.label_values_cache
                    .insert(key, strings, self.is_active_segment(segment))
            };
            result.extend(values.iter().cloned());
        }
        Ok(result.into_iter().collect())
    }

    /// Reconstructs stream label sets selected by Loki stream selectors.
    pub async fn series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Labels>> {
        self.validate_discovery_range(start_ns, end_ns)?;
        if selectors.is_empty() {
            return Err(Error::Invalid(
                "series requires at least one match[] selector".into(),
            ));
        }
        let selectors = selectors
            .iter()
            .map(|selector| SeriesSelector::parse(selector))
            .collect::<Result<Vec<_>>>()?;
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            for selector in &selectors {
                let stream_ids = self.stream_ids(namespace, segment, &selector.exact).await?;
                for (_, labels) in self.stream_labels(namespace, segment, stream_ids).await? {
                    if selector.matches(&labels) {
                        result.insert(Arc::unwrap_or_clone(labels));
                    }
                }
            }
        }
        Ok(result.into_iter().collect())
    }

    fn validate_discovery_range(&self, start_ns: i64, end_ns: i64) -> Result<()> {
        if end_ns < start_ns {
            Err(Error::Invalid("end_ns must be >= start_ns".to_owned()))
        } else {
            Ok(())
        }
    }

    pub(super) fn discovery_segments(&self, start_ns: i64, end_ns: i64) -> Result<Vec<SegmentId>> {
        let last = segment_for(end_ns, self.segment_ns);
        let mut segment = segment_for(start_ns, self.segment_ns);
        let mut result = Vec::new();
        loop {
            result.push(segment);
            if segment == last {
                return Ok(result);
            }
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("discovery segment range overflow".into()))?;
        }
    }

    fn is_active_segment(&self, segment: SegmentId) -> bool {
        let now_ns = common::time::now_ns();
        segment_for(now_ns, self.segment_ns) == segment
    }

    /// Reads rows in the inclusive timestamp range matching every exact label.
    pub async fn read(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
    ) -> Result<Vec<LogRow>> {
        let targets = self
            .scan_targets(
                namespace,
                start_ns,
                end_ns,
                &StreamFilter::exact(matchers.to_vec()),
            )
            .await?;
        self.read_bounded(namespace, targets, &PageBudget::new(usize::MAX))
            .await
    }

    /// Lists the selected streams and live overlapping pages of every segment
    /// in `[start_ns, end_ns]`, so the page estimate and the read share one
    /// metadata walk.
    pub(crate) async fn scan_targets(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        filter: &StreamFilter,
    ) -> Result<ScanTargets> {
        let now_unix_ms = unix_time_ms()?;
        let segments = futures::stream::iter(self.segment_ids((start_ns, end_ns), false)?)
            .map(|segment| async move {
                let streams = self
                    .segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
                    .await?;
                Ok::<_, Error>((segment, streams))
            })
            .buffered(SEGMENT_LIST_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(ScanTargets {
            range: (start_ns, end_ns),
            segments,
        })
    }

    /// Segments covering `[start_ns, end_ns]` in scan order.
    fn segment_ids(&self, (start_ns, end_ns): (i64, i64), reverse: bool) -> Result<Vec<SegmentId>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let last_segment = segment_for(end_ns, self.segment_ns);
        let mut segment = segment_for(start_ns, self.segment_ns);
        let mut segments = vec![segment];
        while segment != last_segment {
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
            segments.push(segment);
        }
        if reverse {
            segments.reverse();
        }
        Ok(segments)
    }

    /// Reads every target page, charging each to `budget`. This is the
    /// storage primitive used by the query engine to put a hard bound on I/O.
    pub(crate) async fn read_bounded(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
    ) -> Result<Vec<LogRow>> {
        let mut rows = Vec::new();
        let mut consume = |chunk: Vec<LogRow>| {
            rows.extend(chunk);
            Ok(ControlFlow::Continue(()))
        };
        for (segment, streams) in targets.segments {
            let _: ControlFlow<()> = self
                .read_segment_pages(
                    namespace,
                    segment,
                    streams,
                    targets.range,
                    budget,
                    false,
                    &mut consume,
                )
                .await?;
        }
        Ok(rows)
    }

    /// Reads overlapping pages in scan order, handing `consume` ascending
    /// chunks of rows. Every row in a later chunk is strictly later (or, with
    /// `reverse`, strictly earlier) than every row already consumed, so
    /// `consume` returns `Break` once no later row can matter. Pages are
    /// fetched in order of their nearest timestamp and a row is released only
    /// when no unread page can precede it, so a satisfied consumer stops
    /// mid-segment. Only pages actually fetched are charged to `budget`.
    pub(crate) async fn read_segments(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        mut consume: impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let now_unix_ms = unix_time_ms()?;
        for segment in self.segment_ids((start_ns, end_ns), reverse)? {
            let streams = self
                .segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
                .await?;
            let flow = self
                .read_segment_pages(
                    namespace,
                    segment,
                    streams,
                    (start_ns, end_ns),
                    budget,
                    reverse,
                    &mut consume,
                )
                .await?;
            if flow.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// One segment of [`Self::read_segments`].
    #[allow(clippy::too_many_arguments)]
    async fn read_segment_pages(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: Vec<SegmentStream>,
        (start_ns, end_ns): (i64, i64),
        budget: &PageBudget,
        reverse: bool,
        consume: &mut impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<ControlFlow<()>> {
        let mut pages = Vec::new();
        for stream in streams {
            let fingerprint = stream.labels.fingerprint();
            for (page_id, metadata) in stream.pages {
                let bound = if reverse {
                    metadata.max_timestamp_ns
                } else {
                    metadata.min_timestamp_ns
                };
                pages.push((
                    bound,
                    stream.stream_id,
                    stream.labels.clone(),
                    fingerprint,
                    page_id,
                    metadata.level,
                ));
            }
        }
        pages.sort_unstable_by(|a, b| {
            let bound = if reverse {
                b.0.cmp(&a.0)
            } else {
                a.0.cmp(&b.0)
            };
            bound
                .then(a.1.cmp(&b.1))
                .then(a.4.sequence.cmp(&b.4.sequence))
        });
        let bounds = pages.iter().map(|page| page.0).collect::<Vec<_>>();
        let mut decoded = futures::stream::iter(pages)
            .map(
                move |(_, stream_id, labels, fingerprint, page_id, level)| async move {
                    budget.take()?;
                    let entries = self
                        .load_page(namespace, segment, stream_id, page_id, level)
                        .await?
                        .decode_range(start_ns, end_ns)?;
                    Ok::<_, Error>((labels, fingerprint, page_id, entries))
                },
            )
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = PendingRows::new(reverse);
        let mut fetched = 0;
        while let Some((labels, fingerprint, page_id, entries)) = decoded.try_next().await? {
            fetched += 1;
            for (row_index, entry) in entries.into_iter().enumerate() {
                pending.push(
                    (entry.timestamp_ns, fingerprint, page_id.sequence, row_index),
                    LogRow {
                        labels: labels.clone(),
                        entry,
                    },
                );
            }
            let ready = pending.release(bounds.get(fetched).copied());
            if !ready.is_empty() && consume(ready)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Uses the segment-local term index to identify pages and rows before
    /// payload reads. `None` means at least one segment predates the index and
    /// the caller must use the exact scan fallback.
    pub(crate) async fn read_match_bounded(
        &self,
        namespace: &Namespace,
        targets: &ScanTargets,
        match_plan: (&[String], Option<usize>),
        max_pages: usize,
    ) -> Result<Option<Vec<(LogRow, f32)>>> {
        let (terms, top_k) = match_plan;
        let (start_ns, end_ns) = targets.range;
        let mut rows = Vec::new();
        let mut pages_read = 0usize;

        for (segment, streams) in &targets.segments {
            let segment = *segment;
            let Some(by_page) = self
                .segment_match_scores(namespace, segment, streams, terms, top_k)
                .await?
            else {
                return Ok(None);
            };
            // Collected so no closure is held across an await: that trips
            // rustc's higher-ranked `Send` inference in the HTTP handlers.
            let pages = streams
                .iter()
                .flat_map(|stream| {
                    stream
                        .pages
                        .iter()
                        .map(move |(page_id, metadata)| (stream, *page_id, metadata.level))
                })
                .collect::<Vec<_>>();
            for (stream, page_id, level) in pages {
                let Some(page_scores) = by_page.get(&(stream.stream_id, page_id.sequence)) else {
                    continue;
                };
                if pages_read == max_pages {
                    return Err(Error::Query(format!(
                        "query exceeded max_pages ({max_pages})"
                    )));
                }
                pages_read += 1;
                let page = self
                    .load_page(namespace, segment, stream.stream_id, page_id, level)
                    .await?;
                let matched = page.decode_rows_where(start_ns, end_ns, |row_id| {
                    page_scores.contains_key(&row_id)
                })?;
                // Stored postings are candidates, never authority.
                rows.extend(
                    matched
                        .into_iter()
                        .filter(|(_, entry)| source_matches(&DEFAULT_ANALYZER, &entry.line, terms))
                        .map(|(row_id, entry)| {
                            let row = LogRow {
                                labels: stream.labels.clone(),
                                entry,
                            };
                            (row, page_scores[&row_id])
                        }),
                );
            }
        }
        Ok(Some(rows))
    }

    /// Term-index scores for one segment, keyed by stored page and then by
    /// row within that page. `None` means the segment predates the index.
    async fn segment_match_scores(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: &[SegmentStream],
        terms: &[String],
        top_k: Option<usize>,
    ) -> Result<Option<HashMap<(StreamId, u64), HashMap<u32, f32>>>> {
        // Postings address written pages; a merged page covers several.
        let mut leaves: HashMap<(StreamId, u64), (u64, u32)> = HashMap::new();
        for stream in streams {
            for (page_id, metadata) in &stream.pages {
                leaves.extend(metadata.leaves(page_id.sequence).into_iter().map(
                    |(leaf, first_row)| ((stream.stream_id, leaf), (page_id.sequence, first_row)),
                ));
            }
        }
        let mut by_page: HashMap<(StreamId, u64), HashMap<u32, f32>> = HashMap::new();
        if leaves.is_empty() {
            return Ok(Some(by_page));
        }
        let allowed_pages = leaves.keys().copied().collect::<HashSet<_>>();
        let Some(scores) = block_max_scores(
            self.storage.as_ref(),
            namespace,
            segment,
            terms,
            &allowed_pages,
            top_k,
        )
        .await?
        else {
            return Ok(None);
        };
        for (address, score) in scores {
            let (sequence, first_row) = leaves[&(address.stream_id, address.page_sequence)];
            by_page
                .entry((address.stream_id, sequence))
                .or_default()
                .insert(first_row.saturating_add(address.row_id), score);
        }
        Ok(Some(by_page))
    }

    async fn load_page(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_id: StreamId,
        page_id: PageId,
        level: u8,
    ) -> Result<Page> {
        let payload = self
            .storage
            .get(payload_key(namespace, segment, stream_id, page_id, level))
            .await?
            .ok_or_else(|| Error::Corrupt("page metadata has no payload".to_owned()))?;
        Page::decode(payload.value)
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(handle) = &self.write_handle {
            let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
            self.writer()?.flush().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let coordinator_result =
            if let Some(coordinator) = self.write_coordinator.lock().await.take() {
                coordinator.stop().await.map_err(Error::Invalid)
            } else {
                Ok(())
            };
        let storage_result = self.storage.close().await.map_err(Error::from);
        coordinator_result?;
        storage_result?;
        Ok(())
    }

    pub(super) async fn stream_ids(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        matchers: &[Label],
    ) -> Result<Vec<StreamId>> {
        if matchers.is_empty() {
            let mut result = Vec::new();
            let mut iterator = self
                .storage
                .scan_prefix_iter(
                    forward_prefix(namespace, segment),
                    BytesRange::unbounded(),
                    None,
                )
                .await?;
            while let Some(record) = iterator.next().await? {
                result.push(decode_forward_key(&record.key)?);
            }
            return Ok(result);
        }

        let records = futures::future::try_join_all(
            matchers
                .iter()
                .map(|matcher| self.storage.get(posting_key(namespace, segment, matcher))),
        )
        .await?;
        let mut result: Option<RoaringBitmap> = None;
        for record in records {
            let Some(record) = record else {
                return Ok(Vec::new());
            };
            let bitmap = decode_postings(&record.value)?;
            match &mut result {
                Some(result) => *result &= bitmap,
                None => result = Some(bitmap),
            }
        }
        Ok(result.unwrap_or_default().iter().collect())
    }

    /// Fetches forward labels for `stream_ids` concurrently, in input order.
    async fn stream_labels(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_ids: Vec<StreamId>,
    ) -> Result<Vec<(StreamId, Arc<Labels>)>> {
        futures::stream::iter(stream_ids)
            .map(|stream_id| async move {
                let record = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".to_owned())
                    })?;
                Ok::<_, Error>((stream_id, Arc::new(decode_labels(&record.value)?)))
            })
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Streams in `segment` selected by `filter`, each with its live pages
    /// overlapping `[start_ns, end_ns]`. Labels are checked before any page
    /// metadata is scanned, so non-exact matchers prune streams without I/O.
    async fn segment_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        filter: &StreamFilter,
        (start_ns, end_ns): (i64, i64),
        now_unix_ms: u64,
    ) -> Result<Vec<SegmentStream>> {
        let stream_ids = self.stream_ids(namespace, segment, &filter.exact).await?;
        let labelled = self.stream_labels(namespace, segment, stream_ids).await?;
        futures::stream::iter(
            labelled
                .into_iter()
                .filter(|(_, labels)| filter.matches(labels)),
        )
        .map(|(stream_id, labels)| async move {
            let mut pages = Vec::new();
            let mut metadata = self
                .storage
                .scan_prefix_iter(
                    metadata_prefix(namespace, segment, stream_id),
                    BytesRange::unbounded(),
                    None,
                )
                .await?;
            while let Some(record) = metadata.next().await? {
                let (_, page_id) = decode_metadata_key(&record.key)?;
                let page = decode_metadata(&record.value)?;
                if page.is_expired_at(now_unix_ms)
                    || page.max_timestamp_ns < start_ns
                    || page.min_timestamp_ns > end_ns
                {
                    continue;
                }
                pages.push((page_id, page));
            }
            Ok::<_, Error>(SegmentStream {
                stream_id,
                labels,
                pages,
            })
        })
        .buffered(STREAM_METADATA_CONCURRENCY)
        .try_collect()
        .await
    }
}

struct SegmentStream {
    stream_id: StreamId,
    labels: Arc<Labels>,
    pages: Vec<(PageId, StoredPageMetadata)>,
}

/// Output of [`LogDb::scan_targets`].
pub(crate) struct ScanTargets {
    range: (i64, i64),
    segments: Vec<(SegmentId, Vec<SegmentStream>)>,
}

impl ScanTargets {
    pub(crate) fn estimate(&self) -> QueryEstimate {
        let mut estimate = QueryEstimate::default();
        for page in self
            .segments
            .iter()
            .flat_map(|(_, streams)| streams)
            .flat_map(|stream| &stream.pages)
            .map(|(_, page)| page)
        {
            estimate.pages = estimate.pages.saturating_add(1);
            estimate.compressed_bytes = estimate
                .compressed_bytes
                .saturating_add(u64::from(page.payload_bytes));
            estimate.lines = estimate.lines.saturating_add(u64::from(page.row_count));
        }
        estimate
    }
}

type RowKey = (i64, StreamFingerprint, u64, usize);

/// Decoded rows waiting until no unread page can precede them.
struct PendingRows {
    heap: BinaryHeap<PendingRow>,
    reverse: bool,
}

struct PendingRow {
    key: RowKey,
    reverse: bool,
    row: LogRow,
}

impl PartialEq for PendingRow {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PendingRow {}

impl PartialOrd for PendingRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingRow {
    /// `BinaryHeap` pops the greatest row, which must be the next in scan order.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let order = self.key.cmp(&other.key);
        if self.reverse { order } else { order.reverse() }
    }
}

impl PendingRows {
    fn new(reverse: bool) -> Self {
        Self {
            heap: BinaryHeap::new(),
            reverse,
        }
    }

    fn push(&mut self, key: RowKey, row: LogRow) {
        self.heap.push(PendingRow {
            key,
            reverse: self.reverse,
            row,
        });
    }

    /// Pops rows strictly before `next_bound` (the nearest timestamp any
    /// unread page may hold) in ascending order; `None` releases everything.
    fn release(&mut self, next_bound: Option<i64>) -> Vec<LogRow> {
        let mut ready = Vec::new();
        while let Some(top) = self.heap.peek_mut() {
            let timestamp = top.key.0;
            let safe = match next_bound {
                None => true,
                Some(bound) if self.reverse => timestamp > bound,
                Some(bound) => timestamp < bound,
            };
            if !safe {
                break;
            }
            ready.push(PeekMut::pop(top).row);
        }
        if self.reverse {
            ready.reverse();
        }
        ready
    }
}

/// Stream selection for a query: postings intersect `exact`, then a stream is
/// kept when any selector matches its labels (or when there are none).
pub(crate) struct StreamFilter {
    exact: Vec<Label>,
    selectors: Vec<SeriesSelector>,
}

impl StreamFilter {
    pub(crate) fn exact(exact: Vec<Label>) -> Self {
        Self {
            exact,
            selectors: Vec::new(),
        }
    }

    pub(crate) fn new<'a>(
        exact: Vec<Label>,
        selectors: impl IntoIterator<Item = &'a [crate::logql::Spanned<crate::logql::Matcher>]>,
    ) -> Result<Self> {
        Ok(Self {
            exact,
            selectors: selectors
                .into_iter()
                .map(SeriesSelector::from_matchers)
                .collect::<Result<_>>()?,
        })
    }

    fn matches(&self, labels: &Labels) -> bool {
        self.selectors.is_empty()
            || self
                .selectors
                .iter()
                .any(|selector| selector.matches(labels))
    }
}

struct SeriesSelector {
    exact: Vec<Label>,
    matchers: Vec<SeriesMatcher>,
}

enum SeriesMatcher {
    Equal(String, String),
    NotEqual(String, String),
    Regex(String, regex::Regex),
    NotRegex(String, regex::Regex),
}

impl SeriesSelector {
    fn parse(source: &str) -> Result<Self> {
        use crate::logql::Expr;

        let query = crate::logql::parse(source).map_err(|error| Error::Query(error.to_string()))?;
        let Expr::Log(log) = query.value else {
            return Err(Error::Invalid(
                "series match[] must be a stream selector".into(),
            ));
        };
        if !log.stages.is_empty() || log.range.is_some() || log.offset.is_some() {
            return Err(Error::Invalid(
                "series match[] must contain only a stream selector".into(),
            ));
        }
        Self::from_matchers(&log.selector.value.matchers)
    }

    fn from_matchers(source: &[crate::logql::Spanned<crate::logql::Matcher>]) -> Result<Self> {
        use crate::logql::MatchOp;

        let mut exact = Vec::new();
        let mut matchers = Vec::with_capacity(source.len());
        for matcher in source {
            let name = matcher.value.label.clone();
            let value = matcher.value.value.clone();
            match matcher.value.op {
                MatchOp::Equal => {
                    exact.push(Label::new(&name, &value));
                    matchers.push(SeriesMatcher::Equal(name, value));
                }
                MatchOp::NotEqual => {
                    matchers.push(SeriesMatcher::NotEqual(name, value));
                }
                MatchOp::Regex => {
                    matchers.push(SeriesMatcher::Regex(name, anchored_regex(&value)?));
                }
                MatchOp::NotRegex => {
                    matchers.push(SeriesMatcher::NotRegex(name, anchored_regex(&value)?));
                }
            }
        }
        Ok(Self { exact, matchers })
    }

    fn matches(&self, labels: &Labels) -> bool {
        self.matchers.iter().all(|matcher| {
            let (name, expected) = match matcher {
                SeriesMatcher::Equal(name, value) | SeriesMatcher::NotEqual(name, value) => {
                    (name, Some(value))
                }
                SeriesMatcher::Regex(name, _) | SeriesMatcher::NotRegex(name, _) => (name, None),
            };
            let actual = labels
                .iter()
                .find(|label| label.name == *name)
                .map_or("", |label| label.value.as_str());
            match (matcher, expected) {
                (SeriesMatcher::Equal(_, _), Some(expected)) => actual == expected,
                (SeriesMatcher::NotEqual(_, _), Some(expected)) => actual != expected,
                (SeriesMatcher::Regex(_, regex), None) => regex.is_match(actual),
                (SeriesMatcher::NotRegex(_, regex), None) => !regex.is_match(actual),
                _ => unreachable!(),
            }
        })
    }
}

fn anchored_regex(pattern: &str) -> Result<regex::Regex> {
    Ok(regex::Regex::new(&format!("^(?:{pattern})$"))?)
}
