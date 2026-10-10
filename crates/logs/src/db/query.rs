// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: stream selection, object block reads and ordered row merging.

use std::collections::binary_heap::PeekMut;
use std::ops::{Bound, Range};

use common::storage::ReadHints;

use super::*;
use crate::codec::{
    StoredRun, decode_object, decode_rollup_forward_key, decode_run, decode_run_key, directory_key,
    segment_run_prefix, stream_run_prefix,
};
use crate::object::{
    Block, ReadNeeds, RowSample, block_header, decode_block_rows, decode_block_where,
    decode_samples_where, read_blocks,
};
use crate::query::{BatchSlice, ColumnBatch, StreamBatches, Tie};
use crate::search::{self, TermStats};

#[cfg(test)]
#[path = "profile.rs"]
mod profile;

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
        let per_partition: Vec<_> =
            futures::stream::iter(self.discovery_partitions(start_ns, end_ns)?)
                .map(|partition| self.partition_label_names(namespace, partition))
                .buffer_unordered(SEGMENT_DISCOVERY_CONCURRENCY)
                .try_collect()
                .await?;
        Ok(sorted_union(&per_partition))
    }

    /// Sorted stream-label names written to `partition`.
    async fn partition_label_names(
        &self,
        namespace: &Namespace,
        partition: Partition,
    ) -> Result<Arc<Versioned<Vec<String>>>> {
        let version = self.partition_version(namespace, partition).await?;
        self.partition_label_names_at(namespace, partition, version)
            .await
    }

    /// [`Self::partition_label_names`] with the partition's version already
    /// read.
    async fn partition_label_names_at(
        &self,
        namespace: &Namespace,
        partition: Partition,
        version: PartitionVersion,
    ) -> Result<Arc<Versioned<Vec<String>>>> {
        let cache = &self.caches.label_names;
        let key = (namespace.clone(), partition);
        if let Some(names) = cache.get(&key).filter(|cached| cached.0 == version) {
            return Ok(names);
        }
        let names = catalog_names(
            self.storage.as_ref(),
            &catalog_prefix(namespace, partition),
            Some(""),
        )
        .await?;
        Ok(cache.insert(key, (version, names), false))
    }

    /// The stream counter of `partition`, read before any of its discovery
    /// records so a scan taken after it covers every stream it counts.
    pub(super) async fn partition_version(
        &self,
        namespace: &Namespace,
        partition: Partition,
    ) -> Result<PartitionVersion> {
        let key = match partition {
            Partition::Segment(segment) => next_stream_id_key(namespace, segment),
            Partition::Rollup(period) => rollup_next_stream_id_key(namespace, period),
        };
        self.storage
            .get(key)
            .await?
            .map(|record| decode_stream_id(&record.value))
            .transpose()
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
        let per_partition: Vec<_> =
            futures::stream::iter(self.discovery_partitions(start_ns, end_ns)?)
                .map(|partition| self.partition_label_values(namespace, partition, name))
                .buffer_unordered(SEGMENT_DISCOVERY_CONCURRENCY)
                .try_collect()
                .await?;
        Ok(sorted_union(&per_partition))
    }

    /// Sorted values of stream label `name` written to `partition`.
    async fn partition_label_values(
        &self,
        namespace: &Namespace,
        partition: Partition,
        name: &str,
    ) -> Result<Arc<Versioned<Vec<String>>>> {
        let version = self.partition_version(namespace, partition).await?;
        let cache = &self.caches.label_values;
        let key = (namespace.clone(), partition, name.to_owned());
        if let Some(values) = cache.get(&key).filter(|cached| cached.0 == version) {
            return Ok(values);
        }
        let values = catalog_values(
            self.storage.as_ref(),
            &catalog_prefix(namespace, partition),
            "",
            name,
        )
        .await?
        .into_iter()
        .map(|value| match value {
            DiscoveryValue::String(value) => Ok(value),
            _ => Err(Error::Corrupt(
                "stream-label catalog contains a non-string value".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
        Ok(cache.insert(key, (version, values), false))
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
        let exact_sets = selectors
            .iter()
            .map(|selector| selector.exact.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let exact_sets = &exact_sets;
        let per_partition: Vec<Vec<(Vec<Label>, SegmentSeries)>> =
            futures::stream::iter(self.discovery_partitions(start_ns, end_ns)?)
                .map(|partition| async move {
                    let version = self.partition_version(namespace, partition).await?;
                    let names = self
                        .partition_label_names_at(namespace, partition, version)
                        .await?;
                    let names = &names.1;
                    let mut selected = Vec::with_capacity(exact_sets.len());
                    for exact in exact_sets {
                        // The catalog is written in the same batch as the
                        // postings, so a missing name rules the partition
                        // out.
                        if names.is_empty()
                            || exact
                                .iter()
                                .any(|label| names.binary_search(&label.name).is_err())
                        {
                            continue;
                        }
                        let streams = self
                            .partition_series(namespace, partition, version, exact)
                            .await?;
                        selected.push((exact.clone(), streams));
                    }
                    Ok::<_, Error>(selected)
                })
                .buffer_unordered(SEGMENT_LIST_CONCURRENCY)
                .try_collect()
                .await?;
        let mut result = BTreeSet::new();
        for (exact, streams) in per_partition.iter().flatten() {
            for selector in selectors.iter().filter(|selector| selector.exact == *exact) {
                result.extend(
                    streams
                        .1
                        .iter()
                        .filter(|labels| selector.matches(labels))
                        .map(Arc::clone),
                );
            }
        }
        Ok(result.into_iter().map(Arc::unwrap_or_clone).collect())
    }

    /// Label sets of the streams in `partition` holding every `exact` label.
    async fn partition_series(
        &self,
        namespace: &Namespace,
        partition: Partition,
        version: PartitionVersion,
        exact: &[Label],
    ) -> Result<SegmentSeries> {
        let cache = &self.caches.series;
        let key = (namespace.clone(), partition, exact.to_vec());
        if let Some(streams) = cache.get(&key).filter(|cached| cached.0 == version) {
            return Ok(streams);
        }
        let streams = match partition {
            Partition::Segment(segment) => {
                let stream_ids = self.stream_ids(namespace, segment, exact).await?;
                self.stream_labels(namespace, segment, stream_ids)
                    .await?
                    .into_iter()
                    .map(|(_, labels)| labels)
                    .collect::<Vec<_>>()
            }
            Partition::Rollup(period) => self.rollup_series(namespace, period, exact).await?,
        };
        if streams.len() > SERIES_CACHE_MAX_STREAMS {
            return Ok(Arc::new((version, streams)));
        }
        Ok(cache.insert(key, (version, streams), false))
    }

    /// [`Self::partition_series`] for a rollup period. A posting can name a
    /// stream whose forward labels expired after a later write of another
    /// stream refreshed it; that stream's segment records expired with its
    /// own last write, so it is skipped here too.
    async fn rollup_series(
        &self,
        namespace: &Namespace,
        period: SegmentId,
        exact: &[Label],
    ) -> Result<Vec<Arc<Labels>>> {
        let forward = rollup_forward_prefix(namespace, period);
        let prefix_len = forward.len();
        let scan = |range| {
            self.scan_labels(forward.clone(), range, |key| {
                decode_rollup_forward_key(key, prefix_len)
            })
        };
        if exact.is_empty() {
            let labelled = scan(BytesRange::unbounded()).await?;
            return Ok(labelled.into_iter().map(|(_, labels)| labels).collect());
        }
        let records = futures::future::try_join_all(exact.iter().map(|label| {
            self.storage
                .get(rollup_posting_key(namespace, period, label))
        }))
        .await?;
        let mut selected: Option<RoaringBitmap> = None;
        for record in records {
            let Some(record) = record else {
                return Ok(Vec::new());
            };
            let bitmap = decode_postings(&record.value)?;
            match &mut selected {
                Some(selected) => *selected &= bitmap,
                None => selected = Some(bitmap),
            }
        }
        let ids = selected.unwrap_or_default().iter().collect::<Vec<_>>();
        if let Some(span) = self.span_scan(&ids) {
            let labelled = scan(span).await?;
            return Ok(labelled
                .into_iter()
                .filter(|(id, _)| ids.binary_search(id).is_ok())
                .map(|(_, labels)| labels)
                .collect());
        }
        let labelled: Vec<Option<Arc<Labels>>> = futures::stream::iter(ids)
            .map(|id| async move {
                self.storage
                    .get(rollup_forward_key(namespace, period, id))
                    .await?
                    .map(|record| decode_labels(&record.value).map(Arc::new))
                    .transpose()
            })
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(labelled.into_iter().flatten().collect())
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

    /// Partitions covering exactly the segments [`Self::discovery_segments`]
    /// returns: each rollup period whose segments all fall inside the range,
    /// and the remaining segments individually.
    pub(super) fn discovery_partitions(
        &self,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Partition>> {
        let segments = self.discovery_segments(start_ns, end_ns)?;
        let Some(rollup_ns) = self.rollup_ns else {
            return Ok(segments.into_iter().map(Partition::Segment).collect());
        };
        let per_period = usize::try_from(rollup_ns / self.segment_ns)
            .map_err(|_| Error::Invalid("discovery rollup period is too long".into()))?;
        let mut partitions = Vec::new();
        let mut index = 0;
        while let Some(&segment) = segments.get(index) {
            if segment_for(segment, rollup_ns) == segment && segments.len() - index >= per_period {
                partitions.push(Partition::Rollup(segment));
                index += per_period;
            } else {
                partitions.push(Partition::Segment(segment));
                index += 1;
            }
        }
        Ok(partitions)
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
            line_terms: filter.line_terms.clone(),
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
        self.read_bounded_with(namespace, targets, budget, |chunk| {
            rows.extend(chunk);
            Ok(())
        })
        .await?;
        Ok(rows)
    }

    /// [`Self::read_bounded`], handing each decoded chunk to `consume` so the
    /// caller keeps only the rows it needs.
    pub(crate) async fn read_bounded_with(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
        consume: impl FnMut(Vec<LogRow>) -> Result<()>,
    ) -> Result<()> {
        self.read_bounded_pending::<PendingRows>(namespace, targets, budget, consume)
            .await
    }

    /// [`Self::read_bounded_with`], handing over slices of per-run column
    /// batches instead of rows.
    pub(crate) async fn read_bounded_columns(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
        consume: impl FnMut(Vec<BatchSlice>) -> Result<()>,
    ) -> Result<()> {
        self.read_bounded_pending::<PendingBatches>(namespace, targets, budget, consume)
            .await
    }

    async fn read_bounded_pending<P: PendingReads>(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
        mut consume: impl FnMut(P::Chunk) -> Result<()>,
    ) -> Result<()> {
        let mut consume = |chunk: P::Chunk| {
            consume(chunk)?;
            Ok(ControlFlow::Continue(()))
        };
        for (ordinal, (segment, streams)) in targets.segments.into_iter().enumerate() {
            let _: ControlFlow<()> = self
                .read_segment_pages::<P>(
                    namespace,
                    (segment, ordinal),
                    streams,
                    targets.range,
                    budget,
                    targets.line_terms.as_deref(),
                    false,
                    &mut consume,
                )
                .await?;
        }
        Ok(())
    }

    /// [`Self::read_segments_columns`], handing over rows.
    #[cfg(test)]
    pub(crate) async fn read_segments(
        &self,
        namespace: &Namespace,
        range: (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        consume: impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        self.read_segments_pending::<PendingRows>(
            namespace, range, filter, budget, reverse, consume,
        )
        .await
    }

    /// Reads overlapping pages in scan order, handing `consume` ascending
    /// chunks of rows as slices of per-run column batches. Every row in a
    /// later chunk is strictly later (or, with `reverse`, strictly earlier)
    /// than every row already consumed, so `consume` returns `Break` once no
    /// later row can matter. Pages are fetched in order of their nearest
    /// timestamp and a row is released only when no unread page can precede
    /// it, so a satisfied consumer stops mid-segment. Only pages actually
    /// fetched are charged to `budget`.
    pub(crate) async fn read_segments_columns(
        &self,
        namespace: &Namespace,
        range: (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        consume: impl FnMut(Vec<BatchSlice>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        self.read_segments_pending::<PendingBatches>(
            namespace, range, filter, budget, reverse, consume,
        )
        .await
    }

    async fn read_segments_pending<P: PendingReads>(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        mut consume: impl FnMut(P::Chunk) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let now_unix_ms = unix_time_ms()?;
        let select_streams = |segment| {
            self.segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
        };
        let mut segments = self
            .segment_ids((start_ns, end_ns), reverse)?
            .into_iter()
            .enumerate();
        let Some(mut segment) = segments.next() else {
            return Ok(());
        };
        let mut streams = select_streams(segment.1).await?;
        loop {
            let read = self.read_segment_pages::<P>(
                namespace,
                (segment.1, segment.0),
                streams,
                (start_ns, end_ns),
                budget,
                filter.line_terms.as_deref(),
                reverse,
                &mut consume,
            );
            let Some(next) = segments.next() else {
                return read.await.map(drop);
            };
            // The next segment's streams are selected while this one's pages
            // are read.
            match unless_break(read, select_streams(next.1)).await? {
                Some(next_streams) => (segment, streams) = (next, next_streams),
                None => return Ok(()),
            }
        }
    }

    /// One segment of [`Self::read_segments_columns`]; `ordinal` is its position
    /// in the read.
    #[allow(clippy::too_many_arguments)]
    async fn read_segment_pages<P: PendingReads>(
        &self,
        namespace: &Namespace,
        (segment, ordinal): (SegmentId, usize),
        streams: Vec<SegmentStream>,
        (start_ns, end_ns): (i64, i64),
        budget: &PageBudget,
        line_terms: Option<&[String]>,
        reverse: bool,
        consume: &mut impl FnMut(P::Chunk) -> Result<ControlFlow<()>>,
    ) -> Result<ControlFlow<()>> {
        let ordinal = u32::try_from(ordinal).unwrap_or(u32::MAX);
        let mut streams = streams;
        let candidates = match line_terms {
            Some(terms) => {
                self.line_candidates(namespace, segment, &streams, terms)
                    .await?
            }
            None => None,
        };
        if let Some(candidates) = &candidates {
            for stream in &mut streams {
                stream.runs.retain(|(object_id, _)| {
                    candidates.contains_key(&(stream.stream_id, *object_id))
                });
            }
            streams.retain(|stream| !stream.runs.is_empty());
        }
        let candidates = &candidates;
        let units = plan_reads(&streams, reverse, COALESCE_GAP_BLOCKS);
        let bounds = units.iter().map(|unit| unit.bound).collect::<Vec<_>>();
        let mut decoded = futures::stream::iter(units)
            .map(move |unit| async move {
                budget.take()?;
                let blocks = self
                    .load_blocks(
                        namespace,
                        segment,
                        unit.object,
                        unit.first_block..unit.end_block,
                        ReadNeeds { lines: true },
                    )
                    .await?;
                let mut runs = Vec::with_capacity(unit.runs.len());
                for run in unit.runs {
                    let first = (run.run.first_block - unit.first_block) as usize;
                    let blocks = &blocks[first..first + run.run.blocks as usize];
                    let rows = candidates
                        .as_ref()
                        .and_then(|candidates| candidates.get(&(run.stream_id, run.object_id)));
                    let decoded = P::decode(&run, blocks, (start_ns, end_ns), rows, ordinal)?;
                    runs.push((run, decoded));
                }
                Ok::<_, Error>(runs)
            })
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = P::new(reverse);
        let mut fetched = 0;
        while let Some(runs) = decoded.try_next().await? {
            fetched += 1;
            for (run, decoded) in runs {
                pending.push(run, decoded);
            }
            if let Some(ready) = pending.release(bounds.get(fetched).copied())
                && consume(ready)?.is_break()
            {
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
            let Some(by_run) = self
                .segment_match_scores(namespace, segment, streams, terms, top_k)
                .await?
            else {
                return Ok(None);
            };
            // Collected so no closure is held across an await: that trips
            // rustc's higher-ranked `Send` inference in the HTTP handlers.
            let runs = streams
                .iter()
                .flat_map(|stream| {
                    stream
                        .runs
                        .iter()
                        .map(move |(object_id, run)| (stream, *object_id, *run))
                })
                .collect::<Vec<_>>();
            let runs = runs
                .into_iter()
                .filter_map(|(stream, object_id, run)| {
                    let scores = by_run.get(&(stream.stream_id, object_id))?;
                    Some((stream, object_id, run, scores))
                })
                .collect::<Vec<_>>();
            pages_read = pages_read.saturating_add(runs.len());
            if pages_read > max_pages {
                return Err(Error::Query(format!(
                    "query exceeded max_pages ({max_pages})"
                )));
            }
            let mut loaded = Vec::with_capacity(runs.len());
            for chunk in runs.chunks(PAGE_READ_CONCURRENCY) {
                let mut reads = Vec::with_capacity(chunk.len());
                for &(_, object_id, run, _) in chunk {
                    reads.push(self.load_blocks(
                        namespace,
                        segment,
                        ObjectRef {
                            id: object_id,
                            level: run.level,
                        },
                        run.first_block..run.first_block + run.blocks,
                        ReadNeeds { lines: true },
                    ));
                }
                loaded.extend(futures::future::try_join_all(reads).await?);
            }
            for ((stream, _, _, run_scores), blocks) in runs.into_iter().zip(loaded) {
                let matched = run_rows_where(&blocks, (start_ns, end_ns), |row_id| {
                    run_scores.contains_key(&row_id)
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
                            (row, run_scores[&row_id])
                        }),
                );
            }
        }
        Ok(Some(rows))
    }

    /// Term-index scores for one segment, keyed by `(stream, object)` run and
    /// then by row within that run. `None` means the segment predates the
    /// index.
    async fn segment_match_scores(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: &[SegmentStream],
        terms: &[String],
        top_k: Option<usize>,
    ) -> Result<Option<HashMap<(StreamId, u64), HashMap<u32, f32>>>> {
        let leaves = self.run_leaves(namespace, segment, streams).await?;
        let mut by_run: HashMap<(StreamId, u64), HashMap<u32, f32>> = HashMap::new();
        if leaves.is_empty() {
            return Ok(Some(by_run));
        }
        let allowed_leaves = leaves.keys().copied().collect::<HashSet<_>>();
        let Some(scores) = block_max_scores(
            self.storage.as_ref(),
            namespace,
            segment,
            terms,
            &allowed_leaves,
            top_k,
        )
        .await?
        else {
            return Ok(None);
        };
        for (address, score) in scores {
            let (object_id, first_row) = leaves[&(address.stream_id, address.leaf)];
            by_run
                .entry((address.stream_id, object_id))
                .or_default()
                .insert(first_row.saturating_add(address.row_id), score);
        }
        Ok(Some(by_run))
    }

    /// Rows of the selected runs whose lines may hold every one of `terms`,
    /// keyed by `(stream, object)` run, from the postings of the rarest term;
    /// runs without a candidate are absent. `None` when postings would not
    /// narrow the read: the segment predates the index, or the rarest term
    /// is in more than a fifth of its rows or has more posting blocks than
    /// the selected runs have blocks.
    async fn line_candidates(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: &[SegmentStream],
        terms: &[String],
    ) -> Result<Option<HashMap<(StreamId, u64), HashSet<u32>>>> {
        let storage = self.storage.as_ref();
        let Some(field) = search::field_stats(storage, namespace, segment).await? else {
            return Ok(None);
        };
        let stats = futures::future::try_join_all(
            terms
                .iter()
                .map(|term| search::term_stats(storage, namespace, segment, term)),
        )
        .await?;
        let mut rarest = None;
        for (term, stats) in terms.iter().zip(stats) {
            match stats {
                Some(stats) if stats.documents > 0 => {
                    if rarest.is_none_or(|(_, rarest): (_, TermStats)| {
                        stats.documents < rarest.documents
                    }) {
                        rarest = Some((term, stats));
                    }
                }
                // Postings are written with the rows they index.
                _ => return Ok(Some(HashMap::new())),
            }
        }
        let Some((term, stats)) = rarest else {
            return Ok(None);
        };
        let blocks = streams
            .iter()
            .flat_map(|stream| &stream.runs)
            .map(|(_, run)| u64::from(run.blocks))
            .sum::<u64>();
        if stats.documents.saturating_mul(5) > field.documents || u64::from(stats.blocks) > blocks {
            return Ok(None);
        }
        let leaves = self.run_leaves(namespace, segment, streams).await?;
        let mut by_run: HashMap<(StreamId, u64), HashSet<u32>> = HashMap::new();
        for posting in search::term_postings(storage, namespace, segment, term, stats).await? {
            let address = posting.address;
            if let Some(&(object_id, first_row)) = leaves.get(&(address.stream_id, address.leaf)) {
                by_run
                    .entry((address.stream_id, object_id))
                    .or_default()
                    .insert(first_row.saturating_add(address.row_id));
            }
        }
        Ok(Some(by_run))
    }

    /// Every leaf of the selected runs, `(stream, leaf object)` to the run's
    /// object and the leaf's first row in it. Postings address written
    /// objects; a merged run covers several.
    async fn run_leaves(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: &[SegmentStream],
    ) -> Result<HashMap<(StreamId, u64), (u64, u32)>> {
        let merged = streams
            .iter()
            .flat_map(|stream| &stream.runs)
            .filter(|(_, run)| run.level > 0)
            .map(|(id, run)| ObjectRef {
                id: *id,
                level: run.level,
            })
            .collect::<BTreeSet<_>>();
        let directories: HashMap<ObjectRef, StoredObject> = futures::stream::iter(merged)
            .map(|object| async move {
                let record = self
                    .storage
                    .get(directory_key(namespace, segment, object))
                    .await?
                    .ok_or_else(|| Error::Corrupt("run has no object directory".to_owned()))?;
                Ok::<_, Error>((object, decode_object(&record.value)?))
            })
            .buffer_unordered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await?;
        let mut leaves: HashMap<(StreamId, u64), (u64, u32)> = HashMap::new();
        for stream in streams {
            for (object_id, run) in &stream.runs {
                if run.level == 0 {
                    leaves.insert((stream.stream_id, *object_id), (*object_id, 0));
                    continue;
                }
                let directory = &directories[&ObjectRef {
                    id: *object_id,
                    level: run.level,
                }];
                let entry = directory
                    .runs
                    .binary_search_by_key(&stream.stream_id, |entry| entry.stream_id)
                    .map(|index| &directory.runs[index])
                    .map_err(|_| Error::Corrupt("object directory is missing a run".to_owned()))?;
                let mut first_row = 0u32;
                for leaf in StoredObject::leaves_of(entry, *object_id) {
                    leaves.insert((stream.stream_id, leaf.object_id), (*object_id, first_row));
                    first_row = first_row.saturating_add(leaf.rows);
                }
            }
        }
        Ok(leaves)
    }

    pub(super) async fn load_blocks(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        range: Range<u32>,
        needs: ReadNeeds,
    ) -> Result<Vec<Block>> {
        let (database, first) = (self.database_id, range.start);
        if let Some(blocks) = self.blocks.get(
            database,
            namespace,
            segment,
            object,
            range.clone(),
            needs.lines,
        ) {
            return Ok(blocks);
        }
        let blocks = read_blocks(
            self.storage.as_ref(),
            namespace,
            segment,
            object,
            range,
            needs,
            ReadHints::default(),
        )
        .await?;
        self.blocks
            .insert(database, namespace, segment, object, first, &blocks);
        Ok(blocks)
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

    /// Forward labels for `stream_ids` (ascending), in input order.
    async fn stream_labels(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_ids: Vec<StreamId>,
    ) -> Result<Vec<(StreamId, Arc<Labels>)>> {
        if let Some(span) = self.span_scan(&stream_ids) {
            let mut labelled = self
                .scan_labels(forward_prefix(namespace, segment), span, decode_forward_key)
                .await?;
            labelled.retain(|(stream_id, _)| stream_ids.binary_search(stream_id).is_ok());
            if labelled.len() != stream_ids.len() {
                return Err(Error::Corrupt(
                    "posting references missing forward labels".to_owned(),
                ));
            }
            return Ok(labelled);
        }
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

    /// Streams in `segment` selected by `filter`, each with its live runs
    /// overlapping `[start_ns, end_ns]`. Labels are checked before any run
    /// record is scanned, so non-exact matchers prune streams without I/O.
    async fn segment_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        filter: &StreamFilter,
        (start_ns, end_ns): (i64, i64),
        now_unix_ms: u64,
    ) -> Result<Vec<SegmentStream>> {
        let live = |run: &StoredRun| {
            !run.is_expired_at(now_unix_ms)
                && run.max_timestamp_ns >= start_ns
                && run.min_timestamp_ns <= end_ns
        };
        let labelled = self
            .exact_streams(namespace, segment, &filter.exact)
            .await?
            .1
            .iter()
            .filter(|(_, labels)| filter.matches(labels))
            .map(|(stream_id, labels)| (*stream_id, Arc::clone(labels)))
            .collect::<Vec<_>>();
        let stream_ids = labelled.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        if let Some(span) = self.span_scan(&stream_ids) {
            let mut runs = self
                .spanned_runs(namespace, segment, span, &stream_ids, live)
                .await?;
            return Ok(labelled
                .into_iter()
                .map(|(stream_id, labels)| SegmentStream {
                    stream_id,
                    labels,
                    runs: runs.remove(&stream_id).unwrap_or_default(),
                })
                .collect());
        }
        futures::stream::iter(labelled)
            .map(|(stream_id, labels)| async move {
                let runs = self
                    .stream_runs(namespace, segment, stream_id, live)
                    .await?;
                Ok::<_, Error>(SegmentStream {
                    stream_id,
                    labels,
                    runs,
                })
            })
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Streams of `segment` holding every `exact` label, in ID order, cached
    /// while the segment's stream counter is unchanged.
    async fn exact_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        exact: &[Label],
    ) -> Result<SegmentStreams> {
        let version = self
            .partition_version(namespace, Partition::Segment(segment))
            .await?;
        let key = (namespace.clone(), segment, exact.to_vec());
        if let Some(streams) = self
            .caches
            .streams
            .get(&key)
            .filter(|cached| cached.0 == version)
        {
            return Ok(streams);
        }
        let labelled = if exact.is_empty() {
            self.scan_labels(
                forward_prefix(namespace, segment),
                BytesRange::unbounded(),
                decode_forward_key,
            )
            .await?
        } else {
            let stream_ids = self.stream_ids(namespace, segment, exact).await?;
            self.stream_labels(namespace, segment, stream_ids).await?
        };
        let streams = Arc::new((version, labelled));
        self.caches.streams.insert(key, Arc::clone(&streams));
        Ok(streams)
    }

    /// Runs of `stream_ids` (ascending) passing `keep`, from one scan over
    /// `span` of the segment's run records.
    async fn spanned_runs(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        span: BytesRange,
        stream_ids: &[StreamId],
        keep: impl Fn(&StoredRun) -> bool,
    ) -> Result<HashMap<StreamId, Vec<(u64, StoredRun)>>> {
        let mut runs = HashMap::<StreamId, Vec<_>>::new();
        let mut records = self
            .storage
            .scan_prefix_iter(segment_run_prefix(namespace, segment), span, None)
            .await?;
        while let Some(record) = records.next().await? {
            let (stream_id, object_id) = decode_run_key(&record.key)?;
            if stream_ids.binary_search(&stream_id).is_err() {
                continue;
            }
            let run = decode_run(&record.value)?;
            if keep(&run) {
                runs.entry(stream_id).or_default().push((object_id, run));
            }
        }
        Ok(runs)
    }

    /// Runs of one stream passing `keep`, in object order.
    pub(super) async fn stream_runs(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_id: StreamId,
        keep: impl Fn(&StoredRun) -> bool,
    ) -> Result<Vec<(u64, StoredRun)>> {
        let mut runs = Vec::new();
        let mut records = self
            .storage
            .scan_prefix_iter(
                stream_run_prefix(namespace, segment, stream_id),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while let Some(record) = records.next().await? {
            let (_, object_id) = decode_run_key(&record.key)?;
            let run = decode_run(&record.value)?;
            if keep(&run) {
                runs.push((object_id, run));
            }
        }
        Ok(runs)
    }

    /// Forward-label records under `prefix` in `range`, with stream IDs
    /// decoded from their keys by `decode_id`.
    async fn scan_labels(
        &self,
        prefix: Bytes,
        range: BytesRange,
        decode_id: impl Fn(&[u8]) -> Result<StreamId>,
    ) -> Result<Vec<(StreamId, Arc<Labels>)>> {
        let mut iterator = self.storage.scan_prefix_iter(prefix, range, None).await?;
        let mut labelled = Vec::new();
        while let Some(record) = iterator.next().await? {
            labelled.push((
                decode_id(&record.key)?,
                Arc::new(decode_labels(&record.value)?),
            ));
        }
        Ok(labelled)
    }

    /// The stream-ID span of `stream_ids` (ascending) when one range scan
    /// over it is cheaper than a point operation per stream. A scanned
    /// record costs about an eighth of a point operation; IDs are allocated
    /// sequentially per segment, so selectors matching many streams cover
    /// dense spans.
    fn span_scan(&self, stream_ids: &[StreamId]) -> Option<BytesRange> {
        let (&first, &last) = (stream_ids.first()?, stream_ids.last()?);
        let spread = (last - first) as usize + 1;
        if stream_ids.len() < self.span_scan_min_streams
            || spread > stream_ids.len().saturating_mul(SPAN_SCAN_MAX_SPREAD)
        {
            return None;
        }
        let end = match last.checked_add(1) {
            Some(next) => Bound::Excluded(Bytes::copy_from_slice(&next.to_be_bytes())),
            None => Bound::Unbounded,
        };
        Some(BytesRange::new(
            Bound::Included(Bytes::copy_from_slice(&first.to_be_bytes())),
            end,
        ))
    }
}

/// Prefix the discovery catalog of `partition` is written under.
fn catalog_prefix(namespace: &Namespace, partition: Partition) -> Bytes {
    match partition {
        Partition::Segment(segment) => segment_prefix(namespace, segment),
        Partition::Rollup(period) => rollup_prefix(namespace, period),
    }
}

struct SegmentStream {
    stream_id: StreamId,
    labels: Arc<Labels>,
    /// Live overlapping runs with their object IDs.
    runs: Vec<(u64, StoredRun)>,
}

/// A selected run, as read by one [`ReadUnit`].
struct UnitRun {
    stream_id: StreamId,
    labels: Arc<Labels>,
    fingerprint: StreamFingerprint,
    object_id: u64,
    run: StoredRun,
    /// See [`repeat_ranges`]; shared by the stream's runs in the read.
    repeats: Arc<[(i64, i64)]>,
}

/// Blocks `[first_block, end_block)` of one object, covering every run in
/// `runs`: selected runs of an object separated by at most
/// [`COALESCE_GAP_BLOCKS`] are read together.
struct ReadUnit {
    /// Nearest timestamp in scan order of any of its runs.
    bound: i64,
    object: ObjectRef,
    first_block: u32,
    end_block: u32,
    runs: Vec<UnitRun>,
}

/// Unselected blocks a read unit may span between two selected runs: a
/// scanned block costs far less than a separate read.
const COALESCE_GAP_BLOCKS: u32 = 8;

/// Read units of a segment's selected runs, in scan order of their bounds.
fn plan_reads(streams: &[SegmentStream], reverse: bool, max_gap_blocks: u32) -> Vec<ReadUnit> {
    let mut by_object = BTreeMap::<ObjectRef, Vec<UnitRun>>::new();
    for stream in streams {
        let fingerprint = stream.labels.fingerprint();
        let repeats = repeat_ranges(&stream.runs);
        for &(object_id, run) in &stream.runs {
            by_object
                .entry(ObjectRef {
                    id: object_id,
                    level: run.level,
                })
                .or_default()
                .push(UnitRun {
                    stream_id: stream.stream_id,
                    labels: Arc::clone(&stream.labels),
                    fingerprint,
                    object_id,
                    run,
                    repeats: Arc::clone(&repeats),
                });
        }
    }
    let nearest = |run: &StoredRun| {
        if reverse {
            run.max_timestamp_ns
        } else {
            run.min_timestamp_ns
        }
    };
    let mut units = Vec::new();
    for (object, mut runs) in by_object {
        runs.sort_unstable_by_key(|run| run.run.first_block);
        let mut current: Option<ReadUnit> = None;
        for run in runs {
            let first_block = run.run.first_block;
            let end_block = first_block.saturating_add(run.run.blocks);
            let bound = nearest(&run.run);
            match &mut current {
                Some(unit) if first_block.saturating_sub(unit.end_block) <= max_gap_blocks => {
                    unit.end_block = unit.end_block.max(end_block);
                    unit.bound = if reverse {
                        unit.bound.max(bound)
                    } else {
                        unit.bound.min(bound)
                    };
                    unit.runs.push(run);
                }
                _ => {
                    units.extend(current.take());
                    current = Some(ReadUnit {
                        bound,
                        object,
                        first_block,
                        end_block,
                        runs: vec![run],
                    });
                }
            }
        }
        units.extend(current);
    }
    units.sort_by(|a, b| {
        let bound = if reverse {
            b.bound.cmp(&a.bound)
        } else {
            a.bound.cmp(&b.bound)
        };
        bound
            .then(a.object.cmp(&b.object))
            .then(a.first_block.cmp(&b.first_block))
    });
    units
}

/// Rows of a run's blocks in `[start_ns, end_ns]`, in timestamp order. A
/// merged run concatenates its leaves, so its rows may need sorting; the
/// sort is stable, keeping write order among equal timestamps.
pub(crate) fn run_entries(blocks: &[Block], range: (i64, i64)) -> Result<Vec<LogEntry>> {
    candidate_entries(blocks, range, |_| true)
}

/// [`run_entries`] of the rows whose index in the run passes `keep`.
fn candidate_entries(
    blocks: &[Block],
    range: (i64, i64),
    keep: impl FnMut(u32) -> bool,
) -> Result<Vec<LogEntry>> {
    let mut entries = run_rows_where(blocks, range, keep)?
        .into_iter()
        .map(|(_, entry)| entry)
        .collect::<Vec<_>>();
    if !entries.is_sorted_by_key(|entry| entry.timestamp_ns) {
        entries.sort_by_key(|entry| entry.timestamp_ns);
    }
    Ok(entries)
}

/// Rows of a run's blocks in `[start_ns, end_ns]` whose row index in the run
/// passes `keep`, in stored order. Blocks outside the range or holding no
/// kept row are not decompressed, and other rows are not materialized.
fn run_rows_where(
    blocks: &[Block],
    (start_ns, end_ns): (i64, i64),
    mut keep: impl FnMut(u32) -> bool,
) -> Result<Vec<(u32, LogEntry)>> {
    let mut rows = Vec::new();
    let mut next_row = 0u32;
    for block in blocks {
        let header = block_header(&block.meta)?;
        let first_row = next_row;
        next_row = next_row
            .checked_add(header.rows)
            .ok_or_else(|| Error::Corrupt("run row count exceeds u32".to_owned()))?;
        if !header.overlaps(start_ns, end_ns) || !(first_row..next_row).any(&mut keep) {
            continue;
        }
        decode_block_where(
            block,
            (start_ns, end_ns),
            |index| keep(first_row + index),
            |index, entry| rows.push((first_row + index, entry)),
        )?;
    }
    Ok(rows)
}

/// Window boundaries `first + k * step` for `k < count`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Boundaries {
    pub first: i64,
    pub step: i64,
    pub count: u64,
}

/// What [`LogDb::read_samples`] reads for a metric query that needs no line
/// content.
#[derive(Clone, Debug)]
pub(crate) struct SampleRead {
    /// Whether rows' structured metadata is needed.
    pub metadata: bool,
    /// Every boundary of every evaluation window `(start, end]`: each
    /// window's start and end.
    pub boundaries: Vec<Boundaries>,
}

impl SampleRead {
    /// Whether a window boundary `b` has `min <= b < max`, so that some
    /// window holds part, but not all, of the rows in `[min, max]`.
    pub(crate) fn splits(&self, min: i64, max: i64) -> bool {
        min < max
            && self.boundaries.iter().any(|boundaries| {
                let (first, step) = (
                    i128::from(boundaries.first),
                    i128::from(boundaries.step.max(1)),
                );
                let (min, max) = (i128::from(min), i128::from(max));
                let k = if min <= first {
                    0
                } else {
                    (min - first + step - 1) / step
                };
                k < i128::from(boundaries.count) && first + k * step < max
            })
    }
}

/// Adds to `batches` rows of one stream that every evaluation window holds
/// all or none of: `rows` stored rows from `timestamp_ns` on, with
/// `line_bytes` of lines between them.
fn push_sample(
    batches: &mut StreamBatches,
    labels: &Arc<Labels>,
    timestamp_ns: i64,
    (rows, line_bytes): (u32, u64),
    metadata: &crate::Fields,
) {
    let metadata = metadata
        .iter()
        .map(|field| (field.name.as_str(), field.value.as_str()));
    batches.push(labels, timestamp_ns, "", metadata, Some((rows, line_bytes)));
}

fn push_row_sample(batches: &mut StreamBatches, labels: &Arc<Labels>, sample: &RowSample) {
    let weight = (1, sample.line_len.into());
    push_sample(
        batches,
        labels,
        sample.timestamp_ns,
        weight,
        &sample.structured_metadata,
    );
}

impl LogDb {
    /// Reads `targets` for a metric query that needs no line content, as
    /// lineless batches whose rows each stand for stored rows that every
    /// evaluation window holds all or none of, deduplicated as queries
    /// require. Structured metadata is read only with
    /// [`SampleRead::metadata`].
    ///
    /// Queries drop rows of a stream that repeat an earlier row's timestamp,
    /// line and structured metadata. A run that is duplicate-free and whose
    /// time range meets no other run of its stream holds no such row, so it
    /// is counted from its run record when no window boundary splits it, and
    /// otherwise each of its blocks is counted from its header or, when split,
    /// decoded from its meta value alone. Other runs are decoded from their
    /// meta values, and their lines are read only for a stream where two rows
    /// share a timestamp, line length and, if read, structured metadata.
    /// Each read unit is charged to `budget`, as a full read would charge it.
    pub(crate) async fn read_samples(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
        read: &SampleRead,
        mut consume: impl FnMut(Vec<BatchSlice>) -> Result<()>,
    ) -> Result<()> {
        let mut batches = StreamBatches::new(true);
        let mut consume = |batches: &mut StreamBatches| {
            let slices = batches.take();
            if slices.is_empty() {
                return Ok(());
            }
            consume(slices)
        };
        for (segment, streams) in targets.segments {
            self.read_segment_samples(
                namespace,
                segment,
                streams,
                targets.range,
                budget,
                read,
                &mut batches,
                &mut consume,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_segment_samples(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: Vec<SegmentStream>,
        range: (i64, i64),
        budget: &PageBudget,
        read: &SampleRead,
        batches: &mut StreamBatches,
        consume: &mut impl FnMut(&mut StreamBatches) -> Result<()>,
    ) -> Result<()> {
        let mut isolated = HashSet::new();
        let mut with_metadata = HashSet::new();
        let mut decoded = HashSet::new();
        for stream in &streams {
            // `__error__` metadata fails the query, so a stream holding any
            // reads all of its runs' metadata; deduplication compares rows
            // across runs, so it must see the same fields of each.
            let metadata = read.metadata || stream.runs.iter().any(|(_, run)| run.error_metadata);
            if metadata {
                with_metadata.insert(stream.stream_id);
            }
            let alone = isolated_runs(&stream.runs);
            for (&(object_id, run), alone) in stream.runs.iter().zip(alone) {
                if alone && !metadata && !read.splits(run.min_timestamp_ns, run.max_timestamp_ns) {
                    push_sample(
                        batches,
                        &stream.labels,
                        run.min_timestamp_ns,
                        (run.rows, run.line_bytes),
                        &crate::Fields::default(),
                    );
                    continue;
                }
                if alone {
                    isolated.insert((stream.stream_id, object_id));
                }
                decoded.insert((stream.stream_id, object_id, run.first_block));
            }
        }
        consume(batches)?;
        // Planned over every run, as the query's page estimate is: dropping
        // counted runs first could split one unit into two and overrun it.
        // Sample-only metric queries skip line payloads, so one wider range
        // per object is cheaper than many sparse object-store reads.
        let units = plan_reads(&streams, false, u32::MAX)
            .into_iter()
            .filter_map(|mut unit| {
                unit.runs.retain(|run| {
                    decoded.contains(&(run.stream_id, run.object_id, run.run.first_block))
                });
                unit.first_block = unit.runs.iter().map(|run| run.run.first_block).min()?;
                unit.end_block = unit
                    .runs
                    .iter()
                    .map(|run| run.run.first_block + run.run.blocks)
                    .max()?;
                Some(unit)
            })
            .collect::<Vec<_>>();
        let mut loaded = futures::stream::iter(units)
            .map(|unit| async move {
                budget.take()?;
                let blocks = self
                    .load_blocks(
                        namespace,
                        segment,
                        unit.object,
                        unit.first_block..unit.end_block,
                        ReadNeeds { lines: false },
                    )
                    .await?;
                let runs = unit
                    .runs
                    .into_iter()
                    .map(|run| {
                        let first = (run.run.first_block - unit.first_block) as usize;
                        let blocks = blocks[first..first + run.run.blocks as usize].to_vec();
                        (run, blocks)
                    })
                    .collect::<Vec<_>>();
                Ok::<_, Error>(runs)
            })
            .buffer_unordered(PAGE_READ_CONCURRENCY);
        let mut unsure = HashMap::<StreamId, Vec<(UnitRun, Vec<RowSample>)>>::new();
        while let Some(runs) = loaded.try_next().await? {
            for (run, blocks) in runs {
                let metadata = with_metadata.contains(&run.stream_id);
                if isolated.contains(&(run.stream_id, run.object_id)) {
                    isolated_samples(&run.labels, &blocks, range, read, metadata, batches)?;
                } else {
                    let samples = run_samples(&blocks, range, metadata)?;
                    unsure
                        .entry(run.stream_id)
                        .or_default()
                        .push((run, samples));
                }
            }
            consume(batches)?;
        }
        drop(loaded);
        for (stream_id, runs) in unsure {
            let metadata = with_metadata.contains(&stream_id);
            self.deduplicated_samples(namespace, segment, runs, range, metadata, batches)
                .await?;
            consume(batches)?;
        }
        Ok(())
    }

    /// Adds samples of one stream's runs that may hold duplicates to
    /// `batches`, deduplicated.
    async fn deduplicated_samples(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        runs: Vec<(UnitRun, Vec<RowSample>)>,
        range: (i64, i64),
        metadata: bool,
        batches: &mut StreamBatches,
    ) -> Result<()> {
        let Some(labels) = runs.first().map(|(run, _)| Arc::clone(&run.labels)) else {
            return Ok(());
        };
        let mut keys = runs
            .iter()
            .flat_map(|(_, samples)| samples)
            .map(|sample| {
                (
                    sample.timestamp_ns,
                    sample.line_len,
                    &sample.structured_metadata,
                )
            })
            .collect::<Vec<_>>();
        keys.sort_unstable_by(|a, b| {
            (a.0, a.1)
                .cmp(&(b.0, b.1))
                .then_with(|| a.2.iter().cmp(b.2.iter()))
        });
        if keys.windows(2).all(|pair| pair[0] != pair[1]) {
            for sample in runs.iter().flat_map(|(_, samples)| samples) {
                push_row_sample(batches, &labels, sample);
            }
            return Ok(());
        }
        let mut entries = Vec::new();
        for (run, _) in &runs {
            let blocks = self
                .load_blocks(
                    namespace,
                    segment,
                    ObjectRef {
                        id: run.object_id,
                        level: run.run.level,
                    },
                    run.run.first_block..run.run.first_block + run.run.blocks,
                    ReadNeeds { lines: true },
                )
                .await?;
            entries.extend(run_entries(&blocks, range)?);
        }
        entries.sort_by(|a, b| {
            (a.timestamp_ns, &a.line)
                .cmp(&(b.timestamp_ns, &b.line))
                .then_with(|| {
                    a.structured_metadata
                        .iter()
                        .cmp(b.structured_metadata.iter())
                })
        });
        entries.dedup();
        let none = crate::Fields::default();
        for entry in &entries {
            push_sample(
                batches,
                &labels,
                entry.timestamp_ns,
                (1, entry.line.len() as u64),
                if metadata {
                    &entry.structured_metadata
                } else {
                    &none
                },
            );
        }
        Ok(())
    }
}

/// For each of a stream's runs, whether it is duplicate-free and its time
/// range meets no other run's, so none of its rows can repeat another row.
fn isolated_runs(runs: &[(u64, StoredRun)]) -> Vec<bool> {
    let mut order = (0..runs.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|&index| runs[index].1.min_timestamp_ns);
    let mut isolated = vec![false; runs.len()];
    let mut reached = i64::MIN;
    for (position, &index) in order.iter().enumerate() {
        let run = &runs[index].1;
        let after_previous = position == 0 || reached < run.min_timestamp_ns;
        let before_next = order
            .get(position + 1)
            .is_none_or(|&next| runs[next].1.min_timestamp_ns > run.max_timestamp_ns);
        isolated[index] = run.duplicate_free && after_previous && before_next;
        reached = reached.max(run.max_timestamp_ns);
    }
    isolated
}

/// Inclusive time ranges, ascending and disjoint, outside which no row of a
/// stream's `runs` repeats another of their rows: where two runs' time ranges
/// meet, and the whole range of a run not flagged duplicate-free.
pub(super) fn repeat_ranges(runs: &[(u64, StoredRun)]) -> Arc<[(i64, i64)]> {
    let mut order = runs.iter().map(|(_, run)| run).collect::<Vec<_>>();
    order.sort_unstable_by_key(|run| run.min_timestamp_ns);
    let mut ranges = Vec::new();
    let mut reached: Option<i64> = None;
    for run in order {
        if !run.duplicate_free {
            ranges.push((run.min_timestamp_ns, run.max_timestamp_ns));
        }
        if let Some(reached) = reached
            && reached >= run.min_timestamp_ns
        {
            ranges.push((run.min_timestamp_ns, reached.min(run.max_timestamp_ns)));
        }
        reached = Some(reached.map_or(run.max_timestamp_ns, |at| at.max(run.max_timestamp_ns)));
    }
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    for (min, max) in ranges {
        match merged.last_mut() {
            Some(last) if min <= last.1 => last.1 = last.1.max(max),
            _ => merged.push((min, max)),
        }
    }
    merged.into()
}

/// Samples of an isolated run's blocks in `range`: a block no window
/// boundary splits is counted from its header, others from their meta value.
fn isolated_samples(
    labels: &Arc<Labels>,
    blocks: &[Block],
    range: (i64, i64),
    read: &SampleRead,
    metadata: bool,
    batches: &mut StreamBatches,
) -> Result<()> {
    for block in blocks {
        let header = block_header(&block.meta)?;
        if !header.overlaps(range.0, range.1) {
            continue;
        }
        if !metadata && !read.splits(header.min_timestamp_ns, header.max_timestamp_ns) {
            push_sample(
                batches,
                labels,
                header.min_timestamp_ns,
                (header.rows, header.line_bytes.into()),
                &crate::Fields::default(),
            );
            continue;
        }
        decode_samples_where(
            block,
            range,
            |_| true,
            metadata,
            |_, sample| push_row_sample(batches, labels, &sample),
        )?;
    }
    Ok(())
}

/// Every row sample of a run's blocks in `range`, from their meta values.
fn run_samples(blocks: &[Block], range: (i64, i64), metadata: bool) -> Result<Vec<RowSample>> {
    let mut samples = Vec::new();
    for block in blocks {
        if !block_header(&block.meta)?.overlaps(range.0, range.1) {
            continue;
        }
        decode_samples_where(
            block,
            range,
            |_| true,
            metadata,
            |_, sample| {
                samples.push(sample);
            },
        )?;
    }
    Ok(samples)
}

/// Output of [`LogDb::scan_targets`].
pub(crate) struct ScanTargets {
    range: (i64, i64),
    segments: Vec<(SegmentId, Vec<SegmentStream>)>,
    /// See [`StreamFilter`].
    line_terms: Option<Vec<String>>,
}

impl ScanTargets {
    /// Estimates distinct objects, planned ranges, and selected run contents.
    /// Lineless reads coalesce every selected range in an object.
    pub(crate) fn estimate(&self, lineless: bool) -> QueryEstimate {
        let mut estimate = QueryEstimate::default();
        for (_, streams) in &self.segments {
            let max_gap_blocks = if lineless {
                u32::MAX
            } else {
                COALESCE_GAP_BLOCKS
            };
            let units = plan_reads(streams, false, max_gap_blocks);
            estimate.read_units = estimate.read_units.saturating_add(units.len());
            estimate.pages = estimate.pages.saturating_add(
                units
                    .iter()
                    .map(|unit| unit.object)
                    .collect::<HashSet<_>>()
                    .len(),
            );
            for (_, run) in streams.iter().flat_map(|stream| &stream.runs) {
                estimate.compressed_bytes = estimate
                    .compressed_bytes
                    .saturating_add(u64::from(run.bytes));
                estimate.lines = estimate.lines.saturating_add(u64::from(run.rows));
            }
        }
        estimate
    }
}

type RowKey = (i64, StreamFingerprint, u64, usize);

/// Decoded pages waiting until no unread page can precede their rows, merged
/// row by row in scan order. Pages hold rows in timestamp order, so the heap
/// orders pages by their next row instead of holding every row.
struct PendingRows {
    heap: BinaryHeap<PendingPage>,
    reverse: bool,
}

struct PendingPage {
    /// Key of the next row in scan order.
    key: RowKey,
    reverse: bool,
    labels: Arc<Labels>,
    /// In reverse scan order, so the next row is popped from the end.
    entries: Vec<LogEntry>,
    rows: usize,
}

impl PendingPage {
    fn key_of_next(&self) -> Option<RowKey> {
        let entry = self.entries.last()?;
        let index = if self.reverse {
            self.entries.len() - 1
        } else {
            self.rows - self.entries.len()
        };
        Some((entry.timestamp_ns, self.key.1, self.key.2, index))
    }

    /// Takes the next row; `false` once the page has no more.
    fn take(&mut self) -> (LogRow, bool) {
        let entry = self.entries.pop().expect("pending pages hold a row");
        let row = LogRow {
            labels: Arc::clone(&self.labels),
            entry,
        };
        match self.key_of_next() {
            Some(key) => {
                self.key = key;
                (row, true)
            }
            None => (row, false),
        }
    }
}

impl PartialEq for PendingPage {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PendingPage {}

impl PartialOrd for PendingPage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingPage {
    /// `BinaryHeap` pops the greatest page, whose next row must be the next
    /// in scan order.
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

    /// Adds a decoded run's rows, in timestamp order.
    fn push_run(
        &mut self,
        labels: Arc<Labels>,
        fingerprint: StreamFingerprint,
        object_id: u64,
        mut entries: Vec<LogEntry>,
    ) {
        if !self.reverse {
            entries.reverse();
        }
        let mut page = PendingPage {
            key: (0, fingerprint, object_id, 0),
            reverse: self.reverse,
            labels,
            rows: entries.len(),
            entries,
        };
        if let Some(key) = page.key_of_next() {
            page.key = key;
            self.heap.push(page);
        }
    }

    /// Pops rows strictly before `next_bound` (the nearest timestamp any
    /// unread page may hold) in ascending order; `None` releases everything.
    fn release(&mut self, next_bound: Option<i64>) -> Vec<LogRow> {
        let mut ready = Vec::new();
        while let Some(mut top) = self.heap.peek_mut() {
            let timestamp = top.key.0;
            let safe = match next_bound {
                None => true,
                Some(bound) if self.reverse => timestamp > bound,
                Some(bound) => timestamp < bound,
            };
            if !safe {
                break;
            }
            let (row, more) = top.take();
            ready.push(row);
            if !more {
                PeekMut::pop(top);
            }
        }
        if self.reverse {
            ready.reverse();
        }
        ready
    }
}

/// How [`LogDb::read_segment_pages`] decodes runs and holds them until no
/// unread page can precede their rows.
trait PendingReads {
    type Run: Send;
    /// Rows released together, all strictly after (or, reversed, before)
    /// every row released earlier.
    type Chunk;
    /// Rows of the run in `range` (and in `rows`, when given), in
    /// timestamp order; `segment` is the segment's position in the read.
    fn decode(
        run: &UnitRun,
        blocks: &[Block],
        range: (i64, i64),
        rows: Option<&HashSet<u32>>,
        segment: u32,
    ) -> Result<Self::Run>;
    fn new(reverse: bool) -> Self;
    fn push(&mut self, run: UnitRun, decoded: Self::Run);
    /// Rows strictly before `next_bound`; `None` releases everything.
    fn release(&mut self, next_bound: Option<i64>) -> Option<Self::Chunk>;
}

impl PendingReads for PendingRows {
    type Run = Vec<LogEntry>;
    type Chunk = Vec<LogRow>;

    fn decode(
        _: &UnitRun,
        blocks: &[Block],
        range: (i64, i64),
        rows: Option<&HashSet<u32>>,
        _: u32,
    ) -> Result<Vec<LogEntry>> {
        match rows {
            Some(rows) => candidate_entries(blocks, range, |row| rows.contains(&row)),
            None => run_entries(blocks, range),
        }
    }

    fn new(reverse: bool) -> Self {
        PendingRows::new(reverse)
    }

    fn push(&mut self, run: UnitRun, entries: Vec<LogEntry>) {
        self.push_run(run.labels, run.fingerprint, run.object_id, entries);
    }

    fn release(&mut self, next_bound: Option<i64>) -> Option<Vec<LogRow>> {
        Some(PendingRows::release(self, next_bound)).filter(|ready| !ready.is_empty())
    }
}

/// Decoded runs as column batches, each released as slices: a run's rows
/// are in timestamp order, so each release takes a prefix (or, reversed, a
/// suffix) of the rows still pending.
struct PendingBatches {
    /// Each run and the rows of it not yet released.
    runs: Vec<(Arc<ColumnBatch>, Range<usize>)>,
    reverse: bool,
}

impl PendingReads for PendingBatches {
    type Run = ColumnBatch;
    type Chunk = Vec<BatchSlice>;

    fn decode(
        run: &UnitRun,
        blocks: &[Block],
        (start_ns, end_ns): (i64, i64),
        rows: Option<&HashSet<u32>>,
        segment: u32,
    ) -> Result<ColumnBatch> {
        let mut batch = ColumnBatch::of_stream(
            &run.labels,
            Tie {
                segment,
                object: run.object_id,
                ..Tie::default()
            },
        );
        let mut keep = |row: u32| rows.is_none_or(|rows| rows.contains(&row));
        let mut next_row = 0u32;
        for block in blocks {
            let header = block_header(&block.meta)?;
            let first_row = next_row;
            next_row = next_row
                .checked_add(header.rows)
                .ok_or_else(|| Error::Corrupt("run row count exceeds u32".to_owned()))?;
            if !header.overlaps(start_ns, end_ns) || !(first_row..next_row).any(&mut keep) {
                continue;
            }
            decode_block_rows(
                block,
                (start_ns, end_ns),
                |index| keep(first_row + index),
                |_, timestamp_ns, line, fields| {
                    batch.push(timestamp_ns, line, fields.iter().copied(), None);
                },
            )?;
        }
        // A merged run concatenates its leaves; a stable sort keeps write
        // order among equal timestamps.
        batch.sort_by_timestamp();
        batch.set_repeats(Arc::clone(&run.repeats));
        Ok(batch)
    }

    fn new(reverse: bool) -> Self {
        Self {
            runs: Vec::new(),
            reverse,
        }
    }

    fn push(&mut self, _: UnitRun, batch: ColumnBatch) {
        if batch.len() > 0 {
            let rows = 0..batch.len();
            self.runs.push((Arc::new(batch), rows));
        }
    }

    fn release(&mut self, next_bound: Option<i64>) -> Option<Vec<BatchSlice>> {
        let mut ready = Vec::new();
        let reverse = self.reverse;
        self.runs.retain_mut(|(batch, rows)| {
            let timestamps = &batch.timestamps()[rows.clone()];
            let released = if reverse {
                let kept = next_bound.map_or(0, |bound| {
                    timestamps.partition_point(|&timestamp| timestamp <= bound)
                });
                let released = rows.start + kept..rows.end;
                rows.end = released.start;
                released
            } else {
                let count = next_bound.map_or(timestamps.len(), |bound| {
                    timestamps.partition_point(|&timestamp| timestamp < bound)
                });
                let released = rows.start..rows.start + count;
                rows.start = released.end;
                released
            };
            if !released.is_empty() {
                ready.push(BatchSlice::new(Arc::clone(batch), released));
            }
            rows.start < rows.end
        });
        Some(ready).filter(|ready| !ready.is_empty())
    }
}

/// Stream selection for a query: postings intersect `exact`, then a stream is
/// kept when any selector matches its labels (or when there are none).
pub(crate) struct StreamFilter {
    exact: Vec<Label>,
    selectors: Vec<SeriesSelector>,
    /// Analyzed terms every row the query keeps holds in its line. Reads
    /// may then skip rows whose postings lack one; such rows are dropped by
    /// the query's line filter anyway.
    line_terms: Option<Vec<String>>,
}

impl StreamFilter {
    pub(crate) fn exact(exact: Vec<Label>) -> Self {
        Self {
            exact,
            selectors: Vec::new(),
            line_terms: None,
        }
    }

    pub(crate) fn with_line_terms(self, line_terms: Option<Vec<String>>) -> Self {
        Self { line_terms, ..self }
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
            line_terms: None,
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
                    // `name=""` also matches streams without the label, which
                    // have no posting to intersect.
                    if !value.is_empty() {
                        exact.push(Label::new(&name, &value));
                    }
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

/// Runs `read` and `next` concurrently, returning `next`'s output unless
/// `read` breaks, in which case `next` is dropped unfinished.
async fn unless_break<T>(
    read: impl Future<Output = Result<ControlFlow<()>>>,
    next: impl Future<Output = Result<T>>,
) -> Result<Option<T>> {
    use futures::future::{Either, select};
    let (read, next) = (std::pin::pin!(read), std::pin::pin!(next));
    match select(read, next).await {
        Either::Left((flow, next)) => {
            if flow?.is_break() {
                return Ok(None);
            }
            next.await.map(Some)
        }
        Either::Right((value, read)) => {
            let value = value?;
            Ok(read.await?.is_continue().then_some(value))
        }
    }
}

fn sorted_union(parts: &[Arc<Versioned<Vec<String>>>]) -> Vec<String> {
    parts
        .iter()
        .flat_map(|part| part.1.iter())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .cloned()
        .collect()
}

fn anchored_regex(pattern: &str) -> Result<regex::Regex> {
    Ok(regex::Regex::new(&format!("^(?:{pattern})$"))?)
}
