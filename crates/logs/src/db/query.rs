// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read path: stream selection, object block reads and ordered row merging.

use std::collections::binary_heap::PeekMut;
use std::ops::Bound;

use super::*;
use crate::codec::{
    StoredRun, decode_object, decode_rollup_forward_key, decode_run, decode_run_key, directory_key,
    segment_run_prefix, stream_run_prefix,
};
use crate::object::{block_header, decode_block, read_blocks};

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
        let per_partition: Vec<Arc<Vec<String>>> =
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
    ) -> Result<Arc<Vec<String>>> {
        let cache = &self.caches.label_names;
        let key = (namespace.clone(), partition);
        if let Some(names) = cache.get(&key) {
            return Ok(names);
        }
        let generation = cache.generation();
        let active = self.is_active(partition);
        let names = catalog_names(
            self.storage.as_ref(),
            &catalog_prefix(namespace, partition),
            Some(""),
        )
        .await?;
        Ok(cache.insert_since(generation, key, names, active))
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
        let per_partition: Vec<Arc<Vec<String>>> =
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
    ) -> Result<Arc<Vec<String>>> {
        let cache = &self.caches.label_values;
        let key = (namespace.clone(), partition, name.to_owned());
        if let Some(values) = cache.get(&key) {
            return Ok(values);
        }
        let generation = cache.generation();
        let active = self.is_active(partition);
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
        Ok(cache.insert_since(generation, key, values, active))
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
                    let names = self.partition_label_names(namespace, partition).await?;
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
                        let streams = self.partition_series(namespace, partition, exact).await?;
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
        exact: &[Label],
    ) -> Result<SegmentSeries> {
        let cache = &self.caches.series;
        let key = (namespace.clone(), partition, exact.to_vec());
        if let Some(streams) = cache.get(&key) {
            return Ok(streams);
        }
        let generation = cache.generation();
        let active = self.is_active(partition);
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
            return Ok(Arc::new(streams));
        }
        Ok(cache.insert_since(generation, key, streams, active))
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

    fn is_active(&self, partition: Partition) -> bool {
        let now_ns = common::time::now_ns();
        match partition {
            Partition::Segment(segment) => segment_for(now_ns, self.segment_ns) == segment,
            Partition::Rollup(period) => self
                .rollup_ns
                .is_some_and(|rollup_ns| segment_for(now_ns, rollup_ns) == period),
        }
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
        mut consume: impl FnMut(Vec<LogRow>) -> Result<()>,
    ) -> Result<()> {
        let mut consume = |chunk: Vec<LogRow>| {
            consume(chunk)?;
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
        Ok(())
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
        let select_streams = |segment| {
            self.segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
        };
        let mut segments = self.segment_ids((start_ns, end_ns), reverse)?.into_iter();
        let Some(mut segment) = segments.next() else {
            return Ok(());
        };
        let mut streams = select_streams(segment).await?;
        loop {
            let read = self.read_segment_pages(
                namespace,
                segment,
                streams,
                (start_ns, end_ns),
                budget,
                reverse,
                &mut consume,
            );
            let Some(next) = segments.next() else {
                return read.await.map(drop);
            };
            // The next segment's streams are selected while this one's pages
            // are read.
            match unless_break(read, select_streams(next)).await? {
                Some(next_streams) => (segment, streams) = (next, next_streams),
                None => return Ok(()),
            }
        }
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
        let units = plan_reads(&streams, reverse);
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
                    )
                    .await?;
                let mut runs = Vec::with_capacity(unit.runs.len());
                for run in unit.runs {
                    let first = (run.run.first_block - unit.first_block) as usize;
                    let blocks = &blocks[first..first + run.run.blocks as usize];
                    let entries = run_entries(blocks, (start_ns, end_ns))?;
                    runs.push((run, entries));
                }
                Ok::<_, Error>(runs)
            })
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = PendingRows::new(reverse);
        let mut fetched = 0;
        while let Some(runs) = decoded.try_next().await? {
            fetched += 1;
            for (run, entries) in runs {
                pending.push_run(run.labels, run.fingerprint, run.object_id, entries);
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
        // Postings address written objects; a merged run covers several.
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

    pub(super) async fn load_blocks(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        range: Range<u32>,
    ) -> Result<Vec<Bytes>> {
        read_blocks(self.storage.as_ref(), namespace, segment, object, range).await
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
        let labelled = if filter.exact.is_empty() {
            self.scan_labels(
                forward_prefix(namespace, segment),
                BytesRange::unbounded(),
                decode_forward_key,
            )
            .await?
        } else {
            let stream_ids = self.stream_ids(namespace, segment, &filter.exact).await?;
            self.stream_labels(namespace, segment, stream_ids).await?
        };
        let labelled = labelled
            .into_iter()
            .filter(|(_, labels)| filter.matches(labels))
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
    labels: Arc<Labels>,
    fingerprint: StreamFingerprint,
    object_id: u64,
    run: StoredRun,
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
fn plan_reads(streams: &[SegmentStream], reverse: bool) -> Vec<ReadUnit> {
    let mut by_object = BTreeMap::<ObjectRef, Vec<UnitRun>>::new();
    for stream in streams {
        let fingerprint = stream.labels.fingerprint();
        for &(object_id, run) in &stream.runs {
            by_object
                .entry(ObjectRef {
                    id: object_id,
                    level: run.level,
                })
                .or_default()
                .push(UnitRun {
                    labels: Arc::clone(&stream.labels),
                    fingerprint,
                    object_id,
                    run,
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
                Some(unit) if first_block.saturating_sub(unit.end_block) <= COALESCE_GAP_BLOCKS => {
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
fn run_entries(blocks: &[Bytes], range: (i64, i64)) -> Result<Vec<LogEntry>> {
    let mut entries = run_rows_where(blocks, range, |_| true)?
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
/// kept row are not decompressed.
fn run_rows_where(
    blocks: &[Bytes],
    (start_ns, end_ns): (i64, i64),
    mut keep: impl FnMut(u32) -> bool,
) -> Result<Vec<(u32, LogEntry)>> {
    let mut rows = Vec::new();
    let mut next_row = 0u32;
    for block in blocks {
        let header = block_header(block)?;
        let first_row = next_row;
        next_row = next_row
            .checked_add(header.rows)
            .ok_or_else(|| Error::Corrupt("run row count exceeds u32".to_owned()))?;
        if !header.overlaps(start_ns, end_ns) || !(first_row..next_row).any(&mut keep) {
            continue;
        }
        for (row_id, entry) in (first_row..).zip(decode_block(block)?) {
            if entry.timestamp_ns >= start_ns && entry.timestamp_ns <= end_ns && keep(row_id) {
                rows.push((row_id, entry));
            }
        }
    }
    Ok(rows)
}

/// Output of [`LogDb::scan_targets`].
pub(crate) struct ScanTargets {
    range: (i64, i64),
    segments: Vec<(SegmentId, Vec<SegmentStream>)>,
}

impl ScanTargets {
    /// `pages` counts the read units a scan charges to its budget.
    pub(crate) fn estimate(&self) -> QueryEstimate {
        let mut estimate = QueryEstimate::default();
        for (_, streams) in &self.segments {
            estimate.pages = estimate
                .pages
                .saturating_add(plan_reads(streams, false).len());
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

fn sorted_union(parts: &[Arc<Vec<String>>]) -> Vec<String> {
    parts
        .iter()
        .flat_map(|part| part.iter())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .cloned()
        .collect()
}

fn anchored_regex(pattern: &str) -> Result<regex::Regex> {
    Ok(regex::Regex::new(&format!("^(?:{pattern})$"))?)
}
