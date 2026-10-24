// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Read path: stream selection, object block reads and ordered row merging.

use std::collections::binary_heap::PeekMut;
use std::ops::{Bound, Range};

use common::storage::ReadHints;

use super::*;
use crate::codec::{
    SegmentSummary, StoredRun, StreamSummary, SummaryBucket, decode_object, decode_period_summary,
    decode_rollup_forward_key, decode_run, decode_run_key, decode_segment_summary,
    decode_stream_summary, decode_stream_summary_key, directory_key, period_stream_summary_key,
    period_stream_summary_prefix, period_summary_key, posting_name_prefix, segment_run_prefix,
    stream_run_prefix, stream_summary_key, stream_summary_prefix, summary_key,
};
use crate::object::{
    Block, FieldSelection, ReadNeeds, RowSample, block_header, decode_block_rows,
    decode_block_where, decode_samples_where, read_blocks,
};
use crate::query::{BatchSlice, ColumnBatch, StreamBatches, Tie, is_error_label};
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
            futures::stream::iter(self.visible_partitions(namespace, start_ns, end_ns)?)
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
            self.reads().as_ref(),
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
        self.reads()
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
            futures::stream::iter(self.visible_partitions(namespace, start_ns, end_ns)?)
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
            self.reads().as_ref(),
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
            futures::stream::iter(self.visible_partitions(namespace, start_ns, end_ns)?)
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
                .buffer_unordered(self.tuning.list_concurrency)
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
        Ok(self
            .rollup_streams(namespace, period, exact)
            .await?
            .into_iter()
            .map(|(_, labels)| labels)
            .collect())
    }

    /// The period-local ID and labels of every stream of `period` holding
    /// every `exact` label, in ID order.
    async fn rollup_streams(
        &self,
        namespace: &Namespace,
        period: SegmentId,
        exact: &[Label],
    ) -> Result<Vec<(StreamId, Arc<Labels>)>> {
        let forward = rollup_forward_prefix(namespace, period);
        let prefix_len = forward.len();
        let scan = |range| {
            self.scan_labels(forward.clone(), range, |key| {
                decode_rollup_forward_key(key, prefix_len)
            })
        };
        if exact.is_empty() {
            return scan(BytesRange::unbounded()).await;
        }
        let reads = self.reads();
        let records = futures::future::try_join_all(
            exact
                .iter()
                .map(|label| reads.get(rollup_posting_key(namespace, period, label))),
        )
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
        if let Some(spans) = self.span_scans(&ids) {
            let labelled = futures::future::try_join_all(spans.into_iter().map(scan)).await?;
            return Ok(labelled
                .into_iter()
                .flatten()
                .filter(|(id, _)| ids.binary_search(id).is_ok())
                .collect());
        }
        let labelled: Vec<Option<(StreamId, Arc<Labels>)>> = futures::stream::iter(ids)
            .map(|id| async move {
                self.reads()
                    .get(rollup_forward_key(namespace, period, id))
                    .await?
                    .map(|record| decode_labels(&record.value).map(|labels| (id, Arc::new(labels))))
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
            segment = SegmentId::new(
                segment
                    .start_ns
                    .checked_add(self.segment_ns)
                    .ok_or_else(|| Error::Invalid("discovery segment range overflow".into()))?,
                self.segment_ns,
            );
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
            let period = segment_for(segment.start_ns, rollup_ns);
            if period.start_ns == segment.start_ns && segments.len() - index >= per_period {
                partitions.push(Partition::Rollup(period));
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
        let list = |segments: Vec<SegmentId>| {
            futures::stream::iter(segments)
                .map(|segment| async move {
                    let streams = self
                        .segment_streams(
                            namespace,
                            segment,
                            filter,
                            (start_ns, end_ns),
                            now_unix_ms,
                        )
                        .await?;
                    Ok::<_, Error>((segment, streams))
                })
                .buffered(self.tuning.list_concurrency)
                .try_collect::<Vec<_>>()
        };
        let visible = self.visible_segment_ids(namespace, (start_ns, end_ns), false)?;
        let (Some(read), Some(rollup_ns)) = (&filter.summaries, self.rollup_ns) else {
            return Ok(ScanTargets {
                range: (start_ns, end_ns),
                segments: list(visible).await?,
                line_terms: filter.line_terms.clone(),
                first_ordinal: 0,
            });
        };
        let period_of = |segment: &SegmentId| segment_for(segment.start_ns, rollup_ns);
        let settled_before = i64::try_from(now_unix_ms)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000)
            .saturating_sub(duration_ns(self.settle_after)?);
        let mut spanned = visible.iter().map(period_of).collect::<Vec<_>>();
        spanned.dedup();
        // Only settled periods are summarized, and a period summary lists
        // every stream of the period, so it pays only for queries reading
        // more than one of its segments.
        spanned.retain(|period| {
            period.start_ns.saturating_add(period.width_ns) <= settled_before
                && visible
                    .iter()
                    .filter(|segment| period_of(segment) == *period)
                    .nth(1)
                    .is_some()
        });
        let (candidates, others): (Vec<_>, Vec<_>) = visible
            .into_iter()
            .partition(|segment| spanned.contains(&period_of(segment)));
        let attempts = futures::stream::iter(spanned)
            .map(|period| async move {
                let streams = self
                    .period_streams(namespace, period, filter, read, now_unix_ms)
                    .await?;
                Ok::<_, Error>((period, streams))
            })
            .buffered(self.tuning.list_concurrency)
            .try_collect::<Vec<_>>();
        let (attempts, mut segments) = futures::future::try_join(attempts, list(others)).await?;
        let mut summarized = Vec::new();
        let mut unsummarized = HashSet::new();
        for (period, streams) in attempts {
            match streams {
                Some(streams) => summarized.push((period, streams)),
                None => {
                    unsummarized.insert(period);
                }
            }
        }
        let fallback = candidates
            .into_iter()
            .filter(|segment| unsummarized.contains(&period_of(segment)))
            .collect::<Vec<_>>();
        if !fallback.is_empty() {
            segments.extend(list(fallback).await?);
        }
        segments.extend(summarized);
        segments.sort_by_key(|(segment, _)| segment.start_ns);
        Ok(ScanTargets {
            range: (start_ns, end_ns),
            segments,
            line_terms: filter.line_terms.clone(),
            first_ordinal: 0,
        })
    }

    /// The rollup markers of `namespace`, which decide which segments reads
    /// use.
    pub(super) fn rollups(&self, namespace: &Namespace) -> Rollups {
        Rollups::markers(
            self.storage.slate_read().as_ref(),
            crate::codec::KEY_SCOPE,
            namespace,
        )
    }

    /// The write generation of the base segment starting at each of
    /// `starts`, as reads resolve it: the next object ID of the sealed rollup
    /// covering it, or of the segment itself, tagged with that segment's
    /// width. Every write allocates an object ID and merges reuse their first
    /// input's, so a generation changes exactly when what reads see might.
    /// `None` for a segment never written, or whose counter expired.
    pub(crate) async fn segment_generations(
        &self,
        namespace: &Namespace,
        starts: &[i64],
    ) -> Result<Vec<Option<u64>>> {
        let rollups = self.rollups(namespace);
        let base = self.segment_ns;
        let resolved = starts
            .iter()
            .map(|&start| {
                let segment = SegmentId::new(start, base);
                rollups.sealed_cover(segment).unwrap_or(segment)
            })
            .collect::<Vec<_>>();
        let distinct = resolved.iter().copied().collect::<BTreeSet<_>>();
        let generations = futures::stream::iter(distinct)
            .map(|segment| async move {
                let next = self
                    .reads()
                    .get(next_object_id_key(namespace, segment))
                    .await?
                    .map(|record| decode_object_id(&record.value))
                    .transpose()?;
                let width = u64::try_from(segment.width_ns / base.max(1)).unwrap_or(u64::MAX);
                Ok::<_, Error>((segment, next.map(|next| next | (width.min(0xff) << 56))))
            })
            .buffer_unordered(self.tuning.list_concurrency)
            .try_collect::<HashMap<_, _>>()
            .await?;
        Ok(resolved
            .iter()
            .map(|segment| generations.get(segment).copied().flatten())
            .collect())
    }

    /// [`Self::segment_ids`], each covered segment replaced by its sealed
    /// rollup.
    pub(super) fn visible_segment_ids(
        &self,
        namespace: &Namespace,
        range: (i64, i64),
        reverse: bool,
    ) -> Result<Vec<SegmentId>> {
        let segments = self.segment_ids(range, false)?;
        let rollups = self.rollups(namespace);
        if rollups.is_empty() {
            return Ok(if reverse {
                segments.into_iter().rev().collect()
            } else {
                segments
            });
        }
        let mut visible = rollups.visible(segments);
        if reverse {
            visible.reverse();
        }
        Ok(visible)
    }

    /// [`Self::discovery_partitions`], each covered segment replaced by its
    /// sealed rollup.
    pub(super) fn visible_partitions(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Partition>> {
        let partitions = self.discovery_partitions(start_ns, end_ns)?;
        let rollups = self.rollups(namespace);
        if rollups.is_empty() {
            return Ok(partitions);
        }
        let mut visible = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let partition = match partition {
                Partition::Segment(segment) => {
                    Partition::Segment(rollups.sealed_cover(segment).unwrap_or(segment))
                }
                rollup @ Partition::Rollup(_) => rollup,
            };
            if !visible.contains(&partition) {
                visible.push(partition);
            }
        }
        Ok(visible)
    }

    /// Every stream of `segment` with live rows, in reads of at most
    /// `streams` streams each.
    pub(super) async fn segment_reads(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: usize,
    ) -> Result<Vec<ScanTargets>> {
        let range = (
            segment.start_ns,
            segment
                .start_ns
                .saturating_add(segment.width_ns)
                .saturating_sub(1),
        );
        let found = self
            .segment_streams(
                namespace,
                segment,
                &StreamFilter::exact(Vec::new()),
                range,
                unix_time_ms()?,
            )
            .await?;
        let mut found = found
            .into_iter()
            .filter(|stream| !stream.runs.is_empty())
            .peekable();
        let mut reads = Vec::new();
        while found.peek().is_some() {
            reads.push(ScanTargets {
                range,
                segments: vec![(segment, found.by_ref().take(streams.max(1)).collect())],
                line_terms: None,
                first_ordinal: 0,
            });
        }
        Ok(reads)
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
            segment = SegmentId::new(
                segment
                    .start_ns
                    .checked_add(self.segment_ns)
                    .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?,
                self.segment_ns,
            );
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

    /// Sends each decoded row chunk before reading more. Used by rollup so
    /// copying a hot stream never retains all of its rows at once. Each
    /// chunk goes with the decode permit of the unit that released it,
    /// which the receiver holds until it is done with the rows. Rows a unit
    /// leaves pending go out under a later chunk's permit, so no unit waits
    /// on permits that rows read before it hold.
    pub(super) async fn read_bounded_chunks<'a>(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &'a PageBudget,
        sender: tokio::sync::mpsc::Sender<(Vec<LogRow>, DecodePermit<'a>)>,
    ) -> Result<()> {
        for (ordinal, (segment, streams)) in (targets.first_ordinal..).zip(targets.segments) {
            self.read_segment_chunks(
                namespace,
                (segment, ordinal),
                streams,
                targets.range,
                budget,
                &sender,
            )
            .await?;
        }
        Ok(())
    }

    async fn read_segment_chunks<'a>(
        &self,
        namespace: &Namespace,
        (segment, ordinal): (SegmentId, usize),
        streams: Vec<SegmentStream>,
        range: (i64, i64),
        budget: &'a PageBudget,
        sender: &tokio::sync::mpsc::Sender<(Vec<LogRow>, DecodePermit<'a>)>,
    ) -> Result<()> {
        let ordinal = u32::try_from(ordinal).unwrap_or(u32::MAX);
        let units = plan_reads(&streams, false, COALESCE_GAP_BLOCKS);
        let bounds = units.iter().map(|unit| unit.bound).collect::<Vec<_>>();
        let mut decoded = futures::stream::iter(units)
            .map(|unit| async move {
                budget.take()?;
                let decoding = budget.decode(&self.memory, unit.decode_bytes(true)).await?;
                let blocks = self
                    .load_blocks(
                        namespace,
                        segment,
                        unit.object,
                        unit.first_block..unit.end_block,
                        ReadNeeds { lines: true },
                    )
                    .await?;
                let runs = tokio::spawn(async move {
                    let mut runs = Vec::with_capacity(unit.runs.len());
                    for run in unit.runs {
                        let first = (run.run.first_block - unit.first_block) as usize;
                        let blocks = &blocks[first..first + run.run.blocks as usize];
                        let decoded = PendingRows::decode(&run, blocks, range, None, ordinal)?;
                        runs.push((run, decoded));
                    }
                    Ok::<_, Error>(runs)
                })
                .await
                .map_err(|error| Error::Invalid(format!("page decode task failed: {error}")))??;
                Ok::<_, Error>((decoding, runs))
            })
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = PendingRows::new(false);
        let mut fetched = 0;
        while let Some((decoding, runs)) = decoded.try_next().await? {
            fetched += 1;
            for (run, decoded) in runs {
                pending.push(run, decoded);
            }
            if let Some(ready) =
                <PendingRows as PendingReads>::release(&mut pending, bounds.get(fetched).copied())
            {
                sender
                    .send((ready, decoding))
                    .await
                    .map_err(|_| Error::Invalid("rollup row consumer stopped".into()))?;
            }
        }
        Ok(())
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
        for (ordinal, (segment, streams)) in (targets.first_ordinal..).zip(targets.segments) {
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
        if self.tuning.segment_lookahead > 0 {
            return self
                .read_segments_pipelined::<P>(
                    namespace,
                    (start_ns, end_ns),
                    filter,
                    budget,
                    reverse,
                    consume,
                )
                .await;
        }
        let now_unix_ms = unix_time_ms()?;
        let select_streams = |segment| {
            self.segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
        };
        let mut segments = self
            .visible_segment_ids(namespace, (start_ns, end_ns), reverse)?
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
        let (streams, candidates) = self
            .narrow_to_candidates(namespace, segment, streams, line_terms)
            .await?;
        let units = plan_reads(&streams, reverse, COALESCE_GAP_BLOCKS);
        let bounds = units.iter().map(|unit| unit.bound).collect::<Vec<_>>();
        let mut decoded = futures::stream::iter(units)
            .map(move |unit| {
                let candidates = Arc::clone(&candidates);
                async move {
                    budget.take()?;
                    // Held until the unit's rows are pending. Units ask the fair
                    // semaphore in read order, so a segment's earliest holder
                    // never waits behind a later one.
                    let decoding = budget.decode(&self.memory, unit.decode_bytes(true)).await?;
                    let blocks = self
                        .load_blocks(
                            namespace,
                            segment,
                            unit.object,
                            unit.first_block..unit.end_block,
                            ReadNeeds { lines: true },
                        )
                        .await?;
                    // Decoded on a task of its own: polled here, every unit of
                    // every segment would decode on the query's one task.
                    let runs = tokio::spawn(async move {
                        let mut runs = Vec::with_capacity(unit.runs.len());
                        for run in unit.runs {
                            let first = (run.run.first_block - unit.first_block) as usize;
                            let blocks = &blocks[first..first + run.run.blocks as usize];
                            let rows = candidates.as_ref().as_ref().and_then(|candidates| {
                                candidates.get(&(run.stream_id, run.object_id))
                            });
                            let decoded =
                                P::decode(&run, blocks, (start_ns, end_ns), rows, ordinal)?;
                            runs.push((run, decoded));
                        }
                        Ok::<_, Error>(runs)
                    })
                    .await
                    .map_err(|error| {
                        Error::Invalid(format!("page decode task failed: {error}"))
                    })??;
                    Ok::<_, Error>((decoding, runs))
                }
            })
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = P::new(reverse);
        let mut fetched = 0;
        while let Some((decoding, runs)) = decoded.try_next().await? {
            fetched += 1;
            for (run, decoded) in runs {
                pending.push(run, decoded);
            }
            drop(decoding);
            if let Some(ready) = pending.release(bounds.get(fetched).copied())
                && consume(ready)?.is_break()
            {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// `streams` without the runs [`Self::line_candidates`] rules out for
    /// `line_terms`, and the candidate rows of those left.
    async fn narrow_to_candidates(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        mut streams: Vec<SegmentStream>,
        line_terms: Option<&[String]>,
    ) -> Result<(Vec<SegmentStream>, Arc<Option<Candidates>>)> {
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
        Ok((streams, Arc::new(candidates)))
    }

    /// [`Self::read_segments_pending`] with segments pipelined: up to
    /// [`ReadTuning::segment_lookahead`] segments past the one being
    /// consumed have their streams and candidates selected, and read units
    /// of later segments start as soon as fewer than
    /// [`ReadTuning::pipeline_units`] are in flight, so a read that needs
    /// many segments costs a few dependent round trips rather than a few
    /// per segment. Rows are released exactly as segment by segment: in
    /// scan order, a segment's last unit releasing all its rows. Breaking
    /// drops the reads still in flight.
    async fn read_segments_pipelined<P: PendingReads>(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        mut consume: impl FnMut(P::Chunk) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let now_unix_ms = unix_time_ms()?;
        let segments = self.visible_segment_ids(namespace, (start_ns, end_ns), reverse)?;
        let line_terms = filter.line_terms.as_deref();
        let plans = futures::stream::iter((0u32..).zip(segments))
            .map(|(ordinal, segment)| async move {
                let streams = self
                    .segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
                    .await?;
                let (streams, candidates) = self
                    .narrow_to_candidates(namespace, segment, streams, line_terms)
                    .await?;
                let units = plan_reads(&streams, reverse, COALESCE_GAP_BLOCKS);
                let mut bounds = units
                    .iter()
                    .skip(1)
                    .map(|unit| Some(unit.bound))
                    .collect::<Vec<_>>();
                bounds.push(None);
                Ok::<_, Error>(
                    units
                        .into_iter()
                        .zip(bounds)
                        .map(move |(unit, next_bound)| PipelinedUnit {
                            segment,
                            ordinal,
                            next_bound,
                            candidates: Arc::clone(&candidates),
                            unit,
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .buffered(self.tuning.segment_lookahead.max(1));
        // Planning runs beside the reads rather than as their source, so it
        // keeps going while the reads have no room for another unit.
        let (planned, mut receiver) = tokio::sync::mpsc::channel(1);
        let plan = async move {
            let mut plans = std::pin::pin!(plans);
            while let Some(plan) = plans.next().await {
                let failed = plan.is_err();
                if planned.send(plan).await.is_err() || failed {
                    return;
                }
            }
        };
        let units = futures::stream::poll_fn(move |context| receiver.poll_recv(context))
            .map_ok(|units| futures::stream::iter(units.into_iter().map(Ok)))
            .try_flatten()
            .map(|unit: Result<PipelinedUnit>| async move {
                let PipelinedUnit {
                    segment,
                    ordinal,
                    next_bound,
                    candidates,
                    unit,
                } = unit?;
                budget.take()?;
                let decoding = budget.decode(&self.memory, unit.decode_bytes(true)).await?;
                let blocks = self
                    .load_blocks(
                        namespace,
                        segment,
                        unit.object,
                        unit.first_block..unit.end_block,
                        ReadNeeds { lines: true },
                    )
                    .await?;
                let runs = tokio::spawn(async move {
                    let mut runs = Vec::with_capacity(unit.runs.len());
                    for run in unit.runs {
                        let first = (run.run.first_block - unit.first_block) as usize;
                        let blocks = &blocks[first..first + run.run.blocks as usize];
                        let rows = candidates
                            .as_ref()
                            .as_ref()
                            .and_then(|candidates| candidates.get(&(run.stream_id, run.object_id)));
                        let decoded = P::decode(&run, blocks, (start_ns, end_ns), rows, ordinal)?;
                        runs.push((run, decoded));
                    }
                    Ok::<_, Error>(runs)
                })
                .await
                .map_err(|error| Error::Invalid(format!("page decode task failed: {error}")))??;
                Ok::<_, Error>((ordinal, next_bound, decoding, runs))
            })
            .buffered(self.tuning.pipeline_units.max(1));
        let read = async move {
            let mut units = std::pin::pin!(units);
            let mut pending = P::new(reverse);
            while let Some((_, next_bound, decoding, runs)) = units.try_next().await? {
                for (run, decoded) in runs {
                    pending.push(run, decoded);
                }
                drop(decoding);
                if let Some(ready) = pending.release(next_bound)
                    && consume(ready)?.is_break()
                {
                    return Ok(());
                }
            }
            Ok(())
        };
        match futures::future::select(std::pin::pin!(read), std::pin::pin!(plan)).await {
            futures::future::Either::Left((read, _)) => read,
            futures::future::Either::Right(((), read)) => read.await,
        }
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
            self.reads().as_ref(),
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
    ) -> Result<Option<Candidates>> {
        let reads = self.reads();
        let storage = reads.as_ref();
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
                    .reads()
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
            self.reads().as_ref(),
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
                .reads()
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

        let reads = self.reads();
        let records = futures::future::try_join_all(
            matchers
                .iter()
                .map(|matcher| reads.get(posting_key(namespace, segment, matcher))),
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
        if let Some(spans) = self.span_scans(&stream_ids) {
            let prefix = forward_prefix(namespace, segment);
            let mut labelled = futures::future::try_join_all(
                spans
                    .into_iter()
                    .map(|span| self.scan_labels(prefix.clone(), span, decode_forward_key)),
            )
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
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
                    .reads()
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
    /// record is scanned, so non-exact matchers prune streams without I/O;
    /// with [`ReadTuning::overlap_runs`], streams the exact labels alone
    /// select have their runs read while their labels are.
    ///
    /// With [`StreamFilter::labels`], streams may carry only those labels,
    /// except one that may fail the query: its rows' merge order, by its
    /// labels' hash, picks the failure reported.
    async fn segment_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        filter: &StreamFilter,
        (start_ns, end_ns): (i64, i64),
        now_unix_ms: u64,
    ) -> Result<Vec<SegmentStream>> {
        let live = move |run: &StoredRun| {
            !run.is_expired_at(now_unix_ms)
                && run.max_timestamp_ns >= start_ns
                && run.min_timestamp_ns <= end_ns
        };
        let projection = filter.labels.as_ref().filter(|_| self.project_labels);
        let (selection, summary) = futures::future::try_join(
            self.select_streams(namespace, segment, &filter.exact, projection),
            self.usable_summary(namespace, segment, filter, now_unix_ms),
        )
        .await?;
        let ((selected, projected), runs) = match selection {
            Selection::Labelled(streams, projected) => ((streams, projected), None),
            Selection::Listed(stream_ids, version)
                if summary.is_none() && self.tuning.overlap_runs && filter.selects_exact() =>
            {
                let (selected, runs) = futures::future::try_join(
                    self.label_streams(
                        namespace,
                        segment,
                        &filter.exact,
                        projection,
                        stream_ids.clone(),
                        version,
                    ),
                    self.segment_runs(namespace, segment, stream_ids, live),
                )
                .await?;
                (selected, Some(runs))
            }
            Selection::Listed(stream_ids, version) => (
                self.label_streams(
                    namespace,
                    segment,
                    &filter.exact,
                    projection,
                    stream_ids,
                    version,
                )
                .await?,
                None,
            ),
        };
        let labelled = selected
            .1
            .iter()
            .filter(|(_, labels)| filter.matches(labels))
            .map(|(stream_id, labels)| (*stream_id, Arc::clone(labels)))
            .collect::<Vec<_>>();
        let (runs, counts) = match runs {
            Some(runs) => (runs, Vec::new()),
            None => {
                self.stream_contents(namespace, segment, &labelled, summary, live)
                    .await?
            }
        };
        debug_assert_eq!(labelled.len(), runs.len());
        let mut counts = counts.into_iter();
        let mut streams = labelled
            .into_iter()
            .zip(runs)
            .map(|((stream_id, labels), runs)| SegmentStream {
                stream_id,
                labels,
                runs,
                counts: counts.next().flatten(),
            })
            .collect::<Vec<_>>();
        if projected {
            let failing = streams
                .iter()
                .filter(|stream| stream.may_fail())
                .map(|stream| stream.stream_id)
                .collect::<Vec<_>>();
            if !failing.is_empty() {
                let mut full = self
                    .stream_labels(namespace, segment, failing)
                    .await?
                    .into_iter();
                for stream in streams.iter_mut().filter(|stream| stream.may_fail()) {
                    let (stream_id, labels) = full.next().ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".to_owned())
                    })?;
                    debug_assert_eq!(stream_id, stream.stream_id);
                    stream.labels = labels;
                }
            }
        }
        Ok(streams)
    }

    /// The summary of `segment`, when `filter` may count streams from one
    /// and it still counts what reads see: written since its last write
    /// and before any run it counts expired, in buckets no window boundary
    /// falls inside.
    async fn usable_summary(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        filter: &StreamFilter,
        now_unix_ms: u64,
    ) -> Result<Option<(SegmentSummary, Arc<SampleRead>)>> {
        let Some(read) = &filter.summaries else {
            return Ok(None);
        };
        let reads = self.reads();
        let (summary, next) = futures::future::try_join(
            reads.get(summary_key(namespace, segment)),
            reads.get(next_object_id_key(namespace, segment)),
        )
        .await?;
        let Some(summary) = summary
            .map(|record| decode_segment_summary(&record.value))
            .transpose()?
        else {
            return Ok(None);
        };
        let generation = next
            .map(|record| decode_object_id(&record.value))
            .transpose()?;
        Ok(
            (summary.is_current(generation, now_unix_ms) && read.aligned_to(summary.bucket_ns))
                .then(|| (summary, Arc::clone(read))),
        )
    }

    /// The streams of every segment of `period` that `filter` selects, all
    /// counted from the period's summary, when it is current and holds
    /// every one of them: summed from the summary of each segment reads
    /// use, at that segment's current generation.
    async fn period_streams(
        &self,
        namespace: &Namespace,
        period: SegmentId,
        filter: &StreamFilter,
        read: &SampleRead,
        now_unix_ms: u64,
    ) -> Result<Option<Vec<SegmentStream>>> {
        let Some(covered) = self.period_segments(&self.rollups(namespace), period) else {
            return Ok(None);
        };
        let (summary, generations, labelled) = futures::future::try_join3(
            async {
                Ok::<_, Error>(
                    self.reads()
                        .get(period_summary_key(namespace, period))
                        .await?,
                )
            },
            self.generations_of(namespace, covered),
            self.rollup_streams(namespace, period, &filter.exact),
        )
        .await?;
        let Some(summary) = summary
            .map(|record| decode_period_summary(&record.value))
            .transpose()?
            .filter(|summary| {
                read.aligned_to(summary.bucket_ns) && summary.is_current(&generations, now_unix_ms)
            })
        else {
            return Ok(None);
        };
        let labelled = labelled
            .into_iter()
            .filter(|(_, labels)| filter.matches(labels))
            .collect::<Vec<_>>();
        let countable = labelled.iter().all(|(_, labels)| {
            read.metadata.fields(labels).is_none()
                && !labels.iter().any(|label| is_error_label(&label.name))
        });
        if !countable {
            return Ok(None);
        }
        let found = self
            .summaries_under(
                period_stream_summary_prefix(namespace, period),
                |stream_id| period_stream_summary_key(namespace, period, stream_id),
                summary.bucket(period.start_ns),
                labelled.iter().map(|(stream_id, _)| *stream_id).collect(),
            )
            .await?;
        let mut streams = Vec::with_capacity(labelled.len());
        for ((stream_id, labels), found) in labelled.into_iter().zip(found) {
            match found {
                Some(None) => return Ok(None),
                Some(Some(buckets)) => streams.push(SegmentStream {
                    stream_id,
                    labels,
                    runs: Vec::new(),
                    counts: Some(StreamCounts {
                        bucket_ns: summary.bucket_ns,
                        buckets,
                    }),
                }),
                None => {}
            }
        }
        Ok(Some(streams))
    }

    /// Runs of each of `labelled` (ascending) passing `keep`, and counts of
    /// those `summary` counts instead: streams it summarized, of which the
    /// read needs no structured metadata and no row can fail. Their runs
    /// are left empty, as are those of streams it holds no rows of.
    async fn stream_contents(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        labelled: &[(StreamId, Arc<Labels>)],
        summary: Option<(SegmentSummary, Arc<SampleRead>)>,
        keep: impl Fn(&StoredRun) -> bool + Copy + Send + Sync + 'static,
    ) -> Result<(Vec<Vec<(u64, StoredRun)>>, Vec<Option<StreamCounts>>)> {
        let mut counts = labelled.iter().map(|_| None).collect::<Vec<_>>();
        let mut listed = vec![true; labelled.len()];
        if let Some((summary, read)) = summary {
            let counted = labelled
                .iter()
                .enumerate()
                .filter(|(_, (_, labels))| {
                    read.metadata.fields(labels).is_none()
                        && !labels.iter().any(|label| is_error_label(&label.name))
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let stream_ids = counted.iter().map(|&index| labelled[index].0).collect();
            let found = self
                .stream_summaries(namespace, segment, &summary, stream_ids)
                .await?;
            for (index, found) in counted.into_iter().zip(found) {
                match found {
                    Some(None) => continue,
                    Some(Some(buckets)) => {
                        counts[index] = Some(StreamCounts {
                            bucket_ns: summary.bucket_ns,
                            buckets,
                        });
                    }
                    None => {}
                }
                listed[index] = false;
            }
        }
        let stream_ids = labelled
            .iter()
            .zip(&listed)
            .filter(|(_, listed)| **listed)
            .map(|((stream_id, _), _)| *stream_id)
            .collect();
        let mut runs = self
            .segment_runs(namespace, segment, stream_ids, keep)
            .await?
            .into_iter();
        let runs = listed
            .iter()
            .map(|listed| {
                if *listed {
                    runs.next().unwrap_or_default()
                } else {
                    Vec::new()
                }
            })
            .collect();
        Ok((runs, counts))
    }

    /// Summaries of `stream_ids` (ascending) in `segment`, in input order;
    /// `None` for a stream the summary holds no rows of.
    async fn stream_summaries(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        summary: &SegmentSummary,
        stream_ids: Vec<StreamId>,
    ) -> Result<Vec<Option<StreamSummary>>> {
        self.summaries_under(
            stream_summary_prefix(namespace, segment),
            |stream_id| stream_summary_key(namespace, segment, stream_id),
            summary.bucket(segment.start_ns),
            stream_ids,
        )
        .await
    }

    /// Summaries of `stream_ids` (ascending) under `prefix`, each keyed by
    /// `key`, in input order; `None` for a stream with no record.
    async fn summaries_under(
        &self,
        prefix: Bytes,
        key: impl Fn(StreamId) -> Bytes + Copy + Send + Sync,
        first: i64,
        stream_ids: Vec<StreamId>,
    ) -> Result<Vec<Option<StreamSummary>>> {
        if let Some(spans) = self.span_scans(&stream_ids) {
            let prefix_len = prefix.len();
            let selected = &stream_ids;
            let scans = spans.into_iter().map(|span| {
                let prefix = prefix.clone();
                async move {
                    let mut records = self.reads().scan_prefix_iter(prefix, span, None).await?;
                    let mut found = Vec::new();
                    while let Some(record) = records.next().await? {
                        let stream_id = decode_stream_summary_key(&record.key, prefix_len)?;
                        if selected.binary_search(&stream_id).is_ok() {
                            found.push((stream_id, decode_stream_summary(&record.value, first)?));
                        }
                    }
                    Ok::<_, Error>(found)
                }
            });
            let mut found = futures::future::try_join_all(scans)
                .await?
                .into_iter()
                .flatten()
                .collect::<HashMap<_, _>>();
            return Ok(stream_ids
                .iter()
                .map(|stream_id| found.remove(stream_id))
                .collect());
        }
        futures::stream::iter(stream_ids)
            .map(|stream_id| async move {
                self.reads()
                    .get(key(stream_id))
                    .await?
                    .map(|record| decode_stream_summary(&record.value, first))
                    .transpose()
            })
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Runs of `stream_ids` (ascending) passing `keep`, in input order.
    async fn segment_runs(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_ids: Vec<StreamId>,
        keep: impl Fn(&StoredRun) -> bool + Copy + Send + Sync + 'static,
    ) -> Result<Vec<Vec<(u64, StoredRun)>>> {
        if let Some(spans) = self.span_scans(&stream_ids) {
            let mut runs = self
                .spanned_runs(namespace, segment, spans, stream_ids.clone(), keep)
                .await?;
            return Ok(stream_ids
                .iter()
                .map(|stream_id| runs.remove(stream_id).unwrap_or_default())
                .collect());
        }
        futures::stream::iter(stream_ids)
            .map(|stream_id| self.stream_runs(namespace, segment, stream_id, keep))
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Streams of `segment` holding every `exact` label, in ID order: cached,
    /// with whether they carry only the labels `projection` names, or else
    /// their IDs and the segment's stream counter, read before them, for
    /// [`Self::label_streams`]. A cached listing is current while the
    /// segment's stream counter is unchanged or, with
    /// [`ReadTuning::postings_validate`] and exact labels, while their
    /// postings select the same streams: a stream's labels never change, and
    /// a new stream holding them joins the postings.
    async fn select_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        exact: &[Label],
        projection: Option<&Arc<[String]>>,
    ) -> Result<Selection> {
        let full_key = (namespace.clone(), segment, exact.to_vec(), None);
        // Listing every stream reads every forward label anyway.
        let projected_key = projection.filter(|_| !exact.is_empty()).map(|names| {
            (
                namespace.clone(),
                segment,
                exact.to_vec(),
                Some(Arc::clone(names)),
            )
        });
        let cached = |current: &dyn Fn(&SegmentStreams) -> bool| {
            if let Some(key) = &projected_key
                && let Some(streams) = self
                    .caches
                    .streams
                    .get(key)
                    .filter(|cached| current(cached))
            {
                return Some(Selection::Labelled(streams, true));
            }
            self.caches
                .streams
                .get(&full_key)
                .filter(|cached| current(cached))
                .map(|streams| Selection::Labelled(streams, false))
        };
        if self.tuning.postings_validate && !exact.is_empty() {
            let stream_ids = self.stream_ids(namespace, segment, exact).await?;
            let same = |cached: &SegmentStreams| {
                cached
                    .1
                    .iter()
                    .map(|(stream_id, _)| *stream_id)
                    .eq(stream_ids.iter().copied())
            };
            return Ok(cached(&same).unwrap_or(Selection::Listed(stream_ids, None)));
        }
        let version = self
            .partition_version(namespace, Partition::Segment(segment))
            .await?;
        if let Some(selection) = cached(&|cached: &SegmentStreams| cached.0 == version) {
            return Ok(selection);
        }
        if exact.is_empty() {
            let labelled = self
                .scan_labels(
                    forward_prefix(namespace, segment),
                    BytesRange::unbounded(),
                    decode_forward_key,
                )
                .await?;
            let streams = Arc::new((version, labelled));
            self.caches.streams.insert(full_key, Arc::clone(&streams));
            return Ok(Selection::Labelled(streams, false));
        }
        let stream_ids = self.stream_ids(namespace, segment, exact).await?;
        Ok(Selection::Listed(stream_ids, version))
    }

    /// Labels `stream_ids`, listed by [`Self::select_streams`], and caches
    /// them tagged with `version`. Projected streams are built from postings
    /// unless [`Self::projected_labels`] finds forward labels cheaper.
    async fn label_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        exact: &[Label],
        projection: Option<&Arc<[String]>>,
        stream_ids: Vec<StreamId>,
        version: PartitionVersion,
    ) -> Result<(SegmentStreams, bool)> {
        if let Some(names) = projection
            && let Some(labelled) = self
                .projected_labels(namespace, segment, exact, &stream_ids, names)
                .await?
        {
            let key = (
                namespace.clone(),
                segment,
                exact.to_vec(),
                Some(Arc::clone(names)),
            );
            let streams = Arc::new((version, labelled));
            self.caches.streams.insert(key, Arc::clone(&streams));
            return Ok((streams, true));
        }
        let labelled = self.stream_labels(namespace, segment, stream_ids).await?;
        let streams = Arc::new((version, labelled));
        self.caches.streams.insert(
            (namespace.clone(), segment, exact.to_vec(), None),
            Arc::clone(&streams),
        );
        Ok((streams, false))
    }

    /// Labels of `stream_ids` (ascending), each holding every `exact` label,
    /// limited to `names`: exact labels as matched, other names' values from
    /// one scan of each name's postings in `segment`, a stream lacking the
    /// label when no posting holds it. `None` once the scans have read more
    /// postings than there are streams, as forward labels, at least one
    /// record per stream, then cost no more.
    async fn projected_labels(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        exact: &[Label],
        stream_ids: &[StreamId],
        names: &[String],
    ) -> Result<Option<Vec<(StreamId, Arc<Labels>)>>> {
        if stream_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let selected = stream_ids.iter().copied().collect::<RoaringBitmap>();
        let read = AtomicUsize::new(0);
        let (selected, read) = (&selected, &read);
        let scans = names
            .iter()
            .filter(|name| !exact.iter().any(|label| label.name == **name))
            .map(|name| async move {
                let prefix = posting_name_prefix(namespace, segment, name);
                let prefix_len = prefix.len();
                let mut records = self
                    .reads()
                    .scan_prefix_iter(prefix, BytesRange::unbounded(), None)
                    .await?;
                let mut values = Vec::new();
                while let Some(record) = records.next().await? {
                    if read.fetch_add(1, Ordering::Relaxed) >= stream_ids.len() {
                        return Ok(None);
                    }
                    let holding = decode_postings(&record.value)? & selected;
                    if holding.is_empty() {
                        continue;
                    }
                    let value = std::str::from_utf8(&record.key[prefix_len..])
                        .map_err(|_| Error::Corrupt("posting value is not UTF-8".to_owned()))?;
                    values.push((Label::new(name, value), holding));
                }
                Ok::<_, Error>(Some(values))
            });
        let mut labels = vec![exact.to_vec(); stream_ids.len()];
        for values in futures::future::try_join_all(scans).await? {
            let Some(values) = values else {
                return Ok(None);
            };
            for (label, holding) in values {
                for stream_id in &holding {
                    let index = stream_ids.binary_search(&stream_id).map_err(|_| {
                        Error::Corrupt("posting intersection left the selection".to_owned())
                    })?;
                    labels[index].push(label.clone());
                }
            }
        }
        stream_ids
            .iter()
            .zip(labels)
            .map(|(stream_id, labels)| Ok((*stream_id, Arc::new(Labels::new(labels)?))))
            .collect::<Result<_>>()
            .map(Some)
    }

    /// Runs of `stream_ids` (ascending) passing `keep`, from one scan over
    /// `span` of the segment's run records. Iterating a wide span is
    /// CPU-bound, so it runs as its own task: polled inside the query's task,
    /// every segment's scan would share one core.
    async fn spanned_runs(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        spans: Vec<BytesRange>,
        stream_ids: Vec<StreamId>,
        keep: impl Fn(&StoredRun) -> bool + Send + Sync + 'static,
    ) -> Result<HashMap<StreamId, Vec<(u64, StoredRun)>>> {
        let storage = self.reads();
        let prefix = segment_run_prefix(namespace, segment);
        tokio::spawn(async move {
            let (stream_ids, keep) = (&stream_ids, &keep);
            let scans = spans.into_iter().map(|span| {
                let (storage, prefix) = (&storage, prefix.clone());
                async move {
                    let mut runs = Vec::new();
                    let mut records = storage.scan_prefix_iter(prefix, span, None).await?;
                    while let Some(record) = records.next().await? {
                        let (stream_id, object_id) = decode_run_key(&record.key)?;
                        if stream_ids.binary_search(&stream_id).is_err() {
                            continue;
                        }
                        let run = decode_run(&record.value)?;
                        if keep(&run) {
                            runs.push((stream_id, object_id, run));
                        }
                    }
                    Ok::<_, Error>(runs)
                }
            });
            let mut runs = HashMap::<StreamId, Vec<_>>::new();
            for (stream_id, object_id, run) in futures::future::try_join_all(scans)
                .await?
                .into_iter()
                .flatten()
            {
                runs.entry(stream_id).or_default().push((object_id, run));
            }
            Ok(runs)
        })
        .await
        .map_err(|error| Error::Invalid(format!("run scan task failed: {error}")))?
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
            .reads()
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
        let mut iterator = self.reads().scan_prefix_iter(prefix, range, None).await?;
        let mut labelled = Vec::new();
        while let Some(record) = iterator.next().await? {
            labelled.push((
                decode_id(&record.key)?,
                Arc::new(decode_labels(&record.value)?),
            ));
        }
        Ok(labelled)
    }

    /// Ranges covering `stream_ids` (ascending) when range scans over them
    /// are cheaper than a point operation per stream. The IDs split at their
    /// widest gaps of more than `span_scan_max_spread` unselected IDs, at
    /// most [`SPAN_SCAN_MAX_SPANS`] spans, since every scan costs its own
    /// object-store reads; the spans must average at least
    /// `span_scan_min_streams` selected IDs and cover at most
    /// `span_scan_max_spread` IDs per selected one. A scanned record costs
    /// about an eighth of a point operation; IDs are allocated in locality
    /// order, so a selector on a coarse label covers a few dense spans.
    pub(super) fn span_scans(&self, stream_ids: &[StreamId]) -> Option<Vec<BytesRange>> {
        if stream_ids.len() < self.span_scan_min_streams {
            return None;
        }
        let mut splits: Vec<(StreamId, usize)> = stream_ids
            .windows(2)
            .enumerate()
            .map(|(index, pair)| (pair[1] - pair[0], index + 1))
            .filter(|(gap, _)| *gap as usize > self.span_scan_max_spread)
            .collect();
        if splits.len() >= SPAN_SCAN_MAX_SPANS {
            splits.select_nth_unstable_by(SPAN_SCAN_MAX_SPANS - 1, |left, right| {
                right.0.cmp(&left.0)
            });
            splits.truncate(SPAN_SCAN_MAX_SPANS - 1);
        }
        let mut bounds: Vec<usize> = splits.into_iter().map(|(_, index)| index).collect();
        bounds.sort_unstable();
        bounds.push(stream_ids.len());
        let mut spans = Vec::with_capacity(bounds.len());
        let mut covered = 0usize;
        let mut first = 0;
        for end in bounds {
            let (low, high) = (stream_ids[first], stream_ids[end - 1]);
            covered += (high - low) as usize + 1;
            let end_key = match high.checked_add(1) {
                Some(next) => Bound::Excluded(Bytes::copy_from_slice(&next.to_be_bytes())),
                None => Bound::Unbounded,
            };
            spans.push(BytesRange::new(
                Bound::Included(Bytes::copy_from_slice(&low.to_be_bytes())),
                end_key,
            ));
            first = end;
        }
        (spans.len().saturating_mul(self.span_scan_min_streams) <= stream_ids.len()
            && covered <= stream_ids.len().saturating_mul(self.span_scan_max_spread))
        .then_some(spans)
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
    /// The stream's rows from the segment's summary, read instead of its
    /// runs, which are then left empty.
    counts: Option<StreamCounts>,
}

/// One stream's [`SummaryBucket`]s, buckets `bucket_ns` wide.
struct StreamCounts {
    bucket_ns: i64,
    buckets: Vec<SummaryBucket>,
}

impl StreamCounts {
    /// Adds a sample of each bucket overlapping `[start, end]`, at its last
    /// instant within it. Summaries are read only when no window boundary
    /// falls inside a bucket, so every window holds all of a bucket's rows
    /// or none of them.
    fn samples(
        &self,
        stream_id: StreamId,
        labels: &Arc<Labels>,
        (start, end): (i64, i64),
        sink: &mut impl SampleSink,
    ) {
        let none = crate::Fields::default();
        for bucket in &self.buckets {
            let first = bucket
                .bucket
                .saturating_mul(self.bucket_ns)
                .saturating_add(1);
            let last = first.saturating_add(self.bucket_ns - 1);
            if last < start || first > end {
                continue;
            }
            let (mut rows, mut line_bytes) = (bucket.rows, bucket.line_bytes);
            while rows > 0 {
                let part = u32::try_from(rows).unwrap_or(u32::MAX);
                sink.sample(stream_id, labels, last.min(end), (part, line_bytes), &none);
                rows -= u64::from(part);
                line_bytes = 0;
            }
        }
    }
}

impl SegmentStream {
    /// Whether a row may fail a metric query: it may carry `__error__`
    /// metadata, or the stream an error label.
    fn may_fail(&self) -> bool {
        self.runs.iter().any(|(_, run)| run.error_metadata)
            || self.labels.iter().any(|label| is_error_label(&label.name))
    }
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

impl ReadUnit {
    /// Estimated bytes its blocks and what they decode to hold: encoded
    /// blocks, spread over the unselected ones it spans, plus their meta
    /// and, when read, lines.
    fn decode_bytes(&self, lines: bool) -> u64 {
        let (bytes, blocks, line_bytes) =
            self.runs
                .iter()
                .fold((0u64, 0u64, 0u64), |(bytes, blocks, line_bytes), run| {
                    (
                        bytes + u64::from(run.run.bytes),
                        blocks + u64::from(run.run.blocks),
                        line_bytes + run.run.line_bytes,
                    )
                });
        let span = u64::from(self.end_block.saturating_sub(self.first_block));
        let encoded = bytes.saturating_mul(span) / blocks.max(1);
        encoded.saturating_mul(2) + if lines { line_bytes } else { 0 }
    }
}

/// Candidate rows by `(stream, object)` run, from [`LogDb::line_candidates`].
type Candidates = HashMap<(StreamId, u64), HashSet<u32>>;

/// A read unit of [`LogDb::read_segments_pipelined`], with its segment and
/// the bound of the segment's next unit, `None` for its last.
struct PipelinedUnit {
    segment: SegmentId,
    ordinal: u32,
    next_bound: Option<i64>,
    candidates: Arc<Option<Candidates>>,
    unit: ReadUnit,
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

/// Which structured metadata a [`SampleRead`] needs. Metric queries promote
/// metadata to labels, so a field is needed when it can become a label the
/// query's series keep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MetadataNeeds {
    None,
    /// Only fields that can become one of these labels, sorted and unique.
    Names(Vec<String>),
    All,
}

impl MetadataNeeds {
    /// The fields a stream labelled `labels` needs read. Metadata `m` is
    /// promoted to label `m`, or to `m_extracted` when a label `m` exists,
    /// whether a stream label or promoted from metadata earlier in name
    /// order. So label `n` can come from metadata `n` when the stream has no
    /// label `n`, and from metadata `n` with any number of `_extracted`
    /// suffixes stripped.
    pub(super) fn fields(&self, labels: &Labels) -> FieldSelection {
        let names = match self {
            Self::None => return FieldSelection::None,
            Self::All => return FieldSelection::All,
            Self::Names(names) => names,
        };
        let mut fields = Vec::new();
        for name in names {
            if !labels.iter().any(|label| label.name == *name) {
                fields.push(name.clone());
            }
            let mut stripped = name.as_str();
            while let Some(shorter) = stripped
                .strip_suffix(EXTRACTED_SUFFIX)
                .filter(|shorter| !shorter.is_empty())
            {
                fields.push(shorter.to_owned());
                stripped = shorter;
            }
        }
        if fields.is_empty() {
            return FieldSelection::None;
        }
        fields.sort_unstable();
        fields.dedup();
        FieldSelection::Only(fields.into())
    }
}

const EXTRACTED_SUFFIX: &str = "_extracted";

/// What [`LogDb::read_samples`] reads for a metric query that needs no line
/// content.
#[derive(Clone, Debug)]
pub(crate) struct SampleRead {
    /// Which of rows' structured metadata is needed.
    pub metadata: MetadataNeeds,
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

    /// Whether every window boundary is a multiple of `width`, so no window
    /// holds part of a `width`-wide summary bucket.
    pub(crate) fn aligned_to(&self, width: i64) -> bool {
        width > 0
            && self.boundaries.iter().all(|boundaries| {
                boundaries.count == 0
                    || (boundaries.first.rem_euclid(width) == 0
                        && (boundaries.count == 1 || boundaries.step.rem_euclid(width) == 0))
            })
    }
}

/// Where [`LogDb::read_samples`] puts what it reads.
trait SampleSink {
    /// Rows of stream `stream_id` that every evaluation window holds all or
    /// none of: `rows` stored rows from `timestamp_ns` on, with `line_bytes`
    /// of lines between them.
    fn sample(
        &mut self,
        stream_id: StreamId,
        labels: &Arc<Labels>,
        timestamp_ns: i64,
        weight: (u32, u64),
        metadata: &crate::Fields,
    );
}

impl SampleSink for StreamBatches {
    fn sample(
        &mut self,
        _: StreamId,
        labels: &Arc<Labels>,
        timestamp_ns: i64,
        (rows, line_bytes): (u32, u64),
        metadata: &crate::Fields,
    ) {
        let metadata = metadata
            .iter()
            .map(|field| (field.name.as_str(), field.value.as_str()));
        self.push(labels, timestamp_ns, "", metadata, Some((rows, line_bytes)));
    }
}

/// Streams [`LogDb::count_segment`] counts per read.
const COUNT_STREAMS: usize = 256;

/// What [`LogDb::count_segment`] found.
#[derive(Default)]
pub(super) struct SegmentCounts {
    /// Every stream with live rows, in ID order.
    pub(super) streams: Vec<(StreamId, StreamSummary)>,
    /// Earliest expiry of the runs counted.
    pub(super) earliest: Option<u64>,
    /// Latest expiry of the runs counted, `None` inside when one never
    /// expires.
    pub(super) latest: Option<Option<u64>>,
}

impl SegmentCounts {
    fn expiry(&mut self, expires_at_unix_ms: Option<u64>) {
        if let Some(at) = expires_at_unix_ms {
            self.earliest = Some(self.earliest.map_or(at, |earliest| earliest.min(at)));
        }
        self.latest = Some(match (self.latest, expires_at_unix_ms) {
            (None, at) => at,
            (Some(Some(latest)), Some(at)) => Some(latest.max(at)),
            _ => None,
        });
    }
}

/// Deduplicated rows and line bytes per summary bucket of each stream.
struct SummarySink {
    summary: SegmentSummary,
    streams: HashMap<StreamId, BTreeMap<i64, (u64, u64)>>,
}

impl SampleSink for SummarySink {
    fn sample(
        &mut self,
        stream_id: StreamId,
        _: &Arc<Labels>,
        timestamp_ns: i64,
        (rows, line_bytes): (u32, u64),
        _: &crate::Fields,
    ) {
        let bucket = self
            .streams
            .entry(stream_id)
            .or_default()
            .entry(self.summary.bucket(timestamp_ns))
            .or_default();
        bucket.0 += u64::from(rows);
        bucket.1 += line_bytes;
    }
}

/// How a sample-only metric query reads each run of one segment.
struct SamplePlan {
    /// Runs no other run of their stream overlaps, by `(stream, object)`,
    /// whose rows need no deduplication.
    isolated: HashSet<(StreamId, u64)>,
    /// The structured metadata fields read from each stream that reads any.
    with_metadata: HashMap<StreamId, FieldSelection>,
    /// Runs whose blocks are decoded, by `(stream, object, first block)`.
    decoded: HashSet<(StreamId, u64, u32)>,
}

impl SamplePlan {
    /// Plans the runs of `streams`, adding to `batches` those counted from
    /// their index entries alone: isolated runs that read no metadata and
    /// that no evaluation window splits.
    fn new(streams: &[SegmentStream], read: &SampleRead, batches: &mut impl SampleSink) -> Self {
        let mut plan = Self {
            isolated: HashSet::new(),
            with_metadata: HashMap::new(),
            decoded: HashSet::new(),
        };
        for stream in streams {
            // `__error__` metadata fails the query, and an errored row keeps
            // every error label, so a stream that may hold one reads all of
            // its runs' metadata; deduplication compares rows across runs, so
            // it must see the same fields of each.
            let fields = if stream.may_fail() {
                FieldSelection::All
            } else {
                read.metadata.fields(&stream.labels)
            };
            let metadata = !fields.is_none();
            if metadata {
                plan.with_metadata.insert(stream.stream_id, fields);
            }
            let alone = isolated_runs(&stream.runs);
            for (&(object_id, run), alone) in stream.runs.iter().zip(alone) {
                if alone && !metadata && !read.splits(run.min_timestamp_ns, run.max_timestamp_ns) {
                    batches.sample(
                        stream.stream_id,
                        &stream.labels,
                        run.min_timestamp_ns,
                        (run.rows, run.line_bytes),
                        &crate::Fields::default(),
                    );
                    continue;
                }
                if alone {
                    plan.isolated.insert((stream.stream_id, object_id));
                }
                plan.decoded
                    .insert((stream.stream_id, object_id, run.first_block));
            }
        }
        plan
    }

    /// Read units of `streams` trimmed to the runs that are decoded.
    ///
    /// Planned over every run, as the query's page estimate is: dropping
    /// counted runs first could split one unit into two and overrun it.
    /// Sample-only metric queries skip line payloads, so one wider range per
    /// object is cheaper than many sparse object-store reads.
    fn units(&self, streams: &[SegmentStream]) -> Vec<ReadUnit> {
        plan_reads(streams, false, u32::MAX)
            .into_iter()
            .filter_map(|mut unit| {
                unit.runs.retain(|run| {
                    self.decoded
                        .contains(&(run.stream_id, run.object_id, run.run.first_block))
                });
                unit.first_block = unit.runs.iter().map(|run| run.run.first_block).min()?;
                unit.end_block = unit
                    .runs
                    .iter()
                    .map(|run| run.run.first_block + run.run.blocks)
                    .max()?;
                Some(unit)
            })
            .collect()
    }
}

fn push_row_sample(
    batches: &mut impl SampleSink,
    stream_id: StreamId,
    labels: &Arc<Labels>,
    sample: &RowSample,
) {
    let weight = (1, sample.line_len.into());
    batches.sample(
        stream_id,
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
    /// require. Only the structured metadata [`SampleRead::metadata`] needs
    /// is read.
    ///
    /// Queries drop rows of a stream that repeat an earlier row's timestamp,
    /// line and structured metadata. A run that is duplicate-free and whose
    /// time range meets no other run of its stream holds no such row, so it
    /// is counted from its run record when no window boundary splits it, and
    /// otherwise each of its blocks is counted from its header or, when split,
    /// decoded from its meta value alone. Other runs are decoded from their
    /// meta values, and their lines are read only for a stream where two rows
    /// share a timestamp, line length and the structured metadata read, if
    /// any: rows that differ in that differ in their full metadata too.
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
    async fn read_segment_samples<S: SampleSink>(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: Vec<SegmentStream>,
        range: (i64, i64),
        budget: &PageBudget,
        read: &SampleRead,
        batches: &mut S,
        consume: &mut impl FnMut(&mut S) -> Result<()>,
    ) -> Result<()> {
        for stream in &streams {
            if let Some(counts) = &stream.counts {
                counts.samples(stream.stream_id, &stream.labels, range, batches);
            }
        }
        let plan = SamplePlan::new(&streams, read, batches);
        consume(batches)?;
        let units = plan.units(&streams);
        let shared_read = Arc::new(read.clone());
        let SamplePlan {
            isolated,
            with_metadata,
            ..
        } = plan;
        let (isolated, with_metadata) = (&isolated, &with_metadata);
        let mut loaded = futures::stream::iter(units)
            .map(|unit| {
                let read = Arc::clone(&shared_read);
                async move {
                    budget.take()?;
                    let decoding = budget
                        .decode(&self.memory, unit.decode_bytes(false))
                        .await?;
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
                            let fields = with_metadata
                                .get(&run.stream_id)
                                .cloned()
                                .unwrap_or(FieldSelection::None);
                            let alone = isolated.contains(&(run.stream_id, run.object_id));
                            (run, fields, alone)
                        })
                        .collect::<Vec<_>>();
                    // Decoded on a task of its own: polled here, every unit of
                    // every segment would decode on the query's one task.
                    let first_block = unit.first_block;
                    let runs = tokio::spawn(async move {
                        runs.into_iter()
                            .map(|(run, fields, alone)| {
                                let first = (run.run.first_block - first_block) as usize;
                                let blocks = &blocks[first..first + run.run.blocks as usize];
                                let samples = if alone {
                                    RunSamples::Isolated(isolated_samples(
                                        blocks, range, &read, &fields,
                                    )?)
                                } else {
                                    RunSamples::Unsure(run_samples(blocks, range, &fields)?)
                                };
                                Ok::<_, Error>((run, samples))
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .await
                    .map_err(|error| {
                        Error::Invalid(format!("sample decode task failed: {error}"))
                    })??;
                    Ok::<_, Error>((decoding, runs))
                }
            })
            .buffer_unordered(PAGE_READ_CONCURRENCY);
        let mut unsure = HashMap::<StreamId, Vec<(UnitRun, Vec<RowSample>)>>::new();
        while let Some((_decoding, runs)) = loaded.try_next().await? {
            for (run, samples) in runs {
                match samples {
                    RunSamples::Isolated(samples) => {
                        for sample in samples {
                            batches.sample(
                                run.stream_id,
                                &run.labels,
                                sample.timestamp_ns,
                                sample.weight,
                                &sample.metadata,
                            );
                        }
                    }
                    RunSamples::Unsure(samples) => unsure
                        .entry(run.stream_id)
                        .or_default()
                        .push((run, samples)),
                }
            }
            consume(batches)?;
        }
        drop(loaded);
        for (stream_id, runs) in unsure {
            let fields = with_metadata
                .get(&stream_id)
                .unwrap_or(&FieldSelection::None);
            self.deduplicated_samples(namespace, segment, runs, range, fields, batches)
                .await?;
            consume(batches)?;
        }
        Ok(())
    }

    /// Every stream of `segment` with live rows, counted in the buckets of
    /// `summary` and deduplicated as lineless reads count them, by a read
    /// with a window boundary at the end of every bucket; `None` for a
    /// stream with a row that may fail a query, which queries read in full.
    pub(super) async fn count_segment(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        summary: &SegmentSummary,
    ) -> Result<SegmentCounts> {
        let range = (
            segment.start_ns,
            segment
                .start_ns
                .saturating_add(segment.width_ns)
                .saturating_sub(1),
        );
        let (first, last) = (summary.bucket(range.0), summary.bucket(range.1));
        let read = SampleRead {
            metadata: MetadataNeeds::None,
            boundaries: vec![Boundaries {
                first: first.saturating_add(1).saturating_mul(summary.bucket_ns),
                step: summary.bucket_ns,
                count: u64::try_from(last - first).unwrap_or(0),
            }],
        };
        let budget = PageBudget::new(usize::MAX);
        let mut counts = SegmentCounts::default();
        for targets in self
            .segment_reads(namespace, segment, COUNT_STREAMS)
            .await?
        {
            for (_, streams) in targets.segments {
                let (failing, counted): (Vec<_>, Vec<_>) =
                    streams.into_iter().partition(SegmentStream::may_fail);
                counts
                    .streams
                    .extend(failing.iter().map(|stream| (stream.stream_id, None)));
                for (_, run) in counted.iter().flat_map(|stream| &stream.runs) {
                    counts.expiry(run.expires_at_unix_ms);
                }
                let mut sink = SummarySink {
                    summary: *summary,
                    streams: HashMap::new(),
                };
                self.read_segment_samples(
                    namespace,
                    segment,
                    counted,
                    range,
                    &budget,
                    &read,
                    &mut sink,
                    &mut |_| Ok(()),
                )
                .await?;
                counts
                    .streams
                    .extend(sink.streams.into_iter().map(|(stream_id, buckets)| {
                        let buckets = buckets
                            .into_iter()
                            .map(|(bucket, (rows, line_bytes))| SummaryBucket {
                                bucket,
                                rows,
                                line_bytes,
                            })
                            .collect();
                        (stream_id, Some(buckets))
                    }));
            }
        }
        counts
            .streams
            .sort_unstable_by_key(|(stream_id, _)| *stream_id);
        Ok(counts)
    }

    /// Adds samples of one stream's runs that may hold duplicates to
    /// `batches`, deduplicated by their full structured metadata, whose
    /// `fields` the samples hold.
    async fn deduplicated_samples(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        runs: Vec<(UnitRun, Vec<RowSample>)>,
        range: (i64, i64),
        fields: &FieldSelection,
        batches: &mut impl SampleSink,
    ) -> Result<()> {
        let Some((stream_id, labels)) = runs
            .first()
            .map(|(run, _)| (run.stream_id, Arc::clone(&run.labels)))
        else {
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
                push_row_sample(batches, stream_id, &labels, sample);
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
        for entry in &entries {
            batches.sample(
                stream_id,
                &labels,
                entry.timestamp_ns,
                (1, entry.line.len() as u64),
                &fields.select(&entry.structured_metadata),
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
    blocks: &[Block],
    range: (i64, i64),
    read: &SampleRead,
    fields: &FieldSelection,
) -> Result<Vec<WeightedSample>> {
    let mut samples = Vec::new();
    for block in blocks {
        let header = block_header(&block.meta)?;
        if !header.overlaps(range.0, range.1) {
            continue;
        }
        if fields.is_none() && !read.splits(header.min_timestamp_ns, header.max_timestamp_ns) {
            samples.push(WeightedSample {
                timestamp_ns: header.min_timestamp_ns,
                weight: (header.rows, header.line_bytes.into()),
                metadata: crate::Fields::default(),
            });
            continue;
        }
        decode_samples_where(
            block,
            range,
            |_| true,
            fields,
            |_, sample| {
                samples.push(WeightedSample {
                    timestamp_ns: sample.timestamp_ns,
                    weight: (1, sample.line_len.into()),
                    metadata: sample.structured_metadata,
                })
            },
        )?;
    }
    Ok(samples)
}

/// `weight` stored rows from `timestamp_ns` on, as [`SampleSink::sample`]
/// takes them.
struct WeightedSample {
    timestamp_ns: i64,
    weight: (u32, u64),
    metadata: crate::Fields,
}

/// A run's decoded samples: an isolated run's are final, others may repeat
/// rows of another run of the stream.
enum RunSamples {
    Isolated(Vec<WeightedSample>),
    Unsure(Vec<RowSample>),
}

/// Every row sample of a run's blocks in `range`, from their meta values.
fn run_samples(
    blocks: &[Block],
    range: (i64, i64),
    fields: &FieldSelection,
) -> Result<Vec<RowSample>> {
    let mut samples = Vec::new();
    for block in blocks {
        if !block_header(&block.meta)?.overlaps(range.0, range.1) {
            continue;
        }
        decode_samples_where(
            block,
            range,
            |_| true,
            fields,
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
    /// Position in the whole read of the first of `segments`.
    first_ordinal: usize,
}

impl ScanTargets {
    /// The segments read, or rollup periods read from their summaries.
    #[cfg(test)]
    pub(super) fn segment_ids(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.segments.iter().map(|(segment, _)| *segment)
    }

    pub(super) fn streams(&self) -> impl Iterator<Item = (StreamId, &Labels)> {
        self.segments.iter().flat_map(|(_, streams)| {
            streams
                .iter()
                .map(|stream| (stream.stream_id, stream.labels.as_ref()))
        })
    }
}

impl ScanTargets {
    /// One target per segment, each keeping its segment's position in the
    /// read, so reading them apart breaks ties as reading them together does.
    pub(crate) fn split(self) -> Vec<ScanTargets> {
        let Self {
            range,
            segments,
            line_terms,
            first_ordinal,
        } = self;
        (first_ordinal..)
            .zip(segments)
            .map(|(ordinal, segment)| ScanTargets {
                range,
                segments: vec![segment],
                line_terms: line_terms.clone(),
                first_ordinal: ordinal,
            })
            .collect()
    }

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
trait PendingReads: 'static {
    type Run: Send + 'static;
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

/// Output of [`LogDb::select_streams`].
enum Selection {
    Labelled(SegmentStreams, bool),
    Listed(Vec<StreamId>, PartitionVersion),
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
    /// Sorted names of the only stream labels the query depends on, every
    /// label the selectors match on among them. Selected streams may then
    /// carry just these labels.
    labels: Option<Arc<[String]>>,
    /// The lineless read of a query that may count streams from segment
    /// summaries; see [`LogDb::segment_streams`].
    summaries: Option<Arc<SampleRead>>,
}

impl StreamFilter {
    pub(crate) fn exact(exact: Vec<Label>) -> Self {
        Self {
            exact,
            selectors: Vec::new(),
            line_terms: None,
            labels: None,
            summaries: None,
        }
    }

    pub(crate) fn with_summaries(self, read: Option<&SampleRead>) -> Self {
        Self {
            summaries: read.cloned().map(Arc::new),
            ..self
        }
    }

    pub(crate) fn with_line_terms(self, line_terms: Option<Vec<String>>) -> Self {
        Self { line_terms, ..self }
    }

    pub(crate) fn with_labels(self, labels: Option<Vec<String>>) -> Self {
        Self {
            labels: labels.map(Arc::from),
            ..self
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
            line_terms: None,
            labels: None,
            summaries: None,
        })
    }

    fn matches(&self, labels: &Labels) -> bool {
        self.selectors.is_empty()
            || self
                .selectors
                .iter()
                .any(|selector| selector.matches(labels))
    }

    /// Whether every stream holding the exact labels [`Self::matches`].
    fn selects_exact(&self) -> bool {
        self.selectors.is_empty()
            || self.selectors.iter().any(|selector| {
                selector.matchers.iter().all(|matcher| {
                    matches!(matcher, SeriesMatcher::Equal(name, value)
                        if self.exact.iter().any(|label| label.name == *name && label.value == *value))
                })
            })
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
