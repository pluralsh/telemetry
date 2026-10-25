// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Per-stream row counts of settled segments, which lineless metric queries
//! whose windows fall on whole buckets read instead of the segments' runs.
//!
//! A summary is stamped with its segment's next object ID. Every write
//! advances it and merges keep it, so a summary counts exactly what reads
//! see until the segment is written again, after which reads ignore it until
//! the next pass rebuilds it.

use std::cmp::Reverse;

use super::*;
use crate::codec::{
    KEY_SCOPE, PeriodSummary, SummaryBucket, decode_dictionary_key, decode_period_summary,
    decode_segment_summary, decode_stream_id, decode_stream_summary, decode_stream_summary_key,
    dictionary_prefix, encode_period_summary, encode_segment_summary, encode_stream_summary,
    period_stream_summary_key, period_summary_key, rollup_dictionary_prefix, stream_summary_key,
    stream_summary_prefix, summary_key,
};

/// Width of summary buckets: queries whose window boundaries all fall on
/// whole minutes read summaries.
pub(crate) const SUMMARY_BUCKET: Duration = Duration::from_secs(60);
/// Segments summarized per pass, newest first, which bounds a pass while a
/// backlog is worked off.
const SUMMARIES_PER_PASS: usize = 32;
/// Stream summaries per storage write.
const SUMMARY_WRITE_BATCH: usize = 1024;

fn summary_failed(namespace: &Namespace, segment: SegmentId, error: &Error) {
    tracing::warn!(%namespace, %segment, %error, "Logs segment summary failed");
    ::metrics::counter!("logs_summary_errors_total").increment(1);
}

impl LogDb {
    /// Summarizes the settled segments reads use that have no current
    /// summary. Returns the segments summarized.
    pub async fn summarize(&self) -> Result<usize> {
        self.summarize_at(unix_time_ms()?).await
    }

    pub(crate) async fn summarize_at(&self, now_unix_ms: u64) -> Result<usize> {
        let (Some(writer), Some(slate)) = (self.writer.clone(), self.storage.slate_read()) else {
            return Ok(0);
        };
        let now_ns = i64::try_from(now_unix_ms)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000);
        let settled_before = now_ns.saturating_sub(duration_ns(self.settle_after)?);
        let expired_before = match self.retention {
            Some(retention) => now_ns.saturating_sub(duration_ns(retention)?),
            None => i64::MIN,
        };
        let mut rollups = HashMap::new();
        let mut segments = Vec::new();
        for (prefix, _) in slate.segment_sizes() {
            let Ok((namespace, segment, _)) = KEY_SCOPE.parse(&prefix) else {
                continue;
            };
            let end_ns = segment.start_ns.saturating_add(segment.width_ns);
            if common::storage::rollup::data_segment(segment) != segment
                || end_ns > settled_before
                || end_ns <= expired_before
            {
                continue;
            }
            let visible = rollups
                .entry(namespace.clone())
                .or_insert_with(|| self.rollups(&namespace))
                .is_visible(segment);
            if visible {
                segments.push((namespace, segment));
            }
        }
        segments.sort_by_key(|(_, segment)| Reverse(segment.start_ns + segment.width_ns));
        segments.dedup();
        {
            let live = segments.iter().collect::<HashSet<_>>();
            self.summarized
                .lock()
                .expect("summary cache poisoned")
                .retain(|key, _| live.contains(key));
        }
        let mut periods = Vec::new();
        if let Some(rollup_ns) = self.rollup_ns {
            for (namespace, segment) in &segments {
                let period = segment_for(segment.start_ns, rollup_ns);
                if period.start_ns.saturating_add(period.width_ns) <= settled_before {
                    periods.push((namespace.clone(), period));
                }
            }
            periods.dedup();
            let live = periods.iter().collect::<HashSet<_>>();
            self.period_summaries
                .lock()
                .expect("summary cache poisoned")
                .retain(|key, _| live.contains(key));
        }
        // A segment that fails to summarize is skipped, so that it cannot
        // keep every older one from being summarized: passes go newest first.
        let mut summarized = 0;
        for (namespace, segment) in segments {
            if summarized == SUMMARIES_PER_PASS {
                break;
            }
            match self
                .summarize_segment(writer.as_ref(), &namespace, segment, now_unix_ms)
                .await
            {
                Ok(true) => summarized += 1,
                Ok(false) => {}
                Err(error) => summary_failed(&namespace, segment, &error),
            }
        }
        let mut periods_summarized = 0;
        for (namespace, period) in periods {
            if periods_summarized == SUMMARIES_PER_PASS {
                break;
            }
            match self
                .summarize_period(writer.as_ref(), &namespace, period)
                .await
            {
                Ok(true) => periods_summarized += 1,
                Ok(false) => {}
                Err(error) => summary_failed(&namespace, period, &error),
            }
        }
        Ok(summarized + periods_summarized)
    }

    /// The segments reads use for `period`, ascending, or `None` when a
    /// sealed rollup reaches past it.
    pub(super) fn period_segments(
        &self,
        rollups: &common::storage::rollup::Rollups,
        period: SegmentId,
    ) -> Option<Vec<SegmentId>> {
        let base = self.segment_ns.max(1);
        let end_ns = period.start_ns.saturating_add(period.width_ns);
        let visible = rollups.visible(
            (0..period.width_ns / base)
                .map(|index| SegmentId::new(period.start_ns + index * base, base)),
        );
        visible
            .iter()
            .all(|segment| {
                segment.start_ns >= period.start_ns
                    && segment.start_ns.saturating_add(segment.width_ns) <= end_ns
            })
            .then_some(visible)
    }

    /// The next object ID of each of `segments`, in order.
    pub(super) async fn generations_of(
        &self,
        namespace: &Namespace,
        segments: Vec<SegmentId>,
    ) -> Result<Vec<(SegmentId, Option<u64>)>> {
        futures::stream::iter(segments)
            .map(|segment| async move {
                let generation = self
                    .reads()
                    .get(next_object_id_key(namespace, segment))
                    .await?
                    .map(|record| decode_object_id(&record.value))
                    .transpose()?;
                Ok::<_, Error>((segment, generation))
            })
            .buffered(self.tuning.list_concurrency)
            .try_collect()
            .await
    }

    /// Sums the current summaries of every segment of `period` into one
    /// keyed by period stream ID, unless that is current or a segment is
    /// not yet summarized. Returns whether it did.
    async fn summarize_period(
        &self,
        writer: &dyn Storage,
        namespace: &Namespace,
        period: SegmentId,
    ) -> Result<bool> {
        let Some(covered) = self.period_segments(&self.rollups(namespace), period) else {
            return Ok(false);
        };
        let bucket_ns = duration_ns(self.summary_bucket)?;
        let generations = self.generations_of(namespace, covered).await?;
        let mut listed = Vec::new();
        let mut expires_at_unix_ms: Option<u64> = None;
        {
            let summarized = self.summarized.lock().expect("summary cache poisoned");
            for (segment, generation) in &generations {
                let Some(generation) = *generation else {
                    continue;
                };
                match summarized.get(&(namespace.clone(), *segment)) {
                    Some(summary)
                        if summary.generation == generation
                            && summary.bucket_ns == bucket_ns
                            && summary.levels =>
                    {
                        listed.push((*segment, generation));
                        if let Some(expiry) = summary.expires_at_unix_ms {
                            expires_at_unix_ms = Some(
                                expires_at_unix_ms.map_or(expiry, |earliest| earliest.min(expiry)),
                            );
                        }
                    }
                    _ => return Ok(false),
                }
            }
        }
        if listed.is_empty() {
            return Ok(false);
        }
        let summary = PeriodSummary {
            segments: listed,
            expires_at_unix_ms,
            bucket_ns,
            levels: true,
        };
        let key = (namespace.clone(), period);
        let cached = self
            .period_summaries
            .lock()
            .expect("summary cache poisoned")
            .get(&key)
            .cloned();
        let stored = match cached {
            Some(stored) => Some(stored),
            None => self
                .storage
                .get(period_summary_key(namespace, period))
                .await?
                .map(|record| decode_period_summary(&record.value))
                .transpose()?,
        };
        if stored.as_ref() == Some(&summary) {
            self.period_summaries
                .lock()
                .expect("summary cache poisoned")
                .insert(key, summary);
            return Ok(false);
        }

        let period_ids = self
            .dictionary(rollup_dictionary_prefix(namespace, period))
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();
        /// Rows and line bytes by bucket and level, `None` once a segment
        /// did not summarize the stream.
        type Summed = Option<BTreeMap<(i64, u8), (u64, u64)>>;
        let mut streams: BTreeMap<StreamId, Summed> = BTreeMap::new();
        for (segment, _) in &summary.segments {
            let fingerprints = self
                .dictionary(dictionary_prefix(namespace, *segment))
                .await?
                .into_iter()
                .map(|(fingerprint, stream_id)| (stream_id, fingerprint))
                .collect::<HashMap<_, _>>();
            let first = summary.bucket(segment.start_ns);
            let prefix = stream_summary_prefix(namespace, *segment);
            let prefix_len = prefix.len();
            let mut records = self
                .reads()
                .scan_prefix_iter(prefix, BytesRange::unbounded(), None)
                .await?;
            while let Some(record) = records.next().await? {
                let stream_id = decode_stream_summary_key(&record.key, prefix_len)?;
                // Writes register every stream in its period too; one that
                // is missing there could not be read by period ID.
                let Some(period_id) = fingerprints
                    .get(&stream_id)
                    .and_then(|fingerprint| period_ids.get(fingerprint))
                else {
                    return Ok(false);
                };
                let entry = streams
                    .entry(*period_id)
                    .or_insert_with(|| Some(BTreeMap::new()));
                match (decode_stream_summary(&record.value, first)?, entry) {
                    (Some(buckets), Some(summed)) => {
                        for bucket in buckets {
                            let summed = summed.entry((bucket.bucket, bucket.level)).or_default();
                            summed.0 += bucket.rows;
                            summed.1 += bucket.line_bytes;
                        }
                    }
                    (None, entry) => *entry = None,
                    (Some(_), None) => {}
                }
            }
        }

        let ttl = match self.retention {
            Some(retention) => Ttl::ExpireAt(
                i64::try_from(unix_time_ms()?)
                    .unwrap_or(i64::MAX)
                    .saturating_add(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX)),
            ),
            None => Ttl::NoExpiry,
        };
        let first = summary.bucket(period.start_ns);
        let mut ops = Vec::with_capacity(streams.len().min(SUMMARY_WRITE_BATCH));
        for (stream_id, buckets) in streams {
            let buckets = buckets.map(|buckets| {
                buckets
                    .into_iter()
                    .map(|((bucket, level), (rows, line_bytes))| SummaryBucket {
                        bucket,
                        level,
                        rows,
                        line_bytes,
                    })
                    .collect()
            });
            ops.push(RecordOp::put_with_ttl(
                period_stream_summary_key(namespace, period, stream_id),
                encode_stream_summary(&buckets, first)?,
                ttl,
            ));
            if ops.len() == SUMMARY_WRITE_BATCH {
                writer.apply(std::mem::take(&mut ops)).await?;
            }
        }
        if !ops.is_empty() {
            writer.apply(ops).await?;
        }
        writer
            .apply(vec![RecordOp::put_with_ttl(
                period_summary_key(namespace, period),
                encode_period_summary(&summary)?,
                ttl,
            )])
            .await?;
        self.period_summaries
            .lock()
            .expect("summary cache poisoned")
            .insert(key, summary);
        ::metrics::counter!("logs_period_summaries_written_total").increment(1);
        Ok(true)
    }

    /// Every entry of the stream dictionary under `prefix`.
    async fn dictionary(&self, prefix: Bytes) -> Result<Vec<(StreamFingerprint, StreamId)>> {
        let prefix_len = prefix.len();
        let mut records = self
            .reads()
            .scan_prefix_iter(prefix, BytesRange::unbounded(), None)
            .await?;
        let mut entries = Vec::new();
        while let Some(record) = records.next().await? {
            entries.push((
                decode_dictionary_key(&record.key, prefix_len)?,
                decode_stream_id(&record.value)?,
            ));
        }
        Ok(entries)
    }

    /// Summarizes `segment` unless its summary is current. Returns whether
    /// it did.
    async fn summarize_segment(
        &self,
        writer: &dyn Storage,
        namespace: &Namespace,
        segment: SegmentId,
        now_unix_ms: u64,
    ) -> Result<bool> {
        let Some(generation) = self
            .storage
            .get(next_object_id_key(namespace, segment))
            .await?
            .map(|record| decode_object_id(&record.value))
            .transpose()?
        else {
            return Ok(false);
        };
        let bucket_ns = duration_ns(self.summary_bucket)?;
        let key = (namespace.clone(), segment);
        // Once its earliest run expires, reads ignore a summary, but it is
        // not rebuilt: that runs out with the segment's retention anyway.
        let current = |summary: &SegmentSummary| {
            summary.bucket_ns == bucket_ns && summary.generation == generation && summary.levels
        };
        let cached = self
            .summarized
            .lock()
            .expect("summary cache poisoned")
            .get(&key)
            .copied();
        let stored = match cached {
            Some(summary) => Some(summary),
            None => self
                .storage
                .get(summary_key(namespace, segment))
                .await?
                .map(|record| decode_segment_summary(&record.value))
                .transpose()?,
        };
        if let Some(stored) = stored.filter(current) {
            self.summarized
                .lock()
                .expect("summary cache poisoned")
                .insert(key, stored);
            return Ok(false);
        }

        let summary = SegmentSummary {
            generation,
            expires_at_unix_ms: None,
            bucket_ns,
            levels: true,
        };
        let counts = self.count_segment(namespace, segment, &summary).await?;
        let summary = SegmentSummary {
            expires_at_unix_ms: counts.earliest,
            ..summary
        };
        let ttl = match counts.latest {
            Some(Some(latest)) => Ttl::ExpireAt(i64::try_from(latest).unwrap_or(i64::MAX)),
            Some(None) => Ttl::NoExpiry,
            None => match self.retention {
                Some(retention) => Ttl::ExpireAt(
                    i64::try_from(now_unix_ms)
                        .unwrap_or(i64::MAX)
                        .saturating_add(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX)),
                ),
                None => Ttl::NoExpiry,
            },
        };
        let first = summary.bucket(segment.start_ns);
        let mut ops = Vec::with_capacity(counts.streams.len().min(SUMMARY_WRITE_BATCH));
        for (stream_id, stream) in &counts.streams {
            ops.push(RecordOp::put_with_ttl(
                stream_summary_key(namespace, segment, *stream_id),
                encode_stream_summary(stream, first)?,
                ttl,
            ));
            if ops.len() == SUMMARY_WRITE_BATCH {
                writer.apply(std::mem::take(&mut ops)).await?;
            }
        }
        if !ops.is_empty() {
            writer.apply(ops).await?;
        }
        // Written last, so reads never take a summary whose streams are
        // only partly written.
        writer
            .apply(vec![RecordOp::put_with_ttl(
                summary_key(namespace, segment),
                encode_segment_summary(&summary)?,
                ttl,
            )])
            .await?;
        self.summarized
            .lock()
            .expect("summary cache poisoned")
            .insert(key, summary);
        ::metrics::counter!("logs_summaries_written_total").increment(1);
        Ok(true)
    }
}
