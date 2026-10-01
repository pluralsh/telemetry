// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Writer-side merging of each stream's small pages.
//!
//! The compactor runs inside the write flusher after every flush, so merges
//! are serialized with page writes and never race a sequence allocation. Its
//! in-memory index of small pages is rebuilt from page metadata the first time
//! a flush touches a segment, which also re-queues the segment's pending
//! payload deletions from their tombstones.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::BytesRange;
use common::storage::{RecordOp, Storage, Ttl, WriteOptions};

use crate::Namespace;
use crate::codec::{
    PageId, StoredPageMetadata, decode_deadline, decode_metadata, decode_metadata_key,
    decode_tombstone_key, encode_deadline, encode_metadata, metadata_key, payload_key,
    segment_metadata_prefix, tombstone_key, tombstone_prefix,
};
use crate::config::{CompactionConfig, PageConfig};
use crate::error::{Error, Result};
use crate::model::{SegmentId, StreamId};
use crate::page::Page;

/// Deletions applied per storage write.
const DELETE_BATCH: usize = 1024;

/// A page written by a flush, reported to the compactor.
pub(crate) struct WrittenPage {
    pub namespace: Namespace,
    pub segment: SegmentId,
    pub stream_id: StreamId,
    pub page_id: PageId,
    pub metadata: StoredPageMetadata,
}

#[derive(Clone, Debug)]
struct Tracked {
    page_id: PageId,
    metadata: StoredPageMetadata,
}

struct SegmentState {
    /// Small pages of each stream, ordered by sequence.
    streams: BTreeMap<StreamId, Vec<Tracked>>,
    touched: Instant,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingDelete {
    deadline_unix_ms: u64,
    payload: Bytes,
    tombstone: Bytes,
}

pub(crate) struct Compactor {
    storage: Arc<dyn Storage>,
    config: CompactionConfig,
    page: PageConfig,
    segment_ns: i64,
    segments: HashMap<(Namespace, SegmentId), SegmentState>,
    deletes: BinaryHeap<Reverse<PendingDelete>>,
}

impl Compactor {
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        config: CompactionConfig,
        page: PageConfig,
        segment_ns: i64,
    ) -> Self {
        Self {
            storage,
            config,
            page,
            segment_ns,
            segments: HashMap::new(),
            deletes: BinaryHeap::new(),
        }
    }

    /// Tracks `written`, applies due deletions, and performs merges up to the
    /// per-flush budget. Failures are logged rather than failing the flush,
    /// whose pages are already written; the index is rebuilt from storage.
    pub(crate) async fn after_flush(&mut self, written: Vec<WrittenPage>) {
        if let Err(error) = self.run(written).await {
            tracing::warn!(%error, "line page compaction failed; rebuilding its index");
            ::metrics::counter!("logs_compaction_errors_total").increment(1);
            self.segments.clear();
        }
    }

    async fn run(&mut self, written: Vec<WrittenPage>) -> Result<()> {
        let now = Instant::now();
        for page in written {
            self.track(page, now).await?;
        }
        let now_unix_ms = unix_time_ms()?;
        self.apply_due_deletes(now_unix_ms).await?;
        let mut budget = self.config.max_merges_per_flush;
        let keys = self.segments.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if budget == 0 {
                break;
            }
            let Some(mut state) = self.segments.remove(&key) else {
                continue;
            };
            let mut deletes = Vec::new();
            let result = self
                .compact_segment(&key, &mut state, now_unix_ms, &mut budget, &mut deletes)
                .await;
            self.segments.insert(key, state);
            self.deletes.extend(deletes.into_iter().map(Reverse));
            result?;
        }
        self.evict(now, now_unix_ms);
        ::metrics::gauge!("logs_compaction_pending_deletes").set(self.deletes.len() as f64);
        Ok(())
    }

    async fn track(&mut self, page: WrittenPage, now: Instant) -> Result<()> {
        let key = (page.namespace, page.segment);
        if let Some(state) = self.segments.get_mut(&key) {
            state.touched = now;
            if is_small(&self.page, &page.metadata) {
                let pages = state.streams.entry(page.stream_id).or_default();
                let at = pages
                    .partition_point(|tracked| tracked.page_id.sequence < page.page_id.sequence);
                pages.insert(
                    at,
                    Tracked {
                        page_id: page.page_id,
                        metadata: page.metadata,
                    },
                );
            }
            return Ok(());
        }
        // The flush already applied `page`, so recovery's scan includes it.
        let state = self.recover(&key.0, key.1, now).await?;
        self.segments.insert(key, state);
        Ok(())
    }

    /// Rebuilds a segment's small-page index from metadata and re-queues its
    /// tombstoned payloads.
    async fn recover(
        &mut self,
        namespace: &Namespace,
        segment: SegmentId,
        now: Instant,
    ) -> Result<SegmentState> {
        let now_unix_ms = unix_time_ms()?;
        let mut streams: BTreeMap<StreamId, Vec<Tracked>> = BTreeMap::new();
        let mut metadata = self
            .storage
            .scan_prefix_iter(
                segment_metadata_prefix(namespace, segment),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while let Some(record) = metadata.next().await? {
            let (stream_id, page_id) = decode_metadata_key(&record.key)?;
            let metadata = decode_metadata(&record.value)?;
            if is_small(&self.page, &metadata) && !metadata.is_expired_at(now_unix_ms) {
                streams
                    .entry(stream_id)
                    .or_default()
                    .push(Tracked { page_id, metadata });
            }
        }
        for pages in streams.values_mut() {
            pages.sort_by_key(|tracked| tracked.page_id.sequence);
        }
        let mut tombstones = self
            .storage
            .scan_prefix_iter(
                tombstone_prefix(namespace, segment),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while let Some(record) = tombstones.next().await? {
            let (stream_id, page_id, level) = decode_tombstone_key(&record.key)?;
            self.deletes.push(Reverse(PendingDelete {
                deadline_unix_ms: decode_deadline(&record.value)?,
                payload: payload_key(namespace, segment, stream_id, page_id, level),
                tombstone: record.key,
            }));
        }
        Ok(SegmentState {
            streams,
            touched: now,
        })
    }

    async fn apply_due_deletes(&mut self, now_unix_ms: u64) -> Result<()> {
        let mut ops = Vec::new();
        while self
            .deletes
            .peek()
            .is_some_and(|Reverse(next)| next.deadline_unix_ms <= now_unix_ms)
        {
            let Reverse(due) = self.deletes.pop().expect("peeked delete");
            ops.push(RecordOp::Delete(due.payload));
            ops.push(RecordOp::Delete(due.tombstone));
            if ops.len() >= DELETE_BATCH {
                self.apply(std::mem::take(&mut ops)).await?;
            }
        }
        self.apply(ops).await
    }

    async fn compact_segment(
        &self,
        (namespace, segment): &(Namespace, SegmentId),
        state: &mut SegmentState,
        now_unix_ms: u64,
        budget: &mut usize,
        deletes: &mut Vec<PendingDelete>,
    ) -> Result<()> {
        let finalize = self.is_settled(*segment, now_unix_ms);
        for (stream_id, pages) in &mut state.streams {
            pages.retain(|tracked| !tracked.metadata.is_expired_at(now_unix_ms));
            while *budget > 0 {
                let Some(range) = self.select(pages, finalize, now_unix_ms) else {
                    break;
                };
                let (merged, replaced) = self
                    .merge(
                        namespace,
                        *segment,
                        *stream_id,
                        &pages[range.clone()],
                        now_unix_ms,
                    )
                    .await?;
                deletes.extend(replaced);
                let replacement = is_small(&self.page, &merged.metadata).then_some(merged);
                pages.splice(range, replacement);
                *budget -= 1;
            }
        }
        state.streams.retain(|_, pages| !pages.is_empty());
        Ok(())
    }

    /// Whether the segment ended at least `finalize_after` ago, so its
    /// remaining small pages merge regardless of fan-in.
    fn is_settled(&self, segment: SegmentId, now_unix_ms: u64) -> bool {
        let finalize_after_ns =
            i64::try_from(self.config.finalize_after.as_nanos()).unwrap_or(i64::MAX);
        let now_ns = i64::try_from(now_unix_ms)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000);
        segment
            .saturating_add(self.segment_ns)
            .saturating_add(finalize_after_ns)
            <= now_ns
    }

    /// The next run of `pages` to merge: `fan_in` consecutive same-level
    /// pages, or when `finalize` is set the longest mergeable run of at least
    /// two pages whose largest page is at most half of it, so late writes to a
    /// settled segment rewrite each row a logarithmic number of times.
    fn select(&self, pages: &[Tracked], finalize: bool, now_unix_ms: u64) -> Option<Range<usize>> {
        let min_age_ms = u64::try_from(self.config.min_age.as_millis()).unwrap_or(u64::MAX);
        let eligible = |tracked: &Tracked| {
            finalize
                || tracked.metadata.level > 0
                || now_unix_ms.saturating_sub(tracked.metadata.written_at_unix_ms) >= min_age_ms
        };
        for start in 0..pages.len() {
            let first = &pages[start];
            if !eligible(first) {
                continue;
            }
            let mut rows = first.metadata.row_count as usize;
            let mut bytes = first.metadata.payload_bytes as usize;
            let mut end = start + 1;
            while end < pages.len() && (finalize || end - start < self.config.fan_in) {
                let next = &pages[end];
                rows += next.metadata.row_count as usize;
                bytes += next.metadata.payload_bytes as usize;
                if !follows(&pages[end - 1], next)
                    || !eligible(next)
                    || (!finalize && next.metadata.level != first.metadata.level)
                    || rows > self.page.max_rows
                    || bytes > self.page.target_size_bytes
                {
                    break;
                }
                end += 1;
            }
            if finalize {
                // The longest balanced prefix of the mergeable run.
                while end - start >= 2 && !balanced(&pages[start..end]) {
                    end -= 1;
                }
            }
            let wanted = if finalize { 2 } else { self.config.fan_in };
            let level = pages[start..end]
                .iter()
                .map(|tracked| tracked.metadata.level)
                .max()
                .unwrap_or(0);
            if end - start >= wanted && level < u8::MAX {
                return Some(start..end);
            }
        }
        None
    }

    /// Replaces `inputs` with one page in a single atomic write and schedules
    /// their payloads for deletion.
    async fn merge(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_id: StreamId,
        inputs: &[Tracked],
        now_unix_ms: u64,
    ) -> Result<(Tracked, Vec<PendingDelete>)> {
        let payloads = futures::future::try_join_all(inputs.iter().map(|tracked| {
            self.storage.get(payload_key(
                namespace,
                segment,
                stream_id,
                tracked.page_id,
                tracked.metadata.level,
            ))
        }))
        .await?;
        let mut rows = Vec::new();
        for (tracked, payload) in inputs.iter().zip(payloads) {
            let payload = payload
                .ok_or_else(|| Error::Corrupt("compaction input has no payload".to_owned()))?;
            let entries = Page::decode(payload.value)?.decode_range(i64::MIN, i64::MAX)?;
            if entries.len() != tracked.metadata.row_count as usize {
                return Err(Error::Corrupt(
                    "compaction input row count differs from its metadata".to_owned(),
                ));
            }
            rows.extend(entries);
        }
        let page = Page::from_entries(&rows, self.page.rows_per_block)?;
        let bytes = page.bytes();
        let first = &inputs[0];
        let last = inputs.last().expect("merge has inputs");
        let expires_at_unix_ms = inputs
            .iter()
            .map(|tracked| tracked.metadata.expires_at_unix_ms)
            .collect::<Option<Vec<_>>>()
            .and_then(|expiries| expiries.into_iter().max());
        let metadata = StoredPageMetadata {
            expires_at_unix_ms,
            min_timestamp_ns: first.metadata.min_timestamp_ns,
            max_timestamp_ns: last.metadata.max_timestamp_ns,
            row_count: page.row_count(),
            payload_bytes: u32::try_from(bytes.len())
                .map_err(|_| Error::Invalid("page payload exceeds u32".to_owned()))?,
            level: inputs
                .iter()
                .map(|tracked| tracked.metadata.level)
                .max()
                .unwrap_or(0)
                + 1,
            written_at_unix_ms: now_unix_ms,
            leaf_rows: inputs
                .iter()
                .flat_map(|tracked| tracked.metadata.leaf_row_counts())
                .collect(),
        };
        let ttl = match expires_at_unix_ms {
            Some(expires_at) => Ttl::ExpireAt(i64::try_from(expires_at).unwrap_or(i64::MAX)),
            None => Ttl::NoExpiry,
        };
        let deadline_unix_ms = now_unix_ms.saturating_add(
            u64::try_from(self.config.delete_delay.as_millis()).unwrap_or(u64::MAX),
        );

        let mut ops = Vec::with_capacity(2 + inputs.len() * 2);
        ops.push(RecordOp::put_with_ttl(
            metadata_key(namespace, segment, stream_id, first.page_id),
            encode_metadata(&metadata)?,
            ttl,
        ));
        ops.push(RecordOp::put_with_ttl(
            payload_key(namespace, segment, stream_id, first.page_id, metadata.level),
            bytes,
            ttl,
        ));
        let mut deletes = Vec::with_capacity(inputs.len());
        for tracked in inputs {
            if tracked.page_id.sequence != first.page_id.sequence {
                ops.push(RecordOp::Delete(metadata_key(
                    namespace,
                    segment,
                    stream_id,
                    tracked.page_id,
                )));
            }
            let tombstone = tombstone_key(
                namespace,
                segment,
                stream_id,
                tracked.page_id,
                tracked.metadata.level,
            );
            ops.push(RecordOp::put_with_ttl(
                tombstone.clone(),
                encode_deadline(deadline_unix_ms),
                ttl,
            ));
            deletes.push(PendingDelete {
                deadline_unix_ms,
                payload: payload_key(
                    namespace,
                    segment,
                    stream_id,
                    tracked.page_id,
                    tracked.metadata.level,
                ),
                tombstone,
            });
        }
        self.apply(ops).await?;
        ::metrics::counter!("logs_compaction_merges_total").increment(1);
        ::metrics::counter!("logs_compaction_input_pages_total").increment(inputs.len() as u64);
        ::metrics::counter!("logs_compaction_written_bytes_total")
            .increment(u64::from(metadata.payload_bytes));
        Ok((
            Tracked {
                page_id: first.page_id,
                metadata,
            },
            deletes,
        ))
    }

    async fn apply(&self, ops: Vec<RecordOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        self.storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: false,
                },
            )
            .await?;
        Ok(())
    }

    /// Drops the index of segments that are settled and have not been written
    /// for a segment duration; a later late write rebuilds it from storage.
    fn evict(&mut self, now: Instant, now_unix_ms: u64) {
        let idle = Duration::from_nanos(u64::try_from(self.segment_ns).unwrap_or(0))
            .max(self.config.finalize_after);
        let settled = self
            .segments
            .iter()
            .filter(|((_, segment), state)| {
                self.is_settled(*segment, now_unix_ms) && now.duration_since(state.touched) >= idle
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in settled {
            self.segments.remove(&key);
        }
    }
}

/// Whether `next` directly continues `previous`: the next sequence after
/// every leaf `previous` covers, and no earlier timestamp.
fn follows(previous: &Tracked, next: &Tracked) -> bool {
    previous
        .page_id
        .sequence
        .checked_add(previous.metadata.leaf_count())
        == Some(next.page_id.sequence)
        && previous.metadata.max_timestamp_ns <= next.metadata.min_timestamp_ns
}

/// Whether no page holds more than half of the run's bytes.
fn balanced(run: &[Tracked]) -> bool {
    let sizes = run
        .iter()
        .map(|tracked| u64::from(tracked.metadata.payload_bytes));
    sizes.clone().max().unwrap_or(0).saturating_mul(2) <= sizes.sum::<u64>()
}

/// Pages under half of both limits are worth merging.
fn is_small(page: &PageConfig, metadata: &StoredPageMetadata) -> bool {
    (metadata.payload_bytes as usize).saturating_mul(2) < page.target_size_bytes
        && (metadata.row_count as usize).saturating_mul(2) < page.max_rows
}

fn unix_time_ms() -> Result<u64> {
    common::time::checked_now_ms().map_err(|error| Error::Invalid(error.to_string()))
}
