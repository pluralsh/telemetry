// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Writer-side merging of each segment's small objects.
//!
//! The compactor runs inside the write flusher after every flush, so merges
//! are serialized with object writes and never race an ID allocation. Its
//! in-memory index of small objects is rebuilt from object directories the
//! first time a flush touches a segment, which also re-queues the segment's
//! pending deletions from their tombstones.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::BytesRange;
use common::storage::{ReadHints, RecordOp, Storage, Ttl, WriteOptions};

use crate::Namespace;
use crate::codec::{
    BlockGroup, ObjectRef, StoredObject, block_key, decode_directory_key, decode_object,
    decode_tombstone, decode_tombstone_key, directory_key, directory_prefix, encode_tombstone,
    run_key, tombstone_key, tombstone_prefix,
};
use crate::config::{CompactionConfig, PageConfig};
use crate::error::{Error, Result};
use crate::model::SegmentId;
use crate::object::{
    MergeInput, ObjectLocation, ObjectProperties, ReadNeeds, merge_objects, object_records,
    read_blocks,
};

/// Deletions applied per storage write.
const DELETE_BATCH: usize = 1024;

/// An object written by a flush, reported to the compactor.
pub(crate) struct WrittenObject {
    pub namespace: Namespace,
    pub segment: SegmentId,
    pub object: ObjectRef,
    pub stored: StoredObject,
}

#[derive(Clone, Debug)]
struct Tracked {
    object: ObjectRef,
    stored: StoredObject,
}

struct SegmentState {
    /// Small objects, ordered by ID.
    objects: Vec<Tracked>,
    touched: Instant,
}

/// A replaced object's blocks, directory and tombstone, deleted once
/// in-flight readers can no longer reference them.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingDelete {
    deadline_unix_ms: u64,
    keys: Vec<Bytes>,
}

impl PendingDelete {
    fn new(
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        blocks: u32,
        deadline_unix_ms: u64,
    ) -> Self {
        let mut keys = BlockGroup::ALL
            .into_iter()
            .flat_map(|group| {
                (0..blocks).map(move |block| block_key(namespace, segment, object, group, block))
            })
            .collect::<Vec<_>>();
        keys.push(directory_key(namespace, segment, object));
        keys.push(tombstone_key(namespace, segment, object));
        Self {
            deadline_unix_ms,
            keys,
        }
    }
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
    /// whose objects are already written; the index is rebuilt from storage.
    pub(crate) async fn after_flush(&mut self, written: Vec<WrittenObject>) {
        if let Err(error) = self.run(written).await {
            tracing::warn!(%error, "log object compaction failed; rebuilding its index");
            ::metrics::counter!("logs_compaction_errors_total").increment(1);
            self.segments.clear();
        }
    }

    async fn run(&mut self, written: Vec<WrittenObject>) -> Result<()> {
        let now = Instant::now();
        for object in written {
            self.track(object, now).await?;
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

    async fn track(&mut self, written: WrittenObject, now: Instant) -> Result<()> {
        let key = (written.namespace, written.segment);
        if let Some(state) = self.segments.get_mut(&key) {
            state.touched = now;
            if is_small(&self.page, &written.stored) {
                let at = state
                    .objects
                    .partition_point(|tracked| tracked.object.id < written.object.id);
                state.objects.insert(
                    at,
                    Tracked {
                        object: written.object,
                        stored: written.stored,
                    },
                );
            }
            return Ok(());
        }
        // The flush already applied `written`, so recovery's scan includes it.
        let state = self.recover(&key.0, key.1, now).await?;
        self.segments.insert(key, state);
        Ok(())
    }

    /// Rebuilds a segment's small-object index from its directories and
    /// re-queues its tombstoned objects. A tombstoned object's directory
    /// outlives its replacement, so tombstones are read first.
    async fn recover(
        &mut self,
        namespace: &Namespace,
        segment: SegmentId,
        now: Instant,
    ) -> Result<SegmentState> {
        let now_unix_ms = unix_time_ms()?;
        let mut dead = HashSet::new();
        let mut tombstones = self
            .storage
            .scan_prefix_iter_with(
                tombstone_prefix(namespace, segment),
                BytesRange::unbounded(),
                None,
                ReadHints::UNCACHED,
            )
            .await?;
        while let Some(record) = tombstones.next().await? {
            let object = decode_tombstone_key(&record.key)?;
            let (deadline_unix_ms, blocks) = decode_tombstone(&record.value)?;
            self.deletes.push(Reverse(PendingDelete::new(
                namespace,
                segment,
                object,
                blocks,
                deadline_unix_ms,
            )));
            dead.insert(object);
        }
        let mut objects = Vec::new();
        let mut directories = self
            .storage
            .scan_prefix_iter_with(
                directory_prefix(namespace, segment),
                BytesRange::unbounded(),
                None,
                ReadHints::UNCACHED,
            )
            .await?;
        while let Some(record) = directories.next().await? {
            let object = decode_directory_key(&record.key)?;
            if dead.contains(&object) {
                continue;
            }
            let stored = decode_object(&record.value)?;
            if is_small(&self.page, &stored) && !stored.is_expired_at(now_unix_ms) {
                objects.push(Tracked { object, stored });
            }
        }
        objects.sort_by_key(|tracked| tracked.object.id);
        Ok(SegmentState {
            objects,
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
            ops.extend(due.keys.into_iter().map(RecordOp::Delete));
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
        state
            .objects
            .retain(|tracked| !tracked.stored.is_expired_at(now_unix_ms));
        while *budget > 0 {
            let Some(range) = self.select(&state.objects, finalize, now_unix_ms) else {
                break;
            };
            let (merged, replaced) = self
                .merge(
                    namespace,
                    *segment,
                    &state.objects[range.clone()],
                    now_unix_ms,
                )
                .await?;
            deletes.extend(replaced);
            let replacement = is_small(&self.page, &merged.stored).then_some(merged);
            state.objects.splice(range, replacement);
            *budget -= 1;
        }
        Ok(())
    }

    /// Whether the segment ended at least `finalize_after` ago, so its
    /// remaining small objects merge regardless of fan-in.
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

    /// The next run of `objects` to merge: `fan_in` adjacent same-level
    /// objects, fewer when the next one would exceed the page limits, or when
    /// `finalize` is set the longest mergeable run of at
    /// least two objects whose largest is at most half of it, so late writes
    /// to a settled segment rewrite each row a logarithmic number of times.
    fn select(
        &self,
        objects: &[Tracked],
        finalize: bool,
        now_unix_ms: u64,
    ) -> Option<Range<usize>> {
        let min_age_ms = u64::try_from(self.config.min_age.as_millis()).unwrap_or(u64::MAX);
        let eligible = |tracked: &Tracked| {
            finalize
                || tracked.object.level > 0
                || now_unix_ms.saturating_sub(tracked.stored.written_at_unix_ms) >= min_age_ms
        };
        for start in 0..objects.len() {
            let first = &objects[start];
            if !eligible(first) {
                continue;
            }
            let mut rows = first.stored.rows();
            let mut bytes = first.stored.bytes();
            let mut end = start + 1;
            let mut full = false;
            while end < objects.len() && (finalize || end - start < self.config.fan_in) {
                let next = &objects[end];
                rows += next.stored.rows();
                bytes += next.stored.bytes();
                if !follows(&objects[end - 1], next)
                    || !eligible(next)
                    || (!finalize && next.object.level != first.object.level)
                {
                    break;
                }
                if rows > self.page.max_rows as u64 || bytes > self.page.target_size_bytes as u64 {
                    full = true;
                    break;
                }
                end += 1;
            }
            if finalize {
                // The longest balanced prefix of the mergeable run.
                while end - start >= 2 && !balanced(&objects[start..end]) {
                    end -= 1;
                }
            }
            // Inputs are each under half the limits, so a run that fills them
            // merges into an object that is no longer small: waiting for
            // `fan_in` inputs that can never fit would strand it until the
            // segment settles.
            let wanted = if finalize || full {
                2
            } else {
                self.config.fan_in
            };
            let level = objects[start..end]
                .iter()
                .map(|tracked| tracked.object.level)
                .max()
                .unwrap_or(0);
            if end - start >= wanted && level < u8::MAX {
                return Some(start..end);
            }
        }
        None
    }

    /// Replaces `inputs` with one object in a single atomic write and
    /// schedules their blocks and directories for deletion. The merged object
    /// takes the first input's ID, so its run records overwrite that input's;
    /// every other input's run records are deleted.
    async fn merge(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        inputs: &[Tracked],
        now_unix_ms: u64,
    ) -> Result<(Tracked, Vec<PendingDelete>)> {
        let started = Instant::now();
        let blocks = futures::future::try_join_all(inputs.iter().map(|tracked| {
            read_blocks(
                self.storage.as_ref(),
                namespace,
                segment,
                tracked.object,
                0..tracked.stored.blocks(),
                ReadNeeds { lines: true },
                ReadHints::UNCACHED,
            )
        }))
        .await?;
        ::metrics::histogram!("logs_compaction_read_seconds")
            .record(started.elapsed().as_secs_f64());
        let merge_inputs = inputs
            .iter()
            .zip(blocks)
            .map(|(tracked, blocks)| MergeInput {
                object_id: tracked.object.id,
                stored: tracked.stored.clone(),
                blocks,
            })
            .collect::<Vec<_>>();
        // Decoding and re-encoding is CPU-bound and can take milliseconds;
        // on the async runtime it would stall queries sharing the worker.
        let started = Instant::now();
        let page = self.page.clone();
        let built = tokio::task::spawn_blocking(move || merge_objects(&page, merge_inputs))
            .await
            .map_err(|error| Error::Invalid(format!("compaction merge task failed: {error}")))??;
        ::metrics::histogram!("logs_compaction_merge_seconds")
            .record(started.elapsed().as_secs_f64());
        if built.runs.is_empty() {
            return Err(Error::Corrupt("compaction inputs hold no rows".to_owned()));
        }

        let first = &inputs[0];
        let last = inputs.last().expect("merge has inputs");
        let object = ObjectRef {
            id: first.object.id,
            level: inputs
                .iter()
                .map(|tracked| tracked.object.level)
                .max()
                .unwrap_or(0)
                + 1,
        };
        let expires_at_unix_ms = inputs
            .iter()
            .map(|tracked| tracked.stored.expires_at_unix_ms)
            .collect::<Option<Vec<_>>>()
            .and_then(|expiries| expiries.into_iter().max());
        let ttl = match expires_at_unix_ms {
            Some(expires_at) => Ttl::ExpireAt(i64::try_from(expires_at).unwrap_or(i64::MAX)),
            None => Ttl::NoExpiry,
        };
        let deadline_unix_ms = now_unix_ms.saturating_add(
            u64::try_from(self.config.delete_delay.as_millis()).unwrap_or(u64::MAX),
        );

        let mut ops = Vec::new();
        let stored = object_records(
            &mut ops,
            ObjectLocation {
                namespace,
                segment,
                object,
            },
            built,
            ObjectProperties {
                expires_at_unix_ms,
                written_at_unix_ms: now_unix_ms,
                span: last.object.id + last.stored.span - first.object.id,
                ttl,
            },
        )?;
        let mut deletes = Vec::with_capacity(inputs.len());
        for tracked in inputs {
            if tracked.object.id != object.id {
                ops.extend(tracked.stored.runs.iter().map(|run| {
                    RecordOp::Delete(run_key(
                        namespace,
                        segment,
                        run.stream_id,
                        tracked.object.id,
                    ))
                }));
            }
            let blocks = tracked.stored.blocks();
            ops.push(RecordOp::put_with_ttl(
                tombstone_key(namespace, segment, tracked.object),
                encode_tombstone(deadline_unix_ms, blocks),
                ttl,
            ));
            deletes.push(PendingDelete::new(
                namespace,
                segment,
                tracked.object,
                blocks,
                deadline_unix_ms,
            ));
        }
        self.apply(ops).await?;
        ::metrics::counter!("logs_compaction_merges_total").increment(1);
        ::metrics::counter!("logs_compaction_input_pages_total").increment(inputs.len() as u64);
        ::metrics::counter!("logs_compaction_written_bytes_total").increment(stored.bytes());
        Ok((Tracked { object, stored }, deletes))
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

/// Whether `next` directly continues `previous`: the next ID after every
/// written object `previous` covers. Merging only such runs keeps every
/// stream's rows in write order.
fn follows(previous: &Tracked, next: &Tracked) -> bool {
    previous.object.id.checked_add(previous.stored.span) == Some(next.object.id)
}

/// Whether no object holds more than half of the run's bytes.
fn balanced(run: &[Tracked]) -> bool {
    let sizes = run.iter().map(|tracked| tracked.stored.bytes());
    sizes.clone().max().unwrap_or(0).saturating_mul(2) <= sizes.sum::<u64>()
}

/// Objects under half of both limits are worth merging.
fn is_small(page: &PageConfig, stored: &StoredObject) -> bool {
    (stored.bytes() as usize).saturating_mul(2) < page.target_size_bytes
        && (stored.rows() as usize).saturating_mul(2) < page.max_rows
}

fn unix_time_ms() -> Result<u64> {
    common::time::checked_now_ms().map_err(|error| Error::Invalid(error.to_string()))
}
