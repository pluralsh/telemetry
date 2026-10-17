// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: object encoding, the write-coordinator delta and its flusher.

use std::time::Instant;

use common::storage::StorageBatch;

use super::*;
use crate::codec::{decode_dictionary_key, dictionary_prefix, rollup_dictionary_prefix};
use crate::model::{Field, locality_key, locality_order};

/// How long a segment's or rollup period's IDs stay in memory after its
/// last write. A later write reloads them.
const IDS_IDLE_EVICTION: Duration = Duration::from_secs(15 * 60);

/// Turns flushed deltas into one atomic write without reading what the
/// segments already hold.
///
/// The flusher is the shard's only writer, so the IDs it allocates are
/// authoritative in memory. A segment or rollup period is read once, the
/// first time this process writes to it, to resume its counters; postings
/// and search statistics are merge operands, and full-text blocks are new
/// records.
pub(super) struct DirectWriter {
    storage: Arc<dyn StorageRead>,
    writer: Arc<dyn Storage>,
    config: Config,
    /// Discovery rollup period, a whole multiple of the segment duration.
    rollup_ns: Option<i64>,
    segments: HashMap<(Namespace, SegmentId), SegmentIds>,
    periods: HashMap<(Namespace, SegmentId), StreamIds>,
}

/// Most locality keys one segment or rollup period reserves ID blocks for;
/// streams of further keys take plain IDs.
const MAX_BLOCK_KEYS: usize = 4_096;
/// Largest block of stream IDs reserved at once.
const MAX_BLOCK_IDS: u32 = 1 << 16;
/// Blocks are only reserved below this ID, which leaves half the ID space
/// to plain allocation however many IDs blocks leave unused.
const BLOCK_ID_LIMIT: StreamId = StreamId::MAX / 2;

/// The leading stream locality label a stream carries, the first of
/// [`Config::stream_locality_labels`] it has, with its value. Streams
/// sharing one take their IDs from the same blocks.
type LocalityKey = Label;

/// Stream IDs allocated in one segment or rollup period.
///
/// With ID blocks, a stream whose [`LocalityKey`] is new to the partition
/// reserves a block of IDs for its key at `next`, and later streams of the
/// key take the block's next ID, so a key's streams stay in a few dense
/// ranges however many flushes they arrive over. `next` stays past every
/// reserved ID, and every write that adds a stream moves it, as discovery
/// caches take the persisted counter for the partition's version. Blocks
/// live only in memory: a recovered partition resumes at the counter, so
/// IDs left in its blocks are never handed out.
struct StreamIds {
    ids: HashMap<StreamFingerprint, StreamId>,
    next: StreamId,
    blocks: HashMap<LocalityKey, Block>,
    touched: Instant,
}

/// The IDs one locality key reserved in a partition.
#[derive(Default)]
struct Block {
    /// The next ID its streams take, while below `end`.
    next: StreamId,
    end: StreamId,
    /// IDs its blocks reserved so far, the size of its next block.
    reserved: u32,
    /// Streams given IDs from its blocks, which sizes its first block in the
    /// next partition.
    streams: u32,
}

impl StreamIds {
    /// Resumes the IDs persisted under `dictionary` and the `counter` key.
    /// The counter is rewritten by every write that allocates, but may have
    /// expired with an idle segment, so it never trails a persisted ID.
    async fn recover(storage: &dyn StorageRead, dictionary: Bytes, counter: Bytes) -> Result<Self> {
        let mut next = storage
            .get(counter)
            .await?
            .map(|record| decode_stream_id(&record.value))
            .transpose()?
            .unwrap_or(0);
        let mut ids = HashMap::new();
        let mut records = storage
            .scan_prefix_iter(dictionary.clone(), BytesRange::unbounded(), None)
            .await?;
        while let Some(record) = records.next().await? {
            let id = decode_stream_id(&record.value)?;
            next = next.max(id.saturating_add(1));
            ids.insert(decode_dictionary_key(&record.key, dictionary.len())?, id);
        }
        Ok(Self {
            ids,
            next,
            blocks: HashMap::new(),
            touched: Instant::now(),
        })
    }

    fn resolve(&mut self, fingerprint: StreamFingerprint) -> Result<StreamId> {
        if let Some(id) = self.ids.get(&fingerprint) {
            return Ok(*id);
        }
        let id = self.plain_id()?;
        self.ids.insert(fingerprint, id);
        Ok(id)
    }

    fn plain_id(&mut self) -> Result<StreamId> {
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segment exhausted stream IDs".to_owned()))?;
        Ok(id)
    }

    /// Gives `fingerprint` an ID unless it has one: from its `key`'s block
    /// when `block_ids` is positive, reserving one of at least `block_ids`
    /// IDs, or of `hint` for a key's first, when the key has none left.
    /// Returns whether it allocated.
    fn allocate(
        &mut self,
        fingerprint: StreamFingerprint,
        key: Option<LocalityKey>,
        block_ids: u32,
        hint: impl FnOnce(&LocalityKey) -> Option<u32>,
    ) -> Result<bool> {
        if self.ids.contains_key(&fingerprint) {
            return Ok(false);
        }
        let id = match key.filter(|_| block_ids > 0) {
            Some(key) => self.block_id(key, block_ids, hint)?,
            None => self.plain_id()?,
        };
        self.ids.insert(fingerprint, id);
        Ok(true)
    }

    fn block_id(
        &mut self,
        key: LocalityKey,
        block_ids: u32,
        hint: impl FnOnce(&LocalityKey) -> Option<u32>,
    ) -> Result<StreamId> {
        if let Some(block) = self.blocks.get_mut(&key)
            && block.next < block.end
        {
            let id = block.next;
            block.next += 1;
            block.streams += 1;
            return Ok(id);
        }
        let reserved = match self.blocks.get(&key) {
            Some(block) => block.reserved,
            None if self.blocks.len() >= MAX_BLOCK_KEYS => return self.plain_id(),
            None => hint(&key).map_or(0, |streams| streams.saturating_add(streams / 4)),
        };
        let size = reserved.max(block_ids).min(MAX_BLOCK_IDS);
        let start = self.next;
        let Some(end) = start.checked_add(size).filter(|end| *end <= BLOCK_ID_LIMIT) else {
            return self.plain_id();
        };
        self.next = end;
        let block = self.blocks.entry(key).or_default();
        block.next = start + 1;
        block.end = end;
        block.reserved = block.reserved.saturating_add(size);
        block.streams += 1;
        Ok(start)
    }

    /// Ends a write that allocated IDs while the counter was `before`: one
    /// that only took IDs from reserved blocks still moves the counter, so
    /// every write that adds a stream changes it.
    fn finish_allocation(&mut self, before: StreamId) -> Result<()> {
        if self.next == before {
            self.plain_id()?;
        }
        Ok(())
    }
}

/// A segment's stream IDs and its next object ID.
struct SegmentIds {
    streams: StreamIds,
    next_object: u64,
}

impl SegmentIds {
    async fn recover(
        storage: &dyn StorageRead,
        namespace: &Namespace,
        segment: SegmentId,
    ) -> Result<Self> {
        let (streams, next_object) = futures::try_join!(
            StreamIds::recover(
                storage,
                dictionary_prefix(namespace, segment),
                next_stream_id_key(namespace, segment),
            ),
            async {
                storage
                    .get(next_object_id_key(namespace, segment))
                    .await?
                    .map(|record| decode_object_id(&record.value))
                    .transpose()
                    .map(|next| next.unwrap_or(0))
            },
        )?;
        Ok(Self {
            streams,
            next_object,
        })
    }
}

/// Storage operations every flush in the process has written, for profiles.
#[cfg(test)]
pub(super) static FLUSHED_OPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl DirectWriter {
    pub(super) fn new(
        storage: Arc<dyn StorageRead>,
        writer: Arc<dyn Storage>,
        config: Config,
        rollup_ns: Option<i64>,
    ) -> Self {
        Self {
            storage,
            writer,
            config,
            rollup_ns,
            segments: HashMap::new(),
            periods: HashMap::new(),
        }
    }

    /// Writes every group atomically and returns the objects it wrote.
    async fn write_groups(&mut self, groups: FrozenLogsWriteDelta) -> Result<Vec<WrittenObject>> {
        self.load_ids(&groups).await?;
        let FrozenLogsWriteDelta { groups, ops } = groups;
        let ttl = self.ttl()?;
        let retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
            written_at_unix_ms: unix_time_ms()?,
        };
        let mut by_namespace: BTreeMap<Namespace, StreamGroups> = BTreeMap::new();
        for ((namespace, segment, fingerprint), group) in groups {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert((segment, fingerprint), group);
        }
        let mut batch = self.writer.new_batch();
        batch.extend(ops);
        let mut written = Vec::new();
        let mut next_objects = Vec::new();
        for (namespace, groups) in by_namespace {
            let mut write = PendingWrite::new(ttl, retention, groups.len());
            self.allocate_in_locality_order(&namespace, &groups)?;
            let mut runs: BTreeMap<SegmentId, Vec<(StreamId, Vec<LogEntry>)>> = BTreeMap::new();
            for ((segment, fingerprint), (labels, entries)) in groups {
                let stream_id = self
                    .segment_ids(&namespace, segment)
                    .streams
                    .resolve(fingerprint)?;
                write.put(
                    dictionary_key(&namespace, segment, fingerprint),
                    encode_stream_id(stream_id),
                );
                write.put(
                    forward_key(&namespace, segment, stream_id),
                    encode_labels(&labels)?,
                );
                for label in labels.iter() {
                    write
                        .postings
                        .entry((segment, label.clone()))
                        .or_default()
                        .insert(stream_id);
                }
                if let Some(rollup_ns) = self.rollup_ns {
                    let period = segment_for(segment.start_ns, rollup_ns);
                    let rollup_id = self.period_ids(&namespace, period).resolve(fingerprint)?;
                    write.put(
                        rollup_dictionary_key(&namespace, period, fingerprint),
                        encode_stream_id(rollup_id),
                    );
                    write.put(
                        rollup_forward_key(&namespace, period, rollup_id),
                        encode_labels(&labels)?,
                    );
                    for label in labels.iter() {
                        write
                            .rollup_postings
                            .entry((period, label.clone()))
                            .or_default()
                            .insert(rollup_id);
                    }
                }
                write.report.rows += entries.len();
                if !entries.is_empty() {
                    runs.entry(segment).or_default().push((stream_id, entries));
                }
            }
            for (segment, mut streams) in runs {
                streams.sort_unstable_by_key(|(stream_id, _)| *stream_id);
                let first = self.segment_ids(&namespace, segment).next_object;
                let next =
                    self.append_segment_objects(&mut write, &namespace, segment, first, streams)?;
                next_objects.push(((namespace.clone(), segment), next));
            }
            written.extend(write.written.drain(..).map(|(segment, object, stored)| {
                WrittenObject {
                    namespace: namespace.clone(),
                    segment,
                    object,
                    stored,
                }
            }));
            self.finish_write(write, &namespace, &mut batch)?;
        }
        #[cfg(test)]
        FLUSHED_OPS.fetch_add(batch.len(), std::sync::atomic::Ordering::Relaxed);
        if !batch.is_empty() {
            self.writer
                .apply_batch(
                    batch,
                    WriteOptions {
                        await_durable: false,
                    },
                )
                .await?;
        }
        // Object IDs advance only once their objects are written, so a failed
        // write leaves no gap that would keep neighbouring objects unmerged.
        // Stream IDs need no rollback: an unwritten ID is never referenced.
        for (key, next) in next_objects {
            if let Some(ids) = self.segments.get_mut(&key) {
                ids.next_object = next;
            }
        }
        self.evict_idle();
        Ok(written)
    }

    /// Allocates the IDs of the streams `groups` adds to each segment and
    /// rollup period in [`locality_order`].
    fn allocate_in_locality_order(
        &mut self,
        namespace: &Namespace,
        groups: &StreamGroups,
    ) -> Result<()> {
        let mut segments: BTreeMap<SegmentId, Vec<(StreamFingerprint, &Labels)>> = BTreeMap::new();
        let mut periods: BTreeMap<SegmentId, Vec<(StreamFingerprint, &Labels)>> = BTreeMap::new();
        for ((segment, fingerprint), (labels, _)) in groups {
            segments
                .entry(*segment)
                .or_default()
                .push((*fingerprint, labels));
            if let Some(rollup_ns) = self.rollup_ns {
                periods
                    .entry(segment_for(segment.start_ns, rollup_ns))
                    .or_default()
                    .push((*fingerprint, labels));
            }
        }
        let priority = &self.config.stream_locality_labels;
        let block_ids = self.config.stream_id_block_size;
        fn ordered<'a>(
            streams: Vec<(StreamFingerprint, &'a Labels)>,
            priority: &[String],
        ) -> Vec<(StreamFingerprint, &'a Labels)> {
            let labels: Vec<&Labels> = streams.iter().map(|(_, labels)| *labels).collect();
            locality_order(&labels, priority)
                .into_iter()
                .map(|position| streams[position])
                .collect()
        }
        for (segment, streams) in segments {
            let streams = ordered(streams, priority);
            let key = (namespace.clone(), segment);
            let mut ids = self
                .segments
                .remove(&key)
                .expect("load_ids loads every written segment");
            ids.streams.touched = Instant::now();
            let previous = preceding(segment).and_then(|previous| {
                self.segments
                    .get(&(namespace.clone(), previous))
                    .map(|ids| &ids.streams)
            });
            let allocated =
                allocate_streams(&mut ids.streams, previous, &streams, priority, block_ids);
            self.segments.insert(key, ids);
            allocated?;
        }
        for (period, mut streams) in periods {
            streams.sort_unstable_by_key(|(fingerprint, _)| *fingerprint);
            streams.dedup_by_key(|(fingerprint, _)| *fingerprint);
            let streams = ordered(streams, priority);
            let key = (namespace.clone(), period);
            let mut ids = self
                .periods
                .remove(&key)
                .expect("load_ids loads every written period");
            ids.touched = Instant::now();
            let previous = preceding(period)
                .and_then(|previous| self.periods.get(&(namespace.clone(), previous)));
            let allocated = allocate_streams(&mut ids, previous, &streams, priority, block_ids);
            self.periods.insert(key, ids);
            allocated?;
        }
        Ok(())
    }

    /// Reads the IDs of every segment and rollup period `groups` touches that
    /// this process has not written since it last evicted them.
    async fn load_ids(&mut self, groups: &FrozenLogsWriteDelta) -> Result<()> {
        let mut segments = BTreeSet::new();
        let mut periods = BTreeSet::new();
        for (namespace, segment, _) in groups.groups.keys() {
            let key = (namespace.clone(), *segment);
            if !self.segments.contains_key(&key) {
                segments.insert(key);
            }
            if let Some(rollup_ns) = self.rollup_ns {
                let key = (namespace.clone(), segment_for(segment.start_ns, rollup_ns));
                if !self.periods.contains_key(&key) {
                    periods.insert(key);
                }
            }
        }
        let storage = self.storage.as_ref();
        let (loaded_segments, loaded_periods) = futures::try_join!(
            futures::future::try_join_all(segments.into_iter().map(|key| async move {
                let ids = SegmentIds::recover(storage, &key.0, key.1).await?;
                Ok::<_, Error>((key, ids))
            })),
            futures::future::try_join_all(periods.into_iter().map(|key| async move {
                let ids = StreamIds::recover(
                    storage,
                    rollup_dictionary_prefix(&key.0, key.1),
                    rollup_next_stream_id_key(&key.0, key.1),
                )
                .await?;
                Ok::<_, Error>((key, ids))
            })),
        )?;
        self.segments.extend(loaded_segments);
        self.periods.extend(loaded_periods);
        Ok(())
    }

    fn segment_ids(&mut self, namespace: &Namespace, segment: SegmentId) -> &mut SegmentIds {
        let ids = self
            .segments
            .get_mut(&(namespace.clone(), segment))
            .expect("load_ids loads every written segment");
        ids.streams.touched = Instant::now();
        ids
    }

    fn period_ids(&mut self, namespace: &Namespace, period: SegmentId) -> &mut StreamIds {
        let ids = self
            .periods
            .get_mut(&(namespace.clone(), period))
            .expect("load_ids loads every written period");
        ids.touched = Instant::now();
        ids
    }

    fn evict_idle(&mut self) {
        self.segments
            .retain(|_, ids| ids.streams.touched.elapsed() < IDS_IDLE_EVICTION);
        self.periods
            .retain(|_, ids| ids.touched.elapsed() < IDS_IDLE_EVICTION);
    }

    /// Packs one segment's streams, in stream order, into level-0 objects
    /// numbered from `first`, and returns the next unallocated object ID.
    fn append_segment_objects(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        first: u64,
        streams: Vec<(StreamId, Vec<LogEntry>)>,
    ) -> Result<u64> {
        let mut builder = ObjectBuilder::new(self.config.page.clone())?;
        for (stream_id, entries) in streams {
            builder.push_run(stream_id, entries)?;
        }
        let mut next = first;
        for built in builder.finish() {
            let object = ObjectRef { id: next, level: 0 };
            next = next
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("segment exhausted object IDs".to_owned()))?;
            write.add_object(namespace, segment, object, built)?;
        }
        write.flush_objects.insert(segment, first);
        write.put(
            next_object_id_key(namespace, segment),
            encode_object_id(next),
        );
        Ok(next)
    }

    /// Adds to `ops` the records accumulated across streams: stream ID
    /// counters, label postings, and the full-text index.
    fn finish_write(
        &self,
        write: PendingWrite,
        namespace: &Namespace,
        ops: &mut StorageBatch,
    ) -> Result<()> {
        let PendingWrite {
            ttl,
            ops: object_ops,
            postings,
            rollup_postings,
            search_deltas,
            flush_objects,
            ..
        } = write;
        ops.extend(object_ops);
        let mut catalogs: BTreeMap<SegmentId, CatalogBatch> = BTreeMap::new();
        let mut counters: BTreeSet<SegmentId> = BTreeSet::new();
        for (segment, label) in postings.keys() {
            counters.insert(*segment);
            catalogs.entry(*segment).or_default().insert(
                "",
                &label.name,
                DiscoveryValue::String(label.value.clone()),
            );
        }
        let mut rollup_catalogs: BTreeMap<SegmentId, CatalogBatch> = BTreeMap::new();
        for (period, label) in rollup_postings.keys() {
            rollup_catalogs.entry(*period).or_default().insert(
                "",
                &label.name,
                DiscoveryValue::String(label.value.clone()),
            );
        }
        // Rewritten with the records they guard, so a counter never expires
        // before the IDs it allocated.
        for period in rollup_catalogs.keys() {
            ops.push(RecordOp::put_with_ttl(
                rollup_next_stream_id_key(namespace, *period),
                encode_stream_id(self.periods[&(namespace.clone(), *period)].next),
                ttl,
            ));
        }
        for ((period, label), bitmap) in rollup_postings {
            ops.push(RecordOp::merge_with_ttl(
                rollup_posting_key(namespace, period, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
        }
        for (period, catalog) in rollup_catalogs {
            ops.extend(catalog.into_ops(&rollup_prefix(namespace, period), ttl));
        }
        for segment in counters {
            ops.push(RecordOp::put_with_ttl(
                next_stream_id_key(namespace, segment),
                encode_stream_id(self.segments[&(namespace.clone(), segment)].streams.next),
                ttl,
            ));
        }
        for ((segment, label), bitmap) in postings {
            ops.push(RecordOp::merge_with_ttl(
                posting_key(namespace, segment, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
        }
        for (segment, catalog) in catalogs {
            ops.extend(catalog.into_ops(&segment_prefix(namespace, segment), ttl));
        }
        for (segment, delta) in search_deltas {
            ops.push(RecordOp::merge_with_ttl(
                field_stats_key(namespace, segment),
                encode_field_stats(FieldStats {
                    documents: delta.documents,
                    total_terms: delta.total_terms,
                }),
                ttl,
            ));
            let flush_object = flush_objects[&segment];
            for (term, postings) in delta.postings {
                term_index_ops(
                    ops,
                    (namespace, segment),
                    &term,
                    postings,
                    flush_object,
                    ttl,
                )?;
            }
        }
        Ok(())
    }

    fn ttl(&self) -> Result<Ttl> {
        self.config
            .retention
            .map(|duration| {
                u64::try_from(duration.as_millis())
                    .map(Ttl::ExpireAfter)
                    .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))
            })
            .transpose()
            .map(|ttl| ttl.unwrap_or(Ttl::NoExpiry))
    }

    fn logical_expiry(&self) -> Result<Option<u64>> {
        self.config
            .retention
            .map(|retention| {
                let retention_ms = u64::try_from(retention.as_millis())
                    .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))?;
                unix_time_ms()?
                    .checked_add(retention_ms)
                    .ok_or_else(|| Error::Invalid("retention expiry overflows u64".to_owned()))
            })
            .transpose()
    }
}

/// Gives `streams`, in locality order, the IDs they lack in `ids`. With
/// `block_ids` positive, a key's first block in `ids` is sized by how many
/// of its streams `previous`, the preceding partition, gave IDs.
fn allocate_streams(
    ids: &mut StreamIds,
    previous: Option<&StreamIds>,
    streams: &[(StreamFingerprint, &Labels)],
    priority: &[String],
    block_ids: u32,
) -> Result<()> {
    let before = ids.next;
    let mut allocated = false;
    for (fingerprint, labels) in streams {
        if ids.ids.contains_key(fingerprint) {
            continue;
        }
        let key = (block_ids > 0)
            .then(|| locality_key(labels, priority))
            .flatten();
        allocated |= ids.allocate(*fingerprint, key, block_ids, |key| {
            previous?.blocks.get(key).map(|block| block.streams)
        })?;
    }
    if allocated {
        ids.finish_allocation(before)?;
    }
    Ok(())
}

/// The partition of the same width just before `partition`.
fn preceding(partition: SegmentId) -> Option<SegmentId> {
    partition
        .start_ns
        .checked_sub(partition.width_ns)
        .map(|start_ns| SegmentId::new(start_ns, partition.width_ns))
}

pub(super) type StreamGroups = BTreeMap<(SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;
type CoordinatedStreamGroups =
    BTreeMap<(Namespace, SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;

pub(super) struct LogsWrite {
    pub(super) namespace: Namespace,
    pub(super) groups: StreamGroups,
    pub(super) ops: Vec<RecordOp>,
}

fn labels_bytes(labels: &Labels) -> usize {
    labels
        .iter()
        .map(|label| size_of::<Label>() + label.name.len() + label.value.len())
        .sum()
}

fn entry_bytes(entry: &LogEntry) -> usize {
    size_of::<LogEntry>()
        + entry.line.len()
        + entry
            .structured_metadata
            .iter()
            .map(|field| size_of::<Field>() + field.name.len() + field.value.len())
            .sum::<usize>()
}

/// Splits `groups` into parts of at most `limit` [`LogsWrite::estimated_bytes`]
/// each, keeping each stream's entries in order; a part exceeds it only to
/// hold one entry.
pub(super) fn split_groups(groups: StreamGroups, limit: u64) -> Vec<StreamGroups> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut parts = Vec::new();
    let mut part = StreamGroups::new();
    let mut bytes = 0usize;
    for (key, (labels, entries)) in groups {
        let labels_bytes = labels_bytes(&labels);
        let mut taken = Vec::new();
        for entry in entries {
            let entry_bytes = entry_bytes(&entry);
            let adds = entry_bytes + if taken.is_empty() { labels_bytes } else { 0 };
            if bytes > 0 && bytes + adds > limit {
                if !taken.is_empty() {
                    part.insert(key, (labels.clone(), std::mem::take(&mut taken)));
                }
                parts.push(std::mem::take(&mut part));
                bytes = 0;
            }
            bytes += entry_bytes + if taken.is_empty() { labels_bytes } else { 0 };
            taken.push(entry);
        }
        if taken.is_empty() {
            bytes += labels_bytes;
        }
        part.insert(key, (labels, taken));
    }
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

impl LogsWrite {
    /// Bytes this write holds once applied: its entries with their line and
    /// structured metadata, each stream's labels, and its ops.
    pub(super) fn estimated_bytes(&self) -> u64 {
        let groups: usize = self
            .groups
            .values()
            .map(|(labels, entries)| {
                labels_bytes(labels) + entries.iter().map(entry_bytes).sum::<usize>()
            })
            .sum();
        let ops: usize = self
            .ops
            .iter()
            .map(|op| {
                64 + match op {
                    RecordOp::Put(put) => put.record.key.len() + put.record.value.len(),
                    RecordOp::Merge(merge) => merge.record.key.len() + merge.record.value.len(),
                    RecordOp::Delete(key) => key.len(),
                }
            })
            .sum();
        (groups + ops) as u64
    }
}

pub(super) struct FrozenLogsWriteDelta {
    groups: CoordinatedStreamGroups,
    ops: Vec<RecordOp>,
}

pub(super) struct LogsWriteDelta {
    groups: CoordinatedStreamGroups,
    ops: Vec<RecordOp>,
}

impl Delta for LogsWriteDelta {
    type Context = ();
    type Write = LogsWrite;
    type Frozen = FrozenLogsWriteDelta;
    type FrozenView = ();
    type ApplyResult = WriteReport;
    type DeltaView = ();
    type Snapshot = ();

    fn init((): Self::Context) -> Self {
        Self {
            groups: BTreeMap::new(),
            ops: Vec::new(),
        }
    }

    fn apply(&mut self, write: Self::Write) -> std::result::Result<WriteReport, String> {
        let report = WriteReport {
            streams: write.groups.len(),
            pages: 0,
            rows: write
                .groups
                .values()
                .map(|(_, entries)| entries.len())
                .sum(),
        };
        for ((segment, fingerprint), (labels, _)) in &write.groups {
            if let Some((existing, _)) =
                self.groups
                    .get(&(write.namespace.clone(), *segment, *fingerprint))
                && existing != labels
            {
                return Err("stream fingerprint collision across writes".to_owned());
            }
        }
        for ((segment, fingerprint), (labels, mut entries)) in write.groups {
            let group = self
                .groups
                .entry((write.namespace.clone(), segment, fingerprint))
                .or_insert_with(|| (labels.clone(), Vec::new()));
            group.1.append(&mut entries);
        }
        self.ops.extend(write.ops);
        Ok(report)
    }

    fn estimate_size(&self) -> usize {
        self.groups
            .values()
            .map(|(labels, entries)| {
                labels
                    .iter()
                    .map(|label| label.name.len() + label.value.len())
                    .sum::<usize>()
                    + entries
                        .iter()
                        .map(|entry| size_of::<i64>() + entry.line.len())
                        .sum::<usize>()
            })
            .sum::<usize>()
            .saturating_add(self.ops.len().saturating_mul(64))
    }

    fn freeze(mut self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        for (_, entries) in self.groups.values_mut() {
            entries.sort_by_key(|entry| entry.timestamp_ns);
        }
        (
            FrozenLogsWriteDelta {
                groups: self.groups,
                ops: self.ops,
            },
            (),
            (),
        )
    }

    fn reader(&self) -> Self::DeltaView {}
}

pub(super) struct LogsFlusher {
    pub(super) direct_writer: DirectWriter,
    pub(super) storage: Arc<dyn Storage>,
    pub(super) compactor: Option<Compactor>,
}

#[async_trait]
impl Flusher<LogsWriteDelta> for LogsFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenLogsWriteDelta,
        _epoch_range: &Range<u64>,
    ) -> std::result::Result<(), String> {
        let written = self
            .direct_writer
            .write_groups(frozen)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(compactor) = &mut self.compactor {
            compactor.after_flush(written).await;
        }
        Ok(())
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

pub(super) fn map_write_error<T>(error: WriteError<T>) -> Error {
    match error {
        WriteError::Backpressure(_) | WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Unavailable("write coordinator is shut down".to_owned()),
        WriteError::ApplyError(_, message) => Error::Invalid(message),
        WriteError::FlushError(message) | WriteError::Internal(message) => {
            Error::Unavailable(message)
        }
    }
}

/// Groups entries by `(segment, stream)`, sorted by timestamp. `route` maps
/// each entry's segment to the one it is written to.
pub(super) fn group_by_stream(
    batches: Vec<LogBatch>,
    segment_ns: i64,
    route: impl Fn(SegmentId) -> SegmentId,
) -> Result<StreamGroups> {
    let mut groups = StreamGroups::new();
    for mut batch in batches {
        batch.labels = batch.labels.without_empty_values();
        let fingerprint = batch.labels.fingerprint();
        for entry in batch.entries {
            let segment = route(segment_for(entry.timestamp_ns, segment_ns));
            let group = groups
                .entry((segment, fingerprint))
                .or_insert_with(|| (batch.labels.clone(), Vec::new()));
            if group.0 != batch.labels {
                return Err(Error::Invalid(
                    "stream fingerprint collision in write batch".to_owned(),
                ));
            }
            group.1.push(entry);
        }
    }
    for (_, entries) in groups.values_mut() {
        entries.sort_by_key(|entry| entry.timestamp_ns);
    }
    Ok(groups)
}

/// Records accumulated for one atomic write. Postings shared by several
/// streams are merged here and emitted once by `finish_write`.
struct PendingWrite {
    ttl: Ttl,
    retention: PageRetention,
    ops: Vec<RecordOp>,
    postings: HashMap<(SegmentId, Label), RoaringBitmap>,
    rollup_postings: HashMap<(SegmentId, Label), RoaringBitmap>,
    search_deltas: BTreeMap<SegmentId, IndexDelta>,
    /// First object ID this write allocated in each segment, which names
    /// its full-text blocks.
    flush_objects: HashMap<SegmentId, u64>,
    written: Vec<(SegmentId, ObjectRef, StoredObject)>,
    report: WriteReport,
}

impl PendingWrite {
    fn new(ttl: Ttl, retention: PageRetention, streams: usize) -> Self {
        Self {
            ttl,
            retention,
            ops: Vec::new(),
            postings: HashMap::new(),
            rollup_postings: HashMap::new(),
            search_deltas: BTreeMap::new(),
            flush_objects: HashMap::new(),
            written: Vec::new(),
            report: WriteReport {
                streams,
                ..WriteReport::default()
            },
        }
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        self.ops.push(RecordOp::put_with_ttl(key, value, self.ttl));
    }

    fn add_object(
        &mut self,
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        built: BuiltObject,
    ) -> Result<()> {
        let delta = self.search_deltas.entry(segment).or_default();
        for run in &built.runs {
            delta.add_run(
                &DEFAULT_ANALYZER,
                run.stream_id,
                object.id,
                run.entries.iter().map(|entry| entry.line.as_str()),
            )?;
        }
        let stored = object_records(
            &mut self.ops,
            ObjectLocation {
                namespace,
                segment,
                object,
            },
            built,
            ObjectProperties {
                expires_at_unix_ms: self.retention.expires_at_unix_ms,
                written_at_unix_ms: self.retention.written_at_unix_ms,
                span: 1,
                ttl: self.retention.physical_ttl,
            },
        )?;
        self.written.push((segment, object, stored));
        self.report.pages += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
    };

    use super::*;
    use crate::config::CompactionConfig;
    use crate::model::Label;

    const HOUR: i64 = 3_600 * 1_000_000_000;
    /// 2024-01-01.
    const DAY: i64 = 19_723 * 24 * HOUR;

    fn config(stream_locality_labels: &[&str]) -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logs-write-test".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            stream_locality_labels: stream_locality_labels
                .iter()
                .map(|name| name.to_string())
                .collect(),
            retention: None,
            compaction: CompactionConfig {
                enabled: false,
                ..CompactionConfig::default()
            },
            block_cache_capacity_bytes: 0,
            result_cache_capacity_bytes: 0,
            ..Config::default()
        }
    }

    /// The IDs the dictionary under `prefix` gives each cluster's streams,
    /// ascending.
    async fn cluster_ids(
        db: &LogDb,
        prefix: Bytes,
        clusters: &HashMap<StreamFingerprint, &str>,
    ) -> BTreeMap<String, Vec<StreamId>> {
        let mut records = db
            .storage
            .scan_prefix_iter(prefix.clone(), BytesRange::unbounded(), None)
            .await
            .unwrap();
        let mut ids: BTreeMap<String, Vec<StreamId>> = BTreeMap::new();
        while let Some(record) = records.next().await.unwrap() {
            let fingerprint = decode_dictionary_key(&record.key, prefix.len()).unwrap();
            ids.entry(clusters[&fingerprint].to_owned())
                .or_default()
                .push(decode_stream_id(&record.value).unwrap());
        }
        for streams in ids.values_mut() {
            streams.sort_unstable();
        }
        ids
    }

    #[tokio::test]
    async fn gives_a_flushed_clusters_streams_one_id_range() {
        // `container` has fewer values than `cluster`, so value counts alone
        // split every cluster's streams by container.
        let mut batches = Vec::new();
        let mut clusters = HashMap::new();
        for cluster in ["mgmt", "prod", "dev", "edge"] {
            for pod in 0..12 {
                let labels = Labels::new(vec![
                    Label::new("cluster", cluster),
                    Label::new("namespace", format!("ns-{}", pod % 4)),
                    Label::new("pod", format!("{cluster}-{pod}")),
                    Label::new("container", format!("c-{}", pod % 3)),
                ])
                .unwrap();
                clusters.insert(labels.fingerprint(), cluster);
                let entries = vec![LogEntry::new(DAY + HOUR / 2, "line")];
                batches.push(LogBatch::new(labels, entries));
            }
        }
        let namespace = Namespace::new("tenant").unwrap();
        let contiguous = |ids: &Vec<StreamId>| ids.windows(2).all(|pair| pair[1] == pair[0] + 1);
        for (priority, grouped) in [(&["cluster", "namespace"][..], true), (&[][..], false)] {
            let db = LogDb::open(config(priority)).await.unwrap();
            db.write(&namespace, batches.clone()).await.unwrap();
            db.flush_memtable().await.unwrap();
            for prefix in [
                dictionary_prefix(&namespace, SegmentId::new(DAY, HOUR)),
                rollup_dictionary_prefix(&namespace, SegmentId::new(DAY, 24 * HOUR)),
            ] {
                let ids = cluster_ids(&db, prefix, &clusters).await;
                assert_eq!(ids.len(), 4);
                for (cluster, ids) in &ids {
                    assert_eq!(contiguous(ids), grouped, "{priority:?} {cluster} {ids:?}");
                }
            }
            db.close().await.unwrap();
        }
    }

    fn empty_ids() -> StreamIds {
        StreamIds {
            ids: HashMap::new(),
            next: 0,
            blocks: HashMap::new(),
            touched: Instant::now(),
        }
    }

    fn fingerprint(index: usize) -> StreamFingerprint {
        let mut fingerprint = [0; 16];
        fingerprint[..8].copy_from_slice(&(index as u64).to_be_bytes());
        fingerprint
    }

    fn key(cluster: &str) -> Option<LocalityKey> {
        Some(Label::new("cluster", cluster))
    }

    #[test]
    fn stream_ids_reserve_blocks_that_grow_then_fall_back_to_plain_ids() {
        let mut ids = empty_ids();
        let mut index = 0;
        let mut allocate = |ids: &mut StreamIds, key: Option<LocalityKey>, hint: Option<u32>| {
            index += 1;
            assert!(ids.allocate(fingerprint(index), key, 4, |_| hint).unwrap());
            ids.ids[&fingerprint(index)]
        };
        let taken: Vec<_> = (0..4).map(|_| allocate(&mut ids, key("a"), None)).collect();
        assert_eq!(taken, vec![0, 1, 2, 3]);
        assert_eq!(allocate(&mut ids, key("b"), None), 4);
        assert_eq!(ids.next, 8);
        // Exhausted, `a` reserves what it has reserved so far, then twice it.
        assert_eq!(allocate(&mut ids, key("a"), None), 8);
        assert_eq!(ids.next, 12);
        for expected in 9..12 {
            assert_eq!(allocate(&mut ids, key("a"), None), expected);
        }
        assert_eq!(allocate(&mut ids, key("a"), None), 12);
        assert_eq!(ids.next, 20);
        assert_eq!(allocate(&mut ids, key("b"), None), 5);
        // Streams without a key take the next unreserved ID.
        assert_eq!(allocate(&mut ids, None, None), 20);
        // A new key's first block holds its hinted streams and a quarter more.
        assert_eq!(allocate(&mut ids, key("c"), Some(8)), 21);
        assert_eq!(ids.next, 31);
        assert_eq!(allocate(&mut ids, key("d"), Some(1 << 20)), 31);
        assert_eq!(ids.next, 31 + MAX_BLOCK_IDS);
        // An existing stream keeps its ID.
        assert!(!ids.allocate(fingerprint(1), key("b"), 4, |_| None).unwrap());
        assert_eq!(ids.ids[&fingerprint(1)], 0);

        // A write that only took reserved IDs still moves the counter.
        let before = ids.next;
        allocate(&mut ids, key("a"), None);
        ids.finish_allocation(before).unwrap();
        assert_eq!(ids.next, before + 1);
        let before = ids.next;
        allocate(&mut ids, None, None);
        ids.finish_allocation(before).unwrap();
        assert_eq!(ids.next, before + 1);

        // Past `MAX_BLOCK_KEYS` keys, new keys take plain IDs while the
        // others keep their blocks.
        let mut ids = empty_ids();
        for cluster in 0..MAX_BLOCK_KEYS {
            allocate(&mut ids, key(&format!("k{cluster}")), None);
        }
        let next = ids.next;
        assert_eq!(allocate(&mut ids, key("new"), None), next);
        assert_eq!(allocate(&mut ids, key("new"), None), next + 1);
        assert_eq!(allocate(&mut ids, key("k0"), None), 1);
        assert_eq!(ids.next, next + 2);

        // Blocks stay below `BLOCK_ID_LIMIT`.
        let mut ids = empty_ids();
        ids.next = BLOCK_ID_LIMIT - 2;
        assert_eq!(allocate(&mut ids, key("a"), None), BLOCK_ID_LIMIT - 2);
        assert_eq!(allocate(&mut ids, key("a"), None), BLOCK_ID_LIMIT - 1);
        assert_eq!(ids.next, BLOCK_ID_LIMIT);
        assert!(ids.blocks.is_empty());
    }

    fn pod(cluster: &str, pod: usize) -> Labels {
        Labels::new(vec![
            Label::new("cluster", cluster),
            Label::new("pod", format!("{cluster}-{pod}")),
        ])
        .unwrap()
    }

    /// Writes a row at `at` for each of `pods`, `(cluster, pod)`, in one
    /// durable flush.
    async fn write_pods(db: &LogDb, namespace: &Namespace, at: i64, pods: &[(&str, usize)]) {
        let batches = pods
            .iter()
            .map(|(cluster, index)| {
                LogBatch::new(
                    pod(cluster, *index),
                    vec![LogEntry::new(at, format!("{cluster} {index}"))],
                )
            })
            .collect();
        db.write_with_durability(namespace, batches, Durability::Durable)
            .await
            .unwrap();
        db.flush_memtable().await.unwrap();
    }

    /// Each cluster's stream IDs in the dictionary under `prefix`, ascending,
    /// and the counter at `counter`.
    async fn pod_ids(
        db: &LogDb,
        prefix: Bytes,
        counter: Bytes,
    ) -> (BTreeMap<String, Vec<StreamId>>, StreamId) {
        let mut clusters = HashMap::new();
        for cluster in ["a", "b", "c"] {
            for index in 0..64 {
                clusters.insert(pod(cluster, index).fingerprint(), cluster);
            }
        }
        let ids = cluster_ids(db, prefix, &clusters).await;
        let all: Vec<StreamId> = ids.values().flatten().copied().collect();
        assert_eq!(
            all.iter().collect::<BTreeSet<_>>().len(),
            all.len(),
            "IDs are unique: {ids:?}"
        );
        let counter = decode_stream_id(&db.storage.get(counter).await.unwrap().unwrap().value);
        (ids, counter.unwrap())
    }

    fn pods(cluster: &'static str, range: Range<usize>) -> Vec<(&'static str, usize)> {
        range.map(|index| (cluster, index)).collect()
    }

    fn segment() -> SegmentId {
        SegmentId::new(DAY, HOUR)
    }

    async fn segment_ids_of(
        db: &LogDb,
        namespace: &Namespace,
    ) -> (BTreeMap<String, Vec<StreamId>>, StreamId) {
        pod_ids(
            db,
            dictionary_prefix(namespace, segment()),
            next_stream_id_key(namespace, segment()),
        )
        .await
    }

    async fn rows_of(db: &LogDb, namespace: &Namespace, cluster: &str) -> usize {
        db.read(
            namespace,
            DAY,
            DAY + HOUR - 1,
            &[Label::new("cluster", cluster)],
        )
        .await
        .unwrap()
        .len()
    }

    #[tokio::test]
    async fn keeps_a_clusters_streams_in_its_blocks_across_flushes() {
        let namespace = Namespace::new("tenant").unwrap();
        let db = LogDb::open(Config {
            stream_id_block_size: 8,
            ..config(&["cluster"])
        })
        .await
        .unwrap();
        let at = DAY + HOUR / 2;
        write_pods(&db, &namespace, at, &[("a", 0), ("a", 1), ("b", 0)]).await;
        write_pods(&db, &namespace, at, &[("a", 2), ("b", 1), ("c", 0)]).await;
        write_pods(&db, &namespace, at, &pods("a", 3..10)).await;
        // Only reserved IDs, so the counter still moves.
        write_pods(&db, &namespace, at, &[("b", 2)]).await;
        let expected: BTreeMap<String, Vec<StreamId>> = [
            ("a", vec![0, 1, 2, 3, 4, 5, 6, 7, 24, 25]),
            ("b", vec![8, 9, 10]),
            ("c", vec![16]),
        ]
        .into_iter()
        .map(|(cluster, ids)| (cluster.to_owned(), ids))
        .collect();
        assert_eq!(
            segment_ids_of(&db, &namespace).await,
            (expected.clone(), 33)
        );
        let period = SegmentId::new(DAY, 24 * HOUR);
        assert_eq!(
            pod_ids(
                &db,
                rollup_dictionary_prefix(&namespace, period),
                rollup_next_stream_id_key(&namespace, period),
            )
            .await,
            (expected, 33)
        );
        assert_eq!(rows_of(&db, &namespace, "a").await, 10);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn sizes_a_clusters_first_block_by_its_streams_in_the_previous_segment() {
        let namespace = Namespace::new("tenant").unwrap();
        let db = LogDb::open(Config {
            stream_id_block_size: 8,
            ..config(&["cluster"])
        })
        .await
        .unwrap();
        write_pods(&db, &namespace, DAY + HOUR / 2, &pods("a", 0..20)).await;
        // The next hour's streams of `a` arrive around a new cluster's.
        let next = SegmentId::new(DAY + HOUR, HOUR);
        let at = DAY + HOUR + HOUR / 2;
        write_pods(&db, &namespace, at, &[("a", 0)]).await;
        write_pods(&db, &namespace, at, &[("b", 0)]).await;
        write_pods(&db, &namespace, at, &pods("a", 1..25)).await;
        let (ids, counter) = pod_ids(
            &db,
            dictionary_prefix(&namespace, next),
            next_stream_id_key(&namespace, next),
        )
        .await;
        assert_eq!(ids["a"], (0..25).collect::<Vec<_>>());
        assert_eq!(ids["b"], vec![25]);
        assert_eq!(counter, 34);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn allocates_as_before_without_locality_labels_or_blocks() {
        let namespace = Namespace::new("tenant").unwrap();
        let mut dictionaries = Vec::new();
        for (labels, block) in [(&[][..], 0), (&[][..], 256), (&["cluster"][..], 0)] {
            let db = LogDb::open(Config {
                stream_id_block_size: block,
                ..config(labels)
            })
            .await
            .unwrap();
            let at = DAY + HOUR / 2;
            write_pods(&db, &namespace, at, &[("b", 0), ("a", 0), ("b", 1)]).await;
            write_pods(&db, &namespace, at, &[("a", 1), ("c", 0)]).await;
            write_pods(&db, &namespace, at, &[("b", 2), ("a", 2)]).await;
            let (ids, counter) = segment_ids_of(&db, &namespace).await;
            let mut all: Vec<StreamId> = ids.values().flatten().copied().collect();
            all.sort_unstable();
            assert_eq!(all, (0..7).collect::<Vec<_>>(), "{labels:?} {block}");
            assert_eq!(counter, 7);
            dictionaries.push(ids);
            db.close().await.unwrap();
        }
        assert_eq!(dictionaries[0], dictionaries[1]);
    }

    fn local_config(
        directory: &std::path::Path,
        stream_locality_labels: &[&str],
        stream_id_block_size: u32,
    ) -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logs-write-test".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: directory.display().to_string(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            stream_id_block_size,
            ..config(stream_locality_labels)
        }
    }

    #[tokio::test]
    async fn resumes_a_segment_with_partial_blocks_without_reusing_ids() {
        let directory = tempfile::tempdir().unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let at = DAY + HOUR / 2;
        let db = LogDb::open(local_config(directory.path(), &["cluster"], 8))
            .await
            .unwrap();
        write_pods(&db, &namespace, at, &[("a", 0), ("a", 1), ("b", 0)]).await;
        write_pods(&db, &namespace, at, &[("a", 2)]).await;
        let (before, counter) = segment_ids_of(&db, &namespace).await;
        assert_eq!(counter, 17);
        // A crash loses the blocks, which only the writer's memory holds.
        drop(db);

        let db = LogDb::open(local_config(directory.path(), &["cluster"], 8))
            .await
            .unwrap();
        write_pods(
            &db,
            &namespace,
            at,
            &[("a", 0), ("a", 3), ("a", 4), ("b", 1)],
        )
        .await;
        write_pods(&db, &namespace, at, &[("c", 0), ("a", 5)]).await;
        let (after, counter) = segment_ids_of(&db, &namespace).await;
        // Existing streams keep their IDs; new ones get blocks past the
        // counter, as the IDs left in the lost blocks may have been used.
        assert_eq!(after["a"], vec![0, 1, 2, 17, 18, 19]);
        assert_eq!(after["b"], vec![8, 25]);
        assert_eq!(after["c"], vec![33]);
        assert_eq!(counter, 41);
        assert!(
            before
                .iter()
                .all(|(cluster, ids)| after[cluster].starts_with(ids))
        );
        assert_eq!(rows_of(&db, &namespace, "a").await, 7);
        assert_eq!(rows_of(&db, &namespace, "b").await, 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn resumes_segments_written_under_another_block_setting() {
        let namespace = Namespace::new("tenant").unwrap();
        let at = DAY + HOUR / 2;
        for (first, second) in [(8, 0), (0, 8)] {
            let directory = tempfile::tempdir().unwrap();
            let db = LogDb::open(local_config(directory.path(), &["cluster"], first))
                .await
                .unwrap();
            write_pods(&db, &namespace, at, &[("a", 0), ("b", 0)]).await;
            write_pods(&db, &namespace, at, &[("a", 1)]).await;
            let (before, counter) = segment_ids_of(&db, &namespace).await;
            db.close().await.unwrap();

            // Without blocks, as binaries before them do, the writer resumes
            // at the counter, past every block; with them, it reserves its
            // first blocks there.
            let db = LogDb::open(local_config(directory.path(), &["cluster"], second))
                .await
                .unwrap();
            write_pods(&db, &namespace, at, &[("a", 2), ("b", 1), ("c", 0)]).await;
            let (after, _) = segment_ids_of(&db, &namespace).await;
            let reserved = if first == 0 { 3 } else { 17 };
            assert_eq!(counter, reserved, "{first} -> {second}");
            let added: Vec<StreamId> = ["a", "b", "c"]
                .iter()
                .map(|cluster| *after[*cluster].last().unwrap())
                .collect();
            let expected = if second == 0 {
                vec![reserved, reserved + 1, reserved + 2]
            } else {
                vec![reserved, reserved + 8, reserved + 16]
            };
            assert_eq!(added, expected, "{first} -> {second}");
            assert!(
                before
                    .iter()
                    .all(|(cluster, ids)| after[cluster].starts_with(ids))
            );
            assert_eq!(rows_of(&db, &namespace, "a").await, 3);
            db.close().await.unwrap();
        }
    }

    #[test]
    fn split_groups_bounds_each_part_and_keeps_every_entry_in_order() {
        let segment = SegmentId::new(DAY, HOUR);
        let mut groups = StreamGroups::new();
        for (stream, rows) in [("a", 10), ("b", 0), ("c", 25), ("d", 1)] {
            let labels = Labels::new(vec![Label::new("app", stream)]).unwrap();
            let entries = (0..rows)
                .map(|row| LogEntry::new(DAY + row, format!("{stream} line {row:04}")))
                .collect();
            groups.insert((segment, labels.fingerprint()), (labels, entries));
        }
        let estimate = |groups: &StreamGroups| {
            LogsWrite {
                namespace: Namespace::new("tenant").unwrap(),
                groups: groups.clone(),
                ops: Vec::new(),
            }
            .estimated_bytes()
        };
        let total = estimate(&groups);
        let limit = total / 5;

        let parts = split_groups(groups.clone(), limit);
        assert!(parts.len() >= 5, "{} parts", parts.len());
        assert!(parts.iter().all(|part| estimate(part) <= limit));
        let mut joined = StreamGroups::new();
        for part in parts {
            for (key, (labels, mut entries)) in part {
                joined
                    .entry(key)
                    .or_insert_with(|| (labels, Vec::new()))
                    .1
                    .append(&mut entries);
            }
        }
        assert_eq!(joined, groups);

        assert_eq!(split_groups(groups.clone(), u64::MAX), vec![groups.clone()]);
        // A part holds one entry even when it alone is over the limit.
        let parts = split_groups(groups, 1);
        assert!(parts.len() >= 36);
        assert!(parts.iter().all(|part| {
            part.values()
                .map(|(_, entries)| entries.len())
                .sum::<usize>()
                <= 1
        }));
    }
}
