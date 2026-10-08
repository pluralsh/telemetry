// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: object encoding, the write-coordinator delta and its flusher.

use std::time::Instant;

use super::*;
use crate::codec::{decode_dictionary_key, dictionary_prefix, rollup_dictionary_prefix};

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

/// Stream IDs allocated in one segment or rollup period.
struct StreamIds {
    ids: HashMap<StreamFingerprint, StreamId>,
    next: StreamId,
    touched: Instant,
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
            touched: Instant::now(),
        })
    }

    fn resolve(&mut self, fingerprint: StreamFingerprint) -> Result<StreamId> {
        if let Some(id) = self.ids.get(&fingerprint) {
            return Ok(*id);
        }
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segment exhausted stream IDs".to_owned()))?;
        self.ids.insert(fingerprint, id);
        Ok(id)
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
        let ttl = self.ttl()?;
        let retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
            written_at_unix_ms: unix_time_ms()?,
        };
        let mut by_namespace: BTreeMap<Namespace, StreamGroups> = BTreeMap::new();
        for ((namespace, segment, fingerprint), group) in groups.groups {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert((segment, fingerprint), group);
        }
        let mut all_ops = Vec::new();
        let mut written = Vec::new();
        let mut next_objects = Vec::new();
        for (namespace, groups) in by_namespace {
            let mut write = PendingWrite::new(ttl, retention, groups.len());
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
                    let period = segment_for(segment, rollup_ns);
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
                runs.entry(segment).or_default().push((stream_id, entries));
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
            all_ops.extend(self.finish_write(write, &namespace)?);
        }
        if !all_ops.is_empty() {
            self.writer
                .apply_with_options(
                    all_ops,
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
                let key = (namespace.clone(), segment_for(*segment, rollup_ns));
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
        let mut next = first;
        let mut builder = ObjectBuilder::new(self.config.page.clone())?;
        for (stream_id, entries) in streams {
            builder.push_run(stream_id, entries)?;
        }
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

    /// Emits the records accumulated across streams: stream ID counters,
    /// label postings, and the full-text index.
    fn finish_write(&self, write: PendingWrite, namespace: &Namespace) -> Result<Vec<RecordOp>> {
        let PendingWrite {
            ttl,
            mut ops,
            postings,
            rollup_postings,
            search_deltas,
            flush_objects,
            ..
        } = write;
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
                    &mut ops,
                    (namespace, segment),
                    &term,
                    postings,
                    flush_object,
                    ttl,
                )?;
            }
        }
        Ok(ops)
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

type StreamGroups = BTreeMap<(SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;
type CoordinatedStreamGroups =
    BTreeMap<(Namespace, SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;

pub(super) struct LogsWrite {
    pub(super) namespace: Namespace,
    pub(super) groups: StreamGroups,
}

pub(super) struct FrozenLogsWriteDelta {
    groups: CoordinatedStreamGroups,
}

pub(super) struct LogsWriteDelta {
    groups: CoordinatedStreamGroups,
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
            .sum()
    }

    fn freeze(mut self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        for (_, entries) in self.groups.values_mut() {
            entries.sort_by_key(|entry| entry.timestamp_ns);
        }
        (
            FrozenLogsWriteDelta {
                groups: self.groups,
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

/// Groups entries by `(segment, stream)`, sorted by timestamp.
pub(super) fn group_by_stream(batches: Vec<LogBatch>, segment_ns: i64) -> Result<StreamGroups> {
    let mut groups = StreamGroups::new();
    for mut batch in batches {
        batch.labels = batch.labels.without_empty_values();
        let fingerprint = batch.labels.fingerprint();
        for entry in batch.entries {
            let segment = segment_for(entry.timestamp_ns, segment_ns);
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
