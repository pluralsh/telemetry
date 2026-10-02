// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: object encoding, the write-coordinator delta and its flusher.

use super::*;

pub(super) struct DirectWriter {
    pub(super) storage: Arc<dyn StorageRead>,
    pub(super) writer: Arc<dyn Storage>,
    pub(super) config: Config,
    /// Discovery rollup period, a whole multiple of the segment duration.
    pub(super) rollup_ns: Option<i64>,
    pub(super) rollup_ids: RollupIds,
}

type RollupKey = (Namespace, SegmentId, StreamFingerprint);

/// Rollup stream IDs this writer has persisted, with when, so later writes
/// of the stream skip the rollup dictionary read. An entry is trusted only
/// while the dictionary record it was written with is still live.
#[derive(Default)]
pub(super) struct RollupIds(std::sync::Mutex<HashMap<RollupKey, (StreamId, u64)>>);

/// Bounds [`RollupIds`]; it is emptied when full.
const ROLLUP_ID_CACHE_ENTRIES: usize = 262_144;

impl RollupIds {
    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<RollupKey, (StreamId, u64)>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The ID persisted for `key`, if its dictionary record is still live.
    fn get(&self, key: &RollupKey, ttl: Ttl, now_unix_ms: u64) -> Option<StreamId> {
        let &(id, written_at) = self.entries().get(key)?;
        let live = match ttl {
            Ttl::ExpireAfter(ttl_ms) => now_unix_ms.saturating_sub(written_at) < ttl_ms,
            _ => true,
        };
        live.then_some(id)
    }

    fn record(&self, ids: impl IntoIterator<Item = (RollupKey, StreamId)>, written_at: u64) {
        let mut entries = self.entries();
        for (key, id) in ids {
            if entries.len() >= ROLLUP_ID_CACHE_ENTRIES {
                entries.clear();
            }
            entries.insert(key, (id, written_at));
        }
    }
}

impl DirectWriter {
    /// Writes every group atomically and returns the objects it wrote.
    async fn write_groups(&self, groups: FrozenLogsWriteDelta) -> Result<Vec<WrittenObject>> {
        let ttl = self.ttl()?;
        let retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
            written_at_unix_ms: unix_time_ms()?,
        };
        let mut rollup_ids = Vec::new();
        let mut by_namespace: BTreeMap<Namespace, StreamGroups> = BTreeMap::new();
        for ((namespace, segment, fingerprint), group) in groups.groups {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert((segment, fingerprint), group);
        }
        let mut all_ops = Vec::new();
        let mut written = Vec::new();
        for (namespace, groups) in by_namespace {
            let mut write = PendingWrite::new(ttl, retention, groups.len());
            let mut runs: BTreeMap<SegmentId, Vec<(StreamId, Vec<LogEntry>)>> = BTreeMap::new();
            for ((segment, fingerprint), (labels, entries)) in groups {
                let stream_id = self
                    .resolve_stream_id(&mut write, &namespace, segment, fingerprint, &labels)
                    .await?;
                self.add_postings(&mut write.postings, segment, &labels, stream_id, |label| {
                    posting_key(&namespace, segment, label)
                })
                .await?;
                if let Some(rollup_ns) = self.rollup_ns {
                    let period = segment_for(segment, rollup_ns);
                    let rollup_id = self
                        .resolve_rollup_id(&mut write, &namespace, period, fingerprint, &labels)
                        .await?;
                    self.add_postings(
                        &mut write.rollup_postings,
                        period,
                        &labels,
                        rollup_id,
                        |label| rollup_posting_key(&namespace, period, label),
                    )
                    .await?;
                }
                write.report.rows += entries.len();
                runs.entry(segment).or_default().push((stream_id, entries));
            }
            for (segment, mut streams) in runs {
                streams.sort_unstable_by_key(|(stream_id, _)| *stream_id);
                self.append_segment_objects(&mut write, &namespace, segment, streams)
                    .await?;
            }
            rollup_ids.extend(
                write.rollup_ids.drain().map(|((period, fingerprint), id)| {
                    ((namespace.clone(), period, fingerprint), id)
                }),
            );
            written.extend(write.written.drain(..).map(|(segment, object, stored)| {
                WrittenObject {
                    namespace: namespace.clone(),
                    segment,
                    object,
                    stored,
                }
            }));
            let (ops, _) = self.finish_write(write, &namespace).await?;
            all_ops.extend(ops);
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
        self.rollup_ids
            .record(rollup_ids, retention.written_at_unix_ms);
        Ok(written)
    }

    /// The stream's period-local rollup ID, allocating the period's next one
    /// if it has none, and rewrites its rollup dictionary and forward-label
    /// records to refresh their TTLs.
    async fn resolve_rollup_id(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        period: SegmentId,
        fingerprint: StreamFingerprint,
        labels: &Labels,
    ) -> Result<StreamId> {
        if let Some(id) = write.rollup_ids.get(&(period, fingerprint)) {
            return Ok(*id);
        }
        // Loaded for every written period, so the counter is rewritten with
        // the records it guards and never expires before them.
        if let Entry::Vacant(entry) = write.rollup_next_ids.entry(period) {
            let next = self
                .storage
                .get(rollup_next_stream_id_key(namespace, period))
                .await?
                .map(|record| decode_stream_id(&record.value))
                .transpose()?
                .unwrap_or(0);
            entry.insert(next);
        }
        let dictionary = rollup_dictionary_key(namespace, period, fingerprint);
        let cached = self.rollup_ids.get(
            &(namespace.clone(), period, fingerprint),
            write.ttl,
            unix_time_ms()?,
        );
        let id = match cached {
            Some(id) => id,
            None => match self.storage.get(dictionary.clone()).await? {
                Some(record) => decode_stream_id(&record.value)?,
                None => {
                    let next = write.rollup_next_ids[&period];
                    let following = next.checked_add(1).ok_or_else(|| {
                        Error::Invalid("rollup period exhausted stream IDs".to_owned())
                    })?;
                    write.rollup_next_ids.insert(period, following);
                    next
                }
            },
        };
        write.rollup_ids.insert((period, fingerprint), id);
        write.put(dictionary, encode_stream_id(id));
        write.put(
            rollup_forward_key(namespace, period, id),
            encode_labels(labels)?,
        );
        Ok(id)
    }

    /// Returns the stream's existing ID, or allocates the segment's next one,
    /// and rewrites its dictionary and forward-label records to refresh TTLs.
    async fn resolve_stream_id(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        fingerprint: StreamFingerprint,
        labels: &Labels,
    ) -> Result<StreamId> {
        let dictionary = dictionary_key(namespace, segment, fingerprint);
        let stream_id = match self.storage.get(dictionary.clone()).await? {
            Some(record) => {
                let stream_id = decode_stream_id(&record.value)?;
                let existing = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| Error::Corrupt("dictionary has no forward labels".to_owned()))?;
                if decode_labels(&existing.value)? != *labels {
                    return Err(Error::Corrupt(
                        "stream fingerprint maps to different labels".to_owned(),
                    ));
                }
                stream_id
            }
            None => self.allocate_stream_id(write, namespace, segment).await?,
        };
        write.put(dictionary, encode_stream_id(stream_id));
        write.put(
            forward_key(namespace, segment, stream_id),
            encode_labels(labels)?,
        );
        Ok(stream_id)
    }

    async fn allocate_stream_id(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
    ) -> Result<StreamId> {
        let next = match write.next_stream_ids.get(&segment) {
            Some(next) => *next,
            None => self
                .storage
                .get(next_stream_id_key(namespace, segment))
                .await?
                .map(|record| decode_stream_id(&record.value))
                .transpose()?
                .unwrap_or(0),
        };
        let following = next
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segment exhausted stream IDs".to_owned()))?;
        write.next_stream_ids.insert(segment, following);
        Ok(next)
    }

    /// Adds `id` to the `scope` posting of every label, loading each posting
    /// from `key` the first time this write touches it.
    async fn add_postings(
        &self,
        postings: &mut HashMap<(SegmentId, Label), RoaringBitmap>,
        scope: SegmentId,
        labels: &Labels,
        id: StreamId,
        key: impl Fn(&Label) -> Bytes,
    ) -> Result<()> {
        for label in labels.iter() {
            let bitmap = match postings.entry((scope, label.clone())) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let bitmap = self
                        .storage
                        .get(key(label))
                        .await?
                        .map(|record| decode_postings(&record.value))
                        .transpose()?
                        .unwrap_or_default();
                    entry.insert(bitmap)
                }
            };
            bitmap.insert(id);
        }
        Ok(())
    }

    /// Packs one segment's streams, in stream order, into level-0 objects.
    async fn append_segment_objects(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        streams: Vec<(StreamId, Vec<LogEntry>)>,
    ) -> Result<()> {
        let next_key = next_object_id_key(namespace, segment);
        let mut next = self
            .storage
            .get(next_key.clone())
            .await?
            .map(|record| decode_object_id(&record.value))
            .transpose()?
            .unwrap_or(0);
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
        write.put(next_key, encode_object_id(next));
        Ok(())
    }

    /// Emits the records accumulated across streams: stream ID counters,
    /// label postings, and the full-text index.
    async fn finish_write(
        &self,
        write: PendingWrite,
        namespace: &Namespace,
    ) -> Result<(Vec<RecordOp>, WriteReport)> {
        let PendingWrite {
            ttl,
            mut ops,
            next_stream_ids,
            postings,
            rollup_next_ids,
            rollup_postings,
            search_deltas,
            report,
            ..
        } = write;
        let mut catalogs: BTreeMap<SegmentId, CatalogBatch> = BTreeMap::new();
        for (segment, label) in postings.keys() {
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
        for (period, next) in rollup_next_ids {
            ops.push(RecordOp::put_with_ttl(
                rollup_next_stream_id_key(namespace, period),
                encode_stream_id(next),
                ttl,
            ));
        }
        for ((period, label), bitmap) in rollup_postings {
            ops.push(RecordOp::put_with_ttl(
                rollup_posting_key(namespace, period, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
        }
        for (period, catalog) in rollup_catalogs {
            ops.extend(catalog.into_ops(&rollup_prefix(namespace, period), ttl));
        }
        for (segment, next) in next_stream_ids {
            ops.push(RecordOp::put_with_ttl(
                next_stream_id_key(namespace, segment),
                encode_stream_id(next),
                ttl,
            ));
        }
        for ((segment, label), bitmap) in postings {
            ops.push(RecordOp::put_with_ttl(
                posting_key(namespace, segment, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
        }
        for (segment, catalog) in catalogs {
            ops.extend(catalog.into_ops(&segment_prefix(namespace, segment), ttl));
        }
        for (segment, delta) in search_deltas {
            self.append_search_index_ops(&mut ops, namespace, segment, delta, ttl)
                .await?;
        }
        Ok((ops, report))
    }

    async fn append_search_index_ops(
        &self,
        ops: &mut Vec<RecordOp>,
        namespace: &Namespace,
        segment: SegmentId,
        delta: IndexDelta,
        ttl: Ttl,
    ) -> Result<()> {
        let mut field = self
            .storage
            .get(field_stats_key(namespace, segment))
            .await?
            .map(|record| decode_field_stats(&record.value))
            .transpose()?
            .unwrap_or_default();
        field.documents = field
            .documents
            .checked_add(delta.documents)
            .ok_or_else(|| Error::Invalid("segment document count overflow".into()))?;
        field.total_terms = field
            .total_terms
            .checked_add(delta.total_terms)
            .ok_or_else(|| Error::Invalid("segment token count overflow".into()))?;
        ops.push(RecordOp::put_with_ttl(
            field_stats_key(namespace, segment),
            encode_field_stats(field),
            ttl,
        ));

        let storage = self.storage.as_ref();
        let mut writes = std::pin::pin!(
            futures::stream::iter(delta.postings)
                .map(|(term, postings)| {
                    term_index_writes(storage, namespace, segment, term, postings)
                })
                .buffer_unordered(TERM_INDEX_CONCURRENCY)
        );
        while let Some(term_writes) = writes.try_next().await? {
            ops.extend(
                term_writes
                    .into_iter()
                    .map(|(key, value)| RecordOp::put_with_ttl(key, value, ttl)),
            );
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
    pub(super) direct_writer: Arc<DirectWriter>,
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

/// Records accumulated for one atomic write. Counters and postings shared by
/// several streams are merged here and emitted once by `finish_write`.
struct PendingWrite {
    ttl: Ttl,
    retention: PageRetention,
    ops: Vec<RecordOp>,
    /// Next unallocated stream ID for each segment that allocated one.
    next_stream_ids: HashMap<SegmentId, StreamId>,
    postings: HashMap<(SegmentId, Label), RoaringBitmap>,
    /// Rollup IDs resolved by this write, keyed by period.
    rollup_ids: HashMap<(SegmentId, StreamFingerprint), StreamId>,
    /// Next unallocated rollup ID of every period this write touches.
    rollup_next_ids: HashMap<SegmentId, StreamId>,
    rollup_postings: HashMap<(SegmentId, Label), RoaringBitmap>,
    search_deltas: BTreeMap<SegmentId, IndexDelta>,
    written: Vec<(SegmentId, ObjectRef, StoredObject)>,
    report: WriteReport,
}

impl PendingWrite {
    fn new(ttl: Ttl, retention: PageRetention, streams: usize) -> Self {
        Self {
            ttl,
            retention,
            ops: Vec::new(),
            next_stream_ids: HashMap::new(),
            postings: HashMap::new(),
            rollup_ids: HashMap::new(),
            rollup_next_ids: HashMap::new(),
            rollup_postings: HashMap::new(),
            search_deltas: BTreeMap::new(),
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
