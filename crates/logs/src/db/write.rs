// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Write path: page encoding, the write-coordinator delta and its flusher.

use super::*;

pub(super) struct DirectWriter {
    pub(super) storage: Arc<dyn StorageRead>,
    pub(super) writer: Arc<dyn Storage>,
    pub(super) config: Config,
}

impl DirectWriter {
    /// Writes every group atomically and returns the pages it wrote.
    async fn write_groups(&self, groups: FrozenLogsWriteDelta) -> Result<Vec<WrittenPage>> {
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
        for (namespace, groups) in by_namespace {
            let mut write = PendingWrite::new(ttl, retention, groups.len());
            for ((segment, fingerprint), (labels, entries)) in groups {
                let stream_id = self
                    .resolve_stream_id(&mut write, &namespace, segment, fingerprint, &labels)
                    .await?;
                self.add_label_postings(&mut write, &namespace, segment, &labels, stream_id)
                    .await?;
                self.append_stream_pages(&mut write, &namespace, segment, stream_id, entries)
                    .await?;
            }
            written.extend(write.written.drain(..).map(
                |(segment, stream_id, page_id, metadata)| WrittenPage {
                    namespace: namespace.clone(),
                    segment,
                    stream_id,
                    page_id,
                    metadata,
                },
            ));
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
        Ok(written)
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

    async fn add_label_postings(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        labels: &Labels,
        stream_id: StreamId,
    ) -> Result<()> {
        for label in labels.iter() {
            let bitmap = match write.postings.entry((segment, label.clone())) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let bitmap = self
                        .storage
                        .get(posting_key(namespace, segment, label))
                        .await?
                        .map(|record| decode_postings(&record.value))
                        .transpose()?
                        .unwrap_or_default();
                    entry.insert(bitmap)
                }
            };
            bitmap.insert(stream_id);
        }
        Ok(())
    }

    async fn append_stream_pages(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        stream_id: StreamId,
        entries: Vec<LogEntry>,
    ) -> Result<()> {
        let page_sequence_key = next_page_sequence_key(namespace, segment, stream_id);
        let mut sequence = self
            .storage
            .get(page_sequence_key.clone())
            .await?
            .map(|record| decode_page_sequence(&record.value))
            .transpose()?
            .unwrap_or(0);
        let row_count = entries.len();
        let mut builder = PageBuilder::new(self.config.page.clone())?;
        for entry in entries {
            if let Some(completed) = builder.append_with_rows(entry)? {
                write.add_page(namespace, segment, stream_id, &mut sequence, completed)?;
            }
        }
        if let Some(completed) = builder.finish_with_rows()? {
            write.add_page(namespace, segment, stream_id, &mut sequence, completed)?;
        }
        write.report.rows += row_count;
        write.put(page_sequence_key, encode_page_sequence(sequence));
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
    search_deltas: BTreeMap<SegmentId, IndexDelta>,
    written: Vec<(SegmentId, StreamId, PageId, StoredPageMetadata)>,
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

    fn add_page(
        &mut self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_id: StreamId,
        sequence: &mut u64,
        (page, rows): (Page, Vec<LogEntry>),
    ) -> Result<()> {
        self.search_deltas.entry(segment).or_default().add_page(
            &DEFAULT_ANALYZER,
            stream_id,
            *sequence,
            rows.iter().map(|row| row.line.as_str()),
        )?;
        let (page_id, metadata) = append_page_ops(
            &mut self.ops,
            namespace,
            PageWriteId {
                segment,
                stream_id,
                sequence: *sequence,
            },
            page,
            self.retention,
        )?;
        self.written.push((segment, stream_id, page_id, metadata));
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("stream exhausted page sequences".to_owned()))?;
        self.report.pages += 1;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct PageWriteId {
    segment: SegmentId,
    stream_id: StreamId,
    sequence: u64,
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    id: PageWriteId,
    page: Page,
    retention: PageRetention,
) -> Result<(PageId, StoredPageMetadata)> {
    let bytes = page.bytes();
    let (Some(first), Some(last)) = (page.blocks().first(), page.blocks().last()) else {
        return Err(Error::Invalid("cannot write an empty page".to_owned()));
    };
    let min_timestamp_ns = first.min_timestamp_ns;
    let max_timestamp_ns = last.max_timestamp_ns;
    let page_id = PageId {
        timestamp_ns: min_timestamp_ns,
        sequence: id.sequence,
    };
    let metadata = StoredPageMetadata {
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        row_count: page.row_count(),
        payload_bytes: u32::try_from(bytes.len())
            .map_err(|_| Error::Invalid("page payload exceeds u32".to_owned()))?,
        level: 0,
        written_at_unix_ms: retention.written_at_unix_ms,
        leaf_rows: Vec::new(),
    };
    ops.push(RecordOp::put_with_ttl(
        metadata_key(namespace, id.segment, id.stream_id, page_id),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(RecordOp::put_with_ttl(
        payload_key(namespace, id.segment, id.stream_id, page_id, 0),
        bytes,
        retention.physical_ttl,
    ));
    Ok((page_id, metadata))
}
