// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::storage::{PutOptions, PutRecordOp, Record, RecordOp, Storage, Ttl, WriteOptions};
use common::{StorageBuilder, StorageSemantics};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use tokio::sync::Mutex;

use crate::Namespace;
use crate::analyzer::DEFAULT_ANALYZER;
use crate::codec::{
    PageId, StoredPageMetadata, decode_forward_key, decode_labels, decode_metadata,
    decode_metadata_key, decode_page_sequence, decode_postings, decode_stream_id, dictionary_key,
    encode_labels, encode_metadata, encode_page_sequence, encode_postings, encode_stream_id,
    field_stats_key, forward_key, forward_range, metadata_key, metadata_range,
    next_page_sequence_key, next_stream_id_key, payload_key, posting_key, segment_for,
};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{
    Label, Labels, LogBatch, LogEntry, LogRow, SegmentId, StreamFingerprint, StreamId,
};
use crate::page::{Page, PageBuilder};
use crate::search::{
    IndexDelta, block_max_scores, decode_field_stats, encode_field_stats, source_matches,
    term_index_writes,
};

/// Terms whose index records are read concurrently while building a write.
const TERM_INDEX_CONCURRENCY: usize = 32;
/// Page payloads fetched concurrently within one query segment.
const PAGE_READ_CONCURRENCY: usize = 16;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    pub streams: usize,
    pub pages: usize,
    pub rows: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

/// Pages a query may still read, shareable across the databases it spans.
#[derive(Debug)]
pub(crate) struct PageBudget {
    limit: usize,
    remaining: AtomicUsize,
}

impl PageBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: AtomicUsize::new(limit),
        }
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    fn take(&self) -> Result<()> {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .map(drop)
            .map_err(|_| Error::Query(format!("query exceeded max_pages ({})", self.limit)))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct QueryEstimate {
    pub compressed_bytes: u64,
    pub lines: u64,
    pub pages: usize,
}

#[derive(Clone, Copy, Debug)]
struct PageRetention {
    physical_ttl: Ttl,
    expires_at_unix_ms: Option<u64>,
}

/// Single-writer/single-node log database over the common SlateDB abstraction.
pub struct LogDb {
    storage: Arc<dyn Storage>,
    config: Config,
    segment_ns: i64,
    write_lock: Mutex<()>,
}

impl LogDb {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let segment_ns = duration_ns(config.segment_duration)?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        Ok(Self {
            storage,
            config,
            segment_ns,
            write_lock: Mutex::new(()),
        })
    }

    /// Atomically writes all generated index and page records.
    pub async fn write(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
    ) -> Result<WriteReport> {
        self.write_with_durability(namespace, batches, Durability::Written)
            .await
    }

    pub async fn write_with_durability(
        &self,
        namespace: &Namespace,
        batches: Vec<LogBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let groups = group_by_stream(batches, self.segment_ns)?;
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }

        let _guard = self.write_lock.lock().await;
        let ttl = self.ttl()?;
        let retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
        };
        let mut write = PendingWrite::new(ttl, retention, groups.len());
        for ((segment, fingerprint), (labels, entries)) in groups {
            let stream_id = self
                .resolve_stream_id(&mut write, namespace, segment, fingerprint, &labels)
                .await?;
            self.add_label_postings(&mut write, namespace, segment, &labels, stream_id)
                .await?;
            self.append_stream_pages(&mut write, namespace, segment, stream_id, entries)
                .await?;
        }
        let (ops, report) = self.finish_write(write, namespace).await?;

        self.storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: durability == Durability::Durable,
                },
            )
            .await?;
        if durability == Durability::Written {
            self.storage.flush().await?;
        }
        Ok(report)
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
        let mut builder = PageBuilder::new(self.config.page.clone(), Instant::now())?;
        for entry in entries {
            if let Some(completed) = builder.append_with_rows(entry, Instant::now())? {
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
        for (segment, next) in next_stream_ids {
            ops.push(put(
                next_stream_id_key(namespace, segment),
                encode_stream_id(next),
                ttl,
            ));
        }
        for ((segment, label), bitmap) in postings {
            ops.push(put(
                posting_key(namespace, segment, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
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
        ops.push(put(
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
                    .map(|(key, value)| put(key, value, ttl)),
            );
        }
        Ok(())
    }

    /// Reads rows in the inclusive timestamp range matching every exact label.
    pub async fn read(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
    ) -> Result<Vec<LogRow>> {
        self.read_bounded(
            namespace,
            start_ns,
            end_ns,
            matchers,
            &PageBudget::new(usize::MAX),
        )
        .await
    }

    pub(crate) async fn estimate_pages(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
    ) -> Result<QueryEstimate> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        let now_unix_ms = unix_time_ms()?;
        let mut segment = first_segment;
        let mut estimate = QueryEstimate::default();
        loop {
            for stream_id in self.stream_ids(namespace, segment, matchers).await? {
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment, stream_id))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let page = decode_metadata(&record.value)?;
                    if page.is_expired_at(now_unix_ms)
                        || page.max_timestamp_ns < start_ns
                        || page.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    estimate.pages = estimate.pages.saturating_add(1);
                    estimate.compressed_bytes = estimate
                        .compressed_bytes
                        .saturating_add(u64::from(page.payload_bytes));
                    estimate.lines = estimate.lines.saturating_add(u64::from(page.row_count));
                }
            }
            if segment == last_segment {
                break;
            }
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
        }
        Ok(estimate)
    }

    /// Reads overlapping pages, charging each to `budget`. This is the
    /// storage primitive used by the query engine to put a hard bound on I/O.
    pub(crate) async fn read_bounded(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
        budget: &PageBudget,
    ) -> Result<Vec<LogRow>> {
        let mut rows = Vec::new();
        self.read_segments(
            namespace,
            (start_ns, end_ns),
            matchers,
            budget,
            false,
            |segment_rows| {
                rows.extend(segment_rows);
                Ok(ControlFlow::Continue(()))
            },
        )
        .await?;
        Ok(rows)
    }

    /// Reads overlapping pages one time segment at a time, handing each
    /// segment's rows (in stable storage order) to `consume`. Segments
    /// partition time, so every row in a later segment is strictly later
    /// (or, with `reverse`, strictly earlier) than every row already
    /// consumed; `consume` returns `Break` once no later row can matter.
    /// Only pages actually read are charged to `budget`.
    pub(crate) async fn read_segments(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (i64, i64),
        matchers: &[Label],
        budget: &PageBudget,
        reverse: bool,
        mut consume: impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        let now_unix_ms = unix_time_ms()?;
        let (mut segment, final_segment, step) = if reverse {
            (last_segment, first_segment, -self.segment_ns)
        } else {
            (first_segment, last_segment, self.segment_ns)
        };

        loop {
            let mut pages = Vec::new();
            for stream_id in self.stream_ids(namespace, segment, matchers).await? {
                let Some(labels_record) = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                else {
                    return Err(Error::Corrupt(
                        "posting references missing forward labels".to_owned(),
                    ));
                };
                let labels = Arc::new(decode_labels(&labels_record.value)?);
                let fingerprint = labels.fingerprint();
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment, stream_id))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let (_, page_id) = decode_metadata_key(&record.key)?;
                    let page_metadata = decode_metadata(&record.value)?;
                    if page_metadata.is_expired_at(now_unix_ms)
                        || page_metadata.max_timestamp_ns < start_ns
                        || page_metadata.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    budget.take()?;
                    pages.push((stream_id, labels.clone(), fingerprint, page_id));
                }
            }
            let mut decoded = futures::stream::iter(pages)
                .map(
                    move |(stream_id, labels, fingerprint, page_id)| async move {
                        let payload = self
                            .storage
                            .get(payload_key(namespace, segment, stream_id, page_id))
                            .await?
                            .ok_or_else(|| {
                                Error::Corrupt("page metadata has no payload".to_owned())
                            })?;
                        let entries =
                            Page::decode(payload.value)?.decode_range(start_ns, end_ns)?;
                        Ok::<_, Error>((labels, fingerprint, page_id, entries))
                    },
                )
                .buffered(PAGE_READ_CONCURRENCY);
            let mut rows = Vec::new();
            while let Some((labels, fingerprint, page_id, entries)) = decoded.try_next().await? {
                rows.extend(entries.into_iter().enumerate().map(|(row_index, entry)| {
                    (
                        (entry.timestamp_ns, fingerprint, page_id.sequence, row_index),
                        LogRow {
                            labels: labels.clone(),
                            entry,
                        },
                    )
                }));
            }
            rows.sort_unstable_by_key(|(key, _)| *key);
            if consume(rows.into_iter().map(|(_, row)| row).collect())?.is_break()
                || segment == final_segment
            {
                return Ok(());
            }
            segment = segment
                .checked_add(step)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
        }
    }

    /// Uses the segment-local term index to identify pages and rows before
    /// payload reads. `None` means at least one segment predates the index and
    /// the caller must use the exact scan fallback.
    pub(crate) async fn read_match_bounded(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
        match_plan: (&[String], Option<usize>),
        max_pages: usize,
    ) -> Result<Option<Vec<(LogRow, f32)>>> {
        let (terms, top_k) = match_plan;
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        let now_unix_ms = unix_time_ms()?;
        let mut segment = first_segment;
        let mut rows = Vec::new();
        let mut pages_read = 0usize;

        loop {
            let stream_ids = self.stream_ids(namespace, segment, matchers).await?;
            let mut candidate_pages = Vec::new();
            let mut allowed_pages = HashSet::new();
            for stream_id in stream_ids {
                let labels_record = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".into())
                    })?;
                let labels = Arc::new(decode_labels(&labels_record.value)?);
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment, stream_id))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let (_, page_id) = decode_metadata_key(&record.key)?;
                    let page_metadata = decode_metadata(&record.value)?;
                    if page_metadata.is_expired_at(now_unix_ms)
                        || page_metadata.max_timestamp_ns < start_ns
                        || page_metadata.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    allowed_pages.insert((stream_id, page_id.sequence));
                    candidate_pages.push((stream_id, labels.clone(), page_id));
                }
            }
            let Some(scores) = block_max_scores(
                self.storage.as_ref(),
                namespace,
                segment,
                terms,
                &allowed_pages,
                top_k,
            )
            .await?
            else {
                return Ok(None);
            };
            let mut by_page: HashMap<(StreamId, u64), HashMap<u32, f32>> = HashMap::new();
            for (address, score) in scores {
                by_page
                    .entry((address.stream_id, address.page_sequence))
                    .or_default()
                    .insert(address.row_id, score);
            }

            for (stream_id, labels, page_id) in candidate_pages {
                let Some(page_scores) = by_page.get(&(stream_id, page_id.sequence)) else {
                    continue;
                };
                if pages_read == max_pages {
                    return Err(Error::Query(format!(
                        "query exceeded max_pages ({max_pages})"
                    )));
                }
                pages_read += 1;
                let payload = self
                    .storage
                    .get(payload_key(namespace, segment, stream_id, page_id))
                    .await?
                    .ok_or_else(|| Error::Corrupt("page metadata has no payload".to_owned()))?;
                let page = Page::decode(payload.value)?;
                for (row_id, entry) in page.decode_rows_where(start_ns, end_ns, |row_id| {
                    page_scores.contains_key(&row_id)
                })? {
                    let score = &page_scores[&row_id];
                    // Stored postings are candidates, never authority.
                    if source_matches(&DEFAULT_ANALYZER, &entry.line, terms) {
                        rows.push((
                            LogRow {
                                labels: labels.clone(),
                                entry,
                            },
                            *score,
                        ));
                    }
                }
            }
            if segment == last_segment {
                break;
            }
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
        }
        Ok(Some(rows))
    }

    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.storage.close().await?;
        Ok(())
    }

    async fn stream_ids(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        matchers: &[Label],
    ) -> Result<Vec<StreamId>> {
        if matchers.is_empty() {
            let mut iterator = self
                .storage
                .scan_iter(forward_range(namespace, segment))
                .await?;
            let mut result = Vec::new();
            while let Some(record) = iterator.next().await? {
                result.push(decode_forward_key(&record.key)?);
            }
            return Ok(result);
        }

        let mut result: Option<RoaringBitmap> = None;
        for matcher in matchers {
            let Some(record) = self
                .storage
                .get(posting_key(namespace, segment, matcher))
                .await?
            else {
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

/// Groups entries by `(segment, stream)`, each group sorted by timestamp.
fn group_by_stream(batches: Vec<LogBatch>, segment_ns: i64) -> Result<StreamGroups> {
    let mut groups = StreamGroups::new();
    for batch in batches {
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
            report: WriteReport {
                streams,
                ..WriteReport::default()
            },
        }
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        self.ops.push(put(key, value, self.ttl));
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
        append_page_ops(
            &mut self.ops,
            namespace,
            segment,
            stream_id,
            *sequence,
            page,
            self.retention,
        )?;
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("stream exhausted page sequences".to_owned()))?;
        self.report.pages += 1;
        Ok(())
    }
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
    sequence: u64,
    page: Page,
    retention: PageRetention,
) -> Result<()> {
    let bytes = page.bytes();
    let min_timestamp_ns = page.blocks().first().unwrap().min_timestamp_ns;
    let max_timestamp_ns = page.blocks().last().unwrap().max_timestamp_ns;
    let page_id = PageId {
        timestamp_ns: min_timestamp_ns,
        sequence,
    };
    let metadata = StoredPageMetadata {
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        row_count: page.row_count(),
        payload_bytes: u32::try_from(bytes.len())
            .map_err(|_| Error::Invalid("page payload exceeds u32".to_owned()))?,
    };
    ops.push(put(
        metadata_key(namespace, segment, stream_id, page_id),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, segment, stream_id, page_id),
        bytes,
        retention.physical_ttl,
    ));
    Ok(())
}

fn put(key: Bytes, value: Bytes, ttl: Ttl) -> RecordOp {
    RecordOp::Put(PutRecordOp::new_with_options(
        Record::new(key, value),
        PutOptions { ttl },
    ))
}

fn duration_ns(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos())
        .map_err(|_| Error::Invalid("duration exceeds i64 nanoseconds".to_owned()))
}

fn unix_time_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid("system clock is before the Unix epoch".to_owned()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::Invalid("Unix timestamp exceeds u64 milliseconds".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
    };

    use super::*;
    use crate::config::PageConfig;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "line-test".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            segment_duration: Duration::from_secs(10),
            retention: Some(Duration::from_secs(60)),
            page: PageConfig {
                target_size_bytes: 64,
                max_rows: 2,
                max_age: Duration::from_secs(60),
                rows_per_block: 1,
            },
        }
    }

    fn labels(service: &str, environment: &str) -> Labels {
        Labels::new(vec![
            Label::new("service", service),
            Label::new("environment", environment),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn writes_and_reads_pages_through_slatedb() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("tenant-a").unwrap();
        let report = db
            .write(
                &namespace,
                vec![
                    LogBatch::new(
                        labels("api", "prod"),
                        vec![
                            LogEntry::new(1, "one"),
                            LogEntry::new(2, "two"),
                            LogEntry::new(11_000_000_000, "next segment"),
                        ],
                    ),
                    LogBatch::new(labels("worker", "prod"), vec![LogEntry::new(3, "work")]),
                ],
            )
            .await
            .unwrap();
        assert_eq!(report.rows, 4);
        assert_eq!(report.streams, 3);

        let prod = db
            .read(
                &namespace,
                0,
                12_000_000_000,
                &[Label::new("environment", "prod")],
            )
            .await
            .unwrap();
        assert_eq!(prod.len(), 4);

        let api = db
            .read(
                &namespace,
                0,
                12_000_000_000,
                &[
                    Label::new("environment", "prod"),
                    Label::new("service", "api"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            api.iter()
                .map(|row| row.entry.line.as_str())
                .collect::<Vec<_>>(),
            vec!["one", "two", "next segment"]
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn repeated_writes_merge_label_postings() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(1, "a")],
            )],
        )
        .await
        .unwrap();
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels("worker", "prod"),
                vec![LogEntry::new(2, "b")],
            )],
        )
        .await
        .unwrap();
        let rows = db
            .read(&namespace, 0, 5, &[Label::new("environment", "prod")])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn identical_writes_allocate_distinct_pages() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let batch = || {
            LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(1, "same accepted entry")],
            )
        };

        db.write(&namespace, vec![batch()]).await.unwrap();
        db.write(&namespace, vec![batch()]).await.unwrap();

        let rows = db
            .read(&namespace, 0, 5, &[Label::new("service", "api")])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].entry, rows[1].entry);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn small_writes_top_up_the_trailing_posting_block() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        for timestamp in 1..=5 {
            db.write(
                &namespace,
                vec![LogBatch::new(
                    labels("search", "prod"),
                    vec![LogEntry::new(timestamp, "needle in haystack")],
                )],
            )
            .await
            .unwrap();
        }
        let stats = db
            .storage
            .get(crate::codec::term_stats_key(&namespace, 0, "needle"))
            .await
            .unwrap()
            .unwrap();
        let stats = crate::search::decode_term_stats(&stats.value).unwrap();
        assert_eq!(stats.documents, 5);
        assert_eq!(stats.blocks, 1);

        let terms = vec!["needle".to_owned()];
        for top_k in [None, Some(2)] {
            let rows = db
                .read_match_bounded(&namespace, 0, 10, &[], (&terms, top_k), 10)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(rows.len(), 5);
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn logical_retention_filters_reads_estimates_and_bm25_before_compaction() {
        let mut config = test_config();
        config.retention = Some(Duration::from_millis(20));
        let db = LogDb::open(config).await.unwrap();
        let namespace = Namespace::default();
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels("search", "prod"),
                vec![LogEntry::new(1, "needle")],
            )],
        )
        .await
        .unwrap();
        assert_eq!(db.read(&namespace, 0, 2, &[]).await.unwrap().len(), 1);

        tokio::time::sleep(Duration::from_millis(40)).await;

        assert!(db.read(&namespace, 0, 2, &[]).await.unwrap().is_empty());
        assert_eq!(
            db.estimate_pages(&namespace, 0, 2, &[]).await.unwrap(),
            QueryEstimate::default()
        );
        let terms = vec!["needle".to_owned()];
        assert_eq!(
            db.read_match_bounded(&namespace, 0, 2, &[], (&terms, None), 10)
                .await
                .unwrap(),
            Some(Vec::new())
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn persisted_logical_expiry_survives_reopen_without_physical_ttl() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logical-expiry-reopen".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: directory.path().to_string_lossy().into_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            ..test_config()
        };
        let namespace = Namespace::default();
        let db = LogDb::open(config.clone()).await.unwrap();
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels("search", "prod"),
                vec![LogEntry::new(1, "needle")],
            )],
        )
        .await
        .unwrap();

        let stream_id = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
        let mut metadata_records = db
            .storage
            .scan_iter(metadata_range(&namespace, 0, stream_id))
            .await
            .unwrap();
        let record = metadata_records.next().await.unwrap().unwrap();
        let mut metadata = decode_metadata(&record.value).unwrap();
        metadata.expires_at_unix_ms = Some(0);
        db.storage
            .apply(vec![put(
                record.key,
                encode_metadata(&metadata).unwrap(),
                Ttl::NoExpiry,
            )])
            .await
            .unwrap();
        db.flush().await.unwrap();
        assert!(db.read(&namespace, 0, 2, &[]).await.unwrap().is_empty());
        db.close().await.unwrap();

        let reopened = LogDb::open(config).await.unwrap();
        assert!(
            reopened
                .read(&namespace, 0, 2, &[])
                .await
                .unwrap()
                .is_empty()
        );
        let terms = vec!["needle".to_owned()];
        assert_eq!(
            reopened
                .read_match_bounded(&namespace, 0, 2, &[], (&terms, None), 10)
                .await
                .unwrap(),
            Some(Vec::new())
        );
        reopened.close().await.unwrap();
    }
}
