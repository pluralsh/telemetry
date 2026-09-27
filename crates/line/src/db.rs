// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::storage::{PutOptions, PutRecordOp, Record, RecordOp, Storage, Ttl, WriteOptions};
use common::{StorageBuilder, StorageSemantics};
use roaring::RoaringBitmap;
use tokio::sync::Mutex;

use crate::codec::{
    CURRENT_PAGE_METADATA_VERSION, PageId, StoredPageMetadata, decode_forward_key, decode_labels,
    decode_metadata, decode_metadata_key, decode_page_sequence, decode_postings, decode_stream_id,
    dictionary_key, encode_labels, encode_metadata, encode_page_sequence, encode_postings,
    encode_stream_id, field_stats_key, forward_key, forward_range, metadata_key, metadata_range,
    next_page_sequence_key, next_stream_id_key, payload_key, posting_key, segment_for,
    term_directory_key, term_posting_block_key, term_stats_key,
};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{Label, Labels, LogBatch, LogEntry, LogRow, SegmentId, StreamId};
use crate::namespace::Namespace;
use crate::page::{Page, PageBuilder};
use crate::search::{
    BlockDirectoryEntry, DIRECTORY_ENTRIES, FieldStats, IndexDelta, POSTINGS_PER_BLOCK, TermStats,
    block_max_scores, decode as decode_search, encode as encode_search, source_matches,
};
use crate::segment::LogSegmentExtractor;

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
        let semantics =
            StorageSemantics::new().with_segment_extractor(LogSegmentExtractor::shared());
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
        let mut groups: BTreeMap<(SegmentId, [u8; 16]), (Labels, Vec<LogEntry>)> = BTreeMap::new();
        for batch in batches {
            let fingerprint = batch.labels.fingerprint();
            for entry in batch.entries {
                let segment = segment_for(entry.timestamp_ns, self.segment_ns);
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
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }
        for (_, entries) in groups.values_mut() {
            entries.sort_by_key(|entry| entry.timestamp_ns);
        }

        let _guard = self.write_lock.lock().await;
        let ttl = self.ttl()?;
        let page_retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
        };
        let mut ops = Vec::new();
        let mut next_ids: HashMap<SegmentId, StreamId> = HashMap::new();
        let mut next_dirty: HashMap<SegmentId, bool> = HashMap::new();
        let mut postings: HashMap<(SegmentId, Label), RoaringBitmap> = HashMap::new();
        let mut search_deltas: BTreeMap<SegmentId, IndexDelta> = BTreeMap::new();
        let mut report = WriteReport {
            streams: groups.len(),
            ..WriteReport::default()
        };

        for ((segment, fingerprint), (labels, entries)) in groups {
            let dictionary = dictionary_key(namespace, segment, fingerprint);
            let stream_id = if let Some(record) = self.storage.get(dictionary.clone()).await? {
                let stream_id = decode_stream_id(&record.value)?;
                let existing = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| Error::Corrupt("dictionary has no forward labels".to_owned()))?;
                if decode_labels(&existing.value)? != labels {
                    return Err(Error::Corrupt(
                        "stream fingerprint maps to different labels".to_owned(),
                    ));
                }
                ops.push(put(dictionary, encode_stream_id(stream_id), ttl));
                ops.push(put(
                    forward_key(namespace, segment, stream_id),
                    encode_labels(&labels)?,
                    ttl,
                ));
                stream_id
            } else {
                let next = match next_ids.get(&segment) {
                    Some(next) => *next,
                    None => {
                        let value = self
                            .storage
                            .get(next_stream_id_key(namespace, segment))
                            .await?
                            .map(|record| decode_stream_id(&record.value))
                            .transpose()?
                            .unwrap_or(0);
                        next_ids.insert(segment, value);
                        value
                    }
                };
                let following = next
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("segment exhausted stream IDs".to_owned()))?;
                next_ids.insert(segment, following);
                next_dirty.insert(segment, true);
                ops.push(put(dictionary, encode_stream_id(next), ttl));
                ops.push(put(
                    forward_key(namespace, segment, next),
                    encode_labels(&labels)?,
                    ttl,
                ));
                next
            };

            for label in labels.iter() {
                let cache_key = (segment, label.clone());
                if !postings.contains_key(&cache_key) {
                    let bitmap = self
                        .storage
                        .get(posting_key(namespace, segment, label))
                        .await?
                        .map(|record| decode_postings(&record.value))
                        .transpose()?
                        .unwrap_or_default();
                    postings.insert(cache_key.clone(), bitmap);
                }
                postings.get_mut(&cache_key).unwrap().insert(stream_id);
            }

            let page_sequence_key = next_page_sequence_key(namespace, segment, stream_id);
            let mut next_page_sequence = self
                .storage
                .get(page_sequence_key.clone())
                .await?
                .map(|record| decode_page_sequence(&record.value))
                .transpose()?
                .unwrap_or(0);
            let mut builder = PageBuilder::new(self.config.page.clone(), Instant::now())?;
            for entry in entries {
                report.rows += 1;
                if let Some(page) = builder.append(entry, Instant::now())? {
                    index_page(
                        search_deltas.entry(segment).or_default(),
                        stream_id,
                        next_page_sequence,
                        &page,
                    )?;
                    append_page_ops(
                        &mut ops,
                        namespace,
                        segment,
                        stream_id,
                        next_page_sequence,
                        page,
                        page_retention,
                    )?;
                    next_page_sequence = next_page_sequence.checked_add(1).ok_or_else(|| {
                        Error::Invalid("stream exhausted page sequences".to_owned())
                    })?;
                    report.pages += 1;
                }
            }
            if let Some(page) = builder.finish()? {
                index_page(
                    search_deltas.entry(segment).or_default(),
                    stream_id,
                    next_page_sequence,
                    &page,
                )?;
                append_page_ops(
                    &mut ops,
                    namespace,
                    segment,
                    stream_id,
                    next_page_sequence,
                    page,
                    page_retention,
                )?;
                next_page_sequence = next_page_sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("stream exhausted page sequences".to_owned()))?;
                report.pages += 1;
            }
            ops.push(put(
                page_sequence_key,
                encode_page_sequence(next_page_sequence),
                ttl,
            ));
        }

        for (segment, dirty) in next_dirty {
            if dirty {
                ops.push(put(
                    next_stream_id_key(namespace, segment),
                    encode_stream_id(next_ids[&segment]),
                    ttl,
                ));
            }
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
            .map(|record| decode_search::<FieldStats>(&record.value))
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
            encode_search(&field)?,
            ttl,
        ));

        for (term, mut term_postings) in delta.postings {
            term_postings.sort_by_key(|posting| posting.address);
            let stats_key = term_stats_key(namespace, segment, &term);
            let mut stats = self
                .storage
                .get(stats_key.clone())
                .await?
                .map(|record| decode_search::<TermStats>(&record.value))
                .transpose()?
                .unwrap_or_default();
            stats.documents = stats
                .documents
                .checked_add(term_postings.len() as u64)
                .ok_or_else(|| Error::Invalid("term document count overflow".into()))?;

            let mut dirty_directories: BTreeMap<u32, Vec<BlockDirectoryEntry>> = BTreeMap::new();
            for chunk in term_postings.chunks(POSTINGS_PER_BLOCK) {
                let ordinal = stats.blocks;
                stats.blocks = stats
                    .blocks
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("term block ordinal overflow".into()))?;
                let directory_ordinal = ordinal / DIRECTORY_ENTRIES as u32;
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    dirty_directories.entry(directory_ordinal)
                {
                    let entries = self
                        .storage
                        .get(term_directory_key(
                            namespace,
                            segment,
                            &term,
                            directory_ordinal,
                        ))
                        .await?
                        .map(|record| decode_search::<Vec<BlockDirectoryEntry>>(&record.value))
                        .transpose()?
                        .unwrap_or_default();
                    if entries.len() > DIRECTORY_ENTRIES {
                        return Err(Error::Corrupt("term directory exceeds bound".into()));
                    }
                    entry.insert(entries);
                }
                let entries = dirty_directories.get_mut(&directory_ordinal).unwrap();
                if entries.len() >= DIRECTORY_ENTRIES {
                    return Err(Error::Corrupt("term directory ordinal is full".into()));
                }
                entries.push(BlockDirectoryEntry {
                    ordinal,
                    postings: u16::try_from(chunk.len())
                        .map_err(|_| Error::Invalid("posting block exceeds u16".into()))?,
                    max_frequency: chunk
                        .iter()
                        .map(|posting| posting.frequency)
                        .max()
                        .unwrap_or(0),
                    min_length: chunk
                        .iter()
                        .map(|posting| posting.length)
                        .min()
                        .unwrap_or(0),
                });
                ops.push(put(
                    term_posting_block_key(namespace, segment, &term, ordinal),
                    encode_search(&chunk)?,
                    ttl,
                ));
            }
            for (ordinal, entries) in dirty_directories {
                ops.push(put(
                    term_directory_key(namespace, segment, &term, ordinal),
                    encode_search(&entries)?,
                    ttl,
                ));
            }
            ops.push(put(stats_key, encode_search(&stats)?, ttl));
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
        self.read_bounded(namespace, start_ns, end_ns, matchers, usize::MAX)
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

    /// Reads at most `max_pages` overlapping pages. This is the storage
    /// primitive used by the query engine to put a hard bound on I/O.
    pub(crate) async fn read_bounded(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        matchers: &[Label],
        max_pages: usize,
    ) -> Result<Vec<LogRow>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let first_segment = segment_for(start_ns, self.segment_ns);
        let last_segment = segment_for(end_ns, self.segment_ns);
        let now_unix_ms = unix_time_ms()?;
        let mut segment = first_segment;
        let mut rows = Vec::new();
        let mut pages_read = 0usize;

        loop {
            let stream_ids = self.stream_ids(namespace, segment, matchers).await?;
            for stream_id in stream_ids {
                let Some(labels_record) = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                else {
                    return Err(Error::Corrupt(
                        "posting references missing forward labels".to_owned(),
                    ));
                };
                let labels = decode_labels(&labels_record.value)?;
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
                    rows.extend(
                        page.decode_range(start_ns, end_ns)?
                            .into_iter()
                            .enumerate()
                            .map(|(row_index, entry)| {
                                (
                                    page_id.sequence,
                                    row_index,
                                    LogRow {
                                        labels: labels.clone(),
                                        entry,
                                    },
                                )
                            }),
                    );
                }
            }
            if segment == last_segment {
                break;
            }
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
        }
        rows.sort_by(|left, right| {
            left.2
                .entry
                .timestamp_ns
                .cmp(&right.2.entry.timestamp_ns)
                .then_with(|| {
                    left.2
                        .labels
                        .fingerprint()
                        .cmp(&right.2.labels.fingerprint())
                })
                .then_with(|| left.0.cmp(&right.0))
                .then_with(|| left.1.cmp(&right.1))
        });
        Ok(rows.into_iter().map(|(_, _, row)| row).collect())
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
            let mut allowed_pages = BTreeSet::new();
            for stream_id in stream_ids {
                let labels_record = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".into())
                    })?;
                let labels = decode_labels(&labels_record.value)?;
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
                for (row_id, entry) in page.decode_range_with_ids(start_ns, end_ns)? {
                    let Some(score) = page_scores.get(&row_id) else {
                        continue;
                    };
                    // Stored postings are candidates, never authority.
                    if source_matches(&entry.line, terms) {
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
        version: CURRENT_PAGE_METADATA_VERSION,
        expires_at_unix_ms: retention.expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        row_count: page.row_count(),
        payload_bytes: u32::try_from(bytes.len())
            .map_err(|_| Error::Invalid("page payload exceeds u32".to_owned()))?,
        blocks: page.blocks().to_vec(),
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

fn index_page(
    search_delta: &mut IndexDelta,
    stream_id: StreamId,
    sequence: u64,
    page: &Page,
) -> Result<()> {
    search_delta.add_page(
        stream_id,
        sequence,
        page.decode_range(i64::MIN, i64::MAX)?
            .into_iter()
            .map(|entry| entry.line),
    )
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
