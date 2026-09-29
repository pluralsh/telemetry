// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{
    Delta, Durability as CoordinatorDurability, Flusher, WriteCoordinator, WriteCoordinatorHandle,
    WriteError,
};
use common::discovery::{
    CatalogBatch, DiscoveryCache, DiscoveryValue, names as catalog_names, values as catalog_values,
};
use common::storage::{
    PutOptions, PutRecordOp, Record, RecordOp, Storage, StorageRead, Ttl, WriteOptions,
};
use common::{StorageBuilder, StorageReaderRuntime, StorageSemantics, create_storage_read};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::Namespace;
use crate::analyzer::DEFAULT_ANALYZER;
use crate::codec::{
    PageId, StoredPageMetadata, decode_forward_key, decode_labels, decode_metadata,
    decode_metadata_key, decode_page_sequence, decode_postings, decode_stream_id, dictionary_key,
    encode_labels, encode_metadata, encode_page_sequence, encode_postings, encode_stream_id,
    field_stats_key, forward_key, forward_range, metadata_key, metadata_range,
    next_page_sequence_key, next_stream_id_key, payload_key, posting_key, segment_for,
    segment_prefix,
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
const WRITE_CHANNEL: &str = "write";

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
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    segment_ns: i64,
    owned_slots: Range<u16>,
    write_handle: Option<WriteCoordinatorHandle<LineWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<LineWriteDelta, LineFlusher>>>,
    label_names_cache: DiscoveryCache<(Namespace, SegmentId), Vec<String>>,
    label_values_cache: DiscoveryCache<(Namespace, SegmentId, String), Vec<String>>,
}

impl LogDb {
    pub async fn open(config: Config) -> Result<Self> {
        Self::open_with_slots(config, 0..sharding::ROUTING_SLOT_COUNT).await
    }

    /// Warms SlateDB caches for recent log segments in this shard.
    pub async fn warm_recent(
        &self,
        namespace: &Namespace,
        warm_range: Duration,
        include_payloads: bool,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let Some(slate) = self.storage.slate_read() else {
            return Ok(());
        };
        let end_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
            .unwrap_or(i64::MAX);
        let range_ns = i64::try_from(warm_range.as_nanos()).unwrap_or(i64::MAX);
        let prefixes = self
            .discovery_segments(end_ns.saturating_sub(range_ns), end_ns)?
            .into_iter()
            .map(|segment| segment_prefix(namespace, segment))
            .collect::<Vec<_>>();
        slate
            .warm_prefixes(&prefixes, include_payloads, cancel)
            .await?;
        Ok(())
    }

    pub(crate) async fn open_with_slots(config: Config, owned_slots: Range<u16>) -> Result<Self> {
        config.validate()?;
        if owned_slots.start >= owned_slots.end || owned_slots.end > sharding::ROUTING_SLOT_COUNT {
            return Err(Error::Invalid(format!(
                "invalid owned routing slot range {owned_slots:?}"
            )));
        }
        let segment_ns = duration_ns(config.segment_duration)?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        let storage_read = storage.clone();
        let direct_writer = Arc::new(DirectWriter {
            storage: storage_read.clone(),
            writer: storage.clone(),
            config: config.clone(),
            owned_slots: owned_slots.clone(),
        });
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            (),
            LineFlusher {
                direct_writer,
                storage: storage.clone(),
            },
        );
        let write_handle = write_coordinator.handle(WRITE_CHANNEL);
        write_coordinator.start();
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            segment_ns,
            owned_slots,
            write_handle: Some(write_handle),
            write_coordinator: Mutex::new(Some(write_coordinator)),
            label_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            label_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    pub(crate) async fn open_reader_with_slots(
        config: Config,
        owned_slots: Range<u16>,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
        if owned_slots.start >= owned_slots.end || owned_slots.end > sharding::ROUTING_SLOT_COUNT {
            return Err(Error::Invalid(format!(
                "invalid owned routing slot range {owned_slots:?}"
            )));
        }
        let segment_ns = duration_ns(config.segment_duration)?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            semantics,
            reader_options,
        )
        .await?;
        Ok(Self {
            storage,
            writer: None,
            segment_ns,
            owned_slots,
            write_handle: None,
            write_coordinator: Mutex::new(None),
            label_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            label_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    fn write_handle(&self) -> Result<&WriteCoordinatorHandle<LineWriteDelta>> {
        self.write_handle
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
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
        let groups = group_by_stream(namespace, batches, self.segment_ns)?;
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }
        self.label_names_cache.clear();
        self.label_values_cache.clear();
        if let Some((_, slot, _)) = groups
            .keys()
            .find(|(_, slot, _)| !self.owned_slots.contains(slot))
        {
            return Err(Error::Invalid(format!(
                "routing slot {slot} is outside opened shard range {:?}",
                self.owned_slots
            )));
        }
        let write = LineWrite {
            namespace: namespace.clone(),
            groups,
        };
        let mut write_handle = self
            .write_handle()?
            .try_write(write)
            .await
            .map_err(map_write_error)?;
        let report = write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)?;

        if durability != Durability::Applied {
            let mut flush_handle = self
                .write_handle()?
                .flush(false)
                .await
                .map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
        }
        if durability == Durability::Durable {
            self.writer
                .as_ref()
                .expect("write handle requires writer")
                .flush()
                .await?;
        }
        Ok(report)
    }
}

struct DirectWriter {
    storage: Arc<dyn StorageRead>,
    writer: Arc<dyn Storage>,
    config: Config,
    owned_slots: Range<u16>,
}

impl DirectWriter {
    async fn write_groups(&self, groups: FrozenLineWriteDelta) -> Result<()> {
        let ttl = self.ttl()?;
        let retention = PageRetention {
            physical_ttl: ttl,
            expires_at_unix_ms: self.logical_expiry()?,
        };
        let mut by_namespace: BTreeMap<Namespace, StreamGroups> = BTreeMap::new();
        for ((namespace, segment, slot, fingerprint), group) in groups.groups {
            by_namespace
                .entry(namespace)
                .or_default()
                .insert((segment, slot, fingerprint), group);
        }
        let mut all_ops = Vec::new();
        for (namespace, groups) in by_namespace {
            let mut write = PendingWrite::new(ttl, retention, groups.len());
            for ((segment, slot, fingerprint), (labels, entries)) in groups {
                if !self.owned_slots.contains(&slot) {
                    return Err(Error::Invalid(format!(
                        "routing slot {slot} is outside opened shard range {:?}",
                        self.owned_slots
                    )));
                }
                let stream_id = self
                    .resolve_stream_id(&mut write, &namespace, segment, slot, fingerprint, &labels)
                    .await?;
                self.add_label_postings(&mut write, &namespace, segment, slot, &labels, stream_id)
                    .await?;
                self.append_stream_pages(&mut write, &namespace, segment, slot, stream_id, entries)
                    .await?;
            }
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
        Ok(())
    }

    /// Returns the stream's existing ID, or allocates the segment's next one,
    /// and rewrites its dictionary and forward-label records to refresh TTLs.
    async fn resolve_stream_id(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        slot: u16,
        fingerprint: StreamFingerprint,
        labels: &Labels,
    ) -> Result<StreamId> {
        let dictionary = dictionary_key(namespace, segment, slot, fingerprint);
        let stream_id = match self.storage.get(dictionary.clone()).await? {
            Some(record) => {
                let stream_id = decode_stream_id(&record.value)?;
                let existing = self
                    .storage
                    .get(forward_key(namespace, segment, slot, stream_id))
                    .await?
                    .ok_or_else(|| Error::Corrupt("dictionary has no forward labels".to_owned()))?;
                if decode_labels(&existing.value)? != *labels {
                    return Err(Error::Corrupt(
                        "stream fingerprint maps to different labels".to_owned(),
                    ));
                }
                stream_id
            }
            None => {
                self.allocate_stream_id(write, namespace, segment, slot)
                    .await?
            }
        };
        write.put(dictionary, encode_stream_id(stream_id));
        write.put(
            forward_key(namespace, segment, slot, stream_id),
            encode_labels(labels)?,
        );
        Ok(stream_id)
    }

    async fn allocate_stream_id(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        slot: u16,
    ) -> Result<StreamId> {
        let next = match write.next_stream_ids.get(&(segment, slot)) {
            Some(next) => *next,
            None => self
                .storage
                .get(next_stream_id_key(namespace, segment, slot))
                .await?
                .map(|record| decode_stream_id(&record.value))
                .transpose()?
                .unwrap_or(0),
        };
        let following = next
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segment exhausted stream IDs".to_owned()))?;
        write.next_stream_ids.insert((segment, slot), following);
        Ok(next)
    }

    async fn add_label_postings(
        &self,
        write: &mut PendingWrite,
        namespace: &Namespace,
        segment: SegmentId,
        slot: u16,
        labels: &Labels,
        stream_id: StreamId,
    ) -> Result<()> {
        for label in labels.iter() {
            let bitmap = match write.postings.entry((segment, slot, label.clone())) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let bitmap = self
                        .storage
                        .get(posting_key(namespace, segment, slot, label))
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
        slot: u16,
        stream_id: StreamId,
        entries: Vec<LogEntry>,
    ) -> Result<()> {
        let page_sequence_key = next_page_sequence_key(namespace, segment, slot, stream_id);
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
                write.add_page(
                    namespace,
                    segment,
                    slot,
                    stream_id,
                    &mut sequence,
                    completed,
                )?;
            }
        }
        if let Some(completed) = builder.finish_with_rows()? {
            write.add_page(
                namespace,
                segment,
                slot,
                stream_id,
                &mut sequence,
                completed,
            )?;
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
        for (segment, _, label) in postings.keys() {
            catalogs.entry(*segment).or_default().insert(
                "",
                &label.name,
                DiscoveryValue::String(label.value.clone()),
            );
        }
        for ((segment, slot), next) in next_stream_ids {
            ops.push(put(
                next_stream_id_key(namespace, segment, slot),
                encode_stream_id(next),
                ttl,
            ));
        }
        for ((segment, slot, label), bitmap) in postings {
            ops.push(put(
                posting_key(namespace, segment, slot, &label),
                encode_postings(&bitmap)?,
                ttl,
            ));
        }
        for (segment, catalog) in catalogs {
            ops.extend(catalog.into_ops(&segment_prefix(namespace, segment), ttl));
        }
        for ((segment, slot), delta) in search_deltas {
            self.append_search_index_ops(&mut ops, namespace, segment, slot, delta, ttl)
                .await?;
        }
        Ok((ops, report))
    }

    async fn append_search_index_ops(
        &self,
        ops: &mut Vec<RecordOp>,
        namespace: &Namespace,
        segment: SegmentId,
        slot: u16,
        delta: IndexDelta,
        ttl: Ttl,
    ) -> Result<()> {
        let mut field = self
            .storage
            .get(field_stats_key(namespace, segment, slot))
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
            field_stats_key(namespace, segment, slot),
            encode_field_stats(field),
            ttl,
        ));

        let storage = self.storage.as_ref();
        let mut writes = std::pin::pin!(
            futures::stream::iter(delta.postings)
                .map(|(term, postings)| {
                    term_index_writes(storage, namespace, segment, slot, term, postings)
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
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            let key = (namespace.clone(), segment);
            let names = if let Some(names) = self.label_names_cache.get(&key) {
                names
            } else {
                let names = catalog_names(
                    self.storage.as_ref(),
                    &segment_prefix(namespace, segment),
                    Some(""),
                )
                .await?;
                self.label_names_cache
                    .insert(key, names, self.is_active_segment(segment))
            };
            result.extend(names.iter().cloned());
        }
        Ok(result.into_iter().collect())
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
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            let key = (namespace.clone(), segment, name.to_owned());
            let values = if let Some(values) = self.label_values_cache.get(&key) {
                values
            } else {
                let mut strings = Vec::new();
                for value in catalog_values(
                    self.storage.as_ref(),
                    &segment_prefix(namespace, segment),
                    "",
                    name,
                )
                .await?
                {
                    match value {
                        DiscoveryValue::String(value) => strings.push(value),
                        _ => {
                            return Err(Error::Corrupt(
                                "stream-label catalog contains a non-string value".into(),
                            ));
                        }
                    }
                }
                self.label_values_cache
                    .insert(key, strings, self.is_active_segment(segment))
            };
            result.extend(values.iter().cloned());
        }
        Ok(result.into_iter().collect())
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
        let mut result = BTreeSet::new();
        for segment in self.discovery_segments(start_ns, end_ns)? {
            for selector in &selectors {
                for (slot, stream_id) in
                    self.stream_ids(namespace, segment, &selector.exact).await?
                {
                    let record = self
                        .storage
                        .get(forward_key(namespace, segment, slot, stream_id))
                        .await?
                        .ok_or_else(|| {
                            Error::Corrupt("posting references missing forward labels".into())
                        })?;
                    let labels = decode_labels(&record.value)?;
                    if selector.matches(&labels) {
                        result.insert(labels);
                    }
                }
            }
        }
        Ok(result.into_iter().collect())
    }

    fn validate_discovery_range(&self, start_ns: i64, end_ns: i64) -> Result<()> {
        if end_ns < start_ns {
            Err(Error::Invalid("end_ns must be >= start_ns".to_owned()))
        } else {
            Ok(())
        }
    }

    fn discovery_segments(&self, start_ns: i64, end_ns: i64) -> Result<Vec<SegmentId>> {
        let last = segment_for(end_ns, self.segment_ns);
        let mut segment = segment_for(start_ns, self.segment_ns);
        let mut result = Vec::new();
        loop {
            result.push(segment);
            if segment == last {
                return Ok(result);
            }
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("discovery segment range overflow".into()))?;
        }
    }

    fn is_active_segment(&self, segment: SegmentId) -> bool {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
            .unwrap_or(i64::MAX);
        segment_for(now_ns, self.segment_ns) == segment
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
            for (slot, stream_id) in self.stream_ids(namespace, segment, matchers).await? {
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment, slot, stream_id))
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
            for (slot, stream_id) in self.stream_ids(namespace, segment, matchers).await? {
                let Some(labels_record) = self
                    .storage
                    .get(forward_key(namespace, segment, slot, stream_id))
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
                    .scan_iter(metadata_range(namespace, segment, slot, stream_id))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let (_, _, page_id) = decode_metadata_key(&record.key)?;
                    let page_metadata = decode_metadata(&record.value)?;
                    if page_metadata.is_expired_at(now_unix_ms)
                        || page_metadata.max_timestamp_ns < start_ns
                        || page_metadata.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    budget.take()?;
                    pages.push((slot, stream_id, labels.clone(), fingerprint, page_id));
                }
            }
            let mut decoded = futures::stream::iter(pages)
                .map(
                    move |(slot, stream_id, labels, fingerprint, page_id)| async move {
                        let payload = self
                            .storage
                            .get(payload_key(namespace, segment, slot, stream_id, page_id))
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
            let mut allowed_pages: HashMap<u16, HashSet<(StreamId, u64)>> = HashMap::new();
            for (slot, stream_id) in stream_ids {
                let labels_record = self
                    .storage
                    .get(forward_key(namespace, segment, slot, stream_id))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".into())
                    })?;
                let labels = Arc::new(decode_labels(&labels_record.value)?);
                let mut metadata = self
                    .storage
                    .scan_iter(metadata_range(namespace, segment, slot, stream_id))
                    .await?;
                while let Some(record) = metadata.next().await? {
                    let (_, _, page_id) = decode_metadata_key(&record.key)?;
                    let page_metadata = decode_metadata(&record.value)?;
                    if page_metadata.is_expired_at(now_unix_ms)
                        || page_metadata.max_timestamp_ns < start_ns
                        || page_metadata.min_timestamp_ns > end_ns
                    {
                        continue;
                    }
                    allowed_pages
                        .entry(slot)
                        .or_default()
                        .insert((stream_id, page_id.sequence));
                    candidate_pages.push((slot, stream_id, labels.clone(), page_id));
                }
            }
            let mut by_page: HashMap<(u16, StreamId, u64), HashMap<u32, f32>> = HashMap::new();
            for (slot, allowed) in &allowed_pages {
                let Some(scores) = block_max_scores(
                    self.storage.as_ref(),
                    namespace,
                    segment,
                    *slot,
                    terms,
                    allowed,
                    top_k,
                )
                .await?
                else {
                    return Ok(None);
                };
                for (address, score) in scores {
                    by_page
                        .entry((*slot, address.stream_id, address.page_sequence))
                        .or_default()
                        .insert(address.row_id, score);
                }
            }

            for (slot, stream_id, labels, page_id) in candidate_pages {
                let Some(page_scores) = by_page.get(&(slot, stream_id, page_id.sequence)) else {
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
                    .get(payload_key(namespace, segment, slot, stream_id, page_id))
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
        if let Some(handle) = &self.write_handle {
            let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
            self.writer
                .as_ref()
                .expect("write handle requires writer")
                .flush()
                .await?;
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

    async fn stream_ids(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        matchers: &[Label],
    ) -> Result<Vec<(u16, StreamId)>> {
        if matchers.is_empty() {
            let mut result = Vec::new();
            for slot in self.owned_slots.clone() {
                let mut iterator = self
                    .storage
                    .scan_iter(forward_range(namespace, segment, slot))
                    .await?;
                while let Some(record) = iterator.next().await? {
                    result.push(decode_forward_key(&record.key)?);
                }
            }
            return Ok(result);
        }

        let mut streams = Vec::new();
        for slot in self.owned_slots.clone() {
            let mut result: Option<RoaringBitmap> = None;
            for matcher in matchers {
                let Some(record) = self
                    .storage
                    .get(posting_key(namespace, segment, slot, matcher))
                    .await?
                else {
                    result = Some(RoaringBitmap::new());
                    break;
                };
                let bitmap = decode_postings(&record.value)?;
                match &mut result {
                    Some(result) => *result &= bitmap,
                    None => result = Some(bitmap),
                }
            }
            streams.extend(
                result
                    .unwrap_or_default()
                    .iter()
                    .map(|stream_id| (slot, stream_id)),
            );
        }
        Ok(streams)
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
        use crate::logql::{Expr, MatchOp};

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
        let mut exact = Vec::new();
        let mut matchers = Vec::with_capacity(log.selector.value.matchers.len());
        for matcher in log.selector.value.matchers {
            let matcher = matcher.value;
            let name = matcher.label;
            let value = matcher.value;
            match matcher.op {
                MatchOp::Equal => {
                    exact.push(Label::new(&name, &value));
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

fn anchored_regex(pattern: &str) -> Result<regex::Regex> {
    Ok(regex::Regex::new(&format!("^(?:{pattern})$"))?)
}

type StreamGroups = BTreeMap<(SegmentId, u16, StreamFingerprint), (Labels, Vec<LogEntry>)>;
type CoordinatedStreamGroups =
    BTreeMap<(Namespace, SegmentId, u16, StreamFingerprint), (Labels, Vec<LogEntry>)>;

struct LineWrite {
    namespace: Namespace,
    groups: StreamGroups,
}

struct FrozenLineWriteDelta {
    groups: CoordinatedStreamGroups,
}

struct LineWriteDelta {
    groups: CoordinatedStreamGroups,
}

impl Delta for LineWriteDelta {
    type Context = ();
    type Write = LineWrite;
    type Frozen = FrozenLineWriteDelta;
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
        for ((segment, slot, fingerprint), (labels, _)) in &write.groups {
            if let Some((existing, _)) =
                self.groups
                    .get(&(write.namespace.clone(), *segment, *slot, *fingerprint))
                && existing != labels
            {
                return Err("stream fingerprint collision across writes".to_owned());
            }
        }
        for ((segment, slot, fingerprint), (labels, mut entries)) in write.groups {
            let group = self
                .groups
                .entry((write.namespace.clone(), segment, slot, fingerprint))
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
            FrozenLineWriteDelta {
                groups: self.groups,
            },
            (),
            (),
        )
    }

    fn reader(&self) -> Self::DeltaView {}
}

struct LineFlusher {
    direct_writer: Arc<DirectWriter>,
    storage: Arc<dyn Storage>,
}

#[async_trait]
impl Flusher<LineWriteDelta> for LineFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenLineWriteDelta,
        _epoch_range: &Range<u64>,
    ) -> std::result::Result<(), String> {
        self.direct_writer
            .write_groups(frozen)
            .await
            .map_err(|error| error.to_string())
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage
            .flush()
            .await
            .map_err(|error| error.to_string())
    }
}

fn map_write_error<T>(error: WriteError<T>) -> Error {
    match error {
        WriteError::Backpressure(_) | WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Unavailable("write coordinator is shut down".to_owned()),
        WriteError::ApplyError(_, message) => Error::Invalid(message),
        WriteError::FlushError(message) | WriteError::Internal(message) => {
            Error::Unavailable(message)
        }
    }
}

/// Groups entries by `(segment, routing slot, stream)`, sorted by timestamp.
fn group_by_stream(
    namespace: &Namespace,
    batches: Vec<LogBatch>,
    segment_ns: i64,
) -> Result<StreamGroups> {
    let mut groups = StreamGroups::new();
    for batch in batches {
        let fingerprint = batch.labels.fingerprint();
        let slot = crate::routing::routing_slot(namespace, &batch.labels);
        for entry in batch.entries {
            let segment = segment_for(entry.timestamp_ns, segment_ns);
            let group = groups
                .entry((segment, slot, fingerprint))
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
    /// Next unallocated stream ID for each segment and slot that allocated one.
    next_stream_ids: HashMap<(SegmentId, u16), StreamId>,
    postings: HashMap<(SegmentId, u16, Label), RoaringBitmap>,
    search_deltas: BTreeMap<(SegmentId, u16), IndexDelta>,
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
        slot: u16,
        stream_id: StreamId,
        sequence: &mut u64,
        (page, rows): (Page, Vec<LogEntry>),
    ) -> Result<()> {
        self.search_deltas
            .entry((segment, slot))
            .or_default()
            .add_page(
                &DEFAULT_ANALYZER,
                stream_id,
                *sequence,
                rows.iter().map(|row| row.line.as_str()),
            )?;
        append_page_ops(
            &mut self.ops,
            namespace,
            PageWriteId {
                segment,
                slot,
                stream_id,
                sequence: *sequence,
            },
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

#[derive(Clone, Copy)]
struct PageWriteId {
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
    sequence: u64,
}

fn append_page_ops(
    ops: &mut Vec<RecordOp>,
    namespace: &Namespace,
    id: PageWriteId,
    page: Page,
    retention: PageRetention,
) -> Result<()> {
    let bytes = page.bytes();
    let min_timestamp_ns = page.blocks().first().unwrap().min_timestamp_ns;
    let max_timestamp_ns = page.blocks().last().unwrap().max_timestamp_ns;
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
    };
    ops.push(put(
        metadata_key(namespace, id.segment, id.slot, id.stream_id, page_id),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, id.segment, id.slot, id.stream_id, page_id),
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
            write_buffer: Default::default(),
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
    async fn discovers_stream_labels_and_series_across_segments() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("discovery").unwrap();
        db.write(
            &namespace,
            vec![
                LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "first")]),
                LogBatch::new(
                    labels("worker", "staging"),
                    vec![LogEntry::new(11_000_000_000, "second")],
                ),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            db.label_names(&namespace, 0, 12_000_000_000).await.unwrap(),
            vec!["environment", "service"]
        );
        assert_eq!(
            db.label_values(&namespace, "service", 0, 12_000_000_000)
                .await
                .unwrap(),
            vec!["api", "worker"]
        );
        assert_eq!(
            db.label_values(&namespace, "environment", 0, 9_000_000_000)
                .await
                .unwrap(),
            vec!["prod"]
        );

        let series = db
            .series(
                &namespace,
                &[r#"{service=~"api|worker",environment!="staging"}"#.to_owned()],
                0,
                12_000_000_000,
            )
            .await
            .unwrap();
        assert_eq!(series, vec![labels("api", "prod")]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn stream_ids_are_local_to_each_segment_slot() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("slot-local").unwrap();
        let first = labels("api", "prod");
        let first_slot = crate::routing::routing_slot(&namespace, &first);
        let second = (0..10_000)
            .map(|candidate| labels(&format!("worker-{candidate}"), "prod"))
            .find(|labels| crate::routing::routing_slot(&namespace, labels) != first_slot)
            .unwrap();
        let second_slot = crate::routing::routing_slot(&namespace, &second);

        db.write(
            &namespace,
            vec![
                LogBatch::new(first, vec![LogEntry::new(1, "first")]),
                LogBatch::new(second, vec![LogEntry::new(2, "second")]),
            ],
        )
        .await
        .unwrap();

        let ids = db.stream_ids(&namespace, 0, &[]).await.unwrap();
        assert!(ids.contains(&(first_slot, 0)));
        assert!(ids.contains(&(second_slot, 0)));
        assert_eq!(db.read(&namespace, 0, 3, &[]).await.unwrap().len(), 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn opened_slot_range_rejects_non_authoritative_writes() {
        let namespace = Namespace::new("owned-slots").unwrap();
        let labels = labels("api", "prod");
        let slot = crate::routing::routing_slot(&namespace, &labels);
        let owned = if slot == 0 { 1..2 } else { 0..1 };
        let db = LogDb::open_with_slots(test_config(), owned).await.unwrap();

        let error = db
            .write(
                &namespace,
                vec![LogBatch::new(labels, vec![LogEntry::new(1, "outside")])],
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("outside opened shard range"));
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
    async fn applied_writes_coalesce_before_flush() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("coalesced").unwrap();
        let labels = labels("api", "prod");
        let slot = crate::routing::routing_slot(&namespace, &labels);

        for (timestamp, line) in [(1, "first"), (2, "second")] {
            let report = db
                .write_with_durability(
                    &namespace,
                    vec![LogBatch::new(
                        labels.clone(),
                        vec![LogEntry::new(timestamp, line)],
                    )],
                    Durability::Applied,
                )
                .await
                .unwrap();
            assert_eq!(report.rows, 1);
            assert_eq!(report.streams, 1);
            assert_eq!(report.pages, 0);
        }

        assert!(db.read(&namespace, 0, 3, &[]).await.unwrap().is_empty());
        db.flush().await.unwrap();

        let rows = db.read(&namespace, 0, 3, &[]).await.unwrap();
        assert_eq!(rows.len(), 2);
        let (_, stream_id) = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
        let mut pages = db
            .storage
            .scan_iter(metadata_range(&namespace, 0, slot, stream_id))
            .await
            .unwrap();
        let mut page_count = 0;
        while pages.next().await.unwrap().is_some() {
            page_count += 1;
        }
        assert_eq!(page_count, 1);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn written_and_durable_force_the_expected_flushes() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "durability-levels".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });
        let namespace = Namespace::new("durability").unwrap();
        let db = LogDb::open(config.clone()).await.unwrap();

        db.write_with_durability(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(1, "applied")],
            )],
            Durability::Applied,
        )
        .await
        .unwrap();
        assert!(db.read(&namespace, 0, 3, &[]).await.unwrap().is_empty());

        db.write_with_durability(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(2, "written")],
            )],
            Durability::Written,
        )
        .await
        .unwrap();
        assert_eq!(db.read(&namespace, 0, 3, &[]).await.unwrap().len(), 2);

        db.write_with_durability(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(3, "durable")],
            )],
            Durability::Durable,
        )
        .await
        .unwrap();
        db.close().await.unwrap();

        let reopened = LogDb::open(config).await.unwrap();
        assert_eq!(reopened.read(&namespace, 0, 4, &[]).await.unwrap().len(), 3);
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn flush_drains_and_close_stops_the_coordinator() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("flush-close").unwrap();
        db.write_with_durability(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(1, "pending")],
            )],
            Durability::Applied,
        )
        .await
        .unwrap();

        db.flush().await.unwrap();
        assert_eq!(db.read(&namespace, 0, 2, &[]).await.unwrap().len(), 1);
        db.close().await.unwrap();

        let error = db
            .write(
                &namespace,
                vec![LogBatch::new(
                    labels("api", "prod"),
                    vec![LogEntry::new(2, "after close")],
                )],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("shut down"));
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
            .get(crate::codec::term_stats_key(
                &namespace,
                0,
                crate::routing::routing_slot(&namespace, &labels("search", "prod")),
                "needle",
            ))
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

        let (slot, stream_id) = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
        let mut metadata_records = db
            .storage
            .scan_iter(metadata_range(&namespace, 0, slot, stream_id))
            .await
            .unwrap();
        let record = metadata_records.next().await.unwrap().unwrap();
        let mut metadata = decode_metadata(&record.value).unwrap();
        metadata.expires_at_unix_ms = Some(0);
        db.writer
            .as_ref()
            .unwrap()
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
