// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use common::{
    BytesRange, StorageBuilder, StorageReaderRuntime, StorageSemantics, create_storage_read,
};
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
    field_stats_key, forward_key, forward_prefix, metadata_key, metadata_prefix,
    next_page_sequence_key, next_stream_id_key, payload_key, posting_key, segment_for,
    segment_prefix,
};
use crate::compaction::{Compactor, WrittenPage};
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
/// Streams whose forward labels and page metadata are read concurrently.
const STREAM_METADATA_CONCURRENCY: usize = 32;
/// Segments whose stream metadata is listed concurrently by `scan_targets`.
const SEGMENT_LIST_CONCURRENCY: usize = 4;
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
    written_at_unix_ms: u64,
}

/// Single-writer/single-node log database over the common SlateDB abstraction.
pub struct LogDb {
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    segment_ns: i64,
    write_handle: Option<WriteCoordinatorHandle<LineWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<LineWriteDelta, LineFlusher>>>,
    label_names_cache: DiscoveryCache<(Namespace, SegmentId), Vec<String>>,
    label_values_cache: DiscoveryCache<(Namespace, SegmentId, String), Vec<String>>,
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
        let storage_read = storage.clone();
        let direct_writer = Arc::new(DirectWriter {
            storage: storage_read.clone(),
            writer: storage.clone(),
            config: config.clone(),
        });
        let compactor = config.compaction.enabled.then(|| {
            Compactor::new(
                storage.clone(),
                config.compaction.clone(),
                config.page.clone(),
                segment_ns,
            )
        });
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            (),
            LineFlusher {
                direct_writer,
                storage: storage.clone(),
                compactor,
            },
        );
        let write_handle = write_coordinator.handle(WRITE_CHANNEL);
        write_coordinator.start();
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            segment_ns,
            write_handle: Some(write_handle),
            write_coordinator: Mutex::new(Some(write_coordinator)),
            label_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            label_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    /// Warms SlateDB caches for recent log segments in this shard.
    pub async fn warm_recent(
        &self,
        namespace: &Namespace,
        warm_range: Duration,
        include_payloads: bool,
        concurrency: usize,
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
            .warm_prefixes("line", &prefixes, include_payloads, concurrency, cancel)
            .await?;
        Ok(())
    }

    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
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
        let groups = group_by_stream(batches, self.segment_ns)?;
        if groups.is_empty() {
            return Ok(WriteReport::default());
        }
        self.label_names_cache.clear();
        self.label_values_cache.clear();
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
}

impl DirectWriter {
    /// Writes every group atomically and returns the pages it wrote.
    async fn write_groups(&self, groups: FrozenLineWriteDelta) -> Result<Vec<WrittenPage>> {
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
                let stream_ids = self.stream_ids(namespace, segment, &selector.exact).await?;
                for (_, labels) in self.stream_labels(namespace, segment, stream_ids).await? {
                    if selector.matches(&labels) {
                        result.insert(Arc::unwrap_or_clone(labels));
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
        let targets = self
            .scan_targets(
                namespace,
                start_ns,
                end_ns,
                &StreamFilter::exact(matchers.to_vec()),
            )
            .await?;
        self.read_bounded(namespace, targets, &PageBudget::new(usize::MAX))
            .await
    }

    /// Lists the selected streams and live overlapping pages of every segment
    /// in `[start_ns, end_ns]`, so the page estimate and the read share one
    /// metadata walk.
    pub(crate) async fn scan_targets(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
        filter: &StreamFilter,
    ) -> Result<ScanTargets> {
        let now_unix_ms = unix_time_ms()?;
        let segments = futures::stream::iter(self.segment_ids((start_ns, end_ns), false)?)
            .map(|segment| async move {
                let streams = self
                    .segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
                    .await?;
                Ok::<_, Error>((segment, streams))
            })
            .buffered(SEGMENT_LIST_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(ScanTargets {
            range: (start_ns, end_ns),
            segments,
        })
    }

    /// Segments covering `[start_ns, end_ns]` in scan order.
    fn segment_ids(&self, (start_ns, end_ns): (i64, i64), reverse: bool) -> Result<Vec<SegmentId>> {
        if end_ns < start_ns {
            return Err(Error::Invalid("end_ns must be >= start_ns".to_owned()));
        }
        let last_segment = segment_for(end_ns, self.segment_ns);
        let mut segment = segment_for(start_ns, self.segment_ns);
        let mut segments = vec![segment];
        while segment != last_segment {
            segment = segment
                .checked_add(self.segment_ns)
                .ok_or_else(|| Error::Invalid("query segment range overflow".to_owned()))?;
            segments.push(segment);
        }
        if reverse {
            segments.reverse();
        }
        Ok(segments)
    }

    /// Reads every target page, charging each to `budget`. This is the
    /// storage primitive used by the query engine to put a hard bound on I/O.
    pub(crate) async fn read_bounded(
        &self,
        namespace: &Namespace,
        targets: ScanTargets,
        budget: &PageBudget,
    ) -> Result<Vec<LogRow>> {
        let mut rows = Vec::new();
        let mut consume = |chunk: Vec<LogRow>| {
            rows.extend(chunk);
            Ok(ControlFlow::Continue(()))
        };
        for (segment, streams) in targets.segments {
            let _: ControlFlow<()> = self
                .read_segment_pages(
                    namespace,
                    segment,
                    streams,
                    targets.range,
                    budget,
                    false,
                    &mut consume,
                )
                .await?;
        }
        Ok(rows)
    }

    /// Reads overlapping pages in scan order, handing `consume` ascending
    /// chunks of rows. Every row in a later chunk is strictly later (or, with
    /// `reverse`, strictly earlier) than every row already consumed, so
    /// `consume` returns `Break` once no later row can matter. Pages are
    /// fetched in order of their nearest timestamp and a row is released only
    /// when no unread page can precede it, so a satisfied consumer stops
    /// mid-segment. Only pages actually fetched are charged to `budget`.
    pub(crate) async fn read_segments(
        &self,
        namespace: &Namespace,
        (start_ns, end_ns): (i64, i64),
        filter: &StreamFilter,
        budget: &PageBudget,
        reverse: bool,
        mut consume: impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let now_unix_ms = unix_time_ms()?;
        for segment in self.segment_ids((start_ns, end_ns), reverse)? {
            let streams = self
                .segment_streams(namespace, segment, filter, (start_ns, end_ns), now_unix_ms)
                .await?;
            let flow = self
                .read_segment_pages(
                    namespace,
                    segment,
                    streams,
                    (start_ns, end_ns),
                    budget,
                    reverse,
                    &mut consume,
                )
                .await?;
            if flow.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// One segment of [`Self::read_segments`].
    #[allow(clippy::too_many_arguments)]
    async fn read_segment_pages(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        streams: Vec<SegmentStream>,
        (start_ns, end_ns): (i64, i64),
        budget: &PageBudget,
        reverse: bool,
        consume: &mut impl FnMut(Vec<LogRow>) -> Result<ControlFlow<()>>,
    ) -> Result<ControlFlow<()>> {
        let mut pages = Vec::new();
        for stream in streams {
            let fingerprint = stream.labels.fingerprint();
            for (page_id, metadata) in stream.pages {
                let bound = if reverse {
                    metadata.max_timestamp_ns
                } else {
                    metadata.min_timestamp_ns
                };
                pages.push((
                    bound,
                    stream.stream_id,
                    stream.labels.clone(),
                    fingerprint,
                    page_id,
                    metadata.level,
                ));
            }
        }
        pages.sort_unstable_by(|a, b| {
            let bound = if reverse {
                b.0.cmp(&a.0)
            } else {
                a.0.cmp(&b.0)
            };
            bound
                .then(a.1.cmp(&b.1))
                .then(a.4.sequence.cmp(&b.4.sequence))
        });
        let bounds = pages.iter().map(|page| page.0).collect::<Vec<_>>();
        let mut decoded = futures::stream::iter(pages)
            .map(
                move |(_, stream_id, labels, fingerprint, page_id, level)| async move {
                    budget.take()?;
                    let payload = self
                        .storage
                        .get(payload_key(namespace, segment, stream_id, page_id, level))
                        .await?
                        .ok_or_else(|| Error::Corrupt("page metadata has no payload".to_owned()))?;
                    let entries = Page::decode(payload.value)?.decode_range(start_ns, end_ns)?;
                    Ok::<_, Error>((labels, fingerprint, page_id, entries))
                },
            )
            .buffered(PAGE_READ_CONCURRENCY);
        let mut pending = PendingRows::new(reverse);
        let mut fetched = 0;
        while let Some((labels, fingerprint, page_id, entries)) = decoded.try_next().await? {
            fetched += 1;
            for (row_index, entry) in entries.into_iter().enumerate() {
                pending.push(
                    (entry.timestamp_ns, fingerprint, page_id.sequence, row_index),
                    LogRow {
                        labels: labels.clone(),
                        entry,
                    },
                );
            }
            let ready = pending.release(bounds.get(fetched).copied());
            if !ready.is_empty() && consume(ready)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Uses the segment-local term index to identify pages and rows before
    /// payload reads. `None` means at least one segment predates the index and
    /// the caller must use the exact scan fallback.
    pub(crate) async fn read_match_bounded(
        &self,
        namespace: &Namespace,
        targets: &ScanTargets,
        match_plan: (&[String], Option<usize>),
        max_pages: usize,
    ) -> Result<Option<Vec<(LogRow, f32)>>> {
        let (terms, top_k) = match_plan;
        let (start_ns, end_ns) = targets.range;
        let mut rows = Vec::new();
        let mut pages_read = 0usize;

        for (segment, streams) in &targets.segments {
            let segment = *segment;
            let mut candidate_pages = Vec::new();
            // Postings address written pages; a merged page covers several.
            let mut leaves: HashMap<(StreamId, u64), (u64, u32)> = HashMap::new();
            for stream in streams {
                for (page_id, metadata) in &stream.pages {
                    for (leaf, first_row) in metadata.leaves(page_id.sequence) {
                        leaves.insert((stream.stream_id, leaf), (page_id.sequence, first_row));
                    }
                    candidate_pages.push((
                        stream.stream_id,
                        stream.labels.clone(),
                        *page_id,
                        metadata.level,
                    ));
                }
            }
            let allowed_pages = leaves.keys().copied().collect::<HashSet<_>>();
            let mut by_page: HashMap<(StreamId, u64), HashMap<u32, f32>> = HashMap::new();
            if !allowed_pages.is_empty() {
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
                for (address, score) in scores {
                    let (sequence, first_row) = leaves[&(address.stream_id, address.page_sequence)];
                    by_page
                        .entry((address.stream_id, sequence))
                        .or_default()
                        .insert(first_row.saturating_add(address.row_id), score);
                }
            }

            for (stream_id, labels, page_id, level) in candidate_pages {
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
                    .get(payload_key(namespace, segment, stream_id, page_id, level))
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
    ) -> Result<Vec<StreamId>> {
        if matchers.is_empty() {
            let mut result = Vec::new();
            let mut iterator = self
                .storage
                .scan_prefix_iter(
                    forward_prefix(namespace, segment),
                    BytesRange::unbounded(),
                    None,
                )
                .await?;
            while let Some(record) = iterator.next().await? {
                result.push(decode_forward_key(&record.key)?);
            }
            return Ok(result);
        }

        let records = futures::future::try_join_all(
            matchers
                .iter()
                .map(|matcher| self.storage.get(posting_key(namespace, segment, matcher))),
        )
        .await?;
        let mut result: Option<RoaringBitmap> = None;
        for record in records {
            let Some(record) = record else {
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

    /// Fetches forward labels for `stream_ids` concurrently, in input order.
    async fn stream_labels(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        stream_ids: Vec<StreamId>,
    ) -> Result<Vec<(StreamId, Arc<Labels>)>> {
        futures::stream::iter(stream_ids)
            .map(|stream_id| async move {
                let record = self
                    .storage
                    .get(forward_key(namespace, segment, stream_id))
                    .await?
                    .ok_or_else(|| {
                        Error::Corrupt("posting references missing forward labels".to_owned())
                    })?;
                Ok::<_, Error>((stream_id, Arc::new(decode_labels(&record.value)?)))
            })
            .buffered(STREAM_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Streams in `segment` selected by `filter`, each with its live pages
    /// overlapping `[start_ns, end_ns]`. Labels are checked before any page
    /// metadata is scanned, so non-exact matchers prune streams without I/O.
    async fn segment_streams(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        filter: &StreamFilter,
        (start_ns, end_ns): (i64, i64),
        now_unix_ms: u64,
    ) -> Result<Vec<SegmentStream>> {
        let stream_ids = self.stream_ids(namespace, segment, &filter.exact).await?;
        let labelled = self.stream_labels(namespace, segment, stream_ids).await?;
        futures::stream::iter(
            labelled
                .into_iter()
                .filter(|(_, labels)| filter.matches(labels)),
        )
        .map(|(stream_id, labels)| async move {
            let mut pages = Vec::new();
            let mut metadata = self
                .storage
                .scan_prefix_iter(
                    metadata_prefix(namespace, segment, stream_id),
                    BytesRange::unbounded(),
                    None,
                )
                .await?;
            while let Some(record) = metadata.next().await? {
                let (_, page_id) = decode_metadata_key(&record.key)?;
                let page = decode_metadata(&record.value)?;
                if page.is_expired_at(now_unix_ms)
                    || page.max_timestamp_ns < start_ns
                    || page.min_timestamp_ns > end_ns
                {
                    continue;
                }
                pages.push((page_id, page));
            }
            Ok::<_, Error>(SegmentStream {
                stream_id,
                labels,
                pages,
            })
        })
        .buffered(STREAM_METADATA_CONCURRENCY)
        .try_collect()
        .await
    }
}

struct SegmentStream {
    stream_id: StreamId,
    labels: Arc<Labels>,
    pages: Vec<(PageId, StoredPageMetadata)>,
}

/// Output of [`LogDb::scan_targets`].
pub(crate) struct ScanTargets {
    range: (i64, i64),
    segments: Vec<(SegmentId, Vec<SegmentStream>)>,
}

impl ScanTargets {
    pub(crate) fn estimate(&self) -> QueryEstimate {
        let mut estimate = QueryEstimate::default();
        for page in self
            .segments
            .iter()
            .flat_map(|(_, streams)| streams)
            .flat_map(|stream| &stream.pages)
            .map(|(_, page)| page)
        {
            estimate.pages = estimate.pages.saturating_add(1);
            estimate.compressed_bytes = estimate
                .compressed_bytes
                .saturating_add(u64::from(page.payload_bytes));
            estimate.lines = estimate.lines.saturating_add(u64::from(page.row_count));
        }
        estimate
    }
}

type RowKey = (i64, StreamFingerprint, u64, usize);

/// Decoded rows waiting until no unread page can precede them.
struct PendingRows {
    heap: BinaryHeap<PendingRow>,
    reverse: bool,
}

struct PendingRow {
    key: RowKey,
    reverse: bool,
    row: LogRow,
}

impl PartialEq for PendingRow {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PendingRow {}

impl PartialOrd for PendingRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingRow {
    /// `BinaryHeap` pops the greatest row, which must be the next in scan order.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let order = self.key.cmp(&other.key);
        if self.reverse { order } else { order.reverse() }
    }
}

impl PendingRows {
    fn new(reverse: bool) -> Self {
        Self {
            heap: BinaryHeap::new(),
            reverse,
        }
    }

    fn push(&mut self, key: RowKey, row: LogRow) {
        self.heap.push(PendingRow {
            key,
            reverse: self.reverse,
            row,
        });
    }

    /// Pops rows strictly before `next_bound` (the nearest timestamp any
    /// unread page may hold) in ascending order; `None` releases everything.
    fn release(&mut self, next_bound: Option<i64>) -> Vec<LogRow> {
        let mut ready = Vec::new();
        while let Some(top) = self.heap.peek() {
            let timestamp = top.key.0;
            let safe = match next_bound {
                None => true,
                Some(bound) if self.reverse => timestamp > bound,
                Some(bound) => timestamp < bound,
            };
            if !safe {
                break;
            }
            ready.push(self.heap.pop().expect("peeked row").row);
        }
        if self.reverse {
            ready.reverse();
        }
        ready
    }
}

/// Stream selection for a query: postings intersect `exact`, then a stream is
/// kept when any selector matches its labels (or when there are none).
pub(crate) struct StreamFilter {
    exact: Vec<Label>,
    selectors: Vec<SeriesSelector>,
}

impl StreamFilter {
    pub(crate) fn exact(exact: Vec<Label>) -> Self {
        Self {
            exact,
            selectors: Vec::new(),
        }
    }

    pub(crate) fn new<'a>(
        exact: Vec<Label>,
        selectors: impl IntoIterator<Item = &'a [crate::logql::Spanned<crate::logql::Matcher>]>,
    ) -> Result<Self> {
        Ok(Self {
            exact,
            selectors: selectors
                .into_iter()
                .map(SeriesSelector::from_matchers)
                .collect::<Result<_>>()?,
        })
    }

    fn matches(&self, labels: &Labels) -> bool {
        self.selectors.is_empty()
            || self
                .selectors
                .iter()
                .any(|selector| selector.matches(labels))
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
        use crate::logql::Expr;

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
        Self::from_matchers(&log.selector.value.matchers)
    }

    fn from_matchers(source: &[crate::logql::Spanned<crate::logql::Matcher>]) -> Result<Self> {
        use crate::logql::MatchOp;

        let mut exact = Vec::new();
        let mut matchers = Vec::with_capacity(source.len());
        for matcher in source {
            let name = matcher.value.label.clone();
            let value = matcher.value.value.clone();
            match matcher.value.op {
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

type StreamGroups = BTreeMap<(SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;
type CoordinatedStreamGroups =
    BTreeMap<(Namespace, SegmentId, StreamFingerprint), (Labels, Vec<LogEntry>)>;

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
    compactor: Option<Compactor>,
}

#[async_trait]
impl Flusher<LineWriteDelta> for LineFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenLineWriteDelta,
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

/// Groups entries by `(segment, stream)`, sorted by timestamp.
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
        level: 0,
        written_at_unix_ms: retention.written_at_unix_ms,
        leaf_rows: Vec::new(),
    };
    ops.push(put(
        metadata_key(namespace, id.segment, id.stream_id, page_id),
        encode_metadata(&metadata)?,
        retention.physical_ttl,
    ));
    ops.push(put(
        payload_key(namespace, id.segment, id.stream_id, page_id, 0),
        bytes,
        retention.physical_ttl,
    ));
    Ok((page_id, metadata))
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
    use crate::config::{CompactionConfig, PageConfig};

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
                rows_per_block: 1,
            },
            compaction: CompactionConfig {
                enabled: false,
                ..CompactionConfig::default()
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
    async fn streams_in_a_segment_share_one_id_space() {
        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("segment-ids").unwrap();

        db.write(
            &namespace,
            vec![
                LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "first")]),
                LogBatch::new(labels("worker", "prod"), vec![LogEntry::new(2, "second")]),
            ],
        )
        .await
        .unwrap();

        let mut ids = db.stream_ids(&namespace, 0, &[]).await.unwrap();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1]);
        assert_eq!(db.read(&namespace, 0, 3, &[]).await.unwrap().len(), 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn limited_log_queries_stop_within_a_segment() {
        use crate::{Direction, QueryOptions, QueryRequest, QueryResult};

        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("early-stop").unwrap();
        let entries = (1..=10)
            .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
            .collect();
        db.write(
            &namespace,
            vec![LogBatch::new(labels("api", "prod"), entries)],
        )
        .await
        .unwrap();
        let first = |direction| {
            let db = &db;
            let namespace = &namespace;
            async move {
                let result = db
                    .query(
                        namespace,
                        &QueryRequest::range(r#"{service="api"}"#, 0, 11, 1),
                        QueryOptions {
                            limit: 1,
                            max_pages: 2,
                            direction,
                            ..QueryOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                let QueryResult::Streams(streams) = result else {
                    panic!("expected streams");
                };
                streams[0].entries[0].timestamp_ns
            }
        };

        // Five two-row pages share one segment; one page answers each query.
        assert_eq!(first(Direction::Forward).await, 1);
        assert_eq!(first(Direction::Backward).await, 10);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn regex_and_negative_matchers_prune_streams_before_page_reads() {
        use crate::{QueryOptions, QueryRequest};

        let db = LogDb::open(test_config()).await.unwrap();
        let namespace = Namespace::new("prune").unwrap();
        let worker = (1..=10)
            .map(|timestamp| LogEntry::new(timestamp, "worker"))
            .collect();
        db.write(
            &namespace,
            vec![
                LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "api")]),
                LogBatch::new(labels("worker", "prod"), worker),
            ],
        )
        .await
        .unwrap();

        for query in [
            r#"count_over_time({service=~"a.i"}[10s])"#,
            r#"count_over_time({environment="prod", service!="worker"}[10s])"#,
            r#"count_over_time({environment="prod", service!~"work.*"}[10s])"#,
        ] {
            db.query(
                &namespace,
                &QueryRequest::instant(query, 10),
                QueryOptions {
                    max_pages: 1,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        }
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
        let stream_id = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
        let mut pages = db
            .storage
            .scan_prefix_iter(
                metadata_prefix(&namespace, 0, stream_id),
                BytesRange::unbounded(),
                None,
            )
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
                .read_match_bounded(
                    &namespace,
                    &db.scan_targets(&namespace, 0, 10, &StreamFilter::exact(Vec::new()))
                        .await
                        .unwrap(),
                    (&terms, top_k),
                    10,
                )
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
            db.scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
                .await
                .unwrap()
                .estimate(),
            QueryEstimate::default()
        );
        let terms = vec!["needle".to_owned()];
        assert_eq!(
            db.read_match_bounded(
                &namespace,
                &db.scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
                    .await
                    .unwrap(),
                (&terms, None),
                10
            )
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
            .scan_prefix_iter(
                metadata_prefix(&namespace, 0, stream_id),
                BytesRange::unbounded(),
                None,
            )
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
                .read_match_bounded(
                    &namespace,
                    &reopened
                        .scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
                        .await
                        .unwrap(),
                    (&terms, None),
                    10
                )
                .await
                .unwrap(),
            Some(Vec::new())
        );
        reopened.close().await.unwrap();
    }

    const SEGMENT_NS: i64 = 10_000_000_000;

    fn compacting_config() -> Config {
        Config {
            page: PageConfig {
                target_size_bytes: 4096,
                max_rows: 64,
                rows_per_block: 4,
            },
            compaction: CompactionConfig {
                enabled: true,
                fan_in: 2,
                min_age: Duration::ZERO,
                finalize_after: Duration::from_secs(3600),
                delete_delay: Duration::ZERO,
                max_merges_per_flush: 256,
            },
            ..test_config()
        }
    }

    fn local_storage(directory: &tempfile::TempDir, path: &str) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: path.to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        })
    }

    /// Start of the segment containing now, so it is still open.
    fn current_segment() -> SegmentId {
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap();
        crate::codec::segment_for(now_ns, SEGMENT_NS)
    }

    async fn count_records(
        db: &LogDb,
        namespace: &Namespace,
        segment: SegmentId,
        record_type: crate::codec::RecordType,
    ) -> usize {
        let mut records = db
            .storage
            .scan_prefix_iter(
                crate::codec::record_type_prefix(namespace, segment, record_type),
                BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        let mut count = 0;
        while records.next().await.unwrap().is_some() {
            count += 1;
        }
        count
    }

    async fn write_line(
        db: &LogDb,
        namespace: &Namespace,
        service: &str,
        timestamp: i64,
        line: &str,
    ) {
        db.write(
            namespace,
            vec![LogBatch::new(
                labels(service, "prod"),
                vec![LogEntry::new(timestamp, line)],
            )],
        )
        .await
        .unwrap();
    }

    async fn lines(db: &LogDb, namespace: &Namespace, start: i64, end: i64) -> Vec<String> {
        db.read(namespace, start, end, &[Label::new("service", "api")])
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.entry.line)
            .collect()
    }

    async fn match_lines(
        db: &LogDb,
        namespace: &Namespace,
        start: i64,
        end: i64,
        max_pages: usize,
    ) -> Result<Vec<String>> {
        let terms = vec!["needle".to_owned()];
        let targets = db
            .scan_targets(
                namespace,
                start,
                end,
                &StreamFilter::exact(vec![Label::new("service", "api")]),
            )
            .await?;
        let mut rows = db
            .read_match_bounded(namespace, &targets, (&terms, None), max_pages)
            .await?
            .expect("indexed match");
        rows.sort_by_key(|(row, _)| row.entry.timestamp_ns);
        Ok(rows.into_iter().map(|(row, _)| row.entry.line).collect())
    }

    #[tokio::test]
    async fn open_segments_merge_equal_level_pages_and_delete_replaced_payloads() {
        use crate::codec::RecordType;

        let db = LogDb::open(compacting_config()).await.unwrap();
        let namespace = Namespace::new("compaction").unwrap();
        let segment = current_segment();
        // Two writes share a timestamp; their order must survive merging.
        let timestamps = [1, 2, 3, 3, 5, 6, 7, 8].map(|offset| segment + offset);
        let expected = (0..timestamps.len())
            .map(|index| format!("needle {index}"))
            .collect::<Vec<_>>();
        for (timestamp, line) in timestamps.iter().zip(&expected) {
            write_line(&db, &namespace, "api", *timestamp, line).await;
        }

        // Eight level-0 pages fold into one level-3 page within the flushes.
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PageMetadata).await,
            1
        );
        let end = segment + 100;
        assert_eq!(lines(&db, &namespace, segment, end).await, expected);
        assert_eq!(
            match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
            expected
        );
        // Replaced payloads stay readable until a later flush deletes them.
        assert!(count_records(&db, &namespace, segment, RecordType::PageTombstone).await > 0);

        write_line(&db, &namespace, "worker", segment + 50, "other stream").await;
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
            0
        );
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PagePayload).await,
            2
        );
        assert_eq!(lines(&db, &namespace, segment, end).await, expected);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn settled_segments_merge_below_fan_in() {
        use crate::codec::RecordType;

        let mut config = compacting_config();
        config.compaction.fan_in = 4;
        config.compaction.min_age = Duration::from_secs(3600);
        config.compaction.finalize_after = Duration::ZERO;
        let db = LogDb::open(config).await.unwrap();
        let namespace = Namespace::new("settled").unwrap();
        let expected = (1..=6)
            .map(|index| format!("needle {index}"))
            .collect::<Vec<_>>();
        for (timestamp, line) in (1..).zip(&expected) {
            write_line(&db, &namespace, "api", timestamp, line).await;
        }

        let pages = count_records(&db, &namespace, 0, RecordType::PageMetadata).await;
        assert!(pages <= 3, "six late writes left {pages} pages");
        assert_eq!(lines(&db, &namespace, 0, 10).await, expected);
        assert_eq!(
            match_lines(&db, &namespace, 0, 10, 3).await.unwrap(),
            expected
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn reopened_writers_rebuild_the_index_and_pending_deletes() {
        use crate::codec::RecordType;

        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            storage: local_storage(&directory, "compaction-reopen"),
            ..compacting_config()
        };
        let namespace = Namespace::new("reopen").unwrap();
        let segment = current_segment();
        let expected = (1..=4)
            .map(|index| format!("needle {index}"))
            .collect::<Vec<_>>();

        let db = LogDb::open(config.clone()).await.unwrap();
        for (offset, line) in (1..).zip(&expected[..2]) {
            write_line(&db, &namespace, "api", segment + offset, line).await;
        }
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
            2
        );
        db.close().await.unwrap();

        let db = LogDb::open(config).await.unwrap();
        write_line(&db, &namespace, "api", segment + 3, &expected[2]).await;
        // Recovery re-queued the tombstones, and the merged page is tracked.
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
            0
        );
        write_line(&db, &namespace, "api", segment + 4, &expected[3]).await;
        assert_eq!(
            count_records(&db, &namespace, segment, RecordType::PageMetadata).await,
            1
        );
        let end = segment + 100;
        assert_eq!(lines(&db, &namespace, segment, end).await, expected);
        assert_eq!(
            match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
            expected
        );
        db.close().await.unwrap();
    }
}
