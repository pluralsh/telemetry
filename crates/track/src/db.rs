// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{
    Delta, Durability as CoordinatorDurability, Flusher, WriteCoordinator, WriteCoordinatorHandle,
    WriteError,
};
use common::discovery::{self, CatalogBatch, DiscoveryCache, DiscoveryValue};
use common::storage::{RecordOp, Storage, StorageRead, StorageSnapshot, Ttl};
use common::{
    BytesRange, StorageBuilder, StorageReaderRuntime, StorageSemantics, create_storage_read,
};
use futures::{StreamExt, TryStreamExt, stream};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use slatedb::config::DbReaderOptions;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::codec::{
    LOCATOR_SEGMENT, PageRef, PageTrace, StoredPageMetadata, TraceLocator, decode_indices,
    decode_locator, decode_locator_trace_id, decode_metadata, decode_posting_sequence,
    decode_sequence, encode_indices, encode_locator, encode_metadata, encode_sequence, locator_key,
    locator_namespace_prefix, locator_prefix, metadata_key, metadata_prefix, next_sequence_key,
    payload_key, posting_key, posting_scan_prefix, segment_for, segment_prefix,
};

/// Concurrent storage reads per query stage.
const READ_CONCURRENCY: usize = 32;
/// Traces materialized per batch, bounding how many pages are held at once.
const MATERIALIZE_BATCH: usize = 256;
/// Segments whose metadata or postings are scanned concurrently.
const SEGMENT_SCAN_CONCURRENCY: usize = 8;
use crate::traceql::PushdownClause;
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Config, Error, Namespace, Page, PageBuilder,
    PageConfig, QueryOptions, Result, SegmentId, Trace, TraceBatch, TraceId, TraceQlResult,
};

mod query;
mod write;

pub(crate) use query::{execute_traceql, merge_traces};
use write::*;

const WRITE_CHANNEL: &str = "write";
const TRACK_FLUSH_PAGES: &str = "track_flush_pages";
const PARTITION_SCOPE: &str = "partition";
const PARTITION_NAME: &str = "segment";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteReport {
    /// Input traces accepted from this request.
    pub traces: usize,
    /// Reserved for API compatibility. Pages are formed across requests by
    /// the flusher and reported through `track_flush_pages`.
    pub pages: usize,
    /// Input spans accepted from this request.
    pub spans: usize,
}

#[derive(Clone, Copy)]
struct Retention {
    physical_ttl: Ttl,
    expires_at_unix_ms: Option<u64>,
}

/// Single-node OTLP trace database over the common SlateDB abstraction.
pub struct TraceDb {
    storage: Arc<dyn StorageRead>,
    writer: Option<Arc<dyn Storage>>,
    write_handle: Option<WriteCoordinatorHandle<TraceWriteDelta>>,
    write_coordinator: Mutex<Option<WriteCoordinator<TraceWriteDelta, TraceFlusher>>>,
    segment_ns: u64,
    catalog_names_cache:
        DiscoveryCache<(Namespace, SegmentId, Option<AttributeScope>), Vec<String>>,
    catalog_values_cache:
        DiscoveryCache<(Namespace, SegmentId, Option<AttributeScope>, String), Vec<DiscoveryValue>>,
}

impl TraceDb {
    /// Warms SlateDB caches for recent trace segments in this shard.
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
        let end_ns = common::time::now_ns().unsigned_abs();
        let range_ns = u64::try_from(warm_range.as_nanos()).unwrap_or(u64::MAX);
        let mut prefixes = self
            .catalog_segments(namespace, end_ns.saturating_sub(range_ns), end_ns)
            .await?
            .into_iter()
            .map(|segment| segment_prefix(namespace, segment))
            .collect::<Vec<_>>();
        prefixes.push(segment_prefix(namespace, LOCATOR_SEGMENT));
        slate
            .warm_prefixes("track", &prefixes, include_payloads, concurrency, cancel)
            .await?;
        Ok(())
    }

    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
        let semantics = StorageSemantics::new()
            .with_segment_extractor(crate::codec::SEGMENT_EXTRACTOR.shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        let storage_read = storage.clone();
        let initial_snapshot = storage.snapshot().await?;
        let flusher = TraceFlusher {
            storage: storage.clone(),
            page_config: config.page.clone(),
            retention: config.retention,
        };
        let mut write_coordinator = WriteCoordinator::new(
            config.write_buffer.clone(),
            vec![WRITE_CHANNEL],
            (),
            initial_snapshot,
            flusher,
        );
        let write_handle = write_coordinator.handle(WRITE_CHANNEL);
        write_coordinator.start();
        Ok(Self {
            storage: storage_read,
            writer: Some(storage),
            write_handle: Some(write_handle),
            write_coordinator: Mutex::new(Some(write_coordinator)),
            segment_ns,
            catalog_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            catalog_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    pub(crate) async fn open_reader(
        config: Config,
        reader_options: DbReaderOptions,
    ) -> Result<Self> {
        config.validate()?;
        let segment_ns = u64::try_from(config.segment_duration.as_nanos())
            .map_err(|_| Error::Invalid("segment duration exceeds u64 nanoseconds".to_owned()))?;
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
            write_handle: None,
            write_coordinator: Mutex::new(None),
            segment_ns,
            catalog_names_cache: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            catalog_values_cache: DiscoveryCache::new(4_096, Duration::from_secs(5)),
        })
    }

    fn write_handle(&self) -> Result<&WriteCoordinatorHandle<TraceWriteDelta>> {
        self.write_handle
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
    }

    pub async fn write(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
    ) -> Result<WriteReport> {
        self.write_with_durability(namespace, batches, Durability::Written)
            .await
    }

    /// Atomically publishes page metadata, payload, locator fragments, and
    /// immutable attribute posting fragments.
    pub async fn write_with_durability(
        &self,
        namespace: &Namespace,
        batches: Vec<TraceBatch>,
        durability: Durability,
    ) -> Result<WriteReport> {
        let mut groups: BTreeMap<SegmentId, Vec<Trace>> = BTreeMap::new();
        let mut report = WriteReport::default();
        for batch in batches {
            for trace in batch.traces {
                let (min_timestamp_ns, _) = trace.timestamp_range();
                let segment = segment_for(min_timestamp_ns, self.segment_ns);
                report.traces += 1;
                report.spans += trace.spans().count();
                groups.entry(segment).or_default().push(trace);
            }
        }
        if groups.is_empty() {
            return Ok(report);
        }
        for traces in groups.values_mut() {
            traces.sort_by_key(|trace| (trace.timestamp_range().0, trace.trace_id));
        }
        self.catalog_names_cache.clear();
        self.catalog_values_cache.clear();
        let write = TraceWrite {
            namespace: namespace.clone(),
            groups,
            report,
        };
        let handle = self.write_handle()?;
        let mut write_handle = handle
            .try_write(write)
            .await
            .map_err(|error| map_write_error(error.discard_inner()))?;
        let report = write_handle
            .wait(CoordinatorDurability::Applied)
            .await
            .map_err(map_write_error)?;
        if durability != Durability::Applied {
            let flush_storage = durability == Durability::Durable;
            let mut flush_handle = handle.flush(flush_storage).await.map_err(map_write_error)?;
            flush_handle
                .wait(if flush_storage {
                    CoordinatorDurability::Durable
                } else {
                    CoordinatorDurability::Written
                })
                .await
                .map_err(map_write_error)?;
        }
        Ok(report)
    }
}

impl TraceDb {
    pub async fn flush(&self) -> Result<()> {
        if let Some(handle) = &self.write_handle {
            let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;
            flush_handle
                .wait(CoordinatorDurability::Written)
                .await
                .map_err(map_write_error)?;
        }
        if let Some(writer) = &self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(coordinator) = self.write_coordinator.lock().await.take() {
            coordinator
                .stop()
                .await
                .map_err(|error| Error::Invalid(format!("write coordinator stopped: {error}")))?;
        }
        self.storage.close().await?;
        Ok(())
    }
}

fn catalog_scope(scope: AttributeScope) -> &'static str {
    match scope {
        AttributeScope::Resource => "resource",
        AttributeScope::Span => "span",
    }
}

fn unix_time_ms() -> Result<u64> {
    common::time::checked_now_ms().map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests;
