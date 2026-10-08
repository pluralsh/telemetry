// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Cross-query caches of decoded read-path records.
//!
//! A page's metadata, payload and attribute postings are written once, in
//! the batch that also advances its segment's next page sequence, and never
//! change. Metadata and decoded pages are cached by page. A segment's
//! postings under one key prefix are cached with the page sequence they
//! cover and revalidated against the segment's next sequence, so a query
//! sees every page published before it read that sequence. A trace's
//! locators change with each page it gains, so they are cached with its
//! head's page count, or, in the writing process, invalidated by writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use moka::future::Cache;

use crate::codec::{StoredPageMetadata, TraceLocator};
use crate::{AttributeValue, Namespace, Page, SegmentId, TraceId};

/// Byte budgets of a database's read caches; zero disables one. Each storage
/// shard holds its own caches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadCacheConfig {
    /// Decoded page metadata, which resolves postings to candidates.
    pub metadata_bytes: u64,
    /// Decoded attribute postings, per segment and indexed field or value.
    pub postings_bytes: u64,
    /// Decoded page payloads.
    pub pages_bytes: u64,
    /// The pages of each trace, which loading it needs.
    pub locators_bytes: u64,
}

impl Default for ReadCacheConfig {
    fn default() -> Self {
        Self {
            metadata_bytes: 16 << 20,
            postings_bytes: 32 << 20,
            pages_bytes: 64 << 20,
            locators_bytes: 8 << 20,
        }
    }
}

type PageKey = (Namespace, SegmentId, u64);

/// Every page of a trace, read when its head counted `pages` pages.
pub(crate) struct TraceLocators {
    pub(crate) pages: u32,
    pub(crate) locators: Vec<TraceLocator>,
}

/// One posting record: the traces of one page holding one value.
#[derive(Clone)]
pub(crate) struct PostingEntry {
    /// The record's value, for prefixes spanning every value of a field.
    pub(crate) value: Option<AttributeValue>,
    pub(crate) sequence: u64,
    pub(crate) indices: Box<[u32]>,
}

/// The posting records under one key prefix of pages below `through`, in
/// key order.
#[derive(Clone)]
pub(crate) struct SegmentPostings {
    pub(crate) through: u64,
    pub(crate) entries: Vec<PostingEntry>,
}

pub(crate) struct ReadCache {
    metadata: Option<Cache<PageKey, Arc<StoredPageMetadata>>>,
    pages: Option<Cache<PageKey, Arc<Page>>>,
    /// Keyed by the posting prefix, which encodes namespace and segment, and
    /// for bounded scans the value range scanned.
    postings: Option<Cache<Bytes, Arc<SegmentPostings>>>,
    locators: Option<Cache<(Namespace, TraceId), Arc<TraceLocators>>>,
    /// Whether this process is the database's only writer and invalidates
    /// the locators of every trace it writes, so cached locators are current
    /// without reading the trace's head.
    local_writer: bool,
    /// Advanced by every locator invalidation, so a read that raced one
    /// drops what it cached.
    locator_epoch: AtomicU64,
}

fn weight(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

impl ReadCache {
    pub(crate) fn new(config: &ReadCacheConfig, local_writer: bool) -> Self {
        let build = |capacity: u64| (capacity > 0).then_some(capacity);
        Self {
            metadata: build(config.metadata_bytes).map(|capacity| {
                Cache::builder()
                    .max_capacity(capacity)
                    .weigher(|_, metadata: &Arc<StoredPageMetadata>| {
                        weight(
                            64 + metadata.traces.len()
                                * std::mem::size_of::<crate::codec::PageTrace>(),
                        )
                    })
                    .build()
            }),
            pages: build(config.pages_bytes).map(|capacity| {
                Cache::builder()
                    .max_capacity(capacity)
                    .weigher(|_, page: &Arc<Page>| {
                        weight(128 + page.bytes().len() + page.directory().len() * 64)
                    })
                    .build()
            }),
            postings: build(config.postings_bytes).map(|capacity| {
                Cache::builder()
                    .max_capacity(capacity)
                    .weigher(|prefix: &Bytes, postings: &Arc<SegmentPostings>| {
                        let entries: usize = postings
                            .entries
                            .iter()
                            .map(|entry| {
                                std::mem::size_of::<PostingEntry>()
                                    + entry.indices.len() * 4
                                    + match &entry.value {
                                        Some(AttributeValue::String(value)) => value.len(),
                                        _ => 0,
                                    }
                            })
                            .sum();
                        weight(64 + prefix.len() + entries)
                    })
                    .build()
            }),
            locators: build(config.locators_bytes).map(|capacity| {
                Cache::builder()
                    .max_capacity(capacity)
                    .weigher(|_, locators: &Arc<TraceLocators>| {
                        weight(96 + locators.locators.len() * std::mem::size_of::<TraceLocator>())
                    })
                    .build()
            }),
            local_writer,
            locator_epoch: AtomicU64::new(0),
        }
    }

    /// `trace_id`'s pages, if cached and known current without its head.
    pub(crate) async fn current_locators(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Option<Arc<TraceLocators>> {
        if !self.local_writer {
            return None;
        }
        self.locators
            .as_ref()?
            .get(&(namespace.clone(), trace_id))
            .await
    }

    /// `trace_id`'s pages, if cached when its head counted `pages`.
    pub(crate) async fn locators(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        pages: u32,
    ) -> Option<Arc<TraceLocators>> {
        let cached = self
            .locators
            .as_ref()?
            .get(&(namespace.clone(), trace_id))
            .await?;
        (self.local_writer || cached.pages == pages).then_some(cached)
    }

    /// The epoch to pass to [`Self::insert_locators`], read before the
    /// records it caches.
    pub(crate) fn locator_epoch(&self) -> u64 {
        self.locator_epoch.load(Ordering::SeqCst)
    }

    /// Caches locators read at `epoch`.
    pub(crate) async fn insert_locators(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
        locators: Arc<TraceLocators>,
        epoch: u64,
    ) {
        let Some(cache) = &self.locators else {
            return;
        };
        let key = (namespace.clone(), trace_id);
        cache.insert(key.clone(), locators).await;
        if self.locator_epoch.load(Ordering::SeqCst) != epoch {
            cache.invalidate(&key).await;
        }
    }

    /// Drops the locators of every trace a flush wrote, after it is written.
    /// The flush did not read which of them were already cached.
    pub(crate) async fn invalidate_traces(&self, traces: Vec<(Namespace, TraceId)>) {
        let Some(cache) = &self.locators else {
            return;
        };
        self.locator_epoch.fetch_add(1, Ordering::SeqCst);
        for key in traces {
            cache.invalidate(&key).await;
        }
    }

    pub(crate) async fn metadata(
        &self,
        namespace: &Namespace,
        (segment, sequence): (SegmentId, u64),
    ) -> Option<Arc<StoredPageMetadata>> {
        self.metadata
            .as_ref()?
            .get(&(namespace.clone(), segment, sequence))
            .await
    }

    pub(crate) async fn insert_metadata(
        &self,
        namespace: &Namespace,
        (segment, sequence): (SegmentId, u64),
        metadata: &Arc<StoredPageMetadata>,
    ) {
        if let Some(cache) = &self.metadata {
            cache
                .insert((namespace.clone(), segment, sequence), Arc::clone(metadata))
                .await;
        }
    }

    pub(crate) async fn page(
        &self,
        namespace: &Namespace,
        (segment, sequence): (SegmentId, u64),
    ) -> Option<Arc<Page>> {
        self.pages
            .as_ref()?
            .get(&(namespace.clone(), segment, sequence))
            .await
    }

    pub(crate) async fn insert_page(
        &self,
        namespace: &Namespace,
        (segment, sequence): (SegmentId, u64),
        page: &Arc<Page>,
    ) {
        if let Some(cache) = &self.pages {
            cache
                .insert((namespace.clone(), segment, sequence), Arc::clone(page))
                .await;
        }
    }

    pub(crate) async fn postings(&self, prefix: &Bytes) -> Option<Arc<SegmentPostings>> {
        self.postings.as_ref()?.get(prefix).await
    }

    pub(crate) async fn insert_postings(&self, prefix: Bytes, postings: &Arc<SegmentPostings>) {
        if let Some(cache) = &self.postings {
            cache.insert(prefix, Arc::clone(postings)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locators(pages: u32) -> Arc<TraceLocators> {
        Arc::new(TraceLocators {
            pages,
            locators: Vec::new(),
        })
    }

    #[tokio::test]
    async fn locators_read_before_an_invalidation_are_not_cached() {
        let cache = ReadCache::new(&ReadCacheConfig::default(), true);
        let namespace = Namespace::default();
        let trace_id = TraceId::new([1; 16]).unwrap();
        let written = || vec![(namespace.clone(), trace_id)];
        let stale = cache.locator_epoch();
        cache.invalidate_traces(written()).await;
        cache
            .insert_locators(&namespace, trace_id, locators(2), stale)
            .await;
        assert!(cache.current_locators(&namespace, trace_id).await.is_none());

        let epoch = cache.locator_epoch();
        cache
            .insert_locators(&namespace, trace_id, locators(2), epoch)
            .await;
        assert!(cache.current_locators(&namespace, trace_id).await.is_some());
        cache.invalidate_traces(written()).await;
        assert!(cache.current_locators(&namespace, trace_id).await.is_none());
    }

    #[tokio::test]
    async fn readers_trust_locators_only_at_the_head_page_count() {
        let cache = ReadCache::new(&ReadCacheConfig::default(), false);
        let namespace = Namespace::default();
        let trace_id = TraceId::new([1; 16]).unwrap();
        let epoch = cache.locator_epoch();
        cache
            .insert_locators(&namespace, trace_id, locators(2), epoch)
            .await;
        assert!(cache.current_locators(&namespace, trace_id).await.is_none());
        assert!(cache.locators(&namespace, trace_id, 2).await.is_some());
        assert!(cache.locators(&namespace, trace_id, 3).await.is_none());
    }
}
