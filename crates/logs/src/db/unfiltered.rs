//! Reads that skip SlateDB's SST filters unless they are cached.

use std::ops::Bound;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use common::storage::{ReadHints, Record, StorageIterator, StorageRead, StorageResult};
use common::{BytesRange, FilterResidency, SlateReadHandle};
use slatedb::FilterContext;

/// Reads through `inner` that SlateDB answers without first fetching the
/// filter of each SST they may touch: gets as scans of `[key, key\0)`, which
/// hold the one key (it filters a scan whose bounds are both `key`), and
/// prefix scans as range scans. Cold, a filter fetch is a dependent round
/// trip before the index and block reads of every SST a segment's records
/// live in, and most of the bytes of a small read; a filter only spares the
/// block reads of SSTs that span a key without holding it. Reads with
/// non-default hints, which a range scan cannot carry, stay filtered.
///
/// Reads whose SSTs all have their filters cached, as a writer's own flushes
/// and compaction outputs do, stay filtered too: those filters cost nothing,
/// and warm, skipping them reads and decodes a block of every L0 SST and
/// sorted run in the segment, which grows with the data.
pub(crate) struct UnfilteredReads {
    inner: Arc<dyn StorageRead>,
    /// Taken at the first read, so every read through this wrapper judges
    /// against one manifest.
    residency: OnceLock<Option<FilterResidency>>,
}

impl UnfilteredReads {
    /// Without `cached_filters`, every read skips filters.
    pub(crate) fn new(inner: Arc<dyn StorageRead>, cached_filters: bool) -> Self {
        let residency = OnceLock::new();
        if !cached_filters {
            residency.get_or_init(|| None);
        }
        Self { inner, residency }
    }

    /// Whether every SST that may hold keys starting with `prefix` has its
    /// filter cached, or none.
    pub(super) fn filters_cached(&self, prefix: &[u8]) -> bool {
        self.residency
            .get_or_init(|| {
                self.inner
                    .slate_read()
                    .and_then(|slate| slate.filter_residency())
            })
            .as_ref()
            .is_some_and(|residency| residency.cached(prefix))
    }
}

#[async_trait]
impl StorageRead for UnfilteredReads {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        if self.filters_cached(&key) {
            return self.inner.get(key).await;
        }
        let mut end = key.to_vec();
        end.push(0);
        let mut records = self
            .inner
            .scan_iter(BytesRange::new(
                Bound::Included(key),
                Bound::Excluded(end.into()),
            ))
            .await?;
        records.next().await
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        self.inner.scan_iter(range).await
    }

    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        if self.filters_cached(&prefix) {
            return self
                .inner
                .scan_prefix_iter(prefix, subrange, filter_context)
                .await;
        }
        self.inner
            .scan_iter(BytesRange::from_prefix_and_subrange(&prefix, &subrange))
            .await
    }

    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        if hints == ReadHints::default() {
            self.get(key).await
        } else {
            self.inner.get_with(key, hints).await
        }
    }

    async fn scan_prefix_iter_with(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
        hints: ReadHints,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        if hints == ReadHints::default() {
            self.scan_prefix_iter(prefix, subrange, filter_context)
                .await
        } else {
            self.inner
                .scan_prefix_iter_with(prefix, subrange, filter_context, hints)
                .await
        }
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.inner.slate_read()
    }
}
