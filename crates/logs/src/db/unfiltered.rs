//! Reads that skip SlateDB's SST filters.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::storage::{ReadHints, Record, StorageIterator, StorageRead, StorageResult};
use common::{BytesRange, SlateReadHandle};
use slatedb::FilterContext;

/// Reads through `inner` that SlateDB answers without first fetching the
/// filter of each SST they may touch: gets as scans of `[key, key\0)`, which
/// hold the one key (it filters a scan whose bounds are both `key`), and
/// prefix scans as range scans. Cold, a filter fetch is a dependent round
/// trip before the index and block reads of every SST a segment's records
/// live in, and most of the bytes of a small read; a filter only spares the
/// block reads of SSTs that span a key without holding it. Reads with
/// non-default hints, which a range scan cannot carry, stay filtered.
pub(crate) struct UnfilteredReads(pub(crate) Arc<dyn StorageRead>);

#[async_trait]
impl StorageRead for UnfilteredReads {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let mut end = key.to_vec();
        end.push(0);
        let mut records = self
            .0
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
        self.0.scan_iter(range).await
    }

    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        _filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        self.0
            .scan_iter(BytesRange::from_prefix_and_subrange(&prefix, &subrange))
            .await
    }

    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        if hints == ReadHints::default() {
            self.get(key).await
        } else {
            self.0.get_with(key, hints).await
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
            self.0
                .scan_prefix_iter_with(prefix, subrange, filter_context, hints)
                .await
        }
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.0.slate_read()
    }
}
