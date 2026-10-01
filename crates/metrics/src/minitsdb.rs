use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig, WriteError};
use common::{BytesRange, StorageError};
use futures::{StreamExt, TryStreamExt};

use crate::storage::{Storage, StorageRead, StorageSnapshot};

const WRITE_CHANNEL: &str = "write";

use crate::Namespace;
use crate::active_series::ActiveSeriesTracker;
use crate::delta::{TsdbContext, TsdbWriteDelta};
use crate::error::Error;
use crate::flusher::TsdbFlusher;
use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{Label, Series, SeriesData, SeriesId, TimeBucket};
use crate::postings_cache::PostingsCache;
use crate::query::BucketQueryReader;
use crate::serde::forward_index::ForwardIndexValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::key::{ForwardIndexKey, InvertedIndexKey, TimeSeriesKey};
use crate::util::Result;

/// Per-bucket query reader over any storage read handle — a
/// [`StorageSnapshot`] on the write path, a
/// [`crate::storage::StorageReader`] on the read-only path.
pub(crate) struct MiniQueryReader<R: StorageRead> {
    namespace: Namespace,
    bucket: TimeBucket,
    snapshot: R,
    /// Cross-query postings cache, with the sequence read before `snapshot`
    /// was taken.
    postings_cache: Option<(Arc<PostingsCache>, u64)>,
}

impl<R: StorageRead> MiniQueryReader<R> {
    pub(crate) fn new(namespace: Namespace, bucket: TimeBucket, storage: R) -> Self {
        Self {
            namespace,
            bucket,
            snapshot: storage,
            postings_cache: None,
        }
    }

    /// Serve inverted-index reads through `cache`. `read_at` must be
    /// [`PostingsCache::read_seq`] as read before `storage` was snapshotted.
    pub(crate) fn with_postings_cache(mut self, cache: Arc<PostingsCache>, read_at: u64) -> Self {
        self.postings_cache = Some((cache, read_at));
        self
    }
}

#[async_trait]
impl<R: StorageRead> BucketQueryReader for MiniQueryReader<R> {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = ForwardIndex::default();
        let specs = self.forward_index_many(series_ids).await?;
        for (&series_id, spec) in series_ids.iter().zip(specs) {
            if let Some(spec) = spec {
                forward_index.series.insert(series_id, spec);
            }
        }
        Ok(Box::new(forward_index))
    }

    async fn all_forward_index(
        &self,
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot
                .get_forward_index(&self.namespace, self.bucket),
        )
        .await?;
        Ok(Box::new(forward_index))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = io_trace_async(IoKindLocal::InvertedIndexFetch, async {
            self.snapshot
                .get_inverted_index_terms(&self.namespace, &self.bucket, terms)
                .await
        })
        .await?;
        Ok(Box::new(inverted_index))
    }

    async fn all_inverted_index(
        &self,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = io_trace_async(
            IoKindLocal::InvertedIndexFetch,
            self.snapshot
                .get_inverted_index(&self.namespace, self.bucket),
        )
        .await?;
        Ok(Box::new(inverted_index))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        io_trace_async(
            IoKindLocal::LabelValuesFetch,
            self.snapshot
                .get_label_values(&self.namespace, &self.bucket, label_name),
        )
        .await
    }

    async fn forward_index_one(
        &self,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot
                .get_forward_index_one(&self.namespace, &self.bucket, series_id),
        )
        .await
    }

    async fn inverted_index_term(&self, term: &Label) -> Result<Option<roaring::RoaringBitmap>> {
        if let Some((cache, _)) = &self.postings_cache
            && let Some(hit) = cache.term(self.bucket, term).await
        {
            return Ok(hit.as_ref().clone());
        }
        let postings = io_trace_async(
            IoKindLocal::InvertedIndexFetch,
            self.snapshot
                .get_inverted_index_term(&self.namespace, &self.bucket, term),
        )
        .await?;
        if let Some((cache, read_at)) = &self.postings_cache {
            cache
                .insert_term(self.bucket, term, *read_at, postings.clone())
                .await;
        }
        Ok(postings)
    }

    async fn label_postings(
        &self,
        label_name: &str,
    ) -> Result<Vec<(String, roaring::RoaringBitmap)>> {
        if let Some((cache, _)) = &self.postings_cache
            && let Some(hit) = cache.label(self.bucket, label_name).await
        {
            return Ok(hit.as_ref().clone());
        }
        let postings = self.scan_label_postings(label_name).await?;
        if let Some((cache, read_at)) = &self.postings_cache {
            cache
                .insert_label(self.bucket, label_name, *read_at, postings.clone())
                .await;
        }
        Ok(postings)
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<SeriesData> {
        let storage_key = TimeSeriesKey {
            namespace: self.namespace.clone(),
            bucket: self.bucket,
            metric_name: metric_name.to_string(),
            series_id,
        };
        let value = io_trace_async(
            IoKindLocal::SamplesFetch,
            self.snapshot.get(storage_key.encode()),
        )
        .await?;

        match value {
            Some(value) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::SamplesFetch,
                    value.len() as u64,
                );
                let raw_len = value.len() as u64;
                let samples = io_trace_sync(IoKindLocal::Deserialize, || {
                    SeriesData::decode_range(value.as_ref(), start_ms, end_ms).map_err(|e| {
                        Error::Internal(format!("Invalid timeseries data in storage: {e}"))
                    })
                })?;
                // Deserialize's bytes are the same bytes the fetch returned —
                // it's decoding that payload. Attribute here so both kinds
                // report throughput.
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::Deserialize,
                    raw_len,
                );
                Ok(samples)
            }
            None => Ok(SeriesData::default()),
        }
    }

    /// One key-range scan instead of a point get per series: a metric's
    /// time-series keys are contiguous and ordered by series ID, so the scan
    /// reads them sequentially (with read-ahead) rather than probing every
    /// SST once per series. Falls back to point gets for small or sparse
    /// requests, where the scan would mostly read unrequested series.
    async fn samples_many(
        &self,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<SeriesData>> {
        if !scan_worthwhile(series_ids) {
            let fetches: Vec<_> = series_ids
                .iter()
                .map(|&series_id| self.samples(series_id, metric_name, start_ms, end_ms))
                .collect();
            return futures::stream::iter(fetches)
                .buffered(crate::query::SAMPLES_MANY_CONCURRENCY)
                .try_collect()
                .await;
        }
        let key = |series_id| {
            TimeSeriesKey {
                namespace: self.namespace.clone(),
                bucket: self.bucket,
                metric_name: metric_name.to_string(),
                series_id,
            }
            .encode()
        };
        self.scan_series(key, series_ids, IoKindLocal::SamplesFetch, |value| {
            crate::promql::trace::record_bytes(
                crate::promql::trace::IoKind::Deserialize,
                value.len() as u64,
            );
            io_trace_sync(IoKindLocal::Deserialize, || {
                SeriesData::decode_range(value, start_ms, end_ms).map_err(|e| {
                    Error::Internal(format!("Invalid timeseries data in storage: {e}"))
                })
            })
        })
        .await
    }

    /// Scan-backed like [`Self::samples_many`]: forward-index keys are
    /// ordered by series ID within the bucket.
    async fn forward_index_many(&self, series_ids: &[SeriesId]) -> Result<Vec<Option<SeriesSpec>>> {
        if !scan_worthwhile(series_ids) {
            let fetches: Vec<_> = series_ids
                .iter()
                .map(|&series_id| self.forward_index_one(series_id))
                .collect();
            return futures::stream::iter(fetches)
                .buffered(crate::query::FORWARD_INDEX_MANY_CONCURRENCY)
                .try_collect()
                .await;
        }
        let key = |series_id| {
            ForwardIndexKey {
                namespace: self.namespace.clone(),
                bucket: self.bucket,
                series_id,
            }
            .encode()
        };
        self.scan_series(key, series_ids, IoKindLocal::ForwardIndexFetch, |value| {
            Ok(Some(ForwardIndexValue::decode(value)?.into()))
        })
        .await
    }
}

impl<R: StorageRead> MiniQueryReader<R> {
    /// One prefix scan over the label's inverted-index keys, which hold
    /// each value's postings.
    async fn scan_label_postings(
        &self,
        label_name: &str,
    ) -> Result<Vec<(String, roaring::RoaringBitmap)>> {
        let kind = IoKindLocal::InvertedIndexFetch;
        let prefix = InvertedIndexKey::attribute_prefix(&self.namespace, &self.bucket, label_name);
        let mut iter = io_trace_async(kind, self.snapshot.scan_prefix(prefix)).await?;
        let mut out = Vec::new();
        while let Some(record) = io_trace_async(kind, iter.next())
            .await
            .map_err(StorageError::from_storage)?
        {
            crate::promql::trace::record_bytes(kind, record.value.len() as u64);
            let key = InvertedIndexKey::decode(record.key.as_ref())?;
            let postings = InvertedIndexValue::decode(record.value.as_ref())?.postings;
            if !postings.is_empty() {
                out.push((key.value, postings));
            }
        }
        Ok(out)
    }

    /// Decode the values of `series_ids` from one scan over keys built by
    /// `key` (which must end in the big-endian series ID), returned in
    /// `series_ids` order. Requested series with no key get `T::default()`.
    /// `series_ids` must be non-empty.
    async fn scan_series<T: Default + Clone>(
        &self,
        key: impl Fn(SeriesId) -> Bytes,
        series_ids: &[SeriesId],
        kind: IoKindLocal,
        mut decode: impl FnMut(&[u8]) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut wanted: Vec<(SeriesId, usize)> = series_ids.iter().copied().zip(0..).collect();
        wanted.sort_unstable();
        let mut out = vec![T::default(); series_ids.len()];
        let mut next = 0;
        let range = BytesRange::new(
            Bound::Included(key(wanted[0].0)),
            Bound::Included(key(wanted[wanted.len() - 1].0)),
        );
        let mut iter = io_trace_async(kind, self.snapshot.scan(range)).await?;
        while next < wanted.len() {
            let Some(record) = io_trace_async(kind, iter.next())
                .await
                .map_err(StorageError::from_storage)?
            else {
                break;
            };
            let Some(series_id) = record
                .key
                .len()
                .checked_sub(4)
                .and_then(|at| record.key[at..].try_into().ok())
                .map(SeriesId::from_be_bytes)
            else {
                continue;
            };
            while wanted.get(next).is_some_and(|&(id, _)| id < series_id) {
                next += 1;
            }
            if wanted.get(next).is_none_or(|&(id, _)| id != series_id) {
                continue;
            }
            crate::promql::trace::record_bytes(kind, record.value.len() as u64);
            let value = decode(record.value.as_ref())?;
            while let Some(&(id, position)) = wanted.get(next)
                && id == series_id
            {
                out[position] = value.clone();
                next += 1;
            }
        }
        Ok(out)
    }
}

/// Below this many series a batch uses point gets.
const SCAN_MIN_SERIES: usize = 8;
/// A batch scans only when its series-ID span is at most this many IDs per
/// requested series, bounding the unrequested keys a scan reads.
const SCAN_MAX_SPAN_PER_SERIES: u64 = 16;

/// Whether `series_ids` is large and dense enough for a range scan to beat
/// point gets.
fn scan_worthwhile(series_ids: &[SeriesId]) -> bool {
    if series_ids.len() < SCAN_MIN_SERIES {
        return false;
    }
    let (Some(&min_id), Some(&max_id)) = (series_ids.iter().min(), series_ids.iter().max()) else {
        return false;
    };
    let span = u64::from(max_id - min_id) + 1;
    span <= series_ids.len() as u64 * SCAN_MAX_SPAN_PER_SERIES
}

// ─── trace helpers ──────────────────────────────────────────────────

use crate::promql::trace::IoKind as IoKindLocal;

async fn io_trace_async<F: std::future::Future<Output = T>, T>(kind: IoKindLocal, fut: F) -> T {
    crate::promql::trace::record_async(kind, fut).await
}

fn io_trace_sync<T>(kind: IoKindLocal, f: impl FnOnce() -> T) -> T {
    crate::promql::trace::record_sync(kind, f)
}

pub(crate) struct MiniTsdb {
    namespace: Namespace,
    bucket: TimeBucket,
    write_coordinator: WriteCoordinator<TsdbWriteDelta, TsdbFlusher>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    pub(crate) fn query_reader(&self) -> MiniQueryReader<StorageSnapshot> {
        let view = self.write_coordinator.view();
        MiniQueryReader::new(self.namespace.clone(), self.bucket, view.snapshot.clone())
    }

    pub(crate) async fn load(
        namespace: Namespace,
        bucket: TimeBucket,
        storage: Arc<Storage>,
        retention: Option<Duration>,
        active_series: Arc<ActiveSeriesTracker>,
        write_buffer: WriteCoordinatorConfig,
        postings_cache: Option<Arc<PostingsCache>>,
    ) -> Result<Self> {
        let snapshot = storage.snapshot().await?;

        let mut series_dict = HashMap::new();
        let next_series_id = snapshot
            .load_series_dictionary(&namespace, &bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        let context = TsdbContext {
            namespace: namespace.clone(),
            bucket,
            series_dict: Arc::new(series_dict),
            next_series_id,
            active_series: active_series.clone(),
        };

        let flusher = TsdbFlusher {
            storage: storage.clone(),
            retention,
            active_series,
            postings_cache,
        };

        let initial_snapshot: StorageSnapshot = storage
            .snapshot()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut write_coordinator = WriteCoordinator::new(
            write_buffer,
            vec![WRITE_CHANNEL.to_string()],
            context,
            initial_snapshot,
            flusher,
        );
        write_coordinator.start();

        Ok(Self {
            namespace,
            bucket,
            write_coordinator,
        })
    }

    /// Ingest a batch of series with samples in a single operation.
    ///
    /// If `timeout` is provided, waits up to the given duration for space in the
    /// write queue. Otherwise, fails immediately when the queue is full.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>()
        )
    )]
    pub(crate) async fn ingest_batch(
        &self,
        series_list: &[Series],
        timeout: Option<Duration>,
    ) -> Result<()> {
        let total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>();

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Starting MiniTsdb batch ingest"
        );

        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut write_handle = match timeout {
            Some(t) => handle
                .write_timeout(series_list.to_vec(), t)
                .await
                .map_err(|e| map_write_error(e.discard_inner()))?,
            None => handle
                .try_write(series_list.to_vec())
                .await
                .map_err(|e| map_write_error(e.discard_inner()))?,
        };

        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(map_write_error)?;

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Completed MiniTsdb batch ingest"
        );

        Ok(())
    }

    /// Ingest a single series with samples.
    pub(crate) async fn ingest(&self, series: &Series) -> Result<()> {
        self.ingest_batch(std::slice::from_ref(series), None).await
    }

    /// Flush pending data to the storage memtable (not yet durable).
    ///
    /// After this returns, the data is visible to snapshot reads but has not
    /// been persisted to durable storage. Call [`Storage::flush`] afterwards
    /// to make the data durable.
    pub(crate) async fn flush_written(&self) -> Result<()> {
        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;

        flush_handle
            .wait(Durability::Written)
            .await
            .map_err(map_write_error)?;

        Ok(())
    }

    /// Gracefully stop the write coordinator, flushing pending data.
    pub(crate) async fn stop(self) -> Result<()> {
        self.write_coordinator
            .stop()
            .await
            .map_err(Error::Internal)?;
        Ok(())
    }
}

fn map_write_error(e: WriteError) -> Error {
    match e {
        WriteError::Backpressure(_) => {
            metrics::counter!(crate::tsdb_metrics::TSDB_BACKPRESSURE).increment(1);
            Error::Backpressure
        }
        WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Internal("Write coordinator shut down".to_string()),
        WriteError::ApplyError(_, msg) => Error::Internal(msg),
        WriteError::FlushError(msg) => Error::Storage(msg),
        WriteError::Internal(msg) => Error::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Sample, Series};
    use crate::storage::in_memory_storage;

    /// Create a MiniTsdb with a custom queue capacity.
    async fn load_with_config(
        bucket: TimeBucket,
        storage: Arc<Storage>,
        queue_capacity: usize,
    ) -> MiniTsdb {
        let snapshot = storage.snapshot().await.unwrap();

        let mut series_dict = HashMap::new();
        let next_series_id = snapshot
            .load_series_dictionary(
                &crate::Namespace::default(),
                &bucket,
                |fingerprint, series_id| {
                    series_dict.insert(fingerprint, series_id);
                },
            )
            .await
            .unwrap();

        let active_series = Arc::new(ActiveSeriesTracker::new(0));
        let context = TsdbContext {
            namespace: crate::Namespace::default(),
            bucket,
            series_dict: Arc::new(series_dict),
            next_series_id,
            active_series: active_series.clone(),
        };

        let flusher = TsdbFlusher {
            storage: storage.clone(),
            retention: None,
            active_series,
            postings_cache: None,
        };

        let initial_snapshot: StorageSnapshot = storage.snapshot().await.unwrap();

        let config = WriteCoordinatorConfig {
            queue_capacity,
            ..Default::default()
        };

        let mut write_coordinator = WriteCoordinator::new(
            config,
            vec![WRITE_CHANNEL.to_string()],
            context,
            initial_snapshot,
            flusher,
        );
        write_coordinator.start();

        MiniTsdb {
            namespace: crate::Namespace::default(),
            bucket,
            write_coordinator,
        }
    }

    fn test_series(name: &str, ts: i64, value: f64) -> Series {
        Series::new(
            name,
            vec![Label::new("host", "server1")],
            vec![Sample::new(ts, value)],
        )
    }

    async fn test_storage() -> Arc<Storage> {
        Arc::new(in_memory_storage().await)
    }

    #[tokio::test]
    async fn should_succeed_ingest_batch_with_timeout_when_queue_has_space() {
        // given - queue has space (capacity=1)
        let bucket = TimeBucket::hour(60);
        let storage = test_storage().await;
        let mini = load_with_config(bucket, storage, 1).await;

        // when
        let s1 = vec![test_series("cpu", 3_700_000, 1.0)];
        let result = mini
            .ingest_batch(&s1, Some(Duration::from_millis(50)))
            .await;

        // then
        assert!(result.is_ok(), "expected success, got {:?}", result);
    }

    #[tokio::test(start_paused = true)]
    async fn should_fail_ingest_batch_when_timeout_too_short_for_drain() {
        // given - queue_capacity=2, coordinator paused
        let bucket = TimeBucket::hour(60);
        let storage = test_storage().await;
        let mini = load_with_config(bucket, storage, 2).await;

        let pause = mini.write_coordinator.pause_handle(WRITE_CHANNEL);
        pause.pause();

        // fill the queue
        let handle = mini.write_coordinator.handle(WRITE_CHANNEL);
        let _wh1 = handle
            .try_write(vec![test_series("cpu", 3_700_000, 1.0)])
            .await
            .unwrap();
        let _wh2 = handle
            .try_write(vec![test_series("cpu", 3_700_001, 2.0)])
            .await
            .unwrap();

        // verify immediate reject with no timeout
        let s3 = vec![test_series("cpu", 3_700_002, 3.0)];
        let result = mini.ingest_batch(&s3, None).await;
        assert!(
            matches!(result, Err(Error::Backpressure)),
            "expected Backpressure with no timeout, got {:?}",
            result
        );

        // unpause after 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            pause.unpause();
        });

        // when - 10ms timeout is too short, queue is still full
        let result = mini
            .ingest_batch(&s3, Some(Duration::from_millis(10)))
            .await;

        // then
        assert!(
            matches!(result, Err(Error::Backpressure)),
            "expected Backpressure with short timeout, got {:?}",
            result
        );

        // when - 5s timeout is long enough to wait for the delayed unpause
        let result = mini.ingest_batch(&s3, Some(Duration::from_secs(5))).await;

        // then
        assert!(
            result.is_ok(),
            "expected success with long timeout, got {:?}",
            result
        );
    }
}
