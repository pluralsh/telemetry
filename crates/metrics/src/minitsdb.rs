use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig, WriteError};
use common::{BytesRange, StorageError};
use futures::{StreamExt, TryStreamExt};

use crate::storage::{Storage, StorageRead};

const WRITE_CHANNEL: &str = "write";

use crate::Namespace;
use crate::active_series::ActiveSeriesTracker;
use crate::delta::{TsdbContext, TsdbWriteDelta};
use crate::error::Error;
use crate::flusher::TsdbFlusher;
use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{Label, Labels, Series, SeriesData, SeriesId, TimeBucket};
use crate::postings_cache::PostingsCache;
use crate::query::{BucketQueryReader, CachedSeriesResolution};
use crate::serde::forward_index::ForwardIndexValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::key::{ForwardIndexKey, InvertedIndexKey, TimeSeriesKey};
use crate::util::Result;

/// Per-bucket query reader over any storage read handle — a
/// [`crate::storage::StorageSnapshot`] on the write path, a
/// [`crate::storage::StorageReader`] on the read-only path.
pub(crate) struct MiniQueryReader<R: StorageRead> {
    namespace: Namespace,
    bucket: TimeBucket,
    snapshot: R,
    /// Cross-query postings cache, with the sequence read before `snapshot`
    /// was taken.
    postings_cache: Option<(Arc<PostingsCache>, u64)>,
    forward_cache: Option<Arc<ForwardIndexCache>>,
    replica_postings: Option<Arc<ReplicaPostings>>,
    series_cache: Option<Arc<SeriesCache>>,
    generation: tokio::sync::OnceCell<Option<u64>>,
}

/// Drives a [`PostingsCache`] for a read-only replica, which sees no
/// flushes to stamp from. Series IDs are allocated densely and become
/// visible in order, and a series' postings never change, so a bucket's
/// postings have grown exactly when the forward-index entry for its first
/// unseen ID (the watermark) exists. The bucket generation advances in the
/// same batch as any new forward-index entry, so the watermark is probed
/// only when a query sees a generation newer than the last one probed at;
/// if the watermark exists, it advances and the bucket is stamped before
/// the query takes its read sequence.
pub(crate) struct ReplicaPostings {
    cache: Arc<PostingsCache>,
    watermark: tokio::sync::Mutex<Option<SeriesId>>,
    /// The generation the watermark was last probed at, `u64::MAX` before
    /// the first probe.
    probed_at: std::sync::atomic::AtomicU64,
}

impl ReplicaPostings {
    pub(crate) fn new(cache: Arc<PostingsCache>) -> Self {
        Self {
            cache,
            watermark: tokio::sync::Mutex::new(None),
            probed_at: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }
}

/// One table of per-series entries under a single cache key, so a batch
/// lookup takes one cache probe and one read lock instead of a probe per
/// series. `weighed` is the size the cache last charged for the table; it
/// is re-inserted once that doubles, since moka weighs only on insert.
pub(crate) struct SeriesTable<V> {
    entries: std::sync::RwLock<HashMap<SeriesId, V, foldhash::fast::RandomState>>,
    bytes: std::sync::atomic::AtomicU64,
    weighed: std::sync::atomic::AtomicU64,
}

impl<V> Default for SeriesTable<V> {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            bytes: Default::default(),
            weighed: Default::default(),
        }
    }
}

impl<V: Clone> SeriesTable<V> {
    fn read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<SeriesId, V, foldhash::fast::RandomState>> {
        self.entries.read().unwrap_or_else(|e| e.into_inner())
    }

    fn get(&self, series_id: SeriesId) -> Option<V> {
        self.read().get(&series_id).cloned()
    }
}

/// Byte-bounded cache of [`SeriesTable`]s whose hot entries remain resident.
pub(crate) struct TableCache<K, V> {
    tables: moka::sync::Cache<K, Arc<SeriesTable<V>>>,
}

impl<K, V> TableCache<K, V>
where
    K: std::hash::Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn new(capacity_bytes: u64, idle_timeout: Duration) -> Self {
        Self {
            tables: moka::sync::Cache::builder()
                .max_capacity(capacity_bytes)
                .time_to_idle(idle_timeout)
                .weigher(|_, table: &Arc<SeriesTable<V>>| {
                    let bytes = table.weighed.load(std::sync::atomic::Ordering::Relaxed);
                    u32::try_from(bytes.max(64)).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }

    fn table(&self, key: K) -> Arc<SeriesTable<V>> {
        self.tables.get_with(key, Default::default)
    }

    /// Adds `found`, each with its size in bytes, to `table`.
    fn insert(
        &self,
        key: impl FnOnce() -> K,
        table: &Arc<SeriesTable<V>>,
        found: impl IntoIterator<Item = (SeriesId, V, u64)>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut added = 0;
        {
            let mut entries = table.entries.write().unwrap_or_else(|e| e.into_inner());
            for (series_id, value, bytes) in found {
                added += bytes;
                entries.insert(series_id, value);
            }
        }
        if added == 0 {
            return;
        }
        let bytes = table.bytes.fetch_add(added, Relaxed) + added;
        if bytes >= 2 * table.weighed.load(Relaxed) + 4096 {
            table.weighed.store(bytes, Relaxed);
            self.tables.insert(key(), Arc::clone(table));
        }
    }
}

/// Cross-query cache of forward-index entries, a table per bucket. An entry
/// is written once, when its series ID is allocated, and IDs are never
/// reused within a bucket, so a cached spec stays valid for the bucket's
/// lifetime; only found entries are cached, since a series may become
/// visible later.
pub(crate) struct ForwardIndexCache {
    specs: TableCache<(Namespace, TimeBucket), SeriesSpec>,
    resolutions: moka::future::Cache<ResolutionCacheKey, Arc<CachedSeriesResolution>>,
}

type ResolutionCacheKey = (Namespace, TimeBucket, u64, Arc<str>);

pub(crate) const DEFAULT_FORWARD_CACHE_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
const FORWARD_CACHE_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

fn spec_bytes(spec: &SeriesSpec) -> u64 {
    let labels: usize = spec
        .labels
        .iter()
        .map(|label| label.name.len() + label.value.len() + 48)
        .sum();
    (labels + 64) as u64
}

impl ForwardIndexCache {
    pub(crate) fn new(capacity_bytes: u64) -> Self {
        let resolution_bytes = capacity_bytes / 4;
        Self {
            specs: TableCache::new(
                capacity_bytes.saturating_sub(resolution_bytes),
                FORWARD_CACHE_IDLE_TIMEOUT,
            ),
            resolutions: moka::future::Cache::builder()
                .max_capacity(resolution_bytes)
                .time_to_idle(FORWARD_CACHE_IDLE_TIMEOUT)
                .weigher(
                    |(_, _, _, key): &ResolutionCacheKey,
                     resolution: &Arc<CachedSeriesResolution>| {
                        let labels: usize = resolution
                            .labels
                            .iter()
                            .flat_map(Labels::iter)
                            .map(|label| 48 + label.name.len() + label.value.len())
                            .sum();
                        u32::try_from(
                            64 + key.len()
                                + 4 * resolution.series_ids.len()
                                + 40 * resolution.labels.len()
                                + labels,
                        )
                        .unwrap_or(u32::MAX)
                    },
                )
                .build(),
        }
    }

    #[cfg(test)]
    pub(crate) fn cached(&self, namespace: &Namespace, bucket: TimeBucket) -> Vec<SeriesId> {
        let Some(table) = self.specs.tables.get(&(namespace.clone(), bucket)) else {
            return Vec::new();
        };
        let mut ids: Vec<SeriesId> = table.read().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    #[cfg(test)]
    pub(crate) async fn resolution_count(&self) -> u64 {
        self.resolutions.run_pending_tasks().await;
        self.resolutions.entry_count()
    }
}

/// Cross-query cache of decoded series, each a bucket's whole value, in a
/// table per bucket generation. The generation is read before any of the
/// query's samples, and every flush advances it in the batch that writes
/// its samples, so an entry never holds data older than its generation's.
/// Samples are served only while their generation record remains visible, so
/// retention still makes an expired bucket unreadable even if its decoded
/// entry remains cached.
/// Discovery's per-bucket series sets live here too, under the same keys.
pub(crate) struct SeriesCache {
    series: TableCache<(Namespace, TimeBucket, u64), Arc<SeriesData>>,
    sets: moka::future::Cache<SetCacheKey, Arc<[Labels]>>,
}

type SetCacheKey = (Namespace, TimeBucket, u64, Arc<str>);

const SERIES_CACHE_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

fn series_bytes(data: &SeriesData) -> u64 {
    let histograms: usize = data
        .histograms
        .iter()
        .map(|sample| {
            let histogram = &sample.histogram;
            128 + 16 * (histogram.positive.len() + histogram.negative.len())
        })
        .sum();
    (96 + 16 * data.timestamps.len() + histograms) as u64
}

impl SeriesCache {
    pub(crate) fn new(capacity_bytes: u64) -> Self {
        let sets_bytes = capacity_bytes / 8;
        Self {
            sets: moka::future::Cache::builder()
                .max_capacity(sets_bytes)
                .time_to_idle(SERIES_CACHE_IDLE_TIMEOUT)
                .weigher(
                    |(_, _, _, key): &(_, _, _, Arc<str>), sets: &Arc<[Labels]>| {
                        let labels: usize = sets
                            .iter()
                            .flat_map(Labels::iter)
                            .map(|label| 48 + label.name.len() + label.value.len())
                            .sum();
                        u32::try_from(64 + key.len() + 40 * sets.len() + labels).unwrap_or(u32::MAX)
                    },
                )
                .build(),
            series: TableCache::new(capacity_bytes - sets_bytes, SERIES_CACHE_IDLE_TIMEOUT),
        }
    }
}

/// The samples of `data` with `start_ms < timestamp <= end_ms`.
fn series_range(data: &SeriesData, start_ms: i64, end_ms: i64) -> SeriesData {
    let floats = data.timestamps.partition_point(|&ts| ts <= start_ms)
        ..data.timestamps.partition_point(|&ts| ts <= end_ms);
    let histograms = data
        .histograms
        .partition_point(|sample| sample.timestamp_ms <= start_ms)
        ..data
            .histograms
            .partition_point(|sample| sample.timestamp_ms <= end_ms);
    SeriesData {
        timestamps: data.timestamps[floats.clone()].to_vec(),
        values: data.values[floats].to_vec(),
        histograms: data.histograms[histograms].to_vec(),
    }
}

impl<R: StorageRead> MiniQueryReader<R> {
    pub(crate) fn new(namespace: Namespace, bucket: TimeBucket, storage: R) -> Self {
        Self {
            namespace,
            bucket,
            snapshot: storage,
            postings_cache: None,
            forward_cache: None,
            replica_postings: None,
            series_cache: None,
            generation: tokio::sync::OnceCell::new(),
        }
    }

    /// Serve sample reads through `cache`.
    pub(crate) fn with_series_cache(mut self, cache: Arc<SeriesCache>) -> Self {
        self.series_cache = Some(cache);
        self
    }

    /// The bucket's write generation in this reader's snapshot, read once.
    async fn generation(&self) -> Result<Option<u64>> {
        self.generation
            .get_or_try_init(|| {
                self.snapshot
                    .get_bucket_generation(&self.namespace, &self.bucket)
            })
            .await
            .copied()
    }

    /// [`Self::samples_many`] through the series cache: hits are cut to the
    /// range, misses decoded whole, cached, then cut.
    async fn cached_samples_many(
        &self,
        cache: &SeriesCache,
        generation: u64,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<SeriesData>> {
        let key = || (self.namespace.clone(), self.bucket, generation);
        let table = cache.series.table(key());
        let mut out = Vec::with_capacity(series_ids.len());
        let mut missing = Vec::new();
        {
            let entries = table.read();
            for (position, &series_id) in series_ids.iter().enumerate() {
                match entries.get(&series_id) {
                    Some(hit) => out.push(series_range(hit, start_ms, end_ms)),
                    None => {
                        missing.push((position, series_id));
                        out.push(SeriesData::default());
                    }
                }
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let missing_ids: Vec<SeriesId> = missing.iter().map(|&(_, id)| id).collect();
        let fetched = self
            .fetch_samples_many(metric_name, &missing_ids, i64::MIN, i64::MAX)
            .await?;
        let mut found = Vec::with_capacity(missing.len());
        for ((position, series_id), data) in missing.into_iter().zip(fetched) {
            out[position] = series_range(&data, start_ms, end_ms);
            let bytes = series_bytes(&data);
            found.push((series_id, Arc::new(data), bytes));
        }
        cache.series.insert(key, &table, found);
        Ok(out)
    }

    /// Serve inverted-index reads through `replica`'s cache once a query
    /// reader is taken with [`Self::for_query`].
    pub(crate) fn with_replica_postings(mut self, replica: Arc<ReplicaPostings>) -> Self {
        self.replica_postings = Some(replica);
        self
    }

    /// A reader for one query, carrying the replica postings cache's read
    /// sequence once the bucket's watermark is revalidated.
    pub(crate) async fn for_query(&self) -> Result<Self>
    where
        R: Clone,
    {
        let mut reader = Self {
            namespace: self.namespace.clone(),
            bucket: self.bucket,
            snapshot: self.snapshot.clone(),
            postings_cache: self.postings_cache.clone(),
            forward_cache: self.forward_cache.clone(),
            replica_postings: None,
            series_cache: self.series_cache.clone(),
            generation: tokio::sync::OnceCell::new(),
        };
        if let Some(replica) = &self.replica_postings {
            let generation = reader.generation().await?;
            let read_at = self.revalidate_postings(replica, generation).await?;
            reader.postings_cache = Some((replica.cache.clone(), read_at));
        }
        Ok(reader)
    }

    async fn revalidate_postings(
        &self,
        replica: &ReplicaPostings,
        generation: Option<u64>,
    ) -> Result<u64> {
        use std::sync::atomic::Ordering;
        let Some(generation) = generation else {
            return Ok(replica.cache.read_seq());
        };
        if replica.probed_at.load(Ordering::Acquire) == generation {
            return Ok(replica.cache.read_seq());
        }
        let mut watermark = replica.watermark.lock().await;
        if replica.probed_at.load(Ordering::Acquire) == generation {
            return Ok(replica.cache.read_seq());
        }
        if let Some(next) = *watermark
            && !self.series_exists(next).await?
        {
            replica.probed_at.store(generation, Ordering::Release);
            return Ok(replica.cache.read_seq());
        }
        let next = self.first_unseen_series(watermark.unwrap_or(0)).await?;
        *watermark = Some(next);
        replica.cache.stamp(self.bucket);
        replica.probed_at.store(generation, Ordering::Release);
        Ok(replica.cache.read_seq())
    }

    /// The first series ID at or after `from` with no forward-index entry,
    /// found by galloping from `from` then bisecting.
    async fn first_unseen_series(&self, from: SeriesId) -> Result<SeriesId> {
        if !self.series_exists(from).await? {
            return Ok(from);
        }
        let (mut seen, mut step) = (u64::from(from), 1u64);
        let mut unseen = loop {
            let probe = (seen + step).min(u64::from(SeriesId::MAX));
            if probe == seen || !self.series_exists(probe as SeriesId).await? {
                break probe.max(seen + 1);
            }
            seen = probe;
            step *= 2;
        };
        while unseen - seen > 1 {
            let mid = seen + (unseen - seen) / 2;
            if self.series_exists(mid as SeriesId).await? {
                seen = mid;
            } else {
                unseen = mid;
            }
        }
        Ok(unseen as SeriesId)
    }

    async fn series_exists(&self, series_id: SeriesId) -> Result<bool> {
        let spec = io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot
                .get_forward_index_one(&self.namespace, &self.bucket, series_id),
        )
        .await?;
        Ok(spec.is_some())
    }

    /// Serve forward-index reads through `cache`.
    pub(crate) fn with_forward_cache(mut self, cache: Arc<ForwardIndexCache>) -> Self {
        self.forward_cache = Some(cache);
        self
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
        let table = self
            .forward_cache
            .as_ref()
            .map(|cache| cache.specs.table((self.namespace.clone(), self.bucket)));
        if let Some(spec) = table.as_ref().and_then(|table| table.get(series_id)) {
            return Ok(Some(spec));
        }
        let spec = io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot
                .get_forward_index_one(&self.namespace, &self.bucket, series_id),
        )
        .await?;
        if let (Some(cache), Some(table), Some(spec)) = (&self.forward_cache, &table, &spec) {
            cache.specs.insert(
                || (self.namespace.clone(), self.bucket),
                table,
                [(series_id, spec.clone(), spec_bytes(spec))],
            );
        }
        Ok(spec)
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

    async fn cached_selector(&self, key: &Arc<str>) -> Option<Arc<roaring::RoaringBitmap>> {
        let (cache, read_at) = self.postings_cache.as_ref()?;
        cache.selector(self.bucket, key, *read_at).await
    }

    async fn cache_selector(&self, key: &Arc<str>, postings: &roaring::RoaringBitmap) {
        if let Some((cache, read_at)) = &self.postings_cache {
            cache
                .insert_selector(self.bucket, key, *read_at, postings.clone())
                .await;
        }
    }

    async fn cached_series_set(&self, key: &Arc<str>) -> Option<Arc<[Labels]>> {
        let cache = self.series_cache.as_ref()?;
        let generation = self.generation().await.ok()??;
        cache
            .sets
            .get(&(self.namespace.clone(), self.bucket, generation, key.clone()))
            .await
    }

    async fn cache_series_set(&self, key: &Arc<str>, series: Arc<[Labels]>) {
        if let Some(cache) = &self.series_cache
            && let Ok(Some(generation)) = self.generation().await
        {
            cache
                .sets
                .insert(
                    (self.namespace.clone(), self.bucket, generation, key.clone()),
                    series,
                )
                .await;
        }
    }

    async fn cached_selector_resolution(
        &self,
        key: &Arc<str>,
    ) -> Option<Arc<CachedSeriesResolution>> {
        let cache = self.forward_cache.as_ref()?;
        let generation = self.generation().await.ok()??;
        cache
            .resolutions
            .get(&(self.namespace.clone(), self.bucket, generation, key.clone()))
            .await
    }

    async fn cache_selector_resolution(
        &self,
        key: &Arc<str>,
        resolution: Arc<CachedSeriesResolution>,
    ) {
        if let Some(cache) = &self.forward_cache
            && let Ok(Some(generation)) = self.generation().await
        {
            cache
                .resolutions
                .insert(
                    (self.namespace.clone(), self.bucket, generation, key.clone()),
                    resolution,
                )
                .await;
        }
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<SeriesData> {
        if let Some(cache) = &self.series_cache
            && let Some(generation) = self.generation().await?
        {
            let mut out = self
                .cached_samples_many(
                    cache,
                    generation,
                    metric_name,
                    &[series_id],
                    start_ms,
                    end_ms,
                )
                .await?;
            return Ok(out.pop().unwrap_or_default());
        }
        self.fetch_samples(series_id, metric_name, start_ms, end_ms)
            .await
    }

    async fn samples_many(
        &self,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<SeriesData>> {
        if let Some(cache) = &self.series_cache
            && let Some(generation) = self.generation().await?
        {
            return self
                .cached_samples_many(cache, generation, metric_name, series_ids, start_ms, end_ms)
                .await;
        }
        self.fetch_samples_many(metric_name, series_ids, start_ms, end_ms)
            .await
    }

    /// Scan-backed like [`Self::samples_many`]: forward-index keys are
    /// ordered by series ID within the bucket.
    async fn forward_index_many(&self, series_ids: &[SeriesId]) -> Result<Vec<Option<SeriesSpec>>> {
        let Some(cache) = &self.forward_cache else {
            return self.fetch_forward_index_many(series_ids).await;
        };
        let table = cache.specs.table((self.namespace.clone(), self.bucket));
        let mut out = Vec::with_capacity(series_ids.len());
        let mut missing = Vec::new();
        {
            let specs = table.read();
            for (position, &series_id) in series_ids.iter().enumerate() {
                let hit = specs.get(&series_id).cloned();
                if hit.is_none() {
                    missing.push((position, series_id));
                }
                out.push(hit);
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let missing_ids: Vec<SeriesId> = missing.iter().map(|&(_, id)| id).collect();
        let fetched = self.fetch_forward_index_many(&missing_ids).await?;
        let mut found = Vec::new();
        for ((position, series_id), spec) in missing.into_iter().zip(fetched) {
            if let Some(spec) = &spec {
                found.push((series_id, spec.clone(), spec_bytes(spec)));
            }
            out[position] = spec;
        }
        cache
            .specs
            .insert(|| (self.namespace.clone(), self.bucket), &table, found);
        Ok(out)
    }
}

impl<R: StorageRead> MiniQueryReader<R> {
    async fn fetch_samples(
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
    async fn fetch_samples_many(
        &self,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<SeriesData>> {
        if !self
            .samples_scan_worthwhile(metric_name, series_ids)
            .await?
        {
            let fetches: Vec<_> = series_ids
                .iter()
                .map(|&series_id| self.fetch_samples(series_id, metric_name, start_ms, end_ms))
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

    /// [`scan_worthwhile`] for one metric's time-series keys. Series IDs are
    /// allocated across all metrics in the bucket, but a scan over
    /// `metric_name`'s key range only visits that metric's series, so a
    /// span that looks sparse in IDs is measured against the metric's
    /// postings before falling back to point gets.
    async fn samples_scan_worthwhile(
        &self,
        metric_name: &str,
        series_ids: &[SeriesId],
    ) -> Result<bool> {
        let Some((min_id, max_id)) = scan_bounds(series_ids) else {
            return Ok(false);
        };
        if scan_worthwhile(series_ids) {
            return Ok(true);
        }
        let Some(postings) = self
            .inverted_index_term(&Label::metric_name(metric_name))
            .await?
        else {
            return Ok(false);
        };
        let mut scanned = roaring::RoaringBitmap::new();
        scanned.insert_range(u64::from(min_id)..u64::from(max_id) + 1);
        scanned &= &postings;
        Ok(scanned.len() <= series_ids.len() as u64 * SCAN_MAX_SPAN_PER_SERIES)
    }

    async fn fetch_forward_index_many(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Vec<Option<SeriesSpec>>> {
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
/// A batch scans only when it would read at most this many keys per
/// requested series. A cached point get costs about as much as stepping
/// the scan iterator over seven keys, so scans stop paying off past that.
const SCAN_MAX_SPAN_PER_SERIES: u64 = 6;

/// Whether `series_ids` is large and dense enough for a range scan to beat
/// point gets.
fn scan_worthwhile(series_ids: &[SeriesId]) -> bool {
    let Some((min_id, max_id)) = scan_bounds(series_ids) else {
        return false;
    };
    let span = u64::from(max_id - min_id) + 1;
    span <= series_ids.len() as u64 * SCAN_MAX_SPAN_PER_SERIES
}

/// The ID range a scan over `series_ids` would cover, or `None` when the
/// batch is too small to scan.
fn scan_bounds(series_ids: &[SeriesId]) -> Option<(SeriesId, SeriesId)> {
    if series_ids.len() < SCAN_MIN_SERIES {
        return None;
    }
    Some((*series_ids.iter().min()?, *series_ids.iter().max()?))
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
        let last_generation = snapshot
            .get_bucket_generation(&namespace, &bucket)
            .await?
            .unwrap_or(0);

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
            last_generation,
        };

        let mut write_coordinator = WriteCoordinator::new(
            write_buffer,
            vec![WRITE_CHANNEL.to_string()],
            context,
            (),
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
            last_generation: 0,
        };

        let config = WriteCoordinatorConfig {
            queue_capacity,
            ..Default::default()
        };

        let mut write_coordinator = WriteCoordinator::new(
            config,
            vec![WRITE_CHANNEL.to_string()],
            context,
            (),
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

    fn host_series(name: &str, host: usize, ts: i64) -> Series {
        Series::new(
            name,
            vec![Label::new("host", format!("h{host}"))],
            vec![Sample::new(ts, host as f64)],
        )
    }

    async fn metric_ids(
        reader: &MiniQueryReader<crate::storage::StorageSnapshot>,
        metric_name: &str,
    ) -> Vec<SeriesId> {
        reader
            .inverted_index_term(&Label::metric_name(metric_name))
            .await
            .unwrap()
            .unwrap()
            .iter()
            .collect()
    }

    #[tokio::test]
    async fn should_keep_bucket_generations_increasing_across_writer_reloads() {
        // given: a stored generation ahead of anything the clock would issue,
        // as left by an earlier writer whose clock ran fast
        let bucket = TimeBucket::hour(60);
        let namespace = Namespace::default();
        let storage = test_storage().await;
        let ahead = 3 * (u64::MAX / 4);
        storage
            .apply(vec![crate::storage::put_bucket_generation(
                &namespace,
                bucket,
                ahead,
                common::Ttl::Default,
            )])
            .await
            .unwrap();

        // when: a writer reloads the bucket and flushes a late sample into it
        let mini = MiniTsdb::load(
            namespace.clone(),
            bucket,
            storage.clone(),
            None,
            Arc::new(ActiveSeriesTracker::new(0)),
            WriteCoordinatorConfig::default(),
            None,
        )
        .await
        .unwrap();
        mini.ingest(&test_series("late", 3_700_000, 1.0))
            .await
            .unwrap();
        mini.flush_written().await.unwrap();

        // then
        let snapshot = storage.snapshot().await.unwrap();
        let generation = snapshot
            .get_bucket_generation(&namespace, &bucket)
            .await
            .unwrap()
            .expect("generation");
        assert!(generation > ahead, "{generation} should follow {ahead}");
    }

    #[tokio::test]
    async fn should_scan_samples_of_a_metric_interleaved_with_others() {
        // given - 20 metrics written host-major, so each metric's series IDs
        // are 20 apart: too sparse for an ID-span check alone
        let bucket = TimeBucket::hour(60);
        let storage = test_storage().await;
        let mini = load_with_config(bucket, storage.clone(), 16).await;
        let batch: Vec<Series> = (0..10)
            .flat_map(|host| (0..20).map(move |m| host_series(&format!("m{m}"), host, 3_700_000)))
            .collect();
        mini.ingest_batch(&batch, None).await.unwrap();
        mini.flush_written().await.unwrap();
        let reader = MiniQueryReader::new(
            Namespace::default(),
            bucket,
            storage.snapshot().await.unwrap(),
        );
        let mut ids = metric_ids(&reader, "m0").await;
        assert!(!scan_worthwhile(&ids));
        ids.reverse();

        // when
        let worthwhile = reader.samples_scan_worthwhile("m0", &ids).await.unwrap();
        let samples = reader.samples_many("m0", &ids, 0, i64::MAX).await.unwrap();

        // then
        assert!(worthwhile);
        let values: Vec<f64> = samples.iter().map(|s| s.values[0]).collect();
        assert_eq!(values, (0..10).rev().map(f64::from).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn should_find_the_first_unseen_series_id_from_any_start() {
        // given - 37 series, so IDs 0..37 exist
        let bucket = TimeBucket::hour(60);
        let storage = test_storage().await;
        let mini = load_with_config(bucket, storage.clone(), 16).await;
        let batch: Vec<Series> = (0..37)
            .map(|host| host_series("m", host, 3_700_000))
            .collect();
        mini.ingest_batch(&batch, None).await.unwrap();
        mini.flush_written().await.unwrap();
        let reader = MiniQueryReader::new(
            Namespace::default(),
            bucket,
            storage.snapshot().await.unwrap(),
        );

        // when
        let mut found = Vec::new();
        for from in [0, 1, 5, 36, 37, 40] {
            found.push(reader.first_unseen_series(from).await.unwrap());
        }

        // then
        assert_eq!(found, vec![37, 37, 37, 37, 37, 40]);
    }

    #[tokio::test]
    async fn should_point_get_a_sparse_selection_within_one_metric() {
        // given - every 8th series of one 400-series metric, so a scan would
        // read 8 keys per requested series
        let bucket = TimeBucket::hour(60);
        let storage = test_storage().await;
        let mini = load_with_config(bucket, storage.clone(), 16).await;
        let batch: Vec<Series> = (0..400)
            .map(|host| host_series("big", host, 3_700_000))
            .collect();
        mini.ingest_batch(&batch, None).await.unwrap();
        mini.flush_written().await.unwrap();
        let reader = MiniQueryReader::new(
            Namespace::default(),
            bucket,
            storage.snapshot().await.unwrap(),
        );
        let ids: Vec<SeriesId> = metric_ids(&reader, "big")
            .await
            .into_iter()
            .step_by(8)
            .collect();

        // when
        let worthwhile = reader.samples_scan_worthwhile("big", &ids).await.unwrap();

        // then
        assert_eq!(ids.len(), 50);
        assert!(!worthwhile);
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
