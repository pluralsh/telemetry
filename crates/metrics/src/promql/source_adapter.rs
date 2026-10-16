//! Adapter from the per-bucket [`QueryReader`] surface to [`SeriesSource`].
//!
//! Fans out per-bucket selector resolution / sample streaming. Callers see
//! single streams; cross-bucket stitching lives inside this source.
//!
//! A series spanning multiple buckets yields one [`SampleBatch`] per bucket
//! in bucket-timestamp order; operators merge across buckets. Selector
//! matcher logic lives in [`selector_util`].

use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use futures::stream::{self, StreamExt, TryStreamExt};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::VectorSelector;
use roaring::RoaringBitmap;

use crate::model::{Label, Labels, SeriesData, SeriesId, TimeBucket};
use crate::query::{CachedSeriesResolution, QueryReader};

use super::index_cache::IndexCache;
use super::memory::QueryError;
use super::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest, SeriesSource,
    TimeRange,
};

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------
//
// Buckets are fully independent keyspaces in RFC 0001's layout (all record
// keys are bucket-prefixed, series IDs are bucket-scoped), so cross-bucket
// fan-out cannot affect correctness. RFC 0007 §"Execution Model" (line 256)
// prohibits implicit spawn-per-series, so intra-bucket sample fan-out is
// polled concurrency within the run's task; the constants below act as both
// scheduler and I/O ceiling.

/// Cross-bucket readahead for resolve.
const METADATA_STAGE_READAHEAD: usize = 32;

/// Cross-bucket readahead for sample batching.
const SAMPLE_STAGE_READAHEAD: usize = 32;

/// Per-key index-fetch fan-out inside one bucket's resolve path. Worst-case
/// in-flight gets during build_physical = `METADATA_STAGE_READAHEAD * INDEX_PER_KEY`.
const INDEX_PER_KEY_CONCURRENCY: usize = 64;

// ---------------------------------------------------------------------------
// Bucket-id encoding
// ---------------------------------------------------------------------------

/// Pack `(start, size)` into the opaque `u64` carried by
/// [`ResolvedSeriesRef::bucket_id`].
#[inline]
pub(crate) fn encode_bucket(bucket: TimeBucket) -> u64 {
    ((bucket.start as u64) << 8) | (bucket.size as u64)
}

/// Inverse of [`encode_bucket`]. `None` only on defensive size=0 guard.
#[inline]
pub(crate) fn decode_bucket(bucket_id: u64) -> Option<TimeBucket> {
    let size_bits = (bucket_id & 0xFF) as u8;
    let start = (bucket_id >> 8) as u32;
    if size_bits == 0 {
        return None;
    }
    Some(TimeBucket {
        start,
        size: size_bits,
    })
}

/// `[start_ms, end_ms)` covered by a bucket.
#[inline]
fn bucket_ms_window(bucket: TimeBucket) -> (i64, i64) {
    let start_ms = (bucket.start as i64) * 60 * 1000;
    let end_ms = start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
    (start_ms, end_ms)
}

/// Matches the retain clause in `QueryPlan::for_matrix`.
#[inline]
fn bucket_overlaps(bucket: TimeBucket, time_range: TimeRange) -> bool {
    if time_range.is_empty() {
        return false;
    }
    let (bucket_start_ms, bucket_end_ms) = bucket_ms_window(bucket);
    !(bucket_end_ms <= time_range.start_ms || bucket_start_ms >= time_range.end_ms_exclusive)
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// [`SeriesSource`] over the crate-internal [`QueryReader`]. One instance per
/// query. `resolve` consults the query-scoped index cache; `samples` needs no
/// cache at all — metric names ride on `ResolvedSeriesRef`.
///
/// The cache lives for the entire query lifetime rather than being dropped
/// after `build_physical_plan`: subquery operators re-enter the planner at
/// execution time (see `build_subquery` → `build_node` → `resolve_leaf`),
/// and that path needs the cache warm for the inner subtree's resolves.
pub(crate) struct QueryReaderSource<R: QueryReader> {
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
}

impl<R: QueryReader> QueryReaderSource<R> {
    /// Creates a fresh query-scoped index cache alongside the adapter.
    pub(crate) fn new(reader: Arc<R>) -> Self {
        Self {
            reader,
            index_cache: Arc::new(IndexCache::new()),
        }
    }
}

impl<R: QueryReader + 'static> SeriesSource for QueryReaderSource<R> {
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        let reader = self.reader.clone();
        let index_cache = self.index_cache.clone();
        let selector = selector.clone();
        resolve_stream(reader, index_cache, selector, time_range, None)
    }

    fn resolve_projected(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
        projection: Arc<[String]>,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        let reader = self.reader.clone();
        let index_cache = self.index_cache.clone();
        let selector = selector.clone();
        resolve_stream(reader, index_cache, selector, time_range, Some(projection))
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let reader = self.reader.clone();
        samples_stream(reader, request)
    }
}

// ---------------------------------------------------------------------------
// resolve()
// ---------------------------------------------------------------------------

/// Fan out selector resolution across overlapping buckets and emit one chunk
/// per non-empty bucket.
fn resolve_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
    selector: VectorSelector,
    time_range: TimeRange,
    projection: Option<Arc<[String]>>,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    async_stream_resolve(reader, index_cache, selector, time_range, projection)
}

/// Up to [`METADATA_STAGE_READAHEAD`] buckets resolve in parallel; emission
/// order is chronological (oldest first) via [`StreamExt::buffered`]. Empty
/// buckets are skipped.
fn async_stream_resolve<R: QueryReader + 'static>(
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
    selector: VectorSelector,
    time_range: TimeRange,
    projection: Option<Arc<[String]>>,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    stream::once(async move {
        let buckets = reader
            .list_buckets()
            .await
            .map_err(storage_err)?;

        let mut filtered: Vec<TimeBucket> = buckets
            .into_iter()
            .filter(|b| bucket_overlaps(*b, time_range))
            .collect();
        // Deterministic emission order so tests and downstream planner
        // see stable ordering across calls; chronological (oldest first).
        filtered.sort_by_key(|b| b.start);

        let selector = Arc::new(selector);
        Ok::<_, QueryError>(
            stream::iter(filtered.into_iter().map(move |bucket| {
                let reader = reader.clone();
                let index_cache = index_cache.clone();
                let selector = selector.clone();
                let projection = projection.clone();
                async move {
                    let started = std::time::Instant::now();
                    let chunk = match projection {
                        Some(projection) => {
                            resolve_one_bucket_projected(
                                reader.as_ref(),
                                &index_cache,
                                bucket,
                                &selector,
                                &projection,
                            )
                            .await
                        }
                        None => {
                            resolve_one_bucket(reader.as_ref(), &index_cache, bucket, &selector)
                                .await
                        }
                    };
                    let series = match &chunk {
                        Ok(Some(chunk)) => chunk.series.len() as u64,
                        _ => 0,
                    };
                    super::trace::record_bucket(
                        encode_bucket(bucket),
                        super::trace::BucketStage::Resolve,
                        started.elapsed().as_nanos() as u64,
                        series,
                    );
                    chunk
                }
            }))
            .buffered(METADATA_STAGE_READAHEAD),
        )
    })
    .try_flatten()
    .try_filter_map(|chunk| async move { Ok(chunk) })
    // Nothing follows the first error; dropping the stream cancels the
    // remaining in-flight buckets.
    .scan(false, |failed, item| {
        let emit = (!*failed).then(|| {
            *failed = item.is_err();
            item
        });
        async move { emit }
    })
}

/// `Ok(None)` when no series match. Forward-index entries populate the
/// per-`(bucket, series_id)` cache, which the samples path later reuses.
async fn resolve_one_bucket<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> Result<Option<ResolvedSeriesChunk>, QueryError> {
    let key = selector_util::selector_cache_key(selector);
    if let Some(resolution) = reader.cached_selector_resolution(&bucket, &key).await {
        return Ok(resolved_chunk(bucket, resolution, None));
    }

    let candidates = selector_util::find_candidates(reader, index_cache, &bucket, selector).await?;
    resolve_candidates(reader, index_cache, bucket, &key, candidates).await
}

/// The full resolution of `candidates`, cached under the selector's `key`.
async fn resolve_candidates<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: TimeBucket,
    key: &Arc<str>,
    candidates: Vec<SeriesId>,
) -> Result<Option<ResolvedSeriesChunk>, QueryError> {
    if candidates.is_empty() {
        reader
            .cache_selector_resolution(
                &bucket,
                key,
                Arc::new(CachedSeriesResolution {
                    series_ids: Arc::from([]),
                    labels: Arc::from([]),
                    fingerprints: None,
                }),
            )
            .await;
        return Ok(None);
    }

    let resolution = Arc::new(full_resolution(reader, index_cache, bucket, candidates).await?);
    reader
        .cache_selector_resolution(&bucket, key, resolution.clone())
        .await;
    Ok(resolved_chunk(bucket, resolution, None))
}

/// The labels of non-empty `candidates` from the forward index.
async fn full_resolution<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: TimeBucket,
    candidates: Vec<SeriesId>,
) -> Result<CachedSeriesResolution, QueryError> {
    // One batched forward-index read through the index cache, which the
    // storage reader serves with a key-range scan when the ids are dense.
    let slots = index_cache
        .forward_index_many(reader, &bucket, &candidates)
        .await
        .map_err(storage_err)?;

    let mut labels_vec: Vec<Labels> = Vec::with_capacity(candidates.len());
    for (sid, slot) in candidates.iter().zip(&slots) {
        let spec = slot.as_ref().as_ref().ok_or_else(|| {
            internal_err(format!(
                "series {} missing from forward index in bucket {:?}",
                sid, bucket
            ))
        })?;
        labels_vec.push(spec.labels.clone());
    }

    Ok(CachedSeriesResolution {
        series_ids: Arc::from(candidates),
        labels: Arc::from(labels_vec),
        fingerprints: None,
    })
}

/// [`resolve_one_bucket`] reading only the `projection` labels, from the
/// inverted index, with each series identified by its stored fingerprint.
/// That needs one metric name for the whole selector (handles carry it in
/// place of the forward-index entry) and a fingerprint for every candidate;
/// otherwise the bucket resolves in full and the caller projects it, which
/// yields the same chunk.
async fn resolve_one_bucket_projected<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: TimeBucket,
    selector: &VectorSelector,
    projection: &[String],
) -> Result<Option<ResolvedSeriesChunk>, QueryError> {
    let Some(metric_name) = selector_util::exact_metric_name(selector) else {
        return resolve_one_bucket(reader, index_cache, bucket, selector).await;
    };
    let metric_name: Arc<str> = Arc::from(metric_name);
    let key = selector_util::selector_cache_key(selector);
    let projected_key = selector_util::projected_cache_key(&key, projection);
    if let Some(resolution) = reader
        .cached_selector_resolution(&bucket, &projected_key)
        .await
    {
        return Ok(resolved_chunk(bucket, resolution, Some(&metric_name)));
    }
    if let Some(resolution) = reader.cached_selector_resolution(&bucket, &key).await {
        return Ok(resolved_chunk(bucket, resolution, None));
    }

    let wanted =
        selector_util::find_candidate_postings(reader, index_cache, &bucket, selector).await?;
    let candidates: Vec<SeriesId> = wanted.iter().collect();
    if candidates.is_empty() {
        return resolve_candidates(reader, index_cache, bucket, &key, candidates).await;
    }
    if crate::read_tuning::read_tuning().unproject_dense && dense(&candidates) {
        let full = full_resolution(reader, index_cache, bucket, candidates).await?;
        let resolution = Arc::new(CachedSeriesResolution {
            fingerprints: Some(full.labels.iter().map(Labels::series_fingerprint).collect()),
            labels: full
                .labels
                .iter()
                .map(|labels| labels.project(projection))
                .collect(),
            series_ids: full.series_ids,
        });
        reader
            .cache_selector_resolution(&bucket, &projected_key, resolution.clone())
            .await;
        return Ok(resolved_chunk(bucket, resolution, Some(&metric_name)));
    }
    let Some(fingerprints) = stored_fingerprints(reader, &bucket, &candidates).await? else {
        return resolve_candidates(reader, index_cache, bucket, &key, candidates).await;
    };
    let labels = projected_labels(
        reader,
        index_cache,
        &bucket,
        (&wanted, &candidates),
        projection,
        &metric_name,
    )
    .await?;
    let resolution = Arc::new(CachedSeriesResolution {
        series_ids: Arc::from(candidates),
        labels: Arc::from(labels),
        fingerprints: Some(Arc::from(fingerprints)),
    });
    reader
        .cache_selector_resolution(&bucket, &projected_key, resolution.clone())
        .await;
    Ok(resolved_chunk(bucket, resolution, Some(&metric_name)))
}

/// A projected resolution reads a postings list per value of each
/// projected label, however few candidates there are; a full one reads the
/// candidates' forward-index entries, a few cluster scans once their IDs
/// span at most this many per candidate.
const DENSE_MAX_SPAN_PER_CANDIDATE: u64 = 2;

/// Whether `candidates` (ascending) are dense enough to resolve in full.
fn dense(candidates: &[SeriesId]) -> bool {
    let (Some(first), Some(last)) = (candidates.first(), candidates.last()) else {
        return false;
    };
    u64::from(last - first) < candidates.len() as u64 * DENSE_MAX_SPAN_PER_CANDIDATE
}

/// Every candidate's stored fingerprint, or `None` when any is missing
/// (data written before fingerprints were stored).
async fn stored_fingerprints<R: QueryReader + ?Sized>(
    reader: &R,
    bucket: &TimeBucket,
    candidates: &[SeriesId],
) -> Result<Option<Vec<u128>>, QueryError> {
    Ok(reader
        .series_fingerprints(bucket, candidates)
        .await
        .map_err(storage_err)?
        .and_then(|fingerprints| fingerprints.into_iter().collect()))
}

/// Each candidate's `projection` labels (sorted names), read from the
/// label's postings; a candidate without a label lacks it here too.
/// `candidates` lists `wanted` in order.
async fn projected_labels<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: &TimeBucket,
    (wanted, candidates): (&RoaringBitmap, &[SeriesId]),
    projection: &[String],
    metric_name: &str,
) -> Result<Vec<Labels>, QueryError> {
    let postings = futures::future::try_join_all(
        projection
            .iter()
            .filter(|name| name.as_str() != METRIC_NAME)
            .map(|name| index_cache.label_postings(reader, bucket, name)),
    )
    .await
    .map_err(storage_err)?;
    let mut postings = postings.iter();
    let mut labels: Vec<Vec<Label>> = vec![Vec::new(); candidates.len()];
    for name in projection {
        if name == METRIC_NAME {
            for series in &mut labels {
                series.push(Label::metric_name(metric_name));
            }
            continue;
        }
        let values = postings
            .next()
            .expect("one postings list per projected label");
        for (value, series_ids) in values.iter() {
            for series_id in series_ids & wanted {
                if let Ok(position) = candidates.binary_search(&series_id) {
                    labels[position].push(Label::new(name.as_str(), value.as_str()));
                }
            }
        }
    }
    Ok(labels.into_iter().map(Labels::new).collect())
}

/// `metric_name`, when given, names every series: projected labels may not
/// carry `__name__`.
fn resolved_chunk(
    bucket: TimeBucket,
    resolution: Arc<CachedSeriesResolution>,
    metric_name: Option<&Arc<str>>,
) -> Option<ResolvedSeriesChunk> {
    if resolution.series_ids.is_empty() {
        return None;
    }
    let bucket_id = encode_bucket(bucket);
    let mut handles: Vec<ResolvedSeriesRef> = Vec::with_capacity(resolution.series_ids.len());
    // Candidates are ordered by series ID, so one metric's series tend to be
    // adjacent and share a name allocation.
    let mut name_of_run: Arc<str> = Arc::from("");
    for (&series_id, labels) in resolution.series_ids.iter().zip(resolution.labels.iter()) {
        let name = match metric_name {
            Some(name) => name.clone(),
            None => {
                let name = labels.metric_name();
                if *name_of_run != *name {
                    name_of_run = Arc::from(name);
                }
                name_of_run.clone()
            }
        };
        handles.push(ResolvedSeriesRef::new(bucket_id, series_id, name));
    }
    Some(ResolvedSeriesChunk {
        bucket_id,
        labels: resolution.labels.clone(),
        series: Arc::from(handles),
        fingerprints: resolution.fingerprints.clone(),
    })
}

// ---------------------------------------------------------------------------
// samples()
// ---------------------------------------------------------------------------

/// One [`SampleBatch`] per contiguous same-bucket run of the caller's series
/// slice; series spanning buckets yield one batch per bucket. Operators merge.
fn samples_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    request: SamplesRequest,
) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
    stream::once(async move {
        match build_sample_batches(reader.as_ref(), &request).await {
            Ok(batches) => batches.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(e) => vec![Err(e)],
        }
    })
    .flat_map(stream::iter)
}

/// Order-preserving: runs are dispatched concurrently up to
/// [`SAMPLE_STAGE_READAHEAD`] but yielded via `buffered` so each batch's
/// `series_range` indexes into the caller's `request.series`.
async fn build_sample_batches<R: QueryReader + ?Sized>(
    reader: &R,
    request: &SamplesRequest,
) -> Result<Vec<SampleBatch>, QueryError> {
    if request.series.is_empty() || request.time_range.is_empty() {
        return Ok(Vec::new());
    }

    // Group contiguous same-bucket series into runs, preserving the
    // caller's order inside each run.
    let runs = contiguous_bucket_runs(&request.series);

    let time_range = request.time_range;
    let out: Vec<SampleBatch> = stream::iter(runs.into_iter().map(|run| {
        let series = request.series.clone();
        async move { build_batch_for_run(reader, run, series, time_range).await }
    }))
    .buffered(SAMPLE_STAGE_READAHEAD)
    .try_collect()
    .await?;

    Ok(out)
}

/// Sample reads are issued as one [`QueryReader::samples_many`] per metric
/// name in the run, polled inside this task (never spawned), so storage can
/// serve a metric's contiguous series with a single range scan. Worst-case
/// in-flight batches = `SAMPLE_STAGE_READAHEAD`, still capped by the sharded
/// reader's I/O permits. Metric names ride on the `ResolvedSeriesRef`,
/// populated by `resolve_one_bucket` — no forward-index lookup here.
async fn build_batch_for_run<R: QueryReader + ?Sized>(
    reader: &R,
    run: BucketRun,
    series: Arc<[ResolvedSeriesRef]>,
    time_range: TimeRange,
) -> Result<SampleBatch, QueryError> {
    let bucket = decode_bucket(run.bucket_id)
        .ok_or_else(|| internal_err(format!("invalid bucket id: {}", run.bucket_id)))?;
    let started = std::time::Instant::now();

    let series_count = run.range.end - run.range.start;
    let mut block = SampleBlock::with_series_count(series_count);

    // The source's time_range is inclusive-exclusive. The existing
    // QueryReader::samples contract is inclusive/inclusive with
    // `timestamp > start_ms && timestamp <= end_ms`
    // (see mock + MiniQueryReader). Translate by passing
    // `start_ms = time_range.start_ms - 1` and
    // `end_ms = time_range.end_ms_exclusive - 1`.
    let start_ms = time_range.start_ms.saturating_sub(1);
    let end_ms = time_range.end_ms_exclusive.saturating_sub(1);

    let refs = &series[run.range.clone()];
    let mut order: Vec<usize> = (0..refs.len()).collect();
    order.sort_by(|&a, &b| refs[a].metric_name.cmp(&refs[b].metric_name));
    for group in order.chunk_by(|&a, &b| refs[a].metric_name == refs[b].metric_name) {
        let series_ids: Vec<SeriesId> = group
            .iter()
            .map(|&i| refs[i].series_id as SeriesId)
            .collect();
        let fetched = reader
            .samples_many(
                &bucket,
                &refs[group[0]].metric_name,
                &series_ids,
                start_ms,
                end_ms,
            )
            .await
            .map_err(storage_err)?;
        for (&col_idx, samples) in group.iter().zip(fetched) {
            fill_column(&mut block, col_idx, samples);
        }
    }
    super::trace::record_bucket(
        run.bucket_id,
        super::trace::BucketStage::Samples,
        started.elapsed().as_nanos() as u64,
        series_count as u64,
    );

    Ok(SampleBatch {
        series_range: run.range,
        samples: block,
    })
}

fn fill_column(block: &mut SampleBlock, col_idx: usize, samples: SeriesData) {
    let SeriesData {
        timestamps,
        values,
        histograms,
    } = samples;
    // Stale markers stay verbatim `f64::from_bits(STALE_NAN)` values (see
    // `crate::model::is_stale_nan`).
    append_column(&mut block.timestamps[col_idx], timestamps);
    append_column(&mut block.values[col_idx], values);
    let (hts_col, h_col) = (
        &mut block.histogram_timestamps[col_idx],
        &mut block.histograms[col_idx],
    );
    hts_col.reserve(histograms.len());
    h_col.reserve(histograms.len());
    for h in histograms {
        hts_col.push(h.timestamp_ms);
        h_col.push(Arc::new(h.histogram));
    }
}

/// Moves `column` into an empty `into` rather than copying it.
fn append_column<T>(into: &mut Vec<T>, mut column: Vec<T>) {
    if into.is_empty() {
        *into = column;
    } else {
        into.append(&mut column);
    }
}

/// A same-bucket contiguous sub-slice of `request.series`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketRun {
    bucket_id: u64,
    range: Range<usize>,
}

/// Preserves caller ordering so each run's `series_range` indexes directly
/// into the caller's request.
fn contiguous_bucket_runs(series: &[ResolvedSeriesRef]) -> Vec<BucketRun> {
    if series.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut start = 0usize;
    let mut current_bucket = series[0].bucket_id;
    for (idx, sref) in series.iter().enumerate().skip(1) {
        if sref.bucket_id != current_bucket {
            runs.push(BucketRun {
                bucket_id: current_bucket,
                range: start..idx,
            });
            current_bucket = sref.bucket_id;
            start = idx;
        }
    }
    runs.push(BucketRun {
        bucket_id: current_bucket,
        range: start..series.len(),
    });
    runs
}

// ---------------------------------------------------------------------------
// QueryError bridging
// ---------------------------------------------------------------------------

#[inline]
fn internal_err(msg: impl Into<String>) -> QueryError {
    QueryError::Internal(msg.into())
}

#[inline]
fn storage_err(error: impl std::fmt::Display) -> QueryError {
    QueryError::Storage(error.to_string())
}

// ---------------------------------------------------------------------------
// selector_util — pure selector/matcher helpers reused across resolve /
// estimate. Equivalent to the logic in `promql::selector` (which ties to
// `CachedQueryReader` and cannot be used here); the two should stay in
// sync behaviourally.
// ---------------------------------------------------------------------------

pub(crate) mod selector_util {
    use super::{
        IndexCache, Label, QueryError, QueryReader, SeriesId, TimeBucket, VectorSelector,
        internal_err, storage_err,
    };
    use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
    use regex_syntax::Parser;
    use regex_syntax::hir::{Class, Hir, HirKind};
    use roaring::RoaringBitmap;
    use std::sync::Arc;

    pub(crate) fn selector_cache_key(selector: &VectorSelector) -> Arc<str> {
        let name_matcher = selector
            .name
            .as_deref()
            .map(|name| Matcher::new(MatchOp::Equal, METRIC_NAME, name));
        crate::postings_cache::selector_key(
            name_matcher
                .iter()
                .chain(&selector.matchers.matchers)
                .map(|m| (m.name.as_str(), match_op_symbol(&m.op), m.value.as_str())),
        )
    }

    /// [`selector_cache_key`] for a resolution projected to `projection`,
    /// which never collides with a full resolution's key.
    pub(crate) fn projected_cache_key(key: &str, projection: &[String]) -> Arc<str> {
        use std::fmt::Write;
        let mut projected = format!("{key}by{}:", projection.len());
        for name in projection {
            let _ = write!(projected, "{}:{name};", name.len());
        }
        Arc::from(projected)
    }

    /// The one metric name every series of `selector` has, when it names
    /// one exactly.
    pub(crate) fn exact_metric_name(selector: &VectorSelector) -> Option<&str> {
        if !selector.matchers.or_matchers.is_empty() {
            return None;
        }
        selector
            .name
            .as_deref()
            .or_else(|| {
                selector
                    .matchers
                    .matchers
                    .iter()
                    .find(|m| m.name == METRIC_NAME && matches!(m.op, MatchOp::Equal))
                    .map(|m| m.value.as_str())
            })
            .filter(|name| !name.is_empty())
    }

    /// The exact strings a regex matches when it is a literal alternation
    /// (`value1|value2|…`), letting a matcher fetch those postings directly
    /// instead of enumerating the label's values. `None` otherwise.
    pub(super) fn regex_literals(pattern: &str) -> Option<Vec<String>> {
        parse_limited_regex(pattern).ok()
    }

    fn parse_limited_regex(pattern: &str) -> Result<Vec<String>, String> {
        let hir = Parser::new()
            .parse(pattern)
            .map_err(|e| format!("invalid regex pattern '{}': {}", pattern, e))?;
        match hir.kind() {
            HirKind::Alternation(alts) => {
                let mut out = Vec::with_capacity(alts.len());
                for alt in alts {
                    out.push(parse_literal(alt, pattern)?);
                }
                Ok(out)
            }
            // `regex-syntax` canonicalizes single-character alternations such
            // as `a|b` into a class. Expand bounded classes so this optimizer
            // detail does not make supported literal alternations fail.
            HirKind::Class(class) => parse_class(class, pattern),
            HirKind::Literal(_) | HirKind::Concat(_) => Ok(vec![parse_literal(&hir, pattern)?]),
            _ => Err(format!(
                "regex '{}' not supported (only literal alternations)",
                pattern
            )),
        }
    }

    fn parse_class(class: &Class, pattern: &str) -> Result<Vec<String>, String> {
        const MAX_LITERALS: usize = 256;
        let mut out = Vec::new();
        match class {
            Class::Unicode(class) => {
                for range in class.iter() {
                    for value in u32::from(range.start())..=u32::from(range.end()) {
                        let value = char::from_u32(value).ok_or_else(|| {
                            format!("invalid Unicode class in pattern: {pattern}")
                        })?;
                        out.push(value.to_string());
                        if out.len() > MAX_LITERALS {
                            return Err(format!(
                                "regex '{pattern}' expands beyond {MAX_LITERALS} literals"
                            ));
                        }
                    }
                }
            }
            Class::Bytes(class) => {
                for range in class.iter() {
                    for value in range.start()..=range.end() {
                        out.push(char::from(value).to_string());
                        if out.len() > MAX_LITERALS {
                            return Err(format!(
                                "regex '{pattern}' expands beyond {MAX_LITERALS} literals"
                            ));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn parse_literal(hir: &Hir, pattern: &str) -> Result<String, String> {
        match hir.kind() {
            HirKind::Empty => Err(format!("empty alternative in pattern: {}", pattern)),
            HirKind::Literal(l) => {
                String::from_utf8(l.0.to_vec()).map_err(|_| "non-UTF-8 literal".to_string())
            }
            HirKind::Concat(hirs) => {
                let mut s = String::new();
                for h in hirs {
                    s.push_str(&parse_literal(h, pattern)?);
                }
                Ok(s)
            }
            _ => Err(format!(
                "regex '{}' not supported (only literal alternations)",
                pattern
            )),
        }
    }

    /// How one matcher constrains the series set, following Prometheus'
    /// postings evaluation: a matcher that rejects the empty string only
    /// selects series carrying one of its accepted values; one that accepts
    /// it also selects series without the label, so it is applied by
    /// removing the series carrying a rejected value.
    struct MatcherPlan<'a> {
        label: &'a str,
        /// Select `values`' series when true; remove them when false.
        include: bool,
        values: PlanValues<'a>,
    }

    enum PlanValues<'a> {
        /// These exact values, fetched by term.
        Literals(Vec<String>),
        /// Values of the label for which `matcher.is_match` equals the
        /// plan's `include`, found by enumerating the label.
        Filter(&'a Matcher),
    }

    fn plan_matcher(m: &Matcher) -> MatcherPlan<'_> {
        let include = !m.is_match("");
        let literals = match (&m.op, include) {
            (MatchOp::Equal, true) | (MatchOp::NotEqual, false) => Some(vec![m.value.clone()]),
            (MatchOp::Re(_), true) => regex_literals(&m.value),
            // A rejected alternation value is excluded; "" is never a
            // stored label value.
            (MatchOp::NotRe(_), false) => regex_literals(&m.value)
                .map(|values| values.into_iter().filter(|v| !v.is_empty()).collect()),
            _ => None,
        };
        MatcherPlan {
            label: &m.name,
            include,
            values: literals.map_or(PlanValues::Filter(m), PlanValues::Literals),
        }
    }

    /// The series of `selector` within `bucket`, in ascending ID order.
    ///
    /// Every matcher — equality, negation, regex, and empty-string — is
    /// resolved against the inverted index, so no forward-index entry is
    /// read for a series the selector excludes.
    pub(crate) async fn find_candidates<R: QueryReader + ?Sized>(
        reader: &R,
        index_cache: &IndexCache,
        bucket: &TimeBucket,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        Ok(
            find_candidate_postings(reader, index_cache, bucket, selector)
                .await?
                .iter()
                .collect(),
        )
    }

    /// [`find_candidates`] as a postings set.
    pub(crate) async fn find_candidate_postings<R: QueryReader + ?Sized>(
        reader: &R,
        index_cache: &IndexCache,
        bucket: &TimeBucket,
        selector: &VectorSelector,
    ) -> Result<RoaringBitmap, QueryError> {
        let name_matcher = selector
            .name
            .as_deref()
            .map(|name| Matcher::new(MatchOp::Equal, METRIC_NAME, name));
        let plans: Vec<MatcherPlan<'_>> = name_matcher
            .iter()
            .chain(&selector.matchers.matchers)
            .map(plan_matcher)
            .collect();
        if !plans.iter().any(|plan| plan.include) {
            return Err(internal_err(
                "vector selector must contain at least one non-empty matcher".to_string(),
            ));
        }
        let key = selector_cache_key(selector);
        if let Some(hit) = reader.cached_selector(bucket, &key).await {
            return Ok(hit.as_ref().clone());
        }

        let sets = futures::future::try_join_all(
            plans
                .iter()
                .map(|plan| plan_postings(reader, index_cache, bucket, plan)),
        )
        .await?;
        let mut included = plans.iter().zip(&sets).filter(|(plan, _)| plan.include);
        let (_, first) = included.next().expect("checked above");
        let mut result = first.clone();
        for (_, set) in included {
            result &= set;
        }
        for (_, set) in plans.iter().zip(&sets).filter(|(plan, _)| !plan.include) {
            result -= set;
        }
        reader.cache_selector(bucket, &key, &result).await;
        Ok(result)
    }

    fn match_op_symbol(op: &MatchOp) -> &'static str {
        match op {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::Re(_) => "=~",
            MatchOp::NotRe(_) => "!~",
        }
    }

    /// Union of the postings of the values `plan` names.
    async fn plan_postings<R: QueryReader + ?Sized>(
        reader: &R,
        index_cache: &IndexCache,
        bucket: &TimeBucket,
        plan: &MatcherPlan<'_>,
    ) -> Result<RoaringBitmap, QueryError> {
        let mut out = RoaringBitmap::new();
        match &plan.values {
            PlanValues::Literals(values) => {
                let terms: Vec<Label> = values
                    .iter()
                    .map(|value| Label::new(plan.label, value.as_str()))
                    .collect();
                let postings = futures::future::try_join_all(
                    terms
                        .iter()
                        .map(|term| index_cache.inverted_index_term(reader, bucket, term)),
                )
                .await
                .map_err(storage_err)?;
                for p in postings.iter().filter_map(|p| p.as_ref().as_ref()) {
                    out |= p;
                }
            }
            PlanValues::Filter(matcher) => {
                let all = index_cache
                    .label_postings(reader, bucket, plan.label)
                    .await
                    .map_err(storage_err)?;
                for (value, p) in all.iter() {
                    if matcher.is_match(value) == plan.include {
                        out |= p;
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
