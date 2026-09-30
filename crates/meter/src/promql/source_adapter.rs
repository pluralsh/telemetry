//! Adapter from the per-bucket [`QueryReader`] surface to [`SeriesSource`].
//!
//! Fans out per-bucket selector resolution / sample streaming. Callers see
//! single streams; cross-bucket stitching lives inside this source.
//!
//! A series spanning multiple buckets yields one [`SampleBatch`] per bucket
//! in bucket-timestamp order; operators merge across buckets. Selector
//! matcher logic lives in [`selector_util`].

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use futures::stream::{self, StreamExt, TryStreamExt};
use promql_parser::parser::VectorSelector;

use crate::model::{Label, Labels, SeriesData, SeriesId, TimeBucket};
use crate::query::QueryReader;

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
fn encode_bucket(bucket: TimeBucket) -> u64 {
    ((bucket.start as u64) << 8) | (bucket.size as u64)
}

/// Inverse of [`encode_bucket`]. `None` only on defensive size=0 guard.
#[inline]
fn decode_bucket(bucket_id: u64) -> Option<TimeBucket> {
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
        resolve_stream(reader, index_cache, selector, time_range)
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
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    async_stream_resolve(reader, index_cache, selector, time_range)
}

/// Up to [`METADATA_STAGE_READAHEAD`] buckets resolve in parallel; emission
/// order is chronological (oldest first) via [`StreamExt::buffered`]. Empty
/// buckets are skipped.
fn async_stream_resolve<R: QueryReader + 'static>(
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    stream::once(async move {
        let buckets = reader
            .list_buckets()
            .await
            .map_err(|e| internal_err(e.to_string()))?;

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
                async move {
                    resolve_one_bucket(reader.as_ref(), &index_cache, bucket, &selector).await
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
    let candidates = selector_util::find_candidates(reader, index_cache, &bucket, selector).await?;
    if candidates.is_empty() {
        return Ok(None);
    }

    // One batched forward-index read through the index cache, which the
    // storage reader serves with a key-range scan when the ids are dense.
    let mut unique_ids = candidates.clone();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    let slots = index_cache
        .forward_index_many(reader, &bucket, &unique_ids)
        .await
        .map_err(|e| internal_err(e.to_string()))?;
    let specs: HashMap<SeriesId, crate::index::SeriesSpec> = unique_ids
        .into_iter()
        .zip(slots)
        .filter_map(|(sid, slot)| slot.as_ref().as_ref().map(|spec| (sid, spec.clone())))
        .collect();

    // Apply negative / empty-string matchers using the materialised specs.
    let needs_filter = selector_util::has_negative_matchers(selector)
        || selector_util::has_empty_string_matchers(selector);
    let filtered_ids: Vec<SeriesId> = if needs_filter {
        selector_util::apply_post_filters_map(&specs, candidates, selector)?
    } else {
        candidates
    };

    if filtered_ids.is_empty() {
        return Ok(None);
    }

    let bucket_id = encode_bucket(bucket);
    let mut labels_vec: Vec<Labels> = Vec::with_capacity(filtered_ids.len());
    let mut handles: Vec<ResolvedSeriesRef> = Vec::with_capacity(filtered_ids.len());
    for sid in &filtered_ids {
        let spec = specs.get(sid).ok_or_else(|| {
            internal_err(format!(
                "series {} missing from forward index in bucket {:?}",
                sid, bucket
            ))
        })?;
        let mut labs = spec.labels.clone();
        labs.sort();
        let metric_name: Arc<str> = labs
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| Arc::from(l.value.as_str()))
            .unwrap_or_else(|| Arc::from(""));
        labels_vec.push(Labels::new(labs));
        handles.push(ResolvedSeriesRef::new(bucket_id, *sid, metric_name));
    }

    Ok(Some(ResolvedSeriesChunk {
        bucket_id,
        labels: Arc::from(labels_vec),
        series: Arc::from(handles),
    }))
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
            .map_err(|e| internal_err(e.to_string()))?;
        for (&col_idx, samples) in group.iter().zip(fetched) {
            fill_column(&mut block, col_idx, samples);
        }
    }

    Ok(SampleBatch {
        series_range: run.range,
        samples: block,
    })
}

fn fill_column(block: &mut SampleBlock, col_idx: usize, samples: SeriesData) {
    let SeriesData { floats, histograms } = samples;
    let (ts_col, val_col) = (&mut block.timestamps[col_idx], &mut block.values[col_idx]);
    ts_col.reserve(floats.len());
    val_col.reserve(floats.len());
    // Preserve stale markers verbatim as STALE_NAN — the
    // storage layer encodes them as `f64::from_bits(STALE_NAN)`,
    // which survives the unmodified `s.value` copy below
    // (see `crate::model::is_stale_nan` and RFC 0007 source→caller
    // contract).
    for s in floats {
        ts_col.push(s.timestamp_ms);
        val_col.push(s.value);
    }
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

/// Free helper (not `impl From`) so both `String` and `Error::to_string()`
/// flow through one path.
#[inline]
fn internal_err(msg: impl Into<String>) -> QueryError {
    QueryError::Internal(msg.into())
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
        internal_err,
    };
    use crate::index::ForwardIndexLookup;
    use promql_parser::label::{METRIC_NAME, MatchOp};
    use regex_syntax::Parser;
    use regex_syntax::hir::{Class, Hir, HirKind};
    use std::collections::HashSet;

    /// Parses `value1|value2|…` into literal alternatives.
    pub(super) fn parse_limited_regex(pattern: &str) -> Result<Vec<String>, String> {
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

    pub(crate) fn has_negative_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::NotEqual | MatchOp::NotRe(_)))
    }

    pub(crate) fn has_empty_string_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::Equal) && m.value.is_empty())
    }

    /// Mirrors `promql::selector::find_candidates_with_reader` but consults
    /// the reader directly (no `CachedQueryReader`).
    pub(crate) async fn find_candidates<R: QueryReader + ?Sized>(
        reader: &R,
        index_cache: &IndexCache,
        bucket: &TimeBucket,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut and_terms: Vec<Label> = Vec::new();
        let mut or_groups: Vec<Vec<Label>> = Vec::new();

        if let Some(name) = &selector.name {
            and_terms.push(Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            });
        }

        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::Equal if !m.value.is_empty() => and_terms.push(Label {
                    name: m.name.clone(),
                    value: m.value.clone(),
                }),
                MatchOp::Equal => {}
                MatchOp::Re(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    let or_terms: Vec<Label> = values
                        .into_iter()
                        .map(|v| Label {
                            name: m.name.clone(),
                            value: v,
                        })
                        .collect();
                    or_groups.push(or_terms);
                }
                _ => {}
            }
        }

        // No positive terms → either "empty string matcher only" (fall
        // back to metric-name scan) or "nothing to do".
        if and_terms.is_empty() && or_groups.is_empty() {
            if !has_empty_string_matchers(selector) {
                return Ok(Vec::new());
            }
            if let Some(name) = &selector.name {
                let metric_term = Label {
                    name: METRIC_NAME.to_string(),
                    value: name.clone(),
                };
                let inv = index_cache
                    .inverted_index(reader, bucket, std::slice::from_ref(&metric_term))
                    .await
                    .map_err(|e| internal_err(e.to_string()))?;
                let res: Vec<SeriesId> = inv.intersect(vec![metric_term]).iter().collect();
                return Ok(res);
            }
            return Err(internal_err(
                "must specify a metric name when using empty label matcher".to_string(),
            ));
        }

        let all_terms: Vec<Label> = or_groups
            .iter()
            .flat_map(|t| t.iter().cloned())
            .chain(and_terms.iter().cloned())
            .collect();
        let inv = index_cache
            .inverted_index(reader, bucket, &all_terms)
            .await
            .map_err(|e| internal_err(e.to_string()))?;

        let mut result_set: HashSet<SeriesId> = if !and_terms.is_empty() {
            inv.intersect(and_terms.clone()).iter().collect()
        } else {
            HashSet::new()
        };

        for or_terms in &or_groups {
            let mut or_result: HashSet<SeriesId> = HashSet::new();
            for term in or_terms {
                let per_term = inv.intersect(vec![term.clone()]);
                or_result.extend(per_term.iter());
            }
            if and_terms.is_empty() && result_set.is_empty() {
                result_set = or_result;
            } else {
                result_set = result_set.intersection(&or_result).cloned().collect();
            }
        }

        let mut v: Vec<SeriesId> = result_set.into_iter().collect();
        v.sort();
        Ok(v)
    }

    /// Matches the post-filter block in `promql::selector::evaluate_selector_with_reader`.
    pub(crate) fn apply_post_filters(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        if has_negative_matchers(selector) {
            out = apply_negative(forward, out, selector)?;
        }
        if has_empty_string_matchers(selector) {
            out = apply_empty_string(forward, out, selector);
        }
        Ok(out)
    }

    /// HashMap-backed variant of [`apply_post_filters`] for the resolve path.
    pub(super) fn apply_post_filters_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        if has_negative_matchers(selector) {
            out = apply_negative_map(specs, out, selector)?;
        }
        if has_empty_string_matchers(selector) {
            out = apply_empty_string_map(specs, out, selector);
        }
        Ok(out)
    }

    fn apply_negative_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::NotEqual => {
                    out.retain(|id| {
                        specs
                            .get(id)
                            .map(|spec| !has_label(&spec.labels, &m.name, &m.value))
                            .unwrap_or(false)
                    });
                }
                MatchOp::NotRe(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    out.retain(|id| {
                        specs
                            .get(id)
                            .map(|spec| !values.iter().any(|v| has_label(&spec.labels, &m.name, v)))
                            .unwrap_or(false)
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn apply_empty_string_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Vec<SeriesId> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            if matches!(m.op, MatchOp::Equal) && m.value.is_empty() {
                out.retain(|id| {
                    specs
                        .get(id)
                        .map(|spec| !has_label_with_non_empty_value(&spec.labels, &m.name))
                        .unwrap_or(false)
                });
            }
        }
        out
    }

    fn apply_negative(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::NotEqual => {
                    out.retain(|id| {
                        forward
                            .spec_matches(id, &|spec| !has_label(&spec.labels, &m.name, &m.value))
                    });
                }
                MatchOp::NotRe(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    out.retain(|id| {
                        forward.spec_matches(id, &|spec| {
                            !values.iter().any(|v| has_label(&spec.labels, &m.name, v))
                        })
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn apply_empty_string(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Vec<SeriesId> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            if matches!(m.op, MatchOp::Equal) && m.value.is_empty() {
                out.retain(|id| {
                    forward.spec_matches(id, &|spec| {
                        !has_label_with_non_empty_value(&spec.labels, &m.name)
                    })
                });
            }
        }
        out
    }

    fn has_label(labels: &[Label], name: &str, value: &str) -> bool {
        labels.iter().any(|l| l.name == name && l.value == value)
    }

    fn has_label_with_non_empty_value(labels: &[Label], name: &str) -> bool {
        labels.iter().any(|l| l.name == name && !l.value.is_empty())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
