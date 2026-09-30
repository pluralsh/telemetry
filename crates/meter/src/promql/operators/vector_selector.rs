//! `VectorSelectorOp` — the storage leaf for PromQL instant vectors (the
//! plain `metric{labels=...}` selector, no brackets). It turns raw
//! samples from a [`SeriesSource`] into [`StepBatch`]es ready for
//! downstream operators to consume.
//!
//! This is where all the PromQL semantics for picking "the sample at
//! step `t`" live: `lookback_delta`, the `@` modifier, `offset`, and
//! `STALE_NAN` handling. The underlying [`SeriesSource`] is deliberately
//! PromQL-unaware and just returns raw samples in an absolute time
//! window; everything else happens here.
//!
//! Per-step semantics:
//! ```text
//!   pin         = @ value when @ is set, else t
//!   effective   = pin - offset               (Offset::Pos subtracts; Neg adds)
//!   window      = (effective - lookback, effective]
//!   sample      = latest non-STALE_NAN sample in window
//! ```
//!
//! `STALE_NAN` is treated as absence — stricter than v1's pipeline.rs
//! but matches Prometheus' "stale marker terminates a series" rule.
//!
//! Output is tiled: one [`StepBatch`] per `(series_chunk, step_chunk)`
//! rectangle (default ~64 steps × 512 series) so a single batch's
//! allocation stays bounded regardless of query width. Samples are
//! drained from the source stream on demand; everything
//! `series_count × step_count`-scaled routes through
//! [`MemoryReservation::try_grow`].
//!
//! [`SeriesSource`]: crate::promql::source::SeriesSource

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::StreamExt;
use promql_parser::parser::{AtModifier, Offset};

use crate::histogram::FloatHistogram;
use crate::model::is_stale_nan;
use crate::promql::timestamp::Timestamp;

use super::super::batch::{BitSet, HistogramCells, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::{
    ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use super::super::trace;

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_STEP_CHUNK: usize = 64;
pub(crate) const DEFAULT_SERIES_CHUNK: usize = 512;
/// Prometheus default.
pub(crate) const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1_000;

// ---------------------------------------------------------------------------
// Batch shape
// ---------------------------------------------------------------------------

/// Tile dimensions; defaults target an L2-sized rectangle.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchShape {
    pub(crate) step_chunk: usize,
    pub(crate) series_chunk: usize,
}

impl BatchShape {
    pub(crate) fn new(step_chunk: usize, series_chunk: usize) -> Self {
        assert!(step_chunk > 0, "step_chunk must be > 0");
        assert!(series_chunk > 0, "series_chunk must be > 0");
        Self {
            step_chunk,
            series_chunk,
        }
    }
}

impl Default for BatchShape {
    fn default() -> Self {
        Self::new(DEFAULT_STEP_CHUNK, DEFAULT_SERIES_CHUNK)
    }
}

// ---------------------------------------------------------------------------
// Memory-guarded per-batch buffers
// ---------------------------------------------------------------------------

/// RAII wrapper around a batch's value / validity allocations. Reserves on
/// [`Self::allocate`] and releases on drop or [`Self::finish`]. Downstream
/// operators re-reserve if they need to hold onto the emitted batch.
struct BatchBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl BatchBuffers {
    /// Returns `QueryError::MemoryLimit` without allocating if the reservation
    /// rejects the grow.
    fn allocate(reservation: &MemoryReservation, len: usize) -> Result<Self, QueryError> {
        let bytes = cell_bytes(len);
        reservation.try_grow(bytes)?;
        Self::fill(reservation.clone(), bytes, len)
    }

    fn fill(reservation: MemoryReservation, bytes: usize, len: usize) -> Result<Self, QueryError> {
        // NaN-fill so accidental reads of invalid cells surface as NaN
        // rather than stale stack data. Callers must consult validity
        // before reading `values`.
        let values = vec![f64::NAN; len];
        let validity = BitSet::with_len(len);
        Ok(Self {
            reservation,
            bytes,
            values,
            validity,
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::take(&mut self.validity);
        // Release now: the caller owns the bytes via the returned vectors.
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for BatchBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

#[inline]
fn cell_bytes(len: usize) -> usize {
    let values = len.saturating_mul(std::mem::size_of::<f64>());
    let words = len.div_ceil(64);
    let validity = words.saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

#[inline]
fn samples_bytes(n: usize) -> usize {
    n.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>())
}

/// Approximate heap footprint of one histogram sample (timestamp, `Arc`
/// header and the bucket vectors).
#[inline]
pub(crate) fn histogram_sample_bytes(h: &FloatHistogram) -> usize {
    let buckets = h.positive.len() + h.negative.len() + h.custom_values.len();
    std::mem::size_of::<i64>()
        + std::mem::size_of::<FloatHistogram>()
        + 2 * std::mem::size_of::<usize>()
        + buckets.saturating_mul(std::mem::size_of::<crate::histogram::Bucket>())
}

/// Flatten a chunk's per-series source handles into one request, returning
/// each request entry's series offset within the chunk.
///
/// Entries are ordered by `(bucket_id, series_id)` so every bucket forms one
/// contiguous run the source can fetch in a single batch (and, in storage,
/// a single key-range scan) rather than one run per series. Each series'
/// handles are already sorted by that key (see `resolve_leaf`), so a stable
/// sort keeps their relative order and samples still arrive per series in
/// bucket order.
pub(crate) fn flatten_bucket_major(
    request_series: &[Arc<[ResolvedSeriesRef]>],
) -> (Vec<ResolvedSeriesRef>, Vec<usize>) {
    let mut entries: Vec<(ResolvedSeriesRef, usize)> = Vec::new();
    for (series_off, series_refs) in request_series.iter().enumerate() {
        debug_assert!(
            !series_refs.is_empty(),
            "every logical series must have at least one source handle",
        );
        entries.extend(series_refs.iter().map(|sref| (sref.clone(), series_off)));
    }
    entries.sort_by_key(|(sref, _)| (sref.bucket_id, sref.series_id));
    entries.into_iter().unzip()
}

// ---------------------------------------------------------------------------
// Lookback / @ / offset resolution
// ---------------------------------------------------------------------------

/// Per-step `pin - offset` times. The operator looks up
/// `(times[k] - lookback, times[k]]`. `@` / `@ start()` / `@ end()` collapse
/// all entries to the pinned value.
#[derive(Debug, Clone)]
struct EffectiveTimes {
    times: Arc<[i64]>,
}

impl EffectiveTimes {
    fn compute(
        step_timestamps: &[i64],
        grid: &StepGrid,
        at: Option<&AtModifier>,
        offset: Option<&Offset>,
    ) -> Self {
        // Phase 1: pin.
        let times: Vec<i64> = match at {
            Some(AtModifier::At(t)) => {
                let pin = Timestamp::from(*t).as_millis();
                vec![pin; step_timestamps.len()]
            }
            Some(AtModifier::Start) => vec![grid.start_ms; step_timestamps.len()],
            Some(AtModifier::End) => vec![grid.end_ms; step_timestamps.len()],
            None => step_timestamps.to_vec(),
        };
        // Phase 2: apply offset (matches evaluator.rs:1143-1156).
        let offset_ms = match offset {
            Some(Offset::Pos(d)) => -(d.as_millis() as i64),
            Some(Offset::Neg(d)) => d.as_millis() as i64,
            None => 0,
        };
        let shifted: Vec<i64> = times
            .into_iter()
            .map(|t| t.saturating_add(offset_ms))
            .collect();
        Self {
            times: Arc::from(shifted),
        }
    }

    #[inline]
    fn get(&self, step_idx: usize) -> i64 {
        self.times[step_idx]
    }

    /// Minimum and maximum effective time across all steps. Used to size
    /// the `SamplesRequest` time window so the source returns enough samples
    /// for every step's lookback window.
    fn time_range_with_lookback(&self, lookback_ms: i64) -> TimeRange {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for &t in self.times.iter() {
            if t < min {
                min = t;
            }
            if t > max {
                max = t;
            }
        }
        if min == i64::MAX {
            // Empty grid — return a zero window. The adapter treats
            // `end <= start` as empty and short-circuits.
            return TimeRange::new(0, 0);
        }
        // Lookback window is (t - lookback, t]; the source needs samples
        // in `[min - lookback + 1, max + 1)`. We use `+ 1` on the exclusive
        // end so that samples at exactly `max` are included (TimeRange is
        // inclusive-exclusive).
        let start = min.saturating_sub(lookback_ms).saturating_add(1);
        let end = max.saturating_add(1);
        TimeRange::new(start, end)
    }
}

// ---------------------------------------------------------------------------
// Per-chunk sample state
// ---------------------------------------------------------------------------

/// Pre-materialised per-series-chunk sample columns.
struct ChunkSamples {
    reservation: MemoryReservation,
    bytes: usize,
    /// Indexed by chunk-local series offset (0..chunk_len).
    timestamps: Vec<Vec<i64>>,
    values: Vec<Vec<f64>>,
    histogram_timestamps: Vec<Vec<i64>>,
    histograms: Vec<Vec<Arc<FloatHistogram>>>,
}

impl ChunkSamples {
    fn new(reservation: MemoryReservation, chunk_len: usize) -> Self {
        fn columns<T>(n: usize) -> Vec<Vec<T>> {
            (0..n).map(|_| Vec::new()).collect()
        }
        Self {
            reservation,
            bytes: 0,
            timestamps: columns(chunk_len),
            values: columns(chunk_len),
            histogram_timestamps: columns(chunk_len),
            histograms: columns(chunk_len),
        }
    }

    /// `request_to_series[request_idx]` maps each sample column back to its
    /// logical output series (a logical series may span several request refs).
    fn absorb(
        &mut self,
        batch: SampleBatch,
        request_to_series: &[usize],
    ) -> Result<(), QueryError> {
        let mut total_new = 0usize;
        for col in batch.samples.timestamps.iter() {
            total_new = total_new.saturating_add(col.len());
        }
        let mut bytes = samples_bytes(total_new);
        for col in batch.samples.histograms.iter() {
            for h in col {
                bytes = bytes.saturating_add(histogram_sample_bytes(h));
            }
        }
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);

        let samples = batch.samples;
        for (block_idx, (((mut ts_col, mut val_col), mut hts_col), mut h_col)) in samples
            .timestamps
            .into_iter()
            .zip(samples.values)
            .zip(samples.histogram_timestamps)
            .zip(samples.histograms)
            .enumerate()
        {
            let request_idx = batch.series_range.start + block_idx;
            let local_idx = request_to_series[request_idx];
            // Append rather than replace — a series may span several
            // SampleBatches (one per bucket for cross-bucket series).
            self.timestamps[local_idx].append(&mut ts_col);
            self.values[local_idx].append(&mut val_col);
            self.histogram_timestamps[local_idx].append(&mut hts_col);
            self.histograms[local_idx].append(&mut h_col);
        }
        Ok(())
    }
}

impl Drop for ChunkSamples {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Operator state machine
// ---------------------------------------------------------------------------

type SampleStream<'a> = Pin<Box<dyn Stream<Item = Result<SampleBatch, QueryError>> + Send + 'a>>;

enum State<'a> {
    Init,
    /// Series chunk being hydrated from the source stream.
    LoadingChunk {
        chunk_start: usize,
        chunk_len: usize,
        #[allow(clippy::type_complexity)]
        future: Pin<Box<dyn Future<Output = Result<ChunkSamples, QueryError>> + Send + 'a>>,
    },
    /// Chunk loaded; step chunks emit one batch per poll.
    Emitting {
        chunk_start: usize,
        chunk_len: usize,
        samples: Box<ChunkSamples>,
        next_step_chunk_start: usize,
    },
    Done,
    /// Terminal error; subsequent polls return `Ready(None)`.
    Errored,
    /// Transient placeholder used while swapping state inside `next()`.
    Transitioning,
}

// ---------------------------------------------------------------------------
// Operator struct
// ---------------------------------------------------------------------------

/// Storage leaf for PromQL instant vectors (`metric{labels=...}`). Pulls
/// raw samples from a [`SeriesSource`] and applies lookback / `@` /
/// `offset` / stale-marker semantics to emit tiled [`StepBatch`]es.
pub(crate) struct VectorSelectorOp<'a, S: SeriesSource + 'a> {
    // Plan-time inputs ------------------------------------------------------
    source: Arc<S>,
    request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    effective_times: EffectiveTimes,
    lookback_ms: i64,
    shape: BatchShape,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    state: State<'a>,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> VectorSelectorOp<'a, S> {
    /// `request_series[i]` is the group of bucket-local source handles for
    /// logical series `i` (deduplicated by fingerprint).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<S>,
        series: Arc<SeriesSchema>,
        request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        lookback_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> Self {
        assert_eq!(
            series.len(),
            request_series.len(),
            "series roster and request_series must be length-aligned",
        );
        let step_timestamps: Arc<[i64]> = Arc::from(
            (0..grid.step_count)
                .map(|k| grid.start_ms + (k as i64) * grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let effective_times =
            EffectiveTimes::compute(&step_timestamps, &grid, at.as_ref(), offset.as_ref());
        let schema = OperatorSchema::new(SchemaRef::Static(series), grid);
        Self {
            source,
            request_series,
            schema,
            step_timestamps,
            effective_times,
            lookback_ms,
            shape,
            reservation,
            state: State::Init,
        }
    }

    fn total_series(&self) -> usize {
        self.request_series.len()
    }

    fn chunk_request(&self, chunk_start: usize, chunk_end: usize) -> (SamplesRequest, Vec<usize>) {
        let (flat, request_to_series) =
            flatten_bucket_major(&self.request_series[chunk_start..chunk_end]);
        let window = self
            .effective_times
            .time_range_with_lookback(self.lookback_ms);
        (
            SamplesRequest::new(Arc::from(flat), window),
            request_to_series,
        )
    }

    fn start_chunk_load(&mut self, chunk_start: usize) -> State<'a>
    where
        S: 'a,
    {
        let chunk_end = (chunk_start + self.shape.series_chunk).min(self.total_series());
        let chunk_len = chunk_end - chunk_start;
        let (request, request_to_series) = self.chunk_request(chunk_start, chunk_end);
        let source = self.source.clone();
        let reservation = self.reservation.clone();

        let future = Box::pin(async move {
            let mut samples = ChunkSamples::new(reservation, chunk_len);
            let stream = source.samples(request);
            let mut stream: SampleStream<'_> = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let batch = item?;
                // `absorb` copies per-series (ts, value) pairs into this
                // chunk's columnar staging vectors. Pure CPU, measured
                // separately from the source's I/O wait (which is already
                // attributed to `object_storage_fetch` / `deserialize`).
                trace::record_subphase_sync("absorb", || {
                    samples.absorb(batch, &request_to_series)
                })?;
            }
            // Silence "unused variable" when chunk_start isn't read
            // by absorb — it's threaded into the state machine as an
            // identifier for the series slice we're hydrating.
            let _ = chunk_start;
            Ok(samples)
        });

        State::LoadingChunk {
            chunk_start,
            chunk_len,
            future,
        }
    }

    fn build_batch(
        &self,
        chunk_start: usize,
        chunk_len: usize,
        samples: &ChunkSamples,
        step_chunk_start: usize,
    ) -> Result<StepBatch, QueryError> {
        let grid = &self.schema.step_grid;
        let step_chunk_end = (step_chunk_start + self.shape.step_chunk).min(grid.step_count);
        let step_count = step_chunk_end - step_chunk_start;
        let cell_count = step_count * chunk_len;

        let mut buffers = BatchBuffers::allocate(&self.reservation, cell_count)?;
        // Per-cell source-sample timestamp (0 is a safe placeholder when
        // the cell is absent — callers must consult validity before
        // reading). RFC 0007 §6.3.7 / at_modifier.test `timestamp()`
        // semantics require the matching sample's actual timestamp, not
        // the step timestamp.
        let mut source_timestamps = vec![0i64; cell_count];

        // Forward two-pointer per series: `effective_times` is non-decreasing
        // across steps (monotonic grid, or constant when `@` is set), and
        // each series' timestamps are ascending, so a cursor advancing past
        // samples with `ts <= window_hi` is correct across all steps. The
        // candidate is always `cursor - 1` (the newest in-window sample).
        // This turns the old O(step_count × series × ts.len()) reverse scan
        // into O(step_count × series + Σ ts.len()).
        let mut cursors = vec![0usize; chunk_len];
        let mut hist_cursors = vec![0usize; chunk_len];
        let has_histograms = samples.histograms.iter().any(|col| !col.is_empty());
        let mut histogram_cells: HistogramCells = if has_histograms {
            vec![None; cell_count]
        } else {
            Vec::new()
        };

        for step_off in 0..step_count {
            let step_idx = step_chunk_start + step_off;
            let effective = self.effective_times.get(step_idx);
            let window_lo = effective.saturating_sub(self.lookback_ms); // exclusive
            let window_hi = effective; // inclusive

            for (series_off, cursor) in cursors.iter_mut().enumerate() {
                let ts = &samples.timestamps[series_off];
                let vs = &samples.values[series_off];
                while *cursor < ts.len() && ts[*cursor] <= window_hi {
                    *cursor += 1;
                }
                let float_idx = (*cursor > 0 && ts[*cursor - 1] > window_lo).then(|| *cursor - 1);

                let hist_idx = if has_histograms {
                    let hts = &samples.histogram_timestamps[series_off];
                    let hc = &mut hist_cursors[series_off];
                    while *hc < hts.len() && hts[*hc] <= window_hi {
                        *hc += 1;
                    }
                    (*hc > 0 && hts[*hc - 1] > window_lo).then(|| *hc - 1)
                } else {
                    None
                };

                let cell = step_off * chunk_len + series_off;
                // The newest in-window sample of either kind wins; a
                // histogram wins a timestamp tie, matching storage.
                match (float_idx, hist_idx) {
                    (Some(fi), Some(hi))
                        if samples.histogram_timestamps[series_off][hi] >= ts[fi] =>
                    {
                        histogram_cells[cell] = Some(samples.histograms[series_off][hi].clone());
                        source_timestamps[cell] = samples.histogram_timestamps[series_off][hi];
                    }
                    (None, Some(hi)) => {
                        histogram_cells[cell] = Some(samples.histograms[series_off][hi].clone());
                        source_timestamps[cell] = samples.histogram_timestamps[series_off][hi];
                    }
                    (Some(fi), _) => {
                        let v = vs[fi];
                        if is_stale_nan(v) {
                            // STALE_NAN terminates the lookback; treat the
                            // cell as absent. Per-step; the cursor stays put
                            // so later steps may select a newer sample.
                            continue;
                        }
                        buffers.values[cell] = v;
                        buffers.validity.set(cell);
                        source_timestamps[cell] = ts[fi];
                    }
                    (None, None) => {}
                }
            }
        }

        let (values, validity) = buffers.finish();
        let series_range = chunk_start..(chunk_start + chunk_len);
        let step_range = step_chunk_start..step_chunk_end;
        let batch = StepBatch::new(
            self.step_timestamps.clone(),
            step_range,
            self.schema.series.clone(),
            series_range,
            values,
            validity,
        )
        .with_source_timestamps(Arc::from(source_timestamps));
        Ok(if has_histograms {
            batch.with_histograms(histogram_cells)
        } else {
            batch
        })
    }
}

impl<S: SeriesSource + Send + Sync + 'static> Operator for VectorSelectorOp<'static, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        loop {
            // Swap out the current state so we can own it across arms.
            let state = std::mem::replace(&mut self.state, State::Transitioning);
            match state {
                State::Init => {
                    if self.total_series() == 0 || self.schema.step_grid.step_count == 0 {
                        self.state = State::Done;
                        return Poll::Ready(None);
                    }
                    // `request_series` is indexed by logical (fingerprint-
                    // deduped) series; each entry holds one ref per bucket
                    // the series lives in. The outer length is the distinct
                    // fingerprint count; the sum of inner lengths is the
                    // total number of bucket-local series handles fetched.
                    let unique_fingerprints = self.request_series.len() as u64;
                    let series_selected: u64 = self
                        .request_series
                        .iter()
                        .map(|refs| refs.len() as u64)
                        .sum();
                    trace::record_counter("series_selected", series_selected);
                    trace::record_counter("unique_fingerprints", unique_fingerprints);
                    let _g = trace::Scope::enter("start_chunk_load");
                    self.state = self.start_chunk_load(0);
                }
                State::LoadingChunk {
                    chunk_start,
                    chunk_len,
                    mut future,
                } => {
                    // Time each poll of the hydration future — covers the
                    // `samples_stream.next().await` + `absorb` loop. Only
                    // Ready polls record (Pending polls don't contribute
                    // CPU, just the wake-up cost).
                    let _g = trace::Scope::enter("hydrate");
                    match future.as_mut().poll(cx) {
                        Poll::Pending => {
                            self.state = State::LoadingChunk {
                                chunk_start,
                                chunk_len,
                                future,
                            };
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(samples)) => {
                            self.state = State::Emitting {
                                chunk_start,
                                chunk_len,
                                samples: Box::new(samples),
                                next_step_chunk_start: 0,
                            };
                        }
                        Poll::Ready(Err(err)) => {
                            self.state = State::Errored;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                State::Emitting {
                    chunk_start,
                    chunk_len,
                    samples,
                    next_step_chunk_start,
                } => {
                    let grid = &self.schema.step_grid;
                    if next_step_chunk_start >= grid.step_count {
                        // All steps for this chunk emitted — advance to
                        // the next series chunk or finish.
                        let next_chunk_start = chunk_start + chunk_len;
                        // Drop `samples` here, releasing its reservation.
                        drop(samples);
                        if next_chunk_start >= self.total_series() {
                            self.state = State::Done;
                            return Poll::Ready(None);
                        }
                        let _g = trace::Scope::enter("start_chunk_load");
                        self.state = self.start_chunk_load(next_chunk_start);
                        continue;
                    }
                    let build_result = {
                        let _g = trace::Scope::enter("build_batch");
                        self.build_batch(chunk_start, chunk_len, &samples, next_step_chunk_start)
                    };
                    let batch = match build_result {
                        Ok(batch) => batch,
                        Err(err) => {
                            self.state = State::Errored;
                            return Poll::Ready(Some(Err(err)));
                        }
                    };
                    self.state = State::Emitting {
                        chunk_start,
                        chunk_len,
                        samples,
                        next_step_chunk_start: next_step_chunk_start + batch.step_count(),
                    };
                    return Poll::Ready(Some(Ok(batch)));
                }
                State::Done | State::Errored => {
                    self.state = State::Done;
                    return Poll::Ready(None);
                }
                State::Transitioning => {
                    unreachable!("transient state observed in next()");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
