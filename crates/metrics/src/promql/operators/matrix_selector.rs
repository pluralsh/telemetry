//! `MatrixSelectorOp` — the storage leaf for PromQL range vectors
//! (`metric[5m]`). It fetches raw samples and repackages them into
//! per-step windows for [`RollupOp`](super::rollup::RollupOp) (`rate`, `*_over_time`, ...) and for
//! [`SubqueryOp`](super::subquery::SubqueryOp) to consume.
//!
//! `metric[range]` produces a *vector of samples* per step, which doesn't
//! fit [`StepBatch`]'s one-float-per-cell shape. So this operator doesn't
//! emit `StepBatch`es on its main `Operator::next` loop (that path is a
//! degenerate "immediate EOS"); the real output is
//! [`MatrixSelectorOp::windows`], which emits [`MatrixWindowBatch`]es.
//! [`RollupOp`](super::rollup::RollupOp) drives this via the [`super::rollup::WindowStream`]
//! trait, which [`super::rollup::MatrixWindowSource`] implements over a
//! `MatrixSelectorOp`.
//!
//! Per-step semantics (no `lookback_delta` — the bracketed `[range]`
//! replaces it):
//! ```text
//!   pin         = @ value when set, else t
//!   effective   = pin - offset
//!   window      = (effective - range, effective]
//!   samples     = source samples in window, ascending, STALE_NAN dropped
//! ```
//!
//! Tiling: one [`MatrixWindowBatch`] per `(series_chunk × step_chunk)` —
//! the same two-level tiling scheme as
//! [`VectorSelectorOp`](super::vector_selector::VectorSelectorOp).
//!
//! [`MatrixWindowBatch`] layout: flat `timestamps` / `values` buffers plus
//! a row-major-by-step `cells: Vec<CellIndex>` where
//! `cells[t * series_count + s] = { offset, len }` indexes into the flat
//! buffers. This operator packs a series chunk's samples into those
//! buffers once and shares them across the chunk's step tiles, so a cell
//! is a range into the series' samples rather than a copy of its window.
//! Adjacent-step cells for the same series advance monotonically, so
//! `RollupOp`'s two-pointer driver reuses state across steps.

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

use super::super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::{
    ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use super::vector_selector::histogram_sample_bytes;

// ---------------------------------------------------------------------------
// Defaults & tile shape (mirrors 3a.1)
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_STEP_CHUNK: usize = 64;
pub(crate) const DEFAULT_SERIES_CHUNK: usize = 512;

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
// Window batch type — the "secondary API" shape
// ---------------------------------------------------------------------------

/// Index into a [`MatrixWindowBatch`]'s flat sample buffer for a single
/// `(step, series)` cell.
///
/// `offset` and `len` are `u32` because a single window cell is bounded
/// by the range's sample count at scrape resolution; billions of samples
/// in one cell would not fit the operator's memory reservation anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellIndex {
    /// Start index into [`MatrixWindowBatch::timestamps`] / [`MatrixWindowBatch::values`].
    pub offset: u32,
    /// Number of samples in the cell's window (`0` for an empty window).
    pub len: u32,
}

impl CellIndex {
    /// Empty cell (no samples in the window).
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    /// `(start, end)` bounds for slicing the flat sample buffer.
    #[inline]
    pub fn range(&self) -> std::ops::Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

/// The window-batch counterpart to [`StepBatch`]: a rectangle of *sample
/// lists* (one list of `(ts, val)` pairs per `(step, series)` cell)
/// instead of single values. This is what flows between range-vector
/// producers ([`MatrixSelectorOp`], [`SubqueryOp`]) and
/// [`RollupOp`](super::rollup::RollupOp).
///
/// Covers a contiguous `(series_range × step_range)` tile. Samples for
/// each cell are packed into the flat `timestamps` / `values` columns and
/// indexed per cell via [`Self::cells`] (row-major by step, matching
/// [`StepBatch`]'s layout).
///
/// Consumers read per-cell slices via [`Self::cell_samples`]. `STALE_NAN`
/// values are already filtered out by the producer — consumers see only
/// valid numeric samples in ascending timestamp order.
///
/// [`SubqueryOp`]: super::subquery::SubqueryOp
#[derive(Debug, Clone)]
pub struct MatrixWindowBatch {
    /// Absolute step timestamps (ms), shared with the rest of the query
    /// via `Arc` just like [`StepBatch::step_timestamps`].
    pub step_timestamps: Arc<[i64]>,
    /// Slice of [`Self::step_timestamps`] covered by this batch.
    pub step_range: std::ops::Range<usize>,

    /// Series roster the planner built.
    pub series: SchemaRef,
    /// Slice of the series roster covered by this batch.
    pub series_range: std::ops::Range<usize>,

    /// Flat sample timestamps (ms). A cell's slice lives at
    /// [`CellIndex::range`]; cells may overlap.
    pub timestamps: Arc<Vec<i64>>,
    /// Flat sample values, parallel to [`Self::timestamps`].
    pub values: Arc<Vec<f64>>,
    /// Per-cell index. Length is `step_count * series_count`, row-major
    /// by step (cell `(t_off, s_off)` lives at `t_off * series_count +
    /// s_off`).
    pub cells: Vec<CellIndex>,

    /// Optional per-step **effective** timestamps — the window-end each
    /// step's samples actually live under after folding `@` / `offset`.
    /// `None` means the effective time equals the step timestamp (the
    /// common no-modifier case). When present, shares layout with
    /// [`Self::step_timestamps`] (absolute, indexed by global step idx)
    /// so consumers slice via `step_range` the same way.
    ///
    /// [`RollupOp`](super::rollup::RollupOp) prefers this over `step_timestamps` when computing
    /// `(window_start, window_end)` for rate-family extrapolation; without
    /// it, `rate(metric[100s] @ 100)` at outer step `t=25s` would compute
    /// rate over the window `(-75s, 25s]` while the packed samples
    /// actually cover `(0, 100s]`, producing a negative rate disjoint
    /// from the data.
    pub effective_times: Option<Arc<[i64]>>,

    /// Native histogram samples per cell, laid out like the float columns.
    /// `None` when no cell in the batch holds a histogram.
    pub histograms: Option<WindowHistograms>,
}

/// Histogram counterpart of [`MatrixWindowBatch`]'s flat float columns.
#[derive(Debug, Clone, Default)]
pub struct WindowHistograms {
    pub timestamps: Arc<Vec<i64>>,
    pub values: Arc<Vec<Arc<FloatHistogram>>>,
    /// Same length and layout as [`MatrixWindowBatch::cells`].
    pub cells: Vec<CellIndex>,
}

impl MatrixWindowBatch {
    /// Steps covered by this batch.
    #[inline]
    pub fn step_count(&self) -> usize {
        self.step_range.end - self.step_range.start
    }

    /// Series covered by this batch.
    #[inline]
    pub fn series_count(&self) -> usize {
        self.series_range.end - self.series_range.start
    }

    /// Total cell count (`step_count * series_count`).
    #[inline]
    pub fn len(&self) -> usize {
        self.step_count() * self.series_count()
    }

    /// `true` if the batch covers zero cells.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Samples for the `(step_off, series_off)` cell (offsets are
    /// batch-local, not grid-global).
    pub fn cell_samples(&self, step_off: usize, series_off: usize) -> (&[i64], &[f64]) {
        debug_assert!(step_off < self.step_count());
        debug_assert!(series_off < self.series_count());
        let idx = step_off * self.series_count() + series_off;
        let r = self.cells[idx].range();
        (&self.timestamps[r.clone()], &self.values[r])
    }

    /// Histogram samples for the `(step_off, series_off)` cell; empty when
    /// the batch carries no histograms.
    pub fn cell_histograms(
        &self,
        step_off: usize,
        series_off: usize,
    ) -> (&[i64], &[Arc<FloatHistogram>]) {
        match &self.histograms {
            Some(h) => {
                let r = h.cells[step_off * self.series_count() + series_off].range();
                (&h.timestamps[r.clone()], &h.values[r])
            }
            None => (&[], &[]),
        }
    }
}

// ---------------------------------------------------------------------------
// Memory-guarded window-batch buffers
// ---------------------------------------------------------------------------

/// Per-series sample column byte estimate.
#[inline]
fn samples_bytes(n: usize) -> usize {
    n.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>())
}

/// Checks a window batch's cell index against the reservation. The bytes
/// are released once the batch is built; the consumer re-reserves if it
/// holds the batch.
fn check_cells(reservation: &MemoryReservation, cell_count: usize) -> Result<(), QueryError> {
    let bytes = cell_count.saturating_mul(std::mem::size_of::<CellIndex>());
    reservation.try_grow(bytes)?;
    reservation.release(bytes);
    Ok(())
}

// ---------------------------------------------------------------------------
// Effective time computation (mirrors 3a.1's `EffectiveTimes` but does
// not subtract lookback — the explicit `[range]` replaces it).
// ---------------------------------------------------------------------------

/// Pre-computed per-step evaluation times after applying `@` then
/// `offset`. Range is **not** folded in here; the operator subtracts it
/// per-step when sliding the window.
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
        // Phase 2: apply offset (matches evaluator.rs:1749-1764).
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

    /// Time window the source must cover so the operator has samples for
    /// every step's `(effective - range, effective]` slot.
    fn time_range_with_range(&self, range_ms: i64) -> TimeRange {
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
            return TimeRange::new(0, 0);
        }
        // Window is `(t - range, t]`; source needs samples in
        // `[min - range + 1, max + 1)` (inclusive-exclusive TimeRange
        // convention).
        let start = min.saturating_sub(range_ms).saturating_add(1);
        let end = max.saturating_add(1);
        TimeRange::new(start, end)
    }
}

// ---------------------------------------------------------------------------
// Per-chunk sample state (same shape as 3a.1's ChunkSamples)
// ---------------------------------------------------------------------------

/// Per-series sample columns for the current series chunk.
struct ChunkSamples {
    reservation: MemoryReservation,
    bytes: usize,
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

    /// Moves every series' samples, minus `STALE_NAN`s, into one shared
    /// column per kind.
    fn pack(mut self) -> Result<PackedChunk, QueryError> {
        let total: usize = self.timestamps.iter().map(Vec::len).sum();
        let histogram_total: usize = self.histogram_timestamps.iter().map(Vec::len).sum();
        if u32::try_from(total.max(histogram_total)).is_err() {
            return Err(QueryError::Internal(format!(
                "matrix selector chunk of {total} samples exceeds the cell index range"
            )));
        }
        // The per-series columns are freed as the packed ones fill, but
        // both exist at the start.
        let transient = samples_bytes(total);
        self.reservation.try_grow(transient)?;

        let mut timestamps = Vec::with_capacity(total);
        let mut values = Vec::with_capacity(total);
        let mut starts = Vec::with_capacity(self.timestamps.len() + 1);
        for (ts, vs) in std::mem::take(&mut self.timestamps)
            .into_iter()
            .zip(std::mem::take(&mut self.values))
        {
            starts.push(timestamps.len());
            for (t, v) in ts.into_iter().zip(vs) {
                if !is_stale_nan(v) {
                    timestamps.push(t);
                    values.push(v);
                }
            }
        }
        starts.push(timestamps.len());

        let mut histogram_timestamps = Vec::with_capacity(histogram_total);
        let mut histograms = Vec::with_capacity(histogram_total);
        let mut histogram_starts = Vec::with_capacity(self.histograms.len() + 1);
        for (hts, hs) in std::mem::take(&mut self.histogram_timestamps)
            .into_iter()
            .zip(std::mem::take(&mut self.histograms))
        {
            histogram_starts.push(histogram_timestamps.len());
            histogram_timestamps.extend(hts);
            histograms.extend(hs);
        }
        histogram_starts.push(histogram_timestamps.len());
        self.reservation.release(transient);

        Ok(PackedChunk {
            reservation: self.reservation.clone(),
            bytes: std::mem::take(&mut self.bytes),
            timestamps: Arc::new(timestamps),
            values: Arc::new(values),
            starts,
            histogram_timestamps: Arc::new(histogram_timestamps),
            histograms: Arc::new(histograms),
            histogram_starts,
        })
    }

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

/// A loaded series chunk whose samples sit in shared flat columns, so its
/// window batches index into them instead of copying.
struct PackedChunk {
    reservation: MemoryReservation,
    bytes: usize,
    timestamps: Arc<Vec<i64>>,
    values: Arc<Vec<f64>>,
    /// Chunk-local series `s` owns `starts[s]..starts[s + 1]`.
    starts: Vec<usize>,
    histogram_timestamps: Arc<Vec<i64>>,
    histograms: Arc<Vec<Arc<FloatHistogram>>>,
    histogram_starts: Vec<usize>,
}

impl PackedChunk {
    fn has_histograms(&self) -> bool {
        !self.histograms.is_empty()
    }
}

impl Drop for PackedChunk {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Sliding-walk driver — the correctness-sensitive bit
// ---------------------------------------------------------------------------

/// Two-pointer cursor for a single series' sample column.
///
/// Invariant: `lo` is the index of the first sample whose timestamp is
/// `> window_lo_exclusive` (exclusive lower bound). `hi` is one past the
/// index of the last sample whose timestamp is `<= window_hi_inclusive`.
/// Callers advance both monotonically as the window slides forward.
///
/// Because `effective` times are monotonic in the step index **only when
/// `@` is not set and offset is constant** — which is the common case —
/// but the operator must also handle `@`-pinned cases (all windows
/// identical) and other non-monotonic configurations, the driver
/// *resets* the cursors when it detects the window moved backward. This
/// keeps the code correct at the cost of a single `O(samples_per_series)`
/// scan per reset; in the common monotonic case the two-pointer sweep is
/// amortised O(samples_per_series) across all steps.
struct SeriesCursor {
    lo: usize,
    hi: usize,
    /// Last window `(lo_excl, hi_incl]` processed, used to detect
    /// backward jumps that require a reset.
    last_window_hi: i64,
}

impl SeriesCursor {
    fn new() -> Self {
        Self {
            lo: 0,
            hi: 0,
            last_window_hi: i64::MIN,
        }
    }

    /// Advance the cursor to cover `(window_lo_exclusive, window_hi_inclusive]`
    /// in `timestamps`. Returns the `[lo, hi)` range of samples in-window.
    ///
    /// `timestamps` is assumed ascending, with `STALE_NAN`s already dropped
    /// by [`ChunkSamples::pack`].
    fn advance(
        &mut self,
        timestamps: &[i64],
        window_lo_exclusive: i64,
        window_hi_inclusive: i64,
    ) -> std::ops::Range<usize> {
        // Backward jump (e.g. `@` pinning all steps to a fixed time, then
        // a forward-shifted offset): reset.
        if window_hi_inclusive < self.last_window_hi {
            self.lo = 0;
            self.hi = 0;
        }
        self.last_window_hi = window_hi_inclusive;

        // Advance `lo` past samples <= window_lo_exclusive.
        while self.lo < timestamps.len() && timestamps[self.lo] <= window_lo_exclusive {
            self.lo += 1;
        }
        // `hi` may be behind `lo` after a reset — pull it up.
        if self.hi < self.lo {
            self.hi = self.lo;
        }
        // Advance `hi` past samples <= window_hi_inclusive.
        while self.hi < timestamps.len() && timestamps[self.hi] <= window_hi_inclusive {
            self.hi += 1;
        }
        // If `lo` moved past `hi` (window slid entirely past earlier
        // samples), normalise.
        if self.lo > self.hi {
            self.hi = self.lo;
        }
        self.lo..self.hi
    }
}

/// The cell of chunk-local series `series_off` for the window
/// `(lo_exclusive, hi_inclusive]`, as a range into the packed `timestamps`.
fn cell(
    starts: &[usize],
    series_off: usize,
    cursor: &mut SeriesCursor,
    timestamps: &[i64],
    lo_exclusive: i64,
    hi_inclusive: i64,
) -> CellIndex {
    let base = starts[series_off];
    let range = cursor.advance(
        &timestamps[base..starts[series_off + 1]],
        lo_exclusive,
        hi_inclusive,
    );
    // `PackedChunk` bounds its columns to `u32` lengths.
    CellIndex {
        offset: (base + range.start) as u32,
        len: range.len() as u32,
    }
}

// ---------------------------------------------------------------------------
// Operator state machine
// ---------------------------------------------------------------------------

type SampleStream<'a> = Pin<Box<dyn Stream<Item = Result<SampleBatch, QueryError>> + Send + 'a>>;

enum State<'a> {
    Init,
    LoadingChunk {
        chunk_start: usize,
        chunk_len: usize,
        #[allow(clippy::type_complexity)]
        future: Pin<Box<dyn Future<Output = Result<PackedChunk, QueryError>> + Send + 'a>>,
    },
    Emitting {
        chunk_start: usize,
        chunk_len: usize,
        samples: Box<PackedChunk>,
        /// Per-series cursor for the current series chunk. `cursors[i]`
        /// tracks chunk-local series `i`.
        cursors: Vec<SeriesCursor>,
        /// Histogram-column cursors, parallel to `cursors`.
        hist_cursors: Vec<SeriesCursor>,
        /// Next step chunk's starting index within the full grid.
        next_step_chunk_start: usize,
    },
    Done,
    Errored,
    Transitioning,
}

// ---------------------------------------------------------------------------
// Operator struct
// ---------------------------------------------------------------------------

/// Storage leaf for PromQL range vectors (`metric[range]`). Fetches raw
/// samples from a [`SeriesSource`] and packs them into per-step windows
/// for a downstream [`RollupOp`](super::rollup::RollupOp) to reduce.
///
/// See module docs for the two-API arrangement (degenerate
/// `Operator::next`, plus the useful [`Self::windows`] secondary API) and
/// for the sliding-window semantics.
pub(crate) struct MatrixSelectorOp<'a, S: SeriesSource + 'a> {
    // Plan-time inputs ------------------------------------------------------
    source: Arc<S>,
    request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    effective_times: EffectiveTimes,
    /// `true` when `@` or `offset` shifts the per-step effective time away
    /// from `step_timestamps`. When set, [`MatrixWindowBatch`] carries the
    /// `effective_times` array so the enclosing `RollupOp` can compute
    /// rate-family extrapolation over the window the samples actually
    /// live in.
    has_effective_shift: bool,
    range_ms: i64,
    shape: BatchShape,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    state: State<'a>,
    /// A consumed batch's cell index, kept for the next batch. Unreserved,
    /// but never more than one batch's.
    spare_cells: Vec<CellIndex>,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> MatrixSelectorOp<'a, S> {
    /// Build an operator from resolved inputs.
    ///
    /// * `source` — storage handle.
    /// * `series` — post-resolve series roster.
    /// * `request_series` — parallel per-logical-series opaque source handle
    ///   groups. A single logical series may map to several bucket-local refs
    ///   after planner-side roster deduplication.
    /// * `grid` — **outer** step grid the query runs on. Downstream
    ///   operators that fuse (`Rollup`, `Subquery`) still see this grid
    ///   via [`Operator::schema`].
    /// * `at` / `offset` — selector modifiers.
    /// * `range_ms` — explicit bracketed window in ms (e.g. `[5m]` →
    ///   `300_000`). Must be `> 0`.
    /// * `reservation` — per-query reservation (cloned).
    /// * `shape` — tile dimensions; [`BatchShape::default`] outside tests.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<S>,
        series: Arc<SeriesSchema>,
        request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        range_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> Self {
        assert_eq!(
            series.len(),
            request_series.len(),
            "series roster and request_series must be length-aligned",
        );
        assert!(range_ms > 0, "matrix range must be > 0 ms");
        let step_timestamps: Arc<[i64]> = Arc::from(
            (0..grid.step_count)
                .map(|k| grid.start_ms + (k as i64) * grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let effective_times =
            EffectiveTimes::compute(&step_timestamps, &grid, at.as_ref(), offset.as_ref());
        let has_effective_shift = at.is_some()
            || matches!(
                offset,
                Some(Offset::Pos(d)) | Some(Offset::Neg(d)) if d.as_millis() > 0
            );
        let schema = OperatorSchema::new(SchemaRef::Static(series), grid);
        Self {
            source,
            request_series,
            schema,
            step_timestamps,
            effective_times,
            has_effective_shift,
            range_ms,
            shape,
            reservation,
            state: State::Init,
            spare_cells: Vec::new(),
        }
    }

    /// Takes back a batch this operator emitted once its consumer is done
    /// with it, so the next batch reuses its cell index.
    pub(crate) fn recycle(&mut self, window: MatrixWindowBatch) {
        self.spare_cells = window.cells;
    }

    fn total_series(&self) -> usize {
        self.request_series.len()
    }

    fn chunk_request(&self, chunk_start: usize, chunk_end: usize) -> (SamplesRequest, Vec<usize>) {
        let (flat, request_to_series) = super::vector_selector::flatten_bucket_major(
            &self.request_series[chunk_start..chunk_end],
        );
        let window = self.effective_times.time_range_with_range(self.range_ms);
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
                samples.absorb(batch, &request_to_series)?;
            }
            samples.pack()
        });

        State::LoadingChunk {
            chunk_start,
            chunk_len,
            future,
        }
    }

    fn build_window_batch(
        &mut self,
        chunk_start: usize,
        chunk_len: usize,
        samples: &PackedChunk,
        cursors: &mut [SeriesCursor],
        hist_cursors: &mut [SeriesCursor],
        step_chunk_start: usize,
    ) -> Result<MatrixWindowBatch, QueryError> {
        let grid = &self.schema.step_grid;
        let step_chunk_end = (step_chunk_start + self.shape.step_chunk).min(grid.step_count);
        let step_count = step_chunk_end - step_chunk_start;
        let cell_count = step_count * chunk_len;

        check_cells(&self.reservation, cell_count)?;
        let mut cells = std::mem::take(&mut self.spare_cells);
        cells.clear();
        cells.resize(cell_count, CellIndex::EMPTY);
        let mut histogram_cells = samples
            .has_histograms()
            .then(|| vec![CellIndex::EMPTY; cell_count]);

        for step_off in 0..step_count {
            let step_idx = step_chunk_start + step_off;
            let effective = self.effective_times.get(step_idx);
            let window_lo = effective.saturating_sub(self.range_ms); // exclusive
            let window_hi = effective; // inclusive

            for (series_off, cursor) in cursors.iter_mut().enumerate().take(chunk_len) {
                let cell_idx = step_off * chunk_len + series_off;
                cells[cell_idx] = cell(
                    &samples.starts,
                    series_off,
                    cursor,
                    &samples.timestamps,
                    window_lo,
                    window_hi,
                );
                if let Some(out) = histogram_cells.as_mut() {
                    out[cell_idx] = cell(
                        &samples.histogram_starts,
                        series_off,
                        &mut hist_cursors[series_off],
                        &samples.histogram_timestamps,
                        window_lo,
                        window_hi,
                    );
                }
            }
        }

        let histograms = histogram_cells.map(|cells| WindowHistograms {
            timestamps: samples.histogram_timestamps.clone(),
            values: samples.histograms.clone(),
            cells,
        });
        let timestamps = samples.timestamps.clone();
        let values = samples.values.clone();
        let series_range = chunk_start..(chunk_start + chunk_len);
        let step_range = step_chunk_start..step_chunk_end;
        let effective_times = if self.has_effective_shift {
            Some(self.effective_times.times.clone())
        } else {
            None
        };
        Ok(MatrixWindowBatch {
            step_timestamps: self.step_timestamps.clone(),
            step_range,
            series: self.schema.series.clone(),
            series_range,
            timestamps,
            values,
            cells,
            effective_times,
            histograms,
        })
    }

    /// Secondary API — the useful one.
    ///
    /// Polls for the next window batch. Consumers (`Rollup`, `Subquery`)
    /// drive the operator through this method rather than
    /// [`Operator::next`], which is a degenerate end-of-stream for this
    /// operator.
    pub(crate) fn windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>>
    where
        S: 'a,
    {
        loop {
            let state = std::mem::replace(&mut self.state, State::Transitioning);
            match state {
                State::Init => {
                    if self.total_series() == 0 || self.schema.step_grid.step_count == 0 {
                        self.state = State::Done;
                        return Poll::Ready(None);
                    }
                    self.state = self.start_chunk_load(0);
                }
                State::LoadingChunk {
                    chunk_start,
                    chunk_len,
                    mut future,
                } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = State::LoadingChunk {
                            chunk_start,
                            chunk_len,
                            future,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(samples)) => {
                        let cursors = (0..chunk_len).map(|_| SeriesCursor::new()).collect();
                        let hist_cursors = (0..chunk_len).map(|_| SeriesCursor::new()).collect();
                        self.state = State::Emitting {
                            chunk_start,
                            chunk_len,
                            samples: Box::new(samples),
                            cursors,
                            hist_cursors,
                            next_step_chunk_start: 0,
                        };
                    }
                    Poll::Ready(Err(err)) => {
                        self.state = State::Errored;
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                State::Emitting {
                    chunk_start,
                    chunk_len,
                    samples,
                    mut cursors,
                    mut hist_cursors,
                    next_step_chunk_start,
                } => {
                    let grid = &self.schema.step_grid;
                    if next_step_chunk_start >= grid.step_count {
                        let next_chunk_start = chunk_start + chunk_len;
                        drop(samples);
                        if next_chunk_start >= self.total_series() {
                            self.state = State::Done;
                            return Poll::Ready(None);
                        }
                        self.state = self.start_chunk_load(next_chunk_start);
                        continue;
                    }
                    match self.build_window_batch(
                        chunk_start,
                        chunk_len,
                        &samples,
                        &mut cursors,
                        &mut hist_cursors,
                        next_step_chunk_start,
                    ) {
                        Ok(batch) => {
                            let step_advance = batch.step_count();
                            self.state = State::Emitting {
                                chunk_start,
                                chunk_len,
                                samples,
                                cursors,
                                hist_cursors,
                                next_step_chunk_start: next_step_chunk_start + step_advance,
                            };
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Err(err) => {
                            self.state = State::Errored;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                State::Done | State::Errored => {
                    self.state = State::Done;
                    return Poll::Ready(None);
                }
                State::Transitioning => {
                    unreachable!("transient state observed in windows()");
                }
            }
        }
    }
}

// Operator trait compliance — the `Operator::next` surface is degenerate
// for this operator (matrix output cannot fit `StepBatch`'s single-float
// cell shape). Consumers use `MatrixSelectorOp::windows` instead.
impl<S: SeriesSource + Send + Sync + 'static> Operator for MatrixSelectorOp<'static, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    /// Degenerate: immediately returns end-of-stream. See the module
    /// docs and [`MatrixSelectorOp::windows`].
    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
