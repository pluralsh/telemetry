//! `SubqueryOp` implements PromQL's subquery syntax (`expr[range:step]`)
//! — the "compute an inner range vector at a finer step, then feed it to
//! a range function" construct.
//!
//! A subquery is logically an inner range-vector producer and feeds
//! into a downstream [`RollupOp`](super::rollup::RollupOp), so `SubqueryOp` implements the same
//! [`WindowStream`] contract `MatrixSelectorOp` does — it emits
//! [`MatrixWindowBatch`]es, one per outer step. `Operator::next` is a
//! degenerate "immediate EOS", same arrangement as [`MatrixSelectorOp`](super::matrix_selector::MatrixSelectorOp).
//!
//! Like Prometheus, the inner expression is evaluated once, on a grid of
//! absolute multiples of the subquery step spanning every outer window.
//! Inner points are step-aligned, so the value at an inner point is the
//! same whichever outer window it falls in. The operator drains that one
//! child into per-series columns, then emits each outer step's window
//! `(effective - range, effective]` as a slice of those columns.
//!
//! The subquery shares its parent's [`MemoryReservation`]; the drained
//! columns are charged to it until the operator drops. A
//! [`SchemaRef::Deferred`] child schema is a planner bug — `count_values`
//! inside a subquery is out of scope.

use std::task::{Context, Poll};

use super::super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::matrix_selector::{CellIndex, MatrixWindowBatch, WindowHistograms};
use super::rollup::WindowStream;
use crate::histogram::FloatHistogram;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Byte-estimate helpers (mirror 3a.2's window_bytes shape)
// ---------------------------------------------------------------------------

const SAMPLE_BYTES: usize = std::mem::size_of::<i64>() + std::mem::size_of::<f64>();

#[inline]
fn window_bytes(cells: usize, samples: usize) -> usize {
    let cell_bytes = cells.saturating_mul(std::mem::size_of::<CellIndex>());
    cell_bytes.saturating_add(samples.saturating_mul(SAMPLE_BYTES))
}

// ---------------------------------------------------------------------------
// WindowBuffers — RAII-guarded outer-step batch allocation
// ---------------------------------------------------------------------------

/// RAII wrapper around the working `timestamps` / `values` / `cells`
/// buffers for a single emitted [`MatrixWindowBatch`].
///
/// Reserves the cell-index array up front, grows the flat sample buffer
/// as samples arrive, releases the entire reservation on [`Drop`], and
/// transfers ownership of the inner vectors via [`Self::finish`]
/// (downstream re-reserves if it retains the batch).
struct WindowBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    cells: Vec<CellIndex>,
}

impl WindowBuffers {
    /// Refills `spare`'s columns rather than allocating new ones.
    fn allocate(
        reservation: &MemoryReservation,
        cell_count: usize,
        spare: (Vec<i64>, Vec<f64>, Vec<CellIndex>),
    ) -> Result<Self, QueryError> {
        let bytes = window_bytes(cell_count, 0);
        reservation.try_grow(bytes)?;
        let (mut timestamps, mut values, mut cells) = spare;
        timestamps.clear();
        values.clear();
        cells.clear();
        cells.resize(cell_count, CellIndex::EMPTY);
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            timestamps,
            values,
            cells,
        })
    }

    fn grow_samples(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        let bytes = extra.saturating_mul(SAMPLE_BYTES);
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    fn finish(mut self) -> (Vec<i64>, Vec<f64>, Vec<CellIndex>) {
        let ts = std::mem::take(&mut self.timestamps);
        let vs = std::mem::take(&mut self.values);
        let cells = std::mem::take(&mut self.cells);
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (ts, vs, cells)
    }
}

impl Drop for WindowBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Inner columns
// ---------------------------------------------------------------------------

/// The drained inner child: per-series samples in ascending timestamp
/// order, charged to `reservation` until drop.
struct InnerColumns {
    reservation: MemoryReservation,
    bytes: usize,
    ts: Vec<Vec<i64>>,
    vs: Vec<Vec<f64>>,
    hist: Vec<Vec<(i64, Arc<FloatHistogram>)>>,
}

impl InnerColumns {
    fn new(reservation: &MemoryReservation, series_count: usize) -> Self {
        Self {
            reservation: reservation.clone(),
            bytes: 0,
            ts: vec![Vec::new(); series_count],
            vs: vec![Vec::new(); series_count],
            hist: vec![Vec::new(); series_count],
        }
    }

    /// Absorb one inner-step [`StepBatch`]. Drops invalid cells
    /// (validity=0), `STALE_NAN` values, and points outside `span` —
    /// consumers (`Rollup`) expect pre-filtered windows.
    fn absorb(&mut self, batch: &StepBatch, span: (i64, i64)) -> Result<(), QueryError> {
        let step_ts = batch.step_timestamps_slice();
        let series_count = batch.series_count();
        // Each batch covers `batch.series_range`; those indices map into
        // the child's series roster (which is ours).
        let series_base = batch.series_range.start;
        let mut added = 0usize;
        for (step_off, &t) in step_ts.iter().enumerate() {
            if t < span.0 || t > span.1 {
                continue;
            }
            for series_off in 0..series_count {
                let global_series = series_base + series_off;
                debug_assert!(
                    global_series < self.ts.len(),
                    "child emitted series index {global_series} outside subquery roster len {}",
                    self.ts.len(),
                );
                if let Some(h) = batch.histogram(batch.cell_index(step_off, series_off)) {
                    self.hist[global_series].push((t, h.clone()));
                    added += 1;
                    continue;
                }
                let Some(v) = batch.get(step_off, series_off) else {
                    continue;
                };
                if crate::model::is_stale_nan(v) {
                    continue;
                }
                self.ts[global_series].push(t);
                self.vs[global_series].push(v);
                added += 1;
            }
        }
        let bytes = added.saturating_mul(SAMPLE_BYTES);
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    /// Restore ascending timestamp order in case the child emitted step
    /// chunks out of order.
    fn finish(&mut self) {
        for (ts, vs) in self.ts.iter_mut().zip(self.vs.iter_mut()) {
            if !ts.is_sorted() {
                let mut pairs: Vec<(i64, f64)> =
                    ts.iter().copied().zip(vs.iter().copied()).collect();
                pairs.sort_by_key(|&(t, _)| t);
                (*ts, *vs) = pairs.into_iter().unzip();
            }
        }
        for col in &mut self.hist {
            if !col.is_sorted_by_key(|(t, _)| *t) {
                col.sort_by_key(|(t, _)| *t);
            }
        }
    }
}

impl Drop for InnerColumns {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

/// Index range of `items` (ascending by `key`) inside the window
/// `(lo_exclusive, hi]`.
fn window_slice<T>(
    items: &[T],
    key: impl Fn(&T) -> i64,
    lo_exclusive: i64,
    hi: i64,
) -> (usize, usize) {
    let start = items.partition_point(|item| key(item) <= lo_exclusive);
    let end = items.partition_point(|item| key(item) <= hi);
    (start, end.max(start))
}

// ---------------------------------------------------------------------------
// Subquery operator
// ---------------------------------------------------------------------------

/// Implements PromQL's subquery syntax `expr[range:step]`.
///
/// Drains one child evaluated over every outer window at the inner step,
/// then emits one [`MatrixWindowBatch`] per outer step holding the
/// samples in `(effective - range, effective]` for a downstream
/// [`RollupOp`] to reduce.
///
/// [`RollupOp`]: super::rollup::RollupOp
pub struct SubqueryOp {
    // Plan-time inputs ------------------------------------------------------
    /// The inner expression over every outer window; `None` once drained.
    child: Option<Box<dyn Operator + Send>>,
    schema: OperatorSchema,
    outer_step_timestamps: Arc<[i64]>,
    /// Per-outer-step effective evaluation timestamp after folding the
    /// subquery's `@` and `offset` modifiers. The inner range window for
    /// outer step `k` is `(effective_times[k] - range_ms,
    /// effective_times[k]]`.
    effective_times: Arc<[i64]>,
    /// `true` when the subquery has a non-trivial `@` or `offset`
    /// modifier and `effective_times` differs from `outer_step_timestamps`.
    /// Propagated to the emitted [`MatrixWindowBatch`] so the enclosing
    /// `RollupOp` computes window math over the actual sample window.
    has_effective_shift: bool,
    series: Arc<SeriesSchema>,
    range_ms: i64,
    /// Inclusive bounds of the union of every outer window.
    span: (i64, i64),
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    inner: InnerColumns,
    next_outer_step: usize,
    errored: bool,
    /// A consumed batch's float columns, refilled for the next outer step.
    /// Unreserved, but never more than one batch's columns.
    spare: (Vec<i64>, Vec<f64>, Vec<CellIndex>),
}

impl SubqueryOp {
    /// Build a subquery re-grid operator.
    ///
    /// * `child` — the inner expression evaluated on a step-aligned grid
    ///   covering every outer window. Its static series roster becomes
    ///   the subquery's.
    /// * `outer_grid` — the **outer** step grid the subquery emits on;
    ///   one `MatrixWindowBatch` per step.
    /// * `range_ms` — bracketed window (e.g. `[5m:…]` → `300_000`). Must
    ///   be `> 0`.
    /// * `effective_times` — per-outer-step effective evaluation times
    ///   with the subquery's `@` / `offset` already folded in. Must be the
    ///   same length as `outer_grid.step_count`. Use [`Self::new`] when
    ///   no `@` / `offset` modifiers are present — it defaults
    ///   `effective_times` to the outer step grid.
    /// * `reservation` — per-query reservation, **shared** with the parent.
    ///
    /// # Panics
    ///
    /// If the child publishes a deferred schema.
    pub fn with_effective_times(
        child: Box<dyn Operator + Send>,
        outer_grid: StepGrid,
        range_ms: i64,
        effective_times: Arc<[i64]>,
        reservation: MemoryReservation,
    ) -> Self {
        assert!(range_ms > 0, "subquery range must be > 0 ms");
        assert_eq!(
            effective_times.len(),
            outer_grid.step_count,
            "effective_times must be length-aligned with outer grid",
        );
        let series = child
            .schema()
            .series
            .as_static()
            .expect("subquery child must publish a static schema")
            .clone();
        let outer_step_timestamps: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let has_effective_shift = outer_step_timestamps
            .iter()
            .zip(effective_times.iter())
            .any(|(o, e)| o != e);
        let span = subquery_span(&effective_times, range_ms);
        let schema = OperatorSchema::new(SchemaRef::Static(series.clone()), outer_grid);
        let inner = InnerColumns::new(&reservation, series.len());
        Self {
            child: Some(child),
            schema,
            outer_step_timestamps,
            effective_times,
            has_effective_shift,
            series,
            range_ms,
            span,
            reservation,
            inner,
            next_outer_step: 0,
            errored: false,
            spare: Default::default(),
        }
    }

    /// Convenience constructor for subqueries without `@` / `offset`
    /// modifiers — the effective evaluation times equal the outer step grid.
    pub fn new(
        child: Box<dyn Operator + Send>,
        outer_grid: StepGrid,
        range_ms: i64,
        reservation: MemoryReservation,
    ) -> Self {
        let outer_ts: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        Self::with_effective_times(child, outer_grid, range_ms, outer_ts, reservation)
    }

    /// Drain the child into [`InnerColumns`]. `Ready(Ok(()))` once done.
    fn drain_child(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), QueryError>> {
        let Some(child) = self.child.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        loop {
            match child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(err) = self.inner.absorb(&batch, self.span) {
                        return Poll::Ready(Err(err));
                    }
                }
            }
        }
        self.child = None;
        self.inner.finish();
        Poll::Ready(Ok(()))
    }

    /// Pack outer step `outer_step_idx`'s window of the drained columns
    /// into a `MatrixWindowBatch`.
    fn build_outer_step_batch(
        &mut self,
        outer_step_idx: usize,
    ) -> Result<MatrixWindowBatch, QueryError> {
        let series_count = self.series.len();
        let effective_t = self.effective_times[outer_step_idx];
        let lo_exclusive = effective_t.saturating_sub(self.range_ms);
        let spare = std::mem::take(&mut self.spare);
        let mut buffers = WindowBuffers::allocate(&self.reservation, series_count, spare)?;

        // Each cell's samples land contiguously; `CellIndex` records its
        // `[offset, offset+len)` slice.
        for series_off in 0..series_count {
            let ts = &self.inner.ts[series_off];
            let (start, end) = window_slice(ts, |t| *t, lo_exclusive, effective_t);
            buffers.grow_samples(end - start)?;
            buffers.cells[series_off] = CellIndex {
                offset: buffers.timestamps.len() as u32,
                len: (end - start) as u32,
            };
            buffers.timestamps.extend_from_slice(&ts[start..end]);
            buffers
                .values
                .extend_from_slice(&self.inner.vs[series_off][start..end]);
        }

        let histograms = if self.inner.hist.iter().any(|col| !col.is_empty()) {
            let mut cells = vec![CellIndex::EMPTY; series_count];
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            for (series_off, col) in self.inner.hist.iter().enumerate() {
                let (start, end) = window_slice(col, |(t, _)| *t, lo_exclusive, effective_t);
                buffers.grow_samples(end - start)?;
                cells[series_off] = CellIndex {
                    offset: timestamps.len() as u32,
                    len: (end - start) as u32,
                };
                for (t, h) in &col[start..end] {
                    timestamps.push(*t);
                    values.push(h.clone());
                }
            }
            Some(WindowHistograms {
                timestamps: Arc::new(timestamps),
                values: Arc::new(values),
                cells,
            })
        } else {
            None
        };

        let (timestamps, values, cells) = buffers.finish();
        let effective_times = self
            .has_effective_shift
            .then(|| self.effective_times.clone());
        Ok(MatrixWindowBatch {
            step_timestamps: self.outer_step_timestamps.clone(),
            step_range: outer_step_idx..(outer_step_idx + 1),
            series: SchemaRef::Static(self.series.clone()),
            series_range: 0..series_count,
            timestamps: Arc::new(timestamps),
            values: Arc::new(values),
            cells,
            effective_times,
            histograms,
        })
    }

    fn fail(&mut self, err: QueryError) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        self.errored = true;
        self.child = None;
        Poll::Ready(Some(Err(err)))
    }

    /// Secondary API — the useful one.
    ///
    /// Polls for the next outer-step [`MatrixWindowBatch`]; one batch per
    /// outer step.
    pub fn windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        if self.errored {
            return Poll::Ready(None);
        }
        if self.next_outer_step >= self.schema.step_grid.step_count {
            return Poll::Ready(None);
        }
        // Empty series roster short-circuit.
        if self.series.is_empty() {
            // Still emit one empty batch per outer step so consumers can
            // observe the outer grid; follows the `MatrixSelectorOp`
            // convention (empty windows are represented as empty
            // `CellIndex`es, not absent batches).
            self.child = None;
            let idx = self.next_outer_step;
            self.next_outer_step += 1;
            return Poll::Ready(Some(Ok(MatrixWindowBatch {
                step_timestamps: self.outer_step_timestamps.clone(),
                step_range: idx..(idx + 1),
                series: SchemaRef::Static(self.series.clone()),
                series_range: 0..0,
                timestamps: Arc::default(),
                values: Arc::default(),
                cells: Vec::new(),
                effective_times: None,
                histograms: None,
            })));
        }
        match self.drain_child(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return self.fail(err),
            Poll::Ready(Ok(())) => {}
        }
        let idx = self.next_outer_step;
        match self.build_outer_step_batch(idx) {
            Ok(batch) => {
                self.next_outer_step += 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Err(err) => self.fail(err),
        }
    }
}

/// Inclusive bounds of the union of the windows `(t - range_ms, t]` over
/// `effective_times`.
pub(crate) fn subquery_span(effective_times: &[i64], range_ms: i64) -> (i64, i64) {
    let lo = effective_times.iter().copied().min().unwrap_or(0);
    let hi = effective_times.iter().copied().max().unwrap_or(0);
    (lo.saturating_sub(range_ms).saturating_add(1), hi)
}

// ---------------------------------------------------------------------------
// Operator impl — degenerate, mirrors MatrixSelectorOp
// ---------------------------------------------------------------------------

impl Operator for SubqueryOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    /// Degenerate: subquery output is a matrix, which does not fit
    /// `StepBatch`'s single-float-per-cell shape. Consumers drive the
    /// operator via [`SubqueryOp::windows`] / [`WindowStream::poll_windows`].
    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(None)
    }
}

// ---------------------------------------------------------------------------
// WindowStream impl — drop-in for `RollupOp<SubqueryOp>`
// ---------------------------------------------------------------------------

impl WindowStream for SubqueryOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        self.windows(cx)
    }

    fn recycle(&mut self, window: MatrixWindowBatch) {
        self.spare = (
            Arc::try_unwrap(window.timestamps).unwrap_or_default(),
            Arc::try_unwrap(window.values).unwrap_or_default(),
            window.cells,
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
