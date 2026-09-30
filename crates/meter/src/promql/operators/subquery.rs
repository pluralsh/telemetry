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
//! The hard part is that the inner child has to be re-evaluated for each
//! outer step (the inner window slides). The operator owns a
//! [`ChildFactory`] closure the planner supplies; it calls the factory
//! once per outer step to build a freshly-planned child covering
//! `(outer_t - range, outer_t]` at the inner step, drains the child, and
//! packs the resulting instant-vector samples into a
//! [`MatrixWindowBatch`].
//!
//! The subquery shares its parent's [`MemoryReservation`]; nested
//! per-subquery reservations are deferred. A [`SchemaRef::Deferred`] child
//! schema is a planner bug — `count_values` inside a subquery is out of
//! scope.

use std::task::{Context, Poll};

use super::super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::TimeRange;
use super::matrix_selector::{CellIndex, MatrixWindowBatch, WindowHistograms};
use super::rollup::WindowStream;
use crate::histogram::FloatHistogram;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Factory shape
// ---------------------------------------------------------------------------

/// Factory the planner hands `SubqueryOp` at construction time.
///
/// Called once per outer step to build a freshly-planned child operator
/// for the inner `(TimeRange, step_ms)` window. The resulting child must
/// publish a schema whose series roster matches the plan-time series
/// (`SubqueryOp`'s output schema was stamped from the same roster); its
/// step grid is what the subquery re-grids onto.
pub type ChildFactory =
    Box<dyn FnMut(TimeRange, i64) -> Result<Box<dyn Operator + Send>, QueryError> + Send>;

// ---------------------------------------------------------------------------
// Byte-estimate helpers (mirror 3a.2's window_bytes shape)
// ---------------------------------------------------------------------------

#[inline]
fn window_bytes(cells: usize, samples: usize) -> usize {
    let cell_bytes = cells.saturating_mul(std::mem::size_of::<CellIndex>());
    let ts_bytes = samples.saturating_mul(std::mem::size_of::<i64>());
    let val_bytes = samples.saturating_mul(std::mem::size_of::<f64>());
    cell_bytes
        .saturating_add(ts_bytes)
        .saturating_add(val_bytes)
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
    fn allocate(reservation: &MemoryReservation, cell_count: usize) -> Result<Self, QueryError> {
        let bytes = window_bytes(cell_count, 0);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            timestamps: Vec::new(),
            values: Vec::new(),
            cells: vec![CellIndex::EMPTY; cell_count],
        })
    }

    fn grow_samples(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        let bytes = extra.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>());
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
// Subquery operator
// ---------------------------------------------------------------------------

/// Implements PromQL's subquery syntax `expr[range:step]`.
///
/// For each outer step, builds a fresh child operator via the
/// planner-supplied [`ChildFactory`] covering the inner window
/// `(outer_t - range, outer_t]` at the inner step, drains the child's
/// [`StepBatch`] stream, and packs the resulting instant-vector samples
/// into a [`MatrixWindowBatch`] for a downstream [`RollupOp`] to reduce.
///
/// See module docs for the architectural invariants, factory shape,
/// shared-reservation choice, and one-batch-per-outer-step emission
/// policy.
///
/// [`RollupOp`]: super::rollup::RollupOp
pub struct SubqueryOp {
    // Plan-time inputs ------------------------------------------------------
    factory: ChildFactory,
    schema: OperatorSchema,
    outer_step_timestamps: Arc<[i64]>,
    /// Per-outer-step effective evaluation timestamp after folding the
    /// subquery's `@` and `offset` modifiers. `effective_times[k]` is what
    /// the child sub-tree should be evaluated at for outer step `k`; the
    /// inner range window is `(effective_times[k] - range_ms,
    /// effective_times[k]]`.
    effective_times: Arc<[i64]>,
    /// `true` when the subquery has a non-trivial `@` or `offset`
    /// modifier and `effective_times` differs from `outer_step_timestamps`.
    /// Propagated to the emitted [`MatrixWindowBatch`] so the enclosing
    /// `RollupOp` computes window math over the actual sample window.
    has_effective_shift: bool,
    series: Arc<SeriesSchema>,
    range_ms: i64,
    inner_step_ms: i64,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    next_outer_step: usize,
    /// Child for `next_outer_step` and the samples drained from it so far.
    /// Kept across `Pending` so a storage wait resumes rather than replans.
    in_flight: Option<InFlight>,
    errored: bool,
}

struct InFlight {
    inner_window: TimeRange,
    child: Box<dyn Operator + Send>,
    per_series_ts: Vec<Vec<i64>>,
    per_series_vs: Vec<Vec<f64>>,
    per_series_hist: Vec<Vec<(i64, Arc<FloatHistogram>)>>,
}

impl SubqueryOp {
    /// Build a subquery re-grid operator.
    ///
    /// * `factory` — planner-supplied factory that materialises a child
    ///   sub-tree for a given `(TimeRange, step_ms)` inner window. Called
    ///   once per outer step.
    /// * `series` — output series roster. Identical to the child's
    ///   published series roster (the sub-tree's output is data-dependent
    ///   but its labelset is plan-time-stable); stamped here so downstream
    ///   operators see it before any poll.
    /// * `outer_grid` — the **outer** step grid the subquery emits on.
    ///   Each outer step triggers one factory call and one
    ///   `MatrixWindowBatch` emission.
    /// * `range_ms` — bracketed window (e.g. `[5m:…]` → `300_000`). Must
    ///   be `> 0`.
    /// * `inner_step_ms` — subquery resolution (`[…:1m]` → `60_000`).
    ///   Must be `> 0`; the planner resolves the PromQL default when
    ///   omitted.
    /// * `reservation` — per-query reservation; **shared** with the
    ///   parent (no nested scope — nested-reservation scoping is a post-MVP
    ///   concern).
    /// * `effective_times` — per-outer-step effective evaluation times
    ///   with the subquery's `@` / `offset` already folded in. Must be the
    ///   same length as `outer_grid.step_count`. Use [`Self::new`] when
    ///   no `@` / `offset` modifiers are present — it defaults
    ///   `effective_times` to the outer step grid.
    pub fn with_effective_times(
        factory: ChildFactory,
        series: Arc<SeriesSchema>,
        outer_grid: StepGrid,
        range_ms: i64,
        inner_step_ms: i64,
        effective_times: Arc<[i64]>,
        reservation: MemoryReservation,
    ) -> Self {
        assert!(range_ms > 0, "subquery range must be > 0 ms");
        assert!(inner_step_ms > 0, "subquery inner step must be > 0 ms");
        assert_eq!(
            effective_times.len(),
            outer_grid.step_count,
            "effective_times must be length-aligned with outer grid",
        );
        let outer_step_timestamps: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let has_effective_shift = outer_step_timestamps
            .iter()
            .zip(effective_times.iter())
            .any(|(o, e)| o != e);
        let schema = OperatorSchema::new(SchemaRef::Static(series.clone()), outer_grid);
        Self {
            factory,
            schema,
            outer_step_timestamps,
            effective_times,
            has_effective_shift,
            series,
            range_ms,
            inner_step_ms,
            reservation,
            next_outer_step: 0,
            in_flight: None,
            errored: false,
        }
    }

    /// Convenience constructor for subqueries without `@` / `offset`
    /// modifiers — the effective evaluation times equal the outer step grid.
    pub fn new(
        factory: ChildFactory,
        series: Arc<SeriesSchema>,
        outer_grid: StepGrid,
        range_ms: i64,
        inner_step_ms: i64,
        reservation: MemoryReservation,
    ) -> Self {
        let outer_ts: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        Self::with_effective_times(
            factory,
            series,
            outer_grid,
            range_ms,
            inner_step_ms,
            outer_ts,
            reservation,
        )
    }

    /// Drive the child operator to completion, packing its emitted
    /// instant-vector samples into a `MatrixWindowBatch` covering the
    /// single outer step at `outer_step_idx`.
    fn build_outer_step_batch(
        &mut self,
        outer_step_idx: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<MatrixWindowBatch, QueryError>> {
        let series_count = self.series.len();
        if self.in_flight.is_none() {
            let effective_t = self.effective_times[outer_step_idx];
            // Window `(effective - range, effective]`, encoded as the
            // inclusive-exclusive `TimeRange` `[effective - range + 1,
            // effective + 1)`. `effective_t` already folds in the subquery's
            // `@` / `offset` modifiers (see `SubqueryOp::with_effective_times`).
            let inner_window = TimeRange::new(
                effective_t.saturating_sub(self.range_ms).saturating_add(1),
                effective_t.saturating_add(1),
            );
            // Factory failure is terminal for the subquery.
            let child = match (self.factory)(inner_window, self.inner_step_ms) {
                Ok(c) => c,
                Err(err) => return Poll::Ready(Err(err)),
            };
            // A `Deferred` child means `count_values` inside a subquery,
            // which is out of scope for v1 (RFC §"Core Data Model").
            debug_assert!(
                !child.schema().series.is_deferred(),
                "subquery child must publish a static schema (count_values in subquery is v1 out-of-scope)"
            );
            self.in_flight = Some(InFlight {
                inner_window,
                child,
                per_series_ts: vec![Vec::new(); series_count],
                per_series_vs: vec![Vec::new(); series_count],
                per_series_hist: vec![Vec::new(); series_count],
            });
        }
        let in_flight = self.in_flight.as_mut().expect("in-flight child");

        // Drain the child. Each `StepBatch` is an instant-vector of the
        // inner expression's value at some inner step; the child may
        // emit multiple batches covering the inner grid in chunks.
        loop {
            match in_flight.child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(err) = Self::absorb_batch(
                        &batch,
                        in_flight.inner_window,
                        &mut in_flight.per_series_ts,
                        &mut in_flight.per_series_vs,
                        &mut in_flight.per_series_hist,
                    ) {
                        return Poll::Ready(Err(err));
                    }
                }
            }
        }
        let InFlight {
            per_series_ts,
            per_series_vs,
            per_series_hist,
            ..
        } = self.in_flight.take().expect("in-flight child");

        let mut buffers = match WindowBuffers::allocate(&self.reservation, series_count) {
            Ok(b) => b,
            Err(err) => return Poll::Ready(Err(err)),
        };

        // Pack per-series scratch into the flat output buffers. Each
        // cell's samples land contiguously; `CellIndex` records its
        // `[offset, offset+len)` slice.
        for series_off in 0..series_count {
            let ts = &per_series_ts[series_off];
            let vs = &per_series_vs[series_off];
            debug_assert_eq!(ts.len(), vs.len(), "scratch columns must be length-aligned");
            if let Err(err) = buffers.grow_samples(ts.len()) {
                return Poll::Ready(Err(err));
            }
            let cell_offset = buffers.timestamps.len() as u32;
            let cell_len = ts.len() as u32;
            buffers.timestamps.extend_from_slice(ts);
            buffers.values.extend_from_slice(vs);
            buffers.cells[series_off] = CellIndex {
                offset: cell_offset,
                len: cell_len,
            };
        }

        let histograms = if per_series_hist.iter().any(|col| !col.is_empty()) {
            let mut out = WindowHistograms {
                cells: vec![CellIndex::EMPTY; series_count],
                ..Default::default()
            };
            for (series_off, col) in per_series_hist.into_iter().enumerate() {
                if let Err(err) = buffers.grow_samples(col.len()) {
                    return Poll::Ready(Err(err));
                }
                out.cells[series_off] = CellIndex {
                    offset: out.timestamps.len() as u32,
                    len: col.len() as u32,
                };
                for (t, h) in col {
                    out.timestamps.push(t);
                    out.values.push(h);
                }
            }
            Some(out)
        } else {
            None
        };

        let (timestamps, values, cells) = buffers.finish();

        let effective_times = if self.has_effective_shift {
            Some(self.effective_times.clone())
        } else {
            None
        };
        Poll::Ready(Ok(MatrixWindowBatch {
            step_timestamps: self.outer_step_timestamps.clone(),
            step_range: outer_step_idx..(outer_step_idx + 1),
            series: SchemaRef::Static(self.series.clone()),
            series_range: 0..series_count,
            timestamps,
            values,
            cells,
            effective_times,
            histograms,
        }))
    }

    /// Absorb one inner-step [`StepBatch`] into the per-series scratch
    /// columns. Drops invalid cells (validity=0) and `STALE_NAN` values
    /// up front — consumers (`Rollup`) expect pre-filtered windows.
    fn absorb_batch(
        batch: &StepBatch,
        inner_window: TimeRange,
        per_series_ts: &mut [Vec<i64>],
        per_series_vs: &mut [Vec<f64>],
        per_series_hist: &mut [Vec<(i64, Arc<FloatHistogram>)>],
    ) -> Result<(), QueryError> {
        let step_ts = batch.step_timestamps_slice();
        let series_count = batch.series_count();
        // Each batch covers `batch.series_range`; those indices map into
        // the full plan-time series roster (which matches ours).
        let series_base = batch.series_range.start;
        for (step_off, &t) in step_ts.iter().enumerate() {
            // Drop inner-step samples that fall outside the outer
            // window. The child's grid *should* be aligned to the
            // window we requested, but nothing in the `Operator`
            // contract guarantees the child respects the range exactly
            // — e.g. a matrix-selector-flavoured child may emit a
            // lookback sample slightly outside. Defensive filter.
            if t < inner_window.start_ms || t >= inner_window.end_ms_exclusive {
                continue;
            }
            for series_off in 0..series_count {
                if let Some(h) = batch.histogram(batch.cell_index(step_off, series_off)) {
                    per_series_hist[series_base + series_off].push((t, h.clone()));
                    continue;
                }
                let Some(v) = batch.get(step_off, series_off) else {
                    continue;
                };
                if crate::model::is_stale_nan(v) {
                    continue;
                }
                let global_series = series_base + series_off;
                debug_assert!(
                    global_series < per_series_ts.len(),
                    "child emitted series index {global_series} outside subquery roster len {}",
                    per_series_ts.len(),
                );
                per_series_ts[global_series].push(t);
                per_series_vs[global_series].push(v);
            }
        }
        Ok(())
    }

    /// Secondary API — the useful one.
    ///
    /// Polls for the next outer-step [`MatrixWindowBatch`]. v1 emits one
    /// batch per outer step.
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
            let idx = self.next_outer_step;
            self.next_outer_step += 1;
            return Poll::Ready(Some(Ok(MatrixWindowBatch {
                step_timestamps: self.outer_step_timestamps.clone(),
                step_range: idx..(idx + 1),
                series: SchemaRef::Static(self.series.clone()),
                series_range: 0..0,
                timestamps: Vec::new(),
                values: Vec::new(),
                cells: Vec::new(),
                effective_times: None,
                histograms: None,
            })));
        }
        let idx = self.next_outer_step;
        match self.build_outer_step_batch(idx, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(batch)) => {
                self.next_outer_step += 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Err(err)) => {
                self.errored = true;
                self.in_flight = None;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
