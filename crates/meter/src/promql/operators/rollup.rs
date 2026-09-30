//! `RollupOp` implements every PromQL range function — `rate`,
//! `increase`, `delta`, `*_over_time`, `quantile_over_time`, `resets`,
//! `changes`, and the rest. One operator type, with the specific
//! reduction selected by a [`RollupKind`] enum.
//!
//! The division of labour with the upstream selector is deliberate:
//! [`MatrixSelectorOp`] does the "per-step window" work (fetch samples,
//! slice them per cell, emit [`MatrixWindowBatch`]es); `RollupOp` does
//! the "window → scalar" work (pick a reducer, apply it per cell, emit
//! [`StepBatch`]es). They talk through the [`WindowStream`] trait so
//! [`SubqueryOp`](super::subquery::SubqueryOp) can plug in as an
//! alternative producer without subclassing the selector.
//!
//! Dispatch is a [`RollupKind`] enum-match on a no-alloc
//! `(window_start, window_end, &[ts], &[val]) → Option<f64>` reducer —
//! `None` flips the output cell's validity to 0.
//!
//! Scope: the legacy engine's range functions plus `increase`, `delta`,
//! `irate`, `idelta`, `resets`, `changes`, `last_over_time`,
//! `quantile_over_time`, `present_over_time`, `deriv`, `predict_linear`.
//!
//! Extrapolation for `rate` / `increase` / `delta` is ported verbatim
//! from v1's `counter_increase_correction` + `extrapolated_rate`.
//!
//! Only the output `StepBatch` buffers route through
//! [`MemoryReservation::try_grow`]; the input window batch is already
//! accounted for by `MatrixSelectorOp`.

use std::task::{Context, Poll};

use super::super::batch::{BitSet, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};
use super::super::source::SeriesSource;
use super::matrix_selector::{MatrixSelectorOp, MatrixWindowBatch};

// ---------------------------------------------------------------------------
// RollupKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-window scalar reducer. Adding a rollup is a new variant + arms in
/// [`RollupKind::min_samples`] and [`RollupKind::compute`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RollupKind {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
    Resets,
    Changes,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    CountOverTime,
    LastOverTime,
    StddevOverTime,
    StdvarOverTime,
    /// `quantile_over_time(q, v[range])`. `q` is plan-time constant per
    /// Prometheus semantics; range-vector quantiles with per-series `q`
    /// are not a PromQL construct.
    QuantileOverTime(f64),
    PresentOverTime,
    /// `deriv(v[range])` — per-second slope of a least-squares fit.
    Deriv,
    /// `predict_linear(v[range], t)` — least-squares fit extrapolated `t`
    /// seconds past the evaluation time. `t` is plan-time constant.
    PredictLinear(f64),
}

impl RollupKind {
    /// Minimum number of in-window samples for the kind to produce a
    /// value. Below this threshold the cell is emitted with validity=0.
    ///
    /// Mirrors Prometheus: extrapolated counters/gauges need 2+; the
    /// `*_over_time` family needs 1+.
    #[inline]
    pub fn min_samples(&self) -> usize {
        match self {
            Self::Rate
            | Self::Increase
            | Self::Delta
            | Self::Irate
            | Self::Idelta
            | Self::Deriv
            | Self::PredictLinear(_) => 2,
            _ => 1,
        }
    }

    /// Reduce an in-window sample set to a single scalar, or `None` when
    /// the cell should be emitted with `validity = 0`.
    ///
    /// `window_start_ms` is the exclusive lower bound, `window_end_ms`
    /// the inclusive upper bound (matching [`MatrixSelectorOp`]'s
    /// emission contract). `eval_ms` is the output step's timestamp, which
    /// differs from `window_end_ms` under `@` / `offset`. `ts` / `vs` are
    /// parallel, ascending, and already filtered of `STALE_NAN` by the
    /// matrix operator.
    pub fn compute(
        &self,
        window_start_ms: i64,
        window_end_ms: i64,
        eval_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        if ts.len() < self.min_samples() {
            // Present-over-time still produces a validity-0 cell when empty;
            // callers handle the distinction via the validity bit.
            return None;
        }
        match self {
            Self::Rate => rollup_fns::extrapolated_rate(window_start_ms, window_end_ms, ts, vs),
            Self::Increase => {
                rollup_fns::extrapolated_increase(window_start_ms, window_end_ms, ts, vs)
            }
            Self::Delta => rollup_fns::extrapolated_delta(window_start_ms, window_end_ms, ts, vs),
            Self::Irate => rollup_fns::irate(ts, vs),
            Self::Idelta => rollup_fns::idelta(vs),
            Self::Resets => Some(rollup_fns::resets(vs) as f64),
            Self::Changes => Some(rollup_fns::changes(vs) as f64),
            Self::SumOverTime => Some(rollup_fns::sum_over_time(vs)),
            Self::AvgOverTime => Some(rollup_fns::avg_over_time(vs)),
            Self::MinOverTime => Some(rollup_fns::min_over_time(vs)),
            Self::MaxOverTime => Some(rollup_fns::max_over_time(vs)),
            Self::CountOverTime => Some(vs.len() as f64),
            Self::LastOverTime => vs.last().copied(),
            Self::StddevOverTime => Some(rollup_fns::variance(vs).sqrt()),
            Self::StdvarOverTime => Some(rollup_fns::variance(vs)),
            Self::QuantileOverTime(q) => Some(rollup_fns::quantile(*q, vs)),
            Self::PresentOverTime => {
                if vs.is_empty() {
                    None
                } else {
                    Some(1.0)
                }
            }
            Self::Deriv => Some(rollup_fns::linear_regression(ts, vs, ts[0]).0),
            Self::PredictLinear(seconds) => {
                let (slope, intercept) = rollup_fns::linear_regression(ts, vs, eval_ms);
                Some(intercept + slope * seconds)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WindowStream trait — the injection point for production vs test
// ---------------------------------------------------------------------------

/// The "producer of [`MatrixWindowBatch`]es" abstraction that
/// [`RollupOp`] pulls from — narrower than [`Operator`] (only `schema` +
/// window-poll), so any producer of windowed samples can slot in.
///
/// Production: [`MatrixWindowSource`] wraps a [`MatrixSelectorOp`].
/// Subqueries: [`SubqueryOp`] implements it directly. Tests: hand-built
/// mocks.
///
/// There's no blanket impl for [`MatrixSelectorOp`] because its
/// `Operator::schema` is only exposed for `'static`, but `Rollup::schema`
/// must work for any `'a` before any poll; the wrapper snapshots the
/// schema at construction time to bridge the gap.
///
/// [`SubqueryOp`]: super::subquery::SubqueryOp
pub trait WindowStream: Send {
    fn schema(&self) -> &OperatorSchema;

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>>;
}

// Production wiring lives on [`MatrixWindowSource`] below: it captures
// the upstream schema at construction (which is plan-time-stable) and
// exposes it via `WindowStream::schema`. There is no direct
// `WindowStream for MatrixSelectorOp<'a, S>` impl because the latter's
// `Operator::schema` accessor is only available for `'static` in 3a.2,
// and `Rollup::schema` must be callable for any `'a` before any poll.

// ---------------------------------------------------------------------------
// Memory-accounted output buffers
// ---------------------------------------------------------------------------

#[inline]
fn out_bytes(cells: usize) -> usize {
    // Values + validity bitset (bytes ≈ cells / 8 + one word slack).
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl OutBuffers {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![0.0; cells],
            validity: BitSet::with_len(cells),
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::replace(&mut self.validity, BitSet::with_len(0));
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for OutBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// RollupOp — the operator
// ---------------------------------------------------------------------------

/// Implements every PromQL range function (`rate`, `*_over_time`, ...).
/// Pulls [`MatrixWindowBatch`]es from a [`WindowStream`] and reduces each
/// `(step, series)` window to one scalar via the reducer named by
/// [`RollupKind`], emitting a [`StepBatch`].
///
/// See module docs for the function taxonomy and extrapolation citation.
pub struct RollupOp<W: WindowStream> {
    child: W,
    kind: RollupKind,
    range_ms: i64,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    done: bool,
    errored: bool,
}

impl<W: WindowStream> RollupOp<W> {
    /// Construct a rollup over an arbitrary [`WindowStream`].
    ///
    /// * `child` — any window-stream producer; production = `MatrixSelectorOp`,
    ///   tests = hand-built mock.
    /// * `kind` — plan-time-selected rollup function.
    /// * `range_ms` — bracketed window in ms (same value passed to
    ///   `MatrixSelectorOp`). Used by [`RollupKind::Rate`] et al. for
    ///   extrapolation; `*_over_time` ignore it.
    /// * `reservation` — per-query reservation; cloned into the output
    ///   buffers.
    pub fn new(child: W, kind: RollupKind, range_ms: i64, reservation: MemoryReservation) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            kind,
            range_ms,
            reservation,
            schema,
            done: false,
            errored: false,
        }
    }

    fn reduce_batch(&self, window: &MatrixWindowBatch) -> Result<StepBatch, QueryError> {
        let step_count = window.step_count();
        let series_count = window.series_count();
        let cell_count = step_count * series_count;

        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;

        // Pre-slice the step timestamps covered by this window for
        // per-cell (window_start, window_end) math. Prefer the
        // `effective_times` column when present — the upstream operator
        // has folded `@` / offset into those, so the samples actually
        // live in `(effective - range, effective]` rather than the
        // `(step_t - range, step_t]` window that `step_timestamps`
        // would imply (RFC 0007 §6.3.8; at_modifier #21).
        let window_end_ts: &[i64] = match &window.effective_times {
            Some(eff) => &eff[window.step_range.clone()],
            None => &window.step_timestamps[window.step_range.clone()],
        };

        let eval_ts = &window.step_timestamps[window.step_range.clone()];
        for (step_off, (&window_end, &eval)) in window_end_ts
            .iter()
            .zip(eval_ts)
            .enumerate()
            .take(step_count)
        {
            let window_start = window_end.saturating_sub(self.range_ms);
            for series_off in 0..series_count {
                let (ts, vs) = window.cell_samples(step_off, series_off);
                let out_idx = step_off * series_count + series_off;
                if let Some(v) = self.kind.compute(window_start, window_end, eval, ts, vs) {
                    out.values[out_idx] = v;
                    out.validity.set(out_idx);
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            window.step_timestamps.clone(),
            window.step_range.clone(),
            window.series.clone(),
            window.series_range.clone(),
            values,
            validity,
        ))
    }
}

impl<W: WindowStream> Operator for RollupOp<W> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.child.poll_windows(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(window))) => match self.reduce_batch(&window) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Integration helper: wrap a MatrixSelectorOp without the schema trait gap
// ---------------------------------------------------------------------------

/// The production implementation of [`WindowStream`] — wraps a
/// [`MatrixSelectorOp`] so [`RollupOp`] can drive it for any lifetime
/// `'a`.
///
/// This wrapper exists because `MatrixSelectorOp::schema` is only
/// exposed through `Operator::schema` for the `'static` impl, but
/// `WindowStream::schema` has to be callable for any `'a` before any
/// poll. We resolve the gap by snapshotting the schema at construction
/// and owning it here.
pub struct MatrixWindowSource<'a, S: SeriesSource + Send + Sync + 'a> {
    inner: MatrixSelectorOp<'a, S>,
    schema_snapshot: OperatorSchema,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> MatrixWindowSource<'a, S> {
    /// Wrap a matrix selector, snapshotting its schema now.
    pub fn new(inner: MatrixSelectorOp<'a, S>, schema_snapshot: OperatorSchema) -> Self {
        Self {
            inner,
            schema_snapshot,
        }
    }
}

impl<'a, S: SeriesSource + Send + Sync + 'a> WindowStream for MatrixWindowSource<'a, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema_snapshot
    }

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        self.inner.windows(cx)
    }
}

// ---------------------------------------------------------------------------
// Per-function reducers
// ---------------------------------------------------------------------------

mod rollup_fns {
    /// Kahan-Neumaier compensated summation step. `#[inline(never)]`
    /// matches the existing implementation to guard against compiler
    /// reordering that would change IEEE-754 output (cf. Prometheus
    /// #16714).
    #[inline(never)]
    fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
        let t = sum + inc;
        let new_c = if t.is_infinite() {
            0.0
        } else if sum.abs() >= inc.abs() {
            c + ((sum - t) + inc)
        } else {
            c + ((inc - t) + sum)
        };
        (t, new_c)
    }

    fn counter_increase_correction(vs: &[f64]) -> f64 {
        let mut correction = 0.0;
        let mut prev = vs[0];
        for &v in &vs[1..] {
            if v < prev {
                correction += prev;
            }
            prev = v;
        }
        correction
    }

    /// Shared core of rate/increase/delta. Returns the extrapolation
    /// factor, the (possibly reset-corrected) raw delta, and the time
    /// difference in seconds between first and last sample.
    ///
    /// Kind == `IsCounter` toggles counter-reset correction.
    enum DeltaKind {
        Counter, // rate / increase
        Gauge,   // delta
    }

    fn compute_delta(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
        kind: DeltaKind,
    ) -> Option<(f64, f64)> {
        // caller guarantees ts.len() >= 2 via RollupKind::min_samples
        let n = ts.len();
        let range_seconds = (window_end_ms - window_start_ms) as f64 / 1000.0;
        let first_t = ts[0];
        let last_t = ts[n - 1];
        let first_v = vs[0];
        let last_v = vs[n - 1];

        let time_diff_seconds = (last_t - first_t) as f64 / 1000.0;
        if time_diff_seconds <= 0.0 {
            return None;
        }

        let mut result = last_v - first_v;
        if matches!(kind, DeltaKind::Counter) {
            result += counter_increase_correction(vs);
        }

        let num_minus_one = (n - 1) as f64;
        let avg_interval = time_diff_seconds / num_minus_one;
        let extrapolation_threshold = avg_interval * 1.1;

        let mut duration_to_start = (first_t - window_start_ms) as f64 / 1000.0;
        let mut duration_to_end = (window_end_ms - last_t) as f64 / 1000.0;

        if duration_to_start >= extrapolation_threshold {
            duration_to_start = avg_interval / 2.0;
        }

        // Counter-only zero-bound clip (identical to functions.rs:1060-1066).
        if matches!(kind, DeltaKind::Counter) {
            let mut duration_to_zero = duration_to_start;
            if result > 0.0 && first_v >= 0.0 {
                duration_to_zero = first_v * (time_diff_seconds / result);
            }
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }

        if duration_to_end >= extrapolation_threshold {
            duration_to_end = avg_interval / 2.0;
        }

        // `rate`: factor divides by range_seconds → result is per-second.
        // `increase`/`delta`: no division by range_seconds.
        let factor_unit =
            (time_diff_seconds + duration_to_start + duration_to_end) / time_diff_seconds;
        Some((result * factor_unit, range_seconds))
    }

    pub(super) fn extrapolated_rate(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        let (scaled, range_seconds) =
            compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Counter)?;
        if range_seconds <= 0.0 {
            return None;
        }
        Some(scaled / range_seconds)
    }

    pub(super) fn extrapolated_increase(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        // increase = rate * range_seconds — i.e. the `scaled` result
        // before dividing by range.
        compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Counter)
            .map(|(scaled, _)| scaled)
    }

    pub(super) fn extrapolated_delta(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Gauge)
            .map(|(scaled, _)| scaled)
    }

    pub(super) fn irate(ts: &[i64], vs: &[f64]) -> Option<f64> {
        let n = ts.len();
        // n >= 2 by min_samples
        let prev_t = ts[n - 2];
        let last_t = ts[n - 1];
        let prev_v = vs[n - 2];
        let last_v = vs[n - 1];
        let dt_seconds = (last_t - prev_t) as f64 / 1000.0;
        if dt_seconds <= 0.0 {
            return None;
        }
        // Counter-reset: if last < prev, the diff is `last` itself (as
        // if the series jumped to 0 and counted up to `last`).
        let diff = if last_v < prev_v {
            last_v
        } else {
            last_v - prev_v
        };
        Some(diff / dt_seconds)
    }

    pub(super) fn idelta(vs: &[f64]) -> Option<f64> {
        let n = vs.len();
        Some(vs[n - 1] - vs[n - 2])
    }

    pub(super) fn resets(vs: &[f64]) -> usize {
        let mut count = 0;
        for w in vs.windows(2) {
            if w[1] < w[0] {
                count += 1;
            }
        }
        count
    }

    pub(super) fn changes(vs: &[f64]) -> usize {
        let mut count = 0;
        for w in vs.windows(2) {
            let (a, b) = (w[0], w[1]);
            // NaN != NaN under `!=` — but PromQL treats NaN→NaN as not
            // a change. Follow Prometheus: both NaN ⇒ not a change.
            if a.is_nan() && b.is_nan() {
                continue;
            }
            if a != b {
                count += 1;
            }
        }
        count
    }

    pub(super) fn sum_over_time(vs: &[f64]) -> f64 {
        let mut sum = 0.0;
        let mut c = 0.0;
        for &v in vs {
            (sum, c) = kahan_inc(v, sum, c);
        }
        if sum.is_infinite() { sum } else { sum + c }
    }

    pub(super) fn avg_over_time(vs: &[f64]) -> f64 {
        // caller guarantees vs.len() >= 1
        if vs.len() == 1 {
            return vs[0];
        }
        let mut sum = vs[0];
        let mut c = 0.0;
        let mut mean = 0.0;
        let mut incremental = false;
        for (i, &v) in vs.iter().enumerate().skip(1) {
            let count = (i + 1) as f64;
            if !incremental {
                let (new_sum, new_c) = kahan_inc(v, sum, c);
                if !new_sum.is_infinite() {
                    sum = new_sum;
                    c = new_c;
                    continue;
                }
                incremental = true;
                mean = sum / (count - 1.0);
                c /= count - 1.0;
            }
            let q = (count - 1.0) / count;
            (mean, c) = kahan_inc(v / count, q * mean, q * c);
        }
        if incremental {
            mean + c
        } else {
            let count = vs.len() as f64;
            sum / count + c / count
        }
    }

    /// Prometheus min_over_time semantics: NaN is replaced by any real
    /// value; all-NaN stays NaN.
    pub(super) fn min_over_time(vs: &[f64]) -> f64 {
        let mut min = vs[0];
        for &v in &vs[1..] {
            if v < min || min.is_nan() {
                min = v;
            }
        }
        min
    }

    pub(super) fn max_over_time(vs: &[f64]) -> f64 {
        let mut max = vs[0];
        for &v in &vs[1..] {
            if v > max || max.is_nan() {
                max = v;
            }
        }
        max
    }

    /// Population variance via Welford with Kahan compensation on mean
    /// and M2 accumulators.
    pub(super) fn variance(vs: &[f64]) -> f64 {
        if vs.is_empty() {
            return f64::NAN;
        }
        let mut count = 0.0;
        let mut mean = 0.0;
        let mut c_mean = 0.0;
        let mut m2 = 0.0;
        let mut c_m2 = 0.0;
        for &v in vs {
            count += 1.0;
            let delta = v - (mean + c_mean);
            (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
            let new_delta = v - (mean + c_mean);
            (m2, c_m2) = kahan_inc(delta * new_delta, m2, c_m2);
        }
        (m2 + c_m2) / count
    }

    /// Least-squares `(slope, intercept)` with x in seconds relative to
    /// `intercept_ms`, ported from Prometheus `linearRegression`. A constant
    /// series has slope 0 (NaN when that constant is infinite).
    pub(super) fn linear_regression(ts: &[i64], vs: &[f64], intercept_ms: i64) -> (f64, f64) {
        let init_y = vs[0];
        if vs.iter().all(|&v| v == init_y) {
            return if init_y.is_infinite() {
                (f64::NAN, f64::NAN)
            } else {
                (0.0, init_y)
            };
        }
        let n = vs.len() as f64;
        let (mut sum_x, mut c_x) = (0.0, 0.0);
        let (mut sum_y, mut c_y) = (0.0, 0.0);
        let (mut sum_xy, mut c_xy) = (0.0, 0.0);
        let (mut sum_x2, mut c_x2) = (0.0, 0.0);
        for (&t, &y) in ts.iter().zip(vs) {
            let x = (t - intercept_ms) as f64 / 1e3;
            (sum_x, c_x) = kahan_inc(x, sum_x, c_x);
            (sum_y, c_y) = kahan_inc(y, sum_y, c_y);
            (sum_xy, c_xy) = kahan_inc(x * y, sum_xy, c_xy);
            (sum_x2, c_x2) = kahan_inc(x * x, sum_x2, c_x2);
        }
        let (sum_x, sum_y) = (sum_x + c_x, sum_y + c_y);
        let (sum_xy, sum_x2) = (sum_xy + c_xy, sum_x2 + c_x2);
        let cov_xy = sum_xy - sum_x * sum_y / n;
        let var_x = sum_x2 - sum_x * sum_x / n;
        let slope = cov_xy / var_x;
        (slope, sum_y / n - slope * sum_x / n)
    }

    /// Prometheus quantile_over_time: linear interpolation between ranks.
    ///
    /// - `q < 0` ⇒ `-inf`
    /// - `q > 1` ⇒ `+inf`
    /// - empty ⇒ caller filtered via `min_samples`; returns `NaN` here
    ///   as a guard but is unreachable in normal flow.
    pub(super) fn quantile(q: f64, vs: &[f64]) -> f64 {
        if vs.is_empty() {
            return f64::NAN;
        }
        if q.is_nan() {
            return f64::NAN;
        }
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        let mut sorted: Vec<f64> = vs.to_vec();
        // `total_cmp` handles NaN deterministically, sorting NaN to the
        // end. Prometheus does the same via Go's sort.Float64s.
        sorted.sort_by(|a, b| a.total_cmp(b));
        let n = sorted.len();
        if n == 1 {
            return sorted[0];
        }
        let rank = q * (n - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        if lo == hi {
            return sorted[lo];
        }
        let weight = rank - lo as f64;
        sorted[lo] * (1.0 - weight) + sorted[hi] * weight
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
