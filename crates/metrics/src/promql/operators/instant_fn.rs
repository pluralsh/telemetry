//! `InstantFnOp` implements PromQL's pointwise scalar functions (`abs`,
//! `ln`, `round`, `clamp`, ...) — every function that takes one sample
//! and produces one sample at the same `(step, series)` position. One
//! operator type, with the specific function selected by an
//! [`InstantFnKind`] enum; dispatch is a single `match` per batch.
//!
//! Because the transformation is cell-for-cell, the child's series schema
//! and step grid pass through unchanged. The operator never rearranges
//! series and never buffers across steps.
//!
//! Scope: every pointwise float→float function the legacy engine ships,
//! plus `sgn`. Excluded: `absent` / `absent_over_time` (see
//! [`super::absent`]), `histogram_quantile` / `histogram_fraction` (see
//! [`super::histogram`]), `scalar` / `vector` / `pi` / `time`
//! (planner-level coercions),
//! `label_replace` / `label_join` (label mutation — see [`super::label_manip`]).
//!
//! `timestamp()` returns the value of
//! [`StepBatch::source_timestamps`](super::super::batch::StepBatch::source_timestamps)
//! for the cell when present, else falls back to the step timestamp (in
//! seconds). [`VectorSelectorOp`](super::vector_selector::VectorSelectorOp)
//! is the only operator that populates `source_timestamps`; any operator
//! that derives a new value drops the column.
//!
//! Validity: `InstantFnOp` never flips bits — a clear input cell stays
//! clear, a valid input cell stays valid even when the result is `NaN` /
//! `±Inf`. The output batch's `validity` is pointer-cloned from the input;
//! only `values` is charged through [`MemoryReservation::try_grow`].

use std::task::{Context, Poll};

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};

use super::super::batch::{BitSet, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

// ---------------------------------------------------------------------------
// InstantFnKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-cell reducer. `Round` / `Clamp*` carry their plan-time scalar args
/// inline (constant across steps and series per PromQL).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InstantFnKind {
    // --- unary math / transcendentals ---
    Abs,
    Ceil,
    Floor,
    Exp,
    Ln,
    Log2,
    Log10,
    Sqrt,

    // --- trig ---
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,

    // --- hyperbolic ---
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,

    // --- angular conversion ---
    Deg,
    Rad,

    // --- sign ---
    Sgn,

    // --- round with plan-time `to_nearest` (defaults to 1.0 if unset) ---
    /// `round(v, to_nearest)` — rounds to the nearest multiple of
    /// `to_nearest`, tie-breaking toward `+∞` (matching the legacy engine
    /// at `timeseries/src/promql/functions.rs:216-223`). `to_nearest == 0`
    /// is a no-op per that impl; legal values are any real.
    Round {
        to_nearest: f64,
    },

    // --- clamp family with plan-time scalar bounds ---
    /// `clamp(v, min, max)` — NaN-propagating; `min > max` is caller's
    /// concern (legacy returns an empty vector in that case; this
    /// operator is not in charge of shaping — the planner emits an empty
    /// schema when it detects the case).
    Clamp {
        min: f64,
        max: f64,
    },
    ClampMin {
        min: f64,
    },
    ClampMax {
        max: f64,
    },

    /// `timestamp(v)` — returns the matching **source sample's**
    /// timestamp in seconds when the input cell carries one (the common
    /// case: `timestamp(metric @ t)` and similar bare-selector inputs,
    /// populated by `VectorSelectorOp` into
    /// `StepBatch::source_timestamps`). Falls back to the output step
    /// timestamp when the input is derived (e.g. `timestamp(rate(…))` —
    /// any operator between the source and `timestamp()` drops the
    /// source-timestamp column, matching Prometheus semantics at
    /// `at_modifier.test:193–201`).
    Timestamp,

    /// Calendar/date extraction functions operating on seconds-since-epoch
    /// values (or `vector(time())` for the zero-arg forms).
    Year,
    Month,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    Hour,
    Minute,
    DaysInMonth,

    /// Native-histogram inspection (`histogram_count`, `histogram_sum`,
    /// `histogram_avg`, `histogram_stddev`, `histogram_stdvar`). Read only
    /// histogram cells; float cells are dropped.
    HistogramCount,
    HistogramSum,
    HistogramAvg,
    HistogramStddev,
    HistogramStdvar,
}

impl InstantFnKind {
    fn histogram_stat(&self, h: &crate::histogram::FloatHistogram) -> Option<f64> {
        match *self {
            Self::HistogramCount => Some(h.count),
            Self::HistogramSum => Some(h.sum),
            Self::HistogramAvg => Some(h.sum / h.count),
            Self::HistogramStddev => Some(crate::histogram::variance(h).sqrt()),
            Self::HistogramStdvar => Some(crate::histogram::variance(h)),
            _ => None,
        }
    }

    fn reads_histograms_only(&self) -> bool {
        matches!(
            self,
            Self::HistogramCount
                | Self::HistogramSum
                | Self::HistogramAvg
                | Self::HistogramStddev
                | Self::HistogramStdvar
        )
    }

    /// Reduce a single cell's `(value, step_timestamp_ms)` to a scalar.
    ///
    /// `step_timestamp_ms` is the output step timestamp for the cell (in
    /// absolute UTC ms); only [`Self::Timestamp`] reads it. Called only
    /// for cells whose input validity bit is set.
    #[inline]
    pub fn compute(&self, v: f64, step_timestamp_ms: i64) -> f64 {
        match *self {
            Self::Abs => v.abs(),
            Self::Ceil => v.ceil(),
            Self::Floor => v.floor(),
            Self::Exp => v.exp(),
            Self::Ln => v.ln(),
            Self::Log2 => v.log2(),
            Self::Log10 => v.log10(),
            Self::Sqrt => v.sqrt(),
            Self::Sin => v.sin(),
            Self::Cos => v.cos(),
            Self::Tan => v.tan(),
            Self::Asin => v.asin(),
            Self::Acos => v.acos(),
            Self::Atan => v.atan(),
            Self::Sinh => v.sinh(),
            Self::Cosh => v.cosh(),
            Self::Tanh => v.tanh(),
            Self::Asinh => v.asinh(),
            Self::Acosh => v.acosh(),
            Self::Atanh => v.atanh(),
            Self::Deg => deg(v),
            Self::Rad => rad(v),
            Self::Sgn => sgn(v),
            Self::Round { to_nearest } => round_to_nearest(v, to_nearest),
            Self::Clamp { min, max } => clamp_nan_aware(v, min, max),
            Self::ClampMin { min } => max_nan_aware(v, min),
            Self::ClampMax { max } => min_nan_aware(v, max),
            Self::Timestamp => timestamp_seconds(step_timestamp_ms),
            Self::Year => datetime_from_seconds(v)
                .map(|dt| dt.year() as f64)
                .unwrap_or(f64::NAN),
            Self::Month => datetime_from_seconds(v)
                .map(|dt| dt.month() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfMonth => datetime_from_seconds(v)
                .map(|dt| dt.day() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfYear => datetime_from_seconds(v)
                .map(|dt| dt.ordinal() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfWeek => datetime_from_seconds(v)
                .map(|dt| dt.weekday().num_days_from_sunday() as f64)
                .unwrap_or(f64::NAN),
            Self::Hour => datetime_from_seconds(v)
                .map(|dt| dt.hour() as f64)
                .unwrap_or(f64::NAN),
            Self::Minute => datetime_from_seconds(v)
                .map(|dt| dt.minute() as f64)
                .unwrap_or(f64::NAN),
            Self::DaysInMonth => datetime_from_seconds(v)
                .map(|dt| days_in_month(dt) as f64)
                .unwrap_or(f64::NAN),
            Self::HistogramCount
            | Self::HistogramSum
            | Self::HistogramAvg
            | Self::HistogramStddev
            | Self::HistogramStdvar => f64::NAN,
        }
    }
}

#[inline]
fn datetime_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }

    let seconds = value.trunc();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&seconds) {
        return None;
    }

    DateTime::from_timestamp(seconds as i64, 0)
}

#[inline]
fn days_in_month(dt: DateTime<Utc>) -> u32 {
    let start_of_month =
        NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).expect("valid start of month");
    let start_of_next_month = if dt.month() == 12 {
        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1).expect("valid start of next month")
    } else {
        NaiveDate::from_ymd_opt(dt.year(), dt.month() + 1, 1).expect("valid start of next month")
    };

    start_of_next_month
        .signed_duration_since(start_of_month)
        .num_days() as u32
}

/// Prometheus' `v * 180 / math.Pi`; `to_degrees` multiplies by the
/// folded constant, which can round differently.
#[inline]
fn deg(v: f64) -> f64 {
    v * 180.0 / std::f64::consts::PI
}

/// Prometheus' `v * math.Pi / 180`.
#[inline]
fn rad(v: f64) -> f64 {
    v * std::f64::consts::PI / 180.0
}

/// `round(v, to_nearest)` — ported from
/// `timeseries/src/promql/functions.rs:216-223`. Matches Prometheus'
/// half-up rounding: `(v * inv + 0.5).floor() / inv`.
#[inline]
fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    if to_nearest == 0.0 {
        return value;
    }
    let inv = 1.0 / to_nearest;
    (value * inv + 0.5).floor() / inv
}

/// NaN-propagating `min`. Matches `timeseries/src/promql/functions.rs:269-277`.
#[inline]
fn min_nan_aware(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left < right {
        left
    } else {
        right
    }
}

/// NaN-propagating `max`. Matches `timeseries/src/promql/functions.rs:279-287`.
#[inline]
fn max_nan_aware(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left > right {
        left
    } else {
        right
    }
}

/// `clamp(v, min, max)` — NaN-propagating, matches the legacy engine's
/// `max(min(v, max), min)` composition at
/// `timeseries/src/promql/functions.rs:441`.
#[inline]
fn clamp_nan_aware(v: f64, min: f64, max: f64) -> f64 {
    max_nan_aware(min_nan_aware(v, max), min)
}

#[inline]
fn timestamp_seconds(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

/// Prometheus semantics: -1, 0, +1; NaN passes through.
#[inline]
fn sgn(v: f64) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if v > 0.0 {
        1.0
    } else if v < 0.0 {
        -1.0
    } else {
        0.0
    }
}

/// `kind` applied to every cell, valid or not, for kinds that are total
/// over `f64` (no panics, no per-cell state). `None` for the calendar and
/// histogram kinds, which must only see valid cells.
fn map_total(
    kind: InstantFnKind,
    input: &[f64],
    step_ts: &[i64],
    source_ts: Option<&[i64]>,
    series_count: usize,
) -> Option<Vec<f64>> {
    #[inline(always)]
    fn map(input: &[f64], f: impl Fn(f64) -> f64) -> Vec<f64> {
        input.iter().map(|&v| f(v)).collect()
    }
    use InstantFnKind as K;
    Some(match kind {
        K::Abs => map(input, f64::abs),
        K::Ceil => map(input, f64::ceil),
        K::Floor => map(input, f64::floor),
        K::Exp => map(input, f64::exp),
        K::Ln => map(input, f64::ln),
        K::Log2 => map(input, f64::log2),
        K::Log10 => map(input, f64::log10),
        K::Sqrt => map(input, f64::sqrt),
        K::Sin => map(input, f64::sin),
        K::Cos => map(input, f64::cos),
        K::Tan => map(input, f64::tan),
        K::Asin => map(input, f64::asin),
        K::Acos => map(input, f64::acos),
        K::Atan => map(input, f64::atan),
        K::Sinh => map(input, f64::sinh),
        K::Cosh => map(input, f64::cosh),
        K::Tanh => map(input, f64::tanh),
        K::Asinh => map(input, f64::asinh),
        K::Acosh => map(input, f64::acosh),
        K::Atanh => map(input, f64::atanh),
        K::Deg => map(input, deg),
        K::Rad => map(input, rad),
        K::Sgn => map(input, sgn),
        K::Round { to_nearest } => map(input, |v| round_to_nearest(v, to_nearest)),
        K::Clamp { min, max } => map(input, |v| clamp_nan_aware(v, min, max)),
        K::ClampMin { min } => map(input, |v| max_nan_aware(v, min)),
        K::ClampMax { max } => map(input, |v| min_nan_aware(v, max)),
        K::Timestamp => match source_ts {
            Some(ts) => ts[..input.len()]
                .iter()
                .map(|&ms| timestamp_seconds(ms))
                .collect(),
            None => {
                let mut values = Vec::with_capacity(input.len());
                for &step_ms in step_ts {
                    values.extend(std::iter::repeat_n(
                        timestamp_seconds(step_ms),
                        series_count,
                    ));
                }
                values
            }
        },
        K::Year
        | K::Month
        | K::DayOfMonth
        | K::DayOfYear
        | K::DayOfWeek
        | K::Hour
        | K::Minute
        | K::DaysInMonth
        | K::HistogramCount
        | K::HistogramSum
        | K::HistogramAvg
        | K::HistogramStddev
        | K::HistogramStdvar => return None,
    })
}

// ---------------------------------------------------------------------------
// Memory-accounted output buffer (values only — validity is pointer-cloned)
// ---------------------------------------------------------------------------

#[inline]
fn values_bytes(cells: usize) -> usize {
    cells.saturating_mul(std::mem::size_of::<f64>())
}

/// RAII wrapper around a fresh `Vec<f64>` output column. Reserves on
/// construction; releases on `Drop`. `finish()` transfers the `Vec` to
/// the caller and releases the reservation. Downstream re-reserves if
/// it retains the batch.
struct OutValues {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
}

impl OutValues {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        // NaN-fill so accidental reads of invalid cells surface as NaN
        // rather than zero — matches the 3a.1 convention.
        Self::with_values(reservation, cells, || vec![f64::NAN; cells])
    }

    /// Charges `cells` values before `build` allocates them.
    fn with_values(
        reservation: &MemoryReservation,
        cells: usize,
        build: impl FnOnce() -> Vec<f64>,
    ) -> Result<Self, QueryError> {
        let bytes = values_bytes(cells);
        reservation.try_grow(bytes)?;
        let values = build();
        debug_assert_eq!(values.len(), cells);
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values,
        })
    }

    fn finish(mut self) -> Vec<f64> {
        let values = std::mem::take(&mut self.values);
        self.reservation.release(self.bytes);
        self.bytes = 0;
        values
    }
}

impl Drop for OutValues {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// InstantFnOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's pointwise functions (`abs`, `ln`, `round`, ...).
/// Applies the reducer named by [`InstantFnKind`] to each cell in every
/// [`StepBatch`] pulled from `child`, preserving the child's series
/// schema and step grid.
///
/// See module docs for function taxonomy, timestamp semantics, and the
/// validity / memory-accounting policy.
pub struct InstantFnOp<C: Operator> {
    child: C,
    kind: InstantFnKind,
    reservation: MemoryReservation,
    /// Passthrough of the child's schema — identical series roster, step
    /// grid, and deferred-ness. Captured at construction so the operator
    /// can hand out `&OperatorSchema` without re-querying the child.
    schema: OperatorSchema,
    errored: bool,
    done: bool,
}

impl<C: Operator> InstantFnOp<C> {
    /// Wrap `child` with a pointwise reducer.
    ///
    /// * `child` — any upstream operator producing `StepBatch`es.
    /// * `kind` — plan-time-selected pointwise function (with any plan-time
    ///   scalar arguments baked in; see [`InstantFnKind`]).
    /// * `reservation` — per-query reservation the output values column
    ///   is charged against.
    pub fn new(child: C, kind: InstantFnKind, reservation: MemoryReservation) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            kind,
            reservation,
            schema,
            errored: false,
            done: false,
        }
    }

    fn apply_batch(&self, batch: StepBatch) -> Result<StepBatch, QueryError> {
        let cell_count = batch.len();

        if let InstantFnKind::Clamp { min, max } = self.kind
            && min > max
        {
            let values = OutValues::allocate(&self.reservation, cell_count)?.finish();
            return Ok(StepBatch::new(
                batch.step_timestamps.clone(),
                batch.step_range.clone(),
                batch.series.clone(),
                batch.series_range.clone(),
                values,
                BitSet::with_len(cell_count),
            ));
        }

        if self.kind.reads_histograms_only() {
            let mut out = OutValues::allocate(&self.reservation, cell_count)?;
            let mut validity = BitSet::with_len(cell_count);
            if let Some(cells) = &batch.histograms {
                for (idx, cell) in cells.iter().enumerate() {
                    if let Some(h) = cell
                        && let Some(v) = self.kind.histogram_stat(h)
                    {
                        out.values[idx] = v;
                        validity.set(idx);
                    }
                }
            }
            let values = out.finish();
            return Ok(StepBatch::new(
                batch.step_timestamps.clone(),
                batch.step_range.clone(),
                batch.series.clone(),
                batch.series_range.clone(),
                values,
                validity,
            ));
        }

        let series_count = batch.series_count();
        let step_ts = batch.step_timestamps_slice();

        // Total kinds run over every cell of the row-major `values` in one
        // loop per kind; the rest (calendar functions) only touch valid
        // cells. Either way invalid cells end up with the NaN fill and the
        // pointer-cloned validity bit clear. `timestamp()` prefers the
        // source-sample timestamp when the input batch carries one (RFC
        // 0007 §6.3.7); all other kinds ignore the column.
        let source_ts: Option<&[i64]> = batch.source_timestamps.as_deref();
        let input = &batch.values[..cell_count];
        let kind = self.kind;
        let mut out = OutValues::with_values(&self.reservation, cell_count, || {
            map_total(kind, input, step_ts, source_ts, series_count).unwrap_or_else(|| {
                let mut values = vec![f64::NAN; cell_count];
                batch
                    .validity
                    .for_each_set(|idx| values[idx] = kind.compute(input[idx], 0));
                values
            })
        })?;
        batch
            .validity
            .for_each_clear(|idx| out.values[idx] = f64::NAN);

        // Only `timestamp()` accepts histogram samples; every other
        // function drops them, which clearing the column achieves.
        let mut validity = batch.validity.clone();
        if let (InstantFnKind::Timestamp, Some(cells)) = (self.kind, &batch.histograms) {
            for (idx, cell) in cells.iter().enumerate() {
                if cell.is_some() {
                    let step_ms = step_ts[idx / series_count.max(1)];
                    let ts = source_ts.map_or(step_ms, |ts| ts[idx]);
                    out.values[idx] = self.kind.compute(f64::NAN, ts);
                    validity.set(idx);
                }
            }
        }

        let values = out.finish();
        Ok(StepBatch::new(
            batch.step_timestamps.clone(),
            batch.step_range.clone(),
            batch.series.clone(),
            batch.series_range.clone(),
            values,
            // The function never produces new absences, so the output
            // validity matches the input bit-for-bit (plus histogram cells
            // for `timestamp()`). See the module-level "Validity policy".
            validity,
        ))
    }
}

impl<C: Operator> Operator for InstantFnOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.child.next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(batch))) => match self.apply_batch(batch) {
                Ok(out) => Poll::Ready(Some(Ok(out))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
