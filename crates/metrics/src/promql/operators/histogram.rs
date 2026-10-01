//! `HistogramOp` implements PromQL's `histogram_quantile(φ, b)` and
//! `histogram_fraction(lower, upper, b)` over both `le`-labelled
//! cumulative bucket series and native histogram samples.
//!
//! The planner resolves, per input series, its bucket upper bound (parsed
//! from `le`) and its output group (labels minus `le` and `__name__`).
//! Series without a parseable `le` become native inputs, one output group
//! per labelset minus `__name__`; only their histogram samples count. One
//! group's buckets can arrive spread across several series-chunked
//! batches, so this is a pipeline breaker: it buffers the child's cells
//! densely, then evaluates every `(group, step)` into one full-grid batch.
//!
//! As in Prometheus, a native histogram whose labelset equals a classic
//! bucket group's (labels minus `le`, name included) at the same step
//! suppresses both outputs.

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::histogram::FloatHistogram;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

/// Relative delta between adjacent bucket counts below which the
/// difference is treated as floating-point noise (Prometheus'
/// `smallDeltaTolerance`).
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// Which classic-histogram function to evaluate. Parameters are
/// plan-time constants.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HistogramFnKind {
    /// `histogram_quantile(φ, b)`.
    Quantile(f64),
    /// `histogram_fraction(lower, upper, b)`.
    Fraction { lower: f64, upper: f64 },
}

impl HistogramFnKind {
    /// Evaluate over one group's buckets at one step. Reorders and
    /// truncates `buckets` in place.
    pub fn evaluate(&self, buckets: &mut Vec<Bucket>) -> f64 {
        match *self {
            Self::Quantile(q) => bucket_quantile(q, buckets),
            Self::Fraction { lower, upper } => bucket_fraction(lower, upper, buckets),
        }
    }

    pub fn evaluate_native(&self, h: &FloatHistogram) -> f64 {
        match *self {
            Self::Quantile(q) => crate::histogram::quantile(q, h),
            Self::Fraction { lower, upper } => crate::histogram::fraction(lower, upper, h),
        }
    }
}

/// One classic-histogram bucket: cumulative `count` of observations
/// `<= upper_bound`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// Port of Prometheus' `BucketQuantile`: linear interpolation inside the
/// bucket holding rank `q * observations`, a natural lower bound of 0 when
/// the lowest bucket is positive, and the second-highest bound when the
/// rank lands in the `+Inf` bucket.
pub fn bucket_quantile(q: f64, buckets: &mut Vec<Bucket>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if !sort_and_check_inf(buckets) {
        return f64::NAN;
    }
    coalesce(buckets);
    ensure_monotonic(buckets);

    let n = buckets.len();
    if n < 2 {
        return f64::NAN;
    }
    let observations = buckets[n - 1].count;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let b = buckets[..n - 1].partition_point(|bucket| bucket.count < rank);

    if b == n - 1 {
        return buckets[n - 2].upper_bound;
    }
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }
    let bucket_end = buckets[b].upper_bound;
    let mut count = buckets[b].count;
    let mut bucket_start = 0.0;
    if b > 0 {
        bucket_start = buckets[b - 1].upper_bound;
        count -= buckets[b - 1].count;
        rank -= buckets[b - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// Port of Prometheus' `BucketFraction`: the estimated share of
/// observations in `(lower, upper]`, interpolating linearly inside the
/// buckets holding each bound.
pub fn bucket_fraction(lower: f64, upper: f64, buckets: &mut Vec<Bucket>) -> f64 {
    if !sort_and_check_inf(buckets) {
        return f64::NAN;
    }
    coalesce(buckets);

    let count = buckets[buckets.len() - 1].count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0;
    let mut lower_rank = None;
    let mut upper_rank = None;
    let mut lower_bound = if buckets[0].upper_bound <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };

    for (i, bucket) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].upper_bound;
        }
        let upper_bound = bucket.upper_bound;
        // Infinite-width buckets contribute nothing to interpolation: for
        // `+Inf` upper bounds the formula collapses to `rank`, and a `-Inf`
        // lower bound is special-cased to the bucket's cumulative count.
        let interpolate = |v: f64| {
            if lower_bound == f64::NEG_INFINITY {
                bucket.count
            } else {
                rank + (bucket.count - rank) * (v - lower_bound) / (upper_bound - lower_bound)
            }
        };

        if lower_rank.is_none() && lower_bound >= lower {
            lower_rank = Some(rank);
        }
        if upper_rank.is_none() && lower_bound >= upper {
            upper_rank = Some(rank);
        }
        if lower_rank.is_some() && upper_rank.is_some() {
            break;
        }
        if lower_rank.is_none() && lower_bound < lower && upper_bound > lower {
            lower_rank = Some(interpolate(lower));
        }
        if upper_rank.is_none() && lower_bound < upper && upper_bound > upper {
            upper_rank = Some(interpolate(upper));
        }
        if lower_rank.is_some() && upper_rank.is_some() {
            break;
        }
        rank = bucket.count;
    }

    let lower_rank = lower_rank.map_or(count, |r| r.min(count));
    let upper_rank = upper_rank.map_or(count, |r| r.min(count));
    (upper_rank - lower_rank) / count
}

/// Sort by upper bound; `false` when the highest bucket is not `+Inf`
/// (or there are no buckets), which both functions answer with `NaN`.
fn sort_and_check_inf(buckets: &mut [Bucket]) -> bool {
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    buckets
        .last()
        .is_some_and(|bucket| bucket.upper_bound == f64::INFINITY)
}

/// Merge buckets sharing an upper bound. Input must be sorted.
fn coalesce(buckets: &mut Vec<Bucket>) {
    buckets.dedup_by(|next, kept| {
        if next.upper_bound == kept.upper_bound {
            kept.count += next.count;
            true
        } else {
            false
        }
    });
}

/// Ignore numerically insignificant deltas between adjacent buckets, then
/// flatten any decreases so counts are non-decreasing (Prometheus'
/// `ensureMonotonicAndIgnoreSmallDeltas`).
fn ensure_monotonic(buckets: &mut [Bucket]) {
    let mut prev = buckets[0].count;
    for bucket in &mut buckets[1..] {
        let curr = bucket.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, SMALL_DELTA_TOLERANCE) || curr < prev {
            bucket.count = prev;
            continue;
        }
        prev = curr;
    }
}

/// Port of Prometheus' `util/almost.Equal`.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

// ---------------------------------------------------------------------------
// HistogramOp
// ---------------------------------------------------------------------------

/// Where one input series lands: its output group and bucket bound.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketSeries {
    pub group: u32,
    pub upper_bound: f64,
}

/// Where one native-histogram input series lands: its output group, and
/// the classic group whose labels-minus-`le` equal this series' labels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NativeSeries {
    pub group: u32,
    pub classic_conflict: Option<u32>,
}

/// Evaluates a [`HistogramFnKind`] per `(group, step)`. The output schema
/// is one series per group, supplied by the planner.
pub struct HistogramOp<C: Operator> {
    child: C,
    kind: HistogramFnKind,
    /// Input series indices per group, with their bucket bounds.
    members: Vec<Vec<(usize, f64)>>,
    input_series: usize,
    /// Native inputs in input-series order; `native_slot[series]` indexes it.
    natives: Vec<NativeSeries>,
    native_slot: Vec<Option<usize>>,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Dense `[input_series][step]` copy of the child's output, allocated
    /// on first poll.
    buffer: Option<(Vec<f64>, BitSet)>,
    /// Dense `[native][step]` histogram samples, allocated on first poll.
    hist_buffer: Vec<Option<Arc<FloatHistogram>>>,
    step_timestamps: Option<Arc<[i64]>>,
    buffer_bytes: usize,
    done: bool,
}

impl<C: Operator> HistogramOp<C> {
    /// `inputs[i]` places input series `i`; `None` drops it.
    pub fn new(
        child: C,
        kind: HistogramFnKind,
        inputs: &[Option<BucketSeries>],
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Self {
        let step_grid = child.schema().step_grid;
        let mut members = vec![Vec::new(); output_schema.len()];
        for (series, input) in inputs.iter().enumerate() {
            if let Some(input) = input {
                members[input.group as usize].push((series, input.upper_bound));
            }
        }
        Self {
            child,
            kind,
            members,
            input_series: inputs.len(),
            natives: Vec::new(),
            native_slot: vec![None; inputs.len()],
            reservation,
            schema: OperatorSchema::new(SchemaRef::Static(output_schema), step_grid),
            buffer: None,
            hist_buffer: Vec::new(),
            step_timestamps: None,
            buffer_bytes: 0,
            done: false,
        }
    }

    /// `natives[i]` places input series `i`'s native histogram samples;
    /// `None` ignores them.
    pub fn with_natives(mut self, natives: &[Option<NativeSeries>]) -> Self {
        for (series, native) in natives.iter().enumerate() {
            if let Some(native) = native
                && series < self.native_slot.len()
            {
                self.native_slot[series] = Some(self.natives.len());
                self.natives.push(*native);
            }
        }
        self
    }

    fn step_count(&self) -> usize {
        self.schema.step_grid.step_count
    }

    fn absorb(&mut self, batch: &StepBatch) -> Result<(), QueryError> {
        let step_count = self.step_count();
        if self.buffer.is_none() {
            let cells = self.input_series.saturating_mul(step_count);
            let native_cells = self.natives.len().saturating_mul(step_count);
            let bytes = cell_bytes(cells).saturating_add(
                native_cells.saturating_mul(std::mem::size_of::<Option<Arc<FloatHistogram>>>()),
            );
            self.reservation.try_grow(bytes)?;
            self.buffer_bytes = bytes;
            self.buffer = Some((vec![0.0; cells], BitSet::with_len(cells)));
            self.hist_buffer = vec![None; native_cells];
        }
        if let Some(cells) = &batch.histograms {
            let series_count = batch.series_count();
            for (cell, h) in cells.iter().enumerate() {
                let Some(h) = h else { continue };
                let series = batch.series_range.start + cell % series_count;
                let Some(native) = self.native_slot.get(series).copied().flatten() else {
                    continue;
                };
                let step = batch.step_range.start + cell / series_count;
                let bytes = super::vector_selector::histogram_sample_bytes(h);
                self.reservation.try_grow(bytes)?;
                self.buffer_bytes += bytes;
                self.hist_buffer[native * step_count + step] = Some(h.clone());
            }
        }
        if self.step_timestamps.is_none() {
            self.step_timestamps = Some(batch.step_timestamps.clone());
        }
        let (values, validity) = self.buffer.as_mut().expect("buffer allocated above");
        let series_count = batch.series_count();
        for step_off in 0..batch.step_count() {
            let step = batch.step_range.start + step_off;
            for series_off in 0..series_count {
                let cell = step_off * series_count + series_off;
                if !batch.validity.get(cell) {
                    continue;
                }
                let series = batch.series_range.start + series_off;
                let slot = series * step_count + step;
                values[slot] = batch.values[cell];
                validity.set(slot);
            }
        }
        Ok(())
    }

    fn finalise(&mut self) -> Result<StepBatch, QueryError> {
        let step_count = self.step_count();
        let group_count = self.members.len();
        let cells = step_count.saturating_mul(group_count);
        let out_bytes = cell_bytes(cells);
        self.reservation.try_grow(out_bytes)?;
        let mut out_values = vec![0.0; cells];
        let mut out_validity = BitSet::with_len(cells);

        if let Some((values, validity)) = &self.buffer {
            let mut buckets = Vec::new();
            for (group, members) in self.members.iter().enumerate() {
                for step in 0..step_count {
                    buckets.clear();
                    buckets.extend(members.iter().filter_map(|&(series, upper_bound)| {
                        let slot = series * step_count + step;
                        validity.get(slot).then(|| Bucket {
                            upper_bound,
                            count: values[slot],
                        })
                    }));
                    if buckets.is_empty() {
                        continue;
                    }
                    let cell = step * group_count + group;
                    out_values[cell] = self.kind.evaluate(&mut buckets);
                    out_validity.set(cell);
                }
            }
        }
        let classic_validity = out_validity.clone();
        for (native_idx, native) in self.natives.iter().enumerate() {
            for step in 0..step_count {
                let Some(h) = &self.hist_buffer[native_idx * step_count + step] else {
                    continue;
                };
                if let Some(conflict) = native.classic_conflict {
                    let conflict_cell = step * group_count + conflict as usize;
                    if classic_validity.get(conflict_cell) {
                        out_validity.clear(conflict_cell);
                        continue;
                    }
                }
                let cell = step * group_count + native.group as usize;
                if out_validity.get(cell) || classic_validity.get(cell) {
                    continue;
                }
                out_values[cell] = self.kind.evaluate_native(h);
                out_validity.set(cell);
            }
        }
        self.release_buffer();
        self.reservation.release(out_bytes);

        let step_timestamps = self
            .step_timestamps
            .take()
            .unwrap_or_else(|| grid_timestamps(&self.schema));
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            self.schema.series.clone(),
            0..group_count,
            out_values,
            out_validity,
        ))
    }

    fn release_buffer(&mut self) {
        self.buffer = None;
        self.hist_buffer = Vec::new();
        if self.buffer_bytes > 0 {
            self.reservation.release(self.buffer_bytes);
            self.buffer_bytes = 0;
        }
    }
}

impl<C: Operator> Operator for HistogramOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match self.child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(err) = self.absorb(&batch) {
                        self.done = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Some(self.finalise()));
                }
            }
        }
    }
}

impl<C: Operator> Drop for HistogramOp<C> {
    fn drop(&mut self) {
        self.release_buffer();
    }
}

/// Bytes for a dense `f64` value buffer plus its validity bitmap.
pub(super) fn cell_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// Step timestamps derived from the grid, for breakers whose child
/// emitted no batch to copy them from.
pub(super) fn grid_timestamps(schema: &OperatorSchema) -> Arc<[i64]> {
    let grid = schema.step_grid;
    (0..grid.step_count as i64)
        .map(|i| grid.start_ms + i * grid.step_ms)
        .collect()
}

#[cfg(test)]
mod tests;
