//! Prometheus native histograms.
//!
//! [`FloatHistogram`] mirrors Prometheus' `histogram.FloatHistogram`: bucket
//! counts are absolute `f64`s, bucket boundaries come either from an
//! exponential schema (`-4..=8`) or from explicit custom bounds (schema
//! `-53`, "NHCB"). Buckets are stored sparsely as sorted `(index, count)`
//! pairs rather than Prometheus' span encoding; spans only exist at the
//! wire boundaries ([`FloatHistogram::from_spans`] / [`to_spans`]).
//!
//! Explicitly present buckets with a count of zero are kept until
//! [`FloatHistogram::compact`] runs, because some Prometheus semantics
//! (e.g. quantile estimation in the zero bucket) depend on whether a side
//! has any buckets at all, populated or not. Operations call `compact` in
//! exactly the places Prometheus calls `Compact(0)`.
//!
//! The arithmetic, reset detection, quantile and fraction estimation are
//! ports of `model/histogram/float_histogram.go` and `promql/quantile.go`.

use std::sync::{Arc, OnceLock};

use crate::model::{STALE_NAN, is_stale_nan};
use crate::util::kahan_inc;

/// Schema number reserved for custom-bounds histograms (NHCB).
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;
pub const MIN_EXPONENTIAL_SCHEMA: i32 = -4;
pub const MAX_EXPONENTIAL_SCHEMA: i32 = 8;
/// Schemas above [`MAX_EXPONENTIAL_SCHEMA`] up to this value are accepted on
/// ingest and reduced to [`MAX_EXPONENTIAL_SCHEMA`], as Prometheus does.
pub const MAX_REDUCIBLE_SCHEMA: i32 = 52;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CounterResetHint {
    #[default]
    Unknown,
    CounterReset,
    NotCounterReset,
    Gauge,
}

/// Span-encoded bucket run: `offset` is the gap to the previous span (the
/// absolute starting index for the first span).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub offset: i32,
    pub length: u32,
}

/// One sparse bucket: schema-relative index and absolute count.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bucket {
    pub index: i32,
    pub count: f64,
}

/// A bucket with resolved boundaries, as produced by the bucket iterators.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BoundedBucket {
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistogramError {
    /// One operand uses custom bounds and the other an exponential schema.
    IncompatibleSchema,
}

/// Side results of [`FloatHistogram::add`] / [`FloatHistogram::sub`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CombineOutcome {
    /// One operand hinted a counter reset and the other hinted no reset.
    pub counter_reset_collision: bool,
    /// Custom bounds differed and were intersected.
    pub custom_bounds_reconciled: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FloatHistogram {
    pub counter_reset_hint: CounterResetHint,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    /// Sorted by index, indices unique.
    pub positive: Vec<Bucket>,
    /// Sorted by index, indices unique. Always empty for custom bounds.
    pub negative: Vec<Bucket>,
    /// Upper bounds for custom-bounds histograms; empty otherwise.
    pub custom_values: Arc<[f64]>,
}

impl FloatHistogram {
    #[inline]
    pub fn uses_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_BUCKETS_SCHEMA
    }

    /// Stale markers travel as histograms whose sum is the stale NaN.
    pub fn is_stale_marker(&self) -> bool {
        is_stale_nan(self.sum)
    }

    pub fn stale_marker() -> Self {
        Self {
            sum: f64::from_bits(STALE_NAN),
            ..Self::default()
        }
    }

    /// Builds a histogram side from span-encoded absolute counts.
    pub fn buckets_from_spans(spans: &[Span], counts: &[f64]) -> Result<Vec<Bucket>, String> {
        let expected: usize = spans.iter().map(|s| s.length as usize).sum();
        if expected != counts.len() {
            return Err(format!(
                "spans need {expected} buckets, have {} buckets",
                counts.len()
            ));
        }
        let mut out = Vec::with_capacity(counts.len());
        let mut next_index: i64 = 0;
        let mut counts = counts.iter();
        for (i, span) in spans.iter().enumerate() {
            if i > 0 && span.offset < 0 {
                return Err(format!("span number {} with offset {}", i + 1, span.offset));
            }
            let mut index = next_index + span.offset as i64;
            for _ in 0..span.length {
                let index_i32 = i32::try_from(index).map_err(|_| "bucket index overflow")?;
                let count = *counts.next().expect("length checked above");
                out.push(Bucket {
                    index: index_i32,
                    count,
                });
                index += 1;
            }
            next_index = index;
        }
        Ok(out)
    }

    /// Like [`Self::buckets_from_spans`] for delta-encoded integer counts.
    pub fn buckets_from_delta_spans(spans: &[Span], deltas: &[i64]) -> Result<Vec<Bucket>, String> {
        let mut running: i64 = 0;
        let absolute: Vec<f64> = deltas
            .iter()
            .map(|d| {
                running = running.wrapping_add(*d);
                running as f64
            })
            .collect();
        Self::buckets_from_spans(spans, &absolute)
    }

    /// Upper bound of the bucket at `index`.
    pub fn bound(&self, index: i32) -> f64 {
        bound(index, self.schema, &self.custom_values)
    }

    /// Applies ingest-time normalisation: schemas above the supported range
    /// are reduced to [`MAX_EXPONENTIAL_SCHEMA`].
    pub fn normalize(&mut self) {
        if self.schema > MAX_EXPONENTIAL_SCHEMA && self.schema <= MAX_REDUCIBLE_SCHEMA {
            self.positive = reduce_resolution(&self.positive, self.schema, MAX_EXPONENTIAL_SCHEMA);
            self.negative = reduce_resolution(&self.negative, self.schema, MAX_EXPONENTIAL_SCHEMA);
            self.schema = MAX_EXPONENTIAL_SCHEMA;
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let sorted = |side: &[Bucket]| side.windows(2).all(|w| w[0].index < w[1].index);
        if !sorted(&self.positive) || !sorted(&self.negative) {
            return Err("bucket indices must be strictly increasing".into());
        }
        if self.uses_custom_buckets() {
            if self.zero_count != 0.0 || self.zero_threshold != 0.0 {
                return Err("custom buckets histogram must not use the zero bucket".into());
            }
            if !self.negative.is_empty() {
                return Err("custom buckets histogram must not have negative buckets".into());
            }
            if self.custom_values.iter().any(|v| !v.is_finite())
                || !self.custom_values.windows(2).all(|w| w[0] < w[1])
            {
                return Err("custom bounds must be finite and strictly increasing".into());
            }
            let max_index = self.custom_values.len() as i32;
            if self
                .positive
                .iter()
                .any(|b| b.index < 0 || b.index > max_index)
            {
                return Err("custom buckets histogram has bucket index out of bounds".into());
            }
        } else if (MIN_EXPONENTIAL_SCHEMA..=MAX_EXPONENTIAL_SCHEMA).contains(&self.schema) {
            if !self.custom_values.is_empty() {
                return Err("exponential histogram must not have custom bounds".into());
            }
            if self.zero_count < 0.0 {
                return Err(format!(
                    "zero bucket has observation count of {}",
                    self.zero_count
                ));
            }
            if !(self.zero_threshold >= 0.0 && self.zero_threshold.is_finite()) {
                return Err(format!("invalid zero threshold {}", self.zero_threshold));
            }
            if self.negative.iter().any(|b| b.count < 0.0) {
                return Err("negative side: bucket has negative count".into());
            }
        } else {
            return Err(format!("invalid histogram schema {}", self.schema));
        }
        if self.count < 0.0 {
            return Err(format!("observation count is {}", self.count));
        }
        if self.positive.iter().any(|b| b.count < 0.0) {
            return Err("positive side: bucket has negative count".into());
        }
        Ok(())
    }

    /// Removes every bucket with a count of exactly zero (Prometheus'
    /// `Compact(0)`).
    pub fn compact(&mut self) -> &mut Self {
        self.positive.retain(|b| b.count != 0.0);
        self.negative.retain(|b| b.count != 0.0);
        self
    }

    /// Scales counts and sum. A negative factor makes the result a gauge.
    pub fn mul(&mut self, factor: f64) -> &mut Self {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for b in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            b.count *= factor;
        }
        if factor < 0.0 {
            self.counter_reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    /// Divides counts and sum. Dividing by zero drops all buckets.
    pub fn div(&mut self, scalar: f64) -> &mut Self {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive.clear();
            self.negative.clear();
            return self;
        }
        for b in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            b.count /= scalar;
        }
        if scalar < 0.0 {
            self.counter_reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    /// A copy with the given lower-or-equal exponential schema.
    pub fn copy_to_schema(&self, target_schema: i32) -> Self {
        if target_schema == self.schema {
            return self.clone();
        }
        debug_assert!(!self.uses_custom_buckets() && target_schema < self.schema);
        Self {
            counter_reset_hint: CounterResetHint::Unknown,
            schema: target_schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive: reduce_resolution(&self.positive, self.schema, target_schema),
            negative: reduce_resolution(&self.negative, self.schema, target_schema),
            custom_values: Arc::default(),
        }
    }

    pub fn add(&mut self, other: &FloatHistogram) -> Result<CombineOutcome, HistogramError> {
        self.combine(other, false)
    }

    /// [`Self::add`] with Kahan-compensated `count` and `sum`, the fields
    /// whose observations can cancel catastrophically. The final value is
    /// `self` plus `c` (see [`Compensation::apply`]).
    pub fn kahan_add(
        &mut self,
        other: &FloatHistogram,
        c: &mut Compensation,
    ) -> Result<CombineOutcome, HistogramError> {
        let (count, sum) = (self.count, self.sum);
        let outcome = self.add(other)?;
        (self.count, c.count) = kahan_inc(other.count, count, c.count);
        (self.sum, c.sum) = kahan_inc(other.sum, sum, c.sum);
        Ok(outcome)
    }

    /// Like [`Self::add`] but subtracts. Callers implementing the PromQL `-`
    /// operator must mark the result as a gauge afterwards.
    pub fn sub(&mut self, other: &FloatHistogram) -> Result<CombineOutcome, HistogramError> {
        self.combine(other, true)
    }

    fn combine(
        &mut self,
        other: &FloatHistogram,
        negate: bool,
    ) -> Result<CombineOutcome, HistogramError> {
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(HistogramError::IncompatibleSchema);
        }
        let sign = if negate { -1.0 } else { 1.0 };
        let mut outcome = CombineOutcome {
            counter_reset_collision: self.adjust_counter_reset(other),
            custom_bounds_reconciled: false,
        };
        if !self.uses_custom_buckets() {
            let other_zero_count = self.reconcile_zero_buckets(other);
            self.zero_count += sign * other_zero_count;
        }
        self.count += sign * other.count;
        self.sum += sign * other.sum;

        if self.uses_custom_buckets() {
            if custom_bounds_match(&self.custom_values, &other.custom_values) {
                add_buckets(
                    self.schema,
                    self.zero_threshold,
                    negate,
                    &mut self.positive,
                    &other.positive,
                );
            } else {
                outcome.custom_bounds_reconciled = true;
                let intersected =
                    intersect_custom_bounds(&self.custom_values, &other.custom_values);
                self.positive = add_custom_buckets_with_mismatches(
                    negate,
                    &self.positive,
                    &self.custom_values,
                    &other.positive,
                    &other.custom_values,
                    &intersected,
                );
                self.custom_values = intersected;
            }
            return Ok(outcome);
        }

        let reduced;
        let (other_positive, other_negative) = if other.schema < self.schema {
            self.positive = reduce_resolution(&self.positive, self.schema, other.schema);
            self.negative = reduce_resolution(&self.negative, self.schema, other.schema);
            self.schema = other.schema;
            (&other.positive[..], &other.negative[..])
        } else if other.schema > self.schema {
            reduced = (
                reduce_resolution(&other.positive, other.schema, self.schema),
                reduce_resolution(&other.negative, other.schema, self.schema),
            );
            (&reduced.0[..], &reduced.1[..])
        } else {
            (&other.positive[..], &other.negative[..])
        };
        add_buckets(
            self.schema,
            self.zero_threshold,
            negate,
            &mut self.positive,
            other_positive,
        );
        add_buckets(
            self.schema,
            self.zero_threshold,
            negate,
            &mut self.negative,
            other_negative,
        );
        Ok(outcome)
    }

    fn adjust_counter_reset(&mut self, other: &FloatHistogram) -> bool {
        use CounterResetHint::*;
        match (self.counter_reset_hint, other.counter_reset_hint) {
            (a, b) if a == b => false,
            (Gauge, _) => false,
            (_, Gauge) => {
                self.counter_reset_hint = Gauge;
                false
            }
            (Unknown, _) => false,
            (_, Unknown) => {
                self.counter_reset_hint = Unknown;
                false
            }
            _ => {
                self.counter_reset_hint = Unknown;
                true
            }
        }
    }

    /// The zero count this histogram would have with the given larger (or
    /// equal) zero threshold, plus the threshold actually needed: if the
    /// requested one lands inside a populated bucket it is widened to that
    /// bucket's outer bound.
    fn zero_count_for_larger_threshold(&self, larger_threshold: f64) -> (f64, f64) {
        let mut larger = larger_threshold;
        if larger == self.zero_threshold {
            return (self.zero_count, larger);
        }
        debug_assert!(larger > self.zero_threshold);
        'outer: loop {
            let mut count = self.zero_count;
            for b in &self.positive {
                if self.bound(b.index - 1) >= larger {
                    break;
                }
                count += b.count;
                let upper = self.bound(b.index);
                if upper > larger {
                    if b.count != 0.0 {
                        larger = upper;
                    }
                    break;
                }
            }
            for b in &self.negative {
                if self.bound(b.index - 1) >= larger {
                    break;
                }
                count += b.count;
                let outer_bound = self.bound(b.index);
                if outer_bound > larger {
                    if b.count != 0.0 {
                        larger = outer_bound;
                        continue 'outer;
                    }
                    break;
                }
            }
            return (count, larger);
        }
    }

    /// Drops buckets now covered by the zero bucket (their counts must
    /// already be part of `zero_count`), then compacts.
    fn trim_buckets_in_zero_bucket(&mut self) {
        let threshold = self.zero_threshold;
        let schema = self.schema;
        let custom = self.custom_values.clone();
        for side in [&mut self.positive, &mut self.negative] {
            for b in side.iter_mut() {
                if bound(b.index - 1, schema, &custom) >= threshold {
                    break;
                }
                b.count = 0.0;
            }
        }
        self.compact();
    }

    /// Widens this histogram's and (virtually) `other`'s zero buckets until
    /// they agree; returns the zero count `other` would have.
    fn reconcile_zero_buckets(&mut self, other: &FloatHistogram) -> f64 {
        let mut other_zero_count = other.zero_count;
        let mut other_threshold = other.zero_threshold;
        while other_threshold != self.zero_threshold {
            if self.zero_threshold > other_threshold {
                (other_zero_count, other_threshold) =
                    other.zero_count_for_larger_threshold(self.zero_threshold);
            }
            if other_threshold > self.zero_threshold {
                (self.zero_count, self.zero_threshold) =
                    self.zero_count_for_larger_threshold(other_threshold);
                self.trim_buckets_in_zero_bucket();
            }
        }
        other_zero_count
    }

    /// Whether `self` follows `previous` via a counter reset.
    pub fn detect_reset(&self, previous: &FloatHistogram) -> bool {
        match self.counter_reset_hint {
            CounterResetHint::CounterReset => return true,
            CounterResetHint::NotCounterReset => return false,
            CounterResetHint::Unknown | CounterResetHint::Gauge => {}
        }
        if self.count < previous.count {
            return true;
        }
        if self.uses_custom_buckets() {
            if !previous.uses_custom_buckets() {
                return true;
            }
            if !custom_bounds_match(&self.custom_values, &previous.custom_values) {
                return self.detect_reset_with_mismatched_custom_bounds(previous);
            }
        }
        if self.schema > previous.schema {
            return true;
        }
        if self.zero_threshold < previous.zero_threshold {
            return true;
        }
        let (previous_zero_count, new_threshold) =
            previous.zero_count_for_larger_threshold(self.zero_threshold);
        if new_threshold != self.zero_threshold {
            return true;
        }
        if self.zero_count < previous_zero_count {
            return true;
        }
        let threshold = self.zero_threshold;
        let current_side =
            |side: &[Bucket]| self.buckets_above(side, self.schema, threshold, self.schema);
        let previous_side =
            |side: &[Bucket]| previous.buckets_above(side, previous.schema, threshold, self.schema);
        detect_reset_buckets(
            &current_side(&self.positive),
            &previous_side(&previous.positive),
        ) || detect_reset_buckets(
            &current_side(&self.negative),
            &previous_side(&previous.negative),
        )
    }

    /// `side` reduced to `target_schema`, skipping leading buckets whose
    /// absolute upper bound is `<= absolute_start` (exponential schemas only).
    fn buckets_above(
        &self,
        side: &[Bucket],
        schema: i32,
        absolute_start: f64,
        target_schema: i32,
    ) -> Vec<Bucket> {
        let reduced = if target_schema == schema {
            side.to_vec()
        } else {
            reduce_resolution(side, schema, target_schema)
        };
        if absolute_start == 0.0 || !is_exponential_schema(target_schema) {
            return reduced;
        }
        let skip = reduced
            .iter()
            .take_while(|b| exponential_bound(b.index, target_schema) <= absolute_start)
            .count();
        reduced[skip..].to_vec()
    }

    fn detect_reset_with_mismatched_custom_bounds(&self, previous: &FloatHistogram) -> bool {
        let current_buckets = self.positive_buckets();
        let previous_buckets = previous.positive_buckets();
        let (mut ci, mut pi) = (0usize, 0usize);
        let rollup = |buckets: &[BoundedBucket], i: &mut usize, bound: f64| {
            let mut sum = 0.0;
            while *i < buckets.len() && buckets[*i].upper <= bound {
                sum += buckets[*i].count;
                *i += 1;
            }
            sum
        };
        let (current_bounds, previous_bounds) = (&self.custom_values, &previous.custom_values);
        let (mut cb, mut pb) = (0usize, 0usize);
        while cb <= current_bounds.len() && pb <= previous_bounds.len() {
            let current_bound = current_bounds.get(cb).copied().unwrap_or(f64::INFINITY);
            let previous_bound = previous_bounds.get(pb).copied().unwrap_or(f64::INFINITY);
            if current_bound == previous_bound {
                let current_sum = rollup(&current_buckets, &mut ci, current_bound);
                let previous_sum = rollup(&previous_buckets, &mut pi, current_bound);
                if current_sum < previous_sum {
                    return true;
                }
                cb += 1;
                pb += 1;
            } else if current_bound < previous_bound {
                cb += 1;
            } else {
                pb += 1;
            }
        }
        false
    }

    /// Exact data equality: schema, bounds, zero bucket and every bucket
    /// (including explicit zero-count buckets) compared bitwise.
    pub fn equals(&self, other: &FloatHistogram) -> bool {
        let bits_eq = |a: f64, b: f64| a.to_bits() == b.to_bits();
        let side_eq = |a: &[Bucket], b: &[Bucket]| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| x.index == y.index && bits_eq(x.count, y.count))
        };
        self.schema == other.schema
            && bits_eq(self.count, other.count)
            && bits_eq(self.sum, other.sum)
            && (!self.uses_custom_buckets()
                || custom_bounds_match(&self.custom_values, &other.custom_values))
            && self.zero_threshold == other.zero_threshold
            && bits_eq(self.zero_count, other.zero_count)
            && side_eq(&self.negative, &other.negative)
            && side_eq(&self.positive, &other.positive)
    }

    fn positive_bucket(&self, b: &Bucket) -> BoundedBucket {
        BoundedBucket {
            lower: self.bound(b.index - 1),
            upper: self.bound(b.index),
            count: b.count,
        }
    }

    fn negative_bucket(&self, b: &Bucket) -> BoundedBucket {
        BoundedBucket {
            lower: -self.bound(b.index),
            upper: -self.bound(b.index - 1),
            count: b.count,
        }
    }

    fn positive_buckets(&self) -> Vec<BoundedBucket> {
        self.positive
            .iter()
            .map(|b| self.positive_bucket(b))
            .collect()
    }

    fn zero_bucket(&self) -> BoundedBucket {
        BoundedBucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            count: self.zero_count,
        }
    }

    /// Clamps buckets that straddle the zero bucket to its edge.
    fn clamp_to_zero_bucket(&self, mut b: BoundedBucket) -> BoundedBucket {
        let zt = self.zero_threshold;
        if b.upper < 0.0 && b.upper > -zt {
            b.upper = -zt;
        } else if b.lower > 0.0 && b.lower < zt {
            b.lower = zt;
        }
        b
    }

    /// Every bucket in ascending value order: negative, zero (if populated),
    /// positive.
    pub(crate) fn buckets(&self) -> impl Iterator<Item = BoundedBucket> + '_ {
        self.negative
            .iter()
            .rev()
            .map(|b| self.clamp_to_zero_bucket(self.negative_bucket(b)))
            .chain((self.zero_count > 0.0).then(|| self.zero_bucket()))
            .chain(
                self.positive
                    .iter()
                    .map(|b| self.clamp_to_zero_bucket(self.positive_bucket(b))),
            )
    }

    /// Every bucket in ascending value order: negative, zero (if populated),
    /// positive.
    pub fn all_buckets(&self) -> Vec<BoundedBucket> {
        self.buckets().collect()
    }

    /// Every bucket in descending value order.
    pub fn all_buckets_reverse(&self) -> Vec<BoundedBucket> {
        let mut out = self.all_buckets();
        out.reverse();
        out
    }
}

pub fn is_exponential_schema(schema: i32) -> bool {
    (MIN_EXPONENTIAL_SCHEMA..=MAX_EXPONENTIAL_SCHEMA).contains(&schema)
}

pub fn custom_bounds_match(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

/// Converts sorted sparse buckets back into Prometheus span encoding.
pub fn to_spans(buckets: &[Bucket]) -> (Vec<Span>, Vec<f64>) {
    let mut spans: Vec<Span> = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut next_index: Option<i32> = None;
    for b in buckets {
        match next_index {
            Some(next) if next == b.index => spans.last_mut().expect("span open").length += 1,
            Some(next) => spans.push(Span {
                offset: b.index - next,
                length: 1,
            }),
            None => spans.push(Span {
                offset: b.index,
                length: 1,
            }),
        }
        counts.push(b.count);
        next_index = Some(b.index + 1);
    }
    (spans, counts)
}

fn bound(index: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        let len = custom_values.len() as i32;
        return if index < 0 {
            f64::NEG_INFINITY
        } else if index >= len {
            f64::INFINITY
        } else {
            custom_values[index as usize]
        };
    }
    exponential_bound(index, schema)
}

fn exponential_bound(index: i32, schema: i32) -> f64 {
    // The last finite bucket's upper bound would compute as 2^1024 (=+Inf);
    // it is pinned to f64::MAX so that +Inf observations get their own bucket.
    if schema < 0 {
        let exp = (index as i64) << (-schema);
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp);
    }
    let frac = exponential_fractions(schema)[(index & ((1 << schema) - 1)) as usize];
    let exp = ((index >> schema) as i64) + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

/// Bucket bounds within `[0.5, 1)` for each schema `0..=8`.
fn exponential_fractions(schema: i32) -> &'static [f64] {
    static TABLES: OnceLock<Vec<Vec<f64>>> = OnceLock::new();
    let tables = TABLES.get_or_init(|| {
        (0..=MAX_EXPONENTIAL_SCHEMA)
            .map(|s| {
                let n = 1u32 << s;
                (0..n).map(|j| (j as f64 / n as f64).exp2() / 2.0).collect()
            })
            .collect()
    });
    &tables[schema as usize]
}

fn ldexp(mut x: f64, mut exp: i64) -> f64 {
    let two_pow = |e: i64| f64::from_bits(((e + 1023) as u64) << 52);
    while exp > 1023 {
        x *= two_pow(1023);
        exp -= 1023;
        if x.is_infinite() {
            return x;
        }
    }
    while exp < -1022 {
        x *= two_pow(-1022);
        exp += 1022;
        if x == 0.0 {
            return x;
        }
    }
    x * two_pow(exp)
}

fn target_index(index: i32, origin_schema: i32, target_schema: i32) -> i32 {
    ((index - 1) >> (origin_schema - target_schema)) + 1
}

fn reduce_resolution(buckets: &[Bucket], origin_schema: i32, target_schema: i32) -> Vec<Bucket> {
    let mut out: Vec<Bucket> = Vec::with_capacity(buckets.len());
    for b in buckets {
        let index = target_index(b.index, origin_schema, target_schema);
        match out.last_mut() {
            Some(last) if last.index == index => last.count += b.count,
            _ => out.push(Bucket {
                index,
                count: b.count,
            }),
        }
    }
    out
}

/// Adds (or subtracts) `b` into `a`, both sorted and in `schema`. Leading
/// buckets of `b` whose absolute upper bound is `<= threshold` are skipped
/// (their counts already moved into the zero bucket).
fn add_buckets(schema: i32, threshold: f64, negate: bool, a: &mut Vec<Bucket>, b: &[Bucket]) {
    let skip = if is_exponential_schema(schema) {
        b.iter()
            .take_while(|x| exponential_bound(x.index, schema) <= threshold)
            .count()
    } else {
        0
    };
    let b = &b[skip..];
    if b.is_empty() {
        return;
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut ai = 0;
    for bucket in b {
        let count = if negate { -bucket.count } else { bucket.count };
        while ai < a.len() && a[ai].index < bucket.index {
            out.push(a[ai]);
            ai += 1;
        }
        if ai < a.len() && a[ai].index == bucket.index {
            out.push(Bucket {
                index: bucket.index,
                count: a[ai].count + count,
            });
            ai += 1;
        } else {
            out.push(Bucket {
                index: bucket.index,
                count,
            });
        }
    }
    out.extend_from_slice(&a[ai..]);
    *a = out;
}

fn intersect_custom_bounds(a: &[f64], b: &[f64]) -> Arc<[f64]> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            out.push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    Arc::from(out)
}

/// Maps both custom-bounds bucket sets onto `intersected` and combines them,
/// dropping zero-valued results.
fn add_custom_buckets_with_mismatches(
    negate: bool,
    a: &[Bucket],
    a_bounds: &[f64],
    b: &[Bucket],
    b_bounds: &[f64],
    intersected: &[f64],
) -> Vec<Bucket> {
    let mut target = vec![0.0; intersected.len() + 1];
    let mut map = |buckets: &[Bucket], bounds: &[f64], sign: f64| {
        let mut intersect_index = 0;
        for bucket in buckets {
            let mut target_index = target.len() - 1;
            if let Some(&source_bound) = bounds.get(bucket.index.max(0) as usize) {
                while intersect_index < intersected.len() {
                    if intersected[intersect_index] >= source_bound {
                        target_index = intersect_index;
                        break;
                    }
                    intersect_index += 1;
                }
            }
            target[target_index] += sign * bucket.count;
        }
    };
    map(a, a_bounds, 1.0);
    map(b, b_bounds, if negate { -1.0 } else { 1.0 });
    target
        .into_iter()
        .enumerate()
        .filter(|(_, count)| *count != 0.0)
        .map(|(index, count)| Bucket {
            index: index as i32,
            count,
        })
        .collect()
}

fn detect_reset_buckets(current: &[Bucket], previous: &[Bucket]) -> bool {
    let mut ci = 0;
    for p in previous {
        while ci < current.len() && current[ci].index < p.index {
            ci += 1;
        }
        match current.get(ci) {
            Some(c) if c.index == p.index => {
                if c.count < p.count {
                    return true;
                }
            }
            _ => {
                if p.count != 0.0 {
                    return true;
                }
            }
        }
    }
    false
}

/// `histogram_quantile` for a native histogram. Exponential buckets
/// interpolate on a log scale; custom buckets and the zero bucket linearly.
pub fn quantile(q: f64, h: &FloatHistogram) -> f64 {
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if h.count == 0.0 || q.is_nan() {
        return f64::NAN;
    }
    let forward = h.sum.is_nan() || q < 0.5;
    let (buckets, mut rank) = if forward {
        (h.all_buckets(), q * h.count)
    } else {
        (h.all_buckets_reverse(), (1.0 - q) * h.count)
    };
    let mut bucket = BoundedBucket::default();
    let mut count = 0.0;
    for b in &buckets {
        bucket = *b;
        if b.count == 0.0 {
            continue;
        }
        count += b.count;
        if count >= rank {
            break;
        }
    }
    if !h.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
        if h.negative.is_empty() && !h.positive.is_empty() {
            bucket.lower = 0.0;
        } else if h.positive.is_empty() && !h.negative.is_empty() {
            bucket.upper = 0.0;
        }
    } else if h.uses_custom_buckets() {
        if bucket.lower == f64::NEG_INFINITY {
            if bucket.upper <= 0.0 {
                return bucket.upper;
            }
            bucket.lower = 0.0;
        } else if bucket.upper == f64::INFINITY {
            return bucket.lower;
        }
    }
    if count > h.count {
        count = h.count;
    }
    if count < rank {
        if h.sum.is_nan() {
            return f64::NAN;
        }
        return bucket.upper;
    }
    if forward {
        rank -= count - bucket.count;
    } else {
        rank = count - rank;
    }
    let fraction = rank / bucket.count;
    if h.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        return (log_lower + (log_upper - log_lower) * fraction).exp2();
    }
    -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
}

fn fraction_below(b: &BoundedBucket, v: f64, linear: bool) -> f64 {
    if linear {
        return (v - b.lower) / (b.upper - b.lower);
    }
    let log_lower = b.lower.abs().log2();
    let log_upper = b.upper.abs().log2();
    let log_v = v.abs().log2();
    if v > 0.0 {
        return (log_v - log_lower) / (log_upper - log_lower);
    }
    1.0 - ((log_v - log_upper) / (log_lower - log_upper))
}

/// `histogram_fraction` for a native histogram: the estimated share of
/// observations in `(lower, upper]`.
pub fn fraction(lower: f64, upper: f64, h: &FloatHistogram) -> f64 {
    if h.count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }
    let buckets = h.all_buckets();
    let (mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);
    for original in &buckets {
        let mut b = *original;
        let mut zero_bucket = false;
        if h.uses_custom_buckets() {
            if b.lower == f64::NEG_INFINITY && b.upper > 0.0 {
                b.lower = 0.0;
            }
        } else if b.lower <= 0.0 && b.upper >= 0.0 {
            zero_bucket = true;
            if h.negative.is_empty() && !h.positive.is_empty() {
                b.lower = 0.0;
            } else if h.positive.is_empty() && !h.negative.is_empty() {
                b.upper = 0.0;
            }
        }
        let linear = h.uses_custom_buckets() || zero_bucket;
        let interpolate = |v: f64| {
            if linear {
                if b.lower == f64::NEG_INFINITY {
                    return b.count;
                }
                rank + b.count * fraction_below(&b, v, true)
            } else {
                rank + b.count * fraction_below(&b, v, false)
            }
        };
        if !lower_set && b.lower >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && b.lower >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && b.lower < lower && b.upper > lower {
            lower_rank = interpolate(lower);
            lower_set = true;
        }
        if !upper_set && b.lower < upper && b.upper > upper {
            upper_rank = interpolate(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += b.count;
    }
    let count = if h.sum.is_nan() {
        buckets.iter().map(|b| b.count).sum()
    } else {
        h.count
    };
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / h.count
}

/// Pending Kahan compensation for [`FloatHistogram::kahan_add`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Compensation {
    pub count: f64,
    pub sum: f64,
}

impl Compensation {
    pub fn apply(self, h: &mut FloatHistogram) {
        h.count += self.count;
        h.sum += self.sum;
    }
}

/// Compensated sum of `hs`; `None` when empty.
pub fn kahan_sum<'a>(
    hs: impl IntoIterator<Item = &'a FloatHistogram>,
) -> Result<Option<FloatHistogram>, HistogramError> {
    let mut hs = hs.into_iter();
    let Some(first) = hs.next() else {
        return Ok(None);
    };
    let mut sum = first.clone();
    let mut c = Compensation::default();
    for h in hs {
        sum.kahan_add(h, &mut c)?;
    }
    c.apply(&mut sum);
    Ok(Some(sum))
}

/// Mean of `hs` as Prometheus computes it for floats: a compensated sum
/// divided by the count, switching to an incremental mean once the sum's
/// `count` or `sum` would overflow. `None` when empty.
pub fn kahan_mean<'a>(
    hs: impl IntoIterator<Item = &'a FloatHistogram>,
) -> Result<Option<FloatHistogram>, HistogramError> {
    let mut hs = hs.into_iter();
    let Some(first) = hs.next() else {
        return Ok(None);
    };
    let mut acc = first.clone();
    let mut c = Compensation::default();
    let mut n = 1.0;
    let mut incremental = false;
    for h in hs {
        n += 1.0;
        if !incremental {
            let overflows = (acc.count + h.count).is_infinite() || (acc.sum + h.sum).is_infinite();
            if !overflows {
                acc.kahan_add(h, &mut c)?;
                continue;
            }
            incremental = true;
            acc.div(n - 1.0);
            c.count /= n - 1.0;
            c.sum /= n - 1.0;
        }
        let q = (n - 1.0) / n;
        acc.mul(q);
        c.count *= q;
        c.sum *= q;
        let mut scaled = h.clone();
        scaled.div(n);
        acc.kahan_add(&scaled, &mut c)?;
    }
    if !incremental {
        acc.div(n);
        c.count /= n;
        c.sum /= n;
    }
    c.apply(&mut acc);
    Ok(Some(acc))
}

/// Estimated variance of the observations, using each bucket's geometric
/// (exponential), arithmetic (custom) or zero (zero bucket) midpoint.
pub fn variance(h: &FloatHistogram) -> f64 {
    let mean = h.sum / h.count;
    let (mut sum, mut compensation) = (0.0f64, 0.0f64);
    for b in h.all_buckets() {
        if b.count == 0.0 {
            continue;
        }
        let value = if h.uses_custom_buckets() {
            (b.upper + b.lower) / 2.0
        } else if b.lower <= 0.0 && b.upper >= 0.0 {
            0.0
        } else {
            let v = (b.upper * b.lower).sqrt();
            if b.upper < 0.0 { -v } else { v }
        };
        let delta = value - mean;
        (sum, compensation) = kahan_inc(b.count * delta * delta, sum, compensation);
    }
    (sum + compensation) / h.count
}

#[cfg(test)]
mod tests;
