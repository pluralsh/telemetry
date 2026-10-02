//! Per-group accumulator shared by the streaming reducers.

use crate::util::kahan_inc;

/// Single-pass per-group accumulator. Three overlapping lanes (Kahan sum,
/// NaN-safe min/max, Welford) are all maintained; the reducer reads its own.
#[derive(Debug, Clone, Copy)]
pub(super) struct Accumulator {
    /// Count of valid inputs that contributed. Also the Welford `n`.
    pub(super) count: u64,
    /// Kahan sum and its compensation term.
    sum: f64,
    c_sum: f64,
    /// Min / max running extremum — `any_real` tracks whether we've
    /// absorbed a non-NaN value. When `any_real == false` and
    /// `count > 0`, every contribution so far has been NaN; the
    /// extremum stays NaN and the output is NaN (Prometheus semantics).
    pub(super) min: f64,
    pub(super) max: f64,
    /// True once at least one **non-NaN** valid value has been absorbed.
    /// Used by Min/Max to decide "first real initialises".
    any_real: bool,
    /// Welford accumulators (Kahan-compensated on both mean and M2).
    mean: f64,
    c_mean: f64,
    m2: f64,
    c_m2: f64,
    /// Prometheus' `avg`: the direct Kahan mean until the running sum would
    /// overflow, then an incremental mean seeded from the sum so far.
    avg_incremental: bool,
    avg_mean: f64,
    c_avg_mean: f64,
}

impl Accumulator {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            c_sum: 0.0,
            min: f64::NAN,
            max: f64::NAN,
            any_real: false,
            mean: 0.0,
            c_mean: 0.0,
            m2: 0.0,
            c_m2: 0.0,
            avg_incremental: false,
            avg_mean: 0.0,
            c_avg_mean: 0.0,
        }
    }

    #[inline]
    fn reset(&mut self) {
        *self = Self::new();
    }

    /// Absorb one valid input cell.
    #[inline]
    pub(super) fn absorb(&mut self, v: f64) {
        self.count = self.count.saturating_add(1);
        let n = self.count as f64;

        // Kahan sum lane.
        let (new_sum, new_c) = kahan_inc(v, self.sum, self.c_sum);
        if self.count > 1 && !self.avg_incremental && new_sum.is_infinite() {
            self.avg_incremental = true;
            self.avg_mean = self.sum / (n - 1.0);
            self.c_avg_mean = self.c_sum / (n - 1.0);
        }
        if self.avg_incremental {
            let q = (n - 1.0) / n;
            (self.avg_mean, self.c_avg_mean) =
                kahan_inc(v / n, q * self.avg_mean, q * self.c_avg_mean);
        }
        self.sum = new_sum;
        self.c_sum = new_c;

        // Min/Max lane — NaN-safe: first real value initialises, NaN
        // inputs are ignored once any real value is present. When every
        // contribution is NaN, `min`/`max` stay NaN.
        if !v.is_nan() {
            if !self.any_real {
                self.min = v;
                self.max = v;
                self.any_real = true;
            } else {
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
            }
        }

        // Welford lane with Kahan compensation on both mean and M2.
        let delta = v - (self.mean + self.c_mean);
        let (new_mean, new_c_mean) = kahan_inc(delta / n, self.mean, self.c_mean);
        self.mean = new_mean;
        self.c_mean = new_c_mean;
        let new_delta = v - (self.mean + self.c_mean);
        let (new_m2, new_c_m2) = kahan_inc(delta * new_delta, self.m2, self.c_m2);
        self.m2 = new_m2;
        self.c_m2 = new_c_m2;
    }

    #[inline]
    pub(super) fn sum_value(&self) -> f64 {
        if self.sum.is_infinite() {
            self.sum
        } else {
            self.sum + self.c_sum
        }
    }

    #[inline]
    pub(super) fn avg_value(&self) -> f64 {
        if self.avg_incremental {
            self.avg_mean + self.c_avg_mean
        } else {
            let n = self.count as f64;
            self.sum / n + self.c_sum / n
        }
    }

    #[inline]
    pub(super) fn variance_value(&self) -> f64 {
        // Population variance: M2 / n.
        let n = self.count as f64;
        (self.m2 + self.c_m2) / n
    }
}
