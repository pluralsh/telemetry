//! Per-group accumulator shared by the streaming reducers.

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

        // Kahan sum lane.
        let (new_sum, new_c) = kahan_inc(v, self.sum, self.c_sum);
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
        let n = self.count as f64;
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
        // Mean via Welford is numerically robust. For groups where the
        // Kahan sum hasn't overflowed, sum/count and mean agree to
        // within the compensation term; we emit the Welford mean for
        // the overflow-resistance the task spec mandates.
        self.mean + self.c_mean
    }

    #[inline]
    pub(super) fn variance_value(&self) -> f64 {
        // Population variance: M2 / n.
        let n = self.count as f64;
        (self.m2 + self.c_m2) / n
    }
}

/// Kahan–Neumaier compensated summation step. Matches the implementation
/// in [`super::rollup`] bit-for-bit so sum / avg across streaming and
/// range aggregates round identically.
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
