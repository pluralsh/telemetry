//! Per-(group, step) accumulator grid shared by the streaming reducers.
//!
//! Each kind keeps only the lanes its reducer reads, struct-of-arrays and
//! group-major (`lane[group * step_count + step]`), so one input series
//! updates a contiguous run of per-step accumulators that are independent
//! of each other. Every accumulator still sees its contributions in the
//! order a cell-at-a-time absorb would — ascending input series within a
//! batch, batches in arrival order — so outputs are bit-identical to it.

use super::AggregateKind;
use crate::promql::batch::{BitSet, StepBatch};
use crate::util::kahan_inc;

/// Transpose tile: a series' steps within it form one run.
const TILE_STEPS: usize = 128;
const TILE_SERIES: usize = 64;
/// Batches with fewer steps absorb cell by cell; runs that short don't pay
/// for the transpose.
const MIN_RUN_STEPS: usize = 4;

/// Lanes for one reducer family. `absorb` takes one valid cell; `absorb_run`
/// takes one series' cells for consecutive steps starting at lane `at`,
/// masked by `valid`.
trait Lanes {
    fn absorb(&mut self, at: usize, v: f64);
    fn absorb_run(&mut self, at: usize, values: &[f64], valid: &[bool]);
}

struct CountLanes {
    count: Vec<u64>,
}

impl CountLanes {
    /// Integer counts don't depend on contribution order and read no
    /// values, so each series counts its steps straight off the validity
    /// words, no transpose.
    fn absorb_batch(&mut self, step_count: usize, input: &StepBatch, groups: &[Option<u32>]) {
        let (steps, series) = (input.step_count(), input.series_count());
        let words = input.validity.words();
        for (s, group) in groups.iter().enumerate() {
            let Some(g) = *group else { continue };
            let at = g as usize * step_count + input.step_range.start;
            for (t, count) in self.count[at..at + steps].iter_mut().enumerate() {
                let cell = t * series + s;
                *count = count.saturating_add((words[cell / 64] >> (cell % 64)) & 1);
            }
        }
    }
}

/// Kahan sum.
struct SumLanes {
    count: Vec<u64>,
    sum: Vec<f64>,
    c: Vec<f64>,
}

/// `if ok { new } else { old }`, bit for bit. Spelled as a mask because
/// LLVM lowers `*x = if ok { new } else { *x }` to a conditional store,
/// which keeps the run kernels from vectorizing.
#[inline]
fn blend(ok: bool, new: f64, old: f64) -> f64 {
    let mask = 0u64.wrapping_sub(u64::from(ok));
    f64::from_bits((new.to_bits() & mask) | (old.to_bits() & !mask))
}

#[inline]
fn sum_step(count: &mut u64, sum: &mut f64, c: &mut f64, v: f64, ok: bool) {
    let (t, new_c) = kahan_inc(v, *sum, *c);
    *count = count.saturating_add(u64::from(ok));
    *sum = blend(ok, t, *sum);
    *c = blend(ok, new_c, *c);
}

impl SumLanes {
    #[inline]
    fn value(&self, at: usize) -> f64 {
        let sum = self.sum[at];
        if sum.is_infinite() {
            sum
        } else {
            sum + self.c[at]
        }
    }
}

impl Lanes for SumLanes {
    #[inline]
    fn absorb(&mut self, at: usize, v: f64) {
        sum_step(
            &mut self.count[at],
            &mut self.sum[at],
            &mut self.c[at],
            v,
            true,
        );
    }

    fn absorb_run(&mut self, at: usize, values: &[f64], valid: &[bool]) {
        let n = values.len();
        sum_run(
            &mut self.count[at..at + n],
            &mut self.sum[at..at + n],
            &mut self.c[at..at + n],
            values,
            valid,
        );
    }
}

// The run kernels take each lane as its own slice argument: that is what
// tells LLVM the lanes don't alias, which it needs to vectorize. Lanes
// reached through `&mut self` are just `Vec` pointers to it.
#[inline(never)]
fn sum_run(count: &mut [u64], sum: &mut [f64], c: &mut [f64], values: &[f64], valid: &[bool]) {
    for (((count, sum), c), (&v, &ok)) in count
        .iter_mut()
        .zip(sum)
        .zip(c)
        .zip(values.iter().zip(valid))
    {
        sum_step(count, sum, c, v, ok);
    }
}

/// Prometheus' `avg`: the direct Kahan mean until the running sum would
/// overflow, then an incremental mean seeded from the sum so far.
struct AvgLanes {
    sum: SumLanes,
    incremental: Vec<bool>,
    mean: Vec<f64>,
    c_mean: Vec<f64>,
}

impl AvgLanes {
    #[inline]
    fn value(&self, at: usize) -> f64 {
        if self.incremental[at] {
            self.mean[at] + self.c_mean[at]
        } else {
            let n = self.sum.count[at] as f64;
            self.sum.sum[at] / n + self.sum.c[at] / n
        }
    }
}

impl Lanes for AvgLanes {
    #[inline]
    fn absorb(&mut self, at: usize, v: f64) {
        let count = self.sum.count[at].saturating_add(1);
        self.sum.count[at] = count;
        let n = count as f64;
        let (sum, c) = (self.sum.sum[at], self.sum.c[at]);
        let (new_sum, new_c) = kahan_inc(v, sum, c);
        if count > 1 && !self.incremental[at] && new_sum.is_infinite() {
            self.incremental[at] = true;
            self.mean[at] = sum / (n - 1.0);
            self.c_mean[at] = c / (n - 1.0);
        }
        if self.incremental[at] {
            let q = (n - 1.0) / n;
            (self.mean[at], self.c_mean[at]) =
                kahan_inc(v / n, q * self.mean[at], q * self.c_mean[at]);
        }
        self.sum.sum[at] = new_sum;
        self.sum.c[at] = new_c;
    }

    /// Runs where no lane is, or is about to turn, incremental reduce to
    /// the plain Kahan sum; the rest take the exact per-cell path.
    fn absorb_run(&mut self, at: usize, values: &[f64], valid: &[bool]) {
        let n = values.len();
        let overflows = self.incremental[at..at + n]
            .iter()
            .zip(&self.sum.sum[at..at + n])
            .zip(values.iter().zip(valid))
            .fold(false, |acc, ((&inc, &sum), (&v, &ok))| {
                acc | inc | (ok & (sum + v).is_infinite())
            });
        if !overflows {
            self.sum.absorb_run(at, values, valid);
            return;
        }
        for (i, (&v, &ok)) in values.iter().zip(valid).enumerate() {
            if ok {
                self.absorb(at + i, v);
            }
        }
    }
}

/// NaN-safe extremum: the first real value initialises, NaN inputs are
/// ignored once one is present, and an all-NaN group stays NaN. The
/// extremum is NaN exactly until the first real value, so it doubles as
/// the "any real" flag.
struct ExtremumLanes<const MAX: bool> {
    count: Vec<u64>,
    value: Vec<f64>,
}

#[inline]
fn extremum_step<const MAX: bool>(count: &mut u64, m: &mut f64, v: f64, ok: bool) {
    let better = if MAX { v > *m } else { v < *m };
    let take = ok & (better | (m.is_nan() & !v.is_nan()));
    *count = count.saturating_add(u64::from(ok));
    *m = blend(take, v, *m);
}

impl<const MAX: bool> Lanes for ExtremumLanes<MAX> {
    #[inline]
    fn absorb(&mut self, at: usize, v: f64) {
        extremum_step::<MAX>(&mut self.count[at], &mut self.value[at], v, true);
    }

    fn absorb_run(&mut self, at: usize, values: &[f64], valid: &[bool]) {
        let n = values.len();
        extremum_run::<MAX>(
            &mut self.count[at..at + n],
            &mut self.value[at..at + n],
            values,
            valid,
        );
    }
}

#[inline(never)]
fn extremum_run<const MAX: bool>(
    count: &mut [u64],
    value: &mut [f64],
    values: &[f64],
    valid: &[bool],
) {
    for ((count, m), (&v, &ok)) in count.iter_mut().zip(value).zip(values.iter().zip(valid)) {
        extremum_step::<MAX>(count, m, v, ok);
    }
}

/// Welford with Kahan compensation on both mean and M2.
struct WelfordLanes {
    count: Vec<u64>,
    mean: Vec<f64>,
    c_mean: Vec<f64>,
    m2: Vec<f64>,
    c_m2: Vec<f64>,
}

#[inline]
fn welford_step(
    count: &mut u64,
    (mean, c_mean): (&mut f64, &mut f64),
    (m2, c_m2): (&mut f64, &mut f64),
    v: f64,
    ok: bool,
) {
    let n = count.saturating_add(1) as f64;
    let delta = v - (*mean + *c_mean);
    let (new_mean, new_c_mean) = kahan_inc(delta / n, *mean, *c_mean);
    let new_delta = v - (new_mean + new_c_mean);
    let (new_m2, new_c_m2) = kahan_inc(delta * new_delta, *m2, *c_m2);
    *count = count.saturating_add(u64::from(ok));
    *mean = blend(ok, new_mean, *mean);
    *c_mean = blend(ok, new_c_mean, *c_mean);
    *m2 = blend(ok, new_m2, *m2);
    *c_m2 = blend(ok, new_c_m2, *c_m2);
}

impl WelfordLanes {
    /// Population variance: M2 / n.
    #[inline]
    fn variance(&self, at: usize) -> f64 {
        let n = self.count[at] as f64;
        (self.m2[at] + self.c_m2[at]) / n
    }
}

impl Lanes for WelfordLanes {
    #[inline]
    fn absorb(&mut self, at: usize, v: f64) {
        welford_step(
            &mut self.count[at],
            (&mut self.mean[at], &mut self.c_mean[at]),
            (&mut self.m2[at], &mut self.c_m2[at]),
            v,
            true,
        );
    }

    fn absorb_run(&mut self, at: usize, values: &[f64], valid: &[bool]) {
        let n = values.len();
        welford_run(
            &mut self.count[at..at + n],
            &mut self.mean[at..at + n],
            &mut self.c_mean[at..at + n],
            &mut self.m2[at..at + n],
            &mut self.c_m2[at..at + n],
            values,
            valid,
        );
    }
}

#[inline(never)]
fn welford_run(
    count: &mut [u64],
    mean: &mut [f64],
    c_mean: &mut [f64],
    m2: &mut [f64],
    c_m2: &mut [f64],
    values: &[f64],
    valid: &[bool],
) {
    for ((((count, (mean, c_mean)), m2), c_m2), (&v, &ok)) in count
        .iter_mut()
        .zip(mean.iter_mut().zip(c_mean))
        .zip(m2)
        .zip(c_m2)
        .zip(values.iter().zip(valid))
    {
        welford_step(count, (mean, c_mean), (m2, c_m2), v, ok);
    }
}

enum GridLanes {
    Count(CountLanes),
    Sum(SumLanes),
    Avg(AvgLanes),
    Min(ExtremumLanes<false>),
    Max(ExtremumLanes<true>),
    Welford(WelfordLanes),
}

/// The `(group × step)` accumulator grid of one streaming aggregate, plus
/// the transpose scratch its run kernels read from.
pub(super) struct AccumGrid {
    step_count: usize,
    lanes: GridLanes,
    run_values: Vec<f64>,
    run_valid: Vec<bool>,
}

impl AccumGrid {
    /// Bytes per accumulator for `kind`.
    fn cell_bytes(kind: AggregateKind) -> usize {
        const COUNT: usize = size_of::<u64>();
        const F64: usize = size_of::<f64>();
        match kind {
            AggregateKind::Count | AggregateKind::Group => COUNT,
            AggregateKind::Sum => COUNT + 2 * F64,
            AggregateKind::Avg => COUNT + 4 * F64 + size_of::<bool>(),
            AggregateKind::Min | AggregateKind::Max => COUNT + F64,
            AggregateKind::Stddev | AggregateKind::Stdvar => COUNT + 4 * F64,
            AggregateKind::Topk(_) | AggregateKind::Bottomk(_) | AggregateKind::Quantile(_) => 0,
        }
    }

    fn scratch_cells(kind: AggregateKind, step_count: usize, input_series: usize) -> usize {
        let runs = !matches!(kind, AggregateKind::Count | AggregateKind::Group);
        if !runs || step_count < MIN_RUN_STEPS {
            return 0;
        }
        TILE_STEPS.min(step_count) * TILE_SERIES.min(input_series)
    }

    /// Bytes [`Self::new`] allocates for the same arguments.
    pub(super) fn bytes(
        kind: AggregateKind,
        step_count: usize,
        group_count: usize,
        input_series: usize,
    ) -> usize {
        let grid = step_count
            .saturating_mul(group_count)
            .saturating_mul(Self::cell_bytes(kind));
        let scratch = Self::scratch_cells(kind, step_count, input_series)
            * (size_of::<f64>() + size_of::<bool>());
        grid.saturating_add(scratch)
    }

    /// Panics on breaker kinds, which keep no accumulator grid.
    pub(super) fn new(
        kind: AggregateKind,
        step_count: usize,
        group_count: usize,
        input_series: usize,
    ) -> Self {
        let cells = step_count.saturating_mul(group_count);
        let count = || vec![0u64; cells];
        let zeros = || vec![0.0f64; cells];
        let lanes = match kind {
            AggregateKind::Count | AggregateKind::Group => {
                GridLanes::Count(CountLanes { count: count() })
            }
            AggregateKind::Sum => GridLanes::Sum(SumLanes {
                count: count(),
                sum: zeros(),
                c: zeros(),
            }),
            AggregateKind::Avg => GridLanes::Avg(AvgLanes {
                sum: SumLanes {
                    count: count(),
                    sum: zeros(),
                    c: zeros(),
                },
                incremental: vec![false; cells],
                mean: zeros(),
                c_mean: zeros(),
            }),
            AggregateKind::Min => GridLanes::Min(ExtremumLanes {
                count: count(),
                value: vec![f64::NAN; cells],
            }),
            AggregateKind::Max => GridLanes::Max(ExtremumLanes {
                count: count(),
                value: vec![f64::NAN; cells],
            }),
            AggregateKind::Stddev | AggregateKind::Stdvar => GridLanes::Welford(WelfordLanes {
                count: count(),
                mean: zeros(),
                c_mean: zeros(),
                m2: zeros(),
                c_m2: zeros(),
            }),
            AggregateKind::Topk(_) | AggregateKind::Bottomk(_) | AggregateKind::Quantile(_) => {
                unreachable!("breaker kinds keep no accumulator grid")
            }
        };
        let scratch = Self::scratch_cells(kind, step_count, input_series);
        Self {
            step_count,
            lanes,
            run_values: vec![0.0; scratch],
            run_valid: vec![false; scratch],
        }
    }

    /// Lane index of the accumulator for `(group, step)`.
    #[inline]
    pub(super) fn index(&self, group: usize, step: usize) -> usize {
        group * self.step_count + step
    }

    /// Valid float inputs absorbed by the accumulator at `at`.
    #[inline]
    pub(super) fn count(&self, at: usize) -> u64 {
        match &self.lanes {
            GridLanes::Count(l) => l.count[at],
            GridLanes::Sum(l) => l.count[at],
            GridLanes::Avg(l) => l.sum.count[at],
            GridLanes::Min(l) => l.count[at],
            GridLanes::Max(l) => l.count[at],
            GridLanes::Welford(l) => l.count[at],
        }
    }

    /// Writes the reduced value of every accumulator that absorbed a float
    /// into step-major `values` / `validity` (`[step * group_count +
    /// group]`), leaving the other cells untouched.
    pub(super) fn write_floats(
        &self,
        kind: AggregateKind,
        group_count: usize,
        values: &mut [f64],
        validity: &mut BitSet,
    ) {
        let out = Out {
            step_count: self.step_count,
            group_count,
            values,
            validity,
        };
        match (&self.lanes, kind) {
            (GridLanes::Count(l), AggregateKind::Count) => {
                out.write(&l.count, |at| l.count[at] as f64)
            }
            (GridLanes::Count(l), _) => out.write(&l.count, |_| 1.0),
            (GridLanes::Sum(l), _) => out.write(&l.count, |at| l.value(at)),
            (GridLanes::Avg(l), _) => out.write(&l.sum.count, |at| l.value(at)),
            (GridLanes::Min(l), _) => out.write(&l.count, |at| l.value[at]),
            (GridLanes::Max(l), _) => out.write(&l.count, |at| l.value[at]),
            (GridLanes::Welford(l), AggregateKind::Stddev) => {
                out.write(&l.count, |at| l.variance(at).sqrt())
            }
            (GridLanes::Welford(l), _) => out.write(&l.count, |at| l.variance(at)),
        }
    }

    /// Absorb every valid float cell of `input`. `input_to_group` is the
    /// full group map, indexed by global input series.
    pub(super) fn absorb_batch(&mut self, input: &StepBatch, input_to_group: &[Option<u32>]) {
        let groups = &input_to_group[input.series_range.clone()];
        let Self {
            step_count,
            lanes,
            run_values,
            run_valid,
        } = self;
        let mut scratch = Scratch {
            values: run_values,
            valid: run_valid,
        };
        match lanes {
            GridLanes::Count(l) => l.absorb_batch(*step_count, input, groups),
            GridLanes::Sum(l) => absorb_into(l, *step_count, input, groups, &mut scratch),
            GridLanes::Avg(l) => absorb_into(l, *step_count, input, groups, &mut scratch),
            GridLanes::Min(l) => absorb_into(l, *step_count, input, groups, &mut scratch),
            GridLanes::Max(l) => absorb_into(l, *step_count, input, groups, &mut scratch),
            GridLanes::Welford(l) => absorb_into(l, *step_count, input, groups, &mut scratch),
        }
    }
}

struct Out<'a> {
    step_count: usize,
    group_count: usize,
    values: &'a mut [f64],
    validity: &'a mut BitSet,
}

impl Out<'_> {
    /// Groups at a time per step: a cache line of output, and one cursor
    /// per group into the group-major lanes. Walking a whole group at once
    /// would write at a `group_count × 8` byte stride instead.
    const GROUP_BLOCK: usize = 8;

    fn write(self, count: &[u64], value: impl Fn(usize) -> f64) {
        for g0 in (0..self.group_count).step_by(Self::GROUP_BLOCK) {
            let groups = g0..(g0 + Self::GROUP_BLOCK).min(self.group_count);
            for step in 0..self.step_count {
                for g in groups.clone() {
                    let at = g * self.step_count + step;
                    if count[at] > 0 {
                        let idx = step * self.group_count + g;
                        self.values[idx] = value(at);
                        self.validity.set(idx);
                    }
                }
            }
        }
    }
}

struct Scratch<'a> {
    values: &'a mut [f64],
    valid: &'a mut [bool],
}

fn absorb_into<L: Lanes>(
    lanes: &mut L,
    step_count: usize,
    input: &StepBatch,
    groups: &[Option<u32>],
    scratch: &mut Scratch<'_>,
) {
    if input.step_count() >= MIN_RUN_STEPS {
        absorb_runs(lanes, step_count, input, groups, scratch);
    } else {
        absorb_cells(lanes, step_count, input, groups);
    }
}

fn absorb_cells<L: Lanes>(
    lanes: &mut L,
    step_count: usize,
    input: &StepBatch,
    groups: &[Option<u32>],
) {
    let series = input.series_count();
    for step_off in 0..input.step_count() {
        let step = input.step_range.start + step_off;
        let row = step_off * series;
        input.validity.for_each_set_in(row..row + series, |cell| {
            if let Some(g) = groups[cell - row] {
                lanes.absorb(g as usize * step_count + step, input.values[cell]);
            }
        });
    }
}

/// Transposes `input` tile by tile into series-major runs, then feeds each
/// grouped series' run to its group's lanes. Step tiles go outermost and
/// series ascend within one, so every accumulator still sees series in
/// ascending order.
fn absorb_runs<L: Lanes>(
    lanes: &mut L,
    step_count: usize,
    input: &StepBatch,
    groups: &[Option<u32>],
    scratch: &mut Scratch<'_>,
) {
    let (steps, series) = (input.step_count(), input.series_count());
    let words = input.validity.words();
    for t0 in (0..steps).step_by(TILE_STEPS) {
        let tn = TILE_STEPS.min(steps - t0);
        for s0 in (0..series).step_by(TILE_SERIES) {
            let sn = TILE_SERIES.min(series - s0);
            let values = &mut scratch.values[..tn * sn];
            let valid = &mut scratch.valid[..tn * sn];
            for dt in 0..tn {
                let row = (t0 + dt) * series + s0;
                for (ds, &v) in input.values[row..row + sn].iter().enumerate() {
                    let cell = row + ds;
                    values[ds * tn + dt] = v;
                    valid[ds * tn + dt] = (words[cell / 64] >> (cell % 64)) & 1 == 1;
                }
            }
            for (ds, group) in groups[s0..s0 + sn].iter().enumerate() {
                let Some(g) = *group else { continue };
                let at = g as usize * step_count + input.step_range.start + t0;
                let run = ds * tn..(ds + 1) * tn;
                lanes.absorb_run(at, &values[run.clone()], &valid[run]);
            }
        }
    }
}

/// The cell-at-a-time all-lanes accumulator the grid replaced, kept as the
/// bit-exactness oracle for tests and the baseline for benches.
#[cfg(any(test, feature = "bench-internals"))]
pub(crate) mod reference {
    use super::super::{AggregateKind, GroupMap};
    use crate::promql::batch::{BitSet, StepBatch};

    /// The original out-of-line, branchy Kahan step.
    #[inline(never)]
    pub(crate) fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
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

    #[derive(Debug, Clone, Copy)]
    struct Accumulator {
        count: u64,
        sum: f64,
        c_sum: f64,
        min: f64,
        max: f64,
        any_real: bool,
        mean: f64,
        c_mean: f64,
        m2: f64,
        c_m2: f64,
        avg_incremental: bool,
        avg_mean: f64,
        c_avg_mean: f64,
    }

    impl Accumulator {
        fn new() -> Self {
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
        fn absorb(&mut self, v: f64) {
            self.count = self.count.saturating_add(1);
            let n = self.count as f64;

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

            let delta = v - (self.mean + self.c_mean);
            let (new_mean, new_c_mean) = kahan_inc(delta / n, self.mean, self.c_mean);
            self.mean = new_mean;
            self.c_mean = new_c_mean;
            let new_delta = v - (self.mean + self.c_mean);
            let (new_m2, new_c_m2) = kahan_inc(delta * new_delta, self.m2, self.c_m2);
            self.m2 = new_m2;
            self.c_m2 = new_c_m2;
        }

        fn value(&self, kind: AggregateKind) -> f64 {
            let n = self.count as f64;
            let variance = || (self.m2 + self.c_m2) / n;
            match kind {
                AggregateKind::Sum if self.sum.is_infinite() => self.sum,
                AggregateKind::Sum => self.sum + self.c_sum,
                AggregateKind::Avg if self.avg_incremental => self.avg_mean + self.c_avg_mean,
                AggregateKind::Avg => self.sum / n + self.c_sum / n,
                AggregateKind::Min => self.min,
                AggregateKind::Max => self.max,
                AggregateKind::Count => n,
                AggregateKind::Stddev => variance().sqrt(),
                AggregateKind::Stdvar => variance(),
                AggregateKind::Group => 1.0,
                AggregateKind::Topk(_) | AggregateKind::Bottomk(_) | AggregateKind::Quantile(_) => {
                    unreachable!("breaker kind")
                }
            }
        }
    }

    /// Float cells of `batches` reduced the way the streaming aggregate did
    /// before the per-kind grid: step-major, one all-lanes accumulator per
    /// `(step, group)`. Output cell `(step, group)` at `step * group_count +
    /// group`.
    pub(crate) fn aggregate(
        kind: AggregateKind,
        step_count: usize,
        group_map: &GroupMap,
        batches: &[StepBatch],
    ) -> (Vec<f64>, BitSet) {
        let group_count = group_map.group_count;
        let cells = step_count * group_count;
        let mut accums = vec![Accumulator::new(); cells];
        for input in batches {
            let in_series_count = input.series_count();
            for step_off in 0..input.step_count() {
                let global_step = input.step_range.start + step_off;
                let step_base = step_off * in_series_count;
                let accum_base = global_step * group_count;
                for in_series in 0..in_series_count {
                    let cell = step_base + in_series;
                    if !input.validity.get(cell) {
                        continue;
                    }
                    let global_series = input.series_range.start + in_series;
                    let group = match group_map.input_to_group[global_series] {
                        Some(g) => g as usize,
                        None => continue,
                    };
                    accums[accum_base + group].absorb(input.values[cell]);
                }
            }
        }
        let mut values = vec![0.0; cells];
        let mut validity = BitSet::with_len(cells);
        for (idx, accum) in accums.iter().enumerate() {
            if accum.count > 0 {
                values[idx] = accum.value(kind);
                validity.set(idx);
            }
        }
        (values, validity)
    }
}
