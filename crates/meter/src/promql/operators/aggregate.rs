//! `AggregateOp` implements PromQL's aggregation operators: `sum by (…)`,
//! `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `topk`,
//! `bottomk`, and `quantile`. One operator type handles all of them; the
//! specific reduction is selected by an [`AggregateKind`] enum.
//! (`count_values` has its own operator because its output labels depend on
//! sample values, not just label matchers.)
//!
//! PromQL's `by (…)` / `without (…)` decides which input series end up in
//! the same output group. That mapping is fixed by the query's labels and
//! doesn't depend on sample data, so the planner computes it once up
//! front — the operator just receives a [`GroupMap`]
//! (input series index → output group index) and reads from it. For
//! `sum by (pod) (http_requests_total)`, the planner maps each input
//! series to a group index keyed on its `pod` label value before any
//! samples are fetched. No grouping logic at runtime.
//!
//! # Streaming kinds
//!
//! `Sum`, `Avg`, `Min`, `Max`, `Count`, `Stddev`, `Stdvar`, `Group` apply a
//! per-cell single-pass reducer. The child may tile its output arbitrarily
//! (any `(step_range, series_range)` slice), so the operator buffers a
//! `(step × group)` accumulator grid and only emits once the child signals
//! end-of-stream. Grid footprint is
//! `O(step_count × output_groups × sizeof(Accumulator))`.
//!
//! # Breaker kinds — `Topk`, `Bottomk`, `Quantile`
//!
//! These buffer the whole step before emitting (K-selection and quantile
//! interpolation need every input for the step, so they can't stream
//! cell-by-cell like the reducer kinds — hence "breaker").
//!
//! | kind         | output schema  | per-step scratch        |
//! |--------------|----------------|-------------------------|
//! | `Topk(k)`    | input series   | `k × group_count` heap  |
//! | `Bottomk(k)` | input series   | `k × group_count` heap  |
//! | `Quantile(q)`| group series   | per-group sort buffer   |
//!
//! `topk` / `bottomk` filter (preserve input series, flip validity on
//! unselected cells); `quantile` reduces (one cell per group). The planner
//! picks the output schema; the operator debug-asserts its size matches the
//! variant.
//!
//! Tie-break for `topk` / `bottomk`: equal values go to the lower
//! input-series index (deterministic). NaN inputs rank worst. `k < 1`
//! selects nothing; `k` past input width selects every valid cell.
//!
//! `quantile`: `q < 0` ⇒ `-inf`, `q > 1` ⇒ `+inf`, `q == NaN` ⇒ `NaN`.
//!
//! # Group map
//!
//! `input_to_group: Vec<Option<u32>>` — `None` drops an input from all
//! aggregations. `by ()` ⇒ all inputs map to group 0. `without ()` ⇒ one
//! group per input.
//!
//! # Validity
//!
//! Every kind emits a valid output cell iff at least one valid input
//! contributed to its group. `Group` always emits `1.0`; `Count` emits the
//! count (always ≥ 1 when valid).
//!
//! # Memory
//!
//! Streaming-kind accumulator grid is allocated once at construction and
//! fails with [`QueryError::MemoryLimit`] if it doesn't fit. Breaker
//! scratch is per-step, drained between steps. Output batches allocate
//! per-poll.
//!
//! Output schema is always [`SchemaRef::Static`].

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

mod accumulator;
mod breaker;

use accumulator::*;
use breaker::*;

// ---------------------------------------------------------------------------
// AggregateKind — function selection as data
// ---------------------------------------------------------------------------

/// Discriminant selecting the per-group reducer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateKind {
    /// Kahan-compensated.
    Sum,
    /// Overflow-resistant running mean.
    Avg,
    /// NaN-safe (NaN ignored when a real value exists).
    Min,
    /// NaN-safe (NaN ignored when a real value exists).
    Max,
    /// Integer count emitted as `f64`.
    Count,
    /// Welford single-pass.
    Stddev,
    /// Welford single-pass.
    Stdvar,
    /// Always `1.0` when any input contributed.
    Group,
    /// `topk(k, v)`. Output schema is the **input series**; unselected cells
    /// get `validity = 0`. See module docs for tie-break / k semantics.
    Topk(i64),
    /// `bottomk(k, v)` — smallest-K counterpart to [`Self::Topk`].
    Bottomk(i64),
    /// `quantile(q, v)` — per-group, per-step q-th quantile with linear
    /// interpolation between ranks. Shares the streaming output-schema
    /// shape (one cell per group). See `rollup_fns::quantile` in
    /// `operators::rollup` for the canonical reducer; this operator
    /// calls into the same implementation.
    Quantile(f64),
}

impl AggregateKind {
    /// True for variants that buffer the whole step before emitting
    /// (pipeline breakers).
    #[inline]
    fn is_breaker(self) -> bool {
        matches!(self, Self::Topk(_) | Self::Bottomk(_) | Self::Quantile(_))
    }

    /// True for variants whose output-series count equals the input
    /// series count (filter-shaped). False for reducer-shaped variants
    /// (output-series count == group_count).
    #[inline]
    fn output_is_inputs(self) -> bool {
        matches!(self, Self::Topk(_) | Self::Bottomk(_))
    }
}

// ---------------------------------------------------------------------------
// GroupMap — the planner-built series-index → group-index mapping
// ---------------------------------------------------------------------------

/// Input-series index → output-group index, precomputed by the planner
/// from the query's `by` / `without` clause. The operator reads this array
/// to decide which accumulator absorbs each cell; it never recomputes
/// grouping itself.
///
/// Example: for `sum by (pod) (http_requests_total)`, the planner walks the
/// resolved series list, bucketises by `pod` label value, and records a
/// group index (or `None` to drop the input) for each input series here.
///
/// Invariants (planner-guaranteed):
/// - `input_to_group.len()` equals the input operator's series count.
/// - Every `Some(g)` satisfies `g < group_count`.
#[derive(Debug, Clone)]
pub struct GroupMap {
    /// `None` drops an input from aggregation.
    pub input_to_group: Vec<Option<u32>>,
    pub group_count: usize,
}

impl GroupMap {
    pub fn new(input_to_group: Vec<Option<u32>>, group_count: usize) -> Self {
        debug_assert!(input_to_group.iter().all(|slot| match slot {
            Some(g) => (*g as usize) < group_count,
            None => true,
        }));
        Self {
            input_to_group,
            group_count,
        }
    }

    #[inline]
    pub fn input_series_count(&self) -> usize {
        self.input_to_group.len()
    }
}

// ---------------------------------------------------------------------------
// Memory-accounted output buffers
// ---------------------------------------------------------------------------

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

#[inline]
fn accum_bytes(step_count: usize, group_count: usize) -> usize {
    step_count
        .saturating_mul(group_count)
        .saturating_mul(std::mem::size_of::<Accumulator>())
}

/// Upper-bound bytes for the per-group min-heap scratch used by
/// topk/bottomk. Each of the `group_count` groups holds at most `k`
/// entries; when `k` exceeds the input series count the loop caps the
/// actual length, but we reserve on the naïve product as a conservative
/// upper bound. `k <= 0` ⇒ zero bytes (no selection happens).
#[inline]
fn heap_scratch_bytes(k: i64, group_count: usize, input_series: usize) -> usize {
    if k <= 0 {
        return 0;
    }
    let k = (k as usize).min(input_series);
    group_count
        .saturating_mul(k)
        .saturating_mul(std::mem::size_of::<KHeapEntry>())
}

/// Upper-bound bytes for the per-group sort buffer used by `quantile`.
/// Sum of per-group capacities is at most `input_series_count` (each
/// input belongs to at most one group), so reserve `input_series ×
/// sizeof(f64)` + the `Vec<Vec<f64>>` outer skeleton.
#[inline]
fn sort_scratch_bytes(group_count: usize, input_series: usize) -> usize {
    let values = input_series.saturating_mul(std::mem::size_of::<f64>());
    let outer = group_count.saturating_mul(std::mem::size_of::<Vec<f64>>());
    values.saturating_add(outer)
}

/// Bytes for the full `(step × input_series)` breaker grid (values +
/// validity bit). Reserved up front so an undersize reservation surfaces
/// `QueryError::MemoryLimit` at construction rather than deep in the
/// hot path.
#[inline]
fn breaker_grid_bytes(cells: usize) -> usize {
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
// AggregateOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's aggregation operators (`sum by (…)`, `topk`,
/// `quantile`, ...). One operator type, with the specific reduction
/// selected by an [`AggregateKind`] enum.
///
/// See module docs for supported kinds, group-map semantics, validity
/// rules, and memory accounting.
pub struct AggregateOp<C: Operator> {
    child: C,
    param_child: Option<Box<dyn Operator + Send>>,
    kind: AggregateKind,
    group_map: GroupMap,
    output_schema: Arc<SeriesSchema>,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Optional per-step `k` values for dynamic `topk` / `bottomk`
    /// parameters. Present only when lowering supplied a scalar child.
    param_values: Option<Vec<i64>>,
    /// Bytes reserved for `param_values`; released on `Drop`.
    param_bytes: usize,
    /// `true` once the optional scalar parameter child has been fully
    /// drained into [`Self::param_values`].
    param_loaded: bool,
    /// Per-step-per-group accumulator grid used by the streaming kinds.
    ///
    /// Length = `step_count × group_count`, indexed row-major:
    /// `accums[global_step * group_count + group]`. Allocated once at
    /// construction and absorbed-into across every input batch, regardless
    /// of how the child tiles its emission (step tiles × series tiles).
    /// Empty for breaker kinds.
    ///
    /// Streaming aggregate is a step-bounded breaker — it must see every
    /// series tile for a given step before it can emit, because
    /// aggregations are associative across input rows but not decomposable
    /// into "partial tile sums" without a downstream merge. Buffering the
    /// whole grid keeps the operator correct under arbitrary batch
    /// ordering (step/series tiles interleaved, or split by
    /// `Concurrent`/`Coalesce`).
    accums: Vec<Accumulator>,
    /// Step timestamps captured from the first child batch. Reused for the
    /// single output batch this operator emits on EOS. Streaming kinds
    /// only; breaker kinds echo the child batch's timestamps directly.
    streaming_step_timestamps: Option<Arc<[i64]>>,
    /// Per-group min-heap scratch for `Topk`/`Bottomk`; one heap per
    /// group, drained between steps. Empty for other kinds.
    heaps: Vec<BinaryHeap<KHeapEntry>>,
    /// Per-group sort buffer for `Quantile`; one Vec per group, drained
    /// between steps. Empty for other kinds.
    sort_bufs: Vec<Vec<f64>>,
    /// Bytes reserved for the per-kind scratch (accums | heaps |
    /// sort_bufs); released on `Drop`.
    scratch_bytes: usize,
    /// Full input grid `step_count × input_series_count` used by the
    /// breaker kinds to absorb every child batch before running the
    /// selection / quantile per step. `values` is row-major
    /// (`values[step * input_series + series]`); `validity` runs
    /// parallel. Allocated alongside the per-kind heap/sort buffers so
    /// breakers remain correct under arbitrary input tiling. Empty for
    /// streaming kinds.
    breaker_values: Vec<f64>,
    breaker_validity: BitSet,
    /// Bytes reserved for [`Self::breaker_values`] / `breaker_validity`;
    /// released on `Drop`. Tracked separately from [`Self::scratch_bytes`]
    /// so construction can fail-fast on an undersize reservation.
    breaker_grid_bytes: usize,
    /// Step timestamps captured from the first child batch observed on
    /// the breaker path. Reused for the output batches emitted on EOS.
    /// Breaker kinds only.
    breaker_step_timestamps: Option<Arc<[i64]>>,
    /// `true` once the single output batch has been emitted on EOS.
    emitted: bool,
    /// `true` once at least one child batch has been absorbed. Tracked on
    /// `self` (not as a `next()`-local) so the flag survives
    /// `Poll::Pending` re-entries — under a `Concurrent` producer, `next()`
    /// is entered once per child batch, so a local flag would reset between
    /// absorb and EOS and skip `finalise`.
    saw_any_batch: bool,
    done: bool,
    errored: bool,
}

impl<C: Operator> AggregateOp<C> {
    #[inline]
    fn debug_assert_batch_within_input_roster(&self, input: &StepBatch) {
        debug_assert!(
            input.series_range.end <= self.group_map.input_series_count(),
            "child series_range {:?} exceeds group map input count {}",
            input.series_range,
            self.group_map.input_series_count(),
        );
    }

    /// Construct a streaming aggregate.
    ///
    /// * `child` — upstream operator.
    /// * `kind` — plan-time-selected reducer.
    /// * `group_map` — planner-built input-series → output-group
    ///   mapping; `group_map.input_series_count()` must match the
    ///   child's series count.
    /// * `output_schema` — planner-built output series roster; must
    ///   have `group_map.group_count` entries.
    /// * `reservation` — per-query reservation; charged for the per-
    ///   group scratch and every output batch.
    pub fn new(
        child: C,
        kind: AggregateKind,
        group_map: GroupMap,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Result<Self, QueryError> {
        Self::new_with_param(child, None, kind, group_map, output_schema, reservation)
    }

    pub fn new_with_param(
        child: C,
        param_child: Option<Box<dyn Operator + Send>>,
        kind: AggregateKind,
        group_map: GroupMap,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Result<Self, QueryError> {
        // Output schema shape depends on the variant. Planner is the
        // authority (see module docs §"Breakers / output schema
        // asymmetry"); these debug asserts catch planner bugs in tests.
        if kind.output_is_inputs() {
            debug_assert_eq!(
                group_map.input_series_count(),
                output_schema.len(),
                "topk/bottomk output schema must equal input series count",
            );
        } else {
            debug_assert_eq!(
                group_map.group_count,
                output_schema.len(),
                "streaming / quantile output schema must equal group_count",
            );
        }

        let grid = child.schema().step_grid;
        let schema = OperatorSchema::new(SchemaRef::Static(output_schema.clone()), grid);
        debug_assert!(
            param_child.is_none()
                || matches!(kind, AggregateKind::Topk(_) | AggregateKind::Bottomk(_)),
            "dynamic aggregate params are only supported for topk/bottomk",
        );
        if let Some(param_child) = &param_child {
            debug_assert_eq!(
                param_child.schema().step_grid,
                grid,
                "scalar aggregate param must share the main child step grid",
            );
        }

        let (param_values, param_bytes) = if param_child.is_some() {
            let bytes = grid.step_count.saturating_mul(std::mem::size_of::<i64>());
            reservation.try_grow(bytes)?;
            (Some(vec![0; grid.step_count]), bytes)
        } else {
            (None, 0)
        };

        // Breaker kinds need a full-grid (step × input_series) buffer
        // so `topk` / `bottomk` / `quantile` select globally against the
        // full input at each step rather than per-tile. Allocated
        // alongside the per-kind scratch so construction fails fast on
        // undersize reservations.
        let (breaker_values, breaker_validity, breaker_grid_bytes) = if kind.is_breaker() {
            let cells = grid
                .step_count
                .saturating_mul(group_map.input_series_count());
            let bytes = breaker_grid_bytes(cells);
            reservation.try_grow(bytes)?;
            (vec![f64::NAN; cells], BitSet::with_len(cells), bytes)
        } else {
            (Vec::new(), BitSet::with_len(0), 0)
        };

        // Per-kind scratch allocation. Reserve up front for the hot
        // path; release on `Drop`. Breakers skip the accumulator column
        // entirely (cheaper than paying for unused lanes).
        let (accums, heaps, sort_bufs, bytes) = match kind {
            AggregateKind::Topk(k) | AggregateKind::Bottomk(k) => {
                let heap_k = if param_child.is_some() {
                    group_map.input_series_count() as i64
                } else {
                    k
                };
                let bytes = heap_scratch_bytes(
                    heap_k,
                    group_map.group_count,
                    group_map.input_series_count(),
                );
                reservation.try_grow(bytes)?;
                // One heap per group — capacity is clamped to effective
                // K (≤ input_series_count) to keep the allocation tight.
                let cap = if heap_k <= 0 {
                    0
                } else {
                    (heap_k as usize).min(group_map.input_series_count())
                };
                let heaps = (0..group_map.group_count)
                    .map(|_| BinaryHeap::with_capacity(cap))
                    .collect();
                (Vec::new(), heaps, Vec::new(), bytes)
            }
            AggregateKind::Quantile(_) => {
                let bytes =
                    sort_scratch_bytes(group_map.group_count, group_map.input_series_count());
                reservation.try_grow(bytes)?;
                let sort_bufs: Vec<Vec<f64>> =
                    (0..group_map.group_count).map(|_| Vec::new()).collect();
                (Vec::new(), Vec::new(), sort_bufs, bytes)
            }
            _ => {
                // Streaming kinds buffer a (step × group) accumulator grid
                // so they remain correct under arbitrary input tiling
                // (series tiles × step tiles).
                let bytes = accum_bytes(grid.step_count, group_map.group_count);
                reservation.try_grow(bytes)?;
                let cells = grid.step_count.saturating_mul(group_map.group_count);
                let accums = vec![Accumulator::new(); cells];
                (accums, Vec::new(), Vec::new(), bytes)
            }
        };

        Ok(Self {
            child,
            param_child,
            kind,
            group_map,
            output_schema,
            reservation,
            schema,
            param_values,
            param_bytes,
            param_loaded: false,
            accums,
            streaming_step_timestamps: None,
            heaps,
            sort_bufs,
            scratch_bytes: bytes,
            breaker_values,
            breaker_validity,
            breaker_grid_bytes,
            breaker_step_timestamps: None,
            emitted: false,
            saw_any_batch: false,
            done: false,
            errored: false,
        })
    }

    fn load_param_values(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), QueryError>> {
        if self.param_loaded {
            return Poll::Ready(Ok(()));
        }
        let Some(param_child) = self.param_child.as_mut() else {
            self.param_loaded = true;
            return Poll::Ready(Ok(()));
        };

        loop {
            match param_child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.param_child = None;
                    self.param_loaded = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(batch))) => {
                    debug_assert_eq!(
                        batch.series_count(),
                        1,
                        "aggregate scalar param must produce exactly one series",
                    );
                    for step_off in 0..batch.step_count() {
                        let global_step = batch.step_range.start + step_off;
                        let cell = batch.cell_index(step_off, 0);
                        let value = if batch.validity.get(cell) {
                            batch.values[cell] as i64
                        } else {
                            0
                        };
                        if let Some(param_values) = self.param_values.as_mut() {
                            param_values[global_step] = value;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn k_for_step(&self, step_idx: usize, input_len: usize, static_k: i64) -> usize {
        let k_param = self
            .param_values
            .as_ref()
            .map(|values| values[step_idx])
            .unwrap_or(static_k);
        coerce_k_size(k_param, input_len)
    }

    /// Absorb one child batch into the streaming-kind per-(step, group)
    /// accumulator grid. Idempotent across arbitrary batch ordering:
    /// step tiles × series tiles × `Concurrent`/`Coalesce` interleaving
    /// all funnel into the same buffered grid, and the final aggregate is
    /// associative on insertion order (modulo floating-point rounding,
    /// which the Kahan lane caps).
    fn absorb_batch_streaming(&mut self, input: &StepBatch) {
        debug_assert!(!self.kind.is_breaker());
        self.debug_assert_batch_within_input_roster(input);

        // Capture the step-timestamp `Arc<[i64]>` from the first non-empty
        // batch we see. Every child batch shares the same outer grid, so
        // any single batch's slice is authoritative for its own step span;
        // we only need one source of truth because the output batch covers
        // the entire grid (`0..step_count`). If the child never emits any
        // batches we synthesise a fresh timestamps array in
        // `finalise_streaming` from the plan-time grid.
        if self.streaming_step_timestamps.is_none()
            && input.step_range.start == 0
            && input.step_count() == self.schema.step_grid.step_count
        {
            self.streaming_step_timestamps = Some(input.step_timestamps.clone());
        }

        let step_count_in = input.step_count();
        let in_series_count = input.series_count();
        let group_count = self.group_map.group_count;

        // Row-major: for each step in the input batch, offset to the
        // global step index and absorb every valid cell into
        // `accums[global_step * group_count + group]`.
        for step_off in 0..step_count_in {
            let global_step = input.step_range.start + step_off;
            let step_base = step_off * in_series_count;
            let accum_base = global_step * group_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = input.values[cell];
                self.accums[accum_base + group].absorb(v);
            }
        }
    }

    /// Produce the single output batch covering the full outer grid for
    /// streaming kinds. Called once, on child-EOS, after every batch has
    /// been absorbed via [`Self::absorb_batch_streaming`].
    fn finalise_streaming(&mut self) -> Result<StepBatch, QueryError> {
        debug_assert!(!self.kind.is_breaker());
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let group_count = self.group_map.group_count;
        let out_cells = step_count.saturating_mul(group_count);

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step in 0..step_count {
            let accum_base = step * group_count;
            let out_base = step * group_count;
            for g in 0..group_count {
                let accum = &self.accums[accum_base + g];
                if accum.count == 0 {
                    continue;
                }
                let value = match self.kind {
                    AggregateKind::Sum => accum.sum_value(),
                    AggregateKind::Avg => accum.avg_value(),
                    AggregateKind::Min => accum.min,
                    AggregateKind::Max => accum.max,
                    AggregateKind::Count => accum.count as f64,
                    AggregateKind::Stddev => accum.variance_value().sqrt(),
                    AggregateKind::Stdvar => accum.variance_value(),
                    AggregateKind::Group => 1.0,
                    // Unreachable: breakers are routed through
                    // `reduce_batch_breaker` and never visit this path.
                    AggregateKind::Topk(_)
                    | AggregateKind::Bottomk(_)
                    | AggregateKind::Quantile(_) => {
                        unreachable!("breaker kind routed to streaming finaliser")
                    }
                };
                let idx = out_base + g;
                out.values[idx] = value;
                out.validity.set(idx);
            }
        }

        let step_timestamps = self.streaming_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }
}

impl<C: Operator> Operator for AggregateOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.load_param_values(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                self.errored = true;
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Ok(())) => {}
        }

        // Drain every child batch, then emit a single output batch covering
        // the full grid on EOS. Streaming kinds fold into the per-(step,
        // group) accumulator grid; breaker kinds absorb the full
        // `(step × input_series)` grid and run the per-step selection /
        // quantile globally. Both are correct under arbitrary child tile
        // ordering (step tiles × series tiles, Concurrent / Coalesce
        // reordering).
        if self.emitted {
            self.done = true;
            return Poll::Ready(None);
        }
        let breaker = self.kind.is_breaker();
        loop {
            match self.child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(err))) => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(input))) if breaker => {
                    self.saw_any_batch = true;
                    self.absorb_batch_breaker(&input);
                }
                Poll::Ready(Some(Ok(input))) => {
                    self.saw_any_batch = true;
                    self.absorb_batch_streaming(&input);
                }
            }
        }
        self.emitted = true;
        // A child that produced zero batches emits nothing — matches
        // Prometheus behaviour when a selector has no series and preserves
        // parity with the v1 engine (no empty grid trailing after an empty
        // selector).
        if !self.saw_any_batch {
            self.done = true;
            return Poll::Ready(None);
        }
        let result = if breaker {
            self.finalise_breaker()
        } else {
            self.finalise_streaming()
        };
        self.errored = result.is_err();
        Poll::Ready(Some(result))
    }
}

impl<C: Operator> Drop for AggregateOp<C> {
    fn drop(&mut self) {
        if self.param_bytes > 0 {
            self.reservation.release(self.param_bytes);
            self.param_bytes = 0;
        }
        if self.scratch_bytes > 0 {
            self.reservation.release(self.scratch_bytes);
            self.scratch_bytes = 0;
        }
        if self.breaker_grid_bytes > 0 {
            self.reservation.release(self.breaker_grid_bytes);
            self.breaker_grid_bytes = 0;
        }
    }
}

#[inline]
fn coerce_k_size(k_param: i64, input_len: usize) -> usize {
    let max_k = input_len as i64;
    let coerced = k_param.min(max_k);
    if coerced < 1 { 0 } else { coerced as usize }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
