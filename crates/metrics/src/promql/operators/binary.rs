//! `BinaryOp` implements every PromQL binary expression — arithmetic
//! (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`), comparisons (`==`, `<`, ...),
//! and the set operators (`and`, `or`, `unless`). One operator type drives
//! all of them cell-by-cell from two child streams.
//!
//! The interesting work is "which LHS series pairs with which RHS series?"
//! — PromQL's vector-matching rules (`on(...)`, `ignoring(...)`,
//! `group_left`, `group_right`) decide that from labels alone, so the
//! planner computes it once as a [`MatchTable`] (input index → paired
//! index) and hands it to the operator. The operator never recomputes the
//! match.
//!
//! Shapes:
//!
//! - Vector/vector: output schema follows the "many" side of the match
//!   ([`MatchTable::OneToOne`] / `GroupLeft` → LHS; `GroupRight` → RHS).
//!   Unmatched cells emit `validity = 0`.
//! - Vector/scalar: the scalar side is a single-series degenerate batch
//!   (one series, empty labels), buffered over the full grid and
//!   broadcast across every vector series as the vector side streams.
//!   Output schema = vector side.
//! - Scalar/scalar: both sides are degenerate; the RHS is buffered like
//!   the vector/scalar broadcast side. Single-series output.
//!
//! Comparison ops filter by default: false predicates emit `validity = 0`,
//! true predicates pass through with the **LHS value**. The `bool`
//! modifier switches to `1.0` / `0.0` output.
//!
//! Set ops (`and`, `or`, `unless`) are label-structural, vector/vector only.
//!
//! `/` and `%` follow IEEE 754 (v1's evaluator incorrectly coerced
//! division-by-zero to NaN; engine matches `promqltest` goldens).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::histogram::{CounterResetHint, FloatHistogram};
use crate::model::Labels;

use super::super::batch::{BitSet, HistogramCells, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};

mod buffers;
mod kinds;

use buffers::*;
pub use kinds::*;

// ---------------------------------------------------------------------------
// BinaryOp — the operator
// ---------------------------------------------------------------------------

/// Implements every PromQL binary expression — arithmetic, comparisons,
/// and set operators — by pulling one batch at a time from two upstream
/// children and applying a per-cell reducer.
///
/// See module docs for shape semantics, validity policy, and the `bool`
/// comparison-modifier handling.
pub struct BinaryOp<L: Operator, R: Operator> {
    lhs: L,
    rhs: R,
    kind: BinaryOpKind,
    shape: BinaryShape,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Total series count of each side, captured at construction from
    /// each child's static schema. Used to size the vector/vector buffer
    /// grid; unused for scalar-involving shapes.
    lhs_total_series: usize,
    rhs_total_series: usize,
    /// Buffered drain state for the vector/vector shape. Allocated lazily
    /// on the first `next()` call (so construction doesn't reserve memory
    /// for scalar shapes), absorbed into across child polls, and
    /// consumed-exactly-once when both children hit EOS. `None` for
    /// non-vector/vector shapes.
    vv_lhs: Option<BufferedSide>,
    vv_rhs: Option<BufferedSide>,
    vv_lhs_done: bool,
    vv_rhs_done: bool,
    vv_any_batch: bool,
    vv_emitted: bool,
    /// Buffered scalar side for scalar-involving shapes; see
    /// [`Self::drain_broadcast`].
    broadcast: Option<BufferedSide>,
    broadcast_done: bool,
    done: bool,
    errored: bool,
}

impl<L: Operator, R: Operator> BinaryOp<L, R> {
    /// Construct a vector/vector binary op.
    ///
    /// The planner passes in a pre-computed `match_table` and the
    /// `output_schema` built alongside it. The step grid is taken from
    /// the LHS child (the planner guarantees both children share the same
    /// grid; mismatch is treated as a programmer error).
    pub fn new_vector_vector(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        match_table: MatchTable,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let lhs_total_series = lhs
            .schema()
            .series
            .as_static()
            .map(|s| s.len())
            .unwrap_or(0);
        let rhs_total_series = rhs
            .schema()
            .series
            .as_static()
            .map(|s| s.len())
            .unwrap_or(0);
        let schema = OperatorSchema::new(SchemaRef::Static(output_schema.clone()), grid);
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::VectorVector {
                match_table,
                output_schema,
                partners: Arc::default(),
            },
            reservation,
            schema,
            lhs_total_series,
            rhs_total_series,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            broadcast: None,
            broadcast_done: false,
            done: false,
            errored: false,
        }
    }

    /// Attach the partner-side candidates for match keys shared by several
    /// series. Only meaningful for the vector/vector shape.
    pub fn with_partner_groups(mut self, groups: PartnerGroups) -> Self {
        if let BinaryShape::VectorVector { partners, .. } = &mut self.shape {
            *partners = Arc::new(groups);
        }
        self
    }

    /// Construct a vector/scalar binary op. The LHS is the vector side;
    /// the RHS is a scalar-producing child ([`ConstScalarOp`] or a
    /// reduction that yields a 1-series batch).
    pub fn new_vector_scalar(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let schema = lhs.schema().clone();
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::VectorScalar,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            broadcast: None,
            broadcast_done: false,
            done: false,
            errored: false,
        }
    }

    /// Construct a scalar/vector binary op. The RHS is the vector side.
    pub fn new_scalar_vector(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let schema = rhs.schema().clone();
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::ScalarVector,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            broadcast: None,
            broadcast_done: false,
            done: false,
            errored: false,
        }
    }

    /// Construct a scalar/scalar binary op. Output is a single-series
    /// batch per step range.
    pub fn new_scalar_scalar(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        let out_schema = Arc::new(SeriesSchema::new(labels, fps));
        let schema = OperatorSchema::new(SchemaRef::Static(out_schema), grid);
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::ScalarScalar,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            broadcast: None,
            broadcast_done: false,
            done: false,
            errored: false,
        }
    }

    /// Scalar-involving shapes buffer one scalar side (the *broadcast*
    /// side) and stream the other (the *driver*). Only `scalar OP vector`
    /// broadcasts from the left.
    fn broadcast_on_left(&self) -> bool {
        matches!(self.shape, BinaryShape::ScalarVector)
    }

    /// Drain the broadcast side into a one-series full-grid buffer. The
    /// driver may tile by series and emit any number of batches per step
    /// range (or none, when empty), so its batches are matched to scalar
    /// values by global step rather than paired one-to-one.
    fn drain_broadcast(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), QueryError>> {
        if self.broadcast.is_none() {
            let grid = self.schema.step_grid;
            match BufferedSide::allocate(
                &self.reservation,
                grid.step_count,
                1,
                grid_step_timestamps(grid),
            ) {
                Ok(side) => self.broadcast = Some(side),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
        let side = self.broadcast.as_mut().expect("allocated above");
        if matches!(self.shape, BinaryShape::ScalarVector) {
            drain_into(&mut self.lhs, side, cx)
        } else {
            drain_into(&mut self.rhs, side, cx)
        }
    }

    fn apply(&self, driver: StepBatch, broadcast: &BufferedSide) -> Result<StepBatch, QueryError> {
        match &self.shape {
            BinaryShape::VectorVector { .. } => Err(QueryError::Internal(
                "Binary: vector/vector routed through per-batch apply".to_string(),
            )),
            BinaryShape::VectorScalar => {
                self.apply_vs(driver, broadcast, /* scalar_on_right = */ true)
            }
            BinaryShape::ScalarVector => {
                self.apply_vs(driver, broadcast, /* scalar_on_right = */ false)
            }
            BinaryShape::ScalarScalar => self.apply_ss(driver, broadcast),
        }
    }

    /// Drain both children into the vector/vector buffer grids, returning
    /// `Poll::Ready(Ok(true))` once both sides have reached EOS. Allocates
    /// the grids on first call. Returns `Poll::Pending` when either side
    /// would block.
    fn drain_vv(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, QueryError>> {
        if self.vv_lhs.is_none() {
            let grid = self.schema.step_grid;
            let step_timestamps = grid_step_timestamps(grid);
            match BufferedSide::allocate(
                &self.reservation,
                grid.step_count,
                self.lhs_total_series,
                step_timestamps.clone(),
            ) {
                Ok(side) => self.vv_lhs = Some(side),
                Err(err) => return Poll::Ready(Err(err)),
            }
            match BufferedSide::allocate(
                &self.reservation,
                grid.step_count,
                self.rhs_total_series,
                step_timestamps,
            ) {
                Ok(side) => self.vv_rhs = Some(side),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }

        // Poll each side independently until EOS. The loop below may
        // surface Pending from either child; the caller's waker is
        // registered by the child's poll, so we'll be re-invoked.
        loop {
            let mut progressed = false;
            if !self.vv_lhs_done {
                match self.lhs.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.vv_lhs_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                    Poll::Ready(Some(Ok(batch))) => {
                        if let Some(side) = self.vv_lhs.as_mut() {
                            side.absorb(&batch);
                        }
                        self.vv_any_batch = true;
                        progressed = true;
                    }
                }
            }
            if !self.vv_rhs_done {
                match self.rhs.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.vv_rhs_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                    Poll::Ready(Some(Ok(batch))) => {
                        if let Some(side) = self.vv_rhs.as_mut() {
                            side.absorb(&batch);
                        }
                        self.vv_any_batch = true;
                        progressed = true;
                    }
                }
            }
            if self.vv_lhs_done && self.vv_rhs_done {
                return Poll::Ready(Ok(true));
            }
            if !progressed {
                return Poll::Ready(Ok(false));
            }
        }
    }

    /// Vector/vector hot loop. `match_table` selects the per-output
    /// (lhs_idx, rhs_idx) pair; unmatched rows emit `validity = 0` cells.
    ///
    /// Buffers full lhs/rhs grids before emitting. Children may tile by
    /// series (see `VectorSelectorOp` `series_chunk` default of 512) and
    /// `match_table` indices are global over the children's full series
    /// rosters — a cross-tile match (lhs in tile A, its paired rhs in
    /// tile B) requires both sides fully resolved before the hot loop.
    /// A prior per-batch path emitted one partial output per paired tile
    /// with `series_range = 0..out_series` and garbage cell values for any
    /// row that fell outside the paired tile, causing downstream consumers
    /// to see duplicate-coverage batches for the same step range.
    fn apply_vv(
        &self,
        lhs: BufferedSide,
        rhs: BufferedSide,
        match_table: &MatchTable,
        output_schema: &Arc<SeriesSchema>,
        partners: &PartnerGroups,
    ) -> Result<StepBatch, QueryError> {
        let step_count = lhs.step_count;
        let out_series_count = match_table.len();
        let cell_count = step_count * out_series_count;
        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        // For GroupRight the map index is RHS-indexed and the "many" side is
        // RHS; for OneToOne and GroupLeft it's LHS-indexed. Steps are the
        // outer loop to follow the buffers' step-major layout and so shared
        // match keys can be resolved once per step.
        let (scan_is_lhs, map) = match match_table {
            MatchTable::OneToOne(m) | MatchTable::GroupLeft(m) => (true, m),
            MatchTable::GroupRight(m) => (false, m),
            MatchTable::Set(set) => return self.apply_set(lhs, rhs, set, output_schema, out),
        };
        let any_histograms = lhs.has_histograms() || rhs.has_histograms();
        let mut out_hist: Option<HistogramCells> = None;
        let (scan, partner) = if scan_is_lhs {
            (&lhs, &rhs)
        } else {
            (&rhs, &lhs)
        };
        let shared = SharedKeys::new(
            map,
            partners,
            matches!(match_table, MatchTable::OneToOne(_)),
        );
        let mut chosen = vec![NO_SERIES; shared.groups.len()];
        let mut matched_at = vec![usize::MAX; shared.many_count];

        for step_off in 0..step_count {
            if !shared.groups.is_empty() {
                resolve_partner_groups(
                    scan,
                    partner,
                    &shared.groups,
                    scan_is_lhs,
                    step_off,
                    &mut chosen,
                )?;
            }
            for (out_row, mapped) in map.iter().enumerate() {
                // `out_row` is the scan side's global series index;
                // `map[out_row]` names the first partner series for the
                // row's match key, or None when no partner shares it.
                let partner_idx = match shared.row_group.get(out_row) {
                    Some(&g) if g != NO_SERIES => Some(chosen[g as usize])
                        .filter(|&j| j != NO_SERIES)
                        .map(|j| j as usize),
                    _ => mapped.map(|g| g as usize),
                };
                if let Some(&key) = shared.row_many.get(out_row)
                    && key != NO_SERIES
                    && partner_idx.is_some_and(|j| present(partner, step_off, j))
                    && present(scan, step_off, out_row)
                {
                    if matched_at[key as usize] == step_off {
                        return Err(QueryError::Internal(
                            "multiple matches for labels: many-to-one matching must be explicit \
                             (group_left/group_right)"
                                .to_string(),
                        ));
                    }
                    matched_at[key as usize] = step_off;
                }
                let (lhs_idx, rhs_idx) = if scan_is_lhs {
                    (Some(out_row), partner_idx)
                } else {
                    (partner_idx, Some(out_row))
                };
                let out_idx = step_off * out_series_count + out_row;
                let l_cell = lhs_idx.and_then(|idx| lhs.get(step_off, idx));
                let r_cell = rhs_idx.and_then(|idx| rhs.get(step_off, idx));

                if any_histograms {
                    let l_h = lhs_idx.and_then(|idx| lhs.get_histogram(step_off, idx));
                    let r_h = rhs_idx.and_then(|idx| rhs.get_histogram(step_off, idx));
                    if l_h.is_some() || r_h.is_some() {
                        self.write_mixed_cell(
                            Operand::of(l_cell, l_h),
                            Operand::of(r_cell, r_h),
                            &mut out,
                            &mut out_hist,
                            out_idx,
                        );
                        continue;
                    }
                }
                self.write_cell(
                    class,
                    bool_mod,
                    l_cell,
                    r_cell,
                    &mut out.values,
                    &mut out.validity,
                    out_idx,
                );
            }
        }

        let (values, validity) = out.finish();
        Ok(attach_histograms(
            StepBatch::new(
                lhs.step_timestamps.clone(),
                0..step_count,
                SchemaRef::Static(output_schema.clone()),
                0..out_series_count,
                values,
                validity,
            ),
            out_hist,
        ))
    }

    /// Set-operator loop, following Prometheus `VectorAnd` / `VectorOr` /
    /// `VectorUnless`: each step first collects the signatures present on
    /// both sides, then keeps LHS samples by signature membership and, for
    /// `or`, adds RHS samples whose signature has no LHS sample.
    fn apply_set(
        &self,
        lhs: BufferedSide,
        rhs: BufferedSide,
        set: &SetMatch,
        output_schema: &Arc<SeriesSchema>,
        mut out: OutBuffers,
    ) -> Result<StepBatch, QueryError> {
        let step_count = lhs.step_count;
        let out_series_count = set.rows.len();
        let present = |side: &BufferedSide, step: usize, idx: usize| {
            side.get(step, idx).is_some() || side.get_histogram(step, idx).is_some()
        };
        let mut lhs_present = vec![false; set.key_count];
        let mut rhs_present = vec![false; set.key_count];
        let mut out_hist: Option<HistogramCells> = None;

        for step in 0..step_count {
            lhs_present.fill(false);
            rhs_present.fill(false);
            for (i, &key) in set.lhs_keys.iter().enumerate() {
                if present(&lhs, step, i) {
                    lhs_present[key as usize] = true;
                }
            }
            for (j, &key) in set.rhs_keys.iter().enumerate() {
                if present(&rhs, step, j) {
                    rhs_present[key as usize] = true;
                }
            }
            for (row, &(l, r)) in set.rows.iter().enumerate() {
                let l = l.map(|i| i as usize).filter(|&i| present(&lhs, step, i));
                let chosen = match self.kind {
                    BinaryOpKind::And => l
                        .filter(|&i| rhs_present[set.lhs_keys[i] as usize])
                        .map(|i| (&lhs, i)),
                    BinaryOpKind::Unless => l
                        .filter(|&i| !rhs_present[set.lhs_keys[i] as usize])
                        .map(|i| (&lhs, i)),
                    BinaryOpKind::Or => l.map(|i| (&lhs, i)).or_else(|| {
                        r.map(|j| j as usize)
                            .filter(|&j| {
                                present(&rhs, step, j) && !lhs_present[set.rhs_keys[j] as usize]
                            })
                            .map(|j| (&rhs, j))
                    }),
                    _ => unreachable!("set match tables are only built for set operators"),
                };
                let Some((side, idx)) = chosen else {
                    continue;
                };
                let out_idx = step * out_series_count + row;
                if let Some(h) = side.get_histogram(step, idx) {
                    out_hist.get_or_insert_with(|| vec![None; out.values.len()])[out_idx] =
                        Some(h.clone());
                } else if let Some(v) = side.get(step, idx) {
                    out.values[out_idx] = v;
                    out.validity.set(out_idx);
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(attach_histograms(
            StepBatch::new(
                lhs.step_timestamps.clone(),
                0..step_count,
                SchemaRef::Static(output_schema.clone()),
                0..out_series_count,
                values,
                validity,
            ),
            out_hist,
        ))
    }

    /// Vector/scalar hot loop. `vec_batch` is the vector side; `scalar` is
    /// the buffered one-series scalar side, broadcast across every vector
    /// series. `scalar_on_right == true` when the op is `vector OP scalar`
    /// (preserves operand order for non-commutative ops).
    fn apply_vs(
        &self,
        vec_batch: StepBatch,
        scalar: &BufferedSide,
        scalar_on_right: bool,
    ) -> Result<StepBatch, QueryError> {
        let step_count = vec_batch.step_count();
        let series_count = vec_batch.series_count();
        let cell_count = step_count * series_count;
        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();
        let mut out_hist: Option<HistogramCells> = None;

        for step_off in 0..step_count {
            let scalar = scalar.get(vec_batch.step_range.start + step_off, 0);
            for series_off in 0..series_count {
                let v = cell_of(&vec_batch, step_off, series_off);
                let out_idx = step_off * series_count + series_off;
                if let Some(h) = vec_batch.histogram(out_idx) {
                    let (l, r) = if scalar_on_right {
                        (Some(Operand::Histogram(h)), scalar.map(Operand::Float))
                    } else {
                        (scalar.map(Operand::Float), Some(Operand::Histogram(h)))
                    };
                    self.write_mixed_cell(l, r, &mut out, &mut out_hist, out_idx);
                    continue;
                }
                let (l_cell, r_cell) = if scalar_on_right {
                    (v, scalar)
                } else {
                    (scalar, v)
                };
                self.write_cell(
                    class,
                    bool_mod,
                    l_cell,
                    r_cell,
                    &mut out.values,
                    &mut out.validity,
                    out_idx,
                );
                // A filtering comparison keeps the vector sample even when
                // the scalar is on the left (`3 < v` yields v's values).
                if !scalar_on_right
                    && matches!(class, OpClass::Cmp)
                    && !bool_mod
                    && let Some(v) = v
                {
                    out.values[out_idx] = v;
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(attach_histograms(
            StepBatch::new(
                vec_batch.step_timestamps.clone(),
                vec_batch.step_range.clone(),
                vec_batch.series.clone(),
                vec_batch.series_range.clone(),
                values,
                validity,
            ),
            out_hist,
        ))
    }

    /// Scalar/scalar hot loop. `lhs` is a streamed one-series batch, `rhs`
    /// the buffered scalar side. Produces one output series (1 cell per
    /// step).
    fn apply_ss(&self, lhs: StepBatch, rhs: &BufferedSide) -> Result<StepBatch, QueryError> {
        debug_assert_eq!(lhs.series_count(), 1);
        let step_count = lhs.step_count();
        let mut out = OutBuffers::allocate(&self.reservation, step_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        for step_off in 0..step_count {
            let l = cell_of(&lhs, step_off, 0);
            let r = rhs.get(lhs.step_range.start + step_off, 0);
            self.write_cell(
                class,
                bool_mod,
                l,
                r,
                &mut out.values,
                &mut out.validity,
                step_off,
            );
        }

        let (values, validity) = out.finish();
        // Output schema lives on the operator's `schema`; we just need a
        // SchemaRef here. Since scalar/scalar's schema is Static and
        // single-series, clone it from the operator schema.
        let series = self.schema.series.clone();
        Ok(StepBatch::new(
            lhs.step_timestamps.clone(),
            lhs.step_range.clone(),
            series,
            0..1,
            values,
            validity,
        ))
    }

    /// Write one output cell given the class and the two (optional) input
    /// cells. `None` means "input absence / unmatched" (validity = 0 on
    /// that side).
    ///
    /// The argument count is deliberate: each of these is on the per-cell
    /// hot loop and bundling them into a struct would add an indirection
    /// in the inner loop without improving readability.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn write_cell(
        &self,
        class: OpClass,
        bool_mod: bool,
        l_cell: Option<f64>,
        r_cell: Option<f64>,
        out_values: &mut [f64],
        out_validity: &mut BitSet,
        out_idx: usize,
    ) {
        match class {
            OpClass::Arith => {
                if let (Some(lv), Some(rv)) = (l_cell, r_cell) {
                    out_values[out_idx] = self.kind.apply_arith(lv, rv);
                    out_validity.set(out_idx);
                }
                // else: validity stays clear (NaN-filled); matches 3a.1
                // convention.
            }
            OpClass::Cmp => {
                if let (Some(lv), Some(rv)) = (l_cell, r_cell) {
                    let predicate = self.kind.apply_cmp(lv, rv);
                    if bool_mod {
                        // bool modifier: always emit 0/1.
                        out_values[out_idx] = if predicate { 1.0 } else { 0.0 };
                        out_validity.set(out_idx);
                    } else if predicate {
                        // Filter mode: emit LHS value.
                        out_values[out_idx] = lv;
                        out_validity.set(out_idx);
                    }
                    // false + no bool → validity clear (filtered out).
                }
            }
            OpClass::Set => {
                let l_present = l_cell.is_some();
                let r_present = r_cell.is_some();
                let (emit_v, emit_valid) = match self.kind {
                    BinaryOpKind::And => {
                        if l_present && r_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    BinaryOpKind::Or => {
                        if l_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else if r_present {
                            (r_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    BinaryOpKind::Unless => {
                        if l_present && !r_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    _ => unreachable!("OpClass::Set kinds handled above"),
                };
                if emit_valid {
                    out_values[out_idx] = emit_v;
                    out_validity.set(out_idx);
                }
            }
        }
    }
}

impl<L: Operator, R: Operator> BinaryOp<L, R> {
    /// Slow path for cells where at least one operand is a native
    /// histogram, following Prometheus `vectorElemBinop`: `h + h`, `h - h`,
    /// `h * s`, `s * h` and `h / s` produce histograms; `h == h` / `h != h`
    /// filter on exact equality; every other combination drops the sample
    /// (or yields `0` under `bool`).
    fn write_mixed_cell(
        &self,
        l: Option<Operand<'_>>,
        r: Option<Operand<'_>>,
        out: &mut OutBuffers,
        out_hist: &mut Option<HistogramCells>,
        out_idx: usize,
    ) {
        let cells = out.values.len();
        let mut emit_histogram = |h: Arc<FloatHistogram>| {
            out_hist.get_or_insert_with(|| vec![None; cells])[out_idx] = Some(h);
        };
        match self.kind.class() {
            OpClass::Arith => {
                let (Some(l), Some(r)) = (l, r) else {
                    return;
                };
                let result = match (self.kind, l, r) {
                    (BinaryOpKind::Mul, Operand::Float(f), Operand::Histogram(h))
                    | (BinaryOpKind::Mul, Operand::Histogram(h), Operand::Float(f)) => {
                        let mut h = (**h).clone();
                        h.mul(f);
                        Some(h)
                    }
                    (BinaryOpKind::Div, Operand::Histogram(h), Operand::Float(f)) => {
                        let mut h = (**h).clone();
                        h.div(f);
                        Some(h)
                    }
                    (BinaryOpKind::Add, Operand::Histogram(a), Operand::Histogram(b)) => {
                        let mut h = (**a).clone();
                        h.add(b).ok().map(|_| h)
                    }
                    (BinaryOpKind::Sub, Operand::Histogram(a), Operand::Histogram(b)) => {
                        let mut h = (**a).clone();
                        h.sub(b).ok().map(|_| {
                            h.counter_reset_hint = CounterResetHint::Gauge;
                            h
                        })
                    }
                    _ => None,
                };
                if let Some(mut h) = result {
                    h.compact();
                    emit_histogram(Arc::new(h));
                }
            }
            OpClass::Cmp => {
                let (Some(l), Some(r)) = (l, r) else {
                    return;
                };
                let keep = match (self.kind, l, r) {
                    (BinaryOpKind::Eq { .. }, Operand::Histogram(a), Operand::Histogram(b)) => {
                        a.equals(b)
                    }
                    (BinaryOpKind::Ne { .. }, Operand::Histogram(a), Operand::Histogram(b)) => {
                        !a.equals(b)
                    }
                    _ => false,
                };
                if self.kind.bool_modifier() {
                    out.values[out_idx] = if keep { 1.0 } else { 0.0 };
                    out.validity.set(out_idx);
                } else if keep && let Operand::Histogram(h) = l {
                    emit_histogram(h.clone());
                }
            }
            OpClass::Set => {
                let chosen = match self.kind {
                    BinaryOpKind::And if r.is_some() => l,
                    BinaryOpKind::Or => l.or(r),
                    BinaryOpKind::Unless if r.is_none() => l,
                    _ => None,
                };
                match chosen {
                    Some(Operand::Float(v)) => {
                        out.values[out_idx] = v;
                        out.validity.set(out_idx);
                    }
                    Some(Operand::Histogram(h)) => emit_histogram(h.clone()),
                    None => {}
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Operand<'a> {
    Float(f64),
    Histogram(&'a Arc<FloatHistogram>),
}

impl<'a> Operand<'a> {
    fn of(value: Option<f64>, histogram: Option<&'a Arc<FloatHistogram>>) -> Option<Self> {
        histogram
            .map(Operand::Histogram)
            .or(value.map(Operand::Float))
    }
}

fn attach_histograms(batch: StepBatch, histograms: Option<HistogramCells>) -> StepBatch {
    match histograms {
        Some(cells) => batch.with_histograms(cells),
        None => batch,
    }
}

fn present(side: &BufferedSide, step: usize, idx: usize) -> bool {
    side.get(step, idx).is_some() || side.get_histogram(step, idx).is_some()
}

const NO_SERIES: u32 = u32::MAX;

/// Dense per-row lookups for match keys with more than one series, built
/// once per evaluation so the step loop touches only flat vectors.
struct SharedKeys<'a> {
    /// Candidates of each partner group with several series.
    groups: Vec<&'a [u32]>,
    /// Per output row, its index into `groups`, or [`NO_SERIES`]. Empty when
    /// no group exists.
    row_group: Vec<u32>,
    /// Per output row of a one-to-one match, a dense id for match keys
    /// shared by several rows, or [`NO_SERIES`]. Empty when none is shared.
    row_many: Vec<u32>,
    many_count: usize,
}

impl<'a> SharedKeys<'a> {
    fn new(map: &[Option<u32>], partners: &'a PartnerGroups, one_to_one: bool) -> Self {
        let mut group_of = HashMap::with_capacity(partners.len());
        let mut groups = Vec::with_capacity(partners.len());
        for (&first, candidates) in partners {
            group_of.insert(first, groups.len() as u32);
            groups.push(&**candidates);
        }
        let row_group = if groups.is_empty() {
            Vec::new()
        } else {
            let lookup = |p: &Option<u32>| p.and_then(|p| group_of.get(&p).copied());
            map.iter().map(|p| lookup(p).unwrap_or(NO_SERIES)).collect()
        };

        let mut many_of = HashMap::new();
        if one_to_one {
            let mut seen = HashSet::new();
            for &p in map.iter().flatten() {
                if !seen.insert(p) {
                    let next = many_of.len() as u32;
                    many_of.entry(p).or_insert(next);
                }
            }
        }
        let row_many = if many_of.is_empty() {
            Vec::new()
        } else {
            let lookup = |p: &Option<u32>| p.and_then(|p| many_of.get(&p).copied());
            map.iter().map(|p| lookup(p).unwrap_or(NO_SERIES)).collect()
        };
        Self {
            groups,
            row_group,
            row_many,
            many_count: many_of.len(),
        }
    }
}

/// Picks, for one step, the candidate in each partner group with a sample.
/// Mirrors Prometheus, which rejects a step where the "one" side has two
/// samples for the same match group even if nothing on the other side
/// matches it, unless either side has no samples at all at that step.
fn resolve_partner_groups(
    scan: &BufferedSide,
    partner: &BufferedSide,
    groups: &[&[u32]],
    partner_is_rhs: bool,
    step: usize,
    chosen: &mut [u32],
) -> Result<(), QueryError> {
    let any_present = |side: &BufferedSide| (0..side.total_series).any(|i| present(side, step, i));
    let mut sides_present = None;
    for (slot, candidates) in chosen.iter_mut().zip(groups) {
        *slot = NO_SERIES;
        for &j in candidates.iter() {
            if !present(partner, step, j as usize) {
                continue;
            }
            if *slot != NO_SERIES
                && *sides_present.get_or_insert_with(|| any_present(scan) && any_present(partner))
            {
                let side = if partner_is_rhs { "right" } else { "left" };
                return Err(QueryError::Internal(format!(
                    "found duplicate series for the match group on the {side} hand-side \
                     of the operation;many-to-many matching not allowed: matching labels \
                     must be unique on one side"
                )));
            }
            *slot = j;
        }
    }
    Ok(())
}

/// Read a cell as `Option<f64>`: `Some(value)` iff validity bit set.
#[inline]
fn cell_of(batch: &StepBatch, step_off: usize, series_off: usize) -> Option<f64> {
    let idx = step_off * batch.series_count() + series_off;
    if batch.validity.get(idx) {
        Some(batch.values[idx])
    } else {
        None
    }
}

impl<L: Operator, R: Operator> Operator for BinaryOp<L, R> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }

        // Vector/vector is a pipeline-breaker: drain both children into
        // the full-grid buffers so cross-tile matches (lhs series in tile
        // A paired with rhs series in tile B) can be resolved against
        // a consistent snapshot.
        if matches!(self.shape, BinaryShape::VectorVector { .. }) {
            if self.vv_emitted {
                self.done = true;
                return Poll::Ready(None);
            }
            match self.drain_vv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Ok(false)) => {
                    // No progress in the last round and at least one side
                    // is still live — should only be reachable via a bug
                    // in child Pending signalling. Guard with an internal
                    // error rather than spin.
                    self.errored = true;
                    return Poll::Ready(Some(Err(QueryError::Internal(
                        "Binary: vv drain stalled with live children".to_string(),
                    ))));
                }
                Poll::Ready(Ok(true)) => {}
            }

            self.vv_emitted = true;
            if !self.vv_any_batch {
                // No input from either side — emit no batch (matches the
                // previous streaming behaviour on empty children and keeps
                // `reshape_range` from seeing a zero-valued full grid).
                self.done = true;
                self.vv_lhs = None;
                self.vv_rhs = None;
                return Poll::Ready(None);
            }

            let (match_table, output_schema, partners) = match &self.shape {
                BinaryShape::VectorVector {
                    match_table,
                    output_schema,
                    partners,
                } => (match_table.clone(), output_schema.clone(), partners.clone()),
                _ => unreachable!(),
            };
            let lhs_side = self.vv_lhs.take();
            let rhs_side = self.vv_rhs.take();
            let (lhs_side, rhs_side) = match (lhs_side, rhs_side) {
                (Some(l), Some(r)) => (l, r),
                _ => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(QueryError::Internal(
                        "Binary: vv buffers missing on drain completion".to_string(),
                    ))));
                }
            };
            return match self.apply_vv(lhs_side, rhs_side, &match_table, &output_schema, &partners)
            {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            };
        }

        if !self.broadcast_done {
            match self.drain_broadcast(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(())) => self.broadcast_done = true,
            }
        }
        let polled = if self.broadcast_on_left() {
            self.rhs.next(cx)
        } else {
            self.lhs.next(cx)
        };
        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(e))) => {
                self.errored = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.done = true;
                self.broadcast = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(driver))) => {
                let broadcast = self.broadcast.as_ref().expect("drained above");
                match self.apply(driver, broadcast) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(e) => {
                        self.errored = true;
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
        }
    }
}

/// Absorb `child` into `side` until end of stream.
fn drain_into(
    child: &mut impl Operator,
    side: &mut BufferedSide,
    cx: &mut Context<'_>,
) -> Poll<Result<(), QueryError>> {
    loop {
        match child.next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(Ok(())),
            Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
            Poll::Ready(Some(Ok(batch))) => side.absorb(&batch),
        }
    }
}

fn grid_step_timestamps(grid: StepGrid) -> Arc<[i64]> {
    (0..grid.step_count)
        .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
