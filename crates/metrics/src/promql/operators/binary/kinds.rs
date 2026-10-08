//! Binary operator kinds, plan-time match tables, broadcast shapes and the constant-scalar operator.

use super::*;

// ---------------------------------------------------------------------------
// BinaryOpKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-cell binary function plus the comparison `bool` modifier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOpKind {
    // --- arithmetic ---
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,

    // --- comparisons ---
    Eq { bool_modifier: bool },
    Ne { bool_modifier: bool },
    Gt { bool_modifier: bool },
    Lt { bool_modifier: bool },
    Gte { bool_modifier: bool },
    Lte { bool_modifier: bool },

    // --- set ops (vector/vector only) ---
    And,
    Or,
    Unless,
}

impl BinaryOpKind {
    /// Picks the hot loop (arith / cmp / set) without re-matching per cell.
    #[inline]
    pub(super) fn class(self) -> OpClass {
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow | Self::Atan2 => {
                OpClass::Arith
            }
            Self::Eq { .. }
            | Self::Ne { .. }
            | Self::Gt { .. }
            | Self::Lt { .. }
            | Self::Gte { .. }
            | Self::Lte { .. } => OpClass::Cmp,
            Self::And | Self::Or | Self::Unless => OpClass::Set,
        }
    }

    #[inline]
    pub(super) fn bool_modifier(self) -> bool {
        matches!(
            self,
            Self::Eq {
                bool_modifier: true
            } | Self::Ne {
                bool_modifier: true
            } | Self::Gt {
                bool_modifier: true
            } | Self::Lt {
                bool_modifier: true
            } | Self::Gte {
                bool_modifier: true
            } | Self::Lte {
                bool_modifier: true
            }
        )
    }

    #[inline]
    pub(super) fn apply_arith(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            // IEEE 754 — matches Prometheus (and `promqltest` goldens at
            // operators.test:108-118). See module docs re: legacy divergence.
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Pow => left.powf(right),
            Self::Atan2 => left.atan2(right),
            _ => unreachable!("apply_arith called on non-arith kind"),
        }
    }

    /// Evaluate a comparison predicate. NaN in either operand → false
    /// (IEEE 754; matches Prometheus).
    #[inline]
    pub(super) fn apply_cmp(self, left: f64, right: f64) -> bool {
        match self {
            Self::Eq { .. } => left == right,
            Self::Ne { .. } => left != right,
            Self::Gt { .. } => left > right,
            Self::Lt { .. } => left < right,
            Self::Gte { .. } => left >= right,
            Self::Lte { .. } => left <= right,
            _ => unreachable!("apply_cmp called on non-cmp kind"),
        }
    }
}

/// Binds `$op` to a closure computing `$kind.apply_arith` with the kind
/// fixed at compile time, then evaluates `$body` once per arithmetic kind
/// so its per-cell loop monomorphizes; `$fallback` for non-arith kinds.
macro_rules! with_arith {
    ($kind:expr, |$op:ident| $body:expr, else $fallback:expr) => {
        match $kind {
            BinaryOpKind::Add => {
                let $op = |l: f64, r: f64| BinaryOpKind::Add.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Sub => {
                let $op = |l: f64, r: f64| BinaryOpKind::Sub.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Mul => {
                let $op = |l: f64, r: f64| BinaryOpKind::Mul.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Div => {
                let $op = |l: f64, r: f64| BinaryOpKind::Div.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Mod => {
                let $op = |l: f64, r: f64| BinaryOpKind::Mod.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Pow => {
                let $op = |l: f64, r: f64| BinaryOpKind::Pow.apply_arith(l, r);
                $body
            }
            BinaryOpKind::Atan2 => {
                let $op = |l: f64, r: f64| BinaryOpKind::Atan2.apply_arith(l, r);
                $body
            }
            _ => $fallback,
        }
    };
}
pub(super) use with_arith;

/// Binds `$k` to a [`CmpKernel`] for comparison `$kind` with the predicate
/// fixed at compile time, filtering to the `$keep` operand without the
/// `bool` modifier, then evaluates `$body`; `$fallback` for other kinds.
macro_rules! with_cmp {
    ($kind:expr, $keep:expr, |$k:ident| $body:expr, else $fallback:expr) => {
        match $kind {
            BinaryOpKind::Eq { bool_modifier } => with_cmp!(@bind Eq, bool_modifier, $keep, $k, $body),
            BinaryOpKind::Ne { bool_modifier } => with_cmp!(@bind Ne, bool_modifier, $keep, $k, $body),
            BinaryOpKind::Gt { bool_modifier } => with_cmp!(@bind Gt, bool_modifier, $keep, $k, $body),
            BinaryOpKind::Lt { bool_modifier } => with_cmp!(@bind Lt, bool_modifier, $keep, $k, $body),
            BinaryOpKind::Gte { bool_modifier } => {
                with_cmp!(@bind Gte, bool_modifier, $keep, $k, $body)
            }
            BinaryOpKind::Lte { bool_modifier } => {
                with_cmp!(@bind Lte, bool_modifier, $keep, $k, $body)
            }
            _ => $fallback,
        }
    };
    (@bind $variant:ident, $bool_modifier:ident, $keep:expr, $k:ident, $body:expr) => {{
        let $k = CmpKernel::new(
            |l: f64, r: f64| {
                BinaryOpKind::$variant {
                    bool_modifier: false,
                }
                .apply_cmp(l, r)
            },
            $bool_modifier,
            $keep,
        );
        $body
    }};
}
pub(super) use with_cmp;

/// One elementwise pass over a chunk of at most 64 cells: writes `dst[i]`
/// from the `i`th operands and returns the mask of cells the op keeps
/// given both operands are present. Bits past `dst.len()` are unspecified.
pub(super) trait ChunkKernel: Copy {
    fn run(
        self,
        dst: &mut [f64],
        l: impl Iterator<Item = f64>,
        r: impl Iterator<Item = f64>,
    ) -> u64;
}

#[derive(Clone, Copy)]
pub(super) struct ArithKernel<F>(pub(super) F);

impl<F: Fn(f64, f64) -> f64 + Copy> ChunkKernel for ArithKernel<F> {
    #[inline(always)]
    fn run(
        self,
        dst: &mut [f64],
        l: impl Iterator<Item = f64>,
        r: impl Iterator<Item = f64>,
    ) -> u64 {
        for ((d, l), r) in dst.iter_mut().zip(l).zip(r) {
            *d = (self.0)(l, r);
        }
        u64::MAX
    }
}

/// Which value a comparison emits for a cell whose operands are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CmpOutput {
    /// `bool` modifier: `1.0` / `0.0`, nothing is filtered.
    Bool,
    /// Filter, keeping the left operand where the predicate holds.
    KeepLeft,
    /// Filter, keeping the right operand (the vector of `scalar OP vector`).
    KeepRight,
}

#[derive(Clone, Copy)]
pub(super) struct CmpKernel<P> {
    pred: P,
    output: CmpOutput,
}

impl<P: Fn(f64, f64) -> bool + Copy> CmpKernel<P> {
    #[inline(always)]
    pub(super) fn new(pred: P, bool_modifier: bool, keep: CmpOutput) -> Self {
        let output = if bool_modifier { CmpOutput::Bool } else { keep };
        Self { pred, output }
    }
}

impl<P: Fn(f64, f64) -> bool + Copy> ChunkKernel for CmpKernel<P> {
    #[inline(always)]
    fn run(
        self,
        dst: &mut [f64],
        l: impl Iterator<Item = f64>,
        r: impl Iterator<Item = f64>,
    ) -> u64 {
        let pred = self.pred;
        let cells = dst.iter_mut().zip(l.zip(r));
        let mut keep = 0u64;
        match self.output {
            CmpOutput::Bool => {
                for (d, (l, r)) in cells {
                    *d = f64::from(u8::from(pred(l, r)));
                }
                return u64::MAX;
            }
            CmpOutput::KeepLeft => {
                for (i, (d, (l, r))) in cells.enumerate() {
                    let p = pred(l, r);
                    *d = if p { l } else { f64::NAN };
                    keep |= u64::from(p) << i;
                }
            }
            CmpOutput::KeepRight => {
                for (i, (d, (l, r))) in cells.enumerate() {
                    let p = pred(l, r);
                    *d = if p { r } else { f64::NAN };
                    keep |= u64::from(p) << i;
                }
            }
        }
        keep
    }
}

/// Mask of the low `n` bits, `n <= 64`.
#[inline(always)]
pub(super) fn lane_mask(n: usize) -> u64 {
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpClass {
    Arith,
    Cmp,
    Set,
}

// ---------------------------------------------------------------------------
// MatchTable — plan-time series matching
// ---------------------------------------------------------------------------

/// Pre-computed series matching between LHS and RHS vectors.
///
/// Built by the planner from the input series schemas and the
/// PromQL `on` / `ignoring` / `group_left` / `group_right` modifiers. The
/// operator treats this as an opaque mapping — it does not recompute
/// matching at runtime.
///
/// # Variants
///
/// - [`MatchTable::OneToOne`] — the default vector-vector case. `map[i]`
///   is the RHS series index paired with LHS series `i`, or `None` when
///   there is no match. Output schema follows the LHS.
/// - [`MatchTable::GroupLeft`] — the LHS is the "many" side. `map[i]` is
///   the *single* RHS series index matched to LHS series `i` (the "one"
///   side is unique per group, per PromQL's matching rules). Output
///   schema follows the LHS. The planner is responsible for honouring the
///   `group_left(<labels>)` label-copy semantics when it *builds the
///   output schema* — this operator does not touch labels.
/// - [`MatchTable::GroupRight`] — the RHS is the "many" side. `map[j]` is
///   the single LHS series index matched to RHS series `j`. Output
///   schema follows the RHS.
///
/// # What the planner guarantees
///
/// - Indices in `map` are `< child.schema().series.len()` for the
///   corresponding side.
/// - The output schema (passed to [`BinaryOp::new_vector_vector`]) has
///   length equal to the "many" side and is parallel-indexed with `map`
///   (so `map[i]` is the RHS index for the `i`th output series when
///   `OneToOne` / `GroupLeft`, and the LHS index for the `i`th output
///   series when `GroupRight`).
#[derive(Debug, Clone)]
pub enum MatchTable {
    /// One-to-one matching. `map[i]` is the RHS series index for LHS `i`,
    /// or `None`. Output schema = LHS schema.
    OneToOne(Vec<Option<u32>>),
    /// `group_left`: LHS is "many". `map[i]` is the RHS series index for
    /// LHS `i`, or `None`. Output schema = LHS schema.
    GroupLeft(Vec<Option<u32>>),
    /// `group_right`: RHS is "many". `map[j]` is the LHS series index for
    /// RHS `j`, or `None`. Output schema = RHS schema.
    GroupRight(Vec<Option<u32>>),
    /// `and` / `or` / `unless`: membership is decided per step from the
    /// matching signatures present on each side, so it can't be reduced to
    /// a static pairing.
    Set(Arc<SetMatch>),
}

/// Partner-side series that share a matching key, keyed by the first of
/// them (the index the [`MatchTable`] map points at). Prometheus matches
/// each step's samples, so the partner is whichever candidate has a sample
/// at that step, and two present at once is a matching error. Keys with a
/// single candidate are absent.
pub type PartnerGroups = HashMap<u32, Box<[u32]>>;

/// Per-step set-operator matching. `lhs_keys[i]` / `rhs_keys[j]` are dense
/// signature ids (`< key_count`). Each output row names the LHS and/or RHS
/// series that feed it; `or` rows come from both sides, merged when the
/// labelsets are identical.
#[derive(Debug)]
pub struct SetMatch {
    pub lhs_keys: Vec<u32>,
    pub rhs_keys: Vec<u32>,
    pub key_count: usize,
    pub rows: Vec<(Option<u32>, Option<u32>)>,
}

impl MatchTable {
    /// Number of rows in the table — matches the output series count.
    pub fn len(&self) -> usize {
        match self {
            Self::OneToOne(m) | Self::GroupLeft(m) | Self::GroupRight(m) => m.len(),
            Self::Set(set) => set.rows.len(),
        }
    }

    /// `true` when the table has zero entries (empty output).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// BinaryShape — tags the runtime axis of broadcasting
// ---------------------------------------------------------------------------

/// Shape of the binary operation. Determined at plan time from the
/// children's types.
///
/// Vector/scalar and scalar/vector are distinct so the planner can
/// preserve operand order (non-commutative ops — `Sub`, `Div`, `Pow`,
/// `Atan2`, asymmetric comparisons — care).
#[derive(Debug, Clone)]
pub enum BinaryShape {
    /// Vector op Vector with a pre-built match table and output schema.
    VectorVector {
        match_table: MatchTable,
        /// Output series schema — built by the planner to match the
        /// table's output side (LHS for `OneToOne`/`GroupLeft`, RHS for
        /// `GroupRight`).
        output_schema: Arc<SeriesSchema>,
        partners: Arc<PartnerGroups>,
    },
    /// Vector op Scalar (scalar broadcast on the RHS). Output schema =
    /// LHS vector schema.
    VectorScalar,
    /// Scalar op Vector (scalar broadcast on the LHS). Output schema =
    /// RHS vector schema.
    ScalarVector,
    /// Scalar op Scalar. Output schema = a single-series `Static` schema
    /// built at construction time.
    ScalarScalar,
}

// ---------------------------------------------------------------------------
// ConstScalarOp — scalar-as-operator helper
// ---------------------------------------------------------------------------

/// Wraps a plan-time scalar literal (like `42` in `x + 42`) as an
/// [`Operator`] so [`BinaryOp`] can treat scalar and vector children
/// uniformly — a scalar is simply "a child that produces 1-series
/// batches." Emits one [`StepBatch`] covering the full step grid with the
/// constant replicated, then end-of-stream.
pub struct ConstScalarOp {
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    value: f64,
    reservation: MemoryReservation,
    yielded: bool,
}

impl ConstScalarOp {
    /// Construct a scalar operator producing `value` for every step in
    /// `grid`.
    pub fn new(value: f64, grid: StepGrid, reservation: MemoryReservation) -> Self {
        let step_count = grid.step_count;
        let step_timestamps: Arc<[i64]> = if step_count == 0 {
            Arc::from(Vec::<i64>::new())
        } else {
            let mut v = Vec::with_capacity(step_count);
            for i in 0..step_count {
                v.push(grid.start_ms + (i as i64) * grid.step_ms);
            }
            Arc::from(v)
        };
        // Scalar output is a single, unnamed series.
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        let schema_series = Arc::new(SeriesSchema::new(labels, fps));
        let schema = OperatorSchema::new(SchemaRef::Static(schema_series), grid);
        Self {
            schema,
            step_timestamps,
            value,
            reservation,
            yielded: false,
        }
    }
}

impl Operator for ConstScalarOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.yielded {
            return Poll::Ready(None);
        }
        let step_count = self.schema.step_grid.step_count;
        if step_count == 0 {
            self.yielded = true;
            return Poll::Ready(None);
        }
        // Allocate values column + validity bitset through the reservation.
        let bytes = out_bytes(step_count);
        if let Err(err) = self.reservation.try_grow(bytes) {
            self.yielded = true;
            return Poll::Ready(Some(Err(err)));
        }
        let values = vec![self.value; step_count];
        let validity = BitSet::all_set(step_count);
        self.reservation.release(bytes);
        self.yielded = true;
        Poll::Ready(Some(Ok(StepBatch::new(
            self.step_timestamps.clone(),
            0..step_count,
            self.schema.series.clone(),
            0..1,
            values,
            validity,
        ))))
    }
}
