//! The physical planner — the compilation step that turns a
//! [`LogicalPlan`] into a ready-to-poll tree of `Box<dyn Operator>`,
//! alongside all the plan-time artifacts operators need at construction
//! (resolved series rosters, group maps, match tables).
//!
//! Leaves call [`SeriesSource::resolve`] to materialise the series list
//! a selector spans. `Subquery` owns a child-factory closure that
//! re-plans its inner subtree once per outer step (the inner window
//! slides, so the child has to be rebuilt).
//!
//! This pass does not mutate the logical plan. Exchange-operator
//! insertion policy (when to wrap a subtree in `ConcurrentOp`) lives in
//! [`super::parallelism`].
//!
//! [`CountValuesOp`] publishes [`SchemaRef::Deferred`] because its
//! output labels depend on sample values. The planner rejects
//! compositions that sink a `Deferred` child under a parent that needs a
//! `Static` schema at plan time (aggregate, binary, rollup, instant-fn,
//! ...) with [`PlanError::InvalidMatching`]. Only root-positioned
//! `CountValues` is supported in v1.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use promql_parser::parser;

use crate::model::{Label, Labels};
use crate::util::Fingerprint;

use super::super::batch::{SchemaRef, SeriesSchema};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::operators::absent::AbsentOp;
use super::super::operators::aggregate::{AggregateKind, AggregateOp, GroupMap};
use super::super::operators::binary::{BinaryOp, BinaryOpKind, ConstScalarOp, MatchTable};
use super::super::operators::coercion::{ScalarizeOp, TimeScalarOp};
use super::super::operators::concurrent::ConcurrentOp;
use super::super::operators::count_values::CountValuesOp;
use super::super::operators::histogram::{BucketSeries, HistogramOp};
use super::super::operators::instant_fn::InstantFnOp;
use super::super::operators::label_manip::{LabelManipKind, LabelManipOp};
use super::super::operators::matrix_selector::MatrixSelectorOp;
use super::super::operators::rollup::{MatrixWindowSource, RollupOp};
use super::super::operators::subquery::{ChildFactory, SubqueryOp};
use super::super::operators::vector_selector::VectorSelectorOp;
use super::super::source::{ResolvedSeriesChunk, ResolvedSeriesRef, SeriesSource, TimeRange};

use super::super::trace;
use super::error::PlanError;
use super::lowering::LoweringContext;
use super::parallelism::ExchangeStats;
use super::plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, LogicalPlan, MatchingAxis, Offset,
};

mod matching;
mod subquery;

use matching::*;
use subquery::*;

/// Wrap `op` in a [`trace::TracingOperator`] tagged with `name` when the
/// context carries a trace collector; otherwise return it unchanged.
#[inline]
fn wrap_op(
    op: Box<dyn Operator + Send>,
    name: &'static str,
    ctx: &LoweringContext,
) -> Box<dyn Operator + Send> {
    trace::maybe_trace(op, name, ctx.trace.as_ref())
}

/// Convert our plan-time [`Offset`] into the parser's
/// `promql_parser::parser::Offset` the leaf operators consume.
fn to_parser_offset(o: Offset) -> parser::Offset {
    match o {
        Offset::Pos(ms) => parser::Offset::Pos(std::time::Duration::from_millis(ms as u64)),
        Offset::Neg(ms) => parser::Offset::Neg(std::time::Duration::from_millis(ms as u64)),
    }
}

/// Convert our plan-time [`AtModifier`] into the parser's `AtModifier`.
fn to_parser_at(a: AtModifier) -> parser::AtModifier {
    match a {
        AtModifier::Start => parser::AtModifier::Start,
        AtModifier::End => parser::AtModifier::End,
        AtModifier::Value(ms) => parser::AtModifier::At(
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms.max(0) as u64),
        ),
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// The output of the planning phase — a ready-to-poll operator tree
/// plus the metadata the executor and the reshape pass need to drive it
/// without re-walking the tree.
///
/// Alongside the root `dyn Operator` this carries:
/// - the output [`SchemaRef`] (what the root will publish);
/// - the [`StepGrid`] every batch lands on;
/// - two flags captured at logical-plan time: `root_is_scalar` (so
///   reshape surfaces a `Scalar` instead of a single-series vector) and
///   `root_instant_vector_sort` (so `topk` / `bottomk` roots come back
///   in the order Prometheus clients expect).
pub struct PhysicalPlan {
    /// Root operator — already wired end-to-end.
    pub root: Box<dyn Operator + Send>,
    /// Schema the root will publish. Mirrors `root.schema().series`.
    pub output_schema: SchemaRef,
    /// Step grid the root emits on.
    pub step_grid: StepGrid,
    /// `true` when the logical-plan root is scalar-typed. Needed because
    /// `vector(scalar)` and scalar roots share the same one-series anonymous
    /// runtime schema but reshape must preserve the top-level query type.
    pub root_is_scalar: bool,
    /// Optional final instant-vector ordering required by the root plan.
    /// Used for ordered `topk` / `bottomk` outputs.
    pub root_instant_vector_sort: Option<InstantVectorSort>,
}

impl std::fmt::Debug for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalPlan")
            .field("output_schema", &self.output_schema)
            .field("step_grid", &self.step_grid)
            .field("root_is_scalar", &self.root_is_scalar)
            .field("root_instant_vector_sort", &self.root_instant_vector_sort)
            .finish_non_exhaustive()
    }
}

pub use super::plan_types::InstantVectorSort;

/// Build a physical plan from an (already-lowered and optionally optimised)
/// [`LogicalPlan`].
///
/// `source` must live for `'static` — the generated operator tree captures
/// `Arc<S>` clones and is returned as `Box<dyn Operator + Send>`. `ctx`
/// carries the query's step grid and lookback (same shape the lowering
/// pass consumed).
pub async fn build_physical_plan<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: MemoryReservation,
    ctx: &LoweringContext,
) -> Result<PhysicalPlan, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let (physical, _stats) = build_physical_plan_with_stats(plan, source, reservation, ctx).await?;
    Ok(physical)
}

/// Variant of [`build_physical_plan`] that also reports the exchange-operator
/// insertion statistics accumulated during the walk.
///
/// The stats are used by unit tests to verify `ConcurrentOp` insertion
/// decisions without downcasting the `dyn Operator` tree. Production
/// callers should prefer [`build_physical_plan`] and discard the stats.
pub async fn build_physical_plan_with_stats<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: MemoryReservation,
    ctx: &LoweringContext,
) -> Result<(PhysicalPlan, ExchangeStats), PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let grid = step_grid_from_ctx(ctx);
    let root_is_scalar = plan.produces_scalar();
    let root_instant_vector_sort = root_instant_vector_sort(&plan);

    let mut stats = ExchangeStats::default();
    let root = build_node(
        plan,
        BuildEnv {
            source,
            reservation: &reservation,
            ctx,
        },
        grid,
        /*under_rollup=*/ false,
        &mut stats,
    )
    .await?;
    let output_schema = root.schema().series.clone();
    let step_grid = root.schema().step_grid;
    Ok((
        PhysicalPlan {
            root,
            output_schema,
            step_grid,
            root_is_scalar,
            root_instant_vector_sort,
        },
        stats,
    ))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn step_grid_from_ctx(ctx: &LoweringContext) -> StepGrid {
    let step_count = if ctx.is_instant() {
        1
    } else {
        // ctx.step_ms > 0 for range queries (lowering sets a `1` sentinel
        // for instant queries, handled above).
        let span = (ctx.end_ms - ctx.start_ms).max(0);
        (span / ctx.step_ms.max(1)) as usize + 1
    };
    StepGrid {
        start_ms: ctx.start_ms,
        end_ms: ctx.end_ms,
        step_ms: ctx.step_ms.max(1),
        step_count,
    }
}

fn root_instant_vector_sort(plan: &LogicalPlan) -> Option<InstantVectorSort> {
    match plan {
        LogicalPlan::Aggregate {
            kind: AggregateKind::Topk(_),
            ..
        } => Some(InstantVectorSort::DescendingValue),
        LogicalPlan::Aggregate {
            kind: AggregateKind::Bottomk(_),
            ..
        } => Some(InstantVectorSort::AscendingValue),
        LogicalPlan::Sort { order, .. } => Some(*order),
        _ => None,
    }
}

fn static_schema(schema: &SchemaRef) -> Result<&Arc<SeriesSchema>, PlanError> {
    match schema {
        SchemaRef::Static(s) => Ok(s),
        SchemaRef::Deferred => Err(PlanError::InvalidMatching(
            "deferred-schema child (count_values) not supported under a schema-sensitive parent \
             in v1"
                .to_string(),
        )),
    }
}

fn map_source_err(err: QueryError) -> PlanError {
    match err {
        QueryError::MemoryLimit { .. } => PlanError::MemoryLimit(err.to_string()),
        other => PlanError::SourceError(other.to_string()),
    }
}

fn map_construct_err(err: QueryError) -> PlanError {
    match err {
        QueryError::MemoryLimit { .. } => PlanError::MemoryLimit(err.to_string()),
        other => PlanError::PhysicalPlanFailed(other.to_string()),
    }
}

/// Compute an outer time-range that bounds the query window. For leaf
/// selectors we fold in the effective lookback (vector) or range (matrix)
/// plus the `@`/offset modifiers so the source returns every sample an
/// operator could consume. Conservative is fine — the source does not
/// widen, and any extra samples land inside the request's window.
fn selector_time_range(
    grid: StepGrid,
    lookback_ms: i64,
    range_ms: i64,
    at: Option<AtModifier>,
    offset: Offset,
) -> TimeRange {
    // Apply `@` pin if present: the window collapses to a single pin point
    // ± modifiers. Otherwise the window tracks the whole grid.
    let (pin_start, pin_end) = match at {
        Some(AtModifier::Value(v)) => (v, v),
        Some(AtModifier::Start) => (grid.start_ms, grid.start_ms),
        Some(AtModifier::End) => (grid.end_ms, grid.end_ms),
        None => (grid.start_ms, grid.end_ms),
    };
    // `Offset::Pos(d)` subtracts `d` from the effective time (looks
    // backward); `Neg(d)` adds `d` (looks forward). The operator path applies
    // the same sign convention (§5 decisions 3a.1). The source's window needs
    // to cover (effective - window, effective], so we shift both ends by the
    // offset in the same direction.
    let signed = offset.signed_ms();
    let effective_start = pin_start.saturating_sub(signed);
    let effective_end = pin_end.saturating_sub(signed);
    // Window size we need on the source. For vector selectors this is the
    // lookback delta; for matrix selectors it is the bracketed range. Both
    // extend into the past from each step.
    let window_ms = lookback_ms.max(range_ms).max(0);
    let start = effective_start.saturating_sub(window_ms);
    // Inclusive-to-exclusive: the operator uses `(effective - window,
    // effective]`. For the source we ask for `[start, effective_end + 1)`.
    let end_exclusive = effective_end.saturating_add(1);
    TimeRange::new(start, end_exclusive.max(start))
}

// ---------------------------------------------------------------------------
// Resolved-series materialisation
// ---------------------------------------------------------------------------

struct ResolvedLeaf {
    schema: Arc<SeriesSchema>,
    request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
}

/// Drain [`SeriesSource::resolve`] into a unique-per-labelset series roster
/// (`labels` + stable fingerprint) plus the per-logical-series grouped source
/// handles needed to load samples across buckets.
///
/// `SeriesSource::resolve` is bucket-scoped by design and may therefore emit
/// one `(labels, ResolvedSeriesRef)` pair per bucket for the same logical
/// series. The execution RFC's core data model expects the planner to collapse
/// these onto one schema row per logical series while preserving every bucket
/// handle for the leaf operator's raw-sample fetch path.
async fn resolve_leaf<S>(
    source: &Arc<S>,
    selector: &parser::VectorSelector,
    time_range: TimeRange,
    reservation: &MemoryReservation,
) -> Result<ResolvedLeaf, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let mut stream: BoxStream<'_, Result<ResolvedSeriesChunk, QueryError>> =
        Box::pin(source.resolve(selector, time_range));

    let mut roster_index: HashMap<Labels, usize> = HashMap::new();
    let mut labels: Vec<Labels> = Vec::new();
    let mut fingerprints: Vec<u128> = Vec::new();
    let mut refs: Vec<Vec<ResolvedSeriesRef>> = Vec::new();

    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res.map_err(map_source_err)?;
        debug_assert_eq!(chunk.labels.len(), chunk.series.len());
        for (label, sref) in chunk.labels.iter().zip(chunk.series.iter()) {
            let canonical = canonicalize_labels(label);
            match roster_index.get(&canonical).copied() {
                Some(idx) => refs[idx].push(sref.clone()),
                None => {
                    roster_index.insert(canonical.clone(), labels.len());
                    fingerprints.push(labels_fingerprint(&canonical));
                    labels.push(canonical);
                    refs.push(vec![sref.clone()]);
                }
            }
        }
    }

    for series_refs in &mut refs {
        series_refs.sort_unstable_by_key(|sref| (sref.bucket_id, sref.series_id));
    }

    // Conservative label-storage reservation: one `Label` struct per label,
    // rounded to 48 bytes (the stable shape today) + per-series overhead.
    let bytes = labels
        .iter()
        .map(|l| l.len().saturating_mul(48))
        .fold(0usize, |a, b| a.saturating_add(b))
        .saturating_add(labels.len().saturating_mul(32));
    reservation.try_grow(bytes).map_err(map_construct_err)?;

    let schema = Arc::new(SeriesSchema::new(
        Arc::from(labels),
        Arc::from(fingerprints),
    ));
    let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(
        refs.into_iter()
            .map(Arc::<[ResolvedSeriesRef]>::from)
            .collect::<Vec<_>>(),
    );
    Ok(ResolvedLeaf {
        schema,
        request_series,
    })
}

#[inline]
fn canonicalize_labels(labels: &Labels) -> Labels {
    let mut canonical: Vec<Label> = labels.iter().cloned().collect();
    canonical.sort();
    Labels::new(canonical)
}

#[inline]
fn labels_fingerprint(labels: &Labels) -> u128 {
    let canonical: Vec<Label> = labels.iter().cloned().collect();
    canonical.fingerprint()
}

// ---------------------------------------------------------------------------
// Plan-tree walk (bottom-up)
// ---------------------------------------------------------------------------

/// Planning inputs shared, unchanged, by every node of one physical plan.
struct BuildEnv<'a, S> {
    source: &'a Arc<S>,
    reservation: &'a MemoryReservation,
    ctx: &'a LoweringContext,
}

impl<S> Clone for BuildEnv<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for BuildEnv<'_, S> {}

fn build_node<'a, S>(
    plan: LogicalPlan,
    env: BuildEnv<'a, S>,
    grid: StepGrid,
    under_rollup: bool,
    stats: &'a mut ExchangeStats,
) -> futures::future::BoxFuture<'a, Result<Box<dyn Operator + Send>, PlanError>>
where
    S: SeriesSource + Send + Sync + 'static,
{
    Box::pin(async move { build_node_inner(plan, env, grid, under_rollup, stats).await })
}

async fn build_node_inner<S>(
    plan: LogicalPlan,
    env: BuildEnv<'_, S>,
    grid: StepGrid,
    under_rollup: bool,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let BuildEnv {
        source,
        reservation,
        ctx,
    } = env;
    match plan {
        LogicalPlan::Scalar(v) => {
            let op = ConstScalarOp::new(v, grid, reservation.clone());
            Ok(wrap_op(Box::new(op), "ConstScalar", ctx))
        }
        LogicalPlan::Time => {
            let op = TimeScalarOp::new(grid, reservation.clone());
            Ok(wrap_op(Box::new(op), "TimeScalar", ctx))
        }
        LogicalPlan::Scalarize { child } => {
            let child_op = build_node(*child, env, grid, false, stats).await?;
            let _ = static_schema(&child_op.schema().series)?;
            let op = ScalarizeOp::new(BoxedOp(child_op), reservation.clone());
            Ok(wrap_op(Box::new(op), "Scalarize", ctx))
        }
        LogicalPlan::Vectorize { child } => build_node(*child, env, grid, false, stats).await,
        LogicalPlan::VectorSelector {
            selector,
            offset,
            at,
            lookback_ms,
        } => {
            let lookback = lookback_ms.unwrap_or(ctx.lookback_delta_ms);
            let time_range = selector_time_range(grid, lookback, 0, at, offset);
            let resolved = resolve_leaf(source, &selector, time_range, reservation).await?;
            let series_count = resolved.schema.len() as u64;
            let at_parser = at.map(to_parser_at);
            let off_parser = to_parser_offset(offset);
            let op = VectorSelectorOp::<'static, S>::new(
                source.clone(),
                resolved.schema,
                resolved.request_series,
                grid,
                at_parser,
                Some(off_parser),
                lookback,
                reservation.clone(),
                super::super::operators::vector_selector::BatchShape::default(),
            );
            // Unit 4.5: wrap the leaf in `ConcurrentOp` when its resolved
            // series count exceeds the configured threshold. Decouples the
            // async `samples()` pull from downstream evaluation.
            Ok(maybe_wrap_concurrent(
                Box::new(op),
                "VectorSelector",
                series_count,
                ctx,
                stats,
            ))
        }
        LogicalPlan::MatrixSelector { .. } => {
            // A bare matrix selector never lives at the output of the plan;
            // it must be wrapped in a `Rollup` or `Subquery`. The Rollup
            // branch (below) matches and handles this directly.
            if !under_rollup {
                return Err(PlanError::UnsupportedExpression(
                    "MatrixSelector without a parent Rollup / Subquery".to_string(),
                ));
            }
            unreachable!("MatrixSelector handled by Rollup branch")
        }
        LogicalPlan::InstantFn { kind, child } => {
            let child_op = build_node(*child, env, grid, false, stats).await?;
            let _ = static_schema(&child_op.schema().series)?;
            let op = InstantFnOp::new(BoxedOp(child_op), kind, reservation.clone());
            Ok(wrap_op(Box::new(op), "InstantFn", ctx))
        }
        LogicalPlan::LabelManip { kind, child } => {
            let child_op = build_node(*child, env, grid, false, stats).await?;
            let input_schema = static_schema(&child_op.schema().series)?.clone();
            let built = build_label_manip(&kind, &input_schema)?;
            let op = LabelManipOp::new(
                BoxedOp(child_op),
                built.input_to_output,
                built.output_schema,
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op), "LabelManip", ctx))
        }
        LogicalPlan::Rollup { kind, child } => {
            // Rollup wraps either a MatrixSelector (directly) or a Subquery.
            match *child {
                LogicalPlan::MatrixSelector {
                    selector,
                    range_ms,
                    offset,
                    at,
                } => {
                    let time_range = selector_time_range(grid, 0, range_ms, at, offset);
                    let resolved = resolve_leaf(source, &selector, time_range, reservation).await?;
                    let series_count = resolved.schema.len() as u64;
                    let at_parser = at.map(to_parser_at);
                    let off_parser = to_parser_offset(offset);
                    let matrix = MatrixSelectorOp::<'static, S>::new(
                        source.clone(),
                        resolved.schema.clone(),
                        resolved.request_series,
                        grid,
                        at_parser,
                        Some(off_parser),
                        range_ms,
                        reservation.clone(),
                        super::super::operators::matrix_selector::BatchShape::default(),
                    );
                    let schema_snapshot =
                        OperatorSchema::new(SchemaRef::Static(resolved.schema), grid);
                    let window = MatrixWindowSource::new(matrix, schema_snapshot);
                    let op = RollupOp::new(window, kind, range_ms, reservation.clone());
                    // Unit 4.5: `MatrixSelectorOp::next` is degenerate (see
                    // §3a.2), so we wrap the enclosing `RollupOp` — which
                    // owns the I/O leaf — instead of the matrix selector
                    // itself. This preserves the "decouple I/O from
                    // evaluation" contract at the right boundary.
                    Ok(maybe_wrap_concurrent(
                        Box::new(op),
                        "Rollup",
                        series_count,
                        ctx,
                        stats,
                    ))
                }
                LogicalPlan::Subquery {
                    child: inner,
                    range_ms,
                    step_ms,
                    offset,
                    at,
                } => {
                    let sub = build_subquery(
                        *inner,
                        env,
                        grid,
                        SubqueryWindow {
                            range_ms,
                            step_ms,
                            offset,
                            at,
                        },
                        stats,
                    )
                    .await?;
                    let op = RollupOp::new(sub, kind, range_ms, reservation.clone());
                    Ok(wrap_op(Box::new(op), "Rollup", ctx))
                }
                other => Err(PlanError::UnsupportedExpression(format!(
                    "Rollup child must be MatrixSelector or Subquery, got {other:?}"
                ))),
            }
        }
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => build_binary(op, *lhs, *rhs, matching, env, grid, stats).await,
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            build_aggregate(
                kind,
                *child,
                param.map(|param| *param),
                grouping,
                env,
                grid,
                stats,
            )
            .await
        }
        LogicalPlan::Sort { child, .. } => build_node(*child, env, grid, false, stats).await,
        LogicalPlan::Histogram { kind, child } => {
            let child_op = build_node(*child, env, grid, false, stats).await?;
            let input_schema = static_schema(&child_op.schema().series)?.clone();
            let built = build_histogram_groups(&input_schema);
            let op = HistogramOp::new(
                BoxedOp(child_op),
                kind,
                &built.inputs,
                build_group_schema(&built.group_labels),
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op), "Histogram", ctx))
        }
        LogicalPlan::Absent { labels, child } => {
            let child_op = build_node(*child, env, grid, false, stats).await?;
            let op = AbsentOp::new(BoxedOp(child_op), labels, reservation.clone());
            Ok(wrap_op(Box::new(op), "Absent", ctx))
        }
        LogicalPlan::Subquery { .. } => Err(PlanError::UnsupportedExpression(
            "bare Subquery without a Rollup parent is not supported in v1".to_string(),
        )),
        LogicalPlan::Rechunk { .. } => Err(PlanError::UnsupportedExpression(
            "Rechunk must be inserted by parallelism planning, not AST lowering".to_string(),
        )),
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => build_count_values(label, *child, grouping, env, grid, stats).await,
        LogicalPlan::Concurrent { .. } | LogicalPlan::Coalesce { .. } => {
            Err(PlanError::UnsupportedExpression(
                "Concurrent / Coalesce must be inserted by parallelism planning, not AST lowering"
                    .to_string(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------

async fn build_binary<S>(
    op: BinaryOpKind,
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    matching: Option<BinaryMatching>,
    env: BuildEnv<'_, S>,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let BuildEnv {
        reservation, ctx, ..
    } = env;
    let lhs_is_scalar = lhs.produces_scalar();
    let rhs_is_scalar = rhs.produces_scalar();

    let lhs_op = build_node(lhs, env, grid, false, stats).await?;
    let rhs_op = build_node(rhs, env, grid, false, stats).await?;

    match (lhs_is_scalar, rhs_is_scalar) {
        (true, true) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_scalar_scalar(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op_box), "Binary", ctx))
        }
        (true, false) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_scalar_vector(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op_box), "Binary", ctx))
        }
        (false, true) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_vector_scalar(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op_box), "Binary", ctx))
        }
        (false, false) => {
            let lhs_schema = static_schema(&lhs_op.schema().series)?.clone();
            let rhs_schema = static_schema(&rhs_op.schema().series)?.clone();
            let include_name = preserves_metric_name(op);
            let built =
                build_match_table(&lhs_schema, &rhs_schema, matching.as_ref(), include_name)?;
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_vector_vector(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                built.table,
                built.output_schema,
                reservation.clone(),
            );
            Ok(wrap_op(Box::new(op_box), "Binary", ctx))
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate
// ---------------------------------------------------------------------------

async fn build_aggregate<S>(
    kind: AggregateKind,
    child: LogicalPlan,
    param: Option<LogicalPlan>,
    grouping: AggregateGrouping,
    env: BuildEnv<'_, S>,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let BuildEnv {
        reservation, ctx, ..
    } = env;
    let child_op = build_node(child, env, grid, false, stats).await?;
    let param_op = if let Some(param) = param {
        Some(build_node(param, env, grid, false, stats).await?)
    } else {
        None
    };
    let input_schema = static_schema(&child_op.schema().series)?.clone();

    // topk/bottomk are filter-shaped: output schema == input schema.
    // streaming kinds + quantile are reducer-shaped: one row per group.
    let is_filter_shape = matches!(kind, AggregateKind::Topk(_) | AggregateKind::Bottomk(_));

    let built = build_group_map(&input_schema, &grouping)?;
    let output_schema = if is_filter_shape {
        input_schema.clone()
    } else {
        build_group_schema(&built.group_labels)
    };

    let op = AggregateOp::new_with_param(
        BoxedOp(child_op),
        param_op,
        kind,
        built.map,
        output_schema,
        reservation.clone(),
    )
    .map_err(map_construct_err)?;
    Ok(wrap_op(Box::new(op), "Aggregate", ctx))
}

// ---------------------------------------------------------------------------
// CountValues
// ---------------------------------------------------------------------------

async fn build_count_values<S>(
    label: String,
    child: LogicalPlan,
    grouping: AggregateGrouping,
    env: BuildEnv<'_, S>,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let BuildEnv {
        reservation, ctx, ..
    } = env;
    let child_op = build_node(child, env, grid, false, stats).await?;
    let input_schema = static_schema(&child_op.schema().series)?.clone();

    let (group_map_opt, group_labels_arc) = if is_trivial_grouping(&grouping) {
        // `count_values(l, expr)` without by/without — single global group
        // (matches the operator's `None` contract: group_labels must be
        // [empty]).
        (None, Arc::<[Labels]>::from(vec![Labels::empty()]))
    } else {
        let built = build_group_map(&input_schema, &grouping)?;
        let group_labels: Arc<[Labels]> = Arc::from(built.group_labels);
        (Some(built.map), group_labels)
    };

    let op = CountValuesOp::new(
        BoxedOp(child_op),
        label,
        group_map_opt,
        group_labels_arc,
        reservation.clone(),
    );
    Ok(wrap_op(Box::new(op), "CountValues", ctx))
}

fn is_trivial_grouping(grouping: &AggregateGrouping) -> bool {
    matches!(grouping, AggregateGrouping::By(labels) if labels.is_empty())
}

// ---------------------------------------------------------------------------
// Exchange-operator insertion
// ---------------------------------------------------------------------------

/// Wrap `op` in [`ConcurrentOp`] when the leaf's resolved cardinality
/// exceeds `ctx.parallelism.concurrent_threshold_series`. Otherwise the
/// op is returned unchanged.
///
/// The returned operator is always `Box<dyn Operator + Send>` so callers
/// can stitch wrapped and unwrapped leaves into the same tree uniformly.
///
/// Stats bookkeeping: each call records either a wrap (`record_wrap`) or
/// a skip (`record_skip`) on `stats`, so tests can verify the decision
/// without downcasting the resulting `dyn Operator`.
fn maybe_wrap_concurrent(
    op: Box<dyn Operator + Send>,
    inner_name: &'static str,
    series_count: u64,
    ctx: &LoweringContext,
    stats: &mut ExchangeStats,
) -> Box<dyn Operator + Send> {
    // Always tag the inner op so its own `next()` timing is recorded even
    // when it's adopted by a ConcurrentOp's spawned task.
    let traced_inner = wrap_op(op, inner_name, ctx);
    if ctx.parallelism.should_wrap_concurrent(series_count) {
        stats.record_wrap();
        // `ConcurrentOp::new` takes `C: Operator + Send + 'static` by
        // value. `Box<dyn Operator + Send>` doesn't itself implement
        // `Operator` (no blanket impl), so we route through the existing
        // `BoxedOp` shim that forwards `schema()` / `next()`.
        let wrapped = ConcurrentOp::new(BoxedOp(traced_inner), ctx.parallelism.channel_bound);
        wrap_op(Box::new(wrapped), "Concurrent", ctx)
    } else {
        stats.record_skip();
        traced_inner
    }
}

// ---------------------------------------------------------------------------
// BoxedOp — a small shim giving `Box<dyn Operator + Send>` a concrete
// `Operator` impl so the generic operator structs (`BinaryOp<L, R>`,
// `AggregateOp<C>`, etc.) can consume trait-object children.
// ---------------------------------------------------------------------------

struct BoxedOp(Box<dyn Operator + Send>);

impl Operator for BoxedOp {
    fn schema(&self) -> &OperatorSchema {
        self.0.schema()
    }

    fn next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<super::super::batch::StepBatch, QueryError>>> {
        self.0.next(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
