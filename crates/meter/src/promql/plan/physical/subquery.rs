//! Subquery lowering: per-outer-step child operator factories.

use super::*;

// ---------------------------------------------------------------------------
// Subquery (factory returning a fresh child operator per outer step)
// ---------------------------------------------------------------------------

/// The range, resolution and time shift of one subquery expression.
pub(super) struct SubqueryWindow {
    pub(super) range_ms: i64,
    pub(super) step_ms: i64,
    pub(super) offset: Offset,
    pub(super) at: Option<AtModifier>,
}

pub(super) async fn build_subquery<S>(
    inner: LogicalPlan,
    env: BuildEnv<'_, S>,
    outer_grid: StepGrid,
    window: SubqueryWindow,
    stats: &mut ExchangeStats,
) -> Result<SubqueryOp, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let BuildEnv {
        source,
        reservation,
        ctx,
    } = env;
    let SubqueryWindow {
        range_ms,
        step_ms,
        offset,
        at,
    } = window;
    // Resolve the inner subtree's output schema by planning it once over
    // a representative inner-grid window. Phase-6 may want to replan at
    // factory call time if the inner plan's series set can vary across
    // outer steps; the `ChildFactory` shape allows that already.
    //
    // For v1 the subquery's output series is assumed plan-time-stable
    // (matches RFC §"Core Data Model": only `count_values` carries
    // deferred schemas, and that is rejected under schema-sensitive
    // parents — which `Rollup` is).

    // Precompute per-outer-step effective evaluation times so the
    // operator can apply `@` / `offset` uniformly per step without
    // re-dispatching on the at/offset shape at runtime.
    let effective_times: Arc<[i64]> = Arc::from(
        (0..outer_grid.step_count)
            .map(|k| outer_step_window(outer_grid, k, range_ms, offset, at).1)
            .collect::<Vec<_>>(),
    );

    // Build a probe child to snapshot the output schema.
    let probe_effective = effective_times
        .first()
        .copied()
        .unwrap_or(outer_grid.start_ms);
    let probe_grid = inner_grid(probe_effective, range_ms, step_ms);
    // Probe the inner subtree once to snapshot its output schema. Stats
    // bookkeeping for the probe is intentionally discarded — the factory
    // will rebuild the subtree (possibly with its own wraps) per outer
    // step; counting the probe's wrap decisions would double-count.
    let mut probe_stats = ExchangeStats::default();
    let probe = build_node(inner.clone(), env, probe_grid, false, &mut probe_stats).await?;
    // Accumulate the probe's wrap/skip decisions into the caller's stats
    // for observability. These wrap decisions are informational — the
    // probe itself is dropped right after schema extraction.
    stats.concurrent_wrapped = stats
        .concurrent_wrapped
        .saturating_add(probe_stats.concurrent_wrapped);
    stats.concurrent_skipped = stats
        .concurrent_skipped
        .saturating_add(probe_stats.concurrent_skipped);
    let inner_schema = static_schema(&probe.schema().series)?.clone();
    drop(probe);

    // Clone captures for the factory closure.
    let source_arc = source.clone();
    let ctx_copy = ctx.clone();
    let reservation_inner = reservation.clone();
    let inner_plan = inner;
    let sub_range_ms = range_ms;
    let factory: ChildFactory = Box::new(move |tr: TimeRange, inner_step_ms: i64| {
        // Use a blocking task-local poll for the factory's sync-Future
        // surface: the factory's return type is sync, but the planner
        // walk is async. Real wiring will replace this with a planner-
        // cached precomputed tree per unique (range, step); for v1 the
        // factory just re-invokes the planner synchronously.
        //
        // The subquery encodes the outer effective time in `tr` as
        // `tr.end_ms_exclusive - 1` (inclusive upper bound). Align the
        // inner grid descending from that point so the inner evaluation
        // timestamps are `{effective, effective - step, …}` rather than
        // the raw range-start the old inner-grid builder produced.
        let effective_t = tr.end_ms_exclusive.saturating_sub(1);
        let grid = inner_grid(effective_t, sub_range_ms, inner_step_ms);
        let src = source_arc.clone();
        // Each child is dropped after its outer step; the scope returns
        // anything its operators still hold at that point.
        let res = reservation_inner.scoped();
        let ctx_cp = ctx_copy.clone();
        let plan = inner_plan.clone();
        // Drive the async recursive planner to completion using a
        // single-threaded runtime. This is a plan-time path, called once
        // per outer step by `SubqueryOp::windows`; we accept the per-call
        // runtime spin-up over propagating async through the operator
        // trait (the trait is sync `poll` and the RFC places subquery
        // re-planning inside `poll_windows`).
        // Factory-owned stats scratch: wraps inserted inside the inner
        // subtree are already reflected in the top-level `ExchangeStats`
        // via the schema-probe walk above, so we discard them here.
        let mut factory_stats = ExchangeStats::default();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // Hand the work back to the existing runtime via
                // `block_in_place`; callers should already be on a
                // multi-thread runtime for the outer query.
                let fut = build_node(
                    plan,
                    BuildEnv {
                        source: &src,
                        reservation: &res,
                        ctx: &ctx_cp,
                    },
                    grid,
                    false,
                    &mut factory_stats,
                );
                tokio::task::block_in_place(|| handle.block_on(fut))
                    .map_err(|e| QueryError::Internal(format!("subquery plan failed: {e}")))
            }
            Err(_) => {
                // No runtime — synthesise a minimal runtime just for the
                // plan. This path is only hit in sync tests that drive the
                // plan outside a tokio context.
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| QueryError::Internal(format!("subquery rt build: {e}")))?;
                let fut = build_node(
                    plan,
                    BuildEnv {
                        source: &src,
                        reservation: &res,
                        ctx: &ctx_cp,
                    },
                    grid,
                    false,
                    &mut factory_stats,
                );
                rt.block_on(fut)
                    .map_err(|e| QueryError::Internal(format!("subquery plan failed: {e}")))
            }
        }
    });

    let sub = SubqueryOp::with_effective_times(
        factory,
        inner_schema,
        outer_grid,
        range_ms,
        step_ms,
        effective_times,
        reservation.clone(),
    );
    Ok(sub)
}

fn outer_step_window(
    outer_grid: StepGrid,
    step_idx: usize,
    range_ms: i64,
    offset: Offset,
    at: Option<AtModifier>,
) -> (i64, i64) {
    let step_idx = step_idx as i64;
    let pinned = match at {
        Some(AtModifier::Value(v)) => v,
        Some(AtModifier::Start) => outer_grid.start_ms,
        Some(AtModifier::End) => outer_grid.end_ms,
        None => outer_grid.start_ms + step_idx * outer_grid.step_ms,
    };
    let effective = pinned.saturating_sub(offset.signed_ms());
    let start = effective.saturating_sub(range_ms).saturating_add(1);
    (start, effective)
}

/// Build the inner step grid for a subquery's `(range, step)` bracket
/// evaluated at effective time `outer_t`.
///
/// Inner evaluation points are the **absolute multiples of
/// `inner_step_ms`** (i.e. `k * step_ms` for integer `k`) that fall in
/// the half-open window `(outer_t - range, outer_t]`. This matches
/// Prometheus' step-aligned subquery layout: when `range < step` and no
/// multiple of `step` lands in the window, the inner grid is empty and
/// the subquery emits no samples (`subquery.test:196` —
/// `min_over_time((topk(1, foo))[1m:5m])` at 12m is `empty`).
///
/// The previous forward-walk from `outer_t - range + 1` produced
/// non-aligned inner timestamps (e.g. `-39999` instead of `-30000` for a
/// `50s:10s` subquery at `t=10s`), and a plain descending walk from
/// `outer_t` always produced at least one inner point regardless of step
/// — wrong for the "range < resolution" fixture above.
pub(super) fn inner_grid(effective_t: i64, range_ms: i64, inner_step_ms: i64) -> StepGrid {
    let range = range_ms.max(1);
    let step = inner_step_ms.max(1);
    let window_lo = effective_t.saturating_sub(range).saturating_add(1); // inclusive
    // `first` = smallest multiple of `step` ≥ `window_lo`, i.e. ceil-div
    // on a positive divisor. `-(-x).div_euclid(s)` gives the Euclidean
    // ceiling for any signed `x`.
    let first = -((-window_lo).div_euclid(step)) * step;
    // `last` = largest multiple of `step` ≤ `effective_t`.
    let last = effective_t.div_euclid(step) * step;
    if first > last {
        // Window contains no multiple of `step` — emit an empty grid so
        // the subquery propagates `absent` through the enclosing rollup.
        return StepGrid {
            start_ms: effective_t,
            end_ms: effective_t,
            step_ms: step,
            step_count: 0,
        };
    }
    let step_count = ((last - first) / step + 1) as usize;
    StepGrid {
        start_ms: first,
        end_ms: last,
        step_ms: step,
        step_count,
    }
}
