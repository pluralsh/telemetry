//! Subquery lowering: one inner evaluation spanning every outer window.

use super::super::super::operators::subquery::subquery_span;
use super::*;

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
    let SubqueryWindow {
        range_ms,
        step_ms,
        offset,
        at,
    } = window;

    // Per-outer-step effective evaluation times, so the operator applies
    // `@` / `offset` uniformly per step.
    let effective_times: Arc<[i64]> = Arc::from(
        (0..outer_grid.step_count)
            .map(|k| outer_step_window(outer_grid, k, range_ms, offset, at).1)
            .collect::<Vec<_>>(),
    );

    // One inner evaluation over the union of every outer window. Inner
    // points are absolute multiples of the step, so each outer window's
    // points are a subset of this grid.
    let (span_lo, span_hi) = subquery_span(&effective_times, range_ms);
    let grid = inner_grid(span_hi, span_hi - span_lo + 1, step_ms);
    let child = build_node(inner, env, grid, false, stats).await?;
    static_schema(&child.schema().series)?;

    Ok(SubqueryOp::with_effective_times(
        child,
        outer_grid,
        range_ms,
        effective_times,
        env.reservation.clone(),
    ))
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
