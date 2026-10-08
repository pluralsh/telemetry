//! Preload-range planning: which time ranges a query will touch.

use super::*;

/// Compute the disjoint preload ranges (seconds) a query touches after
/// applying `offset`/`@` modifiers. Falls back to
/// `[(default_start, default_end)]` for selector-free expressions.
pub(crate) fn preload_ranges(
    stmt: &EvalStmt,
    default_start: i64,
    default_end: i64,
) -> Vec<(i64, i64)> {
    let ranges = compute_preload_ranges(&stmt.expr, stmt.start, stmt.end, stmt.lookback_delta);
    if ranges.is_empty() {
        vec![(default_start, default_end)]
    } else {
        ranges
    }
}

/// Default per-query memory cap for the PromQL engine.
///
/// 1 GiB — a conservative ceiling that should comfortably fit any
/// single-digit-GB host's working set while still failing fast on a
/// runaway query.
pub(crate) const DEFAULT_MEMORY_CAP_BYTES: usize = 1024 * 1024 * 1024;

pub(crate) use common::time::{duration_ms as duration_to_ms, unix_millis as system_time_to_ms};

/// Compute the preload ranges (seconds) the PromQL engine's `QueryReader`
/// needs to open by re-parsing `query`.
pub(crate) fn preload_ranges_for_query(
    query: &str,
    start_ms: i64,
    end_ms: i64,
    lookback_delta: Duration,
) -> std::result::Result<Vec<(i64, i64)>, QueryError> {
    let expr =
        promql_parser::parser::parse(query).map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
    let start = UNIX_EPOCH + Duration::from_millis(start_ms.max(0) as u64);
    let end = UNIX_EPOCH + Duration::from_millis(end_ms.max(0) as u64);
    let stmt = EvalStmt {
        expr,
        start,
        end,
        interval: Duration::from_secs(0),
        lookback_delta,
    };
    let default_start_secs = start
        .checked_sub(lookback_delta)
        .unwrap_or(UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    let default_end_secs = end
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    Ok(preload_ranges(&stmt, default_start_secs, default_end_secs))
}

/// The windows one step of `expr` reads, as inclusive `(earliest, latest)`
/// millisecond offsets from the step's time: [`preload_ranges_for_query`]
/// at `start = end = t`, shifted by `-t`. Without `@` every read is relative
/// to the step, so the windows of step `t` are these offsets plus `t`.
/// `None` when `expr` uses `@`, which pins reads to absolute times.
pub(crate) fn step_read_offsets(expr: &Expr, lookback_delta: Duration) -> Option<Vec<(i64, i64)>> {
    if uses_at(expr) {
        return None;
    }
    let mut offsets = Vec::new();
    preload_ranges_inner(
        expr,
        0,
        0,
        0,
        0,
        duration_to_ms(lookback_delta),
        &mut offsets,
    );
    Some(offsets)
}

fn uses_at(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(vs) => vs.at.is_some(),
        Expr::MatrixSelector(ms) => ms.vs.at.is_some(),
        Expr::Subquery(sq) => sq.at.is_some() || uses_at(&sq.expr),
        Expr::Aggregate(agg) => uses_at(&agg.expr) || agg.param.as_deref().is_some_and(uses_at),
        Expr::Binary(b) => uses_at(&b.lhs) || uses_at(&b.rhs),
        Expr::Paren(p) => uses_at(&p.expr),
        Expr::Call(call) => call.args.args.iter().any(|arg| uses_at(arg)),
        Expr::Unary(u) => uses_at(&u.expr),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
        Expr::Extension(_) => true,
    }
}

// ── Preload-range computation (ported from v1 evaluator) ──

/// Walk the AST and compute the disjoint time ranges needed for bucket preloading.
///
/// For each selector, compute the effective evaluation time by applying
/// `@` and `offset` modifiers, then expand by `lookback_delta` (vector) or
/// range (matrix). Returns a sorted, non-overlapping list of
/// `(earliest_secs, latest_secs)` ranges covering all selectors.
///
/// Returns an empty `Vec` when the expression contains no selectors
/// (e.g. `1 + 2`), allowing the caller to fall back to the default window.
fn compute_preload_ranges(
    expr: &Expr,
    query_start: SystemTime,
    query_end: SystemTime,
    lookback_delta: Duration,
) -> Vec<(i64, i64)> {
    let start_ms = system_time_to_ms(query_start);
    let end_ms = system_time_to_ms(query_end);
    let lookback_ms = duration_to_ms(lookback_delta);
    let mut ranges = Vec::new();
    preload_ranges_inner(
        expr,
        start_ms,
        end_ms,
        start_ms,
        end_ms,
        lookback_ms,
        &mut ranges,
    );
    let ranges_secs: Vec<(i64, i64)> = ranges
        .into_iter()
        .map(|(lo, hi)| {
            let start_secs = lo.div_euclid(1000);
            let end_secs = hi.div_euclid(1000) + i64::from(hi.rem_euclid(1000) != 0);
            (start_secs, end_secs)
        })
        .collect();
    normalize_ranges(ranges_secs)
}

/// Sort ranges by start and merge overlapping ones. Adjacent-but-not-overlapping
/// ranges are kept separate since they may map to different buckets.
fn normalize_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|&(start, _)| start);
    let mut merged = Vec::with_capacity(ranges.len());
    let (mut cur_start, mut cur_end) = ranges[0];
    for &(start, end) in &ranges[1..] {
        if start <= cur_end {
            cur_end = cur_end.max(end);
        } else {
            merged.push((cur_start, cur_end));
            cur_start = start;
            cur_end = end;
        }
    }
    merged.push((cur_start, cur_end));
    merged
}

/// Compute the effective evaluation-time range for a selector after applying
/// `@` and `offset` modifiers, then return `(earliest_ms, latest_ms)` after
/// subtracting the backward window (lookback or matrix range).
fn selector_bounds(
    at: Option<&promql_parser::parser::AtModifier>,
    offset: Option<&promql_parser::parser::Offset>,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    backward_window_ms: i64,
) -> (i64, i64) {
    use promql_parser::parser::{AtModifier, Offset};

    let (mut start, mut end) = if let Some(at_mod) = at {
        match at_mod {
            AtModifier::At(time) => {
                let t = system_time_to_ms(*time);
                (t, t)
            }
            AtModifier::Start | AtModifier::End => (at_start_ms, at_end_ms),
        }
    } else {
        (eval_start_ms, eval_end_ms)
    };

    if let Some(off) = offset {
        match off {
            Offset::Pos(d) => {
                let off_ms = duration_to_ms(*d);
                start = start.saturating_sub(off_ms);
                end = end.saturating_sub(off_ms);
            }
            Offset::Neg(d) => {
                let off_ms = duration_to_ms(*d);
                start = start.saturating_add(off_ms);
                end = end.saturating_add(off_ms);
            }
        }
    }

    let earliest = start.saturating_sub(backward_window_ms);
    (earliest, end)
}

fn preload_ranges_inner(
    expr: &Expr,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    lookback_ms: i64,
    out: &mut Vec<(i64, i64)>,
) {
    match expr {
        Expr::VectorSelector(vs) => {
            out.push(selector_bounds(
                vs.at.as_ref(),
                vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
            ));
        }
        Expr::MatrixSelector(ms) => {
            let range_ms = duration_to_ms(ms.range);
            out.push(selector_bounds(
                ms.vs.at.as_ref(),
                ms.vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                range_ms,
            ));
        }
        Expr::Subquery(sq) => {
            let (sq_start, sq_end) = selector_bounds(
                sq.at.as_ref(),
                sq.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                0,
            );
            let range_ms = duration_to_ms(sq.range);
            let inner_eval_start = sq_start.saturating_sub(range_ms);
            preload_ranges_inner(
                &sq.expr,
                at_start_ms,
                at_end_ms,
                inner_eval_start,
                sq_end,
                lookback_ms,
                out,
            );
        }
        Expr::Aggregate(agg) => {
            preload_ranges_inner(
                &agg.expr,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            if let Some(ref param) = agg.param {
                preload_ranges_inner(
                    param,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Binary(b) => {
            preload_ranges_inner(
                &b.lhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            preload_ranges_inner(
                &b.rhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
        }
        Expr::Paren(p) => preload_ranges_inner(
            &p.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::Call(call) => {
            for arg in &call.args.args {
                preload_ranges_inner(
                    arg,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Unary(u) => preload_ranges_inner(
            &u.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}
