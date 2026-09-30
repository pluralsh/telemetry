//! Query execution: planning, running and explaining PromQL against a source.

use super::*;

/// Execution outcome bundling the reshaped query value with an optional
/// tracing snapshot. The snapshot is populated when the
/// [`LoweringContext`](crate::promql::plan::LoweringContext) carried a
/// trace collector.
pub(crate) struct ExecuteOutcome {
    pub value: QueryValue,
    pub trace: Option<crate::promql::trace::QueryTrace>,
}

/// Drive the operator tree to completion and reshape into a
/// [`QueryValue`]. Shared by [`TsdbReadEngine::eval_query`] and
/// [`TsdbReadEngine::eval_query_range`]; keeps the dispatch logic in
/// one place. Generic over the [`QueryReader`] type so both the writer's
/// `TsdbQueryReader` and the reader's `ReaderQueryReader` flow through
/// the same pipeline without duplication.
pub(super) async fn execute_query<R: QueryReader + Send + Sync + 'static>(
    query: &str,
    reader: R,
    ctx: crate::promql::plan::LoweringContext,
    is_instant: bool,
    opts: &QueryOptions,
) -> std::result::Result<ExecuteOutcome, QueryError> {
    let reader =
        crate::query::LimitedQueryReader::new(reader, crate::query::QueryLimits::new(opts));
    let source = Arc::new(crate::promql::source_adapter::QueryReaderSource::new(
        Arc::new(reader),
    ));
    execute_query_source(query, source, ctx, is_instant).await
}

pub(crate) async fn execute_query_source<S: crate::promql::source::SeriesSource + 'static>(
    query: &str,
    source: Arc<S>,
    ctx: crate::promql::plan::LoweringContext,
    is_instant: bool,
) -> std::result::Result<ExecuteOutcome, QueryError> {
    use crate::promql::trace::{self, Phase};
    use std::future::poll_fn;

    let trace_collector = ctx.trace.clone();
    let trace_for_run = trace_collector.clone();

    // Hoist the whole pipeline into a task-local scope so storage-layer
    // code can record I/O timings without the planner having to thread the
    // collector through every function signature.
    let run = async move {
        let trace_collector = trace_for_run;
        // Parse --------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "parse");
        let t0 = Instant::now();
        let parse_res = span.in_scope(|| promql_parser::parser::parse(query));
        let expr = parse_res.map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Parse, t0.elapsed().as_nanos() as u64);
        }

        // Lower --------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "lower");
        let t0 = Instant::now();
        let logical = span
            .in_scope(|| crate::promql::plan::lower(&expr, &ctx))
            .map_err(plan_error_to_query_error)?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Lower, t0.elapsed().as_nanos() as u64);
        }

        // Optimize -----------------------------------------------------
        let span = tracing::debug_span!("phase", name = "optimize");
        let t0 = Instant::now();
        let logical = span.in_scope(|| crate::promql::plan::optimize(logical));
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Optimize, t0.elapsed().as_nanos() as u64);
        }

        let reservation = crate::promql::memory::MemoryReservation::new(DEFAULT_MEMORY_CAP_BYTES);

        // Build physical ----------------------------------------------
        let t0 = Instant::now();
        let mut plan = tracing::Instrument::instrument(
            crate::promql::plan::build_physical_plan(logical, &source, reservation, &ctx),
            tracing::debug_span!("phase", name = "build_physical"),
        )
        .await
        .map_err(plan_error_to_query_error)?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::BuildPhysical, t0.elapsed().as_nanos() as u64);
        }

        // Execute ------------------------------------------------------
        let t0 = Instant::now();
        let mut batches = Vec::new();
        let exec_span = tracing::debug_span!("phase", name = "execute");
        let _exec_guard = exec_span.enter();
        loop {
            match poll_fn(|cx| plan.root.next(cx)).await {
                Some(Ok(batch)) => batches.push(batch),
                Some(Err(e)) => return Err(execution_error_to_query_error(e)),
                None => break,
            }
        }
        drop(_exec_guard);
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Execute, t0.elapsed().as_nanos() as u64);
        }

        // Reshape ------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "reshape");
        let t0 = Instant::now();
        let reshaped = span.in_scope(|| {
            if is_instant {
                crate::promql::reshape::reshape_instant(&plan, batches)
            } else {
                crate::promql::reshape::reshape_range(&plan, batches)
            }
        });
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Reshape, t0.elapsed().as_nanos() as u64);
        }
        reshaped.map_err(|e| QueryError::Execution(e.to_string()))
    };

    let value = match trace_collector.clone() {
        Some(c) => trace::with_trace(c, run).await?,
        None => run.await?,
    };

    Ok(ExecuteOutcome {
        value,
        trace: trace_collector.as_ref().map(|c| c.finish()),
    })
}

/// Dry-run: parse, lower, optimize, and describe the physical plan
/// for `query` without opening a reader or executing any operator.
/// Backs both [`TsdbEngine::explain_query`] and
/// [`TsdbEngine::explain_query_range`].
pub(super) fn explain_query(
    query: &str,
    ctx: &crate::promql::plan::LoweringContext,
) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
    let expr =
        promql_parser::parser::parse(query).map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
    let unoptimized = crate::promql::plan::lower(&expr, ctx).map_err(plan_error_to_query_error)?;
    let logical_unoptimized = crate::promql::plan::describe_logical(&unoptimized);
    let optimized = crate::promql::plan::optimize(unoptimized.clone());
    let logical_optimized = crate::promql::plan::describe_logical(&optimized);
    let physical = crate::promql::plan::describe_physical(&optimized, ctx);
    Ok(crate::promql::plan::ExplainResult {
        schema_version: crate::promql::plan::SCHEMA_VERSION,
        logical_unoptimized,
        logical_optimized,
        physical,
    })
}

/// Translate a [`PlanError`](crate::promql::plan::PlanError) into
/// the crate-wide [`QueryError`].
fn plan_error_to_query_error(e: crate::promql::plan::PlanError) -> QueryError {
    use crate::promql::plan::PlanError;
    match e {
        PlanError::UnknownFunction(_)
        | PlanError::InvalidArgument { .. }
        | PlanError::InvalidTopLevelString
        | PlanError::UnsupportedExpression(_)
        | PlanError::UnsupportedFeature(_) => QueryError::InvalidQuery(e.to_string()),
        PlanError::MemoryLimit(_)
        | PlanError::SourceError(_)
        | PlanError::InvalidMatching(_)
        | PlanError::PhysicalPlanFailed(_) => QueryError::Execution(e.to_string()),
    }
}

/// Translate an execution-time
/// [`QueryError`](crate::promql::memory::QueryError) onto the
/// crate-wide [`QueryError`] for the HTTP / embedded boundary.
fn execution_error_to_query_error(e: crate::promql::memory::QueryError) -> QueryError {
    QueryError::Execution(e.to_string())
}

/// Collapse a [`QueryValue`] into the `Vec<RangeSample>` wire shape.
/// Matrix results pass through; scalar results fan out into a single
/// anonymous series with one sample at each returned timestamp; vector
/// results become a one-sample-per-series matrix.
pub(crate) fn query_value_to_range_samples(
    value: QueryValue,
) -> std::result::Result<Vec<RangeSample>, QueryError> {
    Ok(value.into_matrix())
}

/// Parse multiple match[] selector strings into VectorSelectors.
pub(super) fn parse_selectors(
    matchers: &[&str],
) -> std::result::Result<Vec<VectorSelector>, QueryError> {
    matchers
        .iter()
        .map(|s| {
            let expr = promql_parser::parser::parse(s)
                .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
            match expr {
                Expr::VectorSelector(vs) => Ok(vs),
                _ => Err(QueryError::InvalidQuery(
                    "Expected a vector selector".to_string(),
                )),
            }
        })
        .collect()
}

// ── TsdbReadEngine trait ─────────────────────────────────────────────────
//
// Factors out the 5 duplicated eval/find methods shared between `Tsdb`
// (writer) and `TimeSeriesDbReader` (reader). Each implementor provides
// its own `QueryReader` construction; the query logic lives once in the
// default methods.
