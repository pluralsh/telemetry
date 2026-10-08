//! Internals exposed to benchmarks through the `bench-internals` feature.

use std::sync::Arc;

use crate::sidecar::PageColumns;
use crate::traceql::{CompiledQuery, QueryError, TraceFilter};
use crate::{Page, Trace, TraceQlResult};

/// A parsed, validated, and planned TraceQL query, ready to execute against
/// traces.
pub struct PreparedQuery(CompiledQuery);

pub fn prepare(source: &str) -> Result<PreparedQuery, QueryError> {
    Ok(PreparedQuery(CompiledQuery::new(crate::traceql::parse(
        source,
    )?)))
}

/// Like [`prepare`], but evaluated by the interpreter alone.
pub fn prepare_interpreted(source: &str) -> Result<PreparedQuery, QueryError> {
    Ok(PreparedQuery(CompiledQuery::interpreted(
        crate::traceql::parse(source)?,
    )))
}

pub fn execute(
    trace: &Trace,
    query: &PreparedQuery,
    max_spans: usize,
) -> Result<Option<TraceQlResult>, QueryError> {
    crate::traceql::execute_compiled(trace, &query.0, max_spans)
}

/// Compressed column sidecar bytes in `page`.
pub fn sidecar_len(page: &Page) -> usize {
    page.sidecar_len()
}

/// The page-column prefilter of a query; `None` inside when it cannot prune.
pub struct Prefilter(Option<TraceFilter>);

pub fn prefilter(source: &str) -> Result<Prefilter, QueryError> {
    Ok(Prefilter(crate::traceql::prefilter(
        &crate::traceql::parse(source)?,
    )))
}

/// Decodes `bytes` as a page, then its columns, and counts the traces the
/// prefilter rules out without decoding any trace.
pub fn ruled_out(bytes: bytes::Bytes, prefilter: &Prefilter) -> crate::Result<usize> {
    if prefilter.0.is_none() {
        return Ok(0);
    }
    let page = Page::decode(bytes)?;
    ruled_out_by(&columns(&page)?, prefilter)
}

/// A page's decoded column sidecar.
pub struct Columns(Arc<PageColumns>);

pub fn columns(page: &Page) -> crate::Result<Columns> {
    page.columns().map(Columns)
}

/// Counts the traces the prefilter rules out given already decoded columns.
pub fn ruled_out_by(columns: &Columns, prefilter: &Prefilter) -> crate::Result<usize> {
    let Some(filter) = &prefilter.0 else {
        return Ok(0);
    };
    let exists = columns.0.exists(filter)?;
    let traces = exists.first().map_or(0, Vec::len);
    Ok((0..traces)
        .filter(|&trace| {
            let satisfied = exists.iter().map(|leaf| leaf[trace]).collect::<Vec<_>>();
            !filter.matches(&satisfied)
        })
        .count())
}
