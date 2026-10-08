//! TraceQL syntax, semantic validation, planning, and non-metrics execution.

mod ast;
pub(crate) mod columns;
mod error;
mod execution;
lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, clippy::pedantic)]
    grammar,
    "/traceql/grammar.rs"
);
mod lexer;
mod parser;
mod plan;
mod validation;

pub use ast::*;
pub use error::{ParseError, QueryError, ValidationError};
pub use execution::{MatchedSpan, TraceQlResult, TraceSummary};
pub(crate) use plan::span_intrinsics;
pub use plan::{
    IndexField, IndexPredicate, IndexTest, PushdownClause, QueryPlan, existential, plan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryOptions {
    pub limit: usize,
    /// Cap on verified candidate traces loaded and executed. Candidates are
    /// consumed in result order and loading stops at `limit` matches, so this
    /// bounds work actually done rather than the size of the index hit set.
    pub max_candidate_traces: usize,
    pub max_spans_per_trace: usize,
    pub max_concurrency: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            max_candidate_traces: 10_000,
            max_spans_per_trace: 100_000,
            max_concurrency: 8,
        }
    }
}

pub fn parse_syntax(source: &str) -> Result<Query, ParseError> {
    parser::parse_syntax(source)
}

pub fn validate(query: &Query) -> Result<(), ValidationError> {
    validation::validate(query)
}

pub fn parse(source: &str) -> Result<Query, QueryError> {
    let query = parse_syntax(source)?;
    validate(&query)?;
    Ok(query)
}

pub(crate) use columns::{TraceFilter, prefilter};
#[cfg(test)]
pub(crate) use execution::execute;
pub(crate) use execution::{CompiledQuery, execute_compiled, summarize_compiled};

#[cfg(test)]
mod tests;
