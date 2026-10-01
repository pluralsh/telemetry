//! Span-aware LogQL syntax and validation.
//!
//! Parsing and validation are separate: [`parse_syntax`] accepts structurally
//! valid LogQL and preserves source spans, while [`validate`] applies resource
//! limits and Loki selector semantics. Use [`parse`] for the usual validated
//! path.
//!
//! Logos performs lossless tokenization and the generated LALRPOP parser owns
//! metric-expression productions, associativity, and binary precedence. The
//! contextual parser lowers selectors and pipelines because current LogQL
//! deliberately permits the same pipeline around either side of a range and
//! overloads `!=`/`!~` as both line-filter and metric operators. Lowering those
//! normalized forms directly keeps exact byte spans and avoids duplicating a
//! large set of semantically equivalent range productions. The LALRPOP grammar
//! is therefore intentionally an expression grammar, not a token pass-through.

mod ast;
mod error;
lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, clippy::pedantic)]
    grammar,
    "/logql/grammar.rs"
);
mod lexer;
mod parser;
mod validation;

pub use ast::*;
pub use error::{ParseError, ValidationError};
pub(crate) use validation::IpPattern;
pub use validation::{
    DEFAULT_MAX_DEPTH, DEFAULT_MAX_QUERY_BYTES, ValidationOptions, validate_selector,
};

/// Parse LogQL without applying semantic or resource-limit validation.
pub fn parse_syntax(source: &str) -> Result<Query, ParseError> {
    parser::parse_syntax(source)
}

/// Validate an already parsed query.
pub fn validate(
    query: &Query,
    source_len: usize,
    options: ValidationOptions,
) -> Result<(), ValidationError> {
    validation::validate(query, source_len, options)
}

/// Parse and validate LogQL with production defaults.
pub fn parse(source: &str) -> Result<Query, QueryError> {
    parse_with_options(source, ValidationOptions::default())
}

/// Parse and validate LogQL with caller-provided resource limits.
pub fn parse_with_options(source: &str, options: ValidationOptions) -> Result<Query, QueryError> {
    if source.len() > options.max_query_bytes {
        return Err(QueryError::Validation(ValidationError {
            message: format!(
                "query is {} bytes; maximum is {}",
                source.len(),
                options.max_query_bytes
            ),
            span: Span::new(options.max_query_bytes, source.len()),
        }));
    }
    let query = parse_syntax(source)?;
    validate(&query, source.len(), options)?;
    Ok(query)
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
}
