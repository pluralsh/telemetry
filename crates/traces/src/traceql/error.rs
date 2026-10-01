use super::SourceSpan;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("TraceQL syntax error at {}..{}: {message}", span.start, span.end)]
pub struct ParseError {
    pub message: String,
    pub span: SourceSpan,
}

impl ParseError {
    pub(crate) fn new(message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("TraceQL validation error at {}..{}: {message}", span.start, span.end)]
pub struct ValidationError {
    pub message: String,
    pub span: SourceSpan,
}

impl ValidationError {
    pub(crate) fn new(message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("unsupported TraceQL feature: {0}")]
    Unsupported(String),
    #[error("TraceQL query limit exceeded: {0}")]
    Limit(String),
}
