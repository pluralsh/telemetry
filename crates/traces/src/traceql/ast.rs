use std::fmt;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn join(self, other: Self) -> Self {
        Self::new(self.start, other.end)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: SourceSpan,
}

impl<T> Spanned<T> {
    pub const fn new(value: T, span: SourceSpan) -> Self {
        Self { value, span }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusValue {
    Unset,
    Ok,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KindValue {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StaticValue {
    Nil,
    String(String),
    Bool(bool),
    Int(i64),
    Float(f64),
    Duration(i64),
    Status(StatusValue),
    Kind(KindValue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttributeScope {
    Unscoped,
    Resource,
    Span,
    Instrumentation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribute {
    pub scope: AttributeScope,
    pub name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Intrinsic {
    TraceId,
    SpanId,
    ParentId,
    Name,
    Duration,
    Status,
    StatusMessage,
    Kind,
    RootName,
    RootServiceName,
    ChildCount,
    TraceDuration,
    InstrumentationName,
    InstrumentationVersion,
    NestedSetLeft,
    NestedSetRight,
    NestedSetParent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Or,
    And,
    Equal,
    NotEqual,
    Regex,
    NotRegex,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Static(StaticValue),
    Attribute(Attribute),
    Intrinsic(Intrinsic),
    Unary {
        op: UnaryOp,
        expr: Box<Spanned<Expr>>,
    },
    Binary {
        lhs: Box<Spanned<Expr>>,
        op: BinaryOp,
        rhs: Box<Spanned<Expr>>,
    },
}

pub type FieldExpr = Spanned<Expr>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructuralOp {
    And,
    Union,
    Child,
    Parent,
    Descendant,
    Ancestor,
    Sibling,
    NotChild,
    NotParent,
    NotDescendant,
    NotAncestor,
    NotSibling,
    UnionChild,
    UnionParent,
    UnionDescendant,
    UnionAncestor,
    UnionSibling,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SpansetExpr {
    Filter(FieldExpr),
    Binary {
        lhs: Box<Spanned<SpansetExpr>>,
        op: StructuralOp,
        rhs: Box<Spanned<SpansetExpr>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateOp {
    Count,
    Min,
    Max,
    Avg,
    Sum,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    Static(StaticValue),
    Aggregate {
        op: AggregateOp,
        field: Option<FieldExpr>,
    },
    Binary {
        lhs: Box<ScalarExpr>,
        op: BinaryOp,
        rhs: Box<ScalarExpr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineStage {
    /// `| { ... }`: keeps the spans of each spanset that match.
    SpansetFilter(FieldExpr),
    By(FieldExpr),
    Coalesce,
    Select(Vec<FieldExpr>),
    ScalarFilter {
        lhs: ScalarExpr,
        op: BinaryOp,
        rhs: ScalarExpr,
        span: SourceSpan,
    },
    Metric {
        name: String,
        source: String,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hint {
    pub name: String,
    pub value: StaticValue,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub spanset: Spanned<SpansetExpr>,
    pub stages: Vec<PipelineStage>,
    pub hints: Vec<Hint>,
}

fn precedence(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 1,
        BinaryOp::And => 2,
        BinaryOp::Equal
        | BinaryOp::NotEqual
        | BinaryOp::Regex
        | BinaryOp::NotRegex
        | BinaryOp::Less
        | BinaryOp::LessEqual
        | BinaryOp::Greater
        | BinaryOp::GreaterEqual => 3,
        BinaryOp::Add | BinaryOp::Sub => 4,
        BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 5,
        BinaryOp::Pow => 6,
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DisplaySpanset(&self.spanset.value, 0))?;
        for stage in &self.stages {
            write!(f, " | {stage}")?;
        }
        if !self.hints.is_empty() {
            f.write_str(" with(")?;
            for (index, hint) in self.hints.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{} = {}", hint.name, DisplayStatic(&hint.value))?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

struct DisplayStatic<'a>(&'a StaticValue);

impl fmt::Display for DisplayStatic<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            StaticValue::Nil => f.write_str("nil"),
            StaticValue::String(value) => write!(f, "{}", serde_json::to_string(value).unwrap()),
            StaticValue::Bool(value) => write!(f, "{value}"),
            StaticValue::Int(value) => write!(f, "{value}"),
            StaticValue::Float(value) => write!(f, "{value}"),
            StaticValue::Duration(value) => write!(f, "{value}ns"),
            StaticValue::Status(value) => f.write_str(match value {
                StatusValue::Unset => "unset",
                StatusValue::Ok => "ok",
                StatusValue::Error => "error",
            }),
            StaticValue::Kind(value) => f.write_str(match value {
                KindValue::Unspecified => "unspecified",
                KindValue::Internal => "internal",
                KindValue::Server => "server",
                KindValue::Client => "client",
                KindValue::Producer => "producer",
                KindValue::Consumer => "consumer",
            }),
        }
    }
}

struct DisplayExpr<'a>(&'a Expr, u8);

impl fmt::Display for DisplayExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Expr::Static(value) => write!(f, "{}", DisplayStatic(value)),
            Expr::Attribute(attribute) => {
                let prefix = match attribute.scope {
                    AttributeScope::Unscoped => ".",
                    AttributeScope::Resource => "resource.",
                    AttributeScope::Span => "span.",
                    AttributeScope::Instrumentation => "instrumentation.",
                };
                write!(f, "{prefix}{}", display_attribute_name(&attribute.name))
            }
            Expr::Intrinsic(value) => f.write_str(intrinsic_name(*value)),
            Expr::Unary { op, expr } => {
                let symbol = if *op == UnaryOp::Not { "!" } else { "-" };
                write!(f, "{symbol}{}", DisplayExpr(&expr.value, 7))
            }
            Expr::Binary { lhs, op, rhs } => {
                let precedence = precedence(*op);
                let parentheses = precedence < self.1;
                if parentheses {
                    f.write_str("(")?;
                }
                write!(
                    f,
                    "{} {} {}",
                    DisplayExpr(&lhs.value, precedence),
                    binary_symbol(*op),
                    DisplayExpr(&rhs.value, precedence + u8::from(*op != BinaryOp::Pow))
                )?;
                if parentheses {
                    f.write_str(")")?;
                }
                Ok(())
            }
        }
    }
}

struct DisplaySpanset<'a>(&'a SpansetExpr, u8);

impl fmt::Display for DisplaySpanset<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            SpansetExpr::Filter(expr) => write!(f, "{{ {} }}", DisplayExpr(&expr.value, 0)),
            SpansetExpr::Binary { lhs, op, rhs } => {
                let precedence = if matches!(op, StructuralOp::Union) {
                    1
                } else {
                    2
                };
                let parentheses = precedence < self.1;
                if parentheses {
                    f.write_str("(")?;
                }
                write!(
                    f,
                    "{} {} {}",
                    DisplaySpanset(&lhs.value, precedence),
                    structural_symbol(*op),
                    DisplaySpanset(&rhs.value, precedence + 1)
                )?;
                if parentheses {
                    f.write_str(")")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpansetFilter(expr) => write!(f, "{{ {} }}", DisplayExpr(&expr.value, 0)),
            Self::By(expr) => write!(f, "by({})", DisplayExpr(&expr.value, 0)),
            Self::Coalesce => f.write_str("coalesce()"),
            Self::Select(fields) => {
                f.write_str("select(")?;
                for (index, field) in fields.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", DisplayExpr(&field.value, 0))?;
                }
                f.write_str(")")
            }
            Self::ScalarFilter { lhs, op, rhs, .. } => {
                write!(
                    f,
                    "{} {} {}",
                    DisplayScalar(lhs),
                    binary_symbol(*op),
                    DisplayScalar(rhs)
                )
            }
            Self::Metric { source, .. } => f.write_str(source),
        }
    }
}

struct DisplayScalar<'a>(&'a ScalarExpr);

impl fmt::Display for DisplayScalar<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ScalarExpr::Static(value) => write!(f, "{}", DisplayStatic(value)),
            ScalarExpr::Aggregate { op, field } => {
                let name = match op {
                    AggregateOp::Count => "count",
                    AggregateOp::Min => "min",
                    AggregateOp::Max => "max",
                    AggregateOp::Avg => "avg",
                    AggregateOp::Sum => "sum",
                };
                f.write_str(name)?;
                f.write_str("(")?;
                if let Some(field) = field {
                    write!(f, "{}", DisplayExpr(&field.value, 0))?;
                }
                f.write_str(")")
            }
            ScalarExpr::Binary { lhs, op, rhs } => {
                write!(
                    f,
                    "({} {} {})",
                    DisplayScalar(lhs),
                    binary_symbol(*op),
                    DisplayScalar(rhs)
                )
            }
        }
    }
}

fn display_attribute_name(name: &str) -> String {
    if name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "_-.".contains(character))
    {
        name.to_owned()
    } else {
        serde_json::to_string(name).unwrap()
    }
}

fn intrinsic_name(value: Intrinsic) -> &'static str {
    match value {
        Intrinsic::TraceId => "trace:id",
        Intrinsic::SpanId => "span:id",
        Intrinsic::ParentId => "span:parentID",
        Intrinsic::Name => "span:name",
        Intrinsic::Duration => "span:duration",
        Intrinsic::Status => "span:status",
        Intrinsic::StatusMessage => "span:statusMessage",
        Intrinsic::Kind => "span:kind",
        Intrinsic::RootName => "trace:rootName",
        Intrinsic::RootServiceName => "trace:rootService",
        Intrinsic::ChildCount => "span:childCount",
        Intrinsic::TraceDuration => "trace:duration",
        Intrinsic::InstrumentationName => "instrumentation:name",
        Intrinsic::InstrumentationVersion => "instrumentation:version",
        Intrinsic::NestedSetLeft => "nestedSetLeft",
        Intrinsic::NestedSetRight => "nestedSetRight",
        Intrinsic::NestedSetParent => "nestedSetParent",
    }
}

pub(crate) fn binary_symbol(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "||",
        BinaryOp::And => "&&",
        BinaryOp::Equal => "=",
        BinaryOp::NotEqual => "!=",
        BinaryOp::Regex => "=~",
        BinaryOp::NotRegex => "!~",
        BinaryOp::Less => "<",
        BinaryOp::LessEqual => "<=",
        BinaryOp::Greater => ">",
        BinaryOp::GreaterEqual => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
    }
}

fn structural_symbol(op: StructuralOp) -> &'static str {
    match op {
        StructuralOp::And => "&&",
        StructuralOp::Union => "||",
        StructuralOp::Child => ">",
        StructuralOp::Parent => "<",
        StructuralOp::Descendant => ">>",
        StructuralOp::Ancestor => "<<",
        StructuralOp::Sibling => "~",
        StructuralOp::NotChild => "!>",
        StructuralOp::NotParent => "!<",
        StructuralOp::NotDescendant => "!>>",
        StructuralOp::NotAncestor => "!<<",
        StructuralOp::NotSibling => "!~",
        StructuralOp::UnionChild => "&>",
        StructuralOp::UnionParent => "&<",
        StructuralOp::UnionDescendant => "&>>",
        StructuralOp::UnionAncestor => "&<<",
        StructuralOp::UnionSibling => "&~",
    }
}
