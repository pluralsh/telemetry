//! Span predicates compiled to tests over dense intrinsic columns.
//!
//! A [`ColumnPredicate`] evaluates every span of a [`ColumnSource`] at once
//! into a mask, in loops simple enough for the compiler to vectorize. Exact
//! predicates agree with the interpreter on every span; a [`TraceFilter`] is
//! a necessary condition for a query to return a trace, used to skip traces
//! whose page columns rule it out.

use opentelemetry_proto::tonic::{common::v1::any_value, trace::v1::Span};

use super::ast::{
    AttributeScope, BinaryOp, Expr, FieldExpr, Intrinsic, KindValue, PipelineStage, Query,
    SpansetExpr, StaticValue, StatusValue, StructuralOp, UnaryOp,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Comparison {
    fn from_op(op: BinaryOp) -> Option<Self> {
        Some(match op {
            BinaryOp::Equal => Self::Equal,
            BinaryOp::NotEqual => Self::NotEqual,
            BinaryOp::Less => Self::Less,
            BinaryOp::LessEqual => Self::LessEqual,
            BinaryOp::Greater => Self::Greater,
            BinaryOp::GreaterEqual => Self::GreaterEqual,
            _ => return None,
        })
    }

    /// The comparison with its operands swapped.
    fn flip(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
            other => other,
        }
    }

    fn equality(self) -> Option<bool> {
        match self {
            Self::Equal => Some(true),
            Self::NotEqual => Some(false),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ColumnPredicate {
    Duration(Comparison, i64),
    /// Whether the duration lies in `low..=low + width` (wrapping), or
    /// outside it when `inside` is false. Comparisons against floats compile
    /// to this, which needs no float conversion per span.
    DurationIn {
        low: i64,
        width: u64,
        inside: bool,
    },
    Status {
        equal: bool,
        code: u8,
    },
    Kind {
        equal: bool,
        code: u8,
    },
    Name {
        equal: bool,
        value: String,
    },
    /// `resource.service.name` against a string.
    ServiceName {
        equal: bool,
        value: String,
    },
    /// An attribute of [`DEDICATED_COLUMNS`] against a literal.
    Dedicated {
        column: usize,
        test: DedicatedTest,
    },
    /// An expression whose value cannot vary within one resource, evaluated
    /// once per resource by the interpreter.
    Constant(FieldExpr),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl ColumnPredicate {
    /// Appends the [`DEDICATED_COLUMNS`] indexes the predicate reads.
    pub(crate) fn dedicated_columns(&self, out: &mut Vec<usize>) {
        match self {
            Self::Dedicated { column, .. } => out.push(*column),
            Self::Not(inner) => inner.dedicated_columns(out),
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => {
                lhs.dedicated_columns(out);
                rhs.dedicated_columns(out);
            }
            _ => {}
        }
    }
}

/// Where a predicate will be evaluated, which decides the leaves available.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Target {
    /// A decoded trace, where any expression can be evaluated per resource.
    Trace,
    /// A page's persisted columns.
    Sidecar,
}

/// Dense per-span intrinsic columns, indexed in interpreter span order.
pub(crate) trait ColumnSource {
    fn span_count(&self) -> usize;
    fn durations(&self) -> &[i64];
    fn statuses(&self) -> &[u8];
    fn kinds(&self) -> &[u8];
    /// Sets each span's mask entry to whether its name equals `value`.
    fn names_equal(&self, value: &str, out: &mut [bool]);
    /// `None` when the source cannot evaluate the test.
    fn service_names(&self, equal: bool, value: &str, out: &mut [bool]) -> Option<()>;
    /// Applies `test` to the attribute of [`DEDICATED_COLUMNS`] at
    /// `column`; `None` when the source cannot evaluate it.
    fn dedicated(&self, column: usize, test: &DedicatedTest, out: &mut [bool]) -> Option<()>;
    /// `None` when the source cannot evaluate the test.
    fn constant(&self, expression: &FieldExpr, out: &mut [bool]) -> Option<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DedicatedType {
    String,
    Int,
}

/// A span or resource attribute kept as its own sidecar column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DedicatedColumn {
    pub(crate) scope: AttributeScope,
    pub(crate) name: &'static str,
    pub(crate) kind: DedicatedType,
}

/// Sidecar columns follow this order, so changing it changes the format.
pub(crate) const DEDICATED_COLUMNS: [DedicatedColumn; 8] = [
    dedicated_column(AttributeScope::Span, "http.status_code", DedicatedType::Int),
    dedicated_column(
        AttributeScope::Span,
        "http.response.status_code",
        DedicatedType::Int,
    ),
    dedicated_column(AttributeScope::Span, "http.method", DedicatedType::String),
    dedicated_column(
        AttributeScope::Span,
        "http.request.method",
        DedicatedType::String,
    ),
    dedicated_column(AttributeScope::Span, "http.route", DedicatedType::String),
    dedicated_column(AttributeScope::Span, "db.system", DedicatedType::String),
    dedicated_column(AttributeScope::Span, "rpc.service", DedicatedType::String),
    dedicated_column(
        AttributeScope::Resource,
        "k8s.namespace.name",
        DedicatedType::String,
    ),
];

const fn dedicated_column(
    scope: AttributeScope,
    name: &'static str,
    kind: DedicatedType,
) -> DedicatedColumn {
    DedicatedColumn { scope, name, kind }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DedicatedTest {
    /// `value op literal` on a string column, exactly as the interpreter
    /// evaluates it.
    String { op: BinaryOp, literal: String },
    /// Whether an integer lies in `low..=low + width` (wrapping), or outside
    /// it when `inside` is false. A value of another type may still compare
    /// numerically, so it passes: the test over-approximates.
    Int { low: i64, width: u64, inside: bool },
}

/// Whether each value of a string column passes a [`DedicatedTest::String`],
/// indexed by column code: missing, another type, then each of `values`.
pub(crate) fn string_codes(op: BinaryOp, literal: &str, values: &[String]) -> Vec<bool> {
    let literal = StaticValue::String(literal.to_owned());
    let mut passes = Vec::with_capacity(values.len() + 2);
    // Only `!=` holds between a string and a value of another type.
    passes.extend([false, op == BinaryOp::NotEqual]);
    passes.extend(values.iter().map(|value| {
        super::execution::stored_value_matches(
            op,
            &crate::AttributeValue::String(value.clone()),
            &literal,
        )
    }));
    passes
}

/// The span duration exactly as the interpreter computes it.
pub(crate) fn duration_ns(span: &Span) -> i64 {
    i64::try_from(
        span.end_time_unix_nano
            .saturating_sub(span.start_time_unix_nano),
    )
    .unwrap_or(i64::MAX)
}

pub(crate) fn status_code(span: &Span) -> u8 {
    match span.status.as_ref().map_or(0, |status| status.code) {
        1 => status_value_code(StatusValue::Ok),
        2 => status_value_code(StatusValue::Error),
        _ => status_value_code(StatusValue::Unset),
    }
}

pub(crate) fn kind_code(span: &Span) -> u8 {
    match span.kind {
        kind @ 1..=5 => kind as u8,
        _ => kind_value_code(KindValue::Unspecified),
    }
}

/// Largest value [`kind_code`] returns.
pub(crate) const MAX_KIND_CODE: u8 = 5;
/// Largest value [`status_code`] returns.
pub(crate) const MAX_STATUS_CODE: u8 = 2;

fn status_value_code(status: StatusValue) -> u8 {
    match status {
        StatusValue::Unset => 0,
        StatusValue::Ok => 1,
        StatusValue::Error => 2,
    }
}

fn kind_value_code(kind: KindValue) -> u8 {
    match kind {
        KindValue::Unspecified => 0,
        KindValue::Internal => 1,
        KindValue::Server => 2,
        KindValue::Client => 3,
        KindValue::Producer => 4,
        KindValue::Consumer => 5,
    }
}

/// How `resource.service.name` resolves for one resource, as the
/// interpreter's attribute lookup sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServiceName<'a> {
    Missing,
    String(&'a str),
    /// A scalar of another type.
    Other,
}

pub(crate) fn service_name(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> ServiceName<'_> {
    match scalar(attributes, "service.name") {
        Some(any_value::Value::StringValue(value)) => ServiceName::String(value),
        Some(_) => ServiceName::Other,
        None => ServiceName::Missing,
    }
}

/// The value of the first `name` attribute when it is a scalar, which is
/// what the interpreter's attribute lookup reads; a first attribute of
/// another type hides any later one.
pub(crate) fn scalar<'a>(
    attributes: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    name: &str,
) -> Option<&'a any_value::Value> {
    attributes
        .iter()
        .find(|attribute| attribute.key == name)
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| value.value.as_ref())
        .filter(|value| {
            matches!(
                value,
                any_value::Value::StringValue(_)
                    | any_value::Value::BoolValue(_)
                    | any_value::Value::IntValue(_)
                    | any_value::Value::DoubleValue(_)
            )
        })
}

/// Compiles `expression` when its column form agrees with the interpreter on
/// every span.
pub(crate) fn compile_exact(expression: &FieldExpr, target: Target) -> Option<ColumnPredicate> {
    if target == Target::Trace && is_resource_constant(&expression.value) {
        return Some(ColumnPredicate::Constant(expression.clone()));
    }
    match &expression.value {
        // Operands of `!` always evaluate to booleans here: a non-boolean
        // constant operand would have made the whole negation constant.
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Some(ColumnPredicate::Not(Box::new(compile_exact(expr, target)?))),
        Expr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => Some(ColumnPredicate::And(
            Box::new(compile_exact(lhs, target)?),
            Box::new(compile_exact(rhs, target)?),
        )),
        Expr::Binary {
            lhs,
            op: BinaryOp::Or,
            rhs,
        } => Some(ColumnPredicate::Or(
            Box::new(compile_exact(lhs, target)?),
            Box::new(compile_exact(rhs, target)?),
        )),
        Expr::Binary { lhs, op, rhs } => compile_comparison(&lhs.value, *op, &rhs.value, target),
        _ => None,
    }
}

fn compile_comparison(
    lhs: &Expr,
    op: BinaryOp,
    rhs: &Expr,
    target: Target,
) -> Option<ColumnPredicate> {
    if target == Target::Sidecar
        && let Some((predicate, true)) = dedicated(lhs, op, rhs)
    {
        return Some(predicate);
    }
    let comparison = Comparison::from_op(op)?;
    let (field, literal, comparison) = match (lhs, rhs) {
        (field, Expr::Static(literal)) => (field, literal, comparison),
        (Expr::Static(literal), field) => (field, literal, comparison.flip()),
        _ => return None,
    };
    match (field, literal) {
        (
            Expr::Intrinsic(Intrinsic::Duration),
            StaticValue::Int(value) | StaticValue::Duration(value),
        ) => Some(ColumnPredicate::Duration(comparison, *value)),
        (Expr::Intrinsic(Intrinsic::Duration), StaticValue::Float(value)) => {
            Some(float_duration(comparison, *value))
        }
        (Expr::Intrinsic(Intrinsic::Status), StaticValue::Status(status)) => {
            Some(ColumnPredicate::Status {
                equal: comparison.equality()?,
                code: status_value_code(*status),
            })
        }
        (Expr::Intrinsic(Intrinsic::Kind), StaticValue::Kind(kind)) => {
            Some(ColumnPredicate::Kind {
                equal: comparison.equality()?,
                code: kind_value_code(*kind),
            })
        }
        (Expr::Intrinsic(Intrinsic::Name), StaticValue::String(value)) => {
            Some(ColumnPredicate::Name {
                equal: comparison.equality()?,
                value: value.clone(),
            })
        }
        (Expr::Attribute(attribute), StaticValue::String(value))
            if target == Target::Sidecar
                && attribute.scope == AttributeScope::Resource
                && attribute.name == "service.name" =>
        {
            Some(ColumnPredicate::ServiceName {
                equal: comparison.equality()?,
                value: value.clone(),
            })
        }
        _ => None,
    }
}

/// `duration as f64` compared against `literal`, as IEEE comparisons in the
/// interpreter make it: ordering against NaN is false and `!=` NaN is true.
///
/// Conversion to `f64` is monotonic, so the durations satisfying each
/// comparison, or its negation, form an interval.
fn float_duration(comparison: Comparison, literal: f64) -> ColumnPredicate {
    let (low, width, inside) = float_interval(comparison, literal);
    ColumnPredicate::DurationIn { low, width, inside }
}

/// The `(low, width, inside)` interval test of `value as f64` against
/// `literal`, for `i64` values.
fn float_interval(comparison: Comparison, literal: f64) -> (i64, u64, bool) {
    if literal.is_nan() {
        return (0, u64::MAX, comparison == Comparison::NotEqual);
    }
    interval(
        comparison,
        least(|value| value as f64 >= literal),
        least(|value| value as f64 > literal),
    )
}

/// The `(low, width, inside)` interval test of a comparison, given the
/// least values at least and above its literal.
fn interval(comparison: Comparison, at_least: Option<i64>, above: Option<i64>) -> (i64, u64, bool) {
    let (low, end, inside) = match comparison {
        Comparison::GreaterEqual => (at_least, None, true),
        Comparison::Greater => (above, None, true),
        Comparison::Less => (at_least, None, false),
        Comparison::LessEqual => (above, None, false),
        Comparison::Equal => (at_least, above, true),
        Comparison::NotEqual => (at_least, above, false),
    };
    let (low, high) = match (low, end) {
        (Some(low), None) => (low, i64::MAX),
        (Some(low), Some(end)) if end > low => (low, end - 1),
        // Every value lies in the full-width interval.
        _ => return (0, u64::MAX, !inside),
    };
    (low, high.wrapping_sub(low) as u64, inside)
}

/// The predicate on a [`DEDICATED_COLUMNS`] attribute that `lhs op rhs`
/// compiles to, and whether it is exact rather than an over-approximation.
fn dedicated(lhs: &Expr, op: BinaryOp, rhs: &Expr) -> Option<(ColumnPredicate, bool)> {
    let (attribute, literal, op) = match (lhs, rhs) {
        (Expr::Attribute(attribute), Expr::Static(literal)) => (attribute, literal, op),
        (Expr::Static(literal), Expr::Attribute(attribute)) => {
            let flipped = match op {
                BinaryOp::Less => BinaryOp::Greater,
                BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                BinaryOp::Greater => BinaryOp::Less,
                BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                BinaryOp::Equal | BinaryOp::NotEqual => op,
                _ => return None,
            };
            (attribute, literal, flipped)
        }
        _ => return None,
    };
    let column = DEDICATED_COLUMNS
        .iter()
        .position(|column| column.scope == attribute.scope && column.name == attribute.name)?;
    let test = match (DEDICATED_COLUMNS[column].kind, literal) {
        (DedicatedType::String, StaticValue::String(literal))
            if Comparison::from_op(op).is_some()
                || matches!(op, BinaryOp::Regex | BinaryOp::NotRegex) =>
        {
            DedicatedTest::String {
                op,
                literal: literal.clone(),
            }
        }
        (DedicatedType::Int, StaticValue::Int(literal) | StaticValue::Duration(literal)) => {
            let (low, width, inside) = interval(
                Comparison::from_op(op)?,
                Some(*literal),
                literal.checked_add(1),
            );
            DedicatedTest::Int { low, width, inside }
        }
        (DedicatedType::Int, StaticValue::Float(literal)) => {
            let (low, width, inside) = float_interval(Comparison::from_op(op)?, *literal);
            DedicatedTest::Int { low, width, inside }
        }
        _ => return None,
    };
    let exact = matches!(test, DedicatedTest::String { .. });
    Some((ColumnPredicate::Dedicated { column, test }, exact))
}

/// The least `i64` satisfying `test`, which holds from some value on.
fn least(test: impl Fn(i64) -> bool) -> Option<i64> {
    if !test(i64::MAX) {
        return None;
    }
    let (mut low, mut high) = (i64::MIN, i64::MAX);
    while low < high {
        let middle = ((i128::from(low) + i128::from(high)) >> 1) as i64;
        if test(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Some(low)
}

/// Whether `expression` reads only literals, resource attributes, and
/// trace-level intrinsics, so it has one value per resource.
fn is_resource_constant(expression: &Expr) -> bool {
    match expression {
        Expr::Static(_) => true,
        Expr::Attribute(attribute) => attribute.scope == AttributeScope::Resource,
        Expr::Intrinsic(intrinsic) => matches!(
            intrinsic,
            Intrinsic::TraceId
                | Intrinsic::RootName
                | Intrinsic::RootServiceName
                | Intrinsic::TraceDuration
        ),
        Expr::Unary { expr, .. } => is_resource_constant(&expr.value),
        Expr::Binary { lhs, rhs, .. } => {
            is_resource_constant(&lhs.value) && is_resource_constant(&rhs.value)
        }
    }
}

/// A predicate every span matching `expression` satisfies; `None` when no
/// such predicate is known.
fn over_approximate(expression: &FieldExpr) -> Option<ColumnPredicate> {
    if let Some(exact) = compile_exact(expression, Target::Sidecar) {
        return Some(exact);
    }
    match &expression.value {
        Expr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => match (over_approximate(lhs), over_approximate(rhs)) {
            (Some(lhs), Some(rhs)) => Some(ColumnPredicate::And(Box::new(lhs), Box::new(rhs))),
            (lhs, rhs) => lhs.or(rhs),
        },
        Expr::Binary {
            lhs,
            op: BinaryOp::Or,
            rhs,
        } => Some(ColumnPredicate::Or(
            Box::new(over_approximate(lhs)?),
            Box::new(over_approximate(rhs)?),
        )),
        Expr::Binary { lhs, op, rhs } => {
            dedicated(&lhs.value, *op, &rhs.value).map(|(predicate, _)| predicate)
        }
        _ => None,
    }
}

/// A condition on a trace's spans that every trace a query returns meets.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TraceFilter {
    /// Some span satisfies the predicate.
    Exists(ColumnPredicate),
    All(Vec<TraceFilter>),
    Any(Vec<TraceFilter>),
}

impl TraceFilter {
    /// The `Exists` predicates, depth first.
    pub(crate) fn leaves(&self) -> Vec<&ColumnPredicate> {
        let mut leaves = Vec::new();
        self.collect_leaves(&mut leaves);
        leaves
    }

    fn collect_leaves<'a>(&'a self, leaves: &mut Vec<&'a ColumnPredicate>) {
        match self {
            Self::Exists(predicate) => leaves.push(predicate),
            Self::All(filters) | Self::Any(filters) => {
                for filter in filters {
                    filter.collect_leaves(leaves);
                }
            }
        }
    }

    /// Evaluates the filter given whether each leaf, in [`Self::leaves`]
    /// order, has a satisfying span.
    pub(crate) fn matches(&self, exists: &[bool]) -> bool {
        let mut next = 0;
        let matched = self.matches_from(exists, &mut next);
        debug_assert_eq!(next, exists.len());
        matched
    }

    fn matches_from(&self, exists: &[bool], next: &mut usize) -> bool {
        match self {
            Self::Exists(_) => {
                *next += 1;
                exists[*next - 1]
            }
            // Every child must run so `next` advances past its leaves.
            Self::All(filters) => {
                let mut all = true;
                for filter in filters {
                    all &= filter.matches_from(exists, next);
                }
                all
            }
            Self::Any(filters) => {
                let mut any = false;
                for filter in filters {
                    any |= filter.matches_from(exists, next);
                }
                any
            }
        }
    }
}

/// A necessary condition for `query` to return a trace, over intrinsic
/// columns a page sidecar holds; `None` when every trace may match.
///
/// Spanset evaluation and pipeline stages only ever narrow the spans a
/// filter selected, so each requirement below holds for the final result.
pub(crate) fn prefilter(query: &Query) -> Option<TraceFilter> {
    let mut required = Vec::new();
    required.extend(spanset_requirement(&query.spanset.value));
    for stage in &query.stages {
        if let PipelineStage::SpansetFilter(expression) = stage {
            required.extend(over_approximate(expression).map(TraceFilter::Exists));
        }
    }
    match required.len() {
        0 => None,
        1 => required.pop(),
        _ => Some(TraceFilter::All(required)),
    }
}

/// What decides an [`existential`](super::existential) query over a page's
/// columns: a trace matches exactly when some span of it satisfies the
/// predicate, or, without one, always.
pub(crate) type Decider = Option<ColumnPredicate>;

/// The [`Decider`] of `query`, when it is existential and every filter has
/// an exact sidecar form.
pub(crate) fn decider(query: &Query) -> Option<Decider> {
    if !super::existential(query) {
        return None;
    }
    let mut decider = spanset_decider(&query.spanset.value)?;
    for stage in &query.stages {
        let PipelineStage::SpansetFilter(expression) = stage else {
            return None;
        };
        decider = conjoin(decider, filter_decider(expression)?);
    }
    Some(decider)
}

fn spanset_decider(expression: &SpansetExpr) -> Option<Decider> {
    match expression {
        SpansetExpr::Filter(expression) => filter_decider(expression),
        SpansetExpr::Binary {
            lhs,
            op: StructuralOp::Union,
            rhs,
        } => Some(
            spanset_decider(&lhs.value)?
                .zip(spanset_decider(&rhs.value)?)
                .map(|(lhs, rhs)| ColumnPredicate::Or(Box::new(lhs), Box::new(rhs))),
        ),
        SpansetExpr::Binary { .. } => None,
    }
}

fn filter_decider(expression: &FieldExpr) -> Option<Decider> {
    match &expression.value {
        Expr::Static(StaticValue::Bool(true)) => Some(None),
        _ => compile_exact(expression, Target::Sidecar).map(Some),
    }
}

fn conjoin(lhs: Decider, rhs: Decider) -> Decider {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(ColumnPredicate::And(Box::new(lhs), Box::new(rhs))),
        (lhs, rhs) => lhs.or(rhs),
    }
}

/// A condition every trace with a non-empty result for `expression` meets.
fn spanset_requirement(expression: &SpansetExpr) -> Option<TraceFilter> {
    match expression {
        SpansetExpr::Filter(expression) => over_approximate(expression).map(TraceFilter::Exists),
        SpansetExpr::Binary { lhs, op, rhs } => {
            let lhs = spanset_requirement(&lhs.value);
            let rhs = spanset_requirement(&rhs.value);
            match op {
                StructuralOp::Union => Some(TraceFilter::Any(vec![lhs?, rhs?])),
                // Negated relations keep right spans related to no left span,
                // so the left side may be empty.
                StructuralOp::NotChild
                | StructuralOp::NotParent
                | StructuralOp::NotDescendant
                | StructuralOp::NotAncestor
                | StructuralOp::NotSibling => rhs,
                // Every other operator yields spans only when both sides
                // have some.
                _ => match (lhs, rhs) {
                    (Some(lhs), Some(rhs)) => Some(TraceFilter::All(vec![lhs, rhs])),
                    (lhs, rhs) => lhs.or(rhs),
                },
            }
        }
    }
}

/// Evaluates `predicate` for every span of `source`; `None` when the source
/// cannot evaluate one of its leaves.
pub(crate) fn evaluate(
    predicate: &ColumnPredicate,
    source: &impl ColumnSource,
) -> Option<Vec<bool>> {
    let mut out = vec![false; source.span_count()];
    fill(predicate, source, &mut out)?;
    Some(out)
}

fn fill(predicate: &ColumnPredicate, source: &impl ColumnSource, out: &mut [bool]) -> Option<()> {
    match predicate {
        ColumnPredicate::Duration(comparison, literal) => {
            compare_ints(source.durations(), *comparison, *literal, out);
        }
        ColumnPredicate::DurationIn { low, width, inside } => {
            within(source.durations(), *low, *width, *inside, out);
        }
        ColumnPredicate::Status { equal, code } => {
            compare_codes(source.statuses(), *equal, *code, out);
        }
        ColumnPredicate::Kind { equal, code } => compare_codes(source.kinds(), *equal, *code, out),
        ColumnPredicate::Name { equal, value } => {
            source.names_equal(value, out);
            if !equal {
                negate(out);
            }
        }
        ColumnPredicate::ServiceName { equal, value } => {
            source.service_names(*equal, value, out)?;
        }
        ColumnPredicate::Dedicated { column, test } => source.dedicated(*column, test, out)?,
        ColumnPredicate::Constant(expression) => source.constant(expression, out)?,
        ColumnPredicate::Not(inner) => {
            fill(inner, source, out)?;
            negate(out);
        }
        ColumnPredicate::And(lhs, rhs) => {
            fill(lhs, source, out)?;
            let mut right = vec![false; out.len()];
            fill(rhs, source, &mut right)?;
            for (out, right) in out.iter_mut().zip(&right) {
                *out &= *right;
            }
        }
        ColumnPredicate::Or(lhs, rhs) => {
            fill(lhs, source, out)?;
            let mut right = vec![false; out.len()];
            fill(rhs, source, &mut right)?;
            for (out, right) in out.iter_mut().zip(&right) {
                *out |= *right;
            }
        }
    }
    Some(())
}

fn negate(out: &mut [bool]) {
    for value in out {
        *value = !*value;
    }
}

fn compare_ints(values: &[i64], comparison: Comparison, literal: i64, out: &mut [bool]) {
    let pairs = out.iter_mut().zip(values);
    match comparison {
        Comparison::Equal => pairs.for_each(|(out, &value)| *out = value == literal),
        Comparison::NotEqual => pairs.for_each(|(out, &value)| *out = value != literal),
        Comparison::Less => pairs.for_each(|(out, &value)| *out = value < literal),
        Comparison::LessEqual => pairs.for_each(|(out, &value)| *out = value <= literal),
        Comparison::Greater => pairs.for_each(|(out, &value)| *out = value > literal),
        Comparison::GreaterEqual => pairs.for_each(|(out, &value)| *out = value >= literal),
    }
}

/// One unsigned comparison per value, which vectorizes even without 64-bit
/// signed vector compares (x86 SSE2).
fn within(values: &[i64], low: i64, width: u64, inside: bool, out: &mut [bool]) {
    for (out, &value) in out.iter_mut().zip(values) {
        *out = (value.wrapping_sub(low) as u64 <= width) == inside;
    }
}

/// Whether any entry is set, scanning fixed-size blocks the compiler
/// vectorizes instead of exiting early byte by byte.
pub(crate) fn any_set(mask: &[bool]) -> bool {
    const BLOCK: usize = 32;
    let (blocks, tail) = mask.as_chunks::<BLOCK>();
    blocks
        .iter()
        .any(|block| block.iter().fold(false, |any, &set| any | set))
        || tail.iter().fold(false, |any, &set| any | set)
}

fn compare_codes(values: &[u8], equal: bool, code: u8, out: &mut [bool]) {
    let pairs = out.iter_mut().zip(values);
    if equal {
        pairs.for_each(|(out, &value)| *out = value == code);
    } else {
        pairs.for_each(|(out, &value)| *out = value != code);
    }
}

/// Random traces and queries exercising every column leaf and its edges.
#[cfg(test)]
pub(crate) mod testing {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
    use proptest::prelude::*;

    use crate::{Trace, TraceId};

    const DURATIONS: [u64; 8] = [
        0,
        1,
        999,
        1_000,
        1_000_000,
        5_000_000,
        1_000_000_000,
        u64::MAX,
    ];

    #[derive(Clone, Debug)]
    struct SpanSpec {
        parent: Option<usize>,
        name: usize,
        duration: usize,
        status: i32,
        kind: i32,
        http_status: Option<i64>,
        /// Indexes into [`dedicated_attributes`].
        route: usize,
        status_code: usize,
        method: usize,
    }

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn span_spec() -> impl Strategy<Value = SpanSpec> {
        (
            proptest::option::of(0_usize..8),
            0_usize..3,
            0..DURATIONS.len(),
            0_i32..5,
            0_i32..7,
            proptest::option::of(prop_oneof![Just(200_i64), Just(500)]),
            (0_usize..7, 0_usize..9, 0_usize..5),
        )
            .prop_map(
                |(
                    parent,
                    name,
                    duration,
                    status,
                    kind,
                    http_status,
                    (route, status_code, method),
                )| {
                    SpanSpec {
                        parent,
                        name,
                        duration,
                        status,
                        kind,
                        http_status,
                        route,
                        status_code,
                        method,
                    }
                },
            )
    }

    /// Choice `choice` of values for a dedicated attribute `key`: missing,
    /// typed as its column, of another type, repeated (the first counts),
    /// or hidden behind a first value the interpreter cannot read.
    fn dedicated_attributes(key: &str, choice: usize) -> Vec<KeyValue> {
        use any_value::Value;

        let string = |value: &str| kv(key, Value::StringValue(value.to_owned()));
        let int = |value: i64| kv(key, Value::IntValue(value));
        let unreadable = || kv(key, Value::BytesValue(vec![1]));
        match (key, choice) {
            (_, 0) => Vec::new(),
            ("http.status_code", choice) => match choice {
                1 => vec![int(200)],
                2 => vec![int(500)],
                3 => vec![int(503)],
                4 => vec![kv(key, Value::DoubleValue(500.0))],
                5 => vec![string("500")],
                6 => vec![int(200), int(500)],
                7 => vec![unreadable(), int(500)],
                _ => vec![int(i64::MIN)],
            },
            ("http.method", choice) => match choice {
                1 => vec![string("GET")],
                2 => vec![string("POST")],
                3 => vec![kv(key, Value::BoolValue(true))],
                _ => vec![string("GET"), string("POST")],
            },
            (_, choice) => match choice {
                1 => vec![string("/a")],
                2 => vec![string("/b/1")],
                3 => vec![int(1)],
                4 => vec![string("/a"), string("/b/1")],
                5 => vec![unreadable(), string("/a")],
                _ => vec![string("")],
            },
        }
    }

    /// `service.name` as a string, missing, or of another type.
    fn service() -> impl Strategy<Value = Option<any_value::Value>> {
        prop_oneof![
            Just(Some(any_value::Value::StringValue("a".to_owned()))),
            Just(Some(any_value::Value::StringValue("b".to_owned()))),
            Just(Some(any_value::Value::IntValue(7))),
            Just(None),
        ]
    }

    pub(crate) fn trace(id: u8) -> impl Strategy<Value = Trace> {
        prop::collection::vec(
            (
                (service(), 0_usize..7),
                prop::collection::vec(prop::collection::vec(span_spec(), 0..5), 1..3),
            ),
            1..4,
        )
        .prop_filter("a trace needs a span", |resources| {
            resources
                .iter()
                .flat_map(|(_, scopes)| scopes)
                .flatten()
                .next()
                .is_some()
        })
        .prop_map(move |resources| build(id, resources))
    }

    type ResourceSpec = ((Option<any_value::Value>, usize), Vec<Vec<SpanSpec>>);

    fn build(id: u8, resources: Vec<ResourceSpec>) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        let names = ["get", "put", "del"];
        let mut position = 0_u8;
        let resource_spans = resources
            .into_iter()
            .map(|((service, namespace), scopes)| {
                let mut attributes = vec![kv(
                    "tier",
                    any_value::Value::StringValue(
                        if position.is_multiple_of(2) { "x" } else { "y" }.into(),
                    ),
                )];
                attributes.extend(service.map(|value| kv("service.name", value)));
                attributes.extend(dedicated_attributes("k8s.namespace.name", namespace));
                ResourceSpans {
                    resource: Some(Resource {
                        attributes,
                        ..Default::default()
                    }),
                    scope_spans: scopes
                        .into_iter()
                        .map(|spans| ScopeSpans {
                            spans: spans
                                .into_iter()
                                .map(|spec| {
                                    position += 1;
                                    let duration = DURATIONS[spec.duration];
                                    let start = if duration == u64::MAX {
                                        0
                                    } else {
                                        1_000 + u64::from(position)
                                    };
                                    Span {
                                        trace_id: trace_id.as_bytes().to_vec(),
                                        span_id: vec![position; 8],
                                        parent_span_id: spec
                                            .parent
                                            .filter(|&parent| parent + 1 < usize::from(position))
                                            .map(|parent| vec![parent as u8 + 1; 8])
                                            .unwrap_or_default(),
                                        name: names[spec.name].to_owned(),
                                        kind: spec.kind,
                                        start_time_unix_nano: start,
                                        end_time_unix_nano: start.saturating_add(duration),
                                        attributes: spec
                                            .http_status
                                            .map(|code| {
                                                kv("http.status", any_value::Value::IntValue(code))
                                            })
                                            .into_iter()
                                            .chain(dedicated_attributes("http.route", spec.route))
                                            .chain(dedicated_attributes(
                                                "http.status_code",
                                                spec.status_code,
                                            ))
                                            .chain(dedicated_attributes("http.method", spec.method))
                                            .collect(),
                                        status: (spec.status > 0).then(|| Status {
                                            code: spec.status - 1,
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();
        Trace::new(trace_id, resource_spans).unwrap()
    }

    fn comparison() -> impl Strategy<Value = &'static str> {
        prop::sample::select(vec!["=", "!=", "<", "<=", ">", ">="])
    }

    fn equality() -> impl Strategy<Value = &'static str> {
        prop::sample::select(vec!["=", "!="])
    }

    /// Comparisons of the dedicated attributes [`trace`] generates.
    pub(crate) fn dedicated_leaf() -> impl Strategy<Value = String> {
        let string_ops = prop::sample::select(vec!["=", "!=", "<", "<=", ">", ">=", "=~", "!~"]);
        let strings = prop::sample::select(vec!["/a", "/b/1", "", "zzz", "/b.*", "/a|zzz"]);
        prop_oneof![
            (string_ops.clone(), strings.clone())
                .prop_map(|(op, value)| format!("span.http.route {op} \"{value}\"")),
            (string_ops, strings.clone()).prop_map(|(op, value)| {
                format!("resource.k8s.namespace.name {op} \"{value}\"")
            }),
            (comparison(), strings)
                .prop_map(|(op, value)| format!("\"{value}\" {op} span.http.route")),
            (
                comparison(),
                prop::sample::select(vec!["GET", "POST", "PUT"])
            )
                .prop_map(|(op, value)| format!("span.http.method {op} \"{value}\"")),
            (
                comparison(),
                prop::sample::select(vec![
                    "200",
                    "500",
                    "503",
                    "499.5",
                    "500.0",
                    "1ms",
                    "0",
                    "9223372036854775807",
                ])
            )
                .prop_map(|(op, value)| format!("span.http.status_code {op} {value}")),
            (comparison(), prop::sample::select(vec!["200", "499.5"]))
                .prop_map(|(op, value)| format!("{value} {op} span.http.status_code")),
            Just("span.http.status_code = \"500\"".to_owned()),
            Just("span.http.response.status_code >= 500".to_owned()),
            Just("span.http.request.method = \"GET\"".to_owned()),
            Just(".http.route = \"/a\"".to_owned()),
        ]
    }

    fn leaf() -> impl Strategy<Value = String> {
        let durations = prop::sample::select(vec![
            "0", "1000", "1ns", "1us", "999ns", "1ms", "5ms", "1s", "1.5", "1000.0",
        ]);
        prop_oneof![
            dedicated_leaf(),
            dedicated_leaf(),
            dedicated_leaf(),
            (comparison(), durations.clone())
                .prop_map(|(op, value)| format!("duration {op} {value}")),
            (comparison(), durations).prop_map(|(op, value)| format!("{value} {op} duration")),
            (
                equality(),
                prop::sample::select(vec!["unset", "ok", "error"])
            )
                .prop_map(|(op, value)| format!("status {op} {value}")),
            (
                equality(),
                prop::sample::select(vec![
                    "unspecified",
                    "internal",
                    "server",
                    "client",
                    "producer",
                    "consumer"
                ])
            )
                .prop_map(|(op, value)| format!("kind {op} {value}")),
            (equality(), prop::sample::select(vec!["get", "put", "nope"]))
                .prop_map(|(op, value)| format!("name {op} \"{value}\"")),
            Just("name =~ \"g.*\"".to_owned()),
            Just("name !~ \"p.*\"".to_owned()),
            (equality(), prop::sample::select(vec!["a", "b", "z"]))
                .prop_map(|(op, value)| format!("resource.service.name {op} \"{value}\"")),
            (equality(), prop::sample::select(vec!["a", "b"]))
                .prop_map(|(op, value)| format!(".service.name {op} \"{value}\"")),
            (equality(), prop::sample::select(vec!["200", "500"]))
                .prop_map(|(op, value)| format!("span.http.status {op} {value}")),
            Just("span.http.status = nil".to_owned()),
            Just("resource.tier = \"x\"".to_owned()),
            Just("trace:duration > 1ms".to_owned()),
            Just("trace:rootService = \"a\"".to_owned()),
            Just("true".to_owned()),
            Just("false".to_owned()),
        ]
    }

    pub(crate) fn filter() -> impl Strategy<Value = String> {
        leaf().prop_recursive(3, 12, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone()).prop_map(|(lhs, rhs)| format!("({lhs}) && ({rhs})")),
                (inner.clone(), inner.clone()).prop_map(|(lhs, rhs)| format!("({lhs}) || ({rhs})")),
                inner.prop_map(|expression| format!("!({expression})")),
            ]
        })
    }

    pub(crate) fn query() -> impl Strategy<Value = String> {
        let structural = prop::sample::select(vec![
            "&&", "||", ">", "<", ">>", "<<", "~", "!>", "!<", "!>>", "!<<", "!~", "&>", "&<",
            "&>>", "&<<", "&~",
        ]);
        prop_oneof![
            filter().prop_map(|filter| format!("{{ {filter} }}")),
            (filter(), filter()).prop_map(|(lhs, rhs)| format!("{{ {lhs} }} | {{ {rhs} }}")),
            (filter(), structural, filter())
                .prop_map(|(lhs, op, rhs)| format!("{{ {lhs} }} {op} {{ {rhs} }}")),
            filter().prop_map(|filter| format!("{{ {filter} }} | avg(duration) > 1ms")),
            filter().prop_map(|filter| format!("{{ {filter} }} | max(duration) >= 1s")),
            filter().prop_map(|filter| format!("{{ {filter} }} | sum(duration) < 2ms")),
            filter().prop_map(|filter| format!("{{ {filter} }} | count() < 2")),
            filter().prop_map(|filter| format!("{{ {filter} }} | by(resource.service.name)")),
            filter().prop_map(|filter| format!("{{ {filter} }} | select(duration, status)")),
            Just("{ }".to_owned()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::traceql::{CompiledQuery, execute_compiled, parse};

    const COMPARISONS: [Comparison; 6] = [
        Comparison::Equal,
        Comparison::NotEqual,
        Comparison::Less,
        Comparison::LessEqual,
        Comparison::Greater,
        Comparison::GreaterEqual,
    ];

    /// Literals at the edges of `i64` to `f64` conversion, with durations at
    /// and around the literal.
    fn float_case() -> impl Strategy<Value = (f64, Vec<i64>)> {
        let literal = prop_oneof![
            any::<f64>(),
            any::<i64>().prop_map(|value| value as f64),
            prop::sample::select(vec![
                0.0,
                -0.0,
                0.5,
                1.5,
                -1.5,
                1_000.0,
                9_007_199_254_740_992.0,
                9_007_199_254_740_993.5,
                9_223_372_036_854_775_807.0,
                -9_223_372_036_854_775_808.0,
                1e300,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
            ]),
        ];
        literal.prop_flat_map(|literal| {
            let near =
                (-1_100_i64..1_100).prop_map(move |delta| (literal as i64).saturating_add(delta));
            let value = prop_oneof![
                near,
                any::<i64>(),
                prop::sample::select(vec![i64::MIN, -1, 0, 1, i64::MAX])
            ];
            (Just(literal), prop::collection::vec(value, 1..80))
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]

        #[test]
        fn float_durations_compile_to_exact_intervals((literal, values) in float_case()) {
            for comparison in COMPARISONS {
                let ColumnPredicate::DurationIn { low, width, inside } =
                    float_duration(comparison, literal)
                else {
                    unreachable!();
                };
                let mut found = vec![false; values.len()];
                within(&values, low, width, inside, &mut found);
                let expected = values
                    .iter()
                    .map(|&value| {
                        let value = value as f64;
                        match comparison {
                            Comparison::Equal => value == literal,
                            Comparison::NotEqual => value != literal,
                            Comparison::Less => value < literal,
                            Comparison::LessEqual => value <= literal,
                            Comparison::Greater => value > literal,
                            Comparison::GreaterEqual => value >= literal,
                        }
                    })
                    .collect::<Vec<_>>();
                prop_assert_eq!(found, expected, "{:?} {}", comparison, literal);
            }
        }

        #[test]
        fn any_set_matches_contains(
            mask in prop::collection::vec(any::<bool>(), 0..200),
            sparse in any::<bool>(),
        ) {
            let mask = if sparse {
                mask.iter()
                    .enumerate()
                    .map(|(index, &set)| set && index % 61 == 7)
                    .collect()
            } else {
                mask
            };
            prop_assert_eq!(any_set(&mask), mask.contains(&true));
        }

        #[test]
        fn column_execution_matches_the_interpreter(
            traces in prop::collection::vec(testing::trace(1), 1..4),
            source in testing::query(),
        ) {
            let Ok(query) = parse(&source) else {
                return Ok(());
            };
            let columnar = CompiledQuery::new(query.clone());
            let interpreted = CompiledQuery::interpreted(query);
            for trace in &traces {
                let expected = execute_compiled(trace, &interpreted, 1_000);
                let found = execute_compiled(trace, &columnar, 1_000);
                prop_assert_eq!(
                    format!("{found:?}"),
                    format!("{expected:?}"),
                    "query {}", source
                );
            }
        }
    }

    #[test]
    fn intrinsic_filters_compile_to_columns() {
        for source in [
            "{ duration > 100ms }",
            "{ 5ms <= duration }",
            "{ duration != 1.5 }",
            "{ status = error }",
            "{ kind != server }",
            "{ name = \"get\" }",
            "{ !(status = ok) || duration < 1s }",
            "{ resource.service.name = \"a\" && duration > 50ms }",
            "{ }",
        ] {
            let query = parse(source).unwrap();
            let SpansetExpr::Filter(expression) = &query.spanset.value else {
                unreachable!();
            };
            assert!(
                compile_exact(expression, Target::Trace).is_some(),
                "{source} should compile"
            );
        }
        for source in [
            "{ span.http.status = 500 }",
            "{ name =~ \"g.*\" }",
            "{ duration * 2 > 1ms }",
            "{ span.http.status = nil }",
        ] {
            let query = parse(source).unwrap();
            let SpansetExpr::Filter(expression) = &query.spanset.value else {
                unreachable!();
            };
            assert!(
                compile_exact(expression, Target::Trace).is_none(),
                "{source} should fall back"
            );
        }
    }

    #[test]
    fn prefilter_is_conservative_for_negations_and_structure() {
        let filter = |source: &str| prefilter(&parse(source).unwrap());
        assert_eq!(filter("{ }"), None);
        assert_eq!(filter("{ span.http.status = 500 }"), None);
        assert_eq!(
            filter("{ !(span.http.status = 500 && duration > 1s) }"),
            None
        );
        assert_eq!(filter("{ name !~ \"p.*\" }"), None);
        assert!(filter("{ span.http.status = 500 } !> { status = error }").is_some());
        assert_eq!(
            filter("{ status = error } !> { span.http.status = 500 }"),
            None
        );
        assert_eq!(
            filter("{ status = error } || { span.http.status = 500 }"),
            None
        );
        assert_eq!(
            filter("{ span.http.status = 500 && duration > 1s }"),
            Some(TraceFilter::Exists(ColumnPredicate::Duration(
                Comparison::Greater,
                1_000_000_000
            )))
        );
        assert_eq!(
            filter("{ duration > 1s } | count() < 1"),
            filter("{ duration > 1s }")
        );
    }

    #[test]
    fn dedicated_attributes_prefilter_only_where_conservative() {
        let filter = |source: &str| prefilter(&parse(source).unwrap());
        let route = DEDICATED_COLUMNS
            .iter()
            .position(|column| column.name == "http.route")
            .unwrap();
        assert_eq!(
            filter(r#"{ !(span.http.route =~ "/api/.*") }"#),
            Some(TraceFilter::Exists(ColumnPredicate::Not(Box::new(
                ColumnPredicate::Dedicated {
                    column: route,
                    test: DedicatedTest::String {
                        op: BinaryOp::Regex,
                        literal: "/api/.*".to_owned(),
                    },
                }
            ))))
        );
        assert!(matches!(
            filter("{ span.http.status_code >= 500 && duration > 1s }"),
            Some(TraceFilter::Exists(ColumnPredicate::And(..)))
        ));
        assert!(matches!(
            filter("{ 500.5 > span.http.status_code }"),
            Some(TraceFilter::Exists(ColumnPredicate::Dedicated {
                test: DedicatedTest::Int { .. },
                ..
            }))
        ));
        for source in [
            "{ !(span.http.status_code >= 500) }",
            r#"{ .http.route = "/a" }"#,
            r#"{ resource.http.route = "/a" }"#,
            r#"{ span.http.route = 1 }"#,
            r#"{ span.http.status_code = "500" }"#,
            r#"{ "/a.*" =~ span.http.route }"#,
            "{ span.http.route = nil }",
        ] {
            assert_eq!(filter(source), None, "{source}");
        }
        let query = parse(r#"{ span.http.route = "/a" }"#).unwrap();
        let SpansetExpr::Filter(expression) = &query.spanset.value else {
            unreachable!();
        };
        assert_eq!(compile_exact(expression, Target::Trace), None);
    }
}
