use regex::Regex;

use super::ast::{
    BinaryOp, Expr, FieldExpr, PipelineStage, Query, ScalarExpr, SourceSpan, SpansetExpr,
    StaticValue, UnaryOp,
};
use super::error::ValidationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueType {
    Nil,
    String,
    Bool,
    Int,
    Float,
    Duration,
    Status,
    Kind,
    Dynamic,
}

pub(crate) fn validate(query: &Query) -> Result<(), ValidationError> {
    validate_spanset(&query.spanset.value)?;
    for stage in &query.stages {
        match stage {
            PipelineStage::SpansetFilter(expression) => validate_filter(expression)?,
            PipelineStage::By(field) => {
                infer(field)?;
                require_span_reference(field, "grouping")?;
            }
            PipelineStage::Select(fields) => {
                for field in fields {
                    if !matches!(field.value, Expr::Attribute(_) | Expr::Intrinsic(_)) {
                        return Err(ValidationError::new(
                            "select fields must be attributes or intrinsics",
                            field.span,
                        ));
                    }
                }
            }
            PipelineStage::ScalarFilter { lhs, op, rhs, span } => {
                validate_scalar_filter(lhs, *op, rhs, *span)?
            }
            PipelineStage::Coalesce | PipelineStage::Metric { .. } => {}
        }
    }
    Ok(())
}

fn validate_spanset(expression: &SpansetExpr) -> Result<(), ValidationError> {
    match expression {
        SpansetExpr::Filter(expression) => validate_filter(expression)?,
        SpansetExpr::Binary { lhs, rhs, .. } => {
            validate_spanset(&lhs.value)?;
            validate_spanset(&rhs.value)?;
        }
    }
    Ok(())
}

fn validate_filter(expression: &FieldExpr) -> Result<(), ValidationError> {
    let found = infer(expression)?;
    if !matches!(found, ValueType::Bool | ValueType::Dynamic) {
        return Err(ValidationError::new(
            "span filter must evaluate to boolean",
            expression.span,
        ));
    }
    Ok(())
}

fn references_span(expression: &FieldExpr) -> bool {
    match &expression.value {
        Expr::Attribute(_) | Expr::Intrinsic(_) => true,
        Expr::Static(_) => false,
        Expr::Unary { expr, .. } => references_span(expr),
        Expr::Binary { lhs, rhs, .. } => references_span(lhs) || references_span(rhs),
    }
}

fn require_span_reference(expression: &FieldExpr, what: &str) -> Result<(), ValidationError> {
    if references_span(expression) {
        Ok(())
    } else {
        Err(ValidationError::new(
            format!("{what} field expressions must reference the span"),
            expression.span,
        ))
    }
}

/// Intrinsics and `resource.service.name` always exist, so `= nil` on them
/// can never match.
fn never_nil(expression: &FieldExpr) -> bool {
    match &expression.value {
        Expr::Intrinsic(_) => true,
        Expr::Attribute(attribute) => {
            attribute.scope == super::AttributeScope::Resource && attribute.name == "service.name"
        }
        _ => false,
    }
}

fn infer(expression: &FieldExpr) -> Result<ValueType, ValidationError> {
    match &expression.value {
        Expr::Static(value) => Ok(static_type(value)),
        Expr::Attribute(_) => Ok(ValueType::Dynamic),
        Expr::Intrinsic(intrinsic) => Ok(match intrinsic {
            super::Intrinsic::TraceId
            | super::Intrinsic::SpanId
            | super::Intrinsic::ParentId
            | super::Intrinsic::Name
            | super::Intrinsic::StatusMessage
            | super::Intrinsic::RootName
            | super::Intrinsic::RootServiceName
            | super::Intrinsic::InstrumentationName
            | super::Intrinsic::InstrumentationVersion => ValueType::String,
            super::Intrinsic::Duration | super::Intrinsic::TraceDuration => ValueType::Duration,
            super::Intrinsic::Status => ValueType::Status,
            super::Intrinsic::Kind => ValueType::Kind,
            super::Intrinsic::ChildCount
            | super::Intrinsic::NestedSetLeft
            | super::Intrinsic::NestedSetRight
            | super::Intrinsic::NestedSetParent => ValueType::Int,
        }),
        Expr::Unary { op, expr } => {
            let found = infer(expr)?;
            match op {
                UnaryOp::Not if compatible(found, ValueType::Bool) => Ok(ValueType::Bool),
                UnaryOp::Neg if numeric(found) => Ok(found),
                UnaryOp::Not => Err(ValidationError::new(
                    format!("logical not requires bool, found {found:?}"),
                    expression.span,
                )),
                UnaryOp::Neg => Err(ValidationError::new(
                    format!("negation requires numeric or duration, found {found:?}"),
                    expression.span,
                )),
            }
        }
        Expr::Binary { lhs, op, rhs } => {
            let left = infer(lhs)?;
            let right = infer(rhs)?;
            match op {
                BinaryOp::And | BinaryOp::Or => {
                    require_compatible(left, ValueType::Bool, lhs.span)?;
                    require_compatible(right, ValueType::Bool, rhs.span)?;
                    Ok(ValueType::Bool)
                }
                BinaryOp::Regex | BinaryOp::NotRegex => {
                    require_compatible(left, ValueType::String, lhs.span)?;
                    require_compatible(right, ValueType::String, rhs.span)?;
                    if let Expr::Static(StaticValue::String(pattern)) = &rhs.value {
                        Regex::new(pattern).map_err(|error| {
                            ValidationError::new(format!("invalid regex: {error}"), rhs.span)
                        })?;
                    }
                    Ok(ValueType::Bool)
                }
                BinaryOp::Equal | BinaryOp::NotEqual => {
                    if *op == BinaryOp::Equal
                        && ((left == ValueType::Nil && never_nil(rhs))
                            || (right == ValueType::Nil && never_nil(lhs)))
                    {
                        return Err(ValidationError::new(
                            "intrinsics and resource.service.name cannot be nil",
                            expression.span,
                        ));
                    }
                    if left != ValueType::Dynamic
                        && right != ValueType::Dynamic
                        && left != ValueType::Nil
                        && right != ValueType::Nil
                        && !comparable(left, right)
                    {
                        return Err(ValidationError::new(
                            format!("cannot compare {left:?} and {right:?}"),
                            expression.span,
                        ));
                    }
                    Ok(ValueType::Bool)
                }
                BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => {
                    if !ordered(left) || !ordered(right) || !comparable(left, right) {
                        return Err(ValidationError::new(
                            format!("cannot order {left:?} and {right:?}"),
                            expression.span,
                        ));
                    }
                    Ok(ValueType::Bool)
                }
                BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::Mod
                | BinaryOp::Pow => arithmetic_type(left, right).ok_or_else(|| {
                    ValidationError::new(
                        format!("invalid arithmetic between {left:?} and {right:?}"),
                        expression.span,
                    )
                }),
            }
        }
    }
}

fn validate_scalar_filter(
    lhs: &ScalarExpr,
    op: BinaryOp,
    rhs: &ScalarExpr,
    span: SourceSpan,
) -> Result<(), ValidationError> {
    if !matches!(
        op,
        BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::Less
            | BinaryOp::LessEqual
            | BinaryOp::Greater
            | BinaryOp::GreaterEqual
    ) {
        return Err(ValidationError::new(
            "aggregate filter requires comparison",
            span,
        ));
    }
    let left = scalar_type(lhs)?;
    let right = scalar_type(rhs)?;
    if !comparable(left, right) {
        return Err(ValidationError::new(
            format!("cannot compare aggregate scalar {left:?} and {right:?}"),
            span,
        ));
    }
    Ok(())
}

fn scalar_type(expression: &ScalarExpr) -> Result<ValueType, ValidationError> {
    match expression {
        ScalarExpr::Static(value) => Ok(static_type(value)),
        ScalarExpr::Aggregate { op, field } => {
            if *op == super::AggregateOp::Count {
                if field.is_some() {
                    return Err(ValidationError::new(
                        "count() does not accept a field",
                        field.as_ref().unwrap().span,
                    ));
                }
                Ok(ValueType::Int)
            } else {
                let field = field.as_ref().ok_or_else(|| {
                    ValidationError::new(
                        "numeric aggregate requires a field",
                        SourceSpan::default(),
                    )
                })?;
                let found = infer(field)?;
                require_span_reference(field, "aggregate")?;
                if numeric(found) {
                    Ok(
                        if *op == super::AggregateOp::Avg && found != ValueType::Duration {
                            ValueType::Float
                        } else {
                            found
                        },
                    )
                } else {
                    Err(ValidationError::new(
                        "numeric aggregate requires numeric field",
                        field.span,
                    ))
                }
            }
        }
        ScalarExpr::Binary { lhs, op, rhs } => {
            let left = scalar_type(lhs)?;
            let right = scalar_type(rhs)?;
            if !matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod
                    | BinaryOp::Pow
            ) {
                return Err(ValidationError::new(
                    "invalid scalar arithmetic operator",
                    SourceSpan::default(),
                ));
            }
            arithmetic_type(left, right).ok_or_else(|| {
                ValidationError::new("invalid scalar arithmetic types", SourceSpan::default())
            })
        }
    }
}

fn static_type(value: &StaticValue) -> ValueType {
    match value {
        StaticValue::Nil => ValueType::Nil,
        StaticValue::String(_) => ValueType::String,
        StaticValue::Bool(_) => ValueType::Bool,
        StaticValue::Int(_) => ValueType::Int,
        StaticValue::Float(_) => ValueType::Float,
        StaticValue::Duration(_) => ValueType::Duration,
        StaticValue::Status(_) => ValueType::Status,
        StaticValue::Kind(_) => ValueType::Kind,
    }
}

fn compatible(found: ValueType, expected: ValueType) -> bool {
    found == ValueType::Dynamic || found == expected
}

fn require_compatible(
    found: ValueType,
    expected: ValueType,
    span: SourceSpan,
) -> Result<(), ValidationError> {
    if compatible(found, expected) {
        Ok(())
    } else {
        Err(ValidationError::new(
            format!("expected {expected:?}, found {found:?}"),
            span,
        ))
    }
}

fn numeric(value: ValueType) -> bool {
    matches!(
        value,
        ValueType::Int | ValueType::Float | ValueType::Duration | ValueType::Dynamic
    )
}

fn ordered(value: ValueType) -> bool {
    numeric(value) || matches!(value, ValueType::String)
}

/// Ints, floats, and durations (as nanoseconds) compare with one another.
fn comparable(left: ValueType, right: ValueType) -> bool {
    left == ValueType::Dynamic
        || right == ValueType::Dynamic
        || left == right
        || (numeric(left) && numeric(right))
}

fn arithmetic_type(left: ValueType, right: ValueType) -> Option<ValueType> {
    if !numeric(left) || !numeric(right) {
        return None;
    }
    if left == ValueType::Dynamic || right == ValueType::Dynamic {
        return Some(ValueType::Dynamic);
    }
    if left == ValueType::Duration || right == ValueType::Duration {
        return Some(ValueType::Duration);
    }
    Some(if left == ValueType::Float || right == ValueType::Float {
        ValueType::Float
    } else {
        ValueType::Int
    })
}
