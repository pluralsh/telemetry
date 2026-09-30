use crate::{AttributeMatcher, AttributeScope as IndexScope, AttributeValue, Result};

use super::ast::{
    AttributeScope, BinaryOp, Expr, FieldExpr, Query, SpansetExpr, StaticValue, StructuralOp,
};

/// Index candidates for one equality: a trace can only match if it holds at
/// least one of these exactly typed attributes.
pub type PushdownClause = Vec<AttributeMatcher>;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryPlan {
    pub query: Query,
    /// Every clause must be satisfied, so candidates intersect across clauses
    /// and union within one.
    pub pushdown: Vec<PushdownClause>,
}

pub fn plan(query: Query) -> Result<QueryPlan> {
    let mut pushdown = Vec::new();
    collect_spanset(&query.spanset.value, &mut pushdown)?;
    Ok(QueryPlan { query, pushdown })
}

fn collect_spanset(expression: &SpansetExpr, output: &mut Vec<PushdownClause>) -> Result<()> {
    match expression {
        SpansetExpr::Filter(expression) => collect_expr(expression, true, output)?,
        SpansetExpr::Binary { lhs, op, rhs } if *op == StructuralOp::And => {
            collect_spanset(&lhs.value, output)?;
            collect_spanset(&rhs.value, output)?;
        }
        SpansetExpr::Binary { .. } => {}
    }
    Ok(())
}

fn collect_expr(
    expression: &FieldExpr,
    positive: bool,
    output: &mut Vec<PushdownClause>,
) -> Result<()> {
    match &expression.value {
        Expr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } if positive => {
            collect_expr(lhs, true, output)?;
            collect_expr(rhs, true, output)?;
        }
        Expr::Binary {
            lhs,
            op: BinaryOp::Equal,
            rhs,
        } if positive => {
            if let Some(clause) = clause(lhs, rhs)? {
                output.push(clause);
            } else if let Some(clause) = clause(rhs, lhs)? {
                output.push(clause);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Largest magnitude below which every integer converts to a distinct `f64`.
const EXACT_FLOAT_INTEGER: f64 = 9_007_199_254_740_992.0;

fn clause(attribute: &FieldExpr, value: &FieldExpr) -> Result<Option<PushdownClause>> {
    let Expr::Attribute(attribute) = &attribute.value else {
        return Ok(None);
    };
    // Unscoped lookups prefer the span attribute and fall back to the
    // resource, so a match needs the value in at least one of them.
    let scopes: &[IndexScope] = match attribute.scope {
        AttributeScope::Resource => &[IndexScope::Resource],
        AttributeScope::Span => &[IndexScope::Span],
        AttributeScope::Unscoped => &[IndexScope::Span, IndexScope::Resource],
    };
    let Expr::Static(value) = &value.value else {
        return Ok(None);
    };
    let Some(values) = equal_values(value) else {
        return Ok(None);
    };
    let mut clause = Vec::with_capacity(scopes.len() * values.len());
    for scope in scopes {
        for value in &values {
            clause.push(AttributeMatcher::new(
                *scope,
                attribute.name.clone(),
                value.clone(),
            )?);
        }
    }
    Ok(Some(clause))
}

/// Every exactly typed stored value that TraceQL equality treats as equal to
/// `value`, or `None` when that set cannot be enumerated.
fn equal_values(value: &StaticValue) -> Option<Vec<AttributeValue>> {
    Some(match value {
        StaticValue::String(value) => vec![AttributeValue::String(value.clone())],
        StaticValue::Bool(value) => vec![AttributeValue::Bool(*value)],
        // Integers equal doubles numerically (so both zero signs); a double
        // with the same value is unique apart from its sign.
        StaticValue::Int(value) => {
            let double = *value as f64;
            let mut values = vec![AttributeValue::Int(*value), AttributeValue::Double(double)];
            if *value == 0 {
                values.push(AttributeValue::Double(-0.0));
            }
            values
        }
        // Doubles equal doubles bitwise and integers numerically. Beyond 2^53
        // many integers round to one double, so they cannot be enumerated.
        StaticValue::Float(value) => {
            let mut values = vec![AttributeValue::Double(*value)];
            if value.is_finite() && value.fract() == 0.0 {
                if value.abs() >= EXACT_FLOAT_INTEGER {
                    return None;
                }
                values.push(AttributeValue::Int(*value as i64));
            }
            values
        }
        StaticValue::Nil
        | StaticValue::Duration(_)
        | StaticValue::Status(_)
        | StaticValue::Kind(_) => return None,
    })
}
