use crate::{AttributeMatcher, AttributeScope as IndexScope, AttributeValue, Result};

use super::ast::{
    AttributeScope, BinaryOp, Expr, FieldExpr, Query, SpansetExpr, StaticValue, StructuralOp,
};

#[derive(Clone, Debug, PartialEq)]
pub struct QueryPlan {
    pub query: Query,
    pub pushdown: Vec<AttributeMatcher>,
}

pub fn plan(query: Query) -> Result<QueryPlan> {
    let mut pushdown = Vec::new();
    collect_spanset(&query.spanset.value, &mut pushdown)?;
    Ok(QueryPlan { query, pushdown })
}

fn collect_spanset(expression: &SpansetExpr, output: &mut Vec<AttributeMatcher>) -> Result<()> {
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
    output: &mut Vec<AttributeMatcher>,
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
            if let Some(matcher) = matcher(lhs, rhs)? {
                output.push(matcher);
            } else if let Some(matcher) = matcher(rhs, lhs)? {
                output.push(matcher);
            }
        }
        _ => {}
    }
    Ok(())
}

fn matcher(attribute: &FieldExpr, value: &FieldExpr) -> Result<Option<AttributeMatcher>> {
    let Expr::Attribute(attribute) = &attribute.value else {
        return Ok(None);
    };
    let scope = match attribute.scope {
        AttributeScope::Resource => IndexScope::Resource,
        AttributeScope::Span => IndexScope::Span,
        AttributeScope::Unscoped => return Ok(None),
    };
    let Expr::Static(value) = &value.value else {
        return Ok(None);
    };
    let value = match value {
        StaticValue::String(value) => AttributeValue::String(value.clone()),
        StaticValue::Bool(value) => AttributeValue::Bool(*value),
        // TraceQL compares integer and floating-point numerics across types.
        // Track's index is exactly typed, so numeric equality is not a safe
        // pushdown: either posting alone could exclude an equal value.
        StaticValue::Int(_) | StaticValue::Float(_) => return Ok(None),
        StaticValue::Nil
        | StaticValue::Duration(_)
        | StaticValue::Status(_)
        | StaticValue::Kind(_) => return Ok(None),
    };
    AttributeMatcher::new(scope, attribute.name.clone(), value).map(Some)
}
