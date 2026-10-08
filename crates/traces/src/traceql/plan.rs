use opentelemetry_proto::tonic::trace::v1::Span;

use crate::{AttributeValue, Error, Result};

use super::ast::{
    AttributeScope, BinaryOp, Expr, FieldExpr, Intrinsic, KindValue, PipelineStage, Query,
    SpansetExpr, StaticValue, StatusValue, StructuralOp,
};

/// What a posting indexes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IndexField {
    Resource,
    Span,
    /// Span intrinsics: [`INTRINSIC_NAME`], [`INTRINSIC_STATUS`],
    /// [`INTRINSIC_KIND`], and [`INTRINSIC_DURATION`].
    Intrinsic,
}

pub(crate) const INTRINSIC_NAME: &str = "name";
/// Every span has a status, so a page has status postings exactly when it was
/// written with intrinsics indexed.
pub(crate) const INTRINSIC_STATUS: &str = "status";
pub(crate) const INTRINSIC_KIND: &str = "kind";
/// Indexed as [`duration_bucket`]s.
pub(crate) const INTRINSIC_DURATION: &str = "duration";

/// One way a trace can satisfy a clause: some span or resource holds a value
/// of `name` in `field` that passes `test`.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexPredicate {
    pub field: IndexField,
    pub name: String,
    pub test: IndexTest,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexTest {
    /// Exactly this typed value: one posting lookup.
    Exact(AttributeValue),
    /// Any value `v` where TraceQL evaluates `v op literal` to true: a scan of
    /// every value of the field.
    Compare(BinaryOp, StaticValue),
    /// A duration bucket that may hold a value `v` with `v op nanoseconds`.
    Duration(BinaryOp, f64),
}

impl IndexPredicate {
    fn exact(field: IndexField, name: &str, value: AttributeValue) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::Invalid("attribute name cannot be empty".to_owned()));
        }
        Ok(Self {
            field,
            name: name.to_owned(),
            test: IndexTest::Exact(value),
        })
    }

    /// Whether a stored `value` of this field can satisfy the predicate.
    pub(crate) fn admits(&self, value: &AttributeValue) -> bool {
        match &self.test {
            IndexTest::Exact(wanted) => wanted.exact_eq(value),
            IndexTest::Compare(op, literal) => {
                super::execution::stored_value_matches(*op, value, literal)
            }
            IndexTest::Duration(op, nanoseconds) => {
                let AttributeValue::Int(bucket) = value else {
                    return false;
                };
                let (low, high) = bucket_bounds(*bucket);
                match op {
                    BinaryOp::Greater => high > *nanoseconds,
                    BinaryOp::GreaterEqual => high >= *nanoseconds,
                    BinaryOp::Less => low < *nanoseconds,
                    BinaryOp::LessEqual => low <= *nanoseconds,
                    BinaryOp::Equal => low <= *nanoseconds && *nanoseconds <= high,
                    _ => true,
                }
            }
        }
    }

    /// Inclusive ranges of stored values, each bounded by two integers or two
    /// doubles, that hold every value [`Self::admits`]; `None` when the whole
    /// field must be scanned. Exact predicates name their one value instead.
    pub(crate) fn value_ranges(&self) -> Option<Vec<(AttributeValue, AttributeValue)>> {
        match &self.test {
            IndexTest::Exact(_) => None,
            IndexTest::Compare(op, literal) => numeric_ranges(*op, literal),
            IndexTest::Duration(..) => {
                let mut admitted = (0..=i64::from(u64::BITS))
                    .filter(|&bucket| self.admits(&AttributeValue::Int(bucket)));
                let first = admitted.next();
                Some(
                    first
                        .map(|first| {
                            let last = admitted.next_back().unwrap_or(first);
                            (AttributeValue::Int(first), AttributeValue::Int(last))
                        })
                        .into_iter()
                        .collect(),
                )
            }
        }
    }
}

/// Ranges holding every stored value `v` with `v op literal` for an ordering
/// or equality against a number. Strings and booleans never compare equal
/// or ordered to numbers, and NaN orders against nothing.
fn numeric_ranges(
    op: BinaryOp,
    literal: &StaticValue,
) -> Option<Vec<(AttributeValue, AttributeValue)>> {
    let (bounded_below, bounded_above) = match op {
        BinaryOp::Greater | BinaryOp::GreaterEqual => (true, false),
        BinaryOp::Less | BinaryOp::LessEqual => (false, true),
        BinaryOp::Equal => (true, true),
        _ => return None,
    };
    let strict = matches!(op, BinaryOp::Greater | BinaryOp::Less);
    let (ints, double) = match *literal {
        StaticValue::Int(value) | StaticValue::Duration(value) => {
            let low = match (bounded_below, strict) {
                (false, _) => Some(i64::MIN),
                (true, true) => value.checked_add(1),
                (true, false) => Some(value),
            };
            let high = match (bounded_above, strict) {
                (false, _) => Some(i64::MAX),
                (true, true) => value.checked_sub(1),
                (true, false) => Some(value),
            };
            (low.zip(high), value as f64)
        }
        // Doubles equal doubles bitwise, so only `=` matches a NaN.
        StaticValue::Float(value) if value.is_nan() => {
            return (op != BinaryOp::Equal).then(Vec::new);
        }
        StaticValue::Float(value) => {
            // Integers compare as their nearest double, which beyond 2^53 may
            // lie on either side of the literal, so the bounds leave room.
            let margin = if value.abs() < EXACT_FLOAT_INTEGER {
                1
            } else {
                1 << 12
            };
            let near = value as i64;
            let low = if bounded_below {
                near.saturating_sub(margin)
            } else {
                i64::MIN
            };
            let high = if bounded_above {
                near.saturating_add(margin)
            } else {
                i64::MAX
            };
            (Some((low, high)), value)
        }
        _ => return None,
    };
    // Both zeros equal a zero literal, and `-0.0` sorts first.
    let (low, high) = if double == 0.0 {
        (-0.0, 0.0)
    } else {
        (double, double)
    };
    let mut ranges = ints
        .filter(|(low, high)| low <= high)
        .map(|(low, high)| (AttributeValue::Int(low), AttributeValue::Int(high)))
        .into_iter()
        .collect::<Vec<_>>();
    ranges.push((
        AttributeValue::Double(if bounded_below {
            low
        } else {
            f64::NEG_INFINITY
        }),
        AttributeValue::Double(if bounded_above { high } else { f64::INFINITY }),
    ));
    Some(ranges)
}

/// Index candidates for one condition: a trace can only match if it
/// satisfies at least one of these predicates.
pub type PushdownClause = Vec<IndexPredicate>;

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
    // A stage filter keeps only matching spans, so a result needs one.
    for stage in &query.stages {
        if let PipelineStage::SpansetFilter(expression) = stage {
            collect_expr(expression, &mut pushdown)?;
        }
    }
    Ok(QueryPlan { query, pushdown })
}

/// Whether one span decides a match: a spanset filter, or a union of them,
/// refined only by spanset filter stages, over fields of the span alone.
/// Every pushdown clause then holds on the matching span itself, so its page
/// is posted by any clause, and a trace matches exactly when some part of it
/// the index saw does.
pub fn existential(query: &Query) -> bool {
    fn spanset(expression: &SpansetExpr) -> bool {
        match expression {
            SpansetExpr::Filter(expression) => span_local(expression),
            SpansetExpr::Binary {
                lhs,
                op: StructuralOp::Union,
                rhs,
            } => spanset(&lhs.value) && spanset(&rhs.value),
            SpansetExpr::Binary { .. } => false,
        }
    }
    spanset(&query.spanset.value)
        && query.stages.iter().all(|stage| match stage {
            PipelineStage::SpansetFilter(expression) => span_local(expression),
            _ => false,
        })
}

fn span_local(expression: &FieldExpr) -> bool {
    match &expression.value {
        Expr::Static(_) | Expr::Attribute(_) => true,
        Expr::Intrinsic(intrinsic) => !matches!(
            intrinsic,
            Intrinsic::RootName
                | Intrinsic::RootServiceName
                | Intrinsic::ChildCount
                | Intrinsic::TraceDuration
                | Intrinsic::NestedSetLeft
                | Intrinsic::NestedSetRight
                | Intrinsic::NestedSetParent
        ),
        Expr::Unary { expr, .. } => span_local(expr),
        Expr::Binary { lhs, rhs, .. } => span_local(lhs) && span_local(rhs),
    }
}

/// The intrinsic postings of one span, as written at ingest.
pub(crate) fn span_intrinsics(span: &Span) -> [(&'static str, AttributeValue); 4] {
    [
        (INTRINSIC_NAME, AttributeValue::String(span.name.clone())),
        (
            INTRINSIC_STATUS,
            AttributeValue::Int(status_code(
                match span.status.as_ref().map_or(0, |s| s.code) {
                    1 => StatusValue::Ok,
                    2 => StatusValue::Error,
                    _ => StatusValue::Unset,
                },
            )),
        ),
        (
            INTRINSIC_KIND,
            AttributeValue::Int(kind_code(match span.kind {
                1 => KindValue::Internal,
                2 => KindValue::Server,
                3 => KindValue::Client,
                4 => KindValue::Producer,
                5 => KindValue::Consumer,
                _ => KindValue::Unspecified,
            })),
        ),
        (
            INTRINSIC_DURATION,
            AttributeValue::Int(duration_bucket(
                span.end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano),
            )),
        ),
    ]
}

const STATUSES: [StatusValue; 3] = [StatusValue::Unset, StatusValue::Ok, StatusValue::Error];
const KINDS: [KindValue; 6] = [
    KindValue::Unspecified,
    KindValue::Internal,
    KindValue::Server,
    KindValue::Client,
    KindValue::Producer,
    KindValue::Consumer,
];

fn status_code(status: StatusValue) -> i64 {
    STATUSES
        .iter()
        .position(|&known| known == status)
        .unwrap_or(0) as i64
}

fn kind_code(kind: KindValue) -> i64 {
    KINDS.iter().position(|&known| known == kind).unwrap_or(0) as i64
}

/// Bit length of a duration: bucket `b > 0` holds `[2^(b-1), 2^b)`.
fn duration_bucket(nanoseconds: u64) -> i64 {
    i64::from(u64::BITS - nanoseconds.leading_zeros())
}

fn bucket_bounds(bucket: i64) -> (f64, f64) {
    match bucket {
        ..=0 => (0.0, 0.0),
        bucket => {
            let low = 2f64.powi(bucket as i32 - 1);
            (low, low * 2.0 - 1.0)
        }
    }
}

fn collect_spanset(expression: &SpansetExpr, output: &mut Vec<PushdownClause>) -> Result<()> {
    match expression {
        SpansetExpr::Filter(expression) => collect_expr(expression, output)?,
        SpansetExpr::Binary { lhs, op, rhs } => {
            // Relations and `&&` match only when both sides do; negated
            // relations return right-hand spans, so only that side is needed.
            let (left, right) = match op {
                StructuralOp::Union => {
                    let (mut left, mut right) = (Vec::new(), Vec::new());
                    collect_spanset(&lhs.value, &mut left)?;
                    collect_spanset(&rhs.value, &mut right)?;
                    output.extend(either(left, right));
                    return Ok(());
                }
                StructuralOp::NotChild
                | StructuralOp::NotParent
                | StructuralOp::NotDescendant
                | StructuralOp::NotAncestor
                | StructuralOp::NotSibling => (false, true),
                _ => (true, true),
            };
            if left {
                collect_spanset(&lhs.value, output)?;
            }
            if right {
                collect_spanset(&rhs.value, output)?;
            }
        }
    }
    Ok(())
}

fn collect_expr(expression: &FieldExpr, output: &mut Vec<PushdownClause>) -> Result<()> {
    let Expr::Binary { lhs, op, rhs } = &expression.value else {
        return Ok(());
    };
    match op {
        BinaryOp::And => {
            collect_expr(lhs, output)?;
            collect_expr(rhs, output)?;
        }
        BinaryOp::Or => {
            let (mut left, mut right) = (Vec::new(), Vec::new());
            collect_expr(lhs, &mut left)?;
            collect_expr(rhs, &mut right)?;
            output.extend(either(left, right));
        }
        op => {
            let clause = match comparison(lhs, *op, rhs)? {
                Some(clause) => Some(clause),
                None => match flipped(*op) {
                    Some(op) => comparison(rhs, op, lhs)?,
                    None => None,
                },
            };
            output.extend(clause);
        }
    }
    Ok(())
}

/// A clause implied by `left || right`: a match satisfies every clause of one
/// side, so it holds a predicate from that side's narrowest clause.
fn either(left: Vec<PushdownClause>, right: Vec<PushdownClause>) -> Option<PushdownClause> {
    let narrowest = |clauses: Vec<PushdownClause>| clauses.into_iter().min_by_key(Vec::len);
    let mut clause = narrowest(left)?;
    clause.extend(narrowest(right)?);
    Some(clause)
}

/// `op` with its operands swapped, for comparisons that have one.
fn flipped(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Equal | BinaryOp::NotEqual => op,
        BinaryOp::Less => BinaryOp::Greater,
        BinaryOp::LessEqual => BinaryOp::GreaterEqual,
        BinaryOp::Greater => BinaryOp::Less,
        BinaryOp::GreaterEqual => BinaryOp::LessEqual,
        _ => return None,
    })
}

/// The clause implied by `field op literal`, when the index can answer it.
fn comparison(
    field: &FieldExpr,
    op: BinaryOp,
    literal: &FieldExpr,
) -> Result<Option<PushdownClause>> {
    let Expr::Static(literal) = &literal.value else {
        return Ok(None);
    };
    match &field.value {
        Expr::Attribute(attribute) => {
            attribute_comparison(attribute.scope, &attribute.name, op, literal)
        }
        Expr::Intrinsic(intrinsic) => Ok(intrinsic_comparison(*intrinsic, op, literal)),
        _ => Ok(None),
    }
}

fn attribute_comparison(
    scope: AttributeScope,
    name: &str,
    op: BinaryOp,
    literal: &StaticValue,
) -> Result<Option<PushdownClause>> {
    // Unscoped lookups prefer the span attribute and fall back to the
    // resource, so a match needs the value in at least one of them.
    let fields: &[IndexField] = match scope {
        AttributeScope::Resource => &[IndexField::Resource],
        AttributeScope::Span => &[IndexField::Span],
        AttributeScope::Unscoped => &[IndexField::Span, IndexField::Resource],
        AttributeScope::Instrumentation => return Ok(None),
    };
    let test = match (op, literal) {
        (BinaryOp::Equal, StaticValue::Nil) => return Ok(None),
        (BinaryOp::Equal, literal) => match equal_values(literal) {
            Some(values) => {
                let mut clause = Vec::with_capacity(fields.len() * values.len());
                for &field in fields {
                    for value in &values {
                        clause.push(IndexPredicate::exact(field, name, value.clone())?);
                    }
                }
                return Ok(Some(clause));
            }
            None => IndexTest::Compare(op, literal.clone()),
        },
        (
            BinaryOp::NotEqual
            | BinaryOp::Regex
            | BinaryOp::NotRegex
            | BinaryOp::Less
            | BinaryOp::LessEqual
            | BinaryOp::Greater
            | BinaryOp::GreaterEqual,
            literal,
        ) => IndexTest::Compare(op, literal.clone()),
        _ => return Ok(None),
    };
    Ok(Some(
        fields
            .iter()
            .map(|&field| IndexPredicate {
                field,
                name: name.to_owned(),
                test: test.clone(),
            })
            .collect(),
    ))
}

fn intrinsic_comparison(
    intrinsic: Intrinsic,
    op: BinaryOp,
    literal: &StaticValue,
) -> Option<PushdownClause> {
    let predicate = |name: &str, test| IndexPredicate {
        field: IndexField::Intrinsic,
        name: name.to_owned(),
        test,
    };
    let codes = |name: &'static str, wanted: i64, count: i64| -> Option<PushdownClause> {
        let codes = match op {
            BinaryOp::Equal => vec![wanted],
            // Every span has one, so `!=` means any other value.
            BinaryOp::NotEqual => (0..count).filter(|&code| code != wanted).collect(),
            _ => return None,
        };
        Some(
            codes
                .into_iter()
                .map(|code| predicate(name, IndexTest::Exact(AttributeValue::Int(code))))
                .collect(),
        )
    };
    match (intrinsic, literal) {
        (Intrinsic::Name, StaticValue::String(value)) if op == BinaryOp::Equal => {
            Some(vec![predicate(
                INTRINSIC_NAME,
                IndexTest::Exact(AttributeValue::String(value.clone())),
            )])
        }
        (Intrinsic::Name, StaticValue::Nil) => None,
        (Intrinsic::Name, literal) => match op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::Regex
            | BinaryOp::NotRegex
            | BinaryOp::Less
            | BinaryOp::LessEqual
            | BinaryOp::Greater
            | BinaryOp::GreaterEqual => Some(vec![predicate(
                INTRINSIC_NAME,
                IndexTest::Compare(op, literal.clone()),
            )]),
            _ => None,
        },
        (Intrinsic::Status, StaticValue::Status(status)) => codes(
            INTRINSIC_STATUS,
            status_code(*status),
            STATUSES.len() as i64,
        ),
        (Intrinsic::Kind, StaticValue::Kind(kind)) => {
            codes(INTRINSIC_KIND, kind_code(*kind), KINDS.len() as i64)
        }
        (Intrinsic::Duration, StaticValue::Duration(value) | StaticValue::Int(value)) => {
            duration_comparison(op, *value as f64)
        }
        (Intrinsic::Duration, StaticValue::Float(value)) => duration_comparison(op, *value),
        _ => None,
    }
    .filter(|clause| !clause.is_empty())
}

fn duration_comparison(op: BinaryOp, nanoseconds: f64) -> Option<PushdownClause> {
    matches!(
        op,
        BinaryOp::Equal
            | BinaryOp::Less
            | BinaryOp::LessEqual
            | BinaryOp::Greater
            | BinaryOp::GreaterEqual
    )
    .then(|| {
        vec![IndexPredicate {
            field: IndexField::Intrinsic,
            name: INTRINSIC_DURATION.to_owned(),
            test: IndexTest::Duration(op, nanoseconds),
        }]
    })
}

/// Largest magnitude below which every integer converts to a distinct `f64`.
const EXACT_FLOAT_INTEGER: f64 = 9_007_199_254_740_992.0;

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
