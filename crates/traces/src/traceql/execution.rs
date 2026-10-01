use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use common::display::hex;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    trace::v1::Span,
};
use regex::Regex;

use crate::{Trace, TraceId};

use super::ast::{
    AggregateOp, AttributeScope, BinaryOp, Expr, FieldExpr, Intrinsic, KindValue, PipelineStage,
    Query, ScalarExpr, SpansetExpr, StaticValue, StatusValue, StructuralOp, UnaryOp,
};
use super::error::QueryError;

#[derive(Clone, Debug, PartialEq)]
pub struct MatchedSpan {
    pub span_id: Vec<u8>,
    pub parent_span_id: Vec<u8>,
    pub name: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub selected: Vec<(String, StaticValue)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TraceQlResult {
    pub trace_id: TraceId,
    pub start_ns: u64,
    pub end_ns: u64,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub spanset_count: usize,
    pub matched_spans: Vec<MatchedSpan>,
}

struct SpanContext<'a> {
    span: &'a Span,
    resource_attributes: &'a [KeyValue],
    scope: Option<&'a InstrumentationScope>,
}

/// Tempo's nested set model bounds for a span; all zero when the span is not
/// reachable from a root.
#[derive(Clone, Copy, Default)]
struct NestedSet {
    left: i64,
    right: i64,
    parent: i64,
}

struct TraceContext<'a> {
    trace: &'a Trace,
    spans: Vec<SpanContext<'a>>,
    by_id: HashMap<Vec<u8>, usize>,
    children: HashMap<Vec<u8>, Vec<usize>>,
    nested_sets: Vec<NestedSet>,
    root: Option<usize>,
    start_ns: u64,
    end_ns: u64,
}

pub(crate) fn execute(
    trace: &Trace,
    query: &Query,
    max_spans: usize,
) -> Result<Option<TraceQlResult>, QueryError> {
    let context = TraceContext::new(trace, max_spans)?;
    for stage in &query.stages {
        if let PipelineStage::Metric { name, .. } = stage {
            return Err(QueryError::Unsupported(format!(
                "metric stage `{name}` is parsed but not executable"
            )));
        }
    }
    let mut spansets = eval_spanset(&query.spanset.value, &context)?;
    let mut selected = Vec::new();
    for stage in &query.stages {
        match stage {
            PipelineStage::SpansetFilter(expression) => {
                for spanset in &mut spansets {
                    spanset.retain(|&index| span_matches(expression, index, &context));
                }
                spansets.retain(|spanset| !spanset.is_empty());
            }
            PipelineStage::By(field) => spansets = group_by(spansets, field, &context)?,
            PipelineStage::Coalesce => {
                let merged = spansets.into_iter().flatten().collect::<BTreeSet<_>>();
                spansets = (!merged.is_empty())
                    .then(|| merged.into_iter().collect())
                    .into_iter()
                    .collect();
            }
            PipelineStage::Select(fields) => selected = fields.clone(),
            PipelineStage::ScalarFilter { lhs, op, rhs, .. } => {
                spansets.retain(|spanset| {
                    let left = eval_scalar(lhs, spanset, &context);
                    let right = eval_scalar(rhs, spanset, &context);
                    match (left, right) {
                        (Ok(left), Ok(right)) => compare(*op, &left, &right).unwrap_or(false),
                        _ => false,
                    }
                });
            }
            PipelineStage::Metric { .. } => unreachable!("metrics rejected before execution"),
        }
    }
    if spansets.is_empty() {
        return Ok(None);
    }
    let matched = spansets.iter().flatten().copied().collect::<BTreeSet<_>>();
    let mut matched_spans = matched
        .into_iter()
        .map(|index| {
            let item = &context.spans[index];
            let selected = selected
                .iter()
                .filter_map(|field| {
                    let value = eval_expr(field, index, &context).ok()?;
                    value.into_static().map(|value| (field_name(field), value))
                })
                .collect();
            MatchedSpan {
                span_id: item.span.span_id.clone(),
                parent_span_id: item.span.parent_span_id.clone(),
                name: item.span.name.clone(),
                start_ns: item.span.start_time_unix_nano,
                end_ns: item.span.end_time_unix_nano,
                selected,
            }
        })
        .collect::<Vec<_>>();
    matched_spans.sort_by(|left, right| {
        (left.start_ns, &left.span_id).cmp(&(right.start_ns, &right.span_id))
    });
    let root = context.root.map(|index| &context.spans[index]);
    Ok(Some(TraceQlResult {
        trace_id: trace.trace_id,
        start_ns: context.start_ns,
        end_ns: context.end_ns,
        root_service_name: root.and_then(|root| {
            lookup_attribute(root.resource_attributes, "service.name")
                .and_then(EvalValue::into_string)
        }),
        root_span_name: root.map(|root| root.span.name.clone()),
        spanset_count: spansets.len(),
        matched_spans,
    }))
}

impl<'a> TraceContext<'a> {
    fn new(trace: &'a Trace, max_spans: usize) -> Result<Self, QueryError> {
        let mut spans = Vec::new();
        for resource_spans in &trace.resource_spans {
            let attributes = resource_spans
                .resource
                .as_ref()
                .map_or(&[][..], |resource| resource.attributes.as_slice());
            for scope in &resource_spans.scope_spans {
                for span in &scope.spans {
                    spans.push(SpanContext {
                        span,
                        resource_attributes: attributes,
                        scope: scope.scope.as_ref(),
                    });
                    if spans.len() > max_spans {
                        return Err(QueryError::Limit(format!(
                            "trace {} has more than {max_spans} spans",
                            trace.trace_id
                        )));
                    }
                }
            }
        }
        let by_id = spans
            .iter()
            .enumerate()
            .map(|(index, span)| (span.span.span_id.clone(), index))
            .collect::<HashMap<_, _>>();
        let mut children: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (index, span) in spans.iter().enumerate() {
            children
                .entry(span.span.parent_span_id.clone())
                .or_default()
                .push(index);
        }
        let root = spans
            .iter()
            .enumerate()
            .filter(|(_, span)| {
                span.span.parent_span_id.is_empty()
                    || !by_id.contains_key(&span.span.parent_span_id)
            })
            .min_by_key(|(_, span)| (span.span.start_time_unix_nano, &span.span.span_id))
            .map(|(index, _)| index);
        let (start_ns, end_ns) = trace.timestamp_range();
        let nested_sets = nested_sets(&spans, &children);
        Ok(Self {
            trace,
            spans,
            by_id,
            children,
            nested_sets,
            root,
            start_ns,
            end_ns,
        })
    }
}

/// Numbers spans depth first from each root span, assigning `left` on the way
/// down and `right` on the way up, as Tempo does at ingest.
fn nested_sets(
    spans: &[SpanContext<'_>],
    children: &HashMap<Vec<u8>, Vec<usize>>,
) -> Vec<NestedSet> {
    let mut output = vec![NestedSet::default(); spans.len()];
    let mut visited = vec![false; spans.len()];
    let mut bound = 1;
    let children_of = |index: usize| {
        let span_id = &spans[index].span.span_id;
        if span_id.is_empty() {
            &[][..]
        } else {
            children.get(span_id).map_or(&[][..], Vec::as_slice)
        }
    };
    for root in 0..spans.len() {
        if !spans[root].span.parent_span_id.is_empty() || visited[root] {
            continue;
        }
        visited[root] = true;
        output[root] = NestedSet {
            left: bound,
            right: 0,
            parent: -1,
        };
        bound += 1;
        let mut stack = vec![(root, 0)];
        while let Some(&(node, next)) = stack.last() {
            let child = children_of(node)
                .iter()
                .enumerate()
                .skip(next)
                .find(|&(_, &child)| !visited[child]);
            if let Some((position, &child)) = child {
                stack.last_mut().expect("non-empty stack").1 = position + 1;
                visited[child] = true;
                output[child] = NestedSet {
                    left: bound,
                    right: 0,
                    parent: output[node].left,
                };
                bound += 1;
                stack.push((child, 0));
            } else {
                output[node].right = bound;
                bound += 1;
                stack.pop();
            }
        }
    }
    output
}

fn eval_spanset(
    expression: &SpansetExpr,
    context: &TraceContext<'_>,
) -> Result<Vec<Vec<usize>>, QueryError> {
    match expression {
        SpansetExpr::Filter(expression) => {
            let matches = (0..context.spans.len())
                .filter(|&index| span_matches(expression, index, context))
                .collect::<Vec<_>>();
            Ok((!matches.is_empty())
                .then_some(matches)
                .into_iter()
                .collect())
        }
        SpansetExpr::Binary { lhs, op, rhs } => {
            let left = eval_spanset(&lhs.value, context)?
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            let right = eval_spanset(&rhs.value, context)?
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            let matched = structural(*op, &left, &right, context);
            Ok((!matched.is_empty())
                .then(|| matched.into_iter().collect())
                .into_iter()
                .collect())
        }
    }
}

fn structural(
    op: StructuralOp,
    left: &BTreeSet<usize>,
    right: &BTreeSet<usize>,
    context: &TraceContext<'_>,
) -> BTreeSet<usize> {
    if op == StructuralOp::Union {
        return left.union(right).copied().collect();
    }
    if op == StructuralOp::And {
        return if left.is_empty() || right.is_empty() {
            BTreeSet::new()
        } else {
            left.union(right).copied().collect()
        };
    }
    // Tempo semantics: `{ A } op { B }` yields the B spans related to some A
    // span (`!` variants: related to none); `&` variants also yield those A
    // spans.
    let (relation, negate, union) = match op {
        StructuralOp::Child => (Relation::Child, false, false),
        StructuralOp::Parent => (Relation::Parent, false, false),
        StructuralOp::Descendant => (Relation::Descendant, false, false),
        StructuralOp::Ancestor => (Relation::Ancestor, false, false),
        StructuralOp::Sibling => (Relation::Sibling, false, false),
        StructuralOp::NotChild => (Relation::Child, true, false),
        StructuralOp::NotParent => (Relation::Parent, true, false),
        StructuralOp::NotDescendant => (Relation::Descendant, true, false),
        StructuralOp::NotAncestor => (Relation::Ancestor, true, false),
        StructuralOp::NotSibling => (Relation::Sibling, true, false),
        StructuralOp::UnionChild => (Relation::Child, false, true),
        StructuralOp::UnionParent => (Relation::Parent, false, true),
        StructuralOp::UnionDescendant => (Relation::Descendant, false, true),
        StructuralOp::UnionAncestor => (Relation::Ancestor, false, true),
        StructuralOp::UnionSibling => (Relation::Sibling, false, true),
        StructuralOp::And | StructuralOp::Union => unreachable!(),
    };
    let left_index = LeftIndex::new(relation, left, context);
    let mut output = BTreeSet::new();
    for &right_index in right {
        let related = left_index.related(right_index, left, context);
        if negate {
            if related.is_empty() {
                output.insert(right_index);
            }
        } else if !related.is_empty() {
            output.insert(right_index);
            if union {
                output.extend(related);
            }
        }
    }
    output
}

/// How a right-hand span must relate to a left-hand span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Relation {
    Child,
    Parent,
    Descendant,
    Ancestor,
    Sibling,
}

/// The left-hand spans of a structural operator, indexed so each right span
/// finds its related left spans without scanning the whole left side.
struct LeftIndex<'l> {
    relation: Relation,
    /// Left spans by parent span id (`Parent`, `Sibling`).
    by_parent: HashMap<&'l [u8], Vec<usize>>,
    /// Left spans by each of their ancestors (`Ancestor`).
    by_ancestor: HashMap<usize, Vec<usize>>,
}

impl<'l> LeftIndex<'l> {
    fn new(relation: Relation, left: &BTreeSet<usize>, context: &'l TraceContext<'_>) -> Self {
        let mut by_parent: HashMap<&[u8], Vec<usize>> = HashMap::new();
        let mut by_ancestor: HashMap<usize, Vec<usize>> = HashMap::new();
        for &left_index in left {
            match relation {
                Relation::Parent | Relation::Sibling => {
                    let parent = context.spans[left_index].span.parent_span_id.as_slice();
                    if !parent.is_empty() {
                        by_parent.entry(parent).or_default().push(left_index);
                    }
                }
                Relation::Ancestor => {
                    for ancestor in ancestors(left_index, context) {
                        by_ancestor.entry(ancestor).or_default().push(left_index);
                    }
                }
                Relation::Child | Relation::Descendant => {}
            }
        }
        Self {
            relation,
            by_parent,
            by_ancestor,
        }
    }

    fn related(
        &self,
        right: usize,
        left: &BTreeSet<usize>,
        context: &TraceContext<'_>,
    ) -> Vec<usize> {
        let span = context.spans[right].span;
        match self.relation {
            Relation::Child => context
                .by_id
                .get(&span.parent_span_id)
                .copied()
                .filter(|parent| left.contains(parent))
                .into_iter()
                .collect(),
            Relation::Descendant => ancestors(right, context)
                .into_iter()
                .filter(|ancestor| left.contains(ancestor))
                .collect(),
            Relation::Parent => self
                .by_parent
                .get(span.span_id.as_slice())
                .cloned()
                .unwrap_or_default(),
            Relation::Sibling if span.parent_span_id.is_empty() => Vec::new(),
            Relation::Sibling => self
                .by_parent
                .get(span.parent_span_id.as_slice())
                .into_iter()
                .flatten()
                .copied()
                .filter(|&sibling| sibling != right)
                .collect(),
            Relation::Ancestor => self.by_ancestor.get(&right).cloned().unwrap_or_default(),
        }
    }
}

/// Parent chain of `index` through `by_id`, stopping at a missing parent or
/// a cycle.
fn ancestors(mut index: usize, context: &TraceContext<'_>) -> Vec<usize> {
    let mut visited = HashSet::from([index]);
    let mut chain = Vec::new();
    while let Some(&parent) = context.by_id.get(&context.spans[index].span.parent_span_id) {
        chain.push(parent);
        if !visited.insert(parent) {
            break;
        }
        index = parent;
    }
    chain
}

fn group_by(
    spansets: Vec<Vec<usize>>,
    field: &FieldExpr,
    context: &TraceContext<'_>,
) -> Result<Vec<Vec<usize>>, QueryError> {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for index in spansets.into_iter().flatten() {
        let value = eval_expr(field, index, context)?;
        groups.entry(value.key()).or_default().push(index);
    }
    Ok(groups.into_values().collect())
}

#[derive(Clone, Debug, PartialEq)]
enum EvalValue {
    Missing,
    Static(StaticValue),
}

impl EvalValue {
    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Static(StaticValue::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    fn into_static(self) -> Option<StaticValue> {
        match self {
            Self::Missing => None,
            Self::Static(value) => Some(value),
        }
    }

    fn into_string(self) -> Option<String> {
        match self {
            Self::Static(StaticValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn key(&self) -> String {
        match self {
            Self::Missing => "missing".to_owned(),
            Self::Static(value) => format!("{value:?}"),
        }
    }
}

fn span_matches(expression: &FieldExpr, index: usize, context: &TraceContext<'_>) -> bool {
    eval_expr(expression, index, context)
        .ok()
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn eval_expr(
    expression: &FieldExpr,
    index: usize,
    context: &TraceContext<'_>,
) -> Result<EvalValue, QueryError> {
    match &expression.value {
        Expr::Static(value) => Ok(EvalValue::Static(value.clone())),
        Expr::Attribute(attribute) => {
            let span = &context.spans[index];
            let value = match attribute.scope {
                AttributeScope::Resource => {
                    lookup_attribute(span.resource_attributes, &attribute.name)
                }
                AttributeScope::Span => lookup_attribute(&span.span.attributes, &attribute.name),
                AttributeScope::Instrumentation => span
                    .scope
                    .and_then(|scope| lookup_attribute(&scope.attributes, &attribute.name)),
                AttributeScope::Unscoped => {
                    lookup_attribute(&span.span.attributes, &attribute.name)
                        .or_else(|| lookup_attribute(span.resource_attributes, &attribute.name))
                }
            };
            Ok(value.unwrap_or(EvalValue::Missing))
        }
        Expr::Intrinsic(intrinsic) => Ok(eval_intrinsic(*intrinsic, index, context)),
        Expr::Unary { op, expr } => {
            let value = eval_expr(expr, index, context)?;
            match (op, value) {
                (UnaryOp::Not, EvalValue::Static(StaticValue::Bool(value))) => {
                    Ok(EvalValue::Static(StaticValue::Bool(!value)))
                }
                (UnaryOp::Neg, EvalValue::Static(StaticValue::Int(value))) => {
                    Ok(EvalValue::Static(StaticValue::Int(value.saturating_neg())))
                }
                (UnaryOp::Neg, EvalValue::Static(StaticValue::Float(value))) => {
                    Ok(EvalValue::Static(StaticValue::Float(-value)))
                }
                (UnaryOp::Neg, EvalValue::Static(StaticValue::Duration(value))) => Ok(
                    EvalValue::Static(StaticValue::Duration(value.saturating_neg())),
                ),
                _ => Ok(EvalValue::Missing),
            }
        }
        Expr::Binary { lhs, op, rhs } => {
            let left = eval_expr(lhs, index, context)?;
            if *op == BinaryOp::And && left.as_bool() == Some(false) {
                return Ok(EvalValue::Static(StaticValue::Bool(false)));
            }
            if *op == BinaryOp::Or && left.as_bool() == Some(true) {
                return Ok(EvalValue::Static(StaticValue::Bool(true)));
            }
            let right = eval_expr(rhs, index, context)?;
            if matches!(
                op,
                BinaryOp::Equal
                    | BinaryOp::NotEqual
                    | BinaryOp::Regex
                    | BinaryOp::NotRegex
                    | BinaryOp::Less
                    | BinaryOp::LessEqual
                    | BinaryOp::Greater
                    | BinaryOp::GreaterEqual
            ) {
                return Ok(EvalValue::Static(StaticValue::Bool(
                    compare(*op, &left, &right).unwrap_or(false),
                )));
            }
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                let result = match op {
                    BinaryOp::And => left.as_bool() == Some(true) && right.as_bool() == Some(true),
                    BinaryOp::Or => left.as_bool() == Some(true) || right.as_bool() == Some(true),
                    _ => unreachable!(),
                };
                return Ok(EvalValue::Static(StaticValue::Bool(result)));
            }
            arithmetic(*op, left, right)
        }
    }
}

fn eval_intrinsic(intrinsic: Intrinsic, index: usize, context: &TraceContext<'_>) -> EvalValue {
    let span = context.spans[index].span;
    let value = match intrinsic {
        Intrinsic::TraceId => StaticValue::String(context.trace.trace_id.to_string()),
        Intrinsic::SpanId => StaticValue::String(hex(&span.span_id)),
        Intrinsic::ParentId => StaticValue::String(hex(&span.parent_span_id)),
        Intrinsic::Name => StaticValue::String(span.name.clone()),
        Intrinsic::Duration => StaticValue::Duration(
            i64::try_from(
                span.end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano),
            )
            .unwrap_or(i64::MAX),
        ),
        Intrinsic::Status => {
            StaticValue::Status(match span.status.as_ref().map_or(0, |s| s.code) {
                1 => StatusValue::Ok,
                2 => StatusValue::Error,
                _ => StatusValue::Unset,
            })
        }
        Intrinsic::StatusMessage => StaticValue::String(
            span.status
                .as_ref()
                .map_or_else(String::new, |s| s.message.clone()),
        ),
        Intrinsic::Kind => StaticValue::Kind(match span.kind {
            1 => KindValue::Internal,
            2 => KindValue::Server,
            3 => KindValue::Client,
            4 => KindValue::Producer,
            5 => KindValue::Consumer,
            _ => KindValue::Unspecified,
        }),
        Intrinsic::RootName => StaticValue::String(
            context
                .root
                .map_or_else(String::new, |root| context.spans[root].span.name.clone()),
        ),
        Intrinsic::RootServiceName => StaticValue::String(
            context
                .root
                .and_then(|root| {
                    lookup_attribute(context.spans[root].resource_attributes, "service.name")
                        .and_then(EvalValue::into_string)
                })
                .unwrap_or_default(),
        ),
        Intrinsic::ChildCount => StaticValue::Int(
            context
                .children
                .get(&span.span_id)
                .map_or(0, Vec::len)
                .try_into()
                .unwrap_or(i64::MAX),
        ),
        Intrinsic::TraceDuration => StaticValue::Duration(
            i64::try_from(context.end_ns.saturating_sub(context.start_ns)).unwrap_or(i64::MAX),
        ),
        Intrinsic::InstrumentationName => StaticValue::String(
            context.spans[index]
                .scope
                .map_or_else(String::new, |scope| scope.name.clone()),
        ),
        Intrinsic::InstrumentationVersion => StaticValue::String(
            context.spans[index]
                .scope
                .map_or_else(String::new, |scope| scope.version.clone()),
        ),
        Intrinsic::NestedSetLeft => StaticValue::Int(context.nested_sets[index].left),
        Intrinsic::NestedSetRight => StaticValue::Int(context.nested_sets[index].right),
        Intrinsic::NestedSetParent => StaticValue::Int(context.nested_sets[index].parent),
    };
    EvalValue::Static(value)
}

fn lookup_attribute(attributes: &[KeyValue], name: &str) -> Option<EvalValue> {
    attributes
        .iter()
        .find(|attribute| attribute.key == name)
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(any_value)
        .map(EvalValue::Static)
}

fn any_value(value: &AnyValue) -> Option<StaticValue> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(StaticValue::String(value.clone())),
        any_value::Value::BoolValue(value) => Some(StaticValue::Bool(*value)),
        any_value::Value::IntValue(value) => Some(StaticValue::Int(*value)),
        any_value::Value::DoubleValue(value) => Some(StaticValue::Float(*value)),
        any_value::Value::ArrayValue(_)
        | any_value::Value::KvlistValue(_)
        | any_value::Value::BytesValue(_) => None,
    }
}

/// Whether an indexed value passes `value op literal`, exactly as a span
/// holding it would.
pub(super) fn stored_value_matches(
    op: BinaryOp,
    value: &crate::AttributeValue,
    literal: &StaticValue,
) -> bool {
    let value = match value {
        crate::AttributeValue::String(value) => StaticValue::String(value.clone()),
        crate::AttributeValue::Bool(value) => StaticValue::Bool(*value),
        crate::AttributeValue::Int(value) => StaticValue::Int(*value),
        crate::AttributeValue::Double(value) => StaticValue::Float(*value),
    };
    compare(
        op,
        &EvalValue::Static(value),
        &EvalValue::Static(literal.clone()),
    )
    .unwrap_or(false)
}

fn compare(op: BinaryOp, left: &EvalValue, right: &EvalValue) -> Option<bool> {
    if matches!(op, BinaryOp::Equal | BinaryOp::NotEqual)
        && (matches!(left, EvalValue::Missing)
            || matches!(left, EvalValue::Static(StaticValue::Nil))
            || matches!(right, EvalValue::Missing)
            || matches!(right, EvalValue::Static(StaticValue::Nil)))
    {
        let equal = matches!(
            (left, right),
            (EvalValue::Missing, EvalValue::Static(StaticValue::Nil))
                | (EvalValue::Static(StaticValue::Nil), EvalValue::Missing)
        );
        return Some(if op == BinaryOp::Equal { equal } else { !equal });
    }
    let (EvalValue::Static(left), EvalValue::Static(right)) = (left, right) else {
        return None;
    };
    let ordering = static_cmp(left, right);
    match op {
        BinaryOp::Equal => Some(static_equal(left, right)),
        BinaryOp::NotEqual => Some(!static_equal(left, right)),
        BinaryOp::Less => ordering.map(|value| value.is_lt()),
        BinaryOp::LessEqual => ordering.map(|value| value.is_le()),
        BinaryOp::Greater => ordering.map(|value| value.is_gt()),
        BinaryOp::GreaterEqual => ordering.map(|value| value.is_ge()),
        BinaryOp::Regex | BinaryOp::NotRegex => {
            let (StaticValue::String(value), StaticValue::String(pattern)) = (left, right) else {
                return None;
            };
            let matched = regex_is_match(pattern, value)?;
            Some(if op == BinaryOp::Regex {
                matched
            } else {
                !matched
            })
        }
        _ => None,
    }
}

const REGEX_CACHE_CAPACITY: usize = 256;

thread_local! {
    /// Comparisons run per span, so patterns compile once per worker thread.
    static REGEX_CACHE: RefCell<HashMap<String, Regex>> = RefCell::new(HashMap::new());
}

/// Returns `None` for an invalid pattern.
fn regex_is_match(pattern: &str, value: &str) -> Option<bool> {
    REGEX_CACHE.with(|cache| {
        if let Some(regex) = cache.borrow().get(pattern) {
            return Some(regex.is_match(value));
        }
        let regex = Regex::new(pattern).ok()?;
        let matched = regex.is_match(value);
        let mut cache = cache.borrow_mut();
        if cache.len() >= REGEX_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(pattern.to_owned(), regex);
        Some(matched)
    })
}

/// Ints, floats, and durations (as nanoseconds) compare numerically, as in
/// Tempo.
fn static_equal(left: &StaticValue, right: &StaticValue) -> bool {
    match (left, right) {
        (StaticValue::Float(left), StaticValue::Float(right)) => left.to_bits() == right.to_bits(),
        (
            StaticValue::Int(left) | StaticValue::Duration(left),
            StaticValue::Int(right) | StaticValue::Duration(right),
        ) => left == right,
        (StaticValue::Float(_), _) | (_, StaticValue::Float(_)) => {
            match (as_float(left), as_float(right)) {
                (Some(left), Some(right)) => left == right,
                _ => false,
            }
        }
        _ => left == right,
    }
}

fn static_cmp(left: &StaticValue, right: &StaticValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (StaticValue::String(left), StaticValue::String(right)) => Some(left.cmp(right)),
        (
            StaticValue::Int(left) | StaticValue::Duration(left),
            StaticValue::Int(right) | StaticValue::Duration(right),
        ) => Some(left.cmp(right)),
        _ => as_float(left)?.partial_cmp(&as_float(right)?),
    }
}

fn arithmetic(op: BinaryOp, left: EvalValue, right: EvalValue) -> Result<EvalValue, QueryError> {
    let (EvalValue::Static(left), EvalValue::Static(right)) = (left, right) else {
        return Ok(EvalValue::Missing);
    };
    let result = match (left, right) {
        (StaticValue::Int(left), StaticValue::Int(right)) => StaticValue::Int(match op {
            BinaryOp::Add => left.saturating_add(right),
            BinaryOp::Sub => left.saturating_sub(right),
            BinaryOp::Mul => left.saturating_mul(right),
            BinaryOp::Div if right != 0 => left / right,
            BinaryOp::Mod if right != 0 => left % right,
            BinaryOp::Pow if right >= 0 => {
                left.saturating_pow(right.try_into().unwrap_or(u32::MAX))
            }
            _ => return Ok(EvalValue::Missing),
        }),
        (StaticValue::Duration(left), StaticValue::Duration(right)) => {
            StaticValue::Duration(match op {
                BinaryOp::Add => left.saturating_add(right),
                BinaryOp::Sub => left.saturating_sub(right),
                _ => return Ok(EvalValue::Missing),
            })
        }
        (left, right) => {
            let Some(left) = as_float(&left) else {
                return Ok(EvalValue::Missing);
            };
            let Some(right) = as_float(&right) else {
                return Ok(EvalValue::Missing);
            };
            StaticValue::Float(match op {
                BinaryOp::Add => left + right,
                BinaryOp::Sub => left - right,
                BinaryOp::Mul => left * right,
                BinaryOp::Div => left / right,
                BinaryOp::Mod => left % right,
                BinaryOp::Pow => left.powf(right),
                _ => return Ok(EvalValue::Missing),
            })
        }
    };
    Ok(EvalValue::Static(result))
}

fn as_float(value: &StaticValue) -> Option<f64> {
    match value {
        StaticValue::Int(value) | StaticValue::Duration(value) => Some(*value as f64),
        StaticValue::Float(value) => Some(*value),
        _ => None,
    }
}

fn eval_scalar(
    expression: &ScalarExpr,
    spanset: &[usize],
    context: &TraceContext<'_>,
) -> Result<EvalValue, QueryError> {
    match expression {
        ScalarExpr::Static(value) => Ok(EvalValue::Static(value.clone())),
        ScalarExpr::Aggregate { op, field } => {
            if *op == AggregateOp::Count {
                return Ok(EvalValue::Static(StaticValue::Int(
                    spanset.len().try_into().unwrap_or(i64::MAX),
                )));
            }
            let field = field.as_ref().expect("validated numeric aggregate field");
            let values = spanset
                .iter()
                .filter_map(|&index| eval_expr(field, index, context).ok())
                .filter_map(|value| match value {
                    EvalValue::Static(StaticValue::Int(value)) => Some(value as f64),
                    EvalValue::Static(StaticValue::Float(value)) => Some(value),
                    EvalValue::Static(StaticValue::Duration(value)) => Some(value as f64),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if values.is_empty() {
                return Ok(EvalValue::Missing);
            }
            let value = match op {
                AggregateOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
                AggregateOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                AggregateOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                AggregateOp::Sum => values.iter().sum(),
                AggregateOp::Count => unreachable!(),
            };
            Ok(EvalValue::Static(StaticValue::Float(value)))
        }
        ScalarExpr::Binary { lhs, op, rhs } => arithmetic(
            *op,
            eval_scalar(lhs, spanset, context)?,
            eval_scalar(rhs, spanset, context)?,
        ),
    }
}

fn field_name(field: &FieldExpr) -> String {
    match &field.value {
        Expr::Attribute(attribute) => attribute.name.clone(),
        Expr::Intrinsic(intrinsic) => format!("{intrinsic:?}"),
        _ => "expression".to_owned(),
    }
}
