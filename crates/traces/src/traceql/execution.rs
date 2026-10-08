use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;

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
use super::columns::{self, ColumnPredicate, ColumnSource, Target};
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

/// A search hit without its spans: what a search response lists per trace.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceSummary {
    pub trace_id: TraceId,
    pub start_ns: u64,
    pub end_ns: u64,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub matched_spans: usize,
}

impl From<TraceQlResult> for TraceSummary {
    fn from(result: TraceQlResult) -> Self {
        Self {
            trace_id: result.trace_id,
            start_ns: result.start_ns,
            end_ns: result.end_ns,
            root_service_name: result.root_service_name,
            root_span_name: result.root_span_name,
            matched_spans: result.matched_spans.len(),
        }
    }
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

/// Structural indexes and intrinsic columns are built on first use, so a
/// query pays only for what it reads.
struct TraceContext<'a> {
    trace: &'a Trace,
    spans: Vec<SpanContext<'a>>,
    /// Span ranges of each `ResourceSpans`, in span order.
    resources: Vec<Range<usize>>,
    columnar: bool,
    by_id: OnceCell<HashMap<&'a [u8], usize>>,
    children: OnceCell<HashMap<&'a [u8], Vec<usize>>>,
    nested_sets: OnceCell<Vec<NestedSet>>,
    root: OnceCell<Option<usize>>,
    durations: OnceCell<Vec<i64>>,
    statuses: OnceCell<Vec<u8>>,
    kinds: OnceCell<Vec<u8>>,
    start_ns: u64,
    end_ns: u64,
}

/// How one spanset filter is evaluated.
#[derive(Clone, Debug)]
struct FilterPlan {
    expression: FieldExpr,
    /// The column form of the top-level conjuncts it covers exactly.
    columns: Option<ColumnPredicate>,
    /// Conjuncts left to the interpreter, for spans the columns admit.
    residual: Vec<FieldExpr>,
}

impl FilterPlan {
    fn new(expression: &FieldExpr) -> Self {
        let mut conjuncts = Vec::new();
        flatten_and(expression, &mut conjuncts);
        let mut columns = None;
        let mut residual = Vec::new();
        for conjunct in conjuncts {
            match columns::compile_exact(conjunct, Target::Trace) {
                Some(compiled) => {
                    columns = Some(match columns {
                        Some(previous) => {
                            ColumnPredicate::And(Box::new(previous), Box::new(compiled))
                        }
                        None => compiled,
                    });
                }
                None => residual.push(conjunct.clone()),
            }
        }
        Self {
            expression: expression.clone(),
            columns,
            residual,
        }
    }

    #[cfg(any(test, feature = "bench-internals"))]
    fn interpreted(expression: &FieldExpr) -> Self {
        Self {
            expression: expression.clone(),
            columns: None,
            residual: Vec::new(),
        }
    }

    /// Whether each span matches, when the plan has a column form.
    fn mask(&self, context: &TraceContext<'_>) -> Option<Vec<bool>> {
        let mut mask = columns::evaluate(self.columns.as_ref()?, context)?;
        if !self.residual.is_empty() {
            for (index, admitted) in mask.iter_mut().enumerate() {
                *admitted = *admitted
                    && self
                        .residual
                        .iter()
                        .all(|conjunct| span_matches(conjunct, index, context));
            }
        }
        Some(mask)
    }
}

/// `span_matches(a && b)` equals `span_matches(a) && span_matches(b)`, so
/// top-level conjuncts are evaluated independently.
fn flatten_and<'e>(expression: &'e FieldExpr, conjuncts: &mut Vec<&'e FieldExpr>) {
    match &expression.value {
        Expr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => {
            flatten_and(lhs, conjuncts);
            flatten_and(rhs, conjuncts);
        }
        _ => conjuncts.push(expression),
    }
}

#[derive(Clone, Debug)]
enum SpansetPlan {
    Filter(FilterPlan),
    Binary {
        lhs: Box<SpansetPlan>,
        op: StructuralOp,
        rhs: Box<SpansetPlan>,
    },
}

impl SpansetPlan {
    fn new(expression: &SpansetExpr, plan: fn(&FieldExpr) -> FilterPlan) -> Self {
        match expression {
            SpansetExpr::Filter(expression) => Self::Filter(plan(expression)),
            SpansetExpr::Binary { lhs, op, rhs } => Self::Binary {
                lhs: Box::new(Self::new(&lhs.value, plan)),
                op: *op,
                rhs: Box::new(Self::new(&rhs.value, plan)),
            },
        }
    }
}

/// A query with its spanset filters planned once for every trace.
#[derive(Clone, Debug)]
pub(crate) struct CompiledQuery {
    query: Query,
    spanset: SpansetPlan,
    /// Aligned with `query.stages`; set for spanset filter stages.
    stage_filters: Vec<Option<FilterPlan>>,
    columnar: bool,
}

impl CompiledQuery {
    pub(crate) fn new(query: Query) -> Self {
        Self::with_planner(query, FilterPlan::new, true)
    }

    /// Evaluates everything with the interpreter alone.
    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn interpreted(query: Query) -> Self {
        Self::with_planner(query, FilterPlan::interpreted, false)
    }

    fn with_planner(query: Query, plan: fn(&FieldExpr) -> FilterPlan, columnar: bool) -> Self {
        let spanset = SpansetPlan::new(&query.spanset.value, plan);
        let stage_filters = query
            .stages
            .iter()
            .map(|stage| match stage {
                PipelineStage::SpansetFilter(expression) => Some(plan(expression)),
                _ => None,
            })
            .collect();
        Self {
            query,
            spanset,
            stage_filters,
            columnar,
        }
    }
}

#[cfg(test)]
pub(crate) fn execute(
    trace: &Trace,
    query: &Query,
    max_spans: usize,
) -> Result<Option<TraceQlResult>, QueryError> {
    execute_compiled(trace, &CompiledQuery::new(query.clone()), max_spans)
}

pub(crate) fn execute_compiled(
    trace: &Trace,
    compiled: &CompiledQuery,
    max_spans: usize,
) -> Result<Option<TraceQlResult>, QueryError> {
    Ok(
        run(trace, compiled, max_spans, true)?.map(|(summary, spanset_count, matched_spans)| {
            TraceQlResult {
                trace_id: summary.trace_id,
                start_ns: summary.start_ns,
                end_ns: summary.end_ns,
                root_service_name: summary.root_service_name,
                root_span_name: summary.root_span_name,
                spanset_count,
                matched_spans,
            }
        }),
    )
}

/// [`execute_compiled`]'s summary, without materializing matched spans.
pub(crate) fn summarize_compiled(
    trace: &Trace,
    compiled: &CompiledQuery,
    max_spans: usize,
) -> Result<Option<TraceSummary>, QueryError> {
    Ok(run(trace, compiled, max_spans, false)?.map(|(summary, ..)| summary))
}

/// A match's summary, its spanset count, and with `spans` its matched spans.
fn run(
    trace: &Trace,
    compiled: &CompiledQuery,
    max_spans: usize,
    spans: bool,
) -> Result<Option<(TraceSummary, usize, Vec<MatchedSpan>)>, QueryError> {
    let query = &compiled.query;
    let context = TraceContext::new(trace, max_spans, compiled.columnar)?;
    for stage in &query.stages {
        if let PipelineStage::Metric { name, .. } = stage {
            return Err(QueryError::Unsupported(format!(
                "metric stage `{name}` is parsed but not executable"
            )));
        }
    }
    let mut spansets = eval_spanset(&compiled.spanset, &context)?;
    let mut selected = Vec::new();
    for (stage, filter) in query.stages.iter().zip(&compiled.stage_filters) {
        match stage {
            PipelineStage::SpansetFilter(expression) => {
                let filter = filter.as_ref().expect("spanset filter stages are planned");
                let mask = if spansets.is_empty() {
                    None
                } else {
                    filter.mask(&context)
                };
                for spanset in &mut spansets {
                    match &mask {
                        Some(mask) => spanset.retain(|&index| mask[index]),
                        None => {
                            spanset.retain(|&index| span_matches(expression, index, &context));
                        }
                    }
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
    let summary = summarize(trace, &context, matched.len());
    if !spans {
        return Ok(Some((summary, spansets.len(), Vec::new())));
    }
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
    Ok(Some((summary, spansets.len(), matched_spans)))
}

fn summarize(trace: &Trace, context: &TraceContext<'_>, matched_spans: usize) -> TraceSummary {
    let root = context.root().map(|index| &context.spans[index]);
    TraceSummary {
        trace_id: trace.trace_id,
        start_ns: context.start_ns,
        end_ns: context.end_ns,
        root_service_name: root.and_then(|root| {
            lookup_attribute(root.resource_attributes, "service.name")
                .and_then(EvalValue::into_string)
        }),
        root_span_name: root.map(|root| root.span.name.clone()),
        matched_spans,
    }
}

impl<'a> TraceContext<'a> {
    fn new(trace: &'a Trace, max_spans: usize, columnar: bool) -> Result<Self, QueryError> {
        let mut spans = Vec::new();
        let mut resources = Vec::with_capacity(trace.resource_spans.len());
        for resource_spans in &trace.resource_spans {
            let first = spans.len();
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
            resources.push(first..spans.len());
        }
        let (start_ns, end_ns) = trace.timestamp_range();
        Ok(Self {
            trace,
            spans,
            resources,
            columnar,
            by_id: OnceCell::new(),
            children: OnceCell::new(),
            nested_sets: OnceCell::new(),
            root: OnceCell::new(),
            durations: OnceCell::new(),
            statuses: OnceCell::new(),
            kinds: OnceCell::new(),
            start_ns,
            end_ns,
        })
    }

    /// Spans by span ID; a repeated ID maps to its last span.
    fn by_id(&self) -> &HashMap<&'a [u8], usize> {
        self.by_id.get_or_init(|| {
            self.spans
                .iter()
                .enumerate()
                .map(|(index, span)| (span.span.span_id.as_slice(), index))
                .collect()
        })
    }

    fn children(&self) -> &HashMap<&'a [u8], Vec<usize>> {
        self.children.get_or_init(|| {
            let mut children: HashMap<&[u8], Vec<usize>> = HashMap::new();
            for (index, span) in self.spans.iter().enumerate() {
                children
                    .entry(span.span.parent_span_id.as_slice())
                    .or_default()
                    .push(index);
            }
            children
        })
    }

    fn nested_sets(&self) -> &[NestedSet] {
        self.nested_sets
            .get_or_init(|| nested_sets(&self.spans, self.children()))
    }

    fn root(&self) -> Option<usize> {
        *self.root.get_or_init(|| {
            let by_id = self.by_id();
            self.spans
                .iter()
                .enumerate()
                .filter(|(_, span)| {
                    span.span.parent_span_id.is_empty()
                        || !by_id.contains_key(span.span.parent_span_id.as_slice())
                })
                .min_by_key(|(_, span)| (span.span.start_time_unix_nano, &span.span.span_id))
                .map(|(index, _)| index)
        })
    }
}

impl ColumnSource for TraceContext<'_> {
    fn span_count(&self) -> usize {
        self.spans.len()
    }

    fn durations(&self) -> &[i64] {
        self.durations.get_or_init(|| {
            self.spans
                .iter()
                .map(|span| columns::duration_ns(span.span))
                .collect()
        })
    }

    fn statuses(&self) -> &[u8] {
        self.statuses.get_or_init(|| {
            self.spans
                .iter()
                .map(|span| columns::status_code(span.span))
                .collect()
        })
    }

    fn kinds(&self) -> &[u8] {
        self.kinds.get_or_init(|| {
            self.spans
                .iter()
                .map(|span| columns::kind_code(span.span))
                .collect()
        })
    }

    fn names_equal(&self, value: &str, out: &mut [bool]) {
        for (out, span) in out.iter_mut().zip(&self.spans) {
            *out = span.span.name == value;
        }
    }

    fn service_names(&self, equal: bool, value: &str, out: &mut [bool]) -> Option<()> {
        let op = if equal {
            BinaryOp::Equal
        } else {
            BinaryOp::NotEqual
        };
        let literal = EvalValue::Str(value);
        for range in &self.resources {
            let Some(first) = self.spans.get(range.start) else {
                continue;
            };
            let found = lookup_attribute(first.resource_attributes, "service.name")
                .unwrap_or(EvalValue::Missing);
            out[range.clone()].fill(compare(op, &found, &literal).unwrap_or(false));
        }
        Some(())
    }

    fn dedicated(
        &self,
        _column: usize,
        _test: &columns::DedicatedTest,
        _out: &mut [bool],
    ) -> Option<()> {
        None
    }

    fn constant(&self, expression: &FieldExpr, out: &mut [bool]) -> Option<()> {
        for range in &self.resources {
            if !range.is_empty() {
                out[range.clone()].fill(span_matches(expression, range.start, self));
            }
        }
        Some(())
    }
}

/// Numbers spans depth first from each root span, assigning `left` on the way
/// down and `right` on the way up, as Tempo does at ingest.
fn nested_sets(spans: &[SpanContext<'_>], children: &HashMap<&[u8], Vec<usize>>) -> Vec<NestedSet> {
    let mut output = vec![NestedSet::default(); spans.len()];
    let mut visited = vec![false; spans.len()];
    let mut bound = 1;
    let children_of = |index: usize| {
        let span_id = spans[index].span.span_id.as_slice();
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
    plan: &SpansetPlan,
    context: &TraceContext<'_>,
) -> Result<Vec<Vec<usize>>, QueryError> {
    match plan {
        SpansetPlan::Filter(filter) => {
            let matches = match filter.mask(context) {
                Some(mask) => mask
                    .iter()
                    .enumerate()
                    .filter_map(|(index, &matched)| matched.then_some(index))
                    .collect::<Vec<_>>(),
                None => (0..context.spans.len())
                    .filter(|&index| span_matches(&filter.expression, index, context))
                    .collect::<Vec<_>>(),
            };
            Ok((!matches.is_empty())
                .then_some(matches)
                .into_iter()
                .collect())
        }
        SpansetPlan::Binary { lhs, op, rhs } => {
            let left = eval_spanset(lhs, context)?
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            let right = eval_spanset(rhs, context)?
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
                .by_id()
                .get(span.parent_span_id.as_slice())
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
    let by_id = context.by_id();
    let mut visited = HashSet::from([index]);
    let mut chain = Vec::new();
    while let Some(&parent) = by_id.get(context.spans[index].span.parent_span_id.as_slice()) {
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

/// A value under evaluation. Strings borrow from the trace or the query, so
/// comparing them per span allocates nothing.
#[derive(Clone, Debug, PartialEq)]
enum EvalValue<'v> {
    Missing,
    Str(&'v str),
    Static(StaticValue),
}

impl EvalValue<'_> {
    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Static(StaticValue::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(value) => Some(value),
            Self::Static(StaticValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn into_static(self) -> Option<StaticValue> {
        match self {
            Self::Missing => None,
            Self::Str(value) => Some(StaticValue::String(value.to_owned())),
            Self::Static(value) => Some(value),
        }
    }

    fn into_string(self) -> Option<String> {
        match self {
            Self::Str(value) => Some(value.to_owned()),
            Self::Static(StaticValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn into_owned(self) -> EvalValue<'static> {
        match self.into_static() {
            Some(value) => EvalValue::Static(value),
            None => EvalValue::Missing,
        }
    }

    fn key(&self) -> String {
        match self {
            Self::Missing => "missing".to_owned(),
            Self::Str(value) => format!("{:?}", StaticValue::String((*value).to_owned())),
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

fn eval_expr<'v, 'a: 'v>(
    expression: &'v FieldExpr,
    index: usize,
    context: &TraceContext<'a>,
) -> Result<EvalValue<'v>, QueryError> {
    match &expression.value {
        Expr::Static(StaticValue::String(value)) => Ok(EvalValue::Str(value)),
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

fn eval_intrinsic<'a>(
    intrinsic: Intrinsic,
    index: usize,
    context: &TraceContext<'a>,
) -> EvalValue<'a> {
    let span = context.spans[index].span;
    let value = match intrinsic {
        Intrinsic::TraceId => StaticValue::String(context.trace.trace_id.to_string()),
        Intrinsic::SpanId => StaticValue::String(hex(&span.span_id)),
        Intrinsic::ParentId => StaticValue::String(hex(&span.parent_span_id)),
        Intrinsic::Name => return EvalValue::Str(&span.name),
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
        Intrinsic::StatusMessage => {
            return EvalValue::Str(span.status.as_ref().map_or("", |s| s.message.as_str()));
        }
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
                .root()
                .map_or_else(String::new, |root| context.spans[root].span.name.clone()),
        ),
        Intrinsic::RootServiceName => StaticValue::String(
            context
                .root()
                .and_then(|root| {
                    lookup_attribute(context.spans[root].resource_attributes, "service.name")
                        .and_then(EvalValue::into_string)
                })
                .unwrap_or_default(),
        ),
        Intrinsic::ChildCount => StaticValue::Int(
            context
                .children()
                .get(span.span_id.as_slice())
                .map_or(0, Vec::len)
                .try_into()
                .unwrap_or(i64::MAX),
        ),
        Intrinsic::TraceDuration => StaticValue::Duration(
            i64::try_from(context.end_ns.saturating_sub(context.start_ns)).unwrap_or(i64::MAX),
        ),
        Intrinsic::InstrumentationName => {
            return EvalValue::Str(
                context.spans[index]
                    .scope
                    .map_or("", |scope| scope.name.as_str()),
            );
        }
        Intrinsic::InstrumentationVersion => {
            return EvalValue::Str(
                context.spans[index]
                    .scope
                    .map_or("", |scope| scope.version.as_str()),
            );
        }
        Intrinsic::NestedSetLeft => StaticValue::Int(context.nested_sets()[index].left),
        Intrinsic::NestedSetRight => StaticValue::Int(context.nested_sets()[index].right),
        Intrinsic::NestedSetParent => StaticValue::Int(context.nested_sets()[index].parent),
    };
    EvalValue::Static(value)
}

fn lookup_attribute<'a>(attributes: &'a [KeyValue], name: &str) -> Option<EvalValue<'a>> {
    let value = attributes
        .iter()
        .find(|attribute| attribute.key == name)?
        .value
        .as_ref()?;
    match value.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(EvalValue::Str(value)),
        _ => any_value(value).map(EvalValue::Static),
    }
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
        crate::AttributeValue::String(value) => EvalValue::Str(value),
        crate::AttributeValue::Bool(value) => EvalValue::Static(StaticValue::Bool(*value)),
        crate::AttributeValue::Int(value) => EvalValue::Static(StaticValue::Int(*value)),
        crate::AttributeValue::Double(value) => EvalValue::Static(StaticValue::Float(*value)),
    };
    let literal = match literal {
        StaticValue::String(literal) => EvalValue::Str(literal),
        literal => EvalValue::Static(literal.clone()),
    };
    compare(op, &value, &literal).unwrap_or(false)
}

/// Like Tempo, only a `nil` comparison can succeed against a missing value:
/// `span.a != 1` is false for a span without `span.a`.
fn compare(op: BinaryOp, left: &EvalValue<'_>, right: &EvalValue<'_>) -> Option<bool> {
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return match op {
            BinaryOp::Equal => Some(left == right),
            BinaryOp::NotEqual => Some(left != right),
            BinaryOp::Less => Some(left < right),
            BinaryOp::LessEqual => Some(left <= right),
            BinaryOp::Greater => Some(left > right),
            BinaryOp::GreaterEqual => Some(left >= right),
            BinaryOp::Regex => regex_is_match(right, left),
            BinaryOp::NotRegex => regex_is_match(right, left).map(|matched| !matched),
            _ => None,
        };
    }
    if matches!(left, EvalValue::Str(_)) || matches!(right, EvalValue::Str(_)) {
        return compare(op, &left.clone().into_owned(), &right.clone().into_owned());
    }
    let nil = |value: &EvalValue<'_>| matches!(value, EvalValue::Static(StaticValue::Nil));
    if matches!(op, BinaryOp::Equal | BinaryOp::NotEqual) && (nil(left) || nil(right)) {
        let equal = matches!(
            (left, right),
            (EvalValue::Missing, EvalValue::Static(StaticValue::Nil))
                | (EvalValue::Static(StaticValue::Nil), EvalValue::Missing)
                | (
                    EvalValue::Static(StaticValue::Nil),
                    EvalValue::Static(StaticValue::Nil)
                )
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

fn arithmetic(
    op: BinaryOp,
    left: EvalValue<'_>,
    right: EvalValue<'_>,
) -> Result<EvalValue<'static>, QueryError> {
    let (EvalValue::Static(left), EvalValue::Static(right)) =
        (left.into_owned(), right.into_owned())
    else {
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

fn eval_scalar<'v, 'a: 'v>(
    expression: &'v ScalarExpr,
    spanset: &[usize],
    context: &TraceContext<'a>,
) -> Result<EvalValue<'v>, QueryError> {
    match expression {
        ScalarExpr::Static(value) => Ok(EvalValue::Static(value.clone())),
        ScalarExpr::Aggregate { op, field } => {
            if *op == AggregateOp::Count {
                return Ok(EvalValue::Static(StaticValue::Int(
                    spanset.len().try_into().unwrap_or(i64::MAX),
                )));
            }
            let field = field.as_ref().expect("validated numeric aggregate field");
            let values = if context.columnar
                && matches!(field.value, Expr::Intrinsic(Intrinsic::Duration))
            {
                let durations = context.durations();
                spanset
                    .iter()
                    .map(|&index| durations[index] as f64)
                    .collect::<Vec<_>>()
            } else {
                spanset
                    .iter()
                    .filter_map(|&index| eval_expr(field, index, context).ok())
                    .filter_map(|value| match value {
                        EvalValue::Static(StaticValue::Int(value)) => Some(value as f64),
                        EvalValue::Static(StaticValue::Float(value)) => Some(value),
                        EvalValue::Static(StaticValue::Duration(value)) => Some(value as f64),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
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
