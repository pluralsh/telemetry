//! Choosing the parts of a PromQL query that shards can evaluate alone.
//!
//! A shard holds every sample of the series placed on it, so work that is
//! done per series is exact where the series is stored, and aggregations
//! that can be combined from partial results can be computed on each shard
//! and merged. [`plan`] finds the largest such subtrees: an aggregation on
//! the explicit support list over per-series operations only. Each becomes
//! a [`Fragment`] of queries every shard evaluates, and is replaced in the
//! coordinator's query by selectors named `__pushdown_<index>` that stand
//! for the partials, with the same aggregation re-applied over them.
//! Anything off the support list is left for the coordinator to evaluate
//! over raw samples, as without pushdown.
//!
//! | Aggregation        | Each shard returns         | Coordinator combines    |
//! |--------------------|----------------------------|-------------------------|
//! | sum, min, max, group | the same aggregation     | the same again          |
//! | count              | count                      | sum                     |
//! | avg                | sum, count                 | sum ÷ sum               |
//! | stddev, stdvar     | sum, sum of squares, count | the usual formula       |
//! | topk, bottomk      | its local top k            | the top k of those      |

use promql_parser::parser::{self, Expr, LabelModifier};

use crate::promql::plan::pushdown_selector_name;

/// What shards evaluate for one pushed-down aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    /// Index of this fragment's first partial; its queries' partials are
    /// `first_index..first_index + queries.len()`.
    pub first_index: usize,
    /// Queries each shard evaluates over the whole step grid.
    pub queries: Vec<String>,
}

/// A query with its pushed-down parts replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pushdown {
    /// The coordinator's query, over `__pushdown_<index>` selectors.
    pub query: String,
    pub fragments: Vec<Fragment>,
}

impl Pushdown {
    /// Every partial's shard query, in index order.
    pub fn queries(&self) -> Vec<&str> {
        self.fragments
            .iter()
            .flat_map(|fragment| fragment.queries.iter().map(String::as_str))
            .collect()
    }
}

/// Functions that map each input series to an output series on its own,
/// reading only that series' samples.
const PER_SERIES_FUNCTIONS: &[&str] = &[
    "rate",
    "irate",
    "increase",
    "delta",
    "idelta",
    "resets",
    "changes",
    "deriv",
    "predict_linear",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "sum_over_time",
    "count_over_time",
    "last_over_time",
    "present_over_time",
    "stddev_over_time",
    "stdvar_over_time",
    "quantile_over_time",
    "abs",
    "ceil",
    "floor",
    "round",
    "exp",
    "ln",
    "log2",
    "log10",
    "sqrt",
    "sign",
    "clamp",
    "clamp_min",
    "clamp_max",
    "deg",
    "rad",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "sinh",
    "cosh",
    "tanh",
    "asinh",
    "acosh",
    "atanh",
    "label_replace",
    "label_join",
];

/// Plans pushdown for `query`, or `None` when nothing can be pushed down or
/// the query uses `@`, whose start/end pinning depends on the whole query's
/// range.
pub fn plan(query: &str) -> Option<Pushdown> {
    let mut expr = parser::parse(query).ok()?;
    if uses_at(&expr) {
        return None;
    }
    let mut fragments = Vec::new();
    let mut next_index = 0;
    rewrite(&mut expr, &mut fragments, &mut next_index);
    if fragments.is_empty() {
        return None;
    }
    Some(Pushdown {
        query: expr.to_string(),
        fragments,
    })
}

fn rewrite(expr: &mut Expr, fragments: &mut Vec<Fragment>, next_index: &mut usize) {
    if let Expr::Aggregate(aggregate) = expr
        && let Some((queries, combine)) = split_aggregate(aggregate)
    {
        let first_index = *next_index;
        *next_index += queries.len();
        let text = combine(first_index);
        if let Ok(replacement) = parser::parse(&text) {
            fragments.push(Fragment {
                first_index,
                queries,
            });
            *expr = Expr::Paren(parser::ParenExpr {
                expr: Box::new(replacement),
            });
            return;
        }
        *next_index = first_index;
    }
    match expr {
        Expr::Aggregate(aggregate) => {
            rewrite(&mut aggregate.expr, fragments, next_index);
            if let Some(param) = &mut aggregate.param {
                rewrite(param, fragments, next_index);
            }
        }
        Expr::Unary(unary) => rewrite(&mut unary.expr, fragments, next_index),
        Expr::Binary(binary) => {
            rewrite(&mut binary.lhs, fragments, next_index);
            rewrite(&mut binary.rhs, fragments, next_index);
        }
        Expr::Paren(paren) => rewrite(&mut paren.expr, fragments, next_index),
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                rewrite(arg, fragments, next_index);
            }
        }
        // A subquery's inner grid is its own: never cut inside it.
        Expr::Subquery(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::Extension(_) => {}
    }
}

type Combine = Box<dyn Fn(usize) -> String>;

/// The queries shards evaluate for `aggregate`, and the coordinator's
/// replacement text given the index of the first partial.
fn split_aggregate(aggregate: &parser::AggregateExpr) -> Option<(Vec<String>, Combine)> {
    if !per_series(&aggregate.expr) || !has_selector(&aggregate.expr) {
        return None;
    }
    let op = aggregate.op.to_string();
    let grouping = grouping(aggregate.modifier.as_ref());
    let inner = aggregate.expr.to_string();
    let pd = |index: usize| pushdown_selector_name(index);
    match op.as_str() {
        "sum" | "min" | "max" | "group" => {
            if aggregate.param.is_some() {
                return None;
            }
            let queries = vec![format!("{op}{grouping}({inner})")];
            Some((
                queries,
                Box::new(move |first| format!("{op}{grouping}({})", pd(first))),
            ))
        }
        "count" => {
            if aggregate.param.is_some() {
                return None;
            }
            let queries = vec![format!("count{grouping}({inner})")];
            Some((
                queries,
                Box::new(move |first| format!("sum{grouping}({})", pd(first))),
            ))
        }
        "avg" => {
            if aggregate.param.is_some() {
                return None;
            }
            let queries = vec![
                format!("sum{grouping}({inner})"),
                format!("count{grouping}({inner})"),
            ];
            Some((
                queries,
                Box::new(move |first| {
                    format!(
                        "sum{grouping}({}) / sum{grouping}({})",
                        pd(first),
                        pd(first + 1)
                    )
                }),
            ))
        }
        "stddev" | "stdvar" => {
            if aggregate.param.is_some() {
                return None;
            }
            let queries = vec![
                format!("sum{grouping}({inner})"),
                format!("sum{grouping}(({inner}) ^ 2)"),
                format!("count{grouping}({inner})"),
            ];
            let root = op == "stddev";
            Some((
                queries,
                Box::new(move |first| {
                    let (sum, squares, count) = (
                        format!("sum{grouping}({})", pd(first)),
                        format!("sum{grouping}({})", pd(first + 1)),
                        format!("sum{grouping}({})", pd(first + 2)),
                    );
                    let variance =
                        format!("clamp_min(({squares} / {count}) - (({sum} / {count}) ^ 2), 0)");
                    if root {
                        format!("sqrt({variance})")
                    } else {
                        variance
                    }
                }),
            ))
        }
        "topk" | "bottomk" => {
            let param = aggregate.param.as_deref()?;
            let Expr::NumberLiteral(k) = unparen(param) else {
                return None;
            };
            let k = k.val;
            let queries = vec![format!("{op}{grouping}({k}, {inner})")];
            Some((
                queries,
                Box::new(move |first| format!("{op}{grouping}({k}, {})", pd(first))),
            ))
        }
        _ => None,
    }
}

fn grouping(modifier: Option<&LabelModifier>) -> String {
    let list = |labels: &promql_parser::label::Labels| labels.labels.join(", ");
    match modifier {
        None => String::new(),
        Some(LabelModifier::Include(labels)) => format!(" by ({})", list(labels)),
        Some(LabelModifier::Exclude(labels)) => format!(" without ({})", list(labels)),
    }
}

fn unparen(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(paren) => unparen(&paren.expr),
        other => other,
    }
}

/// Whether `expr` maps each series to an output series on its own.
fn per_series(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => true,
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => true,
        Expr::Paren(paren) => per_series(&paren.expr),
        Expr::Unary(unary) => per_series(&unary.expr),
        Expr::Call(call) => {
            PER_SERIES_FUNCTIONS.contains(&call.func.name)
                && call.args.args.iter().all(|arg| per_series(arg))
        }
        Expr::Binary(binary) => {
            // One side must be a scalar literal: vector/vector matching can
            // pair series that live on different shards.
            !binary.op.is_set_operator()
                && per_series(&binary.lhs)
                && per_series(&binary.rhs)
                && (is_number(&binary.lhs) != is_number(&binary.rhs))
        }
        Expr::Aggregate(_) | Expr::Subquery(_) | Expr::Extension(_) => false,
    }
}

fn is_number(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(_) => true,
        Expr::Paren(paren) => is_number(&paren.expr),
        Expr::Unary(unary) => is_number(&unary.expr),
        _ => false,
    }
}

fn has_selector(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => true,
        Expr::Paren(paren) => has_selector(&paren.expr),
        Expr::Unary(unary) => has_selector(&unary.expr),
        Expr::Call(call) => call.args.args.iter().any(|arg| has_selector(arg)),
        Expr::Binary(binary) => has_selector(&binary.lhs) || has_selector(&binary.rhs),
        _ => false,
    }
}

fn uses_at(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(vs) => vs.at.is_some(),
        Expr::MatrixSelector(ms) => ms.vs.at.is_some(),
        Expr::Subquery(sq) => sq.at.is_some() || uses_at(&sq.expr),
        Expr::Paren(paren) => uses_at(&paren.expr),
        Expr::Unary(unary) => uses_at(&unary.expr),
        Expr::Call(call) => call.args.args.iter().any(|arg| uses_at(arg)),
        Expr::Binary(binary) => uses_at(&binary.lhs) || uses_at(&binary.rhs),
        Expr::Aggregate(aggregate) => {
            uses_at(&aggregate.expr) || aggregate.param.as_deref().is_some_and(uses_at)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pushed(query: &str) -> Pushdown {
        plan(query).unwrap_or_else(|| panic!("{query} should push down"))
    }

    #[test]
    fn sum_of_rates_is_pushed_and_reapplied() {
        let plan = pushed("sum by (job) (rate(http_requests_total[5m]))");
        assert_eq!(
            plan.queries(),
            vec!["sum by (job)(rate(http_requests_total[5m]))"]
        );
        assert!(plan.query.contains("__pushdown_0"), "{}", plan.query);
        assert!(plan.query.contains("sum by (job)"), "{}", plan.query);
    }

    #[test]
    fn binary_operands_are_pushed_separately() {
        let plan = pushed("sum(rate(a[1m])) / sum(rate(b[1m]))");
        assert_eq!(plan.fragments.len(), 2);
        assert_eq!(plan.queries(), vec!["sum(rate(a[1m]))", "sum(rate(b[1m]))"]);
        assert!(plan.query.contains("__pushdown_0"));
        assert!(plan.query.contains("__pushdown_1"));
    }

    #[test]
    fn avg_and_variance_split_into_parts() {
        assert_eq!(pushed("avg(x)").queries().len(), 2);
        assert_eq!(pushed("stddev by (a) (x)").queries().len(), 3);
        assert_eq!(pushed("count without (a) (x)").queries().len(), 1);
    }

    #[test]
    fn topk_needs_a_literal_k() {
        assert_eq!(pushed("topk(3, rate(x[1m]))").queries().len(), 1);
        assert!(plan("topk(scalar(y), x)").is_none());
    }

    #[test]
    fn outside_the_support_list_is_left_alone() {
        for query in [
            "x",
            "rate(x[5m])",
            "quantile(0.9, x)",
            "count_values(\"v\", x)",
            "sum(x @ 100)",
            "sum(x) @ 100",
            "sum(a + b)",
            "sum(a and b)",
            "sum(timestamp(x))",
            "sum(absent(x))",
            "sum(max_over_time(rate(x[1m])[10m:1m]))",
            "sum(histogram_quantile(0.9, x))",
        ] {
            assert!(plan(query).is_none(), "{query}");
        }
    }

    #[test]
    fn nests_around_unsupported_operators() {
        // The vector/vector sum cannot be pushed, but its aggregated
        // operands can.
        let plan = pushed("sum(sum by (a) (x) + sum by (a) (y))");
        assert_eq!(plan.fragments.len(), 2);
        let plan = pushed("histogram_quantile(0.9, sum by (le) (rate(b[5m])))");
        assert_eq!(plan.fragments.len(), 1);
        assert!(plan.query.starts_with("histogram_quantile"));
    }
}
