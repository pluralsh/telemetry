//! Lowering step: turns a `promql_parser::Expr` (the parse tree) into a
//! [`LogicalPlan`] (the engine's rewrite surface). Pure function of
//! `(Expr, LoweringContext)` — no series resolution, no group maps, no
//! [`SeriesSource`](super::super::source::SeriesSource) binding. That's
//! all the physical planner's job.
//!
//! The lowering does fold plan-time constants that are passed as
//! function arguments in PromQL but logically belong in the operator-kind
//! enum (`clamp` bounds, `quantile_over_time` q, `topk` k). Non-literal
//! values in those argument positions error with
//! [`PlanError::InvalidArgument`].
//!
//! Constant folding, CSE, and type-checking beyond operator-enum
//! coverage happen elsewhere ([`mod@super::optimize`] and the physical
//! planner).

use std::collections::HashSet;
use std::sync::Arc;

use crate::model::{Label, Labels};
use crate::promql::trace::TraceCollector;
use promql_parser::label::MatchOp;
use promql_parser::parser;
use promql_parser::parser::LabelModifier;
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_DIV, T_EQLC, T_GROUP, T_GTE,
    T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MAX, T_MIN, T_MOD, T_MUL, T_NEQ, T_POW,
    T_QUANTILE, T_STDDEV, T_STDVAR, T_SUB, T_SUM, T_TOPK,
};

use crate::promql::operators::aggregate::AggregateKind;
use crate::promql::operators::binary::BinaryOpKind;
use crate::promql::operators::histogram::HistogramFnKind;
use crate::promql::operators::instant_fn::InstantFnKind;
use crate::promql::operators::label_manip::LabelManipKind;
use crate::promql::operators::rollup::RollupKind;
use regex::Regex;

use super::error::PlanError;
use super::plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, InstantVectorSort, LogicalPlan,
    MatchingAxis, Offset,
};

// ---------------------------------------------------------------------------
// LoweringContext
// ---------------------------------------------------------------------------

/// The query's plan-time parameters — evaluation range, step, lookback,
/// and parallelism policy — that the caller supplies to [`lower`] and
/// later to
/// [`build_physical_plan`](super::physical::build_physical_plan). One
/// context travels with the plan through every pass.
#[derive(Debug, Clone)]
pub struct LoweringContext {
    pub start_ms: i64,
    /// Equals `start_ms` for instant queries.
    pub end_ms: i64,
    /// `1` sentinel for instant queries — consult [`Self::is_instant`] before
    /// dividing.
    pub step_ms: i64,
    pub lookback_delta_ms: i64,
    /// Consulted by the physical planner; lowering ignores this field.
    pub parallelism: super::parallelism::Parallelism,
    /// `Some` → physical planner wraps every operator in a
    /// [`TracingOperator`](crate::promql::trace::TracingOperator).
    pub trace: Option<Arc<TraceCollector>>,
}

impl PartialEq for LoweringContext {
    fn eq(&self, other: &Self) -> bool {
        // Trace collector is intentionally excluded — it is observability
        // state, not part of the plan's identity.
        self.start_ms == other.start_ms
            && self.end_ms == other.end_ms
            && self.step_ms == other.step_ms
            && self.lookback_delta_ms == other.lookback_delta_ms
            && self.parallelism == other.parallelism
    }
}

impl Eq for LoweringContext {}

impl LoweringContext {
    pub fn new(start_ms: i64, end_ms: i64, step_ms: i64, lookback_delta_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_delta_ms,
            parallelism: super::parallelism::Parallelism::default(),
            trace: None,
        }
    }

    /// Physical planner treats `start == end` as instant; `step_ms` is a
    /// non-zero sentinel.
    pub fn for_instant(t_ms: i64, lookback_delta_ms: i64) -> Self {
        Self {
            start_ms: t_ms,
            end_ms: t_ms,
            step_ms: 1,
            lookback_delta_ms,
            parallelism: super::parallelism::Parallelism::default(),
            trace: None,
        }
    }

    pub fn with_parallelism(mut self, parallelism: super::parallelism::Parallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Attach a trace collector. Chainable.
    pub fn with_trace(mut self, trace: Arc<TraceCollector>) -> Self {
        self.trace = Some(trace);
        self
    }

    /// `true` if `start_ms == end_ms` (a one-step query).
    #[inline]
    pub fn is_instant(&self) -> bool {
        self.start_ms == self.end_ms
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Lower a parsed PromQL expression into a [`LogicalPlan`].
///
/// See the module docs for the lowering rules.
pub fn lower(expr: &parser::Expr, ctx: &LoweringContext) -> Result<LogicalPlan, PlanError> {
    match expr {
        parser::Expr::VectorSelector(vs) => Ok(LogicalPlan::VectorSelector {
            selector: vs.clone(),
            offset: offset_from_parser(vs.offset.as_ref()),
            at: at_from_parser(vs.at.as_ref())?,
            lookback_ms: Some(ctx.lookback_delta_ms),
        }),
        parser::Expr::MatrixSelector(ms) => Ok(LogicalPlan::MatrixSelector {
            selector: ms.vs.clone(),
            range_ms: ms.range.as_millis() as i64,
            offset: offset_from_parser(ms.vs.offset.as_ref()),
            at: at_from_parser(ms.vs.at.as_ref())?,
        }),
        parser::Expr::NumberLiteral(n) => Ok(LogicalPlan::Scalar(n.val)),
        parser::Expr::StringLiteral(_) => Err(PlanError::InvalidTopLevelString),
        parser::Expr::Paren(p) => lower(&p.expr, ctx),
        parser::Expr::Unary(u) => {
            // Represent `-expr` as `-1 * expr`. Preserves every downstream
            // operator path (scalar/scalar folds via the binop; vector gets
            // a pointwise multiply). Decision logged in §5.
            let child = lower(&u.expr, ctx)?;
            Ok(LogicalPlan::Binary {
                op: BinaryOpKind::Mul,
                lhs: Box::new(LogicalPlan::Scalar(-1.0)),
                rhs: Box::new(child),
                matching: None,
            })
        }
        parser::Expr::Call(call) => lower_call(call, ctx),
        parser::Expr::Aggregate(agg) => lower_aggregate(agg, ctx),
        parser::Expr::Binary(b) => lower_binary(b, ctx),
        parser::Expr::Subquery(sq) => {
            // `step: Option<Duration>` — `None` means "use the global
            // evaluation interval". For v1 that global interval is
            // `ctx.step_ms` for range queries; instant queries default to
            // 1000ms (same as Prometheus' `-query.default-step`).
            let step_ms = match sq.step {
                Some(s) => s.as_millis() as i64,
                None => {
                    if ctx.is_instant() {
                        1000
                    } else {
                        ctx.step_ms
                    }
                }
            };
            Ok(LogicalPlan::Subquery {
                child: Box::new(lower(&sq.expr, ctx)?),
                range_ms: sq.range.as_millis() as i64,
                step_ms,
                offset: offset_from_parser(sq.offset.as_ref()),
                at: at_from_parser(sq.at.as_ref())?,
            })
        }
        parser::Expr::Extension(_) => Err(PlanError::UnsupportedFeature(
            "Expr::Extension not supported".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Offset / @ helpers
// ---------------------------------------------------------------------------

fn offset_from_parser(parser_offset: Option<&parser::Offset>) -> Offset {
    match parser_offset {
        None => Offset::Pos(0),
        Some(o) => Offset::from(o),
    }
}

fn at_from_parser(parser_at: Option<&parser::AtModifier>) -> Result<Option<AtModifier>, PlanError> {
    match parser_at {
        None => Ok(None),
        Some(at) => AtModifier::try_from(at)
            .map(Some)
            .map_err(|msg| PlanError::UnsupportedFeature(msg.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Function call dispatch (Rollup / InstantFn)
// ---------------------------------------------------------------------------

fn lower_call(call: &parser::Call, ctx: &LoweringContext) -> Result<LogicalPlan, PlanError> {
    let name = call.func.name;
    let args: &[Box<parser::Expr>] = &call.args.args;

    match name {
        "pi" => {
            if !args.is_empty() {
                return Err(PlanError::InvalidArgument {
                    function: "pi".to_string(),
                    expected: "no arguments".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            return Ok(LogicalPlan::Scalar(std::f64::consts::PI));
        }
        "time" => {
            if !args.is_empty() {
                return Err(PlanError::InvalidArgument {
                    function: "time".to_string(),
                    expected: "no arguments".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            return Ok(LogicalPlan::Time);
        }
        "vector" => {
            if args.len() != 1 {
                return Err(PlanError::InvalidArgument {
                    function: "vector".to_string(),
                    expected: "exactly one scalar argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let child = lower(&args[0], ctx)?;
            if !child.produces_scalar() {
                return Err(PlanError::InvalidArgument {
                    function: "vector".to_string(),
                    expected: "scalar argument".to_string(),
                    got: describe_logical_plan(&child),
                });
            }
            return Ok(LogicalPlan::Vectorize {
                child: Box::new(child),
            });
        }
        "scalar" => {
            if args.len() != 1 {
                return Err(PlanError::InvalidArgument {
                    function: "scalar".to_string(),
                    expected: "exactly one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let child = lower(&args[0], ctx)?;
            if child.produces_matrix() {
                return Err(PlanError::InvalidArgument {
                    function: "scalar".to_string(),
                    expected: "instant-vector argument".to_string(),
                    got: describe_logical_plan(&child),
                });
            }
            return Ok(if child.produces_scalar() {
                child
            } else {
                LogicalPlan::Scalarize {
                    child: Box::new(child),
                }
            });
        }
        "sort" | "sort_desc" => {
            let order = if name == "sort" {
                InstantVectorSort::AscendingValue
            } else {
                InstantVectorSort::DescendingValue
            };
            let child = lower_instant_vector_arg(name, args, 0, 1, ctx)?;
            return Ok(LogicalPlan::Sort {
                order,
                child: Box::new(child),
            });
        }
        "histogram_quantile" => {
            let child = lower_instant_vector_arg(name, args, 1, 2, ctx)?;
            let q = expect_number_literal(&args[0], name, "literal numeric `φ`")?;
            return Ok(LogicalPlan::Histogram {
                kind: HistogramFnKind::Quantile(q),
                child: Box::new(child),
            });
        }
        "histogram_fraction" => {
            let child = lower_instant_vector_arg(name, args, 2, 3, ctx)?;
            let lower = expect_number_literal(&args[0], name, "literal numeric `lower`")?;
            let upper = expect_number_literal(&args[1], name, "literal numeric `upper`")?;
            return Ok(LogicalPlan::Histogram {
                kind: HistogramFnKind::Fraction { lower, upper },
                child: Box::new(child),
            });
        }
        "absent" => {
            let child = lower_instant_vector_arg(name, args, 0, 1, ctx)?;
            return Ok(LogicalPlan::Absent {
                labels: absent_labels(&args[0]),
                child: Box::new(child),
            });
        }
        "absent_over_time" => {
            let matrix = match args {
                [arg] => lower(arg, ctx)?,
                _ => {
                    return Err(PlanError::InvalidArgument {
                        function: name.to_string(),
                        expected: "exactly one range-vector argument".to_string(),
                        got: format!("{} arguments", args.len()),
                    });
                }
            };
            if !matrix.produces_matrix() {
                return Err(PlanError::InvalidArgument {
                    function: name.to_string(),
                    expected: "range-vector (matrix) argument".to_string(),
                    got: describe_logical_plan(&matrix),
                });
            }
            return Ok(LogicalPlan::Absent {
                labels: absent_labels(&args[0]),
                child: Box::new(LogicalPlan::Rollup {
                    kind: RollupKind::CountOverTime,
                    child: Box::new(matrix),
                }),
            });
        }
        _ => {}
    }

    // ---- Range-vector functions (→ Rollup) -----------------------------
    if let Some(kind) = rollup_kind_for(name, args)? {
        if args.is_empty() {
            return Err(PlanError::InvalidArgument {
                function: name.to_string(),
                expected: "at least one argument".to_string(),
                got: "no arguments".to_string(),
            });
        }
        // For `quantile_over_time(q, v)` the matrix selector is args[1]; for
        // every other rollup it is args[0].
        let matrix_arg = match name {
            "quantile_over_time" => &args[1],
            _ => &args[0],
        };
        let child = lower(matrix_arg, ctx)?;
        if !child.produces_matrix() {
            return Err(PlanError::InvalidArgument {
                function: name.to_string(),
                expected: "range-vector (matrix) argument".to_string(),
                got: describe_logical_plan(&child),
            });
        }
        return Ok(LogicalPlan::Rollup {
            kind,
            child: Box::new(child),
        });
    }

    // ---- Label-manipulation functions ----------------------------------
    if let Some(kind) = label_manip_kind_for(name, args)? {
        let child = lower(&args[0], ctx)?;
        if child.produces_matrix() || child.produces_scalar() {
            return Err(PlanError::InvalidArgument {
                function: name.to_string(),
                expected: "instant-vector argument".to_string(),
                got: describe_logical_plan(&child),
            });
        }
        return Ok(LogicalPlan::LabelManip {
            kind,
            child: Box::new(child),
        });
    }

    // ---- Instant scalar functions (→ InstantFn) ------------------------
    if let Some(kind) = instant_fn_kind_for(name, args)? {
        let child = if args.is_empty() {
            // Calendar/date functions default to `vector(time())` when no
            // argument is supplied. `time()` itself is scalar in PromQL;
            // `vector()` is the explicit coercion to a one-series vector.
            LogicalPlan::Vectorize {
                child: Box::new(LogicalPlan::Time),
            }
        } else {
            lower(&args[0], ctx)?
        };
        if child.produces_matrix() {
            return Err(PlanError::InvalidArgument {
                function: name.to_string(),
                expected: "instant-vector argument".to_string(),
                got: describe_logical_plan(&child),
            });
        }
        return Ok(LogicalPlan::InstantFn {
            kind,
            child: Box::new(child),
        });
    }

    Err(PlanError::UnknownFunction(name.to_string()))
}

/// Check `name` was called with `arity` arguments and lower `args[index]`,
/// which must be an instant vector.
fn lower_instant_vector_arg(
    name: &str,
    args: &[Box<parser::Expr>],
    index: usize,
    arity: usize,
    ctx: &LoweringContext,
) -> Result<LogicalPlan, PlanError> {
    if args.len() != arity {
        return Err(PlanError::InvalidArgument {
            function: name.to_string(),
            expected: format!("exactly {arity} argument(s)"),
            got: format!("{} arguments", args.len()),
        });
    }
    let child = lower(&args[index], ctx)?;
    if child.produces_matrix() || child.produces_scalar() {
        return Err(PlanError::InvalidArgument {
            function: name.to_string(),
            expected: "instant-vector argument".to_string(),
            got: describe_logical_plan(&child),
        });
    }
    Ok(child)
}

/// Output labels for `absent` / `absent_over_time`, per Prometheus'
/// `createLabelsForAbsentFunction`: the first equality matcher of each
/// label except `__name__`, dropping any label that has a second matcher.
/// Arguments other than a bare selector yield no labels.
fn absent_labels(arg: &parser::Expr) -> Labels {
    let selector = match arg {
        parser::Expr::VectorSelector(vs) => vs,
        parser::Expr::MatrixSelector(ms) => &ms.vs,
        _ => return Labels::empty(),
    };
    let mut labels: Vec<Label> = Vec::new();
    let mut set: HashSet<&str> = HashSet::new();
    for matcher in &selector.matchers.matchers {
        if matcher.name == "__name__" {
            continue;
        }
        if matches!(matcher.op, MatchOp::Equal) && set.insert(&matcher.name) {
            labels.push(Label::new(&matcher.name, &matcher.value));
        } else {
            labels.retain(|label| label.name != matcher.name);
        }
    }
    labels.sort();
    Labels::new(labels)
}

fn label_manip_kind_for(
    name: &str,
    args: &[Box<parser::Expr>],
) -> Result<Option<LabelManipKind>, PlanError> {
    let kind = match name {
        "label_replace" => {
            if args.len() != 5 {
                return Err(PlanError::InvalidArgument {
                    function: "label_replace".to_string(),
                    expected: "exactly five arguments".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let dst_label = expect_string_literal(&args[1], "label_replace")?;
            if !is_valid_label_name(&dst_label) {
                return Err(PlanError::InvalidArgument {
                    function: "label_replace".to_string(),
                    expected: "non-empty destination label name".to_string(),
                    got: format!("label {:?}", dst_label),
                });
            }
            let replacement = expect_string_literal(&args[2], "label_replace")?;
            let src_label = expect_string_literal(&args[3], "label_replace")?;
            let regex = expect_string_literal(&args[4], "label_replace")?;
            Regex::new(&format!("^(?s:{regex})$")).map_err(|err| PlanError::InvalidArgument {
                function: "label_replace".to_string(),
                expected: "valid regular expression".to_string(),
                got: err.to_string(),
            })?;
            LabelManipKind::Replace {
                dst_label,
                replacement,
                src_label,
                regex,
            }
        }
        "label_join" => {
            if args.len() < 3 {
                return Err(PlanError::InvalidArgument {
                    function: "label_join".to_string(),
                    expected: "at least three arguments".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let dst_label = expect_string_literal(&args[1], "label_join")?;
            if !is_valid_label_name(&dst_label) {
                return Err(PlanError::InvalidArgument {
                    function: "label_join".to_string(),
                    expected: "non-empty destination label name".to_string(),
                    got: format!("label {:?}", dst_label),
                });
            }
            let separator = expect_string_literal(&args[2], "label_join")?;
            let src_labels = args[3..]
                .iter()
                .map(|arg| expect_string_literal(arg, "label_join"))
                .collect::<Result<Vec<_>, _>>()?;
            LabelManipKind::Join {
                dst_label,
                separator,
                src_labels: Arc::from(src_labels),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(kind))
}

/// If `name` names a range-vector function, return the `RollupKind` to
/// emit. `args` is inspected when the kind carries a plan-time scalar
/// (`quantile_over_time`).
fn rollup_kind_for(
    name: &str,
    args: &[Box<parser::Expr>],
) -> Result<Option<RollupKind>, PlanError> {
    let kind = match name {
        "rate" => RollupKind::Rate,
        "increase" => RollupKind::Increase,
        "delta" => RollupKind::Delta,
        "irate" => RollupKind::Irate,
        "idelta" => RollupKind::Idelta,
        "resets" => RollupKind::Resets,
        "changes" => RollupKind::Changes,
        "sum_over_time" => RollupKind::SumOverTime,
        "avg_over_time" => RollupKind::AvgOverTime,
        "min_over_time" => RollupKind::MinOverTime,
        "max_over_time" => RollupKind::MaxOverTime,
        "count_over_time" => RollupKind::CountOverTime,
        "last_over_time" => RollupKind::LastOverTime,
        "stddev_over_time" => RollupKind::StddevOverTime,
        "stdvar_over_time" => RollupKind::StdvarOverTime,
        "present_over_time" => RollupKind::PresentOverTime,
        "deriv" => RollupKind::Deriv,
        "predict_linear" => {
            if args.len() != 2 {
                return Err(PlanError::InvalidArgument {
                    function: "predict_linear".to_string(),
                    expected: "exactly two arguments (range-vector, t)".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let seconds = expect_number_literal(&args[1], "predict_linear", "literal numeric `t`")?;
            RollupKind::PredictLinear(seconds)
        }
        "quantile_over_time" => {
            if args.len() != 2 {
                return Err(PlanError::InvalidArgument {
                    function: "quantile_over_time".to_string(),
                    expected: "exactly two arguments (q, range-vector)".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let q = expect_number_literal(&args[0], "quantile_over_time", "literal numeric `q`")?;
            RollupKind::QuantileOverTime(q)
        }
        _ => return Ok(None),
    };
    Ok(Some(kind))
}

/// If `name` names a pointwise instant-vector function, return the
/// `InstantFnKind` to emit. `args` is inspected for the `Round` / `Clamp*`
/// scalar bounds.
fn instant_fn_kind_for(
    name: &str,
    args: &[Box<parser::Expr>],
) -> Result<Option<InstantFnKind>, PlanError> {
    let kind = match name {
        "abs" => InstantFnKind::Abs,
        "ceil" => InstantFnKind::Ceil,
        "floor" => InstantFnKind::Floor,
        "exp" => InstantFnKind::Exp,
        "ln" => InstantFnKind::Ln,
        "log2" => InstantFnKind::Log2,
        "log10" => InstantFnKind::Log10,
        "sqrt" => InstantFnKind::Sqrt,
        "sin" => InstantFnKind::Sin,
        "cos" => InstantFnKind::Cos,
        "tan" => InstantFnKind::Tan,
        "asin" => InstantFnKind::Asin,
        "acos" => InstantFnKind::Acos,
        "atan" => InstantFnKind::Atan,
        "sinh" => InstantFnKind::Sinh,
        "cosh" => InstantFnKind::Cosh,
        "tanh" => InstantFnKind::Tanh,
        "asinh" => InstantFnKind::Asinh,
        "acosh" => InstantFnKind::Acosh,
        "atanh" => InstantFnKind::Atanh,
        "deg" => InstantFnKind::Deg,
        "rad" => InstantFnKind::Rad,
        "sgn" => InstantFnKind::Sgn,
        "timestamp" => InstantFnKind::Timestamp,
        "year" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "year".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::Year
        }
        "month" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "month".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::Month
        }
        "day_of_month" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "day_of_month".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::DayOfMonth
        }
        "day_of_year" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "day_of_year".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::DayOfYear
        }
        "day_of_week" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "day_of_week".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::DayOfWeek
        }
        "hour" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "hour".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::Hour
        }
        "minute" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "minute".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::Minute
        }
        "days_in_month" => {
            if args.len() > 1 {
                return Err(PlanError::InvalidArgument {
                    function: "days_in_month".to_string(),
                    expected: "zero or one instant-vector argument".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            InstantFnKind::DaysInMonth
        }
        "round" => {
            // `round(v)` or `round(v, to_nearest)`. `to_nearest` defaults to 1.
            let to_nearest = if args.len() == 1 {
                1.0
            } else if args.len() == 2 {
                expect_number_literal(&args[1], "round", "literal numeric `to_nearest`")?
            } else {
                return Err(PlanError::InvalidArgument {
                    function: "round".to_string(),
                    expected: "one or two arguments".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            };
            InstantFnKind::Round { to_nearest }
        }
        "clamp" => {
            if args.len() != 3 {
                return Err(PlanError::InvalidArgument {
                    function: "clamp".to_string(),
                    expected: "exactly three arguments (v, min, max)".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let min = expect_number_literal(&args[1], "clamp", "literal numeric `min`")?;
            let max = expect_number_literal(&args[2], "clamp", "literal numeric `max`")?;
            InstantFnKind::Clamp { min, max }
        }
        "clamp_min" => {
            if args.len() != 2 {
                return Err(PlanError::InvalidArgument {
                    function: "clamp_min".to_string(),
                    expected: "exactly two arguments (v, min)".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let min = expect_number_literal(&args[1], "clamp_min", "literal numeric `min`")?;
            InstantFnKind::ClampMin { min }
        }
        "clamp_max" => {
            if args.len() != 2 {
                return Err(PlanError::InvalidArgument {
                    function: "clamp_max".to_string(),
                    expected: "exactly two arguments (v, max)".to_string(),
                    got: format!("{} arguments", args.len()),
                });
            }
            let max = expect_number_literal(&args[1], "clamp_max", "literal numeric `max`")?;
            InstantFnKind::ClampMax { max }
        }
        _ => return Ok(None),
    };
    Ok(Some(kind))
}

/// Extract a plan-time constant from a parser expression — a literal or
/// constant arithmetic such as `-1` or `1/6` — erroring with
/// `PlanError::InvalidArgument` if it depends on data.
fn expect_number_literal(
    expr: &parser::Expr,
    function: &str,
    expected: &str,
) -> Result<f64, PlanError> {
    constant_scalar(expr).ok_or_else(|| PlanError::InvalidArgument {
        function: function.to_string(),
        expected: expected.to_string(),
        got: describe_parser_expr(expr),
    })
}

fn constant_scalar(expr: &parser::Expr) -> Option<f64> {
    match expr {
        parser::Expr::NumberLiteral(n) => Some(n.val),
        parser::Expr::Paren(p) => constant_scalar(&p.expr),
        parser::Expr::Unary(u) => constant_scalar(&u.expr).map(|v| -v),
        parser::Expr::Binary(bin) => {
            let lhs = constant_scalar(&bin.lhs)?;
            let rhs = constant_scalar(&bin.rhs)?;
            let op = binary_op_kind(bin.op.id(), true)?;
            super::optimize::apply_scalar_op(op, lhs, rhs)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Aggregate
// ---------------------------------------------------------------------------

fn lower_aggregate(
    agg: &parser::AggregateExpr,
    ctx: &LoweringContext,
) -> Result<LogicalPlan, PlanError> {
    let grouping = AggregateGrouping::from_label_modifier(agg.modifier.as_ref());
    let child = lower(&agg.expr, ctx)?;

    let op_id = agg.op.id();
    match op_id {
        T_SUM => Ok(aggregate_streaming(AggregateKind::Sum, child, grouping)),
        T_AVG => Ok(aggregate_streaming(AggregateKind::Avg, child, grouping)),
        T_MIN => Ok(aggregate_streaming(AggregateKind::Min, child, grouping)),
        T_MAX => Ok(aggregate_streaming(AggregateKind::Max, child, grouping)),
        T_COUNT => Ok(aggregate_streaming(AggregateKind::Count, child, grouping)),
        T_STDDEV => Ok(aggregate_streaming(AggregateKind::Stddev, child, grouping)),
        T_STDVAR => Ok(aggregate_streaming(AggregateKind::Stdvar, child, grouping)),
        T_GROUP => Ok(aggregate_streaming(AggregateKind::Group, child, grouping)),
        T_TOPK | T_BOTTOMK => {
            let param = agg
                .param
                .as_deref()
                .ok_or_else(|| PlanError::InvalidArgument {
                    function: aggregate_op_name(op_id).to_string(),
                    expected: "integer `k` parameter".to_string(),
                    got: "missing parameter".to_string(),
                })?;
            let (k, param_plan) = match lower(param, ctx)? {
                LogicalPlan::Scalar(k_f) => {
                    // Match the existing engine at `evaluator.rs::coerce_k_size`
                    // — cast `f64 → i64` with `as`, accepting platform-defined
                    // overflow on very large inputs. `k < 1` is treated as "no
                    // selection" by the breaker operator; we do not reject it
                    // here.
                    (k_f as i64, None)
                }
                lowered if lowered.produces_scalar() => (0, Some(Box::new(lowered))),
                lowered => {
                    return Err(PlanError::InvalidArgument {
                        function: aggregate_op_name(op_id).to_string(),
                        expected: "scalar `k` parameter".to_string(),
                        got: describe_logical_plan(&lowered),
                    });
                }
            };
            let kind = if op_id == T_TOPK {
                AggregateKind::Topk(k)
            } else {
                AggregateKind::Bottomk(k)
            };
            Ok(LogicalPlan::Aggregate {
                kind,
                child: Box::new(child),
                param: param_plan,
                grouping,
            })
        }
        T_QUANTILE => {
            let param = agg
                .param
                .as_deref()
                .ok_or_else(|| PlanError::InvalidArgument {
                    function: "quantile".to_string(),
                    expected: "numeric `q` parameter".to_string(),
                    got: "missing parameter".to_string(),
                })?;
            let q = expect_number_literal(param, "quantile", "literal numeric `q`")?;
            Ok(LogicalPlan::Aggregate {
                kind: AggregateKind::Quantile(q),
                child: Box::new(child),
                param: None,
                grouping,
            })
        }
        T_COUNT_VALUES => {
            let param = agg
                .param
                .as_deref()
                .ok_or_else(|| PlanError::InvalidArgument {
                    function: "count_values".to_string(),
                    expected: "string label name parameter".to_string(),
                    got: "missing parameter".to_string(),
                })?;
            let label = expect_string_literal(param, "count_values")?;
            Ok(LogicalPlan::CountValues {
                label,
                child: Box::new(child),
                grouping,
            })
        }
        _ => Err(PlanError::UnsupportedExpression(format!(
            "unknown aggregate operator token id {op_id}"
        ))),
    }
}

fn aggregate_streaming(
    kind: AggregateKind,
    child: LogicalPlan,
    grouping: AggregateGrouping,
) -> LogicalPlan {
    LogicalPlan::Aggregate {
        kind,
        child: Box::new(child),
        param: None,
        grouping,
    }
}

fn aggregate_op_name(op_id: u8) -> &'static str {
    match op_id {
        T_SUM => "sum",
        T_AVG => "avg",
        T_MIN => "min",
        T_MAX => "max",
        T_COUNT => "count",
        T_STDDEV => "stddev",
        T_STDVAR => "stdvar",
        T_GROUP => "group",
        T_TOPK => "topk",
        T_BOTTOMK => "bottomk",
        T_QUANTILE => "quantile",
        T_COUNT_VALUES => "count_values",
        _ => "<unknown>",
    }
}

fn expect_string_literal(expr: &parser::Expr, function: &str) -> Result<String, PlanError> {
    match expr {
        parser::Expr::StringLiteral(s) => Ok(s.val.clone()),
        parser::Expr::Paren(p) => expect_string_literal(&p.expr, function),
        other => Err(PlanError::InvalidArgument {
            function: function.to_string(),
            expected: "string literal".to_string(),
            got: describe_parser_expr(other),
        }),
    }
}

fn is_valid_label_name(label: &str) -> bool {
    !label.is_empty()
}

// ---------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------

fn lower_binary(bin: &parser::BinaryExpr, ctx: &LoweringContext) -> Result<LogicalPlan, PlanError> {
    let op_id = bin.op.id();
    let return_bool = bin
        .modifier
        .as_ref()
        .map(|m| m.return_bool)
        .unwrap_or(false);
    let op = binary_op_kind(op_id, return_bool).ok_or_else(|| {
        PlanError::UnsupportedExpression(format!("unknown binary operator token id {op_id}"))
    })?;

    let matching = binary_matching(bin.modifier.as_ref());

    let lhs = lower(&bin.lhs, ctx)?;
    let rhs = lower(&bin.rhs, ctx)?;

    Ok(LogicalPlan::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        matching,
    })
}

fn binary_op_kind(op_id: u8, return_bool: bool) -> Option<BinaryOpKind> {
    Some(match op_id {
        T_ADD => BinaryOpKind::Add,
        T_SUB => BinaryOpKind::Sub,
        T_MUL => BinaryOpKind::Mul,
        T_DIV => BinaryOpKind::Div,
        T_MOD => BinaryOpKind::Mod,
        T_POW => BinaryOpKind::Pow,
        T_ATAN2 => BinaryOpKind::Atan2,
        T_EQLC => BinaryOpKind::Eq {
            bool_modifier: return_bool,
        },
        T_NEQ => BinaryOpKind::Ne {
            bool_modifier: return_bool,
        },
        T_GTR => BinaryOpKind::Gt {
            bool_modifier: return_bool,
        },
        T_LSS => BinaryOpKind::Lt {
            bool_modifier: return_bool,
        },
        T_GTE => BinaryOpKind::Gte {
            bool_modifier: return_bool,
        },
        T_LTE => BinaryOpKind::Lte {
            bool_modifier: return_bool,
        },
        T_LAND => BinaryOpKind::And,
        T_LOR => BinaryOpKind::Or,
        T_LUNLESS => BinaryOpKind::Unless,
        _ => return None,
    })
}

fn binary_matching(modifier: Option<&parser::BinModifier>) -> Option<BinaryMatching> {
    let modifier = modifier?;
    // If there is neither an `on`/`ignoring` clause nor a `group_*` cardinality
    // we leave `matching` as `None` — the physical planner will treat the
    // default (implicit one-to-one on the full label set) without needing a
    // BinaryMatching record.
    if modifier.matching.is_none()
        && matches!(modifier.card, parser::VectorMatchCardinality::OneToOne)
    {
        return None;
    }
    // Axis + labels come from the optional LabelModifier; fall back to
    // `ignoring()` (empty) so set-operator cases where only `group_left` /
    // `group_right` or `many_to_many` is specified still produce a record.
    let (axis, labels) = match modifier.matching.as_ref() {
        Some(LabelModifier::Include(ls)) => (MatchingAxis::On, labels_to_arc(ls)),
        Some(LabelModifier::Exclude(ls)) => (MatchingAxis::Ignoring, labels_to_arc(ls)),
        None => (MatchingAxis::Ignoring, Arc::from(Vec::<String>::new())),
    };

    let cardinality = match &modifier.card {
        parser::VectorMatchCardinality::OneToOne => Cardinality::OneToOne,
        parser::VectorMatchCardinality::ManyToOne(labels) => Cardinality::GroupLeft {
            include: labels_to_arc(labels),
        },
        parser::VectorMatchCardinality::OneToMany(labels) => Cardinality::GroupRight {
            include: labels_to_arc(labels),
        },
        parser::VectorMatchCardinality::ManyToMany => Cardinality::ManyToMany,
    };

    Some(BinaryMatching {
        axis,
        labels,
        cardinality,
    })
}

fn labels_to_arc(labels: &promql_parser::label::Labels) -> Arc<[String]> {
    let mut v: Vec<String> = labels.labels.to_vec();
    v.sort();
    Arc::from(v)
}

// ---------------------------------------------------------------------------
// Diagnostic helpers
// ---------------------------------------------------------------------------

fn describe_parser_expr(expr: &parser::Expr) -> String {
    match expr {
        parser::Expr::VectorSelector(_) => "instant-vector selector".to_string(),
        parser::Expr::MatrixSelector(_) => "range-vector (matrix) selector".to_string(),
        parser::Expr::NumberLiteral(n) => format!("number literal {}", n.val),
        parser::Expr::StringLiteral(s) => format!("string literal \"{}\"", s.val),
        parser::Expr::Call(c) => format!("call `{}`", c.func.name),
        parser::Expr::Aggregate(a) => format!("aggregate `{}`", a.op),
        parser::Expr::Binary(_) => "binary expression".to_string(),
        parser::Expr::Paren(_) => "parenthesised expression".to_string(),
        parser::Expr::Subquery(_) => "subquery".to_string(),
        parser::Expr::Unary(_) => "unary expression".to_string(),
        parser::Expr::Extension(_) => "extension node".to_string(),
    }
}

fn describe_logical_plan(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::VectorSelector { .. } => "instant-vector".to_string(),
        LogicalPlan::MatrixSelector { .. } => "range-vector (matrix)".to_string(),
        LogicalPlan::Scalar(v) => format!("scalar {v}"),
        LogicalPlan::Time => "scalar (time)".to_string(),
        LogicalPlan::Scalarize { .. } => "scalar (from scalar())".to_string(),
        LogicalPlan::Vectorize { .. } => "instant-vector (from vector())".to_string(),
        LogicalPlan::InstantFn { .. } => "instant-vector (from instant fn)".to_string(),
        LogicalPlan::LabelManip { .. } => "instant-vector (from label manipulation)".to_string(),
        LogicalPlan::Rollup { .. } => "instant-vector (from rollup)".to_string(),
        LogicalPlan::Binary { .. } => "vector (from binary op)".to_string(),
        LogicalPlan::Aggregate { .. } => "vector (from aggregate)".to_string(),
        LogicalPlan::Sort { .. } => "instant-vector (from sort)".to_string(),
        LogicalPlan::Histogram { .. } => "instant-vector (from histogram fn)".to_string(),
        LogicalPlan::Absent { .. } => "instant-vector (from absent)".to_string(),
        LogicalPlan::Subquery { .. } => "range-vector (from subquery)".to_string(),
        LogicalPlan::Rechunk { .. } => "vector (from rechunk)".to_string(),
        LogicalPlan::CountValues { .. } => "vector (from count_values)".to_string(),
        LogicalPlan::Concurrent { .. } => "vector (from concurrent)".to_string(),
        LogicalPlan::Coalesce { .. } => "vector (from coalesce)".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
