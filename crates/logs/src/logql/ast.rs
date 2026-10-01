// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::fmt;
use std::sync::Arc;

use crate::query::template::Template;

/// Half-open UTF-8 byte range in the original query.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn join(self, other: Self) -> Self {
        Self::new(self.start, other.end)
    }
}

/// A syntax value and its source location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub const fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }
}

pub type Query = Spanned<Expr>;

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Log(LogExpr),
    Number(f64),
    String(String),
    Vector(Box<Query>),
    LabelReplace {
        expr: Box<Query>,
        dst: String,
        replacement: String,
        src: String,
        regex: String,
    },
    RangeAggregation {
        op: RangeOp,
        parameter: Option<f64>,
        expr: LogExpr,
        grouping: Option<Grouping>,
    },
    LabelAggregation {
        field: String,
        expr: LogExpr,
        grouping: Option<Grouping>,
    },
    VectorAggregation {
        op: VectorOp,
        parameter: Option<f64>,
        expr: Box<Query>,
        grouping: Option<Grouping>,
    },
    Binary {
        lhs: Box<Query>,
        op: BinaryOp,
        modifier: Option<BinaryModifier>,
        rhs: Box<Query>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogExpr {
    pub selector: Spanned<Selector>,
    pub stages: Vec<Spanned<PipelineStage>>,
    pub range: Option<Spanned<String>>,
    pub offset: Option<Spanned<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Selector {
    pub matchers: Vec<Spanned<Matcher>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Matcher {
    pub label: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineStage {
    LineFilter(LineFilter),
    Parser(ParserStage),
    LabelFilter(Spanned<LabelFilterExpr>),
    LineFormat(FormatTemplate),
    LabelFormat(Vec<FormatAssignment>),
    Drop(Vec<LabelSelection>),
    Keep(Vec<LabelSelection>),
    Decolorize,
    Unwrap(Unwrap),
    Match(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFilter {
    pub branches: Vec<LineFilterBranch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFilterBranch {
    pub op: LineFilterOp,
    pub term: LineFilterTerm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
    Pattern,
    NotPattern,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LineFilterTerm {
    String(String),
    Ip(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParserStage {
    Json {
        expressions: Vec<ParserExpression>,
    },
    Logfmt {
        strict: bool,
        keep_empty: bool,
        expressions: Vec<ParserExpression>,
    },
    Regexp(String),
    Pattern(String),
    Unpack,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParserExpression {
    pub label: String,
    pub expression: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelFilterExpr {
    Predicate(LabelPredicate),
    And(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Or(Box<Spanned<Self>>, Box<Spanned<Self>>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelPredicate {
    pub label: String,
    pub op: ComparisonOp,
    pub value: FilterValue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilterValue {
    String(String),
    Number(String),
    Bytes(String),
    Duration(String),
    Ip(String),
    Identifier(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatAssignment {
    pub label: String,
    pub value: FormatValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FormatValue {
    /// `dst=src` moves another label's value.
    Rename(String),
    Template(FormatTemplate),
}

/// The template language of `line_format` and `label_format`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TemplateSyntax {
    /// Go `text/template`, as in Loki.
    #[default]
    Go,
    /// Jinja, selected with the `jinja` keyword after the stage name.
    Jinja,
}

/// A `line_format` or `label_format` template, compiled when the query is
/// parsed. Equality compares the syntax and source.
#[derive(Clone)]
pub struct FormatTemplate {
    pub syntax: TemplateSyntax,
    pub source: String,
    compiled: Arc<Template>,
}

impl FormatTemplate {
    /// Compiles `source`, returning the template error on failure.
    pub fn new(syntax: TemplateSyntax, source: impl Into<String>) -> Result<Self, String> {
        let source = source.into();
        let compiled = Arc::new(Template::compile(syntax, &source)?);
        Ok(Self {
            syntax,
            source,
            compiled,
        })
    }

    pub(crate) fn compiled(&self) -> &Template {
        &self.compiled
    }
}

impl PartialEq for FormatTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.syntax == other.syntax && self.source == other.source
    }
}

impl Eq for FormatTemplate {}

impl fmt::Debug for FormatTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FormatTemplate")
            .field("syntax", &self.syntax)
            .field("source", &self.source)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelection {
    pub label: String,
    pub matcher: Option<(MatchOp, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unwrap {
    pub conversion: Option<Conversion>,
    pub label: String,
    pub post_filter: Option<Box<Spanned<LabelFilterExpr>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Conversion {
    Bytes,
    Duration,
    DurationSeconds,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeOp {
    Count,
    Rate,
    RateCounter,
    Bytes,
    BytesRate,
    Avg,
    Sum,
    Min,
    Max,
    Stddev,
    Stdvar,
    Quantile,
    First,
    Last,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    TopK,
    BottomK,
    Sort,
    SortDesc,
    ApproxTopK,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Grouping {
    pub without: bool,
    pub labels: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    And,
    Or,
    Unless,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryModifier {
    pub return_bool: bool,
    pub matching: Option<VectorMatching>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorMatching {
    pub on: bool,
    pub labels: Vec<String>,
    pub grouping: Option<GroupSide>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupSide {
    pub left: bool,
    pub include: Vec<String>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_expr(self, f, 0)
    }
}

fn fmt_expr(query: &Query, f: &mut fmt::Formatter<'_>, parent_precedence: u8) -> fmt::Result {
    let precedence = match &query.value {
        Expr::Binary { op, .. } => op.precedence(),
        _ => u8::MAX,
    };
    let parentheses = precedence < parent_precedence;
    if parentheses {
        f.write_str("(")?;
    }
    match &query.value {
        Expr::Log(log) => write!(f, "{log}")?,
        Expr::Number(value) => write!(f, "{value}")?,
        Expr::String(value) => write!(f, "{}", quoted(value))?,
        Expr::Vector(expr) => {
            f.write_str("vector(")?;
            fmt_expr(expr, f, 0)?;
            f.write_str(")")?;
        }
        Expr::LabelReplace {
            expr,
            dst,
            replacement,
            src,
            regex,
        } => {
            f.write_str("label_replace(")?;
            fmt_expr(expr, f, 0)?;
            write!(
                f,
                ", {}, {}, {}, {})",
                quoted(dst),
                quoted(replacement),
                quoted(src),
                quoted(regex)
            )?;
        }
        Expr::RangeAggregation {
            op,
            parameter,
            expr,
            grouping,
        } => {
            write!(f, "{}(", op.as_str())?;
            if let Some(parameter) = parameter {
                write!(f, "{parameter}, ")?;
            }
            write!(f, "{expr}")?;
            f.write_str(")")?;
            fmt_grouping(grouping, f)?;
        }
        Expr::LabelAggregation {
            field,
            expr,
            grouping,
        } => {
            write!(f, "approx_count_distinct({field}, {expr})")?;
            fmt_grouping(grouping, f)?;
        }
        Expr::VectorAggregation {
            op,
            parameter,
            expr,
            grouping,
        } => {
            write!(f, "{}", op.as_str())?;
            fmt_grouping(grouping, f)?;
            f.write_str("(")?;
            if let Some(parameter) = parameter {
                write!(f, "{parameter}, ")?;
            }
            fmt_expr(expr, f, 0)?;
            f.write_str(")")?;
        }
        Expr::Binary {
            lhs,
            op,
            modifier,
            rhs,
        } => {
            fmt_expr(lhs, f, precedence)?;
            write!(f, " {}", op.as_str())?;
            if let Some(modifier) = modifier {
                write!(f, "{modifier}")?;
            }
            f.write_str(" ")?;
            fmt_expr(rhs, f, precedence + u8::from(!matches!(op, BinaryOp::Pow)))?;
        }
    }
    if parentheses {
        f.write_str(")")?;
    }
    Ok(())
}

impl fmt::Display for LogExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.selector.value.fmt(f)?;
        for stage in &self.stages {
            write!(f, " {}", stage.value)?;
        }
        if let Some(range) = &self.range {
            write!(f, "[{}]", range.value)?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " offset {}", offset.value)?;
        }
        Ok(())
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (index, matcher) in self.matchers.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(
                f,
                "{}{}{}",
                matcher.value.label,
                matcher.value.op.as_str(),
                quoted(&matcher.value.value)
            )?;
        }
        f.write_str("}")
    }
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LineFilter(filter) => {
                for (index, branch) in filter.branches.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" or")?;
                    } else {
                        write!(f, "{}", branch.op.as_str())?;
                    }
                    match &branch.term {
                        LineFilterTerm::String(value) => write!(f, " {}", quoted(value))?,
                        LineFilterTerm::Ip(value) => write!(f, " ip({})", quoted(value))?,
                    }
                }
                Ok(())
            }
            Self::Parser(parser) => parser.fmt(f),
            Self::LabelFilter(expr) => {
                f.write_str("| ")?;
                fmt_label_filter(expr, f, 0)
            }
            Self::LineFormat(template) => write!(
                f,
                "| line_format {}{}",
                syntax_keyword(template.syntax),
                quoted(&template.source)
            ),
            Self::LabelFormat(assignments) => {
                f.write_str("| label_format ")?;
                let syntax = assignments
                    .iter()
                    .find_map(|assignment| match &assignment.value {
                        FormatValue::Template(template) => Some(template.syntax),
                        FormatValue::Rename(_) => None,
                    });
                f.write_str(syntax_keyword(syntax.unwrap_or_default()))?;
                for (index, assignment) in assignments.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    let value = match &assignment.value {
                        FormatValue::Rename(label) => label.clone(),
                        FormatValue::Template(template) => quoted(&template.source),
                    };
                    write!(f, "{}={value}", assignment.label)?;
                }
                Ok(())
            }
            Self::Drop(labels) => fmt_label_selection("drop", labels, f),
            Self::Keep(labels) => fmt_label_selection("keep", labels, f),
            Self::Decolorize => f.write_str("| decolorize"),
            Self::Unwrap(unwrap) => {
                f.write_str("| unwrap ")?;
                if let Some(conversion) = unwrap.conversion {
                    write!(f, "{}({})", conversion.as_str(), unwrap.label)?;
                } else {
                    f.write_str(&unwrap.label)?;
                }
                if let Some(filter) = &unwrap.post_filter {
                    f.write_str(" | ")?;
                    fmt_label_filter(filter, f, 0)?;
                }
                Ok(())
            }
            Self::Match(query) => write!(f, "| match {}", quoted(query)),
        }
    }
}

impl fmt::Display for ParserStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json { expressions } => {
                f.write_str("| json")?;
                fmt_parser_expressions(expressions, f)
            }
            Self::Logfmt {
                strict,
                keep_empty,
                expressions,
            } => {
                f.write_str("| logfmt")?;
                if *strict {
                    f.write_str(" --strict")?;
                }
                if *keep_empty {
                    f.write_str(" --keep-empty")?;
                }
                fmt_parser_expressions(expressions, f)
            }
            Self::Regexp(value) => write!(f, "| regexp {}", quoted(value)),
            Self::Pattern(value) => write!(f, "| pattern {}", quoted(value)),
            Self::Unpack => f.write_str("| unpack"),
        }
    }
}

fn fmt_parser_expressions(
    expressions: &[ParserExpression],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    if !expressions.is_empty() {
        f.write_str(" ")?;
    }
    for (index, expression) in expressions.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{}={}", expression.label, quoted(&expression.expression))?;
    }
    Ok(())
}

fn fmt_label_selection(
    name: &str,
    labels: &[LabelSelection],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(f, "| {name} ")?;
    for (index, selection) in labels.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        f.write_str(&selection.label)?;
        if let Some((op, value)) = &selection.matcher {
            write!(f, "{}{}", op.as_str(), quoted(value))?;
        }
    }
    Ok(())
}

fn fmt_label_filter(
    expr: &Spanned<LabelFilterExpr>,
    f: &mut fmt::Formatter<'_>,
    parent: u8,
) -> fmt::Result {
    let precedence = match expr.value {
        LabelFilterExpr::Or(_, _) => 1,
        LabelFilterExpr::And(_, _) => 2,
        LabelFilterExpr::Predicate(_) => 3,
    };
    let parentheses = precedence < parent;
    if parentheses {
        f.write_str("(")?;
    }
    match &expr.value {
        LabelFilterExpr::Predicate(predicate) => {
            write!(
                f,
                "{} {} {}",
                predicate.label, predicate.op, predicate.value
            )?;
        }
        LabelFilterExpr::And(lhs, rhs) => {
            fmt_label_filter(lhs, f, precedence)?;
            f.write_str(" and ")?;
            fmt_label_filter(rhs, f, precedence)?;
        }
        LabelFilterExpr::Or(lhs, rhs) => {
            fmt_label_filter(lhs, f, precedence)?;
            f.write_str(" or ")?;
            fmt_label_filter(rhs, f, precedence)?;
        }
    }
    if parentheses {
        f.write_str(")")?;
    }
    Ok(())
}

fn fmt_grouping(grouping: &Option<Grouping>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let Some(grouping) = grouping {
        write!(f, " {} (", if grouping.without { "without" } else { "by" })?;
        for (index, label) in grouping.labels.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            f.write_str(label)?;
        }
        f.write_str(")")?;
    }
    Ok(())
}

impl fmt::Display for BinaryModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.return_bool {
            f.write_str(" bool")?;
        }
        if let Some(matching) = &self.matching {
            write!(f, " {} (", if matching.on { "on" } else { "ignoring" })?;
            for (index, label) in matching.labels.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                f.write_str(label)?;
            }
            f.write_str(")")?;
            if let Some(grouping) = &matching.grouping {
                write!(
                    f,
                    " {} (",
                    if grouping.left {
                        "group_left"
                    } else {
                        "group_right"
                    }
                )?;
                for (index, label) in grouping.include.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(label)?;
                }
                f.write_str(")")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for FilterValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => f.write_str(&quoted(value)),
            Self::Ip(value) => write!(f, "ip({})", quoted(value)),
            Self::Number(value)
            | Self::Bytes(value)
            | Self::Duration(value)
            | Self::Identifier(value) => f.write_str(value),
        }
    }
}

impl MatchOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Regex => "=~",
            Self::NotRegex => "!~",
        }
    }
}

impl ComparisonOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Regex => "=~",
            Self::NotRegex => "!~",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
        }
    }
}

impl LineFilterOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contains => "|=",
            Self::NotContains => "!=",
            Self::Regex => "|~",
            Self::NotRegex => "!~",
            Self::Pattern => "|>",
            Self::NotPattern => "!>",
        }
    }
}

impl BinaryOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Equal => "==",
            Self::NotEqual => "!=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::And => "and",
            Self::Or => "or",
            Self::Unless => "unless",
        }
    }

    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And | Self::Unless => 2,
            Self::Equal
            | Self::NotEqual
            | Self::Greater
            | Self::GreaterOrEqual
            | Self::Less
            | Self::LessOrEqual => 3,
            Self::Add | Self::Sub => 4,
            Self::Mul | Self::Div | Self::Mod => 5,
            Self::Pow => 6,
        }
    }
}

impl RangeOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count_over_time",
            Self::Rate => "rate",
            Self::RateCounter => "rate_counter",
            Self::Bytes => "bytes_over_time",
            Self::BytesRate => "bytes_rate",
            Self::Avg => "avg_over_time",
            Self::Sum => "sum_over_time",
            Self::Min => "min_over_time",
            Self::Max => "max_over_time",
            Self::Stddev => "stddev_over_time",
            Self::Stdvar => "stdvar_over_time",
            Self::Quantile => "quantile_over_time",
            Self::First => "first_over_time",
            Self::Last => "last_over_time",
            Self::Absent => "absent_over_time",
        }
    }
}

impl VectorOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
            Self::TopK => "topk",
            Self::BottomK => "bottomk",
            Self::Sort => "sort",
            Self::SortDesc => "sort_desc",
            Self::ApproxTopK => "approx_topk",
        }
    }
}

impl Conversion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Duration => "duration",
            Self::DurationSeconds => "duration_seconds",
        }
    }
}

fn quoted(value: &str) -> String {
    format!("{value:?}")
}

fn syntax_keyword(syntax: TemplateSyntax) -> &'static str {
    match syntax {
        TemplateSyntax::Go => "",
        TemplateSyntax::Jinja => "jinja ",
    }
}

/// Read-only traversal hooks for query analysis and planning.
pub trait Visitor {
    fn visit_expr(&mut self, _expr: &Query) {}
    fn visit_log_expr(&mut self, _expr: &LogExpr) {}
    fn visit_matcher(&mut self, _matcher: &Spanned<Matcher>) {}
    fn visit_stage(&mut self, _stage: &Spanned<PipelineStage>) {}
    fn visit_label_filter(&mut self, _filter: &Spanned<LabelFilterExpr>) {}
}

pub fn walk<V: Visitor + ?Sized>(visitor: &mut V, query: &Query) {
    visitor.visit_expr(query);
    match &query.value {
        Expr::Log(log) => walk_log(visitor, log),
        Expr::Vector(expr) => walk(visitor, expr),
        Expr::LabelReplace { expr, .. } => walk(visitor, expr),
        Expr::RangeAggregation { expr, .. } => walk_log(visitor, expr),
        Expr::LabelAggregation { expr, .. } => walk_log(visitor, expr),
        Expr::VectorAggregation { expr, .. } => walk(visitor, expr),
        Expr::Binary { lhs, rhs, .. } => {
            walk(visitor, lhs);
            walk(visitor, rhs);
        }
        Expr::Number(_) | Expr::String(_) => {}
    }
}

fn walk_log<V: Visitor + ?Sized>(visitor: &mut V, log: &LogExpr) {
    visitor.visit_log_expr(log);
    for matcher in &log.selector.value.matchers {
        visitor.visit_matcher(matcher);
    }
    for stage in &log.stages {
        visitor.visit_stage(stage);
        match &stage.value {
            PipelineStage::LabelFilter(filter) => walk_label_filter(visitor, filter),
            PipelineStage::Unwrap(Unwrap {
                post_filter: Some(filter),
                ..
            }) => walk_label_filter(visitor, filter),
            _ => {}
        }
    }
}

fn walk_label_filter<V: Visitor + ?Sized>(visitor: &mut V, filter: &Spanned<LabelFilterExpr>) {
    visitor.visit_label_filter(filter);
    match &filter.value {
        LabelFilterExpr::And(lhs, rhs) | LabelFilterExpr::Or(lhs, rhs) => {
            walk_label_filter(visitor, lhs);
            walk_label_filter(visitor, rhs);
        }
        LabelFilterExpr::Predicate(_) => {}
    }
}
