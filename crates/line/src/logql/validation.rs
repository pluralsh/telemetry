// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::{collections::HashSet, net::IpAddr, sync::LazyLock};

use regex::Regex;

static PATTERN_CAPTURE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<([A-Za-z_][A-Za-z0-9_]*)>").expect("constant regex"));

/// An `ip()` filter argument: one address, a CIDR block, or an inclusive
/// `start-end` range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IpPattern {
    Single(IpAddr),
    Cidr(IpAddr, u8),
    Range(IpAddr, IpAddr),
}

impl IpPattern {
    pub(crate) fn parse(source: &str) -> Option<Self> {
        let source = source.trim();
        if let Some((network, prefix)) = source.split_once('/') {
            let network: IpAddr = network.parse().ok()?;
            let prefix: u8 = prefix.parse().ok()?;
            let bits = if network.is_ipv4() { 32 } else { 128 };
            return (prefix <= bits).then_some(Self::Cidr(network, prefix));
        }
        if let Some((start, end)) = source.split_once('-') {
            let (start, end): (IpAddr, IpAddr) = (start.parse().ok()?, end.parse().ok()?);
            return (start.is_ipv4() == end.is_ipv4()).then_some(Self::Range(start, end));
        }
        source.parse().ok().map(Self::Single)
    }

    pub(crate) fn contains(self, address: IpAddr) -> bool {
        let number = |address: IpAddr| match address {
            IpAddr::V4(address) => (false, u128::from(u32::from(address))),
            IpAddr::V6(address) => (true, u128::from(address)),
        };
        let (v6, value) = number(address);
        match self {
            Self::Single(expected) => expected == address,
            Self::Cidr(network, prefix) => {
                let (network_v6, network) = number(network);
                let bits = if network_v6 { 128 } else { 32 };
                let shift = bits - u32::from(prefix);
                network_v6 == v6
                    && value.checked_shr(shift).unwrap_or(0)
                        == network.checked_shr(shift).unwrap_or(0)
            }
            Self::Range(start, end) => {
                let ((start_v6, start), (_, end)) = (number(start), number(end));
                start_v6 == v6 && (start..=end).contains(&value)
            }
        }
    }
}

fn validate_ip(pattern: &str, span: Span) -> Result<(), ValidationError> {
    IpPattern::parse(pattern)
        .map(|_| ())
        .ok_or_else(|| ValidationError::new(format!("ip: invalid pattern {pattern:?}"), span))
}

fn pattern_captures(pattern: &str) -> impl Iterator<Item = &str> {
    PATTERN_CAPTURE
        .captures_iter(pattern)
        .map(|captures| captures.get(1).expect("group").as_str())
        .filter(|name| *name != "_")
}

use super::ast::{
    ComparisonOp, Expr, LabelFilterExpr, LineFilterOp, LineFilterTerm, LogExpr, MatchOp,
    ParserStage, PipelineStage, Query, Selector, Span, Spanned,
};
use super::error::ValidationError;

pub const DEFAULT_MAX_QUERY_BYTES: usize = 16 * 1024;
pub const DEFAULT_MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationOptions {
    pub max_query_bytes: usize,
    pub max_depth: usize,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }
}

pub fn validate(
    query: &Query,
    source_len: usize,
    options: ValidationOptions,
) -> Result<(), ValidationError> {
    if source_len > options.max_query_bytes {
        return Err(ValidationError::new(
            format!(
                "query is {source_len} bytes; maximum is {}",
                options.max_query_bytes
            ),
            Span::new(options.max_query_bytes, source_len),
        ));
    }
    validate_expr(query, 1, options.max_depth)
}

fn validate_expr(query: &Query, depth: usize, max_depth: usize) -> Result<(), ValidationError> {
    if depth > max_depth {
        return Err(ValidationError::new(
            format!("query nesting exceeds maximum depth of {max_depth}"),
            query.span,
        ));
    }
    match &query.value {
        Expr::Log(log) => validate_log(log, max_depth),
        Expr::Vector(expr) | Expr::LabelReplace { expr, .. } => {
            validate_expr(expr, depth + 1, max_depth)
        }
        Expr::RangeAggregation {
            op,
            parameter: _,
            expr,
            grouping,
        } => {
            let has_unwrap = expr
                .stages
                .iter()
                .any(|stage| matches!(stage.value, PipelineStage::Unwrap(_)));
            let supports_without_unwrap = matches!(
                op,
                super::ast::RangeOp::Count
                    | super::ast::RangeOp::Rate
                    | super::ast::RangeOp::Bytes
                    | super::ast::RangeOp::BytesRate
                    | super::ast::RangeOp::Absent
            );
            let supports_with_unwrap = matches!(
                op,
                super::ast::RangeOp::Avg
                    | super::ast::RangeOp::Sum
                    | super::ast::RangeOp::Min
                    | super::ast::RangeOp::Max
                    | super::ast::RangeOp::Stddev
                    | super::ast::RangeOp::Stdvar
                    | super::ast::RangeOp::Quantile
                    | super::ast::RangeOp::Rate
                    | super::ast::RangeOp::RateCounter
                    | super::ast::RangeOp::Absent
                    | super::ast::RangeOp::First
                    | super::ast::RangeOp::Last
            );
            if has_unwrap && !supports_with_unwrap {
                return Err(ValidationError::new(
                    format!("invalid aggregation {} with unwrap", op.as_str()),
                    query.span,
                ));
            }
            if !has_unwrap && !supports_without_unwrap {
                return Err(ValidationError::new(
                    format!("invalid aggregation {} without unwrap", op.as_str()),
                    query.span,
                ));
            }
            if grouping.is_some()
                && !matches!(
                    op,
                    super::ast::RangeOp::Avg
                        | super::ast::RangeOp::Min
                        | super::ast::RangeOp::Max
                        | super::ast::RangeOp::Stddev
                        | super::ast::RangeOp::Stdvar
                        | super::ast::RangeOp::Quantile
                        | super::ast::RangeOp::First
                        | super::ast::RangeOp::Last
                )
            {
                return Err(ValidationError::new(
                    format!("grouping not allowed for {} aggregation", op.as_str()),
                    query.span,
                ));
            }
            validate_grouping(grouping.as_ref(), query.span)?;
            validate_log(expr, max_depth)
        }
        Expr::LabelAggregation {
            field,
            expr,
            grouping,
        } => {
            if expr
                .stages
                .iter()
                .any(|stage| matches!(stage.value, PipelineStage::Unwrap(_)))
            {
                return Err(ValidationError::new(
                    "unwrap is not supported for approx_count_distinct()",
                    query.span,
                ));
            }
            if grouping.as_ref().is_some_and(|value| value.without) {
                return Err(ValidationError::new(
                    "without is not supported for approx_count_distinct()",
                    query.span,
                ));
            }
            if grouping
                .as_ref()
                .is_some_and(|value| value.labels.contains(field))
            {
                return Err(ValidationError::new(
                    format!("approx_count_distinct() cannot group by the counted field `{field}`"),
                    query.span,
                ));
            }
            validate_grouping(grouping.as_ref(), query.span)?;
            validate_log(expr, max_depth)
        }
        Expr::VectorAggregation {
            parameter,
            expr,
            grouping,
            op,
        } => {
            if matches!(
                op,
                super::ast::VectorOp::TopK
                    | super::ast::VectorOp::BottomK
                    | super::ast::VectorOp::ApproxTopK
            ) && parameter.is_some_and(|value| value <= 0.0 || value.fract() != 0.0)
            {
                return Err(ValidationError::new(
                    "aggregation parameter must be a positive integer",
                    query.span,
                ));
            }
            if *op == super::ast::VectorOp::ApproxTopK && grouping.is_some() {
                return Err(ValidationError::new(
                    "grouping not allowed for approx_topk aggregation",
                    query.span,
                ));
            }
            validate_grouping(grouping.as_ref(), query.span)?;
            validate_expr(expr, depth + 1, max_depth)
        }
        Expr::Binary {
            lhs,
            op,
            modifier,
            rhs,
        } => {
            if matches!(lhs.value, Expr::Log(_)) || matches!(rhs.value, Expr::Log(_)) {
                return Err(ValidationError::new(
                    "binary operators require metric expressions",
                    query.span,
                ));
            }
            if matches!(
                op,
                super::ast::BinaryOp::And | super::ast::BinaryOp::Or | super::ast::BinaryOp::Unless
            ) && (matches!(lhs.value, Expr::Number(_)) || matches!(rhs.value, Expr::Number(_)))
            {
                return Err(ValidationError::new(
                    "set operators do not accept scalar literals",
                    query.span,
                ));
            }
            if modifier.as_ref().is_some_and(|value| value.return_bool)
                && !matches!(
                    op,
                    super::ast::BinaryOp::Equal
                        | super::ast::BinaryOp::NotEqual
                        | super::ast::BinaryOp::Greater
                        | super::ast::BinaryOp::GreaterOrEqual
                        | super::ast::BinaryOp::Less
                        | super::ast::BinaryOp::LessOrEqual
                )
            {
                return Err(ValidationError::new(
                    "`bool` is only valid on comparison operators",
                    query.span,
                ));
            }
            if let Some(matching) = modifier.as_ref().and_then(|value| value.matching.as_ref()) {
                if matches!(
                    op,
                    super::ast::BinaryOp::And
                        | super::ast::BinaryOp::Or
                        | super::ast::BinaryOp::Unless
                ) && matching.grouping.is_some()
                {
                    return Err(ValidationError::new(
                        "group_left/group_right are not valid on set operators",
                        query.span,
                    ));
                }
                unique_labels(&matching.labels, query.span)?;
                if let Some(grouping) = &matching.grouping {
                    unique_labels(&grouping.include, query.span)?;
                    if let Some(overlap) = grouping
                        .include
                        .iter()
                        .find(|label| matching.labels.contains(label))
                    {
                        return Err(ValidationError::new(
                            format!("label `{overlap}` appears in matching and grouping"),
                            query.span,
                        ));
                    }
                }
            }
            validate_expr(lhs, depth + 1, max_depth)?;
            validate_expr(rhs, depth + 1, max_depth)
        }
        Expr::Number(value) if !value.is_finite() => {
            Err(ValidationError::new("number must be finite", query.span))
        }
        Expr::Number(_) | Expr::String(_) => Ok(()),
    }
}

fn validate_log(log: &LogExpr, max_depth: usize) -> Result<(), ValidationError> {
    validate_selector(&log.selector)?;
    if let Some(range) = &log.range
        && (range.value.starts_with('-') || range.value.to_ascii_lowercase().ends_with('b'))
    {
        return Err(ValidationError::new(
            "range must be a positive duration",
            range.span,
        ));
    }
    if let Some(offset) = &log.offset
        && offset.value.to_ascii_lowercase().ends_with('b')
    {
        return Err(ValidationError::new(
            "offset must be a duration",
            offset.span,
        ));
    }
    for stage in &log.stages {
        match &stage.value {
            PipelineStage::LabelFilter(filter) => {
                validate_filter_depth(filter, 1, max_depth)?;
            }
            PipelineStage::LineFilter(filter) => {
                for branch in &filter.branches {
                    let contains = matches!(
                        branch.op,
                        LineFilterOp::Contains | LineFilterOp::NotContains
                    );
                    match &branch.term {
                        LineFilterTerm::Ip(_) if !contains => {
                            return Err(ValidationError::new(
                                "ip: invalid operation; only |= and != support ip()",
                                stage.span,
                            ));
                        }
                        LineFilterTerm::Ip(pattern) => validate_ip(pattern, stage.span)?,
                        LineFilterTerm::String(pattern)
                            if matches!(
                                branch.op,
                                LineFilterOp::Regex | LineFilterOp::NotRegex
                            ) =>
                        {
                            validate_regex(pattern, stage.span)?;
                        }
                        LineFilterTerm::String(pattern)
                            if matches!(
                                branch.op,
                                LineFilterOp::Pattern | LineFilterOp::NotPattern
                            ) && pattern_captures(pattern).next().is_some() =>
                        {
                            return Err(ValidationError::new(
                                "named captures are not allowed in pattern line filters",
                                stage.span,
                            ));
                        }
                        LineFilterTerm::String(_) => {}
                    }
                }
            }
            PipelineStage::Parser(ParserStage::Regexp(pattern)) => {
                validate_regex(pattern, stage.span)?;
                if Regex::new(pattern)
                    .is_ok_and(|regex| regex.capture_names().flatten().count() == 0)
                {
                    return Err(ValidationError::new(
                        "regexp parser requires at least one named capture",
                        stage.span,
                    ));
                }
            }
            PipelineStage::Parser(ParserStage::Pattern(pattern))
                if pattern_captures(pattern).next().is_none() =>
            {
                return Err(ValidationError::new(
                    "pattern parser requires at least one named capture",
                    stage.span,
                ));
            }
            PipelineStage::Unwrap(unwrap) => {
                if let Some(filter) = &unwrap.post_filter {
                    validate_filter_depth(filter, 1, max_depth)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn validate_selector(selector: &Spanned<Selector>) -> Result<(), ValidationError> {
    if selector.value.matchers.is_empty() {
        return Err(ValidationError::new(
            "selector must contain at least one matcher",
            selector.span,
        ));
    }
    let label_name = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("constant regex");
    let mut has_non_empty_matcher = false;
    for matcher in &selector.value.matchers {
        if !label_name.is_match(&matcher.value.label) {
            return Err(ValidationError::new(
                format!("invalid label name `{}`", matcher.value.label),
                matcher.span,
            ));
        }
        match matcher.value.op {
            MatchOp::Regex | MatchOp::NotRegex => {
                let regex = Regex::new(&matcher.value.value).map_err(|error| {
                    ValidationError::new(format!("invalid matcher regex: {error}"), matcher.span)
                })?;
                if (matcher.value.op == MatchOp::Regex && !regex.is_match(""))
                    || (matcher.value.op == MatchOp::NotRegex && regex.is_match(""))
                {
                    has_non_empty_matcher = true;
                }
            }
            MatchOp::Equal if !matcher.value.value.is_empty() => has_non_empty_matcher = true,
            MatchOp::NotEqual if matcher.value.value.is_empty() => has_non_empty_matcher = true,
            MatchOp::Equal | MatchOp::NotEqual => {}
        }
    }
    if !has_non_empty_matcher {
        return Err(ValidationError::new(
            "selector must contain a matcher that cannot match an empty value",
            selector.span,
        ));
    }
    Ok(())
}

fn validate_filter_depth(
    filter: &Spanned<LabelFilterExpr>,
    depth: usize,
    max_depth: usize,
) -> Result<(), ValidationError> {
    if depth > max_depth {
        return Err(ValidationError::new(
            format!("label-filter nesting exceeds maximum depth of {max_depth}"),
            filter.span,
        ));
    }
    match &filter.value {
        LabelFilterExpr::And(lhs, rhs) | LabelFilterExpr::Or(lhs, rhs) => {
            validate_filter_depth(lhs, depth + 1, max_depth)?;
            validate_filter_depth(rhs, depth + 1, max_depth)
        }
        LabelFilterExpr::Predicate(predicate) => {
            use super::ast::FilterValue;
            if predicate.label == "__error__" && !matches!(predicate.value, FilterValue::String(_))
            {
                return Err(ValidationError::new(
                    format!(
                        "__error__ is a string label and cannot be compared with {}{}{}",
                        predicate.label, predicate.op, predicate.value
                    ),
                    filter.span,
                ));
            }
            match (&predicate.value, predicate.op) {
                (FilterValue::String(pattern), ComparisonOp::Regex | ComparisonOp::NotRegex) => {
                    validate_regex(pattern, filter.span)
                }
                (FilterValue::Ip(pattern), ComparisonOp::Equal | ComparisonOp::NotEqual) => {
                    validate_ip(pattern, filter.span)
                }
                (FilterValue::String(_), ComparisonOp::Equal | ComparisonOp::NotEqual)
                | (
                    FilterValue::Number(_) | FilterValue::Bytes(_) | FilterValue::Duration(_),
                    ComparisonOp::Equal
                    | ComparisonOp::NotEqual
                    | ComparisonOp::Greater
                    | ComparisonOp::GreaterOrEqual
                    | ComparisonOp::Less
                    | ComparisonOp::LessOrEqual,
                ) => Ok(()),
                (FilterValue::Identifier(_), _) => Err(ValidationError::new(
                    "label filter values must be strings, numbers, durations, bytes, or ip()",
                    filter.span,
                )),
                _ => Err(ValidationError::new(
                    "comparison operator is not valid for this label-filter value",
                    filter.span,
                )),
            }
        }
    }
}

fn validate_regex(pattern: &str, span: Span) -> Result<(), ValidationError> {
    Regex::new(pattern)
        .map(|_| ())
        .map_err(|error| ValidationError::new(format!("invalid regex: {error}"), span))
}

fn validate_grouping(
    grouping: Option<&super::ast::Grouping>,
    span: Span,
) -> Result<(), ValidationError> {
    if let Some(grouping) = grouping {
        unique_labels(&grouping.labels, span)?;
    }
    Ok(())
}

fn unique_labels(labels: &[String], span: Span) -> Result<(), ValidationError> {
    let mut unique = HashSet::new();
    if let Some(duplicate) = labels.iter().find(|label| !unique.insert(label.as_str())) {
        return Err(ValidationError::new(
            format!("duplicate label `{duplicate}`"),
            span,
        ));
    }
    Ok(())
}
