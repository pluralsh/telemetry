//! In-process LogQL execution over [`LogDb`](crate::LogDb).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use futures::{StreamExt, stream};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::logql::{
    self, BinaryModifier, BinaryOp, ComparisonOp, Conversion, Expr, FilterValue, FormatAssignment,
    Grouping, LabelFilterExpr, LineFilter, LineFilterOp, LineFilterTerm, LogExpr, MatchOp,
    ParserExpression, ParserStage, PipelineStage, Query, RangeOp, Unwrap, VectorMatching, VectorOp,
};
use crate::search::{SCORE_METADATA_FIELD, query_terms, source_matches};
use crate::{Error, Label, Labels, LogDb, LogEntry, Namespace, Result};

/// Ordering of log entries returned by a streams query.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    #[default]
    Forward,
    Backward,
}

pub const DEFAULT_INSTANT_LOG_LOOKBACK_NS: i64 = 30_000_000_000;

/// A parsed LogQL request. Log bounds are `[start_ns, end_ns)`; metric
/// evaluations use LogQL's `(evaluation-range, evaluation]` windows.
#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    /// `None` is an instant query evaluated at `end_ns`.
    pub step_ns: Option<i64>,
}

impl QueryRequest {
    /// Constructs an instant metric request evaluated at `timestamp_ns`.
    pub fn instant(query: impl Into<String>, timestamp_ns: i64) -> Self {
        Self {
            query: query.into(),
            start_ns: timestamp_ns,
            end_ns: timestamp_ns,
            step_ns: None,
        }
    }

    /// Constructs Loki's instant log request with its default 30-second
    /// lookback and an exclusive evaluation-time upper bound.
    pub fn instant_logs(query: impl Into<String>, timestamp_ns: i64) -> Self {
        Self::instant_logs_with_lookback(query, timestamp_ns, DEFAULT_INSTANT_LOG_LOOKBACK_NS)
    }

    pub fn instant_logs_with_lookback(
        query: impl Into<String>,
        timestamp_ns: i64,
        lookback_ns: i64,
    ) -> Self {
        Self {
            query: query.into(),
            start_ns: timestamp_ns.saturating_sub(lookback_ns),
            end_ns: timestamp_ns,
            step_ns: None,
        }
    }

    pub fn range(query: impl Into<String>, start_ns: i64, end_ns: i64, step_ns: i64) -> Self {
        Self {
            query: query.into(),
            start_ns,
            end_ns,
            step_ns: Some(step_ns),
        }
    }
}

/// Resource and presentation controls for one query.
#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub limit: usize,
    pub direction: Direction,
    pub max_pages: usize,
    pub max_concurrency: usize,
    pub max_in_flight_bytes: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            limit: 1_000,
            direction: Direction::Forward,
            max_pages: 10_000,
            max_concurrency: 8,
            max_in_flight_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogStream {
    pub labels: Labels,
    pub entries: Vec<LogEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub timestamp_ns: i64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorSample {
    pub labels: Labels,
    pub sample: Sample,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MatrixSeries {
    pub labels: Labels,
    pub samples: Vec<Sample>,
}

/// Loki-compatible result categories.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result_type", content = "result", rename_all = "lowercase")]
pub enum QueryResult {
    Streams(Vec<LogStream>),
    Vector(Vec<VectorSample>),
    Matrix(Vec<MatrixSeries>),
    Scalar(Sample),
}

#[derive(Clone)]
struct Row {
    timestamp_ns: i64,
    line: String,
    labels: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
    value: Option<f64>,
}

#[derive(Clone)]
struct Point {
    labels: BTreeMap<String, String>,
    value: f64,
}

enum Value {
    Scalar(f64),
    Vector(Vec<Point>),
}

impl LogDb {
    /// Parse, plan, and execute a LogQL query against this database.
    pub async fn query(
        &self,
        namespace: &Namespace,
        request: &QueryRequest,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        validate_request(request, &options)?;
        let query =
            logql::parse(&request.query).map_err(|error| Error::Query(error.to_string()))?;
        let scan_start = request.start_ns.saturating_sub(max_lookback(&query)?);
        let exact = common_exact_matchers(&query);
        let indexed_terms = indexed_match_terms(&query);
        let stored = load_rows(
            self,
            namespace,
            request,
            &query,
            scan_start,
            &exact,
            indexed_terms.as_deref(),
            options.max_pages,
            options.limit,
        )
        .await?;
        let indexed = indexed_terms.is_some();
        evaluate(query, stored, request, options, indexed)
    }
}

pub(crate) async fn query_databases(
    databases: Vec<Arc<LogDb>>,
    namespace: &Namespace,
    request: &QueryRequest,
    options: QueryOptions,
    global_permits: Arc<Semaphore>,
) -> Result<QueryResult> {
    validate_request(request, &options)?;
    let query = logql::parse(&request.query).map_err(|error| Error::Query(error.to_string()))?;
    let lookback = max_lookback(&query)?;
    let scan_start = request.start_ns.saturating_sub(lookback);
    let exact = common_exact_matchers(&query);
    let indexed_terms = indexed_match_terms(&query);
    let exact_for_estimate = &exact;
    let estimate_permits = Arc::clone(&global_permits);
    let estimates = stream::iter(databases.iter().cloned())
        .map(move |database| {
            let permits = Arc::clone(&estimate_permits);
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Query("global query scheduler closed".into()))?;
                database
                    .estimate_pages(namespace, scan_start, request.end_ns, exact_for_estimate)
                    .await
            }
        })
        .buffered(options.max_concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let total_pages = estimates
        .iter()
        .try_fold(0usize, |total, estimate| total.checked_add(estimate.pages))
        .ok_or_else(|| Error::Query("query page estimate overflow".into()))?;
    if total_pages > options.max_pages {
        return Err(Error::Query(format!(
            "query exceeded max_pages ({})",
            options.max_pages
        )));
    }
    let byte_budget = options.max_in_flight_bytes.min(u32::MAX as usize);
    let permits = Arc::new(Semaphore::new(byte_budget));
    let stored = stream::iter(
        databases
            .into_iter()
            .zip(estimates)
            .map(|(database, estimate)| {
                let permits = Arc::clone(&permits);
                let global_permits = Arc::clone(&global_permits);
                let query = &query;
                let exact = &exact;
                let indexed_terms = &indexed_terms;
                async move {
                    let weight = estimate.compressed_bytes.max(1).min(byte_budget as u64) as u32;
                    let _permit = permits
                        .acquire_many_owned(weight)
                        .await
                        .map_err(|_| Error::Query("query scheduler closed".into()))?;
                    let _global_permit = global_permits
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::Query("global query scheduler closed".into()))?;
                    load_rows(
                        &database,
                        namespace,
                        request,
                        query,
                        scan_start,
                        exact,
                        indexed_terms.as_deref(),
                        estimate.pages,
                        options.limit,
                    )
                    .await
                }
            }),
    )
    .buffered(options.max_concurrency)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    evaluate(query, stored, request, options, indexed_terms.is_some())
}

#[allow(clippy::too_many_arguments)]
async fn load_rows(
    database: &LogDb,
    namespace: &Namespace,
    request: &QueryRequest,
    query: &Query,
    scan_start: i64,
    exact: &[Label],
    indexed_terms: Option<&[String]>,
    max_pages: usize,
    limit: usize,
) -> Result<Vec<(crate::LogRow, Option<f32>)>> {
    Ok(if let Some(terms) = indexed_terms {
        let index_top_k = direct_index_top_k(query, limit);
        match database
            .read_match_bounded(
                namespace,
                scan_start,
                request.end_ns,
                exact,
                (terms, index_top_k),
                max_pages,
            )
            .await?
        {
            Some(rows) => rows
                .into_iter()
                .map(|(row, score)| (row, Some(score)))
                .collect(),
            None => database
                .read_bounded(namespace, scan_start, request.end_ns, exact, max_pages)
                .await?
                .into_iter()
                .map(|row| (row, None))
                .collect(),
        }
    } else {
        database
            .read_bounded(namespace, scan_start, request.end_ns, exact, max_pages)
            .await?
            .into_iter()
            .map(|row| (row, None))
            .collect()
    })
}

fn evaluate(
    query: Query,
    stored: Vec<(crate::LogRow, Option<f32>)>,
    request: &QueryRequest,
    options: QueryOptions,
    indexed: bool,
) -> Result<QueryResult> {
    let rows = stored
        .into_iter()
        .map(|(row, score)| {
            let mut metadata = row
                .entry
                .structured_metadata
                .iter()
                .map(|field| (field.name.clone(), field.value.clone()))
                .collect::<BTreeMap<_, _>>();
            if let Some(score) = score {
                metadata.insert(SCORE_METADATA_FIELD.to_owned(), score.to_string());
            }
            Row {
                timestamp_ns: row.entry.timestamp_ns,
                line: row.entry.line,
                labels: row
                    .labels
                    .iter()
                    .map(|label| (label.name.clone(), label.value.clone()))
                    .collect(),
                metadata,
                value: None,
            }
        })
        .collect::<Vec<_>>();

    if let Expr::Log(log) = &query.value {
        let mut result =
            eval_log_window(log, &rows, request.start_ns, request.end_ns, Window::Log)?;
        if indexed {
            result.sort_by(|left, right| {
                match_score(right)
                    .total_cmp(&match_score(left))
                    .then_with(|| row_order(left, right))
            });
        } else {
            result.sort_by(row_order);
            if options.direction == Direction::Backward {
                result.reverse();
            }
        }
        result.truncate(options.limit);
        return streams(result);
    }

    if let Some(step) = request.step_ns {
        let mut series: BTreeMap<Vec<(String, String)>, MatrixSeries> = BTreeMap::new();
        let mut timestamp = request.start_ns;
        loop {
            match eval_expr(&query, &rows, timestamp)? {
                Value::Scalar(value) => {
                    series
                        .entry(Vec::new())
                        .or_insert_with(|| MatrixSeries {
                            labels: Labels::new(Vec::new()).expect("empty labels"),
                            samples: Vec::new(),
                        })
                        .samples
                        .push(Sample {
                            timestamp_ns: timestamp,
                            value,
                        });
                }
                Value::Vector(points) => {
                    for point in points {
                        let key = point.labels.clone().into_iter().collect::<Vec<_>>();
                        series
                            .entry(key)
                            .or_insert_with(|| MatrixSeries {
                                labels: map_labels(&point.labels),
                                samples: Vec::new(),
                            })
                            .samples
                            .push(Sample {
                                timestamp_ns: timestamp,
                                value: point.value,
                            });
                    }
                }
            }
            if timestamp == request.end_ns {
                break;
            }
            timestamp = timestamp.saturating_add(step).min(request.end_ns);
        }
        Ok(QueryResult::Matrix(series.into_values().collect()))
    } else {
        match eval_expr(&query, &rows, request.end_ns)? {
            Value::Scalar(value) => Ok(QueryResult::Scalar(Sample {
                timestamp_ns: request.end_ns,
                value,
            })),
            Value::Vector(mut points) => {
                points.sort_by(|a, b| a.labels.cmp(&b.labels));
                Ok(QueryResult::Vector(
                    points
                        .into_iter()
                        .map(|point| VectorSample {
                            labels: map_labels(&point.labels),
                            sample: Sample {
                                timestamp_ns: request.end_ns,
                                value: point.value,
                            },
                        })
                        .collect(),
                ))
            }
        }
    }
}

fn validate_request(request: &QueryRequest, options: &QueryOptions) -> Result<()> {
    if request.end_ns < request.start_ns {
        return Err(Error::Query("end_ns must be >= start_ns".into()));
    }
    if matches!(request.step_ns, Some(step) if step <= 0) {
        return Err(Error::Query("step_ns must be positive".into()));
    }
    if options.limit == 0
        || options.max_pages == 0
        || options.max_concurrency == 0
        || options.max_in_flight_bytes == 0
    {
        return Err(Error::Query("limit and max_pages must be positive".into()));
    }
    Ok(())
}

fn map_labels(map: &BTreeMap<String, String>) -> Labels {
    Labels::new(
        map.iter()
            .map(|(name, value)| Label::new(name, value))
            .collect(),
    )
    .expect("map has unique non-empty label names")
}

fn streams(rows: Vec<Row>) -> Result<QueryResult> {
    let mut grouped: BTreeMap<BTreeMap<String, String>, Vec<LogEntry>> = BTreeMap::new();
    for row in rows {
        grouped.entry(row.labels).or_default().push(LogEntry {
            timestamp_ns: row.timestamp_ns,
            line: row.line,
            structured_metadata: crate::Fields::new(
                row.metadata
                    .into_iter()
                    .map(|(name, value)| crate::Field::new(name, value))
                    .collect(),
            )?,
        });
    }
    Ok(QueryResult::Streams(
        grouped
            .into_iter()
            .map(|(labels, entries)| LogStream {
                labels: map_labels(&labels),
                entries,
            })
            .collect(),
    ))
}

fn row_order(a: &Row, b: &Row) -> Ordering {
    a.timestamp_ns
        .cmp(&b.timestamp_ns)
        .then_with(|| a.labels.cmp(&b.labels))
        .then_with(|| a.line.cmp(&b.line))
        .then_with(|| a.metadata.cmp(&b.metadata))
}

fn common_exact_matchers(query: &Query) -> Vec<Label> {
    let mut selectors = Vec::new();
    collect_log_exprs(query, &mut selectors);
    if selectors.is_empty() {
        return Vec::new();
    }
    let mut common: Option<BTreeSet<(String, String)>> = None;
    for log in selectors {
        let exact = log
            .selector
            .value
            .matchers
            .iter()
            .filter(|matcher| matcher.value.op == MatchOp::Equal)
            .map(|matcher| (matcher.value.label.clone(), matcher.value.value.clone()))
            .collect::<BTreeSet<_>>();
        common = Some(match common {
            None => exact,
            Some(current) => current.intersection(&exact).cloned().collect(),
        });
    }
    common
        .unwrap_or_default()
        .into_iter()
        .map(|(name, value)| Label::new(name, value))
        .collect()
}

fn collect_log_exprs<'a>(query: &'a Query, result: &mut Vec<&'a LogExpr>) {
    match &query.value {
        Expr::Log(log)
        | Expr::RangeAggregation { expr: log, .. }
        | Expr::LabelAggregation { expr: log, .. } => result.push(log),
        Expr::Vector(expr) | Expr::LabelReplace { expr, .. } => collect_log_exprs(expr, result),
        Expr::VectorAggregation { expr, .. } => collect_log_exprs(expr, result),
        Expr::Binary { lhs, rhs, .. } => {
            collect_log_exprs(lhs, result);
            collect_log_exprs(rhs, result);
        }
        Expr::Number(_) | Expr::String(_) => {}
    }
}

fn indexed_match_terms(query: &Query) -> Option<Vec<String>> {
    let mut logs = Vec::new();
    collect_log_exprs(query, &mut logs);
    let mut common: Option<Vec<String>> = None;
    for log in logs {
        let mut selected = None;
        for stage in &log.stages {
            match &stage.value {
                PipelineStage::Match(value) => {
                    let terms = query_terms(value);
                    if terms.is_empty() {
                        return None;
                    }
                    selected = Some(terms);
                    break;
                }
                PipelineStage::LineFormat(_)
                | PipelineStage::Decolorize
                | PipelineStage::Parser(ParserStage::Unpack) => return None,
                _ => {}
            }
        }
        let selected = selected?;
        if common.as_ref().is_some_and(|current| *current != selected) {
            return None;
        }
        common = Some(selected);
    }
    common
}

fn direct_index_top_k(query: &Query, limit: usize) -> Option<usize> {
    let Expr::Log(log) = &query.value else {
        return None;
    };
    (log.stages.len() == 1
        && matches!(log.stages[0].value, PipelineStage::Match(_))
        && log
            .selector
            .value
            .matchers
            .iter()
            .all(|matcher| matcher.value.op == MatchOp::Equal))
    .then_some(limit)
}

fn match_score(row: &Row) -> f32 {
    row.metadata
        .get(SCORE_METADATA_FIELD)
        .and_then(|score| score.parse().ok())
        .unwrap_or(0.0)
}

fn max_lookback(query: &Query) -> Result<i64> {
    let mut logs = Vec::new();
    collect_log_exprs(query, &mut logs);
    logs.into_iter().try_fold(0i64, |maximum, log| {
        let range = log
            .range
            .as_ref()
            .map(|value| parse_duration_ns(&value.value))
            .transpose()?
            .unwrap_or(0);
        let offset = log
            .offset
            .as_ref()
            .map(|value| parse_duration_ns(&value.value))
            .transpose()?
            .unwrap_or(0);
        Ok(maximum.max(range.saturating_add(offset)))
    })
}

fn eval_metric_log(log: &LogExpr, rows: &[Row], start: i64, end: i64) -> Result<Vec<Row>> {
    eval_log_window(log, rows, start, end, Window::Metric)
}

#[derive(Clone, Copy)]
enum Window {
    Log,
    Metric,
}

fn eval_log_window(
    log: &LogExpr,
    rows: &[Row],
    start: i64,
    end: i64,
    window: Window,
) -> Result<Vec<Row>> {
    let offset = log
        .offset
        .as_ref()
        .map(|value| parse_duration_ns(&value.value))
        .transpose()?
        .unwrap_or(0);
    let start = start.saturating_sub(offset);
    let end = end.saturating_sub(offset);
    let mut output = Vec::new();
    for source in rows {
        let outside = match window {
            Window::Log => source.timestamp_ns < start || source.timestamp_ns >= end,
            Window::Metric => source.timestamp_ns <= start || source.timestamp_ns > end,
        };
        if outside || !selector_matches(log, &source.labels)? {
            continue;
        }
        let mut row = source.clone();
        let mut keep = true;
        for stage in &log.stages {
            if !apply_stage(&mut row, &stage.value)? {
                keep = false;
                break;
            }
        }
        if keep {
            output.push(row);
        }
    }
    Ok(output)
}

fn selector_matches(log: &LogExpr, labels: &BTreeMap<String, String>) -> Result<bool> {
    for matcher in &log.selector.value.matchers {
        let actual = labels.get(&matcher.value.label).map_or("", String::as_str);
        let matched = match matcher.value.op {
            MatchOp::Equal => actual == matcher.value.value,
            MatchOp::NotEqual => actual != matcher.value.value,
            MatchOp::Regex => {
                Regex::new(&format!("^(?:{})$", matcher.value.value))?.is_match(actual)
            }
            MatchOp::NotRegex => {
                !Regex::new(&format!("^(?:{})$", matcher.value.value))?.is_match(actual)
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn apply_stage(row: &mut Row, stage: &PipelineStage) -> Result<bool> {
    match stage {
        PipelineStage::LineFilter(filter) => line_filter(&row.line, filter),
        PipelineStage::Parser(parser) => {
            parse_stage(row, parser)?;
            Ok(true)
        }
        PipelineStage::LabelFilter(filter) => label_filter(row, &filter.value),
        PipelineStage::LineFormat(template) => {
            match render_template(template, row) {
                Ok(line) => row.line = line,
                Err(error) => set_error(row, "TemplateFormatErr", &error.to_string()),
            }
            Ok(true)
        }
        PipelineStage::LabelFormat(assignments) => {
            label_format(row, assignments);
            Ok(true)
        }
        PipelineStage::Drop(selections) => {
            row.labels.retain(|name, value| {
                !selections.iter().any(|selection| {
                    selection.label == *name
                        && selection.matcher.as_ref().is_none_or(|(op, expected)| {
                            string_match(*op, value, expected).unwrap_or(false)
                        })
                })
            });
            Ok(true)
        }
        PipelineStage::Keep(selections) => {
            row.labels.retain(|name, value| {
                selections.iter().any(|selection| {
                    selection.label == *name
                        && selection.matcher.as_ref().is_none_or(|(op, expected)| {
                            string_match(*op, value, expected).unwrap_or(false)
                        })
                })
            });
            Ok(true)
        }
        PipelineStage::Decolorize => {
            row.line = ansi_regex().replace_all(&row.line, "").into_owned();
            Ok(true)
        }
        PipelineStage::Unwrap(unwrap) => apply_unwrap(row, unwrap),
        // The index only produces candidates; the source line remains the
        // authority so stale/colliding postings cannot create false matches.
        PipelineStage::Match(query) => Ok(source_matches(&row.line, &query_terms(query))),
    }
}

fn line_filter(line: &str, filter: &LineFilter) -> Result<bool> {
    for branch in &filter.branches {
        let term = match &branch.term {
            LineFilterTerm::String(value) | LineFilterTerm::Ip(value) => value,
        };
        let found = match (&branch.op, &branch.term) {
            (LineFilterOp::Contains | LineFilterOp::NotContains, LineFilterTerm::Ip(value)) => line
                .split(|character: char| {
                    !(character.is_ascii_hexdigit() || ".:/".contains(character))
                })
                .any(|candidate| ip_matches(candidate, value)),
            (LineFilterOp::Contains | LineFilterOp::NotContains, _) => line.contains(term),
            (LineFilterOp::Regex | LineFilterOp::NotRegex, _) => Regex::new(term)?.is_match(line),
            (LineFilterOp::Pattern | LineFilterOp::NotPattern, _) => {
                pattern_regex(term, false)?.is_match(line)
            }
        };
        let accepted = match branch.op {
            LineFilterOp::NotContains | LineFilterOp::NotRegex | LineFilterOp::NotPattern => !found,
            _ => found,
        };
        if accepted {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_stage(row: &mut Row, parser: &ParserStage) -> Result<()> {
    if let ParserStage::Logfmt {
        strict,
        keep_empty,
        expressions,
    } = parser
    {
        let (labels, error) = parse_logfmt(&row.line, *strict, *keep_empty, expressions)?;
        merge_parsed(row, labels);
        if let Some(error) = error {
            set_error(row, "LogfmtParserErr", &error);
        }
        return Ok(());
    }
    let result = match parser {
        ParserStage::Json { expressions } => parse_json(&row.line, expressions),
        ParserStage::Logfmt { .. } => unreachable!(),
        ParserStage::Regexp(expression) => {
            let regex = Regex::new(expression)?;
            let mut labels = BTreeMap::new();
            if let Some(captures) = regex.captures(&row.line) {
                for name in regex.capture_names().flatten() {
                    if let Some(value) = captures.name(name) {
                        labels.insert(name.to_owned(), value.as_str().to_owned());
                    }
                }
            }
            Ok(labels)
        }
        ParserStage::Pattern(pattern) => {
            let regex = pattern_regex(pattern, true)?;
            let mut labels = BTreeMap::new();
            if let Some(captures) = regex.captures(&row.line) {
                for name in regex.capture_names().flatten() {
                    if let Some(value) = captures.name(name) {
                        labels.insert(name.to_owned(), value.as_str().to_owned());
                    }
                }
            }
            Ok(labels)
        }
        ParserStage::Unpack => unpack(&row.line).map(|(line, labels)| {
            row.line = line;
            labels
        }),
    };
    match result {
        Ok(labels) => {
            merge_parsed(row, labels);
            Ok(())
        }
        Err(error) => {
            set_error(row, parser_error_name(parser), &error.to_string());
            Ok(())
        }
    }
}

fn parser_error_name(parser: &ParserStage) -> &'static str {
    match parser {
        ParserStage::Json { .. } => "JSONParserErr",
        ParserStage::Logfmt { .. } => "LogfmtParserErr",
        ParserStage::Regexp(_) => "RegexpParserErr",
        ParserStage::Pattern(_) => "PatternParserErr",
        ParserStage::Unpack => "UnpackParserErr",
    }
}

fn merge_parsed(row: &mut Row, labels: BTreeMap<String, String>) {
    for (mut name, value) in labels {
        if row.labels.contains_key(&name) {
            name.push_str("_extracted");
        }
        row.labels.insert(name, value);
    }
}

fn set_error(row: &mut Row, kind: &str, details: &str) {
    row.labels.insert("__error__".into(), kind.into());
    row.labels
        .insert("__error_details__".into(), details.into());
}

fn parse_json(line: &str, expressions: &[ParserExpression]) -> Result<BTreeMap<String, String>> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    let mut flattened = BTreeMap::new();
    if expressions.is_empty() {
        flatten_json("", &value, &mut flattened);
    } else {
        for expression in expressions {
            let extracted =
                json_path(&value, &expression.expression)?.map_or_else(String::new, json_string);
            flattened.insert(expression.label.clone(), extracted);
        }
    }
    Ok(flattened)
}

fn flatten_json(prefix: &str, value: &serde_json::Value, output: &mut BTreeMap<String, String>) {
    if let serde_json::Value::Object(object) = value {
        for (name, value) in object {
            let name = sanitize_label(if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}_{name}")
            });
            if value.is_object() {
                flatten_json(&name, value, output);
            } else {
                output.insert(name, json_string(value));
            }
        }
    }
}

fn json_path<'a>(
    mut value: &'a serde_json::Value,
    path: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let mut cursor = 0usize;
    let bytes = path.as_bytes();
    while cursor < path.len() {
        if bytes[cursor] == b'.' {
            cursor += 1;
            if cursor == path.len() {
                return Err(Error::Query("JSON path cannot end with '.'".into()));
            }
        }
        if bytes[cursor] == b'[' {
            cursor += 1;
            while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor == path.len() {
                return Err(Error::Query("unterminated JSON path bracket".into()));
            }
            if bytes[cursor] == b'"' {
                let start = cursor;
                cursor += 1;
                let mut escaped = false;
                while cursor < path.len() {
                    let byte = bytes[cursor];
                    cursor += 1;
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        break;
                    }
                }
                let key: String = serde_json::from_str(&path[start..cursor])?;
                while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if bytes.get(cursor) != Some(&b']') {
                    return Err(Error::Query("JSON path quoted key requires ']'".into()));
                }
                cursor += 1;
                let Some(next) = value.get(&key) else {
                    return Ok(None);
                };
                value = next;
            } else {
                let start = cursor;
                while cursor < path.len() && bytes[cursor].is_ascii_digit() {
                    cursor += 1;
                }
                let index = path[start..cursor]
                    .parse::<usize>()
                    .map_err(|_| Error::Query("JSON array index must be an integer".into()))?;
                while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if bytes.get(cursor) != Some(&b']') {
                    return Err(Error::Query("JSON array index requires ']'".into()));
                }
                cursor += 1;
                let Some(next) = value.get(index) else {
                    return Ok(None);
                };
                value = next;
            }
        } else {
            let start = cursor;
            let first = bytes[cursor];
            if !(first.is_ascii_alphabetic() || first == b'_') {
                return Err(Error::Query("invalid JSON path identifier".into()));
            }
            cursor += 1;
            while cursor < path.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            let Some(next) = value.get(&path[start..cursor]) else {
                return Ok(None);
            };
            value = next;
        }
        if cursor < path.len() && bytes[cursor] != b'.' && bytes[cursor] != b'[' {
            return Err(Error::Query("invalid JSON path separator".into()));
        }
    }
    Ok(Some(value))
}

fn json_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn parse_logfmt(
    line: &str,
    strict: bool,
    keep_empty: bool,
    expressions: &[ParserExpression],
) -> Result<(BTreeMap<String, String>, Option<String>)> {
    let mut all = BTreeMap::new();
    let mut cursor = 0usize;
    let mut parse_error = None;
    while cursor < line.len() {
        while cursor < line.len() && line.as_bytes()[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == line.len() {
            break;
        }
        let token_start = cursor;
        let key_start = cursor;
        while cursor < line.len()
            && !line.as_bytes()[cursor].is_ascii_whitespace()
            && !matches!(line.as_bytes()[cursor], b'=' | b'"')
        {
            cursor += 1;
        }
        if cursor == key_start || line.as_bytes().get(cursor) == Some(&b'"') {
            let error = format!("logfmt syntax error at pos {} : invalid key", cursor + 1);
            if strict {
                parse_error = Some(error);
                break;
            }
            skip_logfmt_token(line, &mut cursor);
            continue;
        }
        let key = sanitize_label(line[key_start..cursor].to_owned());
        let value = if line.as_bytes().get(cursor) == Some(&b'=') {
            cursor += 1;
            if line.as_bytes().get(cursor) == Some(&b'"') {
                match scan_logfmt_quoted(line, &mut cursor) {
                    Ok(value) => value,
                    Err(error) if strict => {
                        parse_error = Some(error);
                        break;
                    }
                    Err(_) => {
                        skip_logfmt_token(line, &mut cursor);
                        continue;
                    }
                }
            } else {
                let start = cursor;
                while cursor < line.len() && !line.as_bytes()[cursor].is_ascii_whitespace() {
                    if matches!(line.as_bytes()[cursor], b'=' | b'"') {
                        let error = format!(
                            "logfmt syntax error at pos {} : unexpected '{}'",
                            cursor + 1,
                            char::from(line.as_bytes()[cursor])
                        );
                        if strict {
                            parse_error = Some(error);
                            break;
                        }
                        skip_logfmt_token(line, &mut cursor);
                        break;
                    }
                    cursor += 1;
                }
                if cursor < line.len() && !line.as_bytes()[cursor].is_ascii_whitespace() {
                    continue;
                }
                line[start..cursor].to_owned()
            }
        } else {
            String::new()
        };
        if key.is_empty() {
            if strict {
                parse_error = Some(format!(
                    "logfmt syntax error at pos {} : invalid key",
                    token_start + 1
                ));
                break;
            }
            continue;
        }
        if keep_empty || !value.is_empty() {
            all.entry(key).or_insert(value);
        }
    }
    if expressions.is_empty() {
        Ok((all, parse_error))
    } else {
        Ok((
            expressions
                .iter()
                .map(|expression| {
                    (
                        expression.label.clone(),
                        all.get(&sanitize_label(expression.expression.clone()))
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect(),
            parse_error,
        ))
    }
}

fn skip_logfmt_token(line: &str, cursor: &mut usize) {
    while *cursor < line.len() && !line.as_bytes()[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }
}

fn scan_logfmt_quoted(line: &str, cursor: &mut usize) -> std::result::Result<String, String> {
    *cursor += 1;
    let mut output = String::new();
    let mut chars = line[*cursor..].char_indices();
    while let Some((relative, character)) = chars.next() {
        *cursor += character.len_utf8();
        match character {
            '"' => return Ok(output),
            '\\' => {
                let Some((_, escaped)) = chars.next() else {
                    return Err(format!(
                        "logfmt syntax error at pos {} : invalid quoted value",
                        *cursor + relative + 1
                    ));
                };
                *cursor += escaped.len_utf8();
                output.push(match escaped {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '"' => '"',
                    '\\' => '\\',
                    other => other,
                });
            }
            value => output.push(value),
        }
    }
    Err(format!(
        "logfmt syntax error at pos {} : unterminated quoted value",
        *cursor + 1
    ))
}

fn unpack(line: &str) -> Result<(String, BTreeMap<String, String>)> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::Query("packed value must be an object".into()))?;
    let unpacked = object
        .get("_entry")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(line)
        .to_owned();
    let mut labels = BTreeMap::new();
    for (name, value) in object {
        if name != "_entry" {
            labels.insert(sanitize_label(name.clone()), json_string(value));
        }
    }
    Ok((unpacked, labels))
}

fn sanitize_label(name: String) -> String {
    let mut result = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if result.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        result.insert(0, '_');
    }
    result
}

fn pattern_regex(pattern: &str, captures: bool) -> Result<Regex> {
    let mut source = String::new();
    let mut rest = pattern;
    while let Some(start) = rest.find('<') {
        let Some(relative_end) = rest[start + 1..].find('>') else {
            break;
        };
        let end = start + relative_end + 1;
        source.push_str(&regex::escape(&rest[..start]));
        let name = &rest[start + 1..end];
        if name == "_" || !captures {
            source.push_str(".*?");
        } else {
            source.push_str(&format!("(?P<{name}>.*?)"));
        }
        rest = &rest[end + 1..];
    }
    source.push_str(&regex::escape(rest));
    Regex::new(&format!("(?s)^{source}$")).map_err(Into::into)
}

fn label_filter(row: &Row, expression: &LabelFilterExpr) -> Result<bool> {
    match expression {
        LabelFilterExpr::And(lhs, rhs) => {
            Ok(label_filter(row, &lhs.value)? && label_filter(row, &rhs.value)?)
        }
        LabelFilterExpr::Or(lhs, rhs) => {
            Ok(label_filter(row, &lhs.value)? || label_filter(row, &rhs.value)?)
        }
        LabelFilterExpr::Predicate(predicate) => {
            let actual = lookup(row, &predicate.label).unwrap_or("");
            compare_filter(actual, predicate.op, &predicate.value)
        }
    }
}

fn compare_filter(actual: &str, op: ComparisonOp, expected: &FilterValue) -> Result<bool> {
    if matches!(op, ComparisonOp::Regex | ComparisonOp::NotRegex) {
        let (FilterValue::String(expected) | FilterValue::Identifier(expected)) = expected else {
            return Ok(false);
        };
        let matched = Regex::new(&format!("^(?:{expected})$"))?.is_match(actual);
        return Ok(if op == ComparisonOp::Regex {
            matched
        } else {
            !matched
        });
    }
    let ordering = match expected {
        FilterValue::Number(value) => numeric_cmp(actual, value.parse().ok()),
        FilterValue::Bytes(value) => parse_bytes(actual)
            .ok()
            .and_then(|actual| actual.partial_cmp(&parse_bytes(value).ok()?)),
        FilterValue::Duration(value) => parse_duration_ns(actual)
            .ok()
            .and_then(|actual| actual.cmp(&parse_duration_ns(value).ok()?).into()),
        FilterValue::Ip(value) => {
            let matched = ip_matches(actual, value);
            return Ok(compare_bool(matched, op));
        }
        FilterValue::String(value) | FilterValue::Identifier(value) => Some(actual.cmp(value)),
    };
    Ok(ordering.is_some_and(|ordering| compare_ordering(ordering, op)))
}

fn numeric_cmp(actual: &str, expected: Option<f64>) -> Option<Ordering> {
    actual.parse::<f64>().ok()?.partial_cmp(&expected?)
}

fn compare_bool(equal: bool, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Equal => equal,
        ComparisonOp::NotEqual => !equal,
        _ => false,
    }
}

fn compare_ordering(ordering: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterOrEqual => ordering != Ordering::Less,
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessOrEqual => ordering != Ordering::Greater,
        ComparisonOp::Regex | ComparisonOp::NotRegex => false,
    }
}

fn label_format(row: &mut Row, assignments: &[FormatAssignment]) {
    for assignment in assignments {
        let value = if assignment.rename {
            row.labels
                .remove(&assignment.value)
                .or_else(|| row.metadata.remove(&assignment.value))
                .unwrap_or_default()
        } else {
            match render_template(&assignment.value, row) {
                Ok(value) => value,
                Err(error) => {
                    set_error(row, "TemplateFormatErr", &error.to_string());
                    continue;
                }
            }
        };
        row.labels.insert(assignment.label.clone(), value);
    }
}

fn render_template(template: &str, row: &Row) -> Result<String> {
    render_template_block(template, row)
}

#[derive(Clone, Debug)]
enum TemplateValue {
    String(String),
    Number(f64),
    Bool(bool),
    Time(i64),
}

impl TemplateValue {
    fn text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Number(value) => format_number(*value),
            Self::Bool(value) => value.to_string(),
            Self::Time(value) => Utc
                .timestamp_nanos(*value)
                .format("%Y-%m-%d %H:%M:%S%.f %z %Z")
                .to_string(),
        }
    }

    fn number(&self) -> Result<f64> {
        match self {
            Self::Number(value) => Ok(*value),
            Self::String(value) => value
                .parse()
                .map_err(|_| Error::Query(format!("template value {value:?} is not numeric"))),
            Self::Bool(_) | Self::Time(_) => {
                Err(Error::Query("template value is not numeric".into()))
            }
        }
    }

    fn truthy(&self) -> bool {
        match self {
            Self::String(value) => !value.is_empty(),
            Self::Number(value) => *value != 0.0,
            Self::Bool(value) => *value,
            Self::Time(_) => true,
        }
    }
}

fn render_template_block(template: &str, row: &Row) -> Result<String> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let action_start = start + 2;
        let end = rest[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = rest[action_start..end].trim();
        rest = &rest[end + 2..];
        if let Some(condition) = action.strip_prefix("if ") {
            let (body, tail) = template_branch(rest)?;
            let (yes, no) = split_template_else(body)?;
            let selected = if eval_template_expression(condition, row)?.truthy() {
                yes
            } else {
                no
            };
            output.push_str(&render_template_block(selected, row)?);
            rest = tail;
        } else if action == "else" || action == "end" {
            return Err(Error::Query(format!(
                "unexpected template action {action:?}"
            )));
        } else {
            output.push_str(&eval_template_expression(action, row)?.text());
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn template_branch(input: &str) -> Result<(&str, &str)> {
    let mut depth = 1usize;
    let mut cursor = 0usize;
    while let Some(start) = input[cursor..].find("{{") {
        let start = cursor + start;
        let action_start = start + 2;
        let end = input[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = input[action_start..end].trim();
        if action.starts_with("if ") {
            depth += 1;
        } else if action == "end" {
            depth -= 1;
            if depth == 0 {
                return Ok((&input[..start], &input[end + 2..]));
            }
        }
        cursor = end + 2;
    }
    Err(Error::Query("template if has no end".into()))
}

fn split_template_else(body: &str) -> Result<(&str, &str)> {
    let mut depth = 0usize;
    let mut cursor = 0usize;
    while let Some(start) = body[cursor..].find("{{") {
        let start = cursor + start;
        let action_start = start + 2;
        let end = body[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = body[action_start..end].trim();
        if action.starts_with("if ") {
            depth += 1;
        } else if action == "end" {
            depth = depth.saturating_sub(1);
        } else if action == "else" && depth == 0 {
            return Ok((&body[..start], &body[end + 2..]));
        }
        cursor = end + 2;
    }
    Ok((body, ""))
}

fn eval_template_expression(expression: &str, row: &Row) -> Result<TemplateValue> {
    let commands = split_top_level(expression, '|')?;
    let mut value = None;
    for command in commands {
        let mut tokens = template_tokens(command.trim())?;
        if let Some(previous) = value.take() {
            tokens.push(previous);
        }
        value = Some(eval_template_command(&tokens, row)?);
    }
    value.ok_or_else(|| Error::Query("empty template action".into()))
}

fn split_top_level(input: &str, separator: char) -> Result<Vec<&str>> {
    let mut result = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::Query("unbalanced template parentheses".into()))?;
            }
            value if value == separator && !quoted && depth == 0 => {
                result.push(&input[start..index]);
                start = index + value.len_utf8();
            }
            _ => {}
        }
    }
    if quoted || depth != 0 {
        return Err(Error::Query("unbalanced template expression".into()));
    }
    result.push(&input[start..]);
    Ok(result)
}

fn template_tokens(command: &str) -> Result<Vec<TemplateValue>> {
    let mut result = Vec::new();
    let mut cursor = 0usize;
    let bytes = command.as_bytes();
    while cursor < command.len() {
        while cursor < command.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == command.len() {
            break;
        }
        if bytes[cursor] == b'"' {
            let start = cursor;
            cursor += 1;
            let mut escaped = false;
            while cursor < command.len() {
                let byte = bytes[cursor];
                cursor += 1;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    break;
                }
            }
            if bytes.get(cursor - 1) != Some(&b'"') {
                return Err(Error::Query("unterminated template string".into()));
            }
            let value: String = serde_json::from_str(&command[start..cursor])?;
            result.push(TemplateValue::String(value));
        } else if bytes[cursor] == b'(' {
            let start = cursor + 1;
            let mut depth = 1usize;
            let mut quoted = false;
            cursor += 1;
            while cursor < command.len() && depth != 0 {
                match bytes[cursor] {
                    b'"' => quoted = !quoted,
                    b'(' if !quoted => depth += 1,
                    b')' if !quoted => depth -= 1,
                    _ => {}
                }
                cursor += 1;
            }
            if depth != 0 {
                return Err(Error::Query("unbalanced template parentheses".into()));
            }
            result.push(TemplateValue::String(format!(
                "\0expr:{}",
                &command[start..cursor - 1]
            )));
        } else {
            let start = cursor;
            while cursor < command.len()
                && !bytes[cursor].is_ascii_whitespace()
                && bytes[cursor] != b'('
                && bytes[cursor] != b')'
            {
                cursor += 1;
            }
            result.push(TemplateValue::String(command[start..cursor].to_owned()));
        }
    }
    Ok(result)
}

fn eval_template_command(tokens: &[TemplateValue], row: &Row) -> Result<TemplateValue> {
    let Some(first) = tokens.first() else {
        return Err(Error::Query("empty template command".into()));
    };
    let name = first.text();
    if tokens.len() == 1 {
        return resolve_template_argument(first, row);
    }
    let arguments = tokens[1..]
        .iter()
        .map(|argument| resolve_template_argument(argument, row))
        .collect::<Result<Vec<_>>>()?;
    apply_template_function(&name, &arguments, row)
}

fn resolve_template_argument(value: &TemplateValue, row: &Row) -> Result<TemplateValue> {
    let TemplateValue::String(value) = value else {
        return Ok(value.clone());
    };
    if let Some(expression) = value.strip_prefix("\0expr:") {
        return eval_template_expression(expression, row);
    }
    if let Some(name) = value.strip_prefix('.') {
        return Ok(TemplateValue::String(
            lookup(row, name).unwrap_or("").to_owned(),
        ));
    }
    match value.as_str() {
        "__line__" => Ok(TemplateValue::String(row.line.clone())),
        "__timestamp__" => Ok(TemplateValue::Time(row.timestamp_ns)),
        "now" => Ok(TemplateValue::Time(
            Utc::now().timestamp_nanos_opt().unwrap_or(0),
        )),
        "true" => Ok(TemplateValue::Bool(true)),
        "false" => Ok(TemplateValue::Bool(false)),
        _ => value
            .parse::<f64>()
            .map(TemplateValue::Number)
            .or_else(|_| Ok(TemplateValue::String(value.clone()))),
    }
}

fn apply_template_function(
    name: &str,
    arguments: &[TemplateValue],
    _row: &Row,
) -> Result<TemplateValue> {
    let numbers = || {
        arguments
            .iter()
            .map(TemplateValue::number)
            .collect::<Result<Vec<_>>>()
    };
    let string = |value: String| Ok(TemplateValue::String(value));
    let number = |value: f64| Ok(TemplateValue::Number(value));
    match name {
        "ToLower" | "lower" => unary_string(arguments, str::to_lowercase),
        "ToUpper" | "upper" => unary_string(arguments, str::to_uppercase),
        "title" => unary_string(arguments, |value| {
            value
                .split_whitespace()
                .map(|word| {
                    let mut chars = word.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" ")
        }),
        "TrimSpace" | "trim" => unary_string(arguments, |value| value.trim().to_owned()),
        "Trim" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[0]
                    .text()
                    .trim_matches(|c| arguments[1].text().contains(c))
                    .to_owned(),
            )
        }
        "trimAll" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[1]
                    .text()
                    .trim_matches(|c| arguments[0].text().contains(c))
                    .to_owned(),
            )
        }
        "TrimLeft" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[0]
                    .text()
                    .trim_start_matches(|c| arguments[1].text().contains(c))
                    .to_owned(),
            )
        }
        "TrimRight" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[0]
                    .text()
                    .trim_end_matches(|c| arguments[1].text().contains(c))
                    .to_owned(),
            )
        }
        "TrimPrefix" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[0]
                    .text()
                    .strip_prefix(&arguments[1].text())
                    .unwrap_or(&arguments[0].text())
                    .to_owned(),
            )
        }
        "trimPrefix" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[1]
                    .text()
                    .strip_prefix(&arguments[0].text())
                    .unwrap_or(&arguments[1].text())
                    .to_owned(),
            )
        }
        "TrimSuffix" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[0]
                    .text()
                    .strip_suffix(&arguments[1].text())
                    .unwrap_or(&arguments[0].text())
                    .to_owned(),
            )
        }
        "trimSuffix" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[1]
                    .text()
                    .strip_suffix(&arguments[0].text())
                    .unwrap_or(&arguments[1].text())
                    .to_owned(),
            )
        }
        "replace" => {
            require_args(name, arguments, 3)?;
            string(
                arguments[2]
                    .text()
                    .replace(&arguments[0].text(), &arguments[1].text()),
            )
        }
        "Replace" => {
            require_args(name, arguments, 4)?;
            let count = arguments[3].number()? as isize;
            let source = arguments[0].text();
            string(if count < 0 {
                source.replace(&arguments[1].text(), &arguments[2].text())
            } else {
                source.replacen(&arguments[1].text(), &arguments[2].text(), count as usize)
            })
        }
        "contains" | "hasPrefix" | "hasSuffix" => {
            require_args(name, arguments, 2)?;
            let needle = arguments[0].text();
            let source = arguments[1].text();
            Ok(TemplateValue::Bool(match name {
                "contains" => source.contains(&needle),
                "hasPrefix" => source.starts_with(&needle),
                _ => source.ends_with(&needle),
            }))
        }
        "repeat" => {
            require_args(name, arguments, 2)?;
            string(
                arguments[1]
                    .text()
                    .repeat(arguments[0].number()?.max(0.0) as usize),
            )
        }
        "trunc" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].number()? as isize;
            let chars = arguments[1].text().chars().collect::<Vec<_>>();
            let slice = if count < 0 {
                &chars[chars.len().saturating_sub(count.unsigned_abs())..]
            } else {
                &chars[..chars.len().min(count as usize)]
            };
            string(slice.iter().collect())
        }
        "substr" => {
            require_args(name, arguments, 3)?;
            let chars = arguments[2].text().chars().collect::<Vec<_>>();
            let start = (arguments[0].number()? as usize).min(chars.len());
            let end = if arguments[1].number()? < 0.0 {
                chars.len()
            } else {
                (arguments[1].number()? as usize).min(chars.len())
            };
            string(chars[start.min(end)..end].iter().collect())
        }
        "indent" | "nindent" => {
            require_args(name, arguments, 2)?;
            let padding = " ".repeat(arguments[0].number()?.max(0.0) as usize);
            let value = arguments[1].text().replace('\n', &format!("\n{padding}"));
            string(if name == "nindent" {
                format!("\n{padding}{value}")
            } else {
                format!("{padding}{value}")
            })
        }
        "alignLeft" | "alignRight" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].number()?.max(0.0) as usize;
            let mut chars = arguments[1].text().chars().collect::<Vec<_>>();
            if chars.len() > count {
                chars = if name == "alignLeft" {
                    chars[..count].to_vec()
                } else {
                    chars[chars.len() - count..].to_vec()
                };
            }
            let value = chars.iter().collect::<String>();
            string(if name == "alignLeft" {
                format!("{value:<count$}")
            } else {
                format!("{value:>count$}")
            })
        }
        "default" => {
            require_args(name, arguments, 2)?;
            Ok(if arguments[1].truthy() {
                arguments[1].clone()
            } else {
                arguments[0].clone()
            })
        }
        "int" | "float64" => {
            require_args(name, arguments, 1)?;
            number(if name == "int" {
                arguments[0].number()?.trunc()
            } else {
                arguments[0].number()?
            })
        }
        "add" | "sub" | "mul" | "div" | "mod" | "addf" | "subf" | "mulf" | "divf" => {
            let values = numbers()?;
            if values.is_empty() {
                return Err(Error::Query(format!("{name} requires arguments")));
            }
            let mut value = values[0];
            for next in &values[1..] {
                value = match name {
                    "add" | "addf" => value + next,
                    "sub" | "subf" => value - next,
                    "mul" | "mulf" => value * next,
                    "div" => (value as i64 / *next as i64) as f64,
                    "mod" => (value as i64 % *next as i64) as f64,
                    "divf" => value / next,
                    _ => unreachable!(),
                };
            }
            number(value)
        }
        "min" | "minf" | "max" | "maxf" => {
            let values = numbers()?;
            let value = values
                .into_iter()
                .reduce(if name.starts_with("min") {
                    f64::min
                } else {
                    f64::max
                })
                .ok_or_else(|| Error::Query(format!("{name} requires arguments")))?;
            number(value)
        }
        "ceil" | "floor" => {
            require_args(name, arguments, 1)?;
            number(if name == "ceil" {
                arguments[0].number()?.ceil()
            } else {
                arguments[0].number()?.floor()
            })
        }
        "round" => {
            if arguments.is_empty() || arguments.len() > 3 {
                return Err(Error::Query("round expects one to three arguments".into()));
            }
            let precision = arguments
                .get(1)
                .map(TemplateValue::number)
                .transpose()?
                .unwrap_or(0.0) as i32;
            let factor = 10f64.powi(precision);
            number((arguments[0].number()? * factor).round() / factor)
        }
        "count" => {
            require_args(name, arguments, 2)?;
            number(
                Regex::new(&arguments[0].text())?
                    .find_iter(&arguments[1].text())
                    .count() as f64,
            )
        }
        "regexReplaceAll" | "regexReplaceAllLiteral" => {
            require_args(name, arguments, 3)?;
            let regex = Regex::new(&arguments[0].text())?;
            string(if name == "regexReplaceAll" {
                regex
                    .replace_all(&arguments[1].text(), arguments[2].text())
                    .into_owned()
            } else {
                regex
                    .replace_all(&arguments[1].text(), regex::NoExpand(&arguments[2].text()))
                    .into_owned()
            })
        }
        "bytes" => {
            require_args(name, arguments, 1)?;
            number(parse_bytes(&arguments[0].text())?)
        }
        "duration" | "duration_seconds" => {
            require_args(name, arguments, 1)?;
            number(parse_duration_ns(&arguments[0].text())? as f64 / 1_000_000_000.0)
        }
        "b64enc" => {
            require_args(name, arguments, 1)?;
            string(base64::engine::general_purpose::STANDARD.encode(arguments[0].text()))
        }
        "b64dec" => {
            require_args(name, arguments, 1)?;
            let mut input = arguments[0].text();
            if input.len() % 4 > 1 {
                input.extend(std::iter::repeat_n('=', 4 - input.len() % 4));
            }
            string(
                String::from_utf8_lossy(
                    &base64::engine::general_purpose::STANDARD
                        .decode(input)
                        .map_err(|error| Error::Query(error.to_string()))?,
                )
                .into_owned(),
            )
        }
        "urlencode" => {
            require_args(name, arguments, 1)?;
            string(query_escape(&arguments[0].text()))
        }
        "urldecode" => {
            require_args(name, arguments, 1)?;
            string(query_unescape(&arguments[0].text())?)
        }
        "unixEpoch" | "unixEpochMillis" | "unixEpochNanos" => {
            require_args(name, arguments, 1)?;
            let timestamp = template_timestamp(&arguments[0])?;
            number(
                match name {
                    "unixEpoch" => timestamp as f64 / 1_000_000_000.0,
                    "unixEpochMillis" => (timestamp / 1_000_000) as f64,
                    _ => timestamp as f64,
                }
                .trunc(),
            )
        }
        "unixToTime" => {
            require_args(name, arguments, 1)?;
            Ok(TemplateValue::Time(template_timestamp(&arguments[0])?))
        }
        "date" => {
            require_args(name, arguments, 2)?;
            let timestamp = template_timestamp(&arguments[1])?;
            string(format_go_time(timestamp, &arguments[0].text()))
        }
        "toDate" | "toDateInZone" => {
            require_args(name, arguments, if name == "toDate" { 2 } else { 3 })?;
            let source = arguments.last().expect("required").text();
            let timestamp = parse_go_time(&source, &arguments[0].text())?;
            Ok(TemplateValue::Time(timestamp))
        }
        unknown => Err(Error::Query(format!(
            "unknown or unsupported template function {unknown:?}"
        ))),
    }
}

fn unary_string(
    arguments: &[TemplateValue],
    function: impl FnOnce(&str) -> String,
) -> Result<TemplateValue> {
    require_args("string function", arguments, 1)?;
    Ok(TemplateValue::String(function(&arguments[0].text())))
}

fn require_args(name: &str, arguments: &[TemplateValue], count: usize) -> Result<()> {
    if arguments.len() != count {
        Err(Error::Query(format!(
            "template function {name} expects {count} arguments, got {}",
            arguments.len()
        )))
    } else {
        Ok(())
    }
}

fn template_timestamp(value: &TemplateValue) -> Result<i64> {
    match value {
        TemplateValue::Time(value) => Ok(*value),
        TemplateValue::String(value) => {
            let multiplier = match value.len() {
                5 => 86_400_000_000_000,
                10 => 1_000_000_000,
                13 => 1_000_000,
                16 => 1_000,
                19 => 1,
                _ => {
                    return Err(Error::Query(format!(
                        "cannot infer epoch unit for {value:?}"
                    )));
                }
            };
            value
                .parse::<i64>()
                .map(|value| value.saturating_mul(multiplier))
                .map_err(|error| Error::Query(error.to_string()))
        }
        _ => Err(Error::Query("template value is not a timestamp".into())),
    }
}

fn format_go_time(timestamp_ns: i64, layout: &str) -> String {
    let format = layout
        .replace("2006", "%Y")
        .replace("01", "%m")
        .replace("02", "%d")
        .replace("15", "%H")
        .replace("04", "%M")
        .replace("05", "%S")
        .replace("MST", "%Z");
    Utc.timestamp_nanos(timestamp_ns)
        .format(&format)
        .to_string()
}

fn parse_go_time(source: &str, layout: &str) -> Result<i64> {
    if layout.contains('Z') {
        return DateTime::parse_from_rfc3339(source)
            .map(|value| value.timestamp_nanos_opt().unwrap_or(0))
            .map_err(|error| Error::Query(error.to_string()));
    }
    let format = layout
        .replace("2006", "%Y")
        .replace("01", "%m")
        .replace("02", "%d")
        .replace("15", "%H")
        .replace("04", "%M")
        .replace("05", "%S");
    chrono::NaiveDateTime::parse_from_str(source, &format)
        .map(|value| value.and_utc().timestamp_nanos_opt().unwrap_or(0))
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(source, &format).map(|value| {
                value
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight")
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
            })
        })
        .map_err(|error| Error::Query(error.to_string()))
}

fn query_escape(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                output.push(char::from(byte));
            }
            b' ' => output.push('+'),
            _ => output.push_str(&format!("%{byte:02X}")),
        }
    }
    output
}

fn query_unescape(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = input
                    .next()
                    .ok_or_else(|| Error::Query("truncated URL escape".into()))?;
                let low = input
                    .next()
                    .ok_or_else(|| Error::Query("truncated URL escape".into()))?;
                let hex = [high, low];
                bytes.push(
                    u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or(""), 16)
                        .map_err(|error| Error::Query(error.to_string()))?,
                );
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8(bytes).map_err(|error| Error::Query(error.to_string()))
}

fn format_number(value: f64) -> String {
    if value.abs() >= 1_000_000.0 {
        let scientific = format!("{value:.0e}");
        let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
        let exponent = exponent.parse::<i32>().expect("numeric exponent");
        format!("{mantissa}e{exponent:+03}")
    } else if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn lookup<'a>(row: &'a Row, name: &str) -> Option<&'a str> {
    match name {
        "__line__" => Some(&row.line),
        _ => row
            .labels
            .get(name)
            .or_else(|| row.metadata.get(name))
            .map(String::as_str),
    }
}

fn apply_unwrap(row: &mut Row, unwrap: &Unwrap) -> Result<bool> {
    let source = lookup(row, &unwrap.label).unwrap_or("").to_owned();
    let parsed = match unwrap.conversion {
        None => source.parse::<f64>().ok(),
        Some(Conversion::Bytes) => parse_bytes(&source).ok(),
        Some(Conversion::Duration) => parse_duration_ns(&source).ok().map(|value| value as f64),
        Some(Conversion::DurationSeconds) => parse_duration_ns(&source)
            .ok()
            .map(|value| value as f64 / 1_000_000_000.0),
    };
    match parsed {
        Some(value) => row.value = Some(value),
        None => set_error(row, "SampleExtractionErr", "unable to convert unwrap value"),
    }
    unwrap
        .post_filter
        .as_ref()
        .map_or(Ok(true), |filter| label_filter(row, &filter.value))
}

fn eval_expr(query: &Query, rows: &[Row], timestamp: i64) -> Result<Value> {
    match &query.value {
        Expr::Number(value) => Ok(Value::Scalar(*value)),
        Expr::String(_) => Err(Error::Query("string is not a numeric expression".into())),
        Expr::Vector(expr) => match eval_expr(expr, rows, timestamp)? {
            Value::Scalar(value) => Ok(Value::Vector(vec![Point {
                labels: BTreeMap::new(),
                value,
            }])),
            value => Ok(value),
        },
        Expr::Log(_) => Err(Error::Query(
            "raw log expressions cannot be used as metric operands".into(),
        )),
        Expr::RangeAggregation {
            op,
            parameter,
            expr,
            grouping,
        } => range_aggregation(*op, *parameter, expr, grouping.as_ref(), rows, timestamp),
        Expr::LabelAggregation {
            field,
            expr,
            grouping,
        } => label_aggregation(field, expr, grouping.as_ref(), rows, timestamp),
        Expr::VectorAggregation {
            op,
            parameter,
            expr,
            grouping,
        } => {
            let Value::Vector(points) = eval_expr(expr, rows, timestamp)? else {
                return Err(Error::Query("vector aggregation requires a vector".into()));
            };
            Ok(Value::Vector(vector_aggregation(
                *op,
                *parameter,
                grouping.as_ref(),
                points,
            )))
        }
        Expr::LabelReplace {
            expr,
            dst,
            replacement,
            src,
            regex,
        } => {
            let Value::Vector(mut points) = eval_expr(expr, rows, timestamp)? else {
                return Err(Error::Query("label_replace requires a vector".into()));
            };
            let regex = Regex::new(&format!("^(?:{regex})$"))?;
            for point in &mut points {
                let source = point.labels.get(src).map_or("", String::as_str);
                if regex.is_match(source) {
                    let replaced = regex.replace(source, replacement.as_str()).into_owned();
                    if replaced.is_empty() {
                        point.labels.remove(dst);
                    } else {
                        point.labels.insert(dst.clone(), replaced);
                    }
                }
            }
            Ok(Value::Vector(points))
        }
        Expr::Binary {
            lhs,
            op,
            modifier,
            rhs,
        } => binary(
            eval_expr(lhs, rows, timestamp)?,
            *op,
            modifier.as_ref(),
            eval_expr(rhs, rows, timestamp)?,
        ),
    }
}

fn range_aggregation(
    op: RangeOp,
    parameter: Option<f64>,
    log: &LogExpr,
    grouping: Option<&Grouping>,
    rows: &[Row],
    timestamp: i64,
) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("range aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    // LogQL range windows are left-open and right-closed: (t-range, t].
    let selected = eval_metric_log(log, rows, timestamp.saturating_sub(range), timestamp)?;
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<Row>> = BTreeMap::new();
    for row in selected {
        let mut labels = group_labels(&row.labels, grouping);
        if grouping.is_none_or(|grouping| grouping.without)
            && let Some(label) = log.stages.iter().find_map(|stage| {
                if let PipelineStage::Unwrap(unwrap) = &stage.value {
                    Some(&unwrap.label)
                } else {
                    None
                }
            })
        {
            labels.remove(label);
        }
        groups.entry(labels).or_default().push(row);
    }
    if groups.is_empty() && op == RangeOp::Absent {
        return Ok(Value::Vector(vec![Point {
            labels: selector_equality_labels(log),
            value: 1.0,
        }]));
    }
    let seconds = range as f64 / 1_000_000_000.0;
    let effective_end = timestamp.saturating_sub(
        log.offset
            .as_ref()
            .map(|value| parse_duration_ns(&value.value))
            .transpose()?
            .unwrap_or(0),
    );
    let mut points = Vec::new();
    for (labels, mut group) in groups {
        group.sort_by_key(|row| row.timestamp_ns);
        let values = group
            .iter()
            .filter_map(|row| match op {
                RangeOp::Bytes | RangeOp::BytesRate => Some(row.line.len() as f64),
                RangeOp::Count | RangeOp::Rate | RangeOp::Absent => Some(1.0),
                _ => row.value,
            })
            .collect::<Vec<_>>();
        if values.is_empty() {
            continue;
        }
        let value = match op {
            RangeOp::Count | RangeOp::Bytes => values.iter().sum(),
            RangeOp::Rate | RangeOp::BytesRate => values.iter().sum::<f64>() / seconds,
            RangeOp::RateCounter => {
                let samples = group
                    .iter()
                    .filter_map(|row| row.value.map(|value| (row.timestamp_ns, value)))
                    .collect::<Vec<_>>();
                extrapolated_counter_rate(&samples, range, effective_end)
            }
            RangeOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
            RangeOp::Sum => values.iter().sum(),
            RangeOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
            RangeOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            RangeOp::Stddev => variance(&values).sqrt(),
            RangeOp::Stdvar => variance(&values),
            RangeOp::Quantile => quantile(parameter.unwrap_or(0.5), &values),
            RangeOp::First => values[0],
            RangeOp::Last => *values.last().expect("not empty"),
            RangeOp::Absent => continue,
        };
        points.push(Point { labels, value });
    }
    Ok(Value::Vector(points))
}

fn label_aggregation(
    field: &str,
    log: &LogExpr,
    grouping: Option<&Grouping>,
    rows: &[Row],
    timestamp: i64,
) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("label aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    let selected = eval_metric_log(log, rows, timestamp.saturating_sub(range), timestamp)?;
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeSet<String>> = BTreeMap::new();
    for row in selected {
        if let Some(value) = lookup(&row, field) {
            groups
                .entry(group_labels(&row.labels, grouping))
                .or_default()
                .insert(value.to_owned());
        }
    }
    Ok(Value::Vector(
        groups
            .into_iter()
            .map(|(labels, values)| Point {
                labels,
                value: values.len() as f64,
            })
            .collect(),
    ))
}

fn selector_equality_labels(log: &LogExpr) -> BTreeMap<String, String> {
    log.selector
        .value
        .matchers
        .iter()
        .filter(|matcher| matcher.value.op == MatchOp::Equal)
        .map(|matcher| (matcher.value.label.clone(), matcher.value.value.clone()))
        .collect()
}

fn group_labels(
    labels: &BTreeMap<String, String>,
    grouping: Option<&Grouping>,
) -> BTreeMap<String, String> {
    match grouping {
        None => labels.clone(),
        Some(grouping) if grouping.without => labels
            .iter()
            .filter(|(name, _)| !grouping.labels.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        Some(grouping) => labels
            .iter()
            .filter(|(name, _)| grouping.labels.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

fn vector_aggregation(
    op: VectorOp,
    parameter: Option<f64>,
    grouping: Option<&Grouping>,
    points: Vec<Point>,
) -> Vec<Point> {
    if matches!(
        op,
        VectorOp::Sort
            | VectorOp::SortDesc
            | VectorOp::TopK
            | VectorOp::BottomK
            | VectorOp::ApproxTopK
    ) {
        if matches!(op, VectorOp::Sort | VectorOp::SortDesc) {
            let mut points = points;
            if op == VectorOp::Sort {
                points.sort_by(point_value_order);
            } else {
                points.sort_by(|a, b| point_value_order(b, a));
            }
            return points;
        }
        let mut groups: BTreeMap<BTreeMap<String, String>, Vec<Point>> = BTreeMap::new();
        for point in points {
            groups
                .entry(group_labels(&point.labels, grouping))
                .or_default()
                .push(point);
        }
        let mut selected = Vec::new();
        for (_, mut points) in groups {
            if op == VectorOp::BottomK {
                points.sort_by(point_value_order);
            } else {
                points.sort_by(|a, b| point_value_order(b, a));
            }
            let count = parameter.unwrap_or(0.0).max(0.0) as usize;
            points.truncate(count);
            selected.extend(points);
        }
        return selected;
    }
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<f64>> = BTreeMap::new();
    for point in points {
        groups
            .entry(group_labels(&point.labels, grouping))
            .or_default()
            .push(point.value);
    }
    groups
        .into_iter()
        .map(|(labels, values)| {
            let value = match op {
                VectorOp::Sum => values.iter().sum(),
                VectorOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                VectorOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
                VectorOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                VectorOp::Count => values.len() as f64,
                VectorOp::Stddev => variance(&values).sqrt(),
                VectorOp::Stdvar => variance(&values),
                _ => unreachable!(),
            };
            Point { labels, value }
        })
        .collect()
}

fn point_value_order(a: &Point, b: &Point) -> Ordering {
    a.value
        .total_cmp(&b.value)
        .then_with(|| a.labels.cmp(&b.labels))
}

fn binary(
    lhs: Value,
    op: BinaryOp,
    modifier: Option<&BinaryModifier>,
    rhs: Value,
) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Scalar(lhs), Value::Scalar(rhs)) => {
            Ok(Value::Scalar(binary_number(lhs, op, rhs, modifier)?))
        }
        (Value::Vector(points), Value::Scalar(scalar)) => Ok(Value::Vector(
            points
                .into_iter()
                .filter_map(|mut point| {
                    binary_number(point.value, op, scalar, modifier)
                        .ok()
                        .filter(|value| !value.is_nan())
                        .map(|value| {
                            point.value = value;
                            point
                        })
                })
                .collect(),
        )),
        (Value::Scalar(scalar), Value::Vector(points)) => Ok(Value::Vector(
            points
                .into_iter()
                .filter_map(|mut point| {
                    binary_number(scalar, op, point.value, modifier)
                        .ok()
                        .filter(|value| !value.is_nan())
                        .map(|value| {
                            point.value = value;
                            point
                        })
                })
                .collect(),
        )),
        (Value::Vector(lhs), Value::Vector(rhs)) => {
            Ok(Value::Vector(binary_vectors(lhs, op, modifier, rhs)?))
        }
    }
}

fn binary_vectors(
    lhs: Vec<Point>,
    op: BinaryOp,
    modifier: Option<&BinaryModifier>,
    rhs: Vec<Point>,
) -> Result<Vec<Point>> {
    let matching = modifier.and_then(|modifier| modifier.matching.as_ref());
    if op == BinaryOp::Or {
        let mut result = lhs;
        let existing = result
            .iter()
            .map(|point| match_key(&point.labels, matching))
            .collect::<BTreeSet<_>>();
        result.extend(
            rhs.into_iter()
                .filter(|point| !existing.contains(&match_key(&point.labels, matching))),
        );
        return Ok(result);
    }
    let group_right = matching
        .and_then(|matching| matching.grouping.as_ref())
        .is_some_and(|grouping| !grouping.left);
    let group_left = matching
        .and_then(|matching| matching.grouping.as_ref())
        .is_some_and(|grouping| grouping.left);
    let mut right: BTreeMap<Vec<(String, String)>, Vec<Point>> = BTreeMap::new();
    for point in rhs {
        right
            .entry(match_key(&point.labels, matching))
            .or_default()
            .push(point);
    }
    if !group_right && right.values().any(|points| points.len() > 1) {
        return Err(Error::Query(
            "found duplicate series on the right hand-side; many-to-many matching not allowed: matching labels must be unique on one side".into(),
        ));
    }
    let mut result = Vec::new();
    let mut matched_left = BTreeSet::new();
    let mut output_labels = BTreeSet::new();
    for left in lhs {
        let key = match_key(&left.labels, matching);
        let Some(right_points) = right.get(&key) else {
            if op == BinaryOp::Unless {
                result.push(left);
            }
            continue;
        };
        if op == BinaryOp::And {
            result.push(left);
            continue;
        }
        if op == BinaryOp::Unless {
            continue;
        }
        if !group_left && !group_right && !matched_left.insert(key.clone()) {
            return Err(Error::Query(
                "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
                    .into(),
            ));
        }
        for right in right_points {
            let value = binary_number(left.value, op, right.value, modifier)?;
            if value.is_nan() {
                continue;
            }
            let mut output = if group_right {
                right.clone()
            } else {
                left.clone()
            };
            output.value = value;
            if group_right {
                include_labels(&mut output.labels, &left.labels, matching);
            } else {
                include_labels(&mut output.labels, &right.labels, matching);
            }
            let signature = output.labels.clone().into_iter().collect::<Vec<_>>();
            if !output_labels.insert(signature) {
                return Err(Error::Query(
                    "multiple matches for labels: grouping labels must ensure unique matches"
                        .into(),
                ));
            }
            result.push(output);
        }
    }
    Ok(result)
}

fn match_key(
    labels: &BTreeMap<String, String>,
    matching: Option<&VectorMatching>,
) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(name, _)| {
            matching.is_none_or(|matching| {
                if matching.on {
                    matching.labels.contains(name)
                } else {
                    !matching.labels.contains(name)
                }
            })
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn include_labels(
    left: &mut BTreeMap<String, String>,
    right: &BTreeMap<String, String>,
    matching: Option<&VectorMatching>,
) {
    let Some(side) = matching.and_then(|matching| matching.grouping.as_ref()) else {
        return;
    };
    for name in &side.include {
        if let Some(value) = right.get(name) {
            left.insert(name.clone(), value.clone());
        }
    }
}

fn binary_number(
    lhs: f64,
    op: BinaryOp,
    rhs: f64,
    modifier: Option<&BinaryModifier>,
) -> Result<f64> {
    let comparison = match op {
        BinaryOp::Equal => Some(lhs == rhs),
        BinaryOp::NotEqual => Some(lhs != rhs),
        BinaryOp::Greater => Some(lhs > rhs),
        BinaryOp::GreaterOrEqual => Some(lhs >= rhs),
        BinaryOp::Less => Some(lhs < rhs),
        BinaryOp::LessOrEqual => Some(lhs <= rhs),
        _ => None,
    };
    if let Some(comparison) = comparison {
        return Ok(if modifier.is_some_and(|modifier| modifier.return_bool) {
            f64::from(comparison)
        } else if comparison {
            lhs
        } else {
            f64::NAN
        });
    }
    Ok(match op {
        BinaryOp::Add => lhs + rhs,
        BinaryOp::Sub => lhs - rhs,
        BinaryOp::Mul => lhs * rhs,
        BinaryOp::Div => lhs / rhs,
        BinaryOp::Mod => lhs % rhs,
        BinaryOp::Pow => lhs.powf(rhs),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => {
            return Err(Error::Query("set operators require vectors".into()));
        }
        _ => unreachable!(),
    })
}

fn string_match(op: MatchOp, actual: &str, expected: &str) -> Result<bool> {
    Ok(match op {
        MatchOp::Equal => actual == expected,
        MatchOp::NotEqual => actual != expected,
        MatchOp::Regex => Regex::new(&format!("^(?:{expected})$"))?.is_match(actual),
        MatchOp::NotRegex => !Regex::new(&format!("^(?:{expected})$"))?.is_match(actual),
    })
}

fn variance(values: &[f64]) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64
}

fn quantile(quantile: f64, values: &[f64]) -> f64 {
    if quantile.is_nan() || quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    values[lower] + (values[upper] - values[lower]) * rank.fract()
}

fn extrapolated_counter_rate(samples: &[(i64, f64)], range_ns: i64, range_end: i64) -> f64 {
    if samples.len() < 2 || range_ns <= 0 {
        return 0.0;
    }
    let range_start = range_end.saturating_sub(range_ns);
    let mut result = samples.last().expect("at least two").1 - samples[0].1;
    let mut previous = samples[0].1;
    for (_, value) in &samples[1..] {
        if *value < previous {
            result += previous;
        }
        previous = *value;
    }
    let sampled_interval =
        (samples.last().expect("at least two").0 - samples[0].0) as f64 / 1_000_000_000.0;
    if sampled_interval <= 0.0 {
        return 0.0;
    }
    let average = sampled_interval / (samples.len() - 1) as f64;
    let threshold = average * 1.1;
    let mut to_start = (samples[0].0 - range_start) as f64 / 1_000_000_000.0;
    let mut to_end = (range_end - samples.last().expect("at least two").0) as f64 / 1_000_000_000.0;
    if to_start >= threshold {
        to_start = average / 2.0;
    }
    if result > 0.0 && samples[0].1 >= 0.0 {
        let to_zero = sampled_interval * (samples[0].1 / result);
        if to_zero < to_start {
            to_start = to_zero;
        }
    }
    if to_end >= threshold {
        to_end = average / 2.0;
    }
    result * ((sampled_interval + to_start + to_end) / sampled_interval)
        / (range_ns as f64 / 1_000_000_000.0)
}

fn parse_duration_ns(source: &str) -> Result<i64> {
    let token =
        Regex::new(r"([0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|s|m|h|d|w)").expect("static duration regex");
    let mut total = 0.0;
    let mut consumed = 0;
    for captures in token.captures_iter(source) {
        let matched = captures.get(0).expect("full capture");
        if matched.start() != consumed {
            return Err(Error::Query(format!("invalid duration {source:?}")));
        }
        consumed = matched.end();
        let value: f64 = captures[1]
            .parse()
            .map_err(|_| Error::Query(format!("invalid duration {source:?}")))?;
        let unit = match &captures[2] {
            "ns" => 1.0,
            "us" | "µs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60_000_000_000.0,
            "h" => 3_600_000_000_000.0,
            "d" => 86_400_000_000_000.0,
            "w" => 604_800_000_000_000.0,
            _ => unreachable!(),
        };
        total += value * unit;
    }
    if consumed != source.len() || consumed == 0 || total > i64::MAX as f64 {
        return Err(Error::Query(format!("invalid duration {source:?}")));
    }
    Ok(total as i64)
}

fn parse_bytes(source: &str) -> Result<f64> {
    let captures = Regex::new(r"(?i)^([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b)?$")
        .expect("static bytes regex")
        .captures(source)
        .ok_or_else(|| Error::Query(format!("invalid byte size {source:?}")))?;
    let value: f64 = captures[1]
        .parse()
        .map_err(|_| Error::Query(format!("invalid byte size {source:?}")))?;
    let suffix = captures.get(2).map_or("", |value| value.as_str());
    let power = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "kb" | "kib" => 1,
        "mb" | "mib" => 2,
        "gb" | "gib" => 3,
        "tb" | "tib" => 4,
        "pb" | "pib" => 5,
        "eb" | "eib" => 6,
        _ => return Err(Error::Query(format!("invalid byte size {source:?}"))),
    };
    Ok(value
        * if suffix.to_ascii_lowercase().contains('i') {
            1024f64.powi(power)
        } else {
            1000f64.powi(power)
        })
}

fn ip_matches(candidate: &str, expression: &str) -> bool {
    if let Some((network, prefix)) = expression.split_once('/') {
        let (Ok(candidate), Ok(network), Ok(prefix)) = (
            candidate.parse::<IpAddr>(),
            network.parse::<IpAddr>(),
            prefix.parse::<u8>(),
        ) else {
            return false;
        };
        match (candidate, network) {
            (IpAddr::V4(candidate), IpAddr::V4(network)) if prefix <= 32 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - prefix)
                };
                u32::from(candidate) & mask == u32::from(network) & mask
            }
            (IpAddr::V6(candidate), IpAddr::V6(network)) if prefix <= 128 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - prefix)
                };
                u128::from(candidate) & mask == u128::from(network) & mask
            }
            _ => false,
        }
    } else {
        candidate.parse::<IpAddr>().ok() == expression.parse::<IpAddr>().ok()
            && candidate.parse::<IpAddr>().is_ok()
    }
}

fn ansi_regex() -> Regex {
    Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").expect("static ANSI regex")
}
