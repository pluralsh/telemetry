//! In-process LogQL execution over [`LogDb`](crate::LogDb).

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use futures::{StreamExt, TryStreamExt, stream};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::analyzer::DEFAULT_ANALYZER;
use crate::db::{PageBudget, ScanTargets, StreamFilter};
use crate::logql::{
    self, BinaryModifier, BinaryOp, ComparisonOp, Conversion, Expr, FilterValue, FormatAssignment,
    FormatValue, Grouping, IpPattern, LabelFilterExpr, LineFilter, LineFilterOp, LineFilterTerm,
    LogExpr, MatchOp, ParserExpression, ParserStage, PipelineStage, Query, RangeOp, Unwrap,
    VectorMatching, VectorOp,
};
use crate::search::{SCORE_METADATA_FIELD, query_terms, source_matches};
use crate::{Error, Label, Labels, LogDb, LogEntry, Namespace, Result};

mod label_regex;
mod metric;
mod parallel;
mod pipeline;
mod regex_cache;
pub(crate) mod template;
mod units;

use label_regex::*;
use metric::*;
use parallel::ParallelSink;
use pipeline::*;
use regex_cache::*;
use units::*;

/// Ordering of log entries returned by a streams query.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    #[default]
    Forward,
    Backward,
}

pub const DEFAULT_INSTANT_LOG_LOOKBACK_NS: i64 = 30_000_000_000;

const ERROR_LABEL: &str = "__error__";
const ERROR_DETAILS_LABEL: &str = "__error_details__";
const PRESERVE_ERROR_LABEL: &str = "__preserve_error__";

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

    /// Rounds a metric range query out to step multiples (start down, end
    /// up), as Loki's query frontend does before splitting, so every step
    /// lands on a multiple of the step whatever bounds were requested. The
    /// engine itself evaluates the bounds it is given, like Loki's.
    pub fn frontend_step_aligned(mut self) -> Self {
        let Some(step) = self.step_ns else {
            return self;
        };
        let metric =
            logql::parse(&self.query).is_ok_and(|query| !matches!(query.value, Expr::Log(_)));
        if !metric {
            return self;
        }
        self.start_ns -= self.start_ns.rem_euclid(step);
        if let remainder @ 1.. = self.end_ns.rem_euclid(step) {
            self.end_ns = self.end_ns.saturating_add(step - remainder);
        }
        self
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
            max_concurrency: 16,
            max_in_flight_bytes: 128 * 1024 * 1024,
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
    /// Shared across a stream's rows; stages that change labels copy on write.
    labels: LabelMap,
    metadata: BTreeMap<String, String>,
    value: Option<f64>,
    /// Loki's hash of the stored stream's labels, which stages never change.
    /// Loki merges samples sharing a timestamp in ascending hash order.
    stream: u64,
    /// Hash of the stored line and structured metadata, set for metric rows
    /// whose stages rewrite `line` or that carry metadata.
    source: Option<u64>,
}

#[derive(Clone)]
struct Point {
    /// Shared with the range series it came from, since a series yields a
    /// point at every step its window covers.
    labels: LabelMap,
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
        let plan = ScanPlan::new(request, &query, &options)?;
        let rows = load_rows(
            self,
            namespace,
            &plan,
            &PageBudget::new(options.max_pages),
            None,
        )
        .await?;
        plan.evaluate(vec![rows], options)
    }
}

/// Storage-facing decisions for one query, shared by every database read.
pub(crate) struct ScanPlan<'a> {
    request: &'a QueryRequest,
    query: &'a Query,
    pub(crate) scan_start: i64,
    pub(crate) streams: StreamFilter,
    indexed_terms: Option<Vec<String>>,
    limit: usize,
    direction: Direction,
    /// Owned copies for pipeline workers, which outlive any borrow.
    shared: Option<Arc<(Query, QueryRequest)>>,
}

impl<'a> ScanPlan<'a> {
    pub(crate) fn new(
        request: &'a QueryRequest,
        query: &'a Query,
        options: &QueryOptions,
    ) -> Result<Self> {
        Ok(Self {
            shared: parallel::enabled().then(|| Arc::new((query.clone(), request.clone()))),
            request,
            query,
            scan_start: request.start_ns.saturating_sub(max_lookback(query)?),
            streams: stream_filter(query)?,
            indexed_terms: indexed_match_terms(query),
            limit: options.limit,
            direction: options.direction,
        })
    }

    /// Unindexed log queries return the first `limit` rows by timestamp, so
    /// reads stop once that many rows survive the pipeline and loaded rows
    /// have already been through it.
    fn early_stop_log(&self) -> Option<&'a LogExpr> {
        match &self.query.value {
            Expr::Log(log) if self.indexed_terms.is_none() => Some(log),
            _ => None,
        }
    }

    fn sink(&self) -> Result<PipelineSink<'a>> {
        PipelineSink::new(self.query, self.request)
    }

    fn evaluate(&self, sinks: Vec<PipelineSink<'a>>, options: QueryOptions) -> Result<QueryResult> {
        let mut sinks = sinks.into_iter();
        let mut rows = sinks.next().map_or_else(|| self.sink(), Ok)?;
        for other in sinks {
            rows.extend(other);
        }
        evaluate(
            self.query,
            rows.finish(),
            self.request,
            options,
            self.indexed_terms.is_some(),
        )
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
    let plan = ScanPlan::new(request, &query, &options)?;
    let byte_budget = options.max_in_flight_bytes.min(u32::MAX as usize);
    let permits = Arc::new(Semaphore::new(byte_budget));
    // Early-stopping queries may read far fewer pages than an estimate would
    // count, so they skip the metadata pre-walk and share one budget charged
    // per page actually read.
    if plan.early_stop_log().is_some() {
        let shared_budget = PageBudget::new(options.max_pages);
        let rows = stream::iter(databases.into_iter().map(|database| {
            let global_permits = Arc::clone(&global_permits);
            let (plan, shared_budget) = (&plan, &shared_budget);
            async move {
                let _global_permit = global_permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Query("global query scheduler closed".into()))?;
                load_rows(&database, namespace, plan, shared_budget, None).await
            }
        }))
        .buffered(options.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
        return plan.evaluate(rows, options);
    }
    let targets = stream::iter(databases.iter().cloned())
        .map(|database| {
            let global_permits = Arc::clone(&global_permits);
            let plan = &plan;
            async move {
                let _permit = global_permits
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Query("global query scheduler closed".into()))?;
                database
                    .scan_targets(namespace, plan.scan_start, request.end_ns, &plan.streams)
                    .await
            }
        })
        .buffered(options.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let estimates = targets
        .iter()
        .map(|targets| targets.estimate())
        .collect::<Vec<_>>();
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
    let stored = stream::iter(databases.into_iter().zip(targets).zip(estimates).map(
        |((database, targets), estimate)| {
            let permits = Arc::clone(&permits);
            let global_permits = Arc::clone(&global_permits);
            let plan = &plan;
            async move {
                let weight =
                    u32::try_from(estimate.compressed_bytes.max(1).min(byte_budget as u64))
                        .unwrap_or(u32::MAX);
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
                    plan,
                    &PageBudget::new(estimate.pages),
                    Some(targets),
                )
                .await
            }
        },
    ))
    .buffered(options.max_concurrency)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    plan.evaluate(stored, options)
}

/// Reads one database, running the query's pipelines as pages decode.
async fn load_rows<'a>(
    database: &LogDb,
    namespace: &Namespace,
    plan: &ScanPlan<'a>,
    budget: &PageBudget,
    targets: Option<ScanTargets>,
) -> Result<PipelineSink<'a>> {
    let (start, end) = (plan.scan_start, plan.request.end_ns);
    let mut converter = RowConverter::default();
    let sink = plan.sink()?;
    if plan.early_stop_log().is_some() {
        let mut sink = ParallelSink::for_logs(sink, plan.shared.clone());
        database
            .read_segments(
                namespace,
                (start, end),
                &plan.streams,
                budget,
                plan.direction == Direction::Backward,
                |segment| {
                    for row in segment {
                        sink.push(converter.convert(row, None))?;
                    }
                    Ok(
                        if sink.kept_logs(plan.direction, plan.limit)? >= plan.limit {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        },
                    )
                },
            )
            .await?;
        return sink.finish_logs(plan.direction, plan.limit).await;
    }
    let targets = match targets {
        Some(targets) => targets,
        None => {
            database
                .scan_targets(namespace, start, end, &plan.streams)
                .await?
        }
    };
    let mut sink = ParallelSink::new(sink, plan.shared.clone());
    if let Some(terms) = &plan.indexed_terms {
        let index_top_k = direct_index_top_k(plan.query, plan.limit);
        if let Some(rows) = database
            .read_match_bounded(namespace, &targets, (terms, index_top_k), budget.limit())
            .await?
        {
            for (row, score) in rows {
                sink.push(converter.convert(row, Some(score)))?;
            }
            return sink.finish().await;
        }
    }
    database
        .read_bounded_with(namespace, targets, budget, |chunk| {
            for row in chunk {
                sink.push(converter.convert(row, None))?;
            }
            Ok(())
        })
        .await?;
    sink.finish().await
}

/// Converts decoded rows, building each stream's label map once.
#[derive(Default)]
struct RowConverter {
    /// Keyed by `Arc` address; holding the `Arc` keeps the address from being
    /// reused by another stream's labels.
    maps: HashMap<usize, (Arc<crate::Labels>, LabelMap, u64)>,
}

/// A row's or point's labels; shared, since many rows and steps carry the
/// same stream's labels.
type LabelMap = Arc<BTreeMap<String, String>>;

impl RowConverter {
    fn convert(&mut self, row: crate::LogRow, score: Option<f32>) -> Row {
        let (_, labels, stream) = self
            .maps
            .entry(Arc::as_ptr(&row.labels) as usize)
            .or_insert_with(|| {
                let map: BTreeMap<String, String> = row
                    .labels
                    .iter()
                    .map(|label| (label.name.clone(), label.value.clone()))
                    .collect();
                let stream = stream_hash(&map);
                (row.labels.clone(), Arc::new(map), stream)
            });
        to_row(row.entry, labels.clone(), *stream, score)
    }
}

/// Prometheus' `labels.Hash`, which Loki uses as a stream's hash: XXH64 over
/// each name and value, in name order, each followed by `0xff`.
fn stream_hash(labels: &BTreeMap<String, String>) -> u64 {
    let mut bytes = Vec::new();
    for (name, value) in labels {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);
    }
    twox_hash::XxHash64::oneshot(0, &bytes)
}

fn hash_of(value: &impl std::hash::Hash) -> u64 {
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
}

fn to_row(entry: crate::LogEntry, labels: LabelMap, stream: u64, score: Option<f32>) -> Row {
    let mut metadata = entry
        .structured_metadata
        .iter()
        .map(|field| (field.name.clone(), field.value.clone()))
        .collect::<BTreeMap<_, _>>();
    if let Some(score) = score {
        metadata.insert(SCORE_METADATA_FIELD.to_owned(), score.to_string());
    }
    Row {
        timestamp_ns: entry.timestamp_ns,
        line: entry.line,
        labels,
        metadata,
        value: None,
        stream,
        source: None,
    }
}

/// Loki's merge iterators drop an entry when an earlier one from the same
/// stored stream has the same timestamp and identical line: the formatted
/// line for log queries, the stored one for metric samples. Entries that
/// differ only in structured metadata are distinct writes and both kept.
/// `rows` must be in timestamp order; only rows sharing a timestamp are
/// compared.
fn dedupe_rows(rows: &mut Vec<Row>) {
    let mut duplicates: Option<Vec<bool>> = None;
    let mut order = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let timestamp = rows[start].timestamp_ns;
        let end = start
            + rows[start..]
                .iter()
                .take_while(|row| row.timestamp_ns == timestamp)
                .count();
        if end - start > 1 {
            // Sorting the run by identity makes duplicates adjacent, keeping
            // the earliest of each, so a burst sharing one timestamp costs
            // O(k log k) rather than pairwise comparison.
            order.clear();
            order.extend(start..end);
            order.sort_by(|&a, &b| dedupe_order(&rows[a], &rows[b]).then(a.cmp(&b)));
            for pair in order.windows(2) {
                if dedupe_order(&rows[pair[0]], &rows[pair[1]]).is_eq() {
                    duplicates.get_or_insert_with(|| vec![false; rows.len()])[pair[1]] = true;
                }
            }
        }
        start = end;
    }
    if let Some(duplicates) = duplicates {
        let mut index = 0;
        rows.retain(|_| {
            index += 1;
            !duplicates[index - 1]
        });
    }
}

/// Orders rows sharing a timestamp so that rows `dedupe_rows` treats as the
/// same entry compare equal: same stream, then the same stored line (by hash
/// when stages rewrote it) and the same structured metadata.
fn dedupe_order(a: &Row, b: &Row) -> Ordering {
    a.stream
        .cmp(&b.stream)
        .then_with(|| a.source.cmp(&b.source))
        .then_with(|| match (a.source, b.source) {
            (None, None) => a.line.cmp(&b.line),
            _ => Ordering::Equal,
        })
        .then_with(|| a.metadata.cmp(&b.metadata))
}

fn sort_logs(rows: &mut Vec<Row>, direction: Direction, indexed: bool) {
    rows.sort_by(row_order);
    dedupe_rows(rows);
    if indexed {
        rows.sort_by(|left, right| {
            match_score(right)
                .total_cmp(&match_score(left))
                .then_with(|| row_order(left, right))
        });
    } else if direction == Direction::Backward {
        rows.reverse();
    }
}

fn finish_logs(mut rows: Vec<Row>, options: &QueryOptions, indexed: bool) -> Result<QueryResult> {
    sort_logs(&mut rows, options.direction, indexed);
    rows.truncate(options.limit);
    streams(rows)
}

/// Instant vectors keep the order `sort`/`sort_desc` gave them.
fn uses_sort(query: &Query) -> bool {
    uses_vector_op(query, VectorOp::Sort) || uses_vector_op(query, VectorOp::SortDesc)
}

fn uses_vector_op(query: &Query, wanted: VectorOp) -> bool {
    match &query.value {
        Expr::VectorAggregation { op, expr, .. } => *op == wanted || uses_vector_op(expr, wanted),
        Expr::Vector(expr) | Expr::LabelReplace { expr, .. } => uses_vector_op(expr, wanted),
        Expr::Binary { lhs, rhs, .. } => uses_vector_op(lhs, wanted) || uses_vector_op(rhs, wanted),
        _ => false,
    }
}

fn evaluate(
    query: &Query,
    mut rows: MetricRows,
    request: &QueryRequest,
    options: QueryOptions,
    indexed: bool,
) -> Result<QueryResult> {
    if let Expr::Log(log) = &query.value {
        return finish_logs(rows.take(log), &options, indexed);
    }

    if let Some(step) = request.step_ns {
        if uses_vector_op(query, VectorOp::ApproxTopK) {
            return Err(Error::Query(
                "approx_topk error: count min sketches are only supported on instant queries"
                    .into(),
            ));
        }
        let mut matrix = Matrix::default();
        let mut timestamp = request.start_ns;
        loop {
            match eval_expr(query, &rows, timestamp)? {
                Value::Scalar(value) => matrix.push(
                    vec![Point {
                        labels: Arc::default(),
                        value,
                    }],
                    timestamp,
                ),
                Value::Vector(points) => matrix.push(points, timestamp),
            }
            match timestamp.checked_add(step) {
                Some(next) if next <= request.end_ns => timestamp = next,
                _ => break,
            }
        }
        Ok(QueryResult::Matrix(matrix.finish()))
    } else {
        match eval_expr(query, &rows, request.end_ns)? {
            Value::Scalar(value) => Ok(QueryResult::Scalar(Sample {
                timestamp_ns: request.end_ns,
                value,
            })),
            Value::Vector(mut points) => {
                if !uses_sort(query) {
                    points.sort_by(|a, b| a.labels.cmp(&b.labels));
                }
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

/// A range query's series, in label order once finished.
#[derive(Default)]
struct Matrix {
    slots: BTreeMap<LabelMap, usize>,
    series: Vec<MatrixSeries>,
    /// The previous step's points by label allocation. A series yields the
    /// same allocation at every step, so most points skip the ordered
    /// lookup; holding the `Arc`s keeps their addresses from being reused,
    /// and a shared `Arc` is never changed in place.
    previous: HashMap<*const BTreeMap<String, String>, (LabelMap, usize)>,
}

impl Matrix {
    fn push(&mut self, points: Vec<Point>, timestamp_ns: i64) {
        let mut current = HashMap::with_capacity(points.len());
        for point in points {
            let address = Arc::as_ptr(&point.labels);
            let slot = match self.previous.get(&address) {
                Some((_, slot)) => *slot,
                None => match self.slots.get(&*point.labels) {
                    Some(slot) => *slot,
                    None => {
                        self.series.push(MatrixSeries {
                            labels: map_labels(&point.labels),
                            samples: Vec::new(),
                        });
                        self.slots
                            .insert(Arc::clone(&point.labels), self.series.len() - 1);
                        self.series.len() - 1
                    }
                },
            };
            self.series[slot].samples.push(Sample {
                timestamp_ns,
                value: point.value,
            });
            current.insert(address, (point.labels, slot));
        }
        self.previous = current;
    }

    fn finish(self) -> Vec<MatrixSeries> {
        let mut series = self.series.into_iter().map(Some).collect::<Vec<_>>();
        self.slots
            .into_values()
            .filter_map(|slot| series[slot].take())
            .collect()
    }
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
    let mut grouped: BTreeMap<LabelMap, Vec<LogEntry>> = BTreeMap::new();
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

/// Postings narrow streams by the exact matchers every selector shares; the
/// full selectors then prune the remaining streams by label before page I/O.
fn stream_filter(query: &Query) -> Result<StreamFilter> {
    let mut selectors = Vec::new();
    collect_log_exprs(query, &mut selectors);
    StreamFilter::new(
        common_exact_matchers(query),
        selectors
            .into_iter()
            .map(|log| log.selector.value.matchers.as_slice()),
    )
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
            // `name=""` also matches streams without the label, which have
            // no posting to intersect.
            .filter(|matcher| matcher.value.op == MatchOp::Equal && !matcher.value.value.is_empty())
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
                    let terms = query_terms(&DEFAULT_ANALYZER, value);
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

/// Runs every log expression's pipeline over rows as they are read, so only
/// rows some pipeline keeps are held.
struct PipelineSink<'a> {
    query: &'a Query,
    logs: Vec<&'a LogExpr>,
    offsets: Vec<i64>,
    /// Log queries keep rows in `[start, end)` of the request; metric
    /// queries keep every row read and promote metadata to labels.
    log_window: Option<(i64, i64)>,
    outputs: Vec<Vec<Row>>,
    /// Per log expression: whether a stage replaces the stored line, which
    /// metric deduplication then has to remember.
    rewrites_line: Vec<bool>,
    /// Per log expression: whether its samples keep no labels and no stage
    /// reads one, so parsers can be skipped as Loki does.
    skip_parsers: Vec<bool>,
    /// Per log expression, per stage: whether it runs.
    runs: Vec<Vec<bool>>,
}

impl<'a> PipelineSink<'a> {
    fn new(query: &'a Query, request: &QueryRequest) -> Result<Self> {
        let mut logs = Vec::new();
        collect_log_exprs(query, &mut logs);
        let mut seen = HashSet::new();
        logs.retain(|log| seen.insert(*log as *const LogExpr));
        let offsets = logs
            .iter()
            .map(|log| log_offset(log))
            .collect::<Result<Vec<_>>>()?;
        let mut aggregations = Vec::new();
        collect_range_aggregations(query, &mut aggregations);
        let runs = logs
            .iter()
            .map(|log| {
                let sampled = aggregations
                    .iter()
                    .find_map(|(op, sampled, _)| std::ptr::eq(*sampled, *log).then_some(*op));
                running_stages(log, sampled)
            })
            .collect::<Vec<_>>();
        let running = |index: usize| {
            logs[index]
                .stages
                .iter()
                .zip(&runs[index])
                .filter(|(_, runs)| **runs)
                .map(|(stage, _)| &stage.value)
        };
        let rewrites_line = (0..logs.len())
            .map(|index| {
                running(index).any(|stage| {
                    matches!(
                        stage,
                        PipelineStage::LineFormat(_)
                            | PipelineStage::Decolorize
                            | PipelineStage::Parser(ParserStage::Unpack)
                    )
                })
            })
            .collect();
        let mut label_free = HashSet::new();
        collect_label_free_logs(query, &mut label_free);
        let skip_parsers = (0..logs.len())
            .map(|index| {
                label_free.contains(&(logs[index] as *const LogExpr))
                    && !running(index).any(reads_labels)
            })
            .collect();
        Ok(Self {
            runs,
            outputs: vec![Vec::new(); logs.len()],
            log_window: matches!(query.value, Expr::Log(_))
                .then_some((request.start_ns, request.end_ns)),
            query,
            logs,
            offsets,
            rewrites_line,
            skip_parsers,
        })
    }

    fn push(&mut self, source: Row) -> Result<()> {
        let last = self.logs.len().saturating_sub(1);
        let mut source = Some(source);
        for (index, log) in self.logs.iter().enumerate() {
            let candidate = source.as_ref().expect("the source moves on the last pass");
            if let Some((start, end)) = self.log_window {
                let offset = self.offsets[index];
                let timestamp = candidate.timestamp_ns;
                if timestamp < start.saturating_sub(offset)
                    || timestamp >= end.saturating_sub(offset)
                {
                    continue;
                }
            }
            if !selector_matches(log, &candidate.labels)? {
                continue;
            }
            let mut row = if index == last {
                source.take().expect("the source moves on the last pass")
            } else {
                candidate.clone()
            };
            if self.log_window.is_none() {
                if self.rewrites_line[index] || !row.metadata.is_empty() {
                    row.source = Some(hash_of(&(&row.line, &row.metadata)));
                }
                promote_metadata(&mut row);
            }
            if apply_stages(log, &self.runs[index], &mut row, self.skip_parsers[index])? {
                self.outputs[index].push(row);
            }
        }
        Ok(())
    }

    /// Rows kept for a log query.
    fn kept(&self) -> usize {
        self.outputs.first().map_or(0, Vec::len)
    }

    /// Keeps the first `limit` log rows in `direction`.
    fn truncate_logs(&mut self, direction: Direction, limit: usize) {
        if let Some(rows) = self.outputs.first_mut() {
            sort_logs(rows, direction, false);
            rows.truncate(limit);
        }
    }

    fn extend(&mut self, other: Self) {
        self.extend_outputs(other.outputs);
    }

    /// Appends rows from a sink over the same query, whose log expressions
    /// are collected in the same order.
    fn extend_outputs(&mut self, outputs: Vec<Vec<Row>>) {
        for (output, rows) in self.outputs.iter_mut().zip(outputs) {
            output.extend(rows);
        }
    }

    fn finish(self) -> MetricRows {
        let metric = self.log_window.is_none();
        let mut by_expr: HashMap<usize, Vec<Row>> = self
            .logs
            .into_iter()
            .zip(self.outputs)
            .map(|(log, mut rows)| {
                if metric {
                    // Same-timestamp samples in Loki's merge order, which
                    // `first_over_time` and `last_over_time` pick between.
                    rows.sort_by_key(|row| (row.timestamp_ns, row.stream));
                    dedupe_rows(&mut rows);
                }
                (log as *const LogExpr as usize, rows)
            })
            .collect();
        let mut aggregations = Vec::new();
        collect_range_aggregations(self.query, &mut aggregations);
        let ranges = aggregations
            .into_iter()
            .filter_map(|(op, log, grouping)| {
                // Each log expression belongs to one aggregation, so its rows
                // are no longer needed once reduced.
                let key = log as *const LogExpr as usize;
                let rows = by_expr.remove(&key)?;
                Some((key, RangeSeries::new(op, log, grouping, rows)))
            })
            .collect();
        MetricRows {
            by_expr,
            ranges,
            label_keys: RefCell::default(),
            relabels: RefCell::default(),
        }
    }
}

static BY_NOTHING: Grouping = Grouping {
    without: false,
    labels: Vec::new(),
};

fn collect_range_aggregations<'a>(
    query: &'a Query,
    result: &mut Vec<(RangeOp, &'a LogExpr, Option<&'a Grouping>)>,
) {
    match &query.value {
        Expr::RangeAggregation {
            op, expr, grouping, ..
        } => result.push((*op, expr, grouping.as_ref())),
        Expr::Vector(expr) | Expr::LabelReplace { expr, .. } => {
            collect_range_aggregations(expr, result);
        }
        Expr::VectorAggregation {
            op: VectorOp::Sum,
            expr,
            grouping,
            ..
        } if matches!(
            &expr.value,
            Expr::RangeAggregation {
                op: RangeOp::Bytes
                    | RangeOp::BytesRate
                    | RangeOp::Sum
                    | RangeOp::Rate
                    | RangeOp::Count,
                grouping: None,
                ..
            }
        ) =>
        {
            // Loki's `canInjectVectorGrouping`: summing these range ops is
            // linear, so grouping samples by the outer labels as they are
            // extracted gives the same sums without a series per label set
            // the parsers produced.
            let Expr::RangeAggregation { op, expr, .. } = &expr.value else {
                unreachable!("matched above")
            };
            result.push((*op, expr, Some(grouping.as_ref().unwrap_or(&BY_NOTHING))));
        }
        Expr::VectorAggregation { expr, .. } => collect_range_aggregations(expr, result),
        Expr::Binary { lhs, rhs, .. } => {
            collect_range_aggregations(lhs, result);
            collect_range_aggregations(rhs, result);
        }
        Expr::Log(_) | Expr::LabelAggregation { .. } | Expr::Number(_) | Expr::String(_) => {}
    }
}

/// Pipeline output of every log expression, sorted by timestamp for metric
/// queries. Stages never change timestamps, so each step's range window is a
/// binary-searched slice rather than a fresh pipeline pass.
struct MetricRows {
    by_expr: HashMap<usize, Vec<Row>>,
    ranges: HashMap<usize, RangeSeries>,
    /// Per binary or aggregation expression, by address, its operands'
    /// series keys, which steps share.
    label_keys: RefCell<HashMap<usize, LabelKeys>>,
    /// Per `label_replace`, by address, its rewrites, which steps share.
    relabels: RefCell<HashMap<usize, Relabel>>,
}

impl MetricRows {
    fn range_series(&self, log: &LogExpr) -> Result<&RangeSeries> {
        self.ranges
            .get(&(log as *const LogExpr as usize))
            .ok_or_else(|| Error::Query("range aggregation was not prepared".into()))
    }

    fn rows(&self, log: &LogExpr) -> Result<&[Row]> {
        self.by_expr
            .get(&(log as *const LogExpr as usize))
            .map(Vec::as_slice)
            .ok_or_else(|| Error::Query("metric expression was not prepared".into()))
    }

    fn take(&mut self, log: &LogExpr) -> Vec<Row> {
        self.by_expr
            .remove(&(log as *const LogExpr as usize))
            .unwrap_or_default()
    }

    /// Rows of `log` in the LogQL range window `(start, end]`, shifted by
    /// the expression's offset.
    fn window(&self, log: &LogExpr, start: i64, end: i64) -> Result<&[Row]> {
        let offset = log_offset(log)?;
        let (start, end) = (start.saturating_sub(offset), end.saturating_sub(offset));
        let rows = self.rows(log)?;
        let low = rows.partition_point(|row| row.timestamp_ns <= start);
        let high = rows.partition_point(|row| row.timestamp_ns <= end).max(low);
        Ok(&rows[low..high])
    }
}

fn log_offset(log: &LogExpr) -> Result<i64> {
    Ok(log
        .offset
        .as_ref()
        .map(|value| parse_duration_ns(&value.value))
        .transpose()?
        .unwrap_or(0))
}

/// Log expressions whose samples Loki extracts with no labels at all:
/// `absent_over_time`, and line-count or byte range aggregations directly
/// under a `sum` without grouping, which Loki pushes into the extractor.
fn collect_label_free_logs(query: &Query, result: &mut HashSet<*const LogExpr>) {
    match &query.value {
        Expr::RangeAggregation {
            op: RangeOp::Absent,
            expr,
            ..
        } => {
            result.insert(expr);
        }
        Expr::VectorAggregation {
            op: VectorOp::Sum,
            expr,
            grouping,
            ..
        } if grouping
            .as_ref()
            .is_none_or(|grouping| !grouping.without && grouping.labels.is_empty()) =>
        {
            if let Expr::RangeAggregation {
                op: RangeOp::Count | RangeOp::Rate | RangeOp::Bytes | RangeOp::BytesRate,
                expr: log,
                grouping: None,
                ..
            } = &expr.value
            {
                result.insert(log);
            } else {
                collect_label_free_logs(expr, result);
            }
        }
        Expr::Vector(expr)
        | Expr::LabelReplace { expr, .. }
        | Expr::VectorAggregation { expr, .. } => collect_label_free_logs(expr, result),
        Expr::Binary { lhs, rhs, .. } => {
            collect_label_free_logs(lhs, result);
            collect_label_free_logs(rhs, result);
        }
        Expr::Log(_)
        | Expr::RangeAggregation { .. }
        | Expr::LabelAggregation { .. }
        | Expr::Number(_)
        | Expr::String(_) => {}
    }
}

/// Loki's `removeLineformat`: samples other than byte counts never see the
/// line, so a `line_format` runs only when a later parser or line filter
/// reads what it wrote. `sampled` is the range aggregation over `log`.
fn running_stages(log: &LogExpr, sampled: Option<RangeOp>) -> Vec<bool> {
    let removes = sampled.is_some_and(|op| !matches!(op, RangeOp::Bytes | RangeOp::BytesRate));
    (0..log.stages.len())
        .map(|index| {
            !removes
                || !matches!(log.stages[index].value, PipelineStage::LineFormat(_))
                || log.stages[index + 1..].iter().any(|stage| {
                    matches!(
                        stage.value,
                        PipelineStage::Parser(_)
                            | PipelineStage::LineFilter(_)
                            | PipelineStage::Match(_)
                    )
                })
        })
        .collect()
}

/// Whether a stage reads labels, which makes Loki run its parsers even when
/// the samples keep no labels.
fn reads_labels(stage: &PipelineStage) -> bool {
    matches!(
        stage,
        PipelineStage::LabelFilter(_)
            | PipelineStage::LineFormat(_)
            | PipelineStage::LabelFormat(_)
            | PipelineStage::Unwrap(_)
    )
}

/// `skip_parsers` mirrors Loki's parser hints: when nothing downstream needs
/// a label, parsers leave the line alone, so they never flag errors either.
fn apply_stages(log: &LogExpr, runs: &[bool], row: &mut Row, skip_parsers: bool) -> Result<bool> {
    for (stage, _) in log.stages.iter().zip(runs).filter(|(_, runs)| **runs) {
        if skip_parsers && matches!(stage.value, PipelineStage::Parser(_)) {
            continue;
        }
        let parser = matches!(stage.value, PipelineStage::Parser(_))
            && !row.labels.contains_key(ERROR_LABEL);
        if !apply_stage(row, &stage.value)? {
            return Ok(false);
        }
        // Loki flags only parser errors, when a filter asks about them, at
        // parse time so later stages can still `drop` the flag.
        if parser && row.labels.contains_key(ERROR_LABEL) && filters_on_error(log) {
            Arc::make_mut(&mut row.labels).insert(PRESERVE_ERROR_LABEL.into(), "true".into());
        }
    }
    Ok(true)
}

fn selector_matches(log: &LogExpr, labels: &BTreeMap<String, String>) -> Result<bool> {
    for matcher in &log.selector.value.matchers {
        let actual = labels.get(&matcher.value.label).map_or("", String::as_str);
        let matched = match matcher.value.op {
            MatchOp::Equal => actual == matcher.value.value,
            MatchOp::NotEqual => actual != matcher.value.value,
            MatchOp::Regex | MatchOp::NotRegex => {
                string_match(matcher.value.op, actual, &matcher.value.value)?
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}
