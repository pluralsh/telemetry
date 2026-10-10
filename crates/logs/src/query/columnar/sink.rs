//! Runs a query's pipelines over batches and reduces what they keep to
//! what evaluation needs: range samples, label samples or log rows.

use std::hash::{BuildHasher, Hasher};

use super::super::*;
use super::batch::{BatchSlice, Fields, Tie, Work, write_pair};

/// What a log expression's surviving rows become.
enum Output<'a> {
    Range {
        op: RangeOp,
        grouping: Option<&'a Grouping>,
        unwraps: bool,
        /// The unwrapped label, which the series drops.
        dropped: Option<&'a str>,
        /// Set when only window totals are needed, which are then summed as
        /// rows are read instead of keeping each row.
        windows: Option<Windows>,
    },
    Labels {
        field: &'a str,
        grouping: Option<&'a Grouping>,
    },
    Logs,
    /// A raw log expression under a metric one, which fails evaluation.
    Unused,
}

/// One query's pipelines and what each produces; cheap to build, so every
/// worker builds its own.
pub(super) struct Plan<'a> {
    pipelines: Pipelines<'a>,
    outputs: Vec<Output<'a>>,
    filters_on_error: Vec<bool>,
    /// For an unindexed log query: its limit and direction, which bound
    /// the rows a batch can contribute.
    preselect: Option<(usize, Direction)>,
}

impl<'a> Plan<'a> {
    pub(super) fn new(
        query: &'a Query,
        request: &QueryRequest,
        preselect: Option<(usize, Direction)>,
    ) -> Result<Self> {
        let pipelines = Pipelines::new(query, request)?;
        let mut aggregations = Vec::new();
        collect_range_aggregations(query, &mut aggregations);
        let mut labels = Vec::new();
        collect_label_aggregations(query, &mut labels);
        let outputs = pipelines
            .logs
            .iter()
            .map(|log| {
                if let Some((op, _, grouping)) = aggregations
                    .iter()
                    .find(|(_, aggregated, _)| std::ptr::eq(*aggregated, *log))
                {
                    let unwrap = log.stages.iter().find_map(|stage| match &stage.value {
                        PipelineStage::Unwrap(unwrap) => Some(unwrap.label.as_str()),
                        _ => None,
                    });
                    let drops = grouping.is_none_or(|grouping| grouping.without);
                    return Ok(Output::Range {
                        op: *op,
                        grouping: *grouping,
                        unwraps: unwrap.is_some(),
                        dropped: unwrap.filter(|_| drops),
                        windows: sums_windows(*op, unwrap.is_some())
                            .then(|| Windows::new(log, request))
                            .transpose()?,
                    });
                }
                if let Some((field, _, grouping)) = labels
                    .iter()
                    .find(|(_, aggregated, _)| std::ptr::eq(*aggregated, *log))
                {
                    return Ok(Output::Labels {
                        field,
                        grouping: *grouping,
                    });
                }
                Ok(
                    if matches!(&query.value, Expr::Log(root) if std::ptr::eq(root, *log)) {
                        Output::Logs
                    } else {
                        Output::Unused
                    },
                )
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            filters_on_error: pipelines
                .logs
                .iter()
                .map(|log| filters_on_error(log))
                .collect(),
            preselect: preselect.filter(|_| matches!(query.value, Expr::Log(_))),
            outputs,
            pipelines,
        })
    }

    pub(super) fn outputs(&self) -> Outputs {
        Outputs(
            self.outputs
                .iter()
                .map(|output| match output {
                    Output::Range {
                        windows: Some(_), ..
                    } => ExprOutput::Totals(Box::default(), SeriesTable::default()),
                    Output::Range { .. } => ExprOutput::Range(Vec::new(), SeriesTable::default()),
                    Output::Labels { .. } => ExprOutput::Labels(Vec::new(), SeriesTable::default()),
                    Output::Logs => ExprOutput::Logs(Vec::new()),
                    Output::Unused => ExprOutput::Unused,
                })
                .collect(),
        )
    }

    /// Runs every log expression's pipeline over `slice`.
    pub(super) fn run(&self, slice: &BatchSlice, outputs: &mut Outputs) -> Result<()> {
        let pipelines = &self.pipelines;
        let batch = &*slice.batch;
        let base = slice.rows.start;
        let metric = pipelines.log_window.is_none();
        let mut stored: Option<Fields> = None;
        let mut identities: Option<Vec<u64>> = None;
        let mut selected = Vec::with_capacity(slice.len());
        for (index, log) in pipelines.logs.iter().enumerate() {
            selected.clear();
            selected.extend(
                slice
                    .rows
                    .clone()
                    .filter(|&row| pipelines.in_log_window(index, batch.timestamps[row]))
                    .map(|row| row as u32),
            );
            if selected.is_empty() || !selector_matches(log, &batch.labels)? {
                continue;
            }
            let tests = &pipelines.line_tests[index];
            if !tests.is_empty() {
                let mut kept = 0;
                for position in 0..selected.len() {
                    let row = selected[position];
                    if passes_line_tests(log, tests, batch.line(row as usize))? {
                        selected[kept] = row;
                        kept += 1;
                    }
                }
                selected.truncate(kept);
                if selected.is_empty() {
                    continue;
                }
            }
            let stored = stored.get_or_insert_with(|| Fields::stored(slice));
            let mut work = Work::new(slice, stored);
            if metric {
                work.promote_metadata(&selected);
            }
            self.run_stages(index, log, &mut work, &mut selected)?;
            if selected.is_empty() {
                continue;
            }
            let identity = |identities: &mut Option<Vec<u64>>, row: u32| {
                if pipelines.lineless {
                    return 0;
                }
                identities.get_or_insert_with(|| {
                    slice.rows.clone().map(|row| batch.identity(row)).collect()
                })[row as usize - base]
            };
            let range_series = |series: &mut SeriesTable,
                                at: usize,
                                grouping: Option<&Grouping>,
                                dropped: Option<&str>| {
                let errored = work.has_error(at);
                series.intern(&work, at, |name| {
                    ((errored && is_error_label(name))
                        || grouping.is_none_or(|grouping| {
                            grouping.labels.iter().any(|label| label == name) != grouping.without
                        }))
                        && dropped != Some(name)
                })
            };
            let range_value = |op, unwraps, at: usize| {
                sample_value(
                    op,
                    unwraps,
                    batch.weights.as_ref().map(|weights| weights[at]),
                    work.line(at).len(),
                    work.value(at),
                )
            };
            match (&self.outputs[index], &mut outputs.0[index]) {
                (
                    Output::Range {
                        op,
                        grouping,
                        unwraps,
                        dropped,
                        windows: Some(windows),
                    },
                    ExprOutput::Totals(totals, series),
                ) => {
                    for &row in &selected {
                        let at = row as usize;
                        let timestamp = batch.timestamps[at];
                        let Some(bucket) = windows.bucket(timestamp) else {
                            continue;
                        };
                        let value = range_value(*op, *unwraps, at);
                        let row = TotalRow {
                            order: (timestamp, batch.stream, batch.tie(at)),
                            bucket,
                            series: match value {
                                Some(_) => range_series(series, at, *grouping, *dropped),
                                None => u32::MAX,
                            },
                            value: value.map(|value| value as u64),
                            failure: pipeline_error(|name| work.label(name, at)),
                        };
                        if !pipelines.lineless && batch.may_repeat(at) {
                            totals.pending.push((batch.identity(at), row));
                        } else {
                            totals.add(row);
                        }
                    }
                }
                (
                    Output::Range {
                        op,
                        grouping,
                        unwraps,
                        dropped,
                        windows: None,
                    },
                    ExprOutput::Range(rows, series),
                ) => {
                    for &row in &selected {
                        let at = row as usize;
                        let value = range_value(*op, *unwraps, at);
                        let series = match value {
                            Some(_) => range_series(series, at, *grouping, *dropped),
                            None => u32::MAX,
                        };
                        rows.push(RangeRow {
                            timestamp_ns: batch.timestamps[at],
                            stream: batch.stream,
                            identity: identity(&mut identities, row),
                            tie: batch.tie(at),
                            value,
                            series,
                            failure: pipeline_error(|name| work.label(name, at)),
                        });
                    }
                }
                (Output::Labels { field, grouping }, ExprOutput::Labels(rows, groups)) => {
                    for &row in &selected {
                        let at = row as usize;
                        let value = work
                            .lookup(field, at)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned);
                        let group = match value {
                            Some(_) => groups.intern(&work, at, |name| {
                                name != *field
                                    && grouping.is_none_or(|grouping| {
                                        grouping.labels.iter().any(|label| label == name)
                                            != grouping.without
                                    })
                            }),
                            None => u32::MAX,
                        };
                        rows.push(LabelRow {
                            timestamp_ns: batch.timestamps[at],
                            stream: batch.stream,
                            identity: identity(&mut identities, row),
                            tie: batch.tie(at),
                            failure: pipeline_error(|name| work.label(name, at)),
                            value,
                            group,
                        });
                    }
                }
                (Output::Logs, ExprOutput::Logs(rows)) => {
                    if let Some((limit, direction)) = self.preselect {
                        preselect(&work, &mut selected, limit, direction);
                    }
                    let mut labels = SeriesTable::default();
                    for &row in &selected {
                        let at = row as usize;
                        let id = labels.intern(&work, at, |_| true);
                        rows.push(LogLine {
                            timestamp_ns: batch.timestamps[at],
                            line: work.line(at).to_owned(),
                            labels: Arc::clone(&labels.labels[id as usize]),
                            metadata: work.metadata_map(at),
                            stream: batch.stream,
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Runs the expression's stages over the selected rows, stage by stage,
    /// keeping those every stage keeps.
    fn run_stages(
        &self,
        index: usize,
        log: &LogExpr,
        work: &mut Work<'_>,
        selected: &mut Vec<u32>,
    ) -> Result<()> {
        let pipelines = &self.pipelines;
        let skip_parsers = pipelines.skip_parsers[index];
        let stages = log
            .stages
            .iter()
            .zip(&pipelines.runs[index])
            .filter(|(_, runs)| **runs)
            .map(|(stage, _)| &stage.value);
        for stage in stages {
            let parser = matches!(stage, PipelineStage::Parser(_));
            if skip_parsers && parser {
                continue;
            }
            let mut kept = 0;
            for position in 0..selected.len() {
                let row = selected[position];
                let at = row as usize;
                let unflagged = parser && !work.has_error(at);
                let mut cursor = work.cursor(at);
                let parsed = match stage {
                    PipelineStage::Parser(ParserStage::Logfmt {
                        strict,
                        keep_empty,
                        expressions,
                    }) if expressions.is_empty() => cursor.logfmt(*strict, *keep_empty),
                    _ => false,
                };
                if !parsed && !apply_stage(&mut cursor, stage)? {
                    continue;
                }
                // Loki flags only parser errors, when a filter asks about
                // them, at parse time so later stages can still `drop` it.
                if unflagged && work.has_error(at) && self.filters_on_error[index] {
                    work.cursor(at)
                        .insert_label(PRESERVE_ERROR_LABEL.into(), "true".into());
                }
                selected[kept] = row;
                kept += 1;
            }
            selected.truncate(kept);
            if selected.is_empty() {
                break;
            }
        }
        Ok(())
    }
}

/// Keeps the rows that can be among a log query's first `limit`: rows past
/// the timestamp at which `limit` distinct rows are reached cannot be, as
/// those rows stay distinct whatever else is read.
fn preselect(work: &Work<'_>, selected: &mut Vec<u32>, limit: usize, direction: Direction) {
    if selected.len() <= limit {
        return;
    }
    let timestamps = &work.batch.timestamps;
    let mut order = selected.clone();
    match direction {
        Direction::Forward => order.sort_by_key(|&row| timestamps[row as usize]),
        Direction::Backward => {
            order.sort_by_key(|&row| std::cmp::Reverse(timestamps[row as usize]))
        }
    }
    let mut seen = HashSet::new();
    let Some(cutoff) = order.iter().find_map(|&row| {
        let at = row as usize;
        (seen.insert(log_identity(work, at)) && seen.len() == limit).then_some(timestamps[at])
    }) else {
        return;
    };
    selected.retain(|&row| match direction {
        Direction::Forward => timestamps[row as usize] <= cutoff,
        Direction::Backward => timestamps[row as usize] >= cutoff,
    });
}

/// What log deduplication compares: stream, line and metadata.
fn log_identity(work: &Work<'_>, row: usize) -> u64 {
    let mut hasher = foldhash::quality::FixedState::default().build_hasher();
    hasher.write_u64(work.batch.stream);
    hasher.write(work.line(row).as_bytes());
    hasher.write_u8(0xff);
    for (name, value) in work.metadata(row) {
        write_pair(&mut hasher, name, value);
    }
    hasher.finish()
}

fn collect_label_aggregations<'a>(
    query: &'a Query,
    result: &mut Vec<(&'a str, &'a LogExpr, Option<&'a Grouping>)>,
) {
    match &query.value {
        Expr::LabelAggregation {
            field,
            expr,
            grouping,
        } => result.push((field.as_str(), expr, grouping.as_ref())),
        Expr::Vector(expr)
        | Expr::LabelReplace { expr, .. }
        | Expr::VectorAggregation { expr, .. } => collect_label_aggregations(expr, result),
        Expr::Binary { lhs, rhs, .. } => {
            collect_label_aggregations(lhs, result);
            collect_label_aggregations(rhs, result);
        }
        Expr::Log(_) | Expr::RangeAggregation { .. } | Expr::Number(_) | Expr::String(_) => {}
    }
}

/// Distinct label sets, found by hash and confirmed by comparison, so a
/// row's set allocates only the first time it is seen.
#[derive(Default)]
pub(super) struct SeriesTable {
    labels: Vec<LabelMap>,
    index: HashMap<u64, Vec<u32>>,
}

impl SeriesTable {
    /// The id of the row's labels that `keep` accepts.
    fn intern(&mut self, work: &Work<'_>, row: usize, keep: impl Fn(&str) -> bool) -> u32 {
        let pairs = || work.labels(row).filter(|(name, _)| keep(name));
        let mut hasher = foldhash::quality::FixedState::default().build_hasher();
        for (name, value) in pairs() {
            write_pair(&mut hasher, name, value);
        }
        let hash = hasher.finish();
        if let Some(ids) = self.index.get(&hash) {
            for &id in ids {
                let known = &self.labels[id as usize];
                let mut candidate = pairs();
                if known
                    .iter()
                    .all(|(name, value)| candidate.next() == Some((name, value)))
                    && candidate.next().is_none()
                {
                    return id;
                }
            }
        }
        let labels = pairs()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        self.insert(hash, Arc::new(labels))
    }

    fn insert(&mut self, hash: u64, labels: LabelMap) -> u32 {
        let id = u32::try_from(self.labels.len()).expect("fewer than u32::MAX label sets");
        self.labels.push(labels);
        self.index.entry(hash).or_default().push(id);
        id
    }

    /// The id of `labels`, which another table holds.
    fn adopt(&mut self, labels: &LabelMap) -> u32 {
        let mut hasher = foldhash::quality::FixedState::default().build_hasher();
        for (name, value) in labels.iter() {
            write_pair(&mut hasher, name, value);
        }
        let hash = hasher.finish();
        if let Some(ids) = self.index.get(&hash)
            && let Some(&id) = ids.iter().find(|&&id| self.labels[id as usize] == *labels)
        {
            return id;
        }
        self.insert(hash, Arc::clone(labels))
    }

    /// Ids in this table of every label set in `other`.
    fn absorb(&mut self, other: &SeriesTable) -> Vec<u32> {
        other
            .labels
            .iter()
            .map(|labels| self.adopt(labels))
            .collect()
    }
}

/// One row's contribution to a range aggregation's window totals.
struct TotalRow {
    /// Timestamp, stream and tie: Loki's merge order.
    order: (i64, u64, Tie),
    bucket: i64,
    /// Index of the row's series labels; meaningless without a value.
    series: u32,
    value: Option<u64>,
    failure: Option<String>,
}

/// A range aggregation's rows summed per series and window bucket as they
/// are read, so it holds memory for its series and steps, not its rows.
#[derive(Default)]
struct Totals {
    sums: HashMap<(u32, i64), u64>,
    /// Per bucket, its first failing row in merge order and the failure.
    failures: HashMap<i64, ((i64, u64, Tie), String)>,
    /// Rows that may repeat a stored row of another batch, with their
    /// identity, in read order; compared once merged in read order.
    pending: Vec<(u64, TotalRow)>,
    /// Rows that may repeat another, of the segment last compared. A
    /// duplicate shares its timestamp, so never spans segments, which are
    /// read one after another.
    seen: HashSet<(i64, u64, u64)>,
    seen_segment: Option<(u32, u32)>,
}

impl Totals {
    fn add(&mut self, row: TotalRow) {
        self.fail(row.bucket, row.order, row.failure);
        if let Some(value) = row.value {
            *self.sums.entry((row.series, row.bucket)).or_default() += value;
        }
    }

    fn fail(&mut self, bucket: i64, order: (i64, u64, Tie), failure: Option<String>) {
        let Some(message) = failure else {
            return;
        };
        match self.failures.entry(bucket) {
            std::collections::hash_map::Entry::Occupied(mut first) => {
                if order < first.get().0 {
                    first.insert((order, message));
                }
            }
            std::collections::hash_map::Entry::Vacant(first) => {
                first.insert((order, message));
            }
        }
    }

    /// Counts pending rows that repeat no row counted before them.
    fn settle(&mut self) {
        for (identity, row) in std::mem::take(&mut self.pending) {
            let (timestamp, stream, tie) = row.order;
            let segment = (tie.database, tie.segment);
            if self.seen_segment != Some(segment) {
                self.seen.clear();
                self.seen_segment = Some(segment);
            }
            if self.seen.insert((timestamp, stream, identity)) {
                self.add(row);
            } else {
                // Duplicates fail alike; the first in merge order names it.
                self.fail(row.bucket, row.order, row.failure);
            }
        }
    }

    /// Adds what a later batch summed, whose series `ids` renumbers.
    fn extend(&mut self, other: Totals, ids: &[u32]) {
        self.settle();
        for ((series, bucket), value) in other.sums {
            *self.sums.entry((ids[series as usize], bucket)).or_default() += value;
        }
        for (bucket, (order, message)) in other.failures {
            self.fail(bucket, order, Some(message));
        }
        self.pending
            .extend(other.pending.into_iter().map(|(identity, mut row)| {
                if row.value.is_some() {
                    row.series = ids[row.series as usize];
                }
                (identity, row)
            }));
        self.settle();
    }

    fn set_database(&mut self, database: u32) {
        for ((_, _, tie), _) in self.failures.values_mut() {
            tie.database = database;
        }
        for (_, row) in &mut self.pending {
            row.order.2.database = database;
        }
    }

    fn into_series(mut self, labels: &[LabelMap]) -> RangeSeries {
        self.settle();
        let mut sums = self
            .sums
            .into_iter()
            .map(|((series, bucket), value)| (bucket, series, value))
            .collect::<Vec<_>>();
        sums.sort_unstable();
        let mut failures = self
            .failures
            .into_iter()
            .map(|(bucket, (_, message))| (bucket, message))
            .collect::<Vec<_>>();
        failures.sort_unstable_by_key(|(bucket, _)| *bucket);
        RangeSeries::from_totals(sums, failures, labels)
    }
}

/// Per log expression, what its pipeline kept.
pub(super) struct Outputs(Vec<ExprOutput>);

enum ExprOutput {
    Totals(Box<Totals>, SeriesTable),
    Range(Vec<RangeRow>, SeriesTable),
    Labels(Vec<LabelRow>, SeriesTable),
    Logs(Vec<LogLine>),
    Unused,
}

impl Outputs {
    pub(super) fn empty() -> Self {
        Self(Vec::new())
    }

    /// Appends what a later batch kept.
    pub(super) fn extend(&mut self, other: Outputs) {
        for (output, other) in self.0.iter_mut().zip(other.0) {
            match (output, other) {
                (ExprOutput::Totals(totals, series), ExprOutput::Totals(more, theirs)) => {
                    let ids = series.absorb(&theirs);
                    totals.extend(*more, &ids);
                }
                (ExprOutput::Range(rows, series), ExprOutput::Range(more, theirs)) => {
                    let ids = series.absorb(&theirs);
                    rows.extend(more.into_iter().map(|mut row| {
                        if row.value.is_some() {
                            row.series = ids[row.series as usize];
                        }
                        row
                    }));
                }
                (ExprOutput::Labels(rows, groups), ExprOutput::Labels(more, theirs)) => {
                    let ids = groups.absorb(&theirs);
                    rows.extend(more.into_iter().map(|mut row| {
                        if row.value.is_some() {
                            row.group = ids[row.group as usize];
                        }
                        row
                    }));
                }
                (ExprOutput::Logs(rows), ExprOutput::Logs(more)) => rows.extend(more),
                _ => {}
            }
        }
    }

    /// Compares rows that may repeat others with those counted so far; only
    /// for outputs that rows are merged into in read order.
    pub(super) fn settle(&mut self) {
        for output in &mut self.0 {
            if let ExprOutput::Totals(totals, _) = output {
                totals.settle();
            }
        }
    }

    /// Marks every row as read from the `database`th database.
    pub(super) fn set_database(&mut self, database: u32) {
        for output in &mut self.0 {
            match output {
                ExprOutput::Totals(totals, _) => totals.set_database(database),
                ExprOutput::Range(rows, _) => {
                    rows.iter_mut().for_each(|row| row.tie.database = database)
                }
                ExprOutput::Labels(rows, _) => {
                    rows.iter_mut().for_each(|row| row.tie.database = database)
                }
                ExprOutput::Logs(_) | ExprOutput::Unused => {}
            }
        }
    }

    /// Rows kept for a log query.
    pub(super) fn kept(&self) -> usize {
        match self.0.first() {
            Some(ExprOutput::Logs(rows)) => rows.len(),
            _ => 0,
        }
    }

    /// Keeps a log query's first `limit` rows in `direction`.
    pub(super) fn truncate_logs(&mut self, direction: Direction, limit: usize) {
        if let Some(ExprOutput::Logs(rows)) = self.0.first_mut() {
            sort_logs(rows, direction, false);
            rows.truncate(limit);
        }
    }

    pub(super) fn into_logs(self) -> Vec<LogLine> {
        match self.0.into_iter().next() {
            Some(ExprOutput::Logs(rows)) => rows,
            _ => Vec::new(),
        }
    }

    /// Reduces metric outputs to what evaluation reads: samples in Loki's
    /// merge order, timestamp then stream hash, each stored row once.
    pub(super) fn finish(self, plan: &Plan<'_>) -> MetricRows {
        let lineless = plan.pipelines.lineless;
        let mut ranges = HashMap::new();
        let mut label_samples = HashMap::new();
        for ((log, output), produced) in plan.pipelines.logs.iter().zip(&plan.outputs).zip(self.0) {
            let key = *log as *const LogExpr as usize;
            match (output, produced) {
                (Output::Range { .. }, ExprOutput::Totals(totals, series)) => {
                    ranges.insert(key, totals.into_series(&series.labels));
                }
                (Output::Range { op, .. }, ExprOutput::Range(mut rows, series)) => {
                    merge_order(&mut rows, lineless, |row| {
                        (row.timestamp_ns, row.stream, row.tie, row.identity)
                    });
                    ranges.insert(key, RangeSeries::from_rows(*op, log, rows, &series.labels));
                }
                (Output::Labels { .. }, ExprOutput::Labels(mut rows, groups)) => {
                    merge_order(&mut rows, lineless, |row| {
                        (row.timestamp_ns, row.stream, row.tie, row.identity)
                    });
                    label_samples.insert(
                        key,
                        LabelSamples {
                            timestamps: rows.iter().map(|row| row.timestamp_ns).collect(),
                            rows,
                            groups: groups.labels,
                        },
                    );
                }
                _ => {}
            }
        }
        MetricRows {
            ranges,
            label_samples,
            label_keys: RefCell::default(),
            relabels: RefCell::default(),
        }
    }
}

/// Sorts rows by timestamp, stream and tie, then drops any whose stored row
/// an earlier one with the same timestamp and stream already holds.
fn merge_order<T>(rows: &mut Vec<T>, lineless: bool, key: impl Fn(&T) -> (i64, u64, Tie, u64)) {
    rows.sort_by_key(|row| {
        let (timestamp, stream, tie, _) = key(row);
        (timestamp, stream, tie)
    });
    if lineless {
        return;
    }
    let mut duplicate = vec![false; rows.len()];
    let mut any = false;
    let mut start = 0;
    while start < rows.len() {
        let (timestamp, stream, ..) = key(&rows[start]);
        let end = start
            + rows[start..]
                .iter()
                .take_while(|row| {
                    let (at, other, ..) = key(row);
                    at == timestamp && other == stream
                })
                .count();
        if end - start > 1 {
            let mut order = (start..end).collect::<Vec<_>>();
            order.sort_by_key(|&index| (key(&rows[index]).3, index));
            for pair in order.windows(2) {
                if key(&rows[pair[0]]).3 == key(&rows[pair[1]]).3 {
                    duplicate[pair[1]] = true;
                    any = true;
                }
            }
        }
        start = end;
    }
    if any {
        let mut index = 0;
        rows.retain(|_| {
            index += 1;
            !duplicate[index - 1]
        });
    }
}
