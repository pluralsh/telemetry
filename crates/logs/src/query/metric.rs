use std::collections::hash_map::Entry;

use super::*;

pub(super) fn eval_expr(query: &Query, rows: &MetricRows, timestamp: i64) -> Result<Value> {
    match &query.value {
        Expr::Number(value) => Ok(Value::Scalar(*value)),
        Expr::String(_) => Err(Error::Query("string is not a numeric expression".into())),
        Expr::Vector(expr) => match eval_expr(expr, rows, timestamp)? {
            Value::Scalar(value) => Ok(Value::Vector(vec![Point {
                labels: Arc::default(),
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
            ..
        } => range_aggregation(*op, *parameter, expr, rows, timestamp),
        Expr::LabelAggregation { expr, .. } => label_aggregation(expr, rows, timestamp),
        Expr::VectorAggregation {
            op,
            parameter,
            expr,
            grouping,
        } => {
            let Value::Vector(points) = eval_expr(expr, rows, timestamp)? else {
                return Err(Error::Query("vector aggregation requires a vector".into()));
            };
            let mut label_keys = rows.label_keys.borrow_mut();
            let keys = label_keys
                .entry(query as *const Query as usize)
                .or_default();
            Ok(Value::Vector(vector_aggregation(
                *op,
                *parameter,
                grouping.as_ref(),
                points,
                keys,
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
            let mut relabels = rows.relabels.borrow_mut();
            let relabel = match relabels.entry(query as *const Query as usize) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(Relabel::new(regex)?),
            };
            relabel.start_step();
            for point in &mut points {
                point.labels = relabel.apply(&point.labels, dst, replacement, src);
            }
            Ok(Value::Vector(points))
        }
        Expr::Binary {
            lhs,
            op,
            modifier,
            rhs,
        } => {
            let (lhs, rhs) = (
                eval_expr(lhs, rows, timestamp)?,
                eval_expr(rhs, rows, timestamp)?,
            );
            let mut label_keys = rows.label_keys.borrow_mut();
            let keys = label_keys
                .entry(query as *const Query as usize)
                .or_default();
            binary(lhs, *op, modifier.as_ref(), rhs, keys)
        }
    }
}

pub(super) fn range_aggregation(
    op: RangeOp,
    parameter: Option<f64>,
    log: &LogExpr,
    rows: &MetricRows,
    timestamp: i64,
) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("range aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    let offset = log_offset(log)?;
    // LogQL range windows are left-open and right-closed: (t-range, t].
    let (start, end) = (
        timestamp.saturating_sub(range).saturating_sub(offset),
        timestamp.saturating_sub(offset),
    );
    let series = rows.range_series(log)?;
    let window = |timestamps: &[i64]| {
        let low = timestamps.partition_point(|&at| at <= start);
        low..timestamps.partition_point(|&at| at <= end).max(low)
    };
    // Loki's absent extracts samples without labels, so rows flagged with
    // `__error__` still count as present rather than failing the query.
    if op == RangeOp::Absent {
        return Ok(Value::Vector(if window(&series.timestamps).is_empty() {
            vec![Point {
                labels: Arc::new(selector_equality_labels(log)),
                value: 1.0,
            }]
        } else {
            Vec::new()
        }));
    }
    // A failing row anywhere in the window fails the query, as the first such
    // row would before grouping.
    let first_failure = series.failures.partition_point(|(at, _)| *at <= start);
    if let Some((at, message)) = series.failures.get(first_failure)
        && *at <= end
    {
        return Err(Error::Query(message.clone()));
    }
    let seconds = range as f64 / 1_000_000_000.0;
    let mut points = Vec::new();
    for group in series.active(start, end) {
        let selected = window(&group.timestamps);
        let values = &group.values[selected.clone()];
        if values.is_empty() {
            continue;
        }
        let labels = Arc::clone(&group.labels);
        let value = match op {
            RangeOp::Count | RangeOp::Bytes => group.sum(selected),
            RangeOp::Rate | RangeOp::BytesRate => group.sum(selected) / seconds,
            RangeOp::RateCounter => {
                let samples = group.timestamps[selected]
                    .iter()
                    .copied()
                    .zip(values.iter().copied())
                    .collect::<Vec<_>>();
                extrapolated_counter_rate(&samples, range, end)
            }
            RangeOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
            RangeOp::Sum => values.iter().sum(),
            RangeOp::Min => extreme(values, Ordering::Less),
            RangeOp::Max => extreme(values, Ordering::Greater),
            RangeOp::Stddev => variance(values).sqrt(),
            RangeOp::Stdvar => variance(values),
            RangeOp::Quantile => quantile(parameter.unwrap_or(0.5), values),
            RangeOp::First => values[0],
            RangeOp::Last => *values.last().expect("not empty"),
            RangeOp::Absent => continue,
        };
        points.push(Point { labels, value });
    }
    Ok(Value::Vector(points))
}

/// The error a metric sample with labels `label` fails its query with: one
/// carrying `__error__` fails it unless the pipeline asked to keep it with
/// `__preserve_error__="true"`.
pub(super) fn pipeline_error<'a>(label: impl Fn(&str) -> Option<&'a str>) -> Option<String> {
    match label(ERROR_LABEL) {
        Some(kind) if label(PRESERVE_ERROR_LABEL) != Some("true") => {
            let details = label(ERROR_DETAILS_LABEL).unwrap_or("");
            Some(format!("pipeline error: {kind}: {details}"))
        }
        _ => None,
    }
}

/// What a row contributes to a range aggregation: a weighted row stands for
/// stored rows that every window holds all or none of, so it contributes
/// their totals at once.
pub(super) fn sample_value(
    op: RangeOp,
    unwraps: bool,
    weight: Option<(u32, u64)>,
    line_bytes: usize,
    unwrapped: Option<f64>,
) -> Option<f64> {
    match (op, weight) {
        (RangeOp::Bytes | RangeOp::BytesRate, Some((_, bytes))) => Some(bytes as f64),
        (RangeOp::Count | RangeOp::Rate, Some((rows, _))) => Some(f64::from(rows)),
        (RangeOp::Bytes | RangeOp::BytesRate, None) => Some(line_bytes as f64),
        (RangeOp::Rate, _) if unwraps => unwrapped,
        (RangeOp::Count | RangeOp::Rate | RangeOp::Absent, _) => Some(1.0),
        _ => unwrapped,
    }
}

/// One row's contribution to a range aggregation.
pub(super) struct RangeRow {
    pub(super) timestamp_ns: i64,
    pub(super) stream: u64,
    /// The stored row's identity among those sharing its timestamp and
    /// stream; unset for lineless reads, which need no deduplication.
    pub(super) identity: u64,
    /// Orders rows sharing a timestamp and stream as Loki's merge does.
    pub(super) tie: Tie,
    pub(super) value: Option<f64>,
    /// Index of the row's series labels; meaningless without a value.
    pub(super) series: u32,
    pub(super) failure: Option<String>,
}

/// A label aggregation's rows, in timestamp order.
#[derive(Default)]
pub(super) struct LabelSamples {
    pub(super) timestamps: Vec<i64>,
    pub(super) rows: Vec<LabelRow>,
    pub(super) groups: Vec<LabelMap>,
}

pub(super) struct LabelRow {
    pub(super) timestamp_ns: i64,
    pub(super) stream: u64,
    pub(super) identity: u64,
    pub(super) tie: Tie,
    pub(super) failure: Option<String>,
    /// The counted field's value, when present and not empty.
    pub(super) value: Option<String>,
    /// Index of the row's group labels; meaningless without a value.
    pub(super) group: u32,
}

/// A range aggregation's rows reduced to what every step needs: each row's
/// group and contributed value are computed once, so a step only
/// binary-searches its window in each group.
pub(super) struct RangeSeries {
    /// Every row's timestamp, ascending.
    timestamps: Vec<i64>,
    /// Rows that fail the query when a window contains them, ascending.
    failures: Vec<(i64, String)>,
    /// In label order; rows that contribute no value are left out.
    groups: Vec<RangeGroup>,
    /// Each contributing row's timestamp and group, ascending, so a step
    /// whose window holds fewer rows than there are groups visits only the
    /// groups it holds.
    members: Vec<(i64, u32)>,
}

struct RangeGroup {
    labels: LabelMap,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    /// Running totals for ops whose values are whole numbers (counts and
    /// byte lengths), where subtraction is exact; empty otherwise.
    totals: Vec<u64>,
}

impl RangeGroup {
    fn sum(&self, window: std::ops::Range<usize>) -> f64 {
        if self.totals.is_empty() {
            self.values[window].iter().sum()
        } else {
            (self.totals[window.end] - self.totals[window.start]) as f64
        }
    }
}

impl RangeSeries {
    /// Groups with a row in `(start, end]`, in label order, possibly with
    /// some that have none.
    fn active(&self, start: i64, end: i64) -> Vec<&RangeGroup> {
        let low = self.members.partition_point(|(at, _)| *at <= start);
        let high = self.members.partition_point(|(at, _)| *at <= end).max(low);
        if high - low >= self.groups.len() {
            return self.groups.iter().collect();
        }
        let mut indices = self.members[low..high]
            .iter()
            .map(|(_, group)| *group)
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        indices
            .into_iter()
            .map(|group| &self.groups[group as usize])
            .collect()
    }

    /// From rows in timestamp order, whose series index `labels`.
    pub(super) fn from_rows(
        op: RangeOp,
        log: &LogExpr,
        rows: Vec<RangeRow>,
        labels: &[LabelMap],
    ) -> Self {
        let unwraps = log
            .stages
            .iter()
            .any(|stage| matches!(stage.value, PipelineStage::Unwrap(_)));
        let whole = match op {
            RangeOp::Count | RangeOp::Bytes | RangeOp::BytesRate => true,
            RangeOp::Rate => !unwraps,
            _ => false,
        };
        let timestamps = rows.iter().map(|row| row.timestamp_ns).collect();
        let mut failures = Vec::new();
        let mut ids = vec![u32::MAX; labels.len()];
        let mut groups: Vec<RangeGroup> = Vec::new();
        let mut members = Vec::new();
        for row in rows {
            if let Some(message) = row.failure {
                failures.push((row.timestamp_ns, message));
            }
            let Some(value) = row.value else {
                continue;
            };
            let id = &mut ids[row.series as usize];
            if *id == u32::MAX {
                *id = u32::try_from(groups.len()).expect("fewer than u32::MAX groups");
                groups.push(RangeGroup {
                    labels: Arc::clone(&labels[row.series as usize]),
                    timestamps: Vec::new(),
                    values: Vec::new(),
                    totals: if whole { vec![0] } else { Vec::new() },
                });
            }
            members.push((row.timestamp_ns, *id));
            let group = &mut groups[*id as usize];
            group.timestamps.push(row.timestamp_ns);
            group.values.push(value);
            if whole {
                let total = group.totals.last().copied().unwrap_or(0);
                group.totals.push(total.saturating_add(value as u64));
            }
        }
        Self::ordered(timestamps, failures, groups, members)
    }

    /// Groups are numbered as they first appear; renumbers them, and
    /// `members`, in label order.
    fn ordered(
        timestamps: Vec<i64>,
        failures: Vec<(i64, String)>,
        groups: Vec<RangeGroup>,
        mut members: Vec<(i64, u32)>,
    ) -> Self {
        let mut order = (0..groups.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&a, &b| groups[a].labels.cmp(&groups[b].labels));
        let mut rank = vec![0u32; groups.len()];
        for (position, &id) in order.iter().enumerate() {
            rank[id] = u32::try_from(position).expect("fewer than u32::MAX groups");
        }
        for (_, id) in &mut members {
            *id = rank[*id as usize];
        }
        let mut ordered = groups.into_iter().zip(&rank).collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(_, position)| **position);
        Self {
            timestamps,
            failures,
            groups: ordered.into_iter().map(|(group, _)| group).collect(),
            members,
        }
    }
}

/// Counts distinct values of the aggregated field per group; the pipeline
/// already narrowed each row's labels to its group, without the field.
pub(super) fn label_aggregation(log: &LogExpr, rows: &MetricRows, timestamp: i64) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("label aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    let samples = rows
        .label_samples
        .get(&(log as *const LogExpr as usize))
        .ok_or_else(|| Error::Query("label aggregation was not prepared".into()))?;
    let offset = log_offset(log)?;
    let (start, end) = (
        timestamp.saturating_sub(range).saturating_sub(offset),
        timestamp.saturating_sub(offset),
    );
    let low = samples.timestamps.partition_point(|&at| at <= start);
    let high = samples.timestamps.partition_point(|&at| at <= end).max(low);
    let mut groups: BTreeMap<&BTreeMap<String, String>, (u32, BTreeSet<&str>)> = BTreeMap::new();
    for row in &samples.rows[low..high] {
        if let Some(message) = &row.failure {
            return Err(Error::Query(message.clone()));
        }
        if let Some(value) = &row.value {
            let labels = &samples.groups[row.group as usize];
            groups
                .entry(labels)
                .or_insert_with(|| (row.group, BTreeSet::new()))
                .1
                .insert(value);
        }
    }
    Ok(Value::Vector(
        groups
            .into_values()
            .map(|(group, values)| Point {
                labels: Arc::clone(&samples.groups[group as usize]),
                value: values.len() as f64,
            })
            .collect(),
    ))
}

/// Loki's `absentLabels`: a label's first equality matcher sets it, but any
/// other matcher on the same name, including a second equality, removes it.
pub(super) fn selector_equality_labels(log: &LogExpr) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut removed = BTreeSet::new();
    for matcher in &log.selector.value.matchers {
        let matcher = &matcher.value;
        if matcher.op == MatchOp::Equal && !labels.contains_key(&matcher.label) {
            labels.insert(matcher.label.clone(), matcher.value.clone());
        } else {
            removed.insert(&matcher.label);
        }
    }
    labels.retain(|name, value| !value.is_empty() && !removed.contains(name));
    labels
}

/// A vector aggregation's group, as borrowed pairs in name order, which sort
/// like the label maps they become, so only each output group allocates its
/// labels. Without `by`/`without` every series falls into one group, unlike
/// a range aggregation, which then keeps each series' labels.
fn vector_group_key<'a>(
    labels: &'a BTreeMap<String, String>,
    grouping: Option<&Grouping>,
) -> Vec<(&'a str, &'a str)> {
    labels
        .iter()
        .filter(|(name, _)| {
            grouping.is_some_and(|grouping| grouping.labels.contains(*name) != grouping.without)
        })
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

pub(super) fn vector_aggregation(
    op: VectorOp,
    parameter: Option<f64>,
    grouping: Option<&Grouping>,
    points: Vec<Point>,
    keys: &mut LabelKeys,
) -> Vec<Point> {
    if matches!(op, VectorOp::Sort | VectorOp::SortDesc) {
        let mut points = points;
        if op == VectorOp::Sort {
            points.sort_by(point_value_order);
        } else {
            points.sort_by(point_value_order_desc);
        }
        return points;
    }
    let groups = vector_groups(&points, grouping, keys);
    if matches!(
        op,
        VectorOp::TopK | VectorOp::BottomK | VectorOp::ApproxTopK
    ) {
        let count = parameter.unwrap_or(0.0).max(0.0) as usize;
        let mut selected = Vec::new();
        for (_, mut points) in groups {
            if op == VectorOp::BottomK {
                points.sort_by(|a, b| point_value_order(a, b));
            } else {
                points.sort_by(|a, b| point_value_order_desc(a, b));
            }
            selected.extend(points.into_iter().take(count).cloned());
        }
        return selected;
    }
    groups
        .into_iter()
        .map(|(id, points)| {
            let values = points.iter().map(|point| point.value).collect::<Vec<_>>();
            let value = match op {
                VectorOp::Sum => values.iter().sum(),
                VectorOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                VectorOp::Min => extreme(&values, Ordering::Less),
                VectorOp::Max => extreme(&values, Ordering::Greater),
                VectorOp::Count => values.len() as f64,
                VectorOp::Stddev => variance(&values).sqrt(),
                VectorOp::Stdvar => variance(&values),
                _ => unreachable!(),
            };
            Point {
                labels: Arc::clone(keys.labels(id)),
                value,
            }
        })
        .collect()
}

/// `points` by aggregation group, in the groups' label order, each group's
/// points in input order.
fn vector_groups<'a>(
    points: &'a [Point],
    grouping: Option<&Grouping>,
    keys: &mut LabelKeys,
) -> Vec<(u32, Vec<&'a Point>)> {
    keys.start_step();
    let mut slots: HashMap<u32, usize> = HashMap::new();
    let mut groups: Vec<(u32, Vec<&Point>)> = Vec::new();
    for point in points {
        let id = keys.id(&point.labels, |labels| {
            vector_group_key(labels, grouping)
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect()
        });
        let slot = *slots.entry(id).or_insert_with(|| {
            groups.push((id, Vec::new()));
            groups.len() - 1
        });
        groups[slot].1.push(point);
    }
    groups.sort_by(|(a, _), (b, _)| keys.labels(*a).cmp(keys.labels(*b)));
    groups
}

/// Prometheus' `min`/`max`: NaN only wins when every value is NaN.
fn extreme(values: &[f64], wanted: Ordering) -> f64 {
    values.iter().copied().fold(f64::NAN, |current, value| {
        if current.is_nan() || value.partial_cmp(&current) == Some(wanted) {
            value
        } else {
            current
        }
    })
}

/// Orders by value with NaN last, as Prometheus sorts in both directions.
pub(super) fn point_value_order(a: &Point, b: &Point) -> Ordering {
    a.value
        .is_nan()
        .cmp(&b.value.is_nan())
        .then_with(|| a.value.total_cmp(&b.value))
        .then_with(|| a.labels.cmp(&b.labels))
}

fn point_value_order_desc(a: &Point, b: &Point) -> Ordering {
    a.value
        .is_nan()
        .cmp(&b.value.is_nan())
        .then_with(|| b.value.total_cmp(&a.value))
        .then_with(|| a.labels.cmp(&b.labels))
}

/// One expression's keys for its operands' series (match keys for a binary
/// expression, groups for an aggregation), interned so that series compare
/// by id. A series' labels keep their allocation from step to step, so keys
/// are remembered by label address for one step, as [`super::Matrix`] does,
/// rather than rebuilt for every point at every step.
#[derive(Default)]
pub(super) struct LabelKeys {
    ids: HashMap<Vec<(String, String)>, u32>,
    /// By id: the key as labels, shared by every step's output for it.
    labels: Vec<LabelMap>,
    /// Holding the labels keeps their addresses from being reused.
    previous: HashMap<*const BTreeMap<String, String>, (LabelMap, u32)>,
    current: HashMap<*const BTreeMap<String, String>, (LabelMap, u32)>,
}

impl LabelKeys {
    fn start_step(&mut self) {
        self.previous = std::mem::take(&mut self.current);
    }

    fn id(
        &mut self,
        labels: &LabelMap,
        key: impl FnOnce(&BTreeMap<String, String>) -> Vec<(String, String)>,
    ) -> u32 {
        let address = Arc::as_ptr(labels);
        if let Some((_, id)) = self.current.get(&address) {
            return *id;
        }
        let id = match self.previous.remove(&address) {
            Some((_, id)) => id,
            None => {
                let key = key(labels);
                match self.ids.get(&key) {
                    Some(id) => *id,
                    None => {
                        let id = u32::try_from(self.ids.len()).expect("fewer than u32::MAX keys");
                        self.labels.push(Arc::new(key.iter().cloned().collect()));
                        self.ids.insert(key, id);
                        id
                    }
                }
            }
        };
        self.current.insert(address, (Arc::clone(labels), id));
        id
    }

    fn labels(&self, id: u32) -> &LabelMap {
        &self.labels[id as usize]
    }
}

/// One `label_replace`'s rewrites, remembered by input label address for one
/// step as [`LabelKeys`] does, so a series is rewritten once rather than at
/// every step and its output keeps one allocation for [`super::Matrix`].
pub(super) struct Relabel {
    regex: Regex,
    /// Holding the inputs keeps their addresses from being reused.
    previous: HashMap<*const BTreeMap<String, String>, (LabelMap, LabelMap)>,
    current: HashMap<*const BTreeMap<String, String>, (LabelMap, LabelMap)>,
}

impl Relabel {
    fn new(regex: &str) -> Result<Self> {
        Ok(Self {
            regex: Regex::new(&format!("^(?:{regex})$"))?,
            previous: HashMap::new(),
            current: HashMap::new(),
        })
    }

    fn start_step(&mut self) {
        self.previous = std::mem::take(&mut self.current);
    }

    fn apply(&mut self, labels: &LabelMap, dst: &str, replacement: &str, src: &str) -> LabelMap {
        let address = Arc::as_ptr(labels);
        if let Some((_, output)) = self.current.get(&address) {
            return Arc::clone(output);
        }
        let output = match self.previous.remove(&address) {
            Some((_, output)) => output,
            None => {
                let source = labels.get(src).map_or("", String::as_str);
                if self.regex.is_match(source) {
                    let replaced = self.regex.replace(source, replacement).into_owned();
                    let mut rewritten = BTreeMap::clone(labels);
                    if replaced.is_empty() {
                        rewritten.remove(dst);
                    } else {
                        rewritten.insert(dst.to_owned(), replaced);
                    }
                    Arc::new(rewritten)
                } else {
                    Arc::clone(labels)
                }
            }
        };
        self.current
            .insert(address, (Arc::clone(labels), Arc::clone(&output)));
        output
    }
}

pub(super) fn binary(
    lhs: Value,
    op: BinaryOp,
    modifier: Option<&BinaryModifier>,
    rhs: Value,
    keys: &mut LabelKeys,
) -> Result<Value> {
    match (lhs, rhs) {
        // Scalar comparisons always yield 1 or 0, with or without `bool`.
        (Value::Scalar(lhs), Value::Scalar(rhs)) if is_comparison(op) => Ok(Value::Scalar(
            f64::from(binary_number(lhs, op, rhs, None)?.is_some()),
        )),
        (Value::Scalar(lhs), Value::Scalar(rhs)) => Ok(Value::Scalar(
            binary_number(lhs, op, rhs, modifier)?.unwrap_or(f64::NAN),
        )),
        (Value::Vector(points), Value::Scalar(scalar)) => Ok(Value::Vector(
            points
                .into_iter()
                .filter_map(|mut point| {
                    binary_number(point.value, op, scalar, modifier)
                        .ok()
                        .flatten()
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
                        .flatten()
                        .map(|value| {
                            // A filtering comparison keeps the vector's value.
                            point.value = if is_comparison(op) && !returns_bool(modifier) {
                                point.value
                            } else {
                                value
                            };
                            point
                        })
                })
                .collect(),
        )),
        (Value::Vector(lhs), Value::Vector(rhs)) => {
            Ok(Value::Vector(binary_vectors(lhs, op, modifier, rhs, keys)?))
        }
    }
}

fn binary_vectors(
    lhs: Vec<Point>,
    op: BinaryOp,
    modifier: Option<&BinaryModifier>,
    rhs: Vec<Point>,
    keys: &mut LabelKeys,
) -> Result<Vec<Point>> {
    keys.start_step();
    let matching = modifier.and_then(|modifier| modifier.matching.as_ref());
    if op == BinaryOp::Or {
        let mut result = lhs;
        let existing = result
            .iter()
            .map(|point| keys.id(&point.labels, |labels| match_key(labels, matching)))
            .collect::<HashSet<_>>();
        result.extend(rhs.into_iter().filter(|point| {
            !existing.contains(&keys.id(&point.labels, |labels| match_key(labels, matching)))
        }));
        return Ok(result);
    }
    let group_right = matching
        .and_then(|matching| matching.grouping.as_ref())
        .is_some_and(|grouping| !grouping.left);
    let group_left = matching
        .and_then(|matching| matching.grouping.as_ref())
        .is_some_and(|grouping| grouping.left);
    let mut right: HashMap<u32, Vec<Point>> = HashMap::new();
    for point in rhs {
        right
            .entry(keys.id(&point.labels, |labels| match_key(labels, matching)))
            .or_default()
            .push(point);
    }
    let set_operator = matches!(op, BinaryOp::And | BinaryOp::Unless);
    if !set_operator && !group_right && right.values().any(|points| points.len() > 1) {
        return Err(Error::Query(
            "found duplicate series on the right hand-side; many-to-many matching not allowed: matching labels must be unique on one side".into(),
        ));
    }
    let mut result = Vec::new();
    let mut matched_left = HashSet::new();
    let mut output_labels = BTreeSet::new();
    for left in lhs {
        let key = keys.id(&left.labels, |labels| match_key(labels, matching));
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
        if !group_left && !group_right && !matched_left.insert(key) {
            return Err(Error::Query(
                "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
                    .into(),
            ));
        }
        for right in right_points {
            let Some(value) = binary_number(left.value, op, right.value, modifier)? else {
                continue;
            };
            let mut output = if group_right {
                right.clone()
            } else {
                left.clone()
            };
            output.value = value;
            if group_right {
                include_labels(Arc::make_mut(&mut output.labels), &left.labels, matching);
            } else if group_left {
                include_labels(Arc::make_mut(&mut output.labels), &right.labels, matching);
            } else if let Some(matching) = matching {
                // One-to-one results keep only the matched-on labels.
                Arc::make_mut(&mut output.labels)
                    .retain(|name, _| matching.labels.contains(name) == matching.on);
            }
            // One-to-one results are labelled by their match key, which the
            // check above already keeps unique.
            if (group_left || group_right) && !output_labels.insert(Arc::clone(&output.labels)) {
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

pub(super) fn match_key(
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

pub(super) fn include_labels(
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

fn is_comparison(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::Greater
            | BinaryOp::GreaterOrEqual
            | BinaryOp::Less
            | BinaryOp::LessOrEqual
    )
}

fn returns_bool(modifier: Option<&BinaryModifier>) -> bool {
    modifier.is_some_and(|modifier| modifier.return_bool)
}

/// `None` is a filtering comparison that did not hold; arithmetic results,
/// NaN included, are always kept.
pub(super) fn binary_number(
    lhs: f64,
    op: BinaryOp,
    rhs: f64,
    modifier: Option<&BinaryModifier>,
) -> Result<Option<f64>> {
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
        return Ok(if returns_bool(modifier) {
            Some(f64::from(comparison))
        } else {
            comparison.then_some(lhs)
        });
    }
    Ok(Some(match op {
        BinaryOp::Add => lhs + rhs,
        BinaryOp::Sub => lhs - rhs,
        BinaryOp::Mul => lhs * rhs,
        // Loki yields NaN rather than ±Inf for a zero divisor.
        BinaryOp::Div | BinaryOp::Mod if rhs == 0.0 => f64::NAN,
        BinaryOp::Div => lhs / rhs,
        BinaryOp::Mod => lhs % rhs,
        BinaryOp::Pow => lhs.powf(rhs),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => {
            return Err(Error::Query("set operators require vectors".into()));
        }
        _ => unreachable!(),
    }))
}

pub(super) fn string_match(op: MatchOp, actual: &str, expected: &str) -> Result<bool> {
    Ok(match op {
        MatchOp::Equal => actual == expected,
        MatchOp::NotEqual => actual != expected,
        MatchOp::Regex => with_regex(RegexKind::Anchored, expected, |regex| {
            regex.is_match(actual)
        })?,
        MatchOp::NotRegex => !with_regex(RegexKind::Anchored, expected, |regex| {
            regex.is_match(actual)
        })?,
    })
}

/// Population variance by Welford's update, as Loki computes it, so equal
/// values give exactly zero.
pub(super) fn variance(values: &[f64]) -> f64 {
    let (mut count, mut mean, mut aux) = (0.0, 0.0, 0.0);
    for &value in values {
        count += 1.0;
        let delta = value - mean;
        mean += delta / count;
        aux += delta * (value - mean);
    }
    aux / count
}

pub(super) fn quantile(quantile: f64, values: &[f64]) -> f64 {
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

/// Loki's `extrapolatedRate` as fixed in grafana/loki#23684. Released Lokis
/// through 3.7 mix nanosecond and millisecond units here and so barely
/// extrapolate at all.
pub(super) fn extrapolated_counter_rate(
    samples: &[(i64, f64)],
    range_ns: i64,
    range_end: i64,
) -> f64 {
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

#[cfg(test)]
mod tests {
    use super::variance;

    #[test]
    fn should_give_zero_variance_for_equal_values() {
        assert_eq!(variance(&[0.1, 0.1, 0.1]), 0.0);
        assert_eq!(variance(&[0.3; 7]), 0.0);
    }

    #[test]
    fn should_compute_population_variance() {
        assert_eq!(variance(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]), 4.0);
    }
}
