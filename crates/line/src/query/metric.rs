use super::*;

pub(super) fn eval_expr(query: &Query, rows: &MetricRows, timestamp: i64) -> Result<Value> {
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
            ..
        } => range_aggregation(*op, *parameter, expr, rows, timestamp),
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
    // A failing row anywhere in the window fails the query, as the first such
    // row would before grouping.
    let first_failure = series.failures.partition_point(|(at, _)| *at <= start);
    if let Some((at, message)) = series.failures.get(first_failure)
        && *at <= end
    {
        return Err(Error::Query(message.clone()));
    }
    if op == RangeOp::Absent {
        return Ok(Value::Vector(if window(&series.timestamps).is_empty() {
            vec![Point {
                labels: selector_equality_labels(log),
                value: 1.0,
            }]
        } else {
            Vec::new()
        }));
    }
    let seconds = range as f64 / 1_000_000_000.0;
    let effective_end = end;
    let mut points = Vec::new();
    for group in &series.groups {
        let selected = window(&group.timestamps);
        let values = &group.values[selected.clone()];
        if values.is_empty() {
            continue;
        }
        let labels = group.labels.clone();
        let value = match op {
            RangeOp::Count | RangeOp::Bytes => group.sum(selected),
            RangeOp::Rate | RangeOp::BytesRate => group.sum(selected) / seconds,
            RangeOp::RateCounter => {
                let samples = group.timestamps[selected]
                    .iter()
                    .copied()
                    .zip(values.iter().copied())
                    .collect::<Vec<_>>();
                extrapolated_counter_rate(&samples, range, effective_end)
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

/// A metric sample carrying `__error__` fails the query unless the pipeline
/// asked to keep it with `__preserve_error__="true"`.
fn check_error(row: &Row) -> Result<()> {
    error_message(row).map_or(Ok(()), |message| Err(Error::Query(message)))
}

fn error_message(row: &Row) -> Option<String> {
    match row.labels.get(ERROR_LABEL) {
        Some(kind) if row.labels.get(PRESERVE_ERROR_LABEL).map(String::as_str) != Some("true") => {
            let details = row
                .labels
                .get(ERROR_DETAILS_LABEL)
                .map_or("", String::as_str);
            Some(format!("pipeline error: {kind}: {details}"))
        }
        _ => None,
    }
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
}

struct RangeGroup {
    labels: BTreeMap<String, String>,
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
    /// `rows` must be in ascending timestamp order.
    pub(super) fn new(
        op: RangeOp,
        log: &LogExpr,
        grouping: Option<&Grouping>,
        rows: &[Row],
    ) -> Self {
        let unwrap_label = log.stages.iter().find_map(|stage| match &stage.value {
            PipelineStage::Unwrap(unwrap) => Some(&unwrap.label),
            _ => None,
        });
        let drop_unwrap_label = grouping.is_none_or(|grouping| grouping.without);
        let whole = match op {
            RangeOp::Count | RangeOp::Bytes | RangeOp::BytesRate => true,
            RangeOp::Rate => unwrap_label.is_none(),
            _ => false,
        };
        let mut failures = Vec::new();
        let mut groups: BTreeMap<BTreeMap<String, String>, RangeGroup> = BTreeMap::new();
        for row in rows {
            // Checked before grouping, which could otherwise hide the label.
            if let Some(message) = error_message(row) {
                failures.push((row.timestamp_ns, message));
            }
            let value = match op {
                RangeOp::Bytes | RangeOp::BytesRate => Some(row.line.len() as f64),
                RangeOp::Rate if unwrap_label.is_some() => row.value,
                RangeOp::Count | RangeOp::Rate | RangeOp::Absent => Some(1.0),
                _ => row.value,
            };
            let Some(value) = value else {
                continue;
            };
            let mut labels = group_labels(&row.labels, grouping);
            if row.labels.contains_key(ERROR_LABEL) {
                labels.extend(
                    row.labels
                        .iter()
                        .filter(|(name, _)| is_error_label(name))
                        .map(|(name, value)| (name.clone(), value.clone())),
                );
            }
            if drop_unwrap_label && let Some(label) = unwrap_label {
                labels.remove(label);
            }
            let group = groups
                .entry(labels)
                .or_insert_with_key(|labels| RangeGroup {
                    labels: labels.clone(),
                    timestamps: Vec::new(),
                    values: Vec::new(),
                    totals: if whole { vec![0] } else { Vec::new() },
                });
            group.timestamps.push(row.timestamp_ns);
            group.values.push(value);
            if whole {
                let total = group.totals.last().copied().unwrap_or(0);
                group.totals.push(total.saturating_add(value as u64));
            }
        }
        Self {
            timestamps: rows.iter().map(|row| row.timestamp_ns).collect(),
            failures,
            groups: groups.into_values().collect(),
        }
    }
}

pub(super) fn label_aggregation(
    field: &str,
    log: &LogExpr,
    grouping: Option<&Grouping>,
    rows: &MetricRows,
    timestamp: i64,
) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("label aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    let selected = rows.window(log, timestamp.saturating_sub(range), timestamp)?;
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeSet<String>> = BTreeMap::new();
    for row in selected {
        check_error(row)?;
        if let Some(value) = lookup(row, field).filter(|value| !value.is_empty()) {
            let mut labels = group_labels(&row.labels, grouping);
            // The counted field would otherwise split every value into its own series.
            labels.remove(field);
            groups.entry(labels).or_default().insert(value.to_owned());
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

pub(super) fn selector_equality_labels(log: &LogExpr) -> BTreeMap<String, String> {
    log.selector
        .value
        .matchers
        .iter()
        .filter(|matcher| matcher.value.op == MatchOp::Equal)
        .map(|matcher| (matcher.value.label.clone(), matcher.value.value.clone()))
        .collect()
}

pub(super) fn group_labels(
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

/// A vector aggregation without `by`/`without` aggregates every series into
/// one, unlike a range aggregation, which then keeps each series' labels.
fn vector_group_labels(
    labels: &BTreeMap<String, String>,
    grouping: Option<&Grouping>,
) -> BTreeMap<String, String> {
    match grouping {
        None => BTreeMap::new(),
        grouping => group_labels(labels, grouping),
    }
}

pub(super) fn vector_aggregation(
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
                points.sort_by(point_value_order_desc);
            }
            return points;
        }
        let mut groups: BTreeMap<BTreeMap<String, String>, Vec<Point>> = BTreeMap::new();
        for point in points {
            groups
                .entry(vector_group_labels(&point.labels, grouping))
                .or_default()
                .push(point);
        }
        let mut selected = Vec::new();
        for (_, mut points) in groups {
            if op == VectorOp::BottomK {
                points.sort_by(point_value_order);
            } else {
                points.sort_by(point_value_order_desc);
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
            .entry(vector_group_labels(&point.labels, grouping))
            .or_default()
            .push(point.value);
    }
    groups
        .into_iter()
        .map(|(labels, values)| {
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
            Point { labels, value }
        })
        .collect()
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

pub(super) fn binary(
    lhs: Value,
    op: BinaryOp,
    modifier: Option<&BinaryModifier>,
    rhs: Value,
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
            Ok(Value::Vector(binary_vectors(lhs, op, modifier, rhs)?))
        }
    }
}

pub(super) fn binary_vectors(
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
    let set_operator = matches!(op, BinaryOp::And | BinaryOp::Unless);
    if !set_operator && !group_right && right.values().any(|points| points.len() > 1) {
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
                include_labels(&mut output.labels, &left.labels, matching);
            } else if group_left {
                include_labels(&mut output.labels, &right.labels, matching);
            } else if let Some(matching) = matching {
                // One-to-one results keep only the matched-on labels.
                output
                    .labels
                    .retain(|name, _| matching.labels.contains(name) == matching.on);
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

pub(super) fn variance(values: &[f64]) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64
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
