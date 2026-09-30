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

pub(super) fn range_aggregation(
    op: RangeOp,
    parameter: Option<f64>,
    log: &LogExpr,
    grouping: Option<&Grouping>,
    rows: &MetricRows,
    timestamp: i64,
) -> Result<Value> {
    let range = log
        .range
        .as_ref()
        .ok_or_else(|| Error::Query("range aggregation requires a range".into()))
        .and_then(|value| parse_duration_ns(&value.value))?;
    // LogQL range windows are left-open and right-closed: (t-range, t].
    let selected = rows.window(log, timestamp.saturating_sub(range), timestamp)?;
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<&Row>> = BTreeMap::new();
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
        if let Some(value) = lookup(row, field) {
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

pub(super) fn point_value_order(a: &Point, b: &Point) -> Ordering {
    a.value
        .total_cmp(&b.value)
        .then_with(|| a.labels.cmp(&b.labels))
}

pub(super) fn binary(
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

pub(super) fn binary_number(
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
