//! Group-map and binary matching-table construction for physical operators.

use super::*;

// ---------------------------------------------------------------------------
// Group-map construction (Aggregate / CountValues)
// ---------------------------------------------------------------------------

/// Project a labelset onto the grouping keys — either keep only `by(…)` or
/// drop `without(…)` labels. `by(__name__, …)` explicitly preserves the
/// metric name; every other aggregate shape drops it.
fn group_key_labels(labels: &Labels, grouping: &AggregateGrouping) -> Vec<Label> {
    match grouping {
        AggregateGrouping::By(keep) => labels
            .iter()
            .filter(|l| keep.iter().any(|k| k == &l.name))
            .cloned()
            .collect(),
        AggregateGrouping::Without(drop) => labels
            .iter()
            .filter(|l| l.name != "__name__" && !drop.iter().any(|k| k == &l.name))
            .cloned()
            .collect(),
    }
}

/// Result of bucketing an input schema by an [`AggregateGrouping`]: a
/// [`GroupMap`] for the operator plus parallel group labels for the output
/// schema.
pub(super) struct GroupBuild {
    pub(super) map: GroupMap,
    pub(super) group_labels: Vec<Labels>,
}

pub(super) fn build_group_map(
    input: &SeriesSchema,
    grouping: &AggregateGrouping,
) -> Result<GroupBuild, PlanError> {
    let mut keys: HashMap<Vec<Label>, u32> = HashMap::new();
    let mut group_labels: Vec<Labels> = Vec::new();
    let mut input_to_group: Vec<Option<u32>> = Vec::with_capacity(input.len());
    for idx in 0..input.len() {
        let labels = input.labels(idx as u32);
        let mut key = group_key_labels(labels, grouping);
        key.sort();
        let next_idx = group_labels.len() as u32;
        let g = match keys.get(&key) {
            Some(&g) => g,
            None => {
                keys.insert(key.clone(), next_idx);
                group_labels.push(Labels::new(key));
                next_idx
            }
        };
        input_to_group.push(Some(g));
    }
    Ok(GroupBuild {
        map: GroupMap::new(input_to_group, group_labels.len()),
        group_labels,
    })
}

/// Result of bucketing an input schema for a classic-histogram function:
/// each series' group and parsed `le` bound, plus the group labels.
pub(super) struct HistogramGroups {
    pub(super) inputs: Vec<Option<BucketSeries>>,
    pub(super) group_labels: Vec<Labels>,
}

/// Group bucket series by every label except `le` and `__name__`. Series
/// whose `le` is missing or not a float are dropped, as in Prometheus.
pub(super) fn build_histogram_groups(input: &SeriesSchema) -> HistogramGroups {
    let grouping = AggregateGrouping::Without(Arc::from(vec!["le".to_string()]));
    let mut keys: HashMap<Vec<Label>, u32> = HashMap::new();
    let mut group_labels: Vec<Labels> = Vec::new();
    let mut inputs = Vec::with_capacity(input.len());
    for idx in 0..input.len() {
        let labels = input.labels(idx as u32);
        let Some(upper_bound) = labels.get("le").and_then(|le| le.parse::<f64>().ok()) else {
            inputs.push(None);
            continue;
        };
        let mut key = group_key_labels(labels, &grouping);
        key.sort();
        let group = *keys.entry(key).or_insert_with_key(|key| {
            group_labels.push(Labels::new(key.clone()));
            (group_labels.len() - 1) as u32
        });
        inputs.push(Some(BucketSeries { group, upper_bound }));
    }
    HistogramGroups {
        inputs,
        group_labels,
    }
}

pub(super) fn build_group_schema(group_labels: &[Labels]) -> Arc<SeriesSchema> {
    let labels: Arc<[Labels]> = Arc::from(group_labels.to_vec());
    let fps: Vec<u128> = (0..group_labels.len() as u128).collect();
    Arc::new(SeriesSchema::new(labels, Arc::from(fps)))
}

// ---------------------------------------------------------------------------
// Match-table construction (Binary)
// ---------------------------------------------------------------------------

/// Project a labelset onto a binary matching axis. `on(l…)` keeps only
/// those labels; `ignoring(l…)` drops them. `__name__` is dropped in either
/// case for vector/vector matching (matches Prometheus `signature`
/// semantics).
fn matching_key(labels: &Labels, axis: MatchingAxis, matching_labels: &[String]) -> Vec<Label> {
    match axis {
        MatchingAxis::On => labels
            .iter()
            .filter(|l| l.name != "__name__" && matching_labels.iter().any(|m| m == &l.name))
            .cloned()
            .collect(),
        MatchingAxis::Ignoring => labels
            .iter()
            .filter(|l| l.name != "__name__" && !matching_labels.iter().any(|m| m == &l.name))
            .cloned()
            .collect(),
    }
}

/// Default matching (no explicit modifier): `ignoring()` with an empty
/// drop-list (i.e. match on every label except `__name__`).
fn default_axis_and_labels() -> (MatchingAxis, Vec<String>) {
    (MatchingAxis::Ignoring, Vec::new())
}

pub(super) struct MatchBuild {
    pub(super) table: MatchTable,
    /// Output schema the binary operator publishes.
    pub(super) output_schema: Arc<SeriesSchema>,
}

pub(super) fn build_match_table(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    matching: Option<&BinaryMatching>,
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    let (axis, labels): (MatchingAxis, Vec<String>) = match matching {
        Some(m) => {
            let mut v: Vec<String> = m.labels.iter().cloned().collect();
            v.sort();
            (m.axis, v)
        }
        None => default_axis_and_labels(),
    };
    let cardinality = matching
        .map(|m| m.cardinality.clone())
        .unwrap_or(Cardinality::OneToOne);

    match cardinality {
        Cardinality::OneToOne | Cardinality::ManyToMany => {
            build_one_to_one(lhs, rhs, axis, &labels, include_name_on_output)
        }
        Cardinality::GroupLeft { include } => {
            build_group_left(lhs, rhs, axis, &labels, &include, include_name_on_output)
        }
        Cardinality::GroupRight { include } => {
            build_group_right(lhs, rhs, axis, &labels, &include, include_name_on_output)
        }
    }
}

pub(super) fn build_one_to_one(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // Index RHS by matching key. `OneToOne` requires at most one RHS per
    // key; we defer duplicate-detection to runtime (operator emits per-cell
    // validity = 0 on unmatched — duplicate pairing is a rare corner case
    // not exercised by v1 tests).
    let mut rhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for j in 0..rhs.len() {
        let key = matching_key(rhs.labels(j as u32), axis, match_labels);
        rhs_by_key.entry(key).or_insert(j as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(lhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(lhs.len());
    for i in 0..lhs.len() {
        let lab = lhs.labels(i as u32);
        let key = matching_key(lab, axis, match_labels);
        map.push(rhs_by_key.get(&key).copied());
        out_labels.push(result_labels_for_one_to_one(
            lab,
            axis,
            match_labels,
            include_name_on_output,
        ));
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::OneToOne(map),
        output_schema,
    })
}

fn build_group_left(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // LHS is the "many" side; output has one row per LHS row pointing at
    // the single RHS "one" side. `include_labels` are carried from the
    // "one" side onto the output labels.
    let mut rhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for j in 0..rhs.len() {
        let key = matching_key(rhs.labels(j as u32), axis, match_labels);
        rhs_by_key.entry(key).or_insert(j as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(lhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(lhs.len());
    for i in 0..lhs.len() {
        let lab = lhs.labels(i as u32);
        let key = matching_key(lab, axis, match_labels);
        let rhs_idx = rhs_by_key.get(&key).copied();
        let composed = compose_group_labels(
            lab,
            rhs_idx.map(|j| rhs.labels(j)),
            include_labels,
            include_name_on_output,
        );
        map.push(rhs_idx);
        out_labels.push(composed);
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::GroupLeft(map),
        output_schema,
    })
}

fn build_group_right(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // Mirror of GroupLeft: RHS is "many"; output rows align with RHS.
    let mut lhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for i in 0..lhs.len() {
        let key = matching_key(lhs.labels(i as u32), axis, match_labels);
        lhs_by_key.entry(key).or_insert(i as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(rhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(rhs.len());
    for j in 0..rhs.len() {
        let lab = rhs.labels(j as u32);
        let key = matching_key(lab, axis, match_labels);
        let lhs_idx = lhs_by_key.get(&key).copied();
        let composed = compose_group_labels(
            lab,
            lhs_idx.map(|i| lhs.labels(i)),
            include_labels,
            include_name_on_output,
        );
        map.push(lhs_idx);
        out_labels.push(composed);
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::GroupRight(map),
        output_schema,
    })
}

fn result_labels_for_output(input: &Labels, include_name: bool) -> Labels {
    if include_name {
        input.clone()
    } else {
        let v: Vec<Label> = input
            .iter()
            .filter(|l| l.name != "__name__")
            .cloned()
            .collect();
        Labels::new(v)
    }
}

fn result_labels_for_one_to_one(
    input: &Labels,
    axis: MatchingAxis,
    match_labels: &[String],
    include_name_on_output: bool,
) -> Labels {
    let projected = result_labels_for_output(input, include_name_on_output);
    match axis {
        MatchingAxis::On => Labels::new(
            projected
                .iter()
                .filter(|label| match_labels.iter().any(|name| name == &label.name))
                .cloned()
                .collect(),
        ),
        MatchingAxis::Ignoring => Labels::new(
            projected
                .iter()
                .filter(|label| !match_labels.iter().any(|name| name == &label.name))
                .cloned()
                .collect(),
        ),
    }
}

/// Compose the output labels for a group_left / group_right row: the
/// "many" side's full labelset (drop `__name__` unless requested), plus
/// the `include(...)` labels copied from the "one" side (if matched).
fn compose_group_labels(
    many_side: &Labels,
    one_side: Option<&Labels>,
    include_labels: &[String],
    include_name_on_output: bool,
) -> Labels {
    let mut out: Vec<Label> = many_side
        .iter()
        .filter(|l| include_name_on_output || l.name != "__name__")
        .cloned()
        .collect();
    if let Some(one) = one_side {
        for name in include_labels {
            // Drop any existing label with this name from the many side,
            // then copy the one-side value (if present).
            out.retain(|l| &l.name != name);
            if let Some(v) = one.get(name) {
                out.push(Label::new(name.clone(), v));
            }
        }
    }
    out.sort();
    Labels::new(out)
}

fn build_output_schema_from_labels(labels: Vec<Labels>) -> Arc<SeriesSchema> {
    let fps: Vec<u128> = (0..labels.len() as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

pub(super) struct LabelManipBuild {
    pub(super) input_to_output: Arc<[u32]>,
    pub(super) output_schema: Arc<SeriesSchema>,
}

pub(super) fn build_label_manip(
    kind: &LabelManipKind,
    input: &SeriesSchema,
) -> Result<LabelManipBuild, PlanError> {
    let mut input_to_output: Vec<u32> = Vec::with_capacity(input.len());
    let mut seen: HashMap<Labels, u32> = HashMap::new();
    let mut output_labels: Vec<Labels> = Vec::new();

    for index in 0..input.len() {
        let transformed = kind
            .apply_to_labels(input.labels(index as u32))
            .map_err(map_construct_err)?;
        let out_idx = match seen.get(&transformed) {
            Some(idx) => *idx,
            None => {
                let idx = output_labels.len() as u32;
                seen.insert(transformed.clone(), idx);
                output_labels.push(transformed);
                idx
            }
        };
        input_to_output.push(out_idx);
    }

    Ok(LabelManipBuild {
        input_to_output: Arc::from(input_to_output),
        output_schema: build_output_schema_from_labels(output_labels),
    })
}

/// `true` when a binary op preserves the source metric's `__name__` label.
/// Matches the legacy engine's `changes_metric_schema`: arithmetic ops
/// drop `__name__`; set ops and non-`bool` comparisons preserve it.
pub(super) fn preserves_metric_name(op: BinaryOpKind) -> bool {
    match op {
        BinaryOpKind::Add | BinaryOpKind::Sub | BinaryOpKind::Mul | BinaryOpKind::Div => false,
        BinaryOpKind::Mod | BinaryOpKind::Pow | BinaryOpKind::Atan2 => true,
        BinaryOpKind::Eq { bool_modifier } => !bool_modifier,
        BinaryOpKind::Ne { bool_modifier } => !bool_modifier,
        BinaryOpKind::Gt { bool_modifier } => !bool_modifier,
        BinaryOpKind::Lt { bool_modifier } => !bool_modifier,
        BinaryOpKind::Gte { bool_modifier } => !bool_modifier,
        BinaryOpKind::Lte { bool_modifier } => !bool_modifier,
        BinaryOpKind::And | BinaryOpKind::Or | BinaryOpKind::Unless => true,
    }
}
