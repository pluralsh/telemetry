use super::*;
use futures::stream;
use promql_parser::label::{MatchOp, Matcher, Matchers};
use promql_parser::parser as pparser;
use std::task::{Context, Waker};

use crate::model::{Label, Labels};
use crate::promql::source::{ResolvedSeriesChunk, SampleBatch, SampleBlock, SamplesRequest};

// ---- mock source -----------------------------------------------------

/// Minimal `SeriesSource` for unit tests. Associates a selector's
/// metric name (via `__name__` matchers) with a roster of (labels,
/// samples) entries.
struct MockSource {
    series: Vec<(Labels, Vec<(i64, f64)>)>,
    /// When set, `resolve()` yields this error instead of the roster.
    fail_resolve: Option<String>,
}

impl MockSource {
    fn new(series: Vec<(Labels, Vec<(i64, f64)>)>) -> Self {
        Self {
            series,
            fail_resolve: None,
        }
    }

    fn failing(msg: &str) -> Self {
        Self {
            series: Vec::new(),
            fail_resolve: Some(msg.to_string()),
        }
    }
}

impl SeriesSource for MockSource {
    fn resolve(
        &self,
        _selector: &pparser::VectorSelector,
        _time_range: TimeRange,
    ) -> impl futures::Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        if let Some(msg) = &self.fail_resolve {
            let err = QueryError::Internal(msg.clone());
            return stream::iter(vec![Err(err)]).left_stream();
        }
        let labels: Vec<Labels> = self.series.iter().map(|(l, _)| l.clone()).collect();
        let name: Arc<str> = Arc::from("m");
        let refs: Vec<ResolvedSeriesRef> = (0..self.series.len())
            .map(|i| ResolvedSeriesRef::new(1, i as u32, name.clone()))
            .collect();
        let chunk = ResolvedSeriesChunk {
            bucket_id: 1,
            labels: Arc::from(labels),
            series: Arc::from(refs),
        };
        stream::iter(vec![Ok(chunk)]).right_stream()
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl futures::Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let mut block = SampleBlock::with_series_count(request.series.len());
        for (col, sref) in request.series.iter().enumerate() {
            let samples = &self.series[sref.series_id as usize].1;
            for (t, v) in samples {
                if *t >= request.time_range.start_ms && *t < request.time_range.end_ms_exclusive {
                    block.timestamps[col].push(*t);
                    block.values[col].push(*v);
                }
            }
        }
        stream::iter(vec![Ok(SampleBatch {
            series_range: 0..request.series.len(),
            samples: block,
        })])
    }
}

struct ChunkedResolveSource {
    chunks: Vec<ResolvedSeriesChunk>,
}

impl ChunkedResolveSource {
    fn new(chunks: Vec<ResolvedSeriesChunk>) -> Self {
        Self { chunks }
    }
}

impl SeriesSource for ChunkedResolveSource {
    fn resolve(
        &self,
        _selector: &pparser::VectorSelector,
        _time_range: TimeRange,
    ) -> impl futures::Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        stream::iter(self.chunks.clone().into_iter().map(Ok))
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl futures::Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let block = SampleBlock::with_series_count(request.series.len());
        stream::iter(vec![Ok(SampleBatch {
            series_range: 0..request.series.len(),
            samples: block,
        })])
    }
}

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

fn labels_of(pairs: &[(&str, &str)]) -> Labels {
    let mut v: Vec<Label> = pairs.iter().map(|(n, val)| Label::new(*n, *val)).collect();
    v.sort();
    Labels::new(v)
}

fn make_ctx() -> LoweringContext {
    LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
}

fn parse(input: &str) -> pparser::Expr {
    pparser::parse(input).unwrap_or_else(|e| panic!("parse({input:?}): {e}"))
}

fn make_selector(metric: &str) -> pparser::VectorSelector {
    let m = Matcher::new(MatchOp::Equal, "__name__", metric);
    pparser::VectorSelector::new(Some(metric.to_string()), Matchers::new(vec![m]))
}

async fn build<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
) -> Result<PhysicalPlan, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    build_physical_plan(plan, source, reservation.clone(), ctx).await
}

fn mk_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---- tests ----------------------------------------------------------

#[test]
fn should_build_vector_selector_from_logical_plan() {
    // given: a mock source with two series under metric `m`
    let source = Arc::new(MockSource::new(vec![
        (
            labels_of(&[("__name__", "m"), ("pod", "a")]),
            vec![(0, 1.0)],
        ),
        (
            labels_of(&[("__name__", "m"), ("pod", "b")]),
            vec![(0, 2.0)],
        ),
    ]));
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 20);

    // when: build the physical plan
    let rt = mk_rt();
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: root is a VectorSelectorOp with the resolved 2-series schema
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 2);
    assert_eq!(physical.step_grid.step_count, 11);
}

#[test]
fn should_dedup_leaf_roster_by_fingerprint_and_group_bucket_requests() {
    // given: the same logical series appears in two bucket-scoped resolve
    // chunks, plus one distinct series in only the newer bucket.
    let name: Arc<str> = Arc::from("m");
    let source = Arc::new(ChunkedResolveSource::new(vec![
        ResolvedSeriesChunk {
            bucket_id: 10,
            labels: Arc::from(vec![labels_of(&[("__name__", "m"), ("pod", "a")])]),
            series: Arc::from(vec![ResolvedSeriesRef::new(10, 7, name.clone())]),
        },
        ResolvedSeriesChunk {
            bucket_id: 20,
            labels: Arc::from(vec![
                labels_of(&[("__name__", "m"), ("pod", "a")]),
                labels_of(&[("__name__", "m"), ("pod", "b")]),
            ]),
            series: Arc::from(vec![
                ResolvedSeriesRef::new(20, 11, name.clone()),
                ResolvedSeriesRef::new(20, 12, name.clone()),
            ]),
        },
    ]));
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();

    // when
    let resolved = rt
        .block_on(resolve_leaf(
            &source,
            &make_selector("m"),
            TimeRange::new(0, 1_000),
            &reservation,
        ))
        .expect("resolved leaf");

    // then: one logical roster row per unique labelset, but the `pod=a`
    // row keeps both bucket-local handles for sample loading.
    assert_eq!(resolved.schema.len(), 2);
    assert_eq!(resolved.request_series.len(), 2);
    assert_eq!(
        resolved.schema.labels(0),
        &labels_of(&[("__name__", "m"), ("pod", "a")])
    );
    assert_eq!(
        resolved.schema.labels(1),
        &labels_of(&[("__name__", "m"), ("pod", "b")])
    );
    assert_eq!(
        resolved.request_series[0].as_ref(),
        &[
            ResolvedSeriesRef::new(10, 7, name.clone()),
            ResolvedSeriesRef::new(20, 11, name.clone()),
        ]
    );
    assert_eq!(
        resolved.request_series[1].as_ref(),
        &[ResolvedSeriesRef::new(20, 12, name.clone())]
    );
    assert_ne!(
        resolved.schema.fingerprint(0),
        resolved.schema.fingerprint(1)
    );
}

#[test]
fn should_build_matrix_selector() {
    // given: a mock source with one series
    let source = Arc::new(MockSource::new(vec![(
        labels_of(&[("__name__", "m")]),
        vec![(0, 1.0), (1_000, 2.0)],
    )]));
    let ctx = make_ctx();
    // Wrap matrix selector in a Rollup so it has a legal parent.
    let plan = LogicalPlan::Rollup {
        kind: crate::promql::operators::rollup::RollupKind::Rate,
        child: Box::new(LogicalPlan::MatrixSelector {
            selector: make_selector("m"),
            range_ms: 5_000,
            offset: Offset::Pos(0),
            at: None,
        }),
    };
    let reservation = MemoryReservation::new(1 << 20);

    // when: build
    let rt = mk_rt();
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: output has the one-series input schema (Rollup passes schema through)
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 1);
}

#[test]
fn should_compute_group_map_for_sum_by_label() {
    // given: three series across two groups (pod=a twice, pod=b once)
    let input = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "1")]),
            labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "2")]),
            labels_of(&[("__name__", "m"), ("pod", "b"), ("inst", "3")]),
        ]),
        Arc::from(vec![0u128, 1, 2]),
    );
    let grouping = AggregateGrouping::By(Arc::from(vec!["pod".to_string()]));
    // when: build the group map
    let built = build_group_map(&input, &grouping).expect("group build");
    // then: two groups, inputs 0 and 1 share group 0, input 2 is group 1
    assert_eq!(built.map.group_count, 2);
    assert_eq!(built.map.input_to_group[0], Some(0));
    assert_eq!(built.map.input_to_group[1], Some(0));
    assert_eq!(built.map.input_to_group[2], Some(1));
    assert_eq!(built.group_labels.len(), 2);
}

#[test]
fn should_compute_group_map_for_sum_without_label() {
    // given: three series; `without (inst)` projects onto (__name__, pod)
    let input = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "1")]),
            labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "2")]),
            labels_of(&[("__name__", "m"), ("pod", "b"), ("inst", "3")]),
        ]),
        Arc::from(vec![0u128, 1, 2]),
    );
    let grouping = AggregateGrouping::Without(Arc::from(vec!["inst".to_string()]));
    // when
    let built = build_group_map(&input, &grouping).unwrap();
    // then: `__name__` is also stripped (aggregate convention), so the
    // keys are just `pod=a` and `pod=b` — 2 groups.
    assert_eq!(built.map.group_count, 2);
    assert_eq!(built.map.input_to_group[0], built.map.input_to_group[1]);
    assert_ne!(built.map.input_to_group[0], built.map.input_to_group[2]);
}

#[test]
fn should_output_input_schema_for_topk() {
    // given: a topk plan over a 3-series selector
    let source = Arc::new(MockSource::new(vec![
        (labels_of(&[("__name__", "m"), ("i", "1")]), vec![(0, 1.0)]),
        (labels_of(&[("__name__", "m"), ("i", "2")]), vec![(0, 2.0)]),
        (labels_of(&[("__name__", "m"), ("i", "3")]), vec![(0, 3.0)]),
    ]));
    let ctx = make_ctx();
    let child = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let plan = LogicalPlan::Aggregate {
        kind: AggregateKind::Topk(2),
        child: Box::new(child),
        param: None,
        grouping: AggregateGrouping::by_empty(),
    };
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();

    // when
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: output schema matches the 3-series input
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 3);
}

#[test]
fn should_output_group_schema_for_sum() {
    // given: sum by (pod) over 3 series in 2 groups
    let source = Arc::new(MockSource::new(vec![
        (
            labels_of(&[("__name__", "m"), ("pod", "a")]),
            vec![(0, 1.0)],
        ),
        (
            labels_of(&[("__name__", "m"), ("pod", "a")]),
            vec![(0, 2.0)],
        ),
        (
            labels_of(&[("__name__", "m"), ("pod", "b")]),
            vec![(0, 3.0)],
        ),
    ]));
    let ctx = make_ctx();
    let child = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let plan = LogicalPlan::Aggregate {
        kind: AggregateKind::Sum,
        child: Box::new(child),
        param: None,
        grouping: AggregateGrouping::By(Arc::from(vec!["pod".to_string()])),
    };
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();

    // when
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: one series per group (2)
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 2);
}

#[test]
fn should_preserve_name_label_when_grouping_by_name() {
    // given
    let input = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "m"), ("env", "prod"), ("inst", "0")]),
            labels_of(&[("__name__", "m"), ("env", "prod"), ("inst", "1")]),
        ]),
        Arc::from(vec![0u128, 1]),
    );

    // when
    let built = build_group_map(
        &input,
        &AggregateGrouping::By(Arc::from(vec!["__name__".to_string(), "env".to_string()])),
    )
    .unwrap();

    // then
    assert_eq!(built.group_labels.len(), 1);
    assert_eq!(built.group_labels[0].get("__name__"), Some("m"));
    assert_eq!(built.group_labels[0].get("env"), Some("prod"));
}

#[test]
fn should_compute_one_to_one_match_table() {
    // given: two 2-series schemas whose label keys line up
    let lhs = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "a"), ("inst", "1")]),
            labels_of(&[("__name__", "a"), ("inst", "2")]),
        ]),
        Arc::from(vec![0u128, 1]),
    );
    let rhs = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "b"), ("inst", "2")]),
            labels_of(&[("__name__", "b"), ("inst", "1")]),
        ]),
        Arc::from(vec![0u128, 1]),
    );
    // when: build a one-to-one match table (default: ignoring on __name__ only)
    let built = build_match_table(&lhs, &rhs, None, false).unwrap();
    // then: inst=1 pairs LHS[0] -> RHS[1]; inst=2 pairs LHS[1] -> RHS[0]
    match built.table {
        MatchTable::OneToOne(map) => {
            assert_eq!(map, vec![Some(1), Some(0)]);
        }
        other => panic!("unexpected table: {other:?}"),
    }
    assert_eq!(built.output_schema.len(), 2);
}

#[test]
fn should_project_output_labels_for_on_matching() {
    // given
    let lhs = SeriesSchema::new(
        Arc::from(vec![labels_of(&[
            ("__name__", "foo"),
            ("env", "prod"),
            ("instance", "i0"),
        ])]),
        Arc::from(vec![0u128]),
    );
    let rhs = SeriesSchema::new(
        Arc::from(vec![labels_of(&[
            ("__name__", "bar"),
            ("env", "prod"),
            ("instance", "i9"),
        ])]),
        Arc::from(vec![1u128]),
    );

    // when
    let built =
        build_one_to_one(&lhs, &rhs, MatchingAxis::On, &["env".to_string()], false).unwrap();

    // then
    assert_eq!(built.output_schema.len(), 1);
    let labels = built.output_schema.labels(0);
    assert_eq!(labels.get("env"), Some("prod"));
    assert_eq!(labels.get("instance"), None);
    assert_eq!(labels.get("__name__"), None);
}

#[test]
fn should_mark_unmatched_series_as_none_in_match_table() {
    // given: LHS has inst=3 not present on RHS
    let lhs = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "a"), ("inst", "1")]),
            labels_of(&[("__name__", "a"), ("inst", "3")]),
        ]),
        Arc::from(vec![0u128, 1]),
    );
    let rhs = SeriesSchema::new(
        Arc::from(vec![labels_of(&[("__name__", "b"), ("inst", "1")])]),
        Arc::from(vec![0u128]),
    );
    // when
    let built = build_match_table(&lhs, &rhs, None, false).unwrap();
    // then: LHS[0] matches RHS[0]; LHS[1] is unmatched (None)
    match built.table {
        MatchTable::OneToOne(map) => assert_eq!(map, vec![Some(0), None]),
        other => panic!("unexpected table: {other:?}"),
    }
}

#[test]
fn should_align_inner_grid_to_multiples_of_inner_step() {
    // `[50s:10s]` at effective_t = 10s produces inner ts aligned to
    // multiples of 10s in `(-40s, 10s]`: {-30, -20, -10, 0, 10}.
    let grid = super::inner_grid(10_000, 50_000, 10_000);
    assert_eq!(grid.start_ms, -30_000);
    assert_eq!(grid.end_ms, 10_000);
    assert_eq!(grid.step_ms, 10_000);
    assert_eq!(grid.step_count, 5);
}

#[test]
fn should_produce_empty_inner_grid_when_range_smaller_than_step() {
    // `[1m:5m]` at effective_t = 12m: window `(11m, 12m]` contains no
    // multiple of 5m, so the inner grid is empty (matches Prometheus
    // `subquery.test:196` expected-empty semantics).
    let grid = super::inner_grid(12 * 60_000, 60_000, 5 * 60_000);
    assert_eq!(grid.step_count, 0);
}

#[test]
fn should_align_inner_grid_on_subquery_offset_window() {
    // `[30s:10s] offset 3s` at outer_t = 1010s shifts effective to
    // 1007s. Window `(977s, 1007s]` → multiples of 10s: {980, 990,
    // 1000}. (`subquery.test:78`.)
    let grid = super::inner_grid(1_007_000, 30_000, 10_000);
    assert_eq!(grid.start_ms, 980_000);
    assert_eq!(grid.end_ms, 1_000_000);
    assert_eq!(grid.step_count, 3);
}

#[test]
fn should_build_subquery_over_one_inner_evaluation() {
    // given: a subquery `foo[3s:1s]` rolled up via rate()
    let source = Arc::new(MockSource::new(vec![(
        labels_of(&[("__name__", "m")]),
        (0..20).map(|i| (i as i64 * 1_000, i as f64)).collect(),
    )]));
    let ctx = LoweringContext::new(0, 5_000, 1_000, 5 * 60_000);
    let plan = LogicalPlan::Rollup {
        kind: crate::promql::operators::rollup::RollupKind::Rate,
        child: Box::new(LogicalPlan::Subquery {
            child: Box::new(LogicalPlan::VectorSelector {
                selector: make_selector("m"),
                offset: Offset::Pos(0),
                at: None,
                lookback_ms: Some(ctx.lookback_delta_ms),
            }),
            range_ms: 3_000,
            step_ms: 1_000,
            offset: Offset::Pos(0),
            at: None,
        }),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();
    // when: build (plan-time only; don't poll the operator tree).
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("subquery physical plan");
    // then: built and output schema is a single-series static ref
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 1);
}

#[test]
fn should_propagate_source_resolve_error_as_plan_error() {
    // given: a source that always errors on resolve
    let source = Arc::new(MockSource::failing("storage down"));
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();
    // when
    let err = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .unwrap_err();
    // then: SourceError with the underlying message
    match err {
        PlanError::SourceError(msg) => assert!(msg.contains("storage down"), "{msg}"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn should_build_rollup_over_matrix_selector_end_to_end() {
    // given: `rate(m[5s])` with an increasing counter
    let source = Arc::new(MockSource::new(vec![(
        labels_of(&[("__name__", "m")]),
        (0..10)
            .map(|i| (i as i64 * 1_000, i as f64 * 10.0))
            .collect(),
    )]));
    let ctx = make_ctx();
    let plan = LogicalPlan::Rollup {
        kind: crate::promql::operators::rollup::RollupKind::Rate,
        child: Box::new(LogicalPlan::MatrixSelector {
            selector: make_selector("m"),
            range_ms: 5_000,
            offset: Offset::Pos(0),
            at: None,
        }),
    };
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();
    // when: build and poll the root once (smoke).
    let mut physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");
    // Drive one poll to check the tree is actually wired.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let _ = physical.root.next(&mut cx);
    // then: output schema is the single input series
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 1);
}

#[test]
fn should_build_nested_aggregate_of_rollup() {
    // given: `sum by (pod) (rate(m[5s]))` over two series
    let source = Arc::new(MockSource::new(vec![
        (
            labels_of(&[("__name__", "m"), ("pod", "a")]),
            (0..10)
                .map(|i| (i as i64 * 1_000, i as f64 * 10.0))
                .collect(),
        ),
        (
            labels_of(&[("__name__", "m"), ("pod", "b")]),
            (0..10)
                .map(|i| (i as i64 * 1_000, i as f64 * 5.0))
                .collect(),
        ),
    ]));
    let ctx = make_ctx();
    let expr = parse("sum by (pod) (rate(m[5s]))");
    let plan = super::super::lowering::lower(&expr, &ctx).unwrap();
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();
    // when
    let physical = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("physical plan");
    // then: output has one series per distinct `pod` — two
    let schema = physical
        .output_schema
        .as_static()
        .expect("static schema")
        .clone();
    assert_eq!(schema.len(), 2);
}

#[test]
fn should_plan_count_values_under_schema_sensitive_parent() {
    // given: `sum(count_values("v", m))` — the Aggregate parent needs a
    // static-schema child.
    let source = Arc::new(MockSource::new(vec![(
        labels_of(&[("__name__", "m")]),
        vec![(0, 1.0)],
    )]));
    let ctx = make_ctx();
    let inner = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let plan = LogicalPlan::Aggregate {
        kind: AggregateKind::Sum,
        child: Box::new(LogicalPlan::CountValues {
            label: "v".to_string(),
            child: Box::new(inner),
            grouping: AggregateGrouping::by_empty(),
        }),
        param: None,
        grouping: AggregateGrouping::by_empty(),
    };
    let reservation = MemoryReservation::new(1 << 20);
    let rt = mk_rt();
    // when
    let plan = rt
        .block_on(build(plan, &source, &reservation, &ctx))
        .expect("count_values is materialised for its parent");
    // then: one group summing the single count_values series
    assert_eq!(
        plan.output_schema.as_static().expect("static schema").len(),
        1
    );
}

#[test]
fn should_build_label_manip_schema_deduplicating_output_labels() {
    // given
    let input = Arc::new(SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "m"), ("src", "a")]),
            labels_of(&[("__name__", "m"), ("src", "b")]),
        ]),
        Arc::from(vec![1u128, 2u128]),
    ));
    let kind = LabelManipKind::Replace {
        dst_label: "src".to_string(),
        replacement: "same".to_string(),
        src_label: "".to_string(),
        regex: "".to_string(),
    };

    // when
    let built = build_label_manip(&kind, &input).unwrap();

    // then
    assert_eq!(built.input_to_output.as_ref(), &[0, 0]);
    assert_eq!(built.output_schema.len(), 1);
    assert_eq!(built.output_schema.labels(0).get("src"), Some("same"));
}

#[test]
fn should_group_left_preserves_include_labels_from_one_side() {
    // given: LHS 2-series sharing `job=web` but with distinct `inst`,
    // RHS 1-series also `job=web` plus a `zone=z` label. Match on
    // `job` (via `ignoring(inst)` which also strips `__name__`) and
    // `group_left(zone)` carries `zone` onto each LHS output.
    let lhs = SeriesSchema::new(
        Arc::from(vec![
            labels_of(&[("__name__", "a"), ("job", "web"), ("inst", "1")]),
            labels_of(&[("__name__", "a"), ("job", "web"), ("inst", "2")]),
        ]),
        Arc::from(vec![0u128, 1]),
    );
    let rhs = SeriesSchema::new(
        Arc::from(vec![labels_of(&[
            ("__name__", "b"),
            ("job", "web"),
            ("zone", "z"),
        ])]),
        Arc::from(vec![0u128]),
    );
    let matching = BinaryMatching {
        axis: MatchingAxis::Ignoring,
        labels: Arc::from(vec!["inst".to_string(), "zone".to_string()]),
        cardinality: Cardinality::GroupLeft {
            include: Arc::from(vec!["zone".to_string()]),
        },
    };
    // when
    let built = build_match_table(&lhs, &rhs, Some(&matching), false).unwrap();
    // then: GroupLeft with both LHS rows mapped to RHS[0], and the
    // output labels carry `zone="z"` from the "one" side.
    match built.table {
        MatchTable::GroupLeft(map) => assert_eq!(map, vec![Some(0), Some(0)]),
        other => panic!("unexpected: {other:?}"),
    }
    let out = built.output_schema;
    assert_eq!(out.len(), 2);
    for i in 0..2 {
        let lbls = out.labels(i as u32);
        assert!(
            lbls.iter().any(|l| l.name == "zone" && l.value == "z"),
            "output row {i} must carry zone=z",
        );
    }
}

// ------------------------------------------------------------------
// Unit 4.5: exchange-operator insertion tests
// ------------------------------------------------------------------

use crate::promql::operators::concurrent::DEFAULT_CHANNEL_BOUND;
use crate::promql::plan::parallelism::Parallelism;

/// Build a mock source with `n` identical single-sample series under
/// metric `m`. Each series gets a distinct `i` label so they pass the
/// selector's roster projection.
fn mock_source_with_n_series(n: usize) -> Arc<MockSource> {
    let series = (0..n)
        .map(|i| {
            (
                labels_of(&[("__name__", "m"), ("i", &i.to_string())]),
                vec![(0, i as f64)],
            )
        })
        .collect();
    Arc::new(MockSource::new(series))
}

async fn build_with_stats<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
) -> Result<(PhysicalPlan, ExchangeStats), PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    build_physical_plan_with_stats(plan, source, reservation.clone(), ctx).await
}

#[test]
fn should_wrap_leaves_above_threshold_in_concurrent() {
    // given: a 128-series selector and the default threshold (64)
    let source = mock_source_with_n_series(128);
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when: build the physical plan with stats
    let (_physical, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: the single leaf is wrapped; no skips recorded
    assert_eq!(stats.concurrent_wrapped, 1);
    assert_eq!(stats.concurrent_skipped, 0);
    assert_eq!(stats.coalesce_inserted, 0);
}

#[test]
fn should_not_wrap_leaves_below_threshold() {
    // given: a 16-series selector and the default threshold (64)
    let source = mock_source_with_n_series(16);
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when
    let (_physical, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: the leaf is skipped; no wraps
    assert_eq!(stats.concurrent_wrapped, 0);
    assert_eq!(stats.concurrent_skipped, 1);
}

#[test]
fn should_leave_intermediate_operators_unwrapped() {
    // given: `sum by (i) (m)` over 128 series — one leaf + one aggregate.
    // The Aggregate is an intermediate CPU-bound op; only the selector
    // leaf should be wrapped.
    let source = mock_source_with_n_series(128);
    let ctx = make_ctx();
    let child = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let plan = LogicalPlan::Aggregate {
        kind: AggregateKind::Sum,
        child: Box::new(child),
        param: None,
        grouping: AggregateGrouping::By(Arc::from(vec!["i".to_string()])),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when
    let (_physical, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: exactly one wrap (the leaf), and exactly one wrap-or-skip
    // decision was recorded (the aggregate is not a wrap candidate, so
    // it doesn't appear in either counter).
    assert_eq!(stats.concurrent_wrapped, 1);
    assert_eq!(stats.concurrent_skipped, 0);
}

#[test]
fn should_use_default_parallelism_when_not_specified() {
    // given: a plain `LoweringContext::new` (no explicit parallelism)
    let ctx = make_ctx();
    // then: parallelism uses the documented defaults
    assert_eq!(
        ctx.parallelism.concurrent_threshold_series,
        Parallelism::DEFAULT_CONCURRENT_THRESHOLD_SERIES,
    );
    assert_eq!(ctx.parallelism.coalesce_max_shards, 0);
}

#[test]
fn should_allow_disabling_concurrent_with_threshold_zero_or_max() {
    // given: two configurations — threshold=0 (wrap every leaf) and
    // threshold=u64::MAX (wrap no leaf). Both use the same 32-series
    // source (between the default's 64 threshold) so the distinction
    // is purely from the knob.
    let source = mock_source_with_n_series(32);
    let plan_fn = || LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(5 * 60_000),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when: threshold = 0 → wrap every leaf (even small ones)
    let ctx_wrap_all = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
        .with_parallelism(Parallelism::new(0, 0, DEFAULT_CHANNEL_BOUND));
    let (_, stats_wrap_all) = rt
        .block_on(build_with_stats(
            plan_fn(),
            &source,
            &reservation,
            &ctx_wrap_all,
        ))
        .expect("physical plan (wrap-all)");

    // when: threshold = u64::MAX → wrap no leaf
    let ctx_wrap_none = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
        .with_parallelism(Parallelism::new(u64::MAX, 0, DEFAULT_CHANNEL_BOUND));
    let (_, stats_wrap_none) = rt
        .block_on(build_with_stats(
            plan_fn(),
            &source,
            &reservation,
            &ctx_wrap_none,
        ))
        .expect("physical plan (wrap-none)");

    // then
    assert_eq!(stats_wrap_all.concurrent_wrapped, 1);
    assert_eq!(stats_wrap_all.concurrent_skipped, 0);
    assert_eq!(stats_wrap_none.concurrent_wrapped, 0);
    assert_eq!(stats_wrap_none.concurrent_skipped, 1);
}

#[test]
fn should_respect_channel_bound() {
    // given: a custom channel bound propagated through Parallelism.
    // We cannot introspect the `ConcurrentOp`'s channel capacity
    // through `dyn Operator`, but we can verify the setting round-trips
    // through `LoweringContext` and that the resulting plan builds
    // (non-zero bounds satisfy `ConcurrentOp::new`'s `assert!(bound > 0)`).
    let source = mock_source_with_n_series(100);
    let ctx = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
        .with_parallelism(Parallelism::new(1, 0, 7));
    assert_eq!(ctx.parallelism.channel_bound, 7);
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();
    // when: build; the ConcurrentOp constructor would panic for bound=0
    let (_, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan (bound=7)");
    // then: leaf wrapped under the threshold=1 setting
    assert_eq!(stats.concurrent_wrapped, 1);
}

#[test]
fn should_skip_coalesce_in_v1() {
    // given: any plan shape — v1 deliberately skips `Coalesce` insertion
    let source = mock_source_with_n_series(256);
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();
    // when
    let (_, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan");
    // then: no coalesce insertions
    assert_eq!(stats.coalesce_inserted, 0);
}

#[test]
fn should_wrap_rollup_leaf_above_threshold() {
    // given: a matrix-selector leaf (wrapped in Rollup) with 200 series.
    // `MatrixSelectorOp::next` is degenerate, so the planner wraps the
    // enclosing `RollupOp` instead — still decouples I/O from
    // evaluation at the right boundary.
    let source = mock_source_with_n_series(200);
    let ctx = make_ctx();
    let plan = LogicalPlan::Rollup {
        kind: crate::promql::operators::rollup::RollupKind::Rate,
        child: Box::new(LogicalPlan::MatrixSelector {
            selector: make_selector("m"),
            range_ms: 5_000,
            offset: Offset::Pos(0),
            at: None,
        }),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when
    let (_, stats) = rt
        .block_on(build_with_stats(plan, &source, &reservation, &ctx))
        .expect("physical plan");

    // then: one wrap (the rollup of the matrix leaf)
    assert_eq!(stats.concurrent_wrapped, 1);
    assert_eq!(stats.concurrent_skipped, 0);
}

// ---- Drift guard: describe_physical vs build_node ------------------
//
// `describe_physical` (in `super::super::explain`) duplicates the
// per-variant dispatch in `build_node`. The risk is that a future
// refactor teaches one path a new trick without touching the other.
// These tests build the real plan against a mock source and then
// assert that the describer's ConcurrentOp node count matches
// `ExchangeStats::concurrent_wrapped` — the only observable side of
// the dispatch divergence we expect to catch here.

fn count_op(node: &super::super::explain::PlanNode, op: &str) -> usize {
    let mut total = if node.op == op { 1 } else { 0 };
    for child in &node.children {
        total += count_op(child, op);
    }
    total
}

#[test]
fn should_describe_physical_agrees_with_build_node_over_threshold() {
    // given: 128 series — selector will be wrapped in ConcurrentOp
    let source = mock_source_with_n_series(128);
    let ctx = make_ctx();
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when: build the real plan and describe the same logical plan
    let (_, stats) = rt
        .block_on(build_with_stats(plan.clone(), &source, &reservation, &ctx))
        .expect("physical plan");
    let described = super::super::explain::describe_physical(&plan, &ctx);

    // then: describer's ConcurrentOp count equals the planner's wrap count
    assert_eq!(
        count_op(&described, "ConcurrentOp"),
        stats.concurrent_wrapped
    );
}

#[test]
fn should_describe_physical_agrees_with_build_node_under_threshold() {
    // given: 16 series and threshold = u64::MAX so no wraps ever happen
    let source = mock_source_with_n_series(16);
    let mut ctx = make_ctx();
    ctx.parallelism = super::super::parallelism::Parallelism::new(u64::MAX, 0, 4);
    let plan = LogicalPlan::VectorSelector {
        selector: make_selector("m"),
        offset: Offset::Pos(0),
        at: None,
        lookback_ms: Some(ctx.lookback_delta_ms),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when: build + describe under the disabled-gate config
    let (_, stats) = rt
        .block_on(build_with_stats(plan.clone(), &source, &reservation, &ctx))
        .expect("physical plan");
    let described = super::super::explain::describe_physical(&plan, &ctx);

    // then: both sides agree — zero wraps, zero describer ConcurrentOp nodes
    assert_eq!(stats.concurrent_wrapped, 0);
    assert_eq!(count_op(&described, "ConcurrentOp"), 0);
}

#[test]
fn should_describe_physical_agrees_with_build_node_for_rollup_of_matrix() {
    // given: rate(m[5m]) over 128 series triggers the matrix-leaf ConcurrentOp wrap
    let source = mock_source_with_n_series(128);
    let ctx = make_ctx();
    let plan = LogicalPlan::Rollup {
        kind: super::super::super::operators::rollup::RollupKind::Rate,
        child: Box::new(LogicalPlan::MatrixSelector {
            selector: make_selector("m"),
            range_ms: 5 * 60_000,
            offset: Offset::Pos(0),
            at: None,
        }),
    };
    let reservation = MemoryReservation::new(1 << 22);
    let rt = mk_rt();

    // when
    let (_, stats) = rt
        .block_on(build_with_stats(plan.clone(), &source, &reservation, &ctx))
        .expect("physical plan");
    let described = super::super::explain::describe_physical(&plan, &ctx);

    // then: the describer places ConcurrentOp at the RollupOp boundary,
    // matching where `build_node` wraps it.
    assert_eq!(stats.concurrent_wrapped, 1);
    assert_eq!(count_op(&described, "ConcurrentOp"), 1);
    // The describer's root must be ConcurrentOp with RollupOp beneath it,
    // mirroring build_node's shape.
    assert_eq!(described.op, "ConcurrentOp");
    assert_eq!(described.children[0].op, "RollupOp");
}
