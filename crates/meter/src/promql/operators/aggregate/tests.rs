use super::*;
use crate::model::{Label, Labels};
use crate::promql::batch::{SchemaRef, SeriesSchema};
use crate::promql::operator::StepGrid;
use std::sync::Arc;
use std::task::Waker;

// ---- waker + mock child -------------------------------------------------

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

struct MockOp {
    schema: OperatorSchema,
    queue: Vec<Result<StepBatch, QueryError>>,
}

impl MockOp {
    fn with_queue(
        schema: Arc<SeriesSchema>,
        grid: StepGrid,
        queue: Vec<Result<StepBatch, QueryError>>,
    ) -> Self {
        Self {
            schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
            queue,
        }
    }

    fn new(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
        Self::with_queue(schema, grid, batches.into_iter().map(Ok).collect())
    }
}

impl Operator for MockOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }
    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.queue.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(self.queue.remove(0)))
        }
    }
}

// ---- fixtures -----------------------------------------------------------

fn mk_schema(prefix: &str, n: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..n)
        .map(|i| {
            Labels::new(vec![
                Label {
                    name: "__name__".to_string(),
                    value: prefix.to_string(),
                },
                Label {
                    name: "i".to_string(),
                    value: i.to_string(),
                },
            ])
        })
        .collect();
    let fps: Vec<u128> = (0..n as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

fn mk_grid(step_count: usize) -> StepGrid {
    StepGrid {
        start_ms: 0,
        end_ms: 10 * (step_count.max(1) as i64 - 1),
        step_ms: 10,
        step_count,
    }
}

fn mk_batch(
    schema: Arc<SeriesSchema>,
    step_count: usize,
    series_count: usize,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    mk_batch_with_series_range(schema, step_count, 0..series_count, values, validity)
}

fn mk_batch_with_series_range(
    schema: Arc<SeriesSchema>,
    step_count: usize,
    series_range: std::ops::Range<usize>,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    let series_count = series_range.len();
    assert_eq!(values.len(), step_count * series_count);
    assert_eq!(validity.len(), step_count * series_count);
    let ts: Arc<[i64]> = Arc::from((0..step_count).map(|i| (i as i64) * 10).collect::<Vec<_>>());
    let mut bits = BitSet::with_len(step_count * series_count);
    for (i, &b) in validity.iter().enumerate() {
        if b {
            bits.set(i);
        }
    }
    StepBatch::new(
        ts,
        0..step_count,
        SchemaRef::Static(schema),
        series_range,
        values,
        bits,
    )
}

fn drive<C: Operator>(op: &mut AggregateOp<C>) -> Vec<Result<StepBatch, QueryError>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(r)) => out.push(r),
            Poll::Pending => panic!("unexpected Pending from sync mock"),
        }
    }
}

fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    (a - b).abs() <= eps * (1.0 + a.abs().max(b.abs()))
}

// ========================================================================
// required tests
// ========================================================================

#[test]
fn should_sum_across_grouped_series_per_step() {
    // given: 4 input series, 2 steps, grouped 2+2 into 2 output groups.
    //   step 0: values [1,2,3,4]  group 0 = s0+s1 = 3; group 1 = s2+s3 = 7
    //   step 1: values [10,20,30,40] group 0 = 30; group 1 = 70
    let in_schema = mk_schema("in", 4);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(2);
    let values = vec![1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0];
    let valid = vec![true; 8];
    let batch = mk_batch(in_schema.clone(), 2, 4, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .expect("operator constructs");
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.step_count(), 2);
    assert_eq!(b.series_count(), 2);
    assert_eq!(b.get(0, 0), Some(3.0));
    assert_eq!(b.get(0, 1), Some(7.0));
    assert_eq!(b.get(1, 0), Some(30.0));
    assert_eq!(b.get(1, 1), Some(70.0));
}

#[test]
fn should_compute_avg_ignoring_invalid_cells() {
    // given: 3 input series in one group. Cell validity pattern:
    //   step 0: [v,v,_]  values 2,4,99 → mean = (2+4)/2 = 3
    //   step 1: [_,_,_]  validity all 0 → output absent
    //   step 2: [v,v,v]  values 5,10,15 → mean = 10
    let in_schema = mk_schema("in", 3);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(3);
    let values = vec![2.0, 4.0, 99.0, 0.0, 0.0, 0.0, 5.0, 10.0, 15.0];
    let valid = vec![true, true, false, false, false, false, true, true, true];
    let batch = mk_batch(in_schema.clone(), 3, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(0)], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Avg,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &outs[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 3.0, 1e-12));
    assert_eq!(b.get(1, 0), None);
    assert!(approx_eq(b.get(2, 0).unwrap(), 10.0, 1e-12));
}

#[test]
fn should_compute_min_and_max_ignoring_nan() {
    // given: 4 series, one group.
    //   step 0: NaN, 5, 3, NaN → min=3, max=5
    //   step 1: NaN, NaN, NaN, NaN → all NaN. The group still has
    //       "count>0 valid contributions" because validity bits are
    //       set; output min/max = NaN (Prometheus: preserves NaN when
    //       nothing else is seen).
    let in_schema = mk_schema("in", 4);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(2);
    let values = vec![
        f64::NAN,
        5.0,
        3.0,
        f64::NAN,
        f64::NAN,
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ];
    let valid = vec![true; 8];
    let batch = mk_batch(in_schema.clone(), 2, 4, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 4], 1);

    let mut op_min = AggregateOp::new(
        child,
        AggregateKind::Min,
        gmap.clone(),
        out_schema.clone(),
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op_min).into_iter().map(|r| r.unwrap()).collect();
    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(3.0));
    // step 1 had all NaN contributions — output is NaN with validity=1
    assert!(b.validity.get(b.cell_index(1, 0)));
    assert!(b.values[b.cell_index(1, 0)].is_nan());

    // max
    let in_schema = mk_schema("in", 4);
    let grid2 = mk_grid(2);
    let values = vec![
        f64::NAN,
        5.0,
        3.0,
        f64::NAN,
        f64::NAN,
        f64::NAN,
        f64::NAN,
        f64::NAN,
    ];
    let valid = vec![true; 8];
    let batch2 = mk_batch(in_schema.clone(), 2, 4, values, valid);
    let child2 = MockOp::new(in_schema, grid2, vec![batch2]);
    let mut op_max = AggregateOp::new(
        child2,
        AggregateKind::Max,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op_max).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(outs[0].get(0, 0), Some(5.0));
}

#[test]
fn should_count_valid_cells_per_group() {
    // given: 3 series, 2 groups (s0,s1 → g0; s2 → g1), 2 steps
    //   step 0: [v,v,v] → counts g0=2, g1=1
    //   step 1: [_,v,_] → counts g0=1, g1 absent (validity=0)
    let in_schema = mk_schema("in", 3);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(2);
    let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
    let valid = vec![true, true, true, false, true, false];
    let batch = mk_batch(in_schema.clone(), 2, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1)], 2);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Count,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(2.0));
    assert_eq!(b.get(0, 1), Some(1.0));
    assert_eq!(b.get(1, 0), Some(1.0));
    assert_eq!(b.get(1, 1), None);
}

#[test]
fn should_compute_stddev_and_stdvar_with_welford() {
    // given: 8 series, one group, values 2,4,4,4,5,5,7,9
    //   population variance = 4, stddev = 2.
    let in_schema = mk_schema("in", 8);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    let valid = vec![true; 8];
    let batch = mk_batch(in_schema.clone(), 1, 8, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 8], 1);

    let mut op_var = AggregateOp::new(
        child,
        AggregateKind::Stdvar,
        gmap.clone(),
        out_schema.clone(),
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op_var).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(outs[0].get(0, 0).unwrap(), 4.0, 1e-12));

    // and: stddev
    let in_schema = mk_schema("in", 8);
    let grid2 = mk_grid(1);
    let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    let valid = vec![true; 8];
    let batch = mk_batch(in_schema.clone(), 1, 8, values, valid);
    let child2 = MockOp::new(in_schema, grid2, vec![batch]);
    let mut op_stddev = AggregateOp::new(
        child2,
        AggregateKind::Stddev,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op_stddev)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert!(approx_eq(outs[0].get(0, 0).unwrap(), 2.0, 1e-12));
}

#[test]
fn should_emit_group_one_when_any_input_contributed() {
    // given: 3 series, 2 groups (s0→g0, s1,s2→g1). Step 0 valid only
    // in s0 and s2 → g0 has 1 contribution, g1 has 1 contribution,
    // both emit 1.0.
    let in_schema = mk_schema("in", 3);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let values = vec![42.0, 0.0, 7.0];
    let valid = vec![true, false, true];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(1), Some(1)], 2);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Group,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(outs[0].get(0, 0), Some(1.0));
    assert_eq!(outs[0].get(0, 1), Some(1.0));
}

#[test]
fn should_set_validity_zero_when_group_empty() {
    // given: 2 series → 1 group; step has all inputs invalid.
    let out_schema = mk_schema("out", 1);

    for kind in [
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::Count,
        AggregateKind::Stddev,
        AggregateKind::Stdvar,
        AggregateKind::Group,
    ] {
        // Rebuild everything since operator + child are consumed.
        let in_schema = mk_schema("in", 2);
        let grid = mk_grid(1);
        let batch = mk_batch(
            in_schema.clone(),
            1,
            2,
            vec![99.0, 99.0],
            vec![false, false],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

        let mut op = AggregateOp::new(
            child,
            kind,
            gmap,
            out_schema.clone(),
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs[0].get(0, 0), None, "kind={kind:?}");
    }
}

#[test]
fn should_handle_by_empty_single_group() {
    // given: 5 series → 1 group (all map to 0). `sum by ()` semantics.
    let in_schema = mk_schema("in", 5);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let valid = vec![true; 5];
    let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(outs[0].series_count(), 1);
    assert_eq!(outs[0].get(0, 0), Some(15.0));
}

#[test]
fn should_handle_without_degenerate_each_series_its_own_group() {
    // given: `sum without ()` — every input series is its own group.
    let in_schema = mk_schema("in", 3);
    let out_schema = mk_schema("out", 3);
    let grid = mk_grid(1);
    let values = vec![7.0, 11.0, 13.0];
    let valid = vec![true; 3];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2)], 3);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    assert_eq!(b.get(0, 0), Some(7.0));
    assert_eq!(b.get(0, 1), Some(11.0));
    assert_eq!(b.get(0, 2), Some(13.0));
}

#[test]
fn should_span_multiple_step_tile_batches() {
    // given: child emits two step-tile batches covering disjoint step
    // ranges of the same outer grid. Batch A = step 0 (values 1, 2),
    // batch B = step 1 (values 10, 20). Both tile into the same
    // output group.
    //
    // After the tile-boundary fix, streaming aggregate is a
    // step-bounded breaker — it buffers the whole grid and emits a
    // single output batch covering every step. Expected output: one
    // batch, step 0 sum = 3, step 1 sum = 30.
    let in_schema = mk_schema("in", 2);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(2);
    let ts: Arc<[i64]> = Arc::from(vec![0i64, 10].into_boxed_slice());

    // Build the two tiles with disjoint step_range — the mk_batch
    // helper always uses `0..step_count`, so assemble the batch
    // directly to get `step_range = 1..2` for batch B.
    let mut bits_a = BitSet::with_len(2);
    bits_a.set(0);
    bits_a.set(1);
    let batch_a = StepBatch::new(
        ts.clone(),
        0..1,
        SchemaRef::Static(in_schema.clone()),
        0..2,
        vec![1.0, 2.0],
        bits_a,
    );
    let mut bits_b = BitSet::with_len(2);
    bits_b.set(0);
    bits_b.set(1);
    let batch_b = StepBatch::new(
        ts,
        1..2,
        SchemaRef::Static(in_schema.clone()),
        0..2,
        vec![10.0, 20.0],
        bits_b,
    );

    let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
    let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.step_count(), 2);
    assert_eq!(b.get(0, 0), Some(3.0));
    assert_eq!(b.get(1, 0), Some(30.0));
}

#[test]
fn should_use_absolute_series_indices_for_streaming_batches() {
    // given: the child publishes only the tail slice of a 4-series roster.
    // The group map still indexes the full roster, so the operator must
    // offset by `input.series_range.start` before looking up groups.
    let in_schema = mk_schema("in", 4);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let batch =
        mk_batch_with_series_range(in_schema.clone(), 1, 2..4, vec![3.0, 5.0], vec![true, true]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    assert_eq!(outs[0].get(0, 0), None);
    assert_eq!(outs[0].get(0, 1), Some(8.0));
}

#[test]
fn should_respect_memory_reservation() {
    // given: a tiny reservation that the accumulator alone doesn't
    // fit into.
    let in_schema = mk_schema("in", 4);
    let out_schema = mk_schema("out", 4);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 4, vec![1.0; 4], vec![true; 4]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2), Some(3)], 4);

    // Cap too small even for the per-group scratch.
    let result = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1),
    );
    match result {
        Err(QueryError::MemoryLimit { .. }) => {}
        _ => panic!("expected MemoryLimit on constructor"),
    }
}

#[test]
fn should_return_static_schema() {
    // given: a minimal aggregate.
    let in_schema = mk_schema("in", 2);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

    let op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();

    // when/then
    assert!(!op.schema().series.is_deferred());
    assert!(op.schema().series.as_static().is_some());
    assert_eq!(
        op.schema().series.as_static().unwrap().len(),
        1,
        "output schema must equal group_count",
    );
}

#[test]
fn should_propagate_error_from_upstream() {
    let in_schema = mk_schema("in", 1);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let child = MockOp::with_queue(
        in_schema,
        grid,
        vec![Err(QueryError::MemoryLimit {
            requested: 10,
            cap: 0,
            already_reserved: 0,
        })],
    );
    let gmap = GroupMap::new(vec![Some(0)], 1);
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let results = drive(&mut op);
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
}

#[test]
fn should_handle_child_end_of_stream() {
    // given: child emits no batches.
    let in_schema = mk_schema("in", 1);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let child = MockOp::new(in_schema, grid, vec![]);
    let gmap = GroupMap::new(vec![Some(0)], 1);
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let results = drive(&mut op);
    assert!(results.is_empty());
}

/// Scripted child action for the [`PendingMockOp`] reproducer: either
/// yield a batch on this poll, return `Poll::Pending` once (mirroring a
/// `Concurrent` exchange with an empty channel), or signal EOS.
enum Action {
    Batch(StepBatch),
    Pending,
    Eos,
}

/// Mock child that interleaves `Poll::Pending` with batches and EOS.
/// Each `Pending` response self-wakes via the context waker so the
/// [`drive_with_pending`] driver re-polls immediately, emulating the
/// pattern produced by `ConcurrentOp` under non-trivial schedules
/// (see trace in RFC 0008 stress harness).
struct PendingMockOp {
    schema: OperatorSchema,
    actions: std::collections::VecDeque<Action>,
}

impl PendingMockOp {
    fn new(schema: Arc<SeriesSchema>, grid: StepGrid, actions: Vec<Action>) -> Self {
        Self {
            schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
            actions: actions.into(),
        }
    }
}

impl Operator for PendingMockOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }
    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        match self.actions.pop_front() {
            Some(Action::Batch(b)) => Poll::Ready(Some(Ok(b))),
            Some(Action::Pending) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Action::Eos) | None => Poll::Ready(None),
        }
    }
}

/// Drive an operator that may return `Pending`, polling again on each
/// `Pending` until `Ready(None)`.
fn drive_with_pending<C: Operator>(op: &mut AggregateOp<C>) -> Vec<Result<StepBatch, QueryError>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(r)) => out.push(r),
            Poll::Pending => continue,
        }
    }
}

#[test]
fn should_finalise_streaming_aggregate_when_child_interleaves_pending() {
    // given: 2 input series grouped into 1 group over 1 step.
    //   Child emits: [Batch(s0=3), Pending, Batch(s1=4), Pending, Eos]
    //   Each Pending forces AggregateOp::next to return Pending and
    //   be re-entered — under the bug, the local `saw_any_batch`
    //   resets on every re-entry, so the final `Eos` poll is entered
    //   with `saw_any_batch=false` and `finalise_streaming` is
    //   skipped, yielding an empty stream even though the grid has
    //   accumulated 3+4=7.
    let in_schema = mk_schema("in", 2);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let b1 = mk_batch_with_series_range(in_schema.clone(), 1, 0..1, vec![3.0], vec![true]);
    let b2 = mk_batch_with_series_range(in_schema.clone(), 1, 1..2, vec![4.0], vec![true]);
    let child = PendingMockOp::new(
        in_schema,
        grid,
        vec![
            Action::Batch(b1),
            Action::Pending,
            Action::Batch(b2),
            Action::Pending,
            Action::Eos,
        ],
    );
    let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive_with_pending(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: one batch with the grouped sum s0+s1 = 7.
    assert_eq!(outs.len(), 1, "aggregate emitted empty stream");
    assert_eq!(outs[0].get(0, 0), Some(7.0));
}

#[test]
fn should_finalise_breaker_aggregate_when_child_interleaves_pending() {
    // given: the same scripted Pending interleaving but for a
    // breaker-kind aggregate (topk). The bug at `saw_any_batch` in
    // the breaker branch mirrors the streaming path.
    let in_schema = mk_schema("in", 2);
    let grid = mk_grid(1);
    let b1 = mk_batch_with_series_range(in_schema.clone(), 1, 0..1, vec![3.0], vec![true]);
    let b2 = mk_batch_with_series_range(in_schema.clone(), 1, 1..2, vec![4.0], vec![true]);
    let child = PendingMockOp::new(
        in_schema.clone(),
        grid,
        vec![
            Action::Batch(b1),
            Action::Pending,
            Action::Batch(b2),
            Action::Pending,
            Action::Eos,
        ],
    );
    // one group containing both series — topk(1) picks the larger.
    let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(1),
        gmap,
        in_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive_with_pending(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: one batch with the top-1 series (s1 = 4.0).
    assert_eq!(outs.len(), 1, "breaker aggregate emitted empty stream");
    // topk(1) over [3.0, 4.0] → 4.0 survives at its original slot.
    let observed: Vec<Option<f64>> = (0..outs[0].series_count())
        .map(|s| outs[0].get(0, s))
        .collect();
    assert!(
        observed.contains(&Some(4.0)),
        "expected topk winner 4.0 in output, got {:?}",
        observed
    );
}

#[test]
fn should_drop_inputs_with_none_group_assignment() {
    // given: 3 series, 2 groups; s1 has `None` assignment (dropped).
    //   step 0: [1, 99, 2] → g0 sums s0=1, g1 sums s2=2 (s1 dropped)
    let in_schema = mk_schema("in", 3);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let values = vec![1.0, 99.0, 2.0];
    let valid = vec![true; 3];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), None, Some(1)], 2);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(outs[0].get(0, 0), Some(1.0));
    assert_eq!(outs[0].get(0, 1), Some(2.0));
}

#[test]
fn should_aggregate_across_multiple_series_tile_batches_for_same_step_range() {
    // given: the child emits two batches covering the SAME step range
    // (0..2) but disjoint series tiles (0..3 then 3..5), mirroring the
    // (step_tile × series_tile) emission pattern of `VectorSelectorOp`
    // when the resolved roster exceeds the default series tile width.
    // Every cell has value 1.0 — the correct total sum per step is 5.0.
    let in_schema = mk_schema("in", 5);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(2);

    let batch_tile_a =
        mk_batch_with_series_range(in_schema.clone(), 2, 0..3, vec![1.0; 6], vec![true; 6]);
    let batch_tile_b =
        mk_batch_with_series_range(in_schema.clone(), 2, 3..5, vec![1.0; 4], vec![true; 4]);

    let child = MockOp::new(in_schema, grid, vec![batch_tile_a, batch_tile_b]);
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .expect("operator constructs");
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: the aggregate must produce one logical answer per step — a
    // single cell of 5.0 per step (one batch) or, if it emits multiple
    // batches for the same step range, the values across those batches
    // must *not* be partial per-tile sums that a downstream consumer
    // would render as duplicate/arbitrary timestamps. Assert both the
    // single-batch shape and the correct per-step total.
    assert_eq!(
        outs.len(),
        1,
        "expected a single output batch covering steps 0..2, got {}",
        outs.len(),
    );
    let b = &outs[0];
    assert_eq!(b.step_count(), 2);
    assert_eq!(b.series_count(), 1);
    assert_eq!(
        b.get(0, 0),
        Some(5.0),
        "step 0 sum must be 5.0 (3 + 2 tiles)"
    );
    assert_eq!(
        b.get(1, 0),
        Some(5.0),
        "step 1 sum must be 5.0 (3 + 2 tiles)"
    );
}

#[test]
fn should_aggregate_many_series_through_default_tile_shape() {
    // given: end-to-end shape reproducing the production symptom —
    // 1500 input series across 3 groups, 4 steps, with the child
    // emitting one batch per (step_tile × series_tile) using the
    // default `series_chunk = 512`. Every valid cell contributes 1.0;
    // correct `sum by (group)` per step is therefore 500.
    const SERIES: usize = 1500;
    const GROUPS: usize = 3;
    const STEPS: usize = 4;
    const SERIES_CHUNK: usize = 512;
    // Split steps into arbitrary step-tiles too (2 + 2) to exercise
    // the (step_tile × series_tile) cross product.
    const STEP_TILES: &[std::ops::Range<usize>] = &[0..2, 2..4];

    let in_schema = mk_schema("in", SERIES);
    let out_schema = mk_schema("out", GROUPS);
    let grid = mk_grid(STEPS);
    let ts: Arc<[i64]> = Arc::from(
        (0..STEPS)
            .map(|i| (i as i64) * 10)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );

    // Round-robin groups (s0→g0, s1→g1, s2→g2, s3→g0, ...). With
    // SERIES=1500 each group gets exactly 500 series.
    let group_assignments: Vec<Option<u32>> =
        (0..SERIES).map(|s| Some((s % GROUPS) as u32)).collect();

    // Assemble the batches: for every step tile × series tile, emit
    // one batch with values=1.0 and validity=1. Series tiling uses
    // `SERIES_CHUNK` matching `VectorSelectorOp`'s default.
    let mut batches: Vec<StepBatch> = Vec::new();
    for step_tile in STEP_TILES {
        let step_count_tile = step_tile.len();
        let mut series_start = 0usize;
        while series_start < SERIES {
            let series_end = (series_start + SERIES_CHUNK).min(SERIES);
            let series_count_tile = series_end - series_start;
            let cells = step_count_tile * series_count_tile;
            let mut bits = BitSet::with_len(cells);
            for i in 0..cells {
                bits.set(i);
            }
            batches.push(StepBatch::new(
                ts.clone(),
                step_tile.clone(),
                SchemaRef::Static(in_schema.clone()),
                series_start..series_end,
                vec![1.0; cells],
                bits,
            ));
            series_start = series_end;
        }
    }

    let child = MockOp::new(in_schema, grid, batches);
    let gmap = GroupMap::new(group_assignments, GROUPS);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Sum,
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .expect("operator constructs");
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one output batch, 4 steps × 3 groups, each cell = 500.
    assert_eq!(outs.len(), 1, "expected one aggregated output batch");
    let b = &outs[0];
    assert_eq!(b.step_count(), STEPS);
    assert_eq!(b.series_count(), GROUPS);
    for step in 0..STEPS {
        for group in 0..GROUPS {
            assert_eq!(
                b.get(step, group),
                Some(500.0),
                "step={step} group={group} expected 500",
            );
        }
    }
}

// ========================================================================
// 3c.1 breaker tests — topk / bottomk / quantile
// ========================================================================

#[test]
fn should_select_top_k_values_per_group() {
    // given: 5 input series in one group. Values 1..=5. topk(2) → two
    // largest values (4, 5) selected; other cells validity=0.
    let in_schema = mk_schema("in", 5);
    // Output schema for topk = input schema shape (5 series).
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let valid = vec![true; 5];
    let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .expect("operator constructs");
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: output preserves the 5-series shape; only s3 and s4 valid.
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.series_count(), 5);
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), None);
    assert_eq!(b.get(0, 3), Some(4.0));
    assert_eq!(b.get(0, 4), Some(5.0));
}

#[test]
fn should_select_bottom_k_values_per_group() {
    // given: 5 input series in one group. Values 1..=5. bottomk(2) →
    // two smallest (1, 2); other cells validity=0.
    let in_schema = mk_schema("in", 5);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let valid = vec![true; 5];
    let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Bottomk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(1.0));
    assert_eq!(b.get(0, 1), Some(2.0));
    assert_eq!(b.get(0, 2), None);
    assert_eq!(b.get(0, 3), None);
    assert_eq!(b.get(0, 4), None);
}

#[test]
fn should_select_all_when_k_ge_group_size() {
    // given: 3 series in one group; topk(10) → every valid input
    // selected (K ≥ group size).
    let in_schema = mk_schema("in", 3);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![7.0, 3.0, 9.0];
    let valid = vec![true; 3];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 3], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(10),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(7.0));
    assert_eq!(b.get(0, 1), Some(3.0));
    assert_eq!(b.get(0, 2), Some(9.0));
}

#[test]
fn should_select_nothing_when_k_zero() {
    // given: 3 valid series; topk(0) ⇒ empty selection; every output
    // cell has validity=0.
    let in_schema = mk_schema("in", 3);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![7.0, 3.0, 9.0];
    let valid = vec![true; 3];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 3], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(0),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), None);
}

#[test]
fn should_handle_negative_k_as_empty() {
    // given: Negative K ⇒ no output cells selected (matches
    // `evaluator.rs::coerce_k_size`).
    let in_schema = mk_schema("in", 3);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![7.0, 3.0, 9.0];
    let valid = vec![true; 3];
    let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 3], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Bottomk(-5),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), None);
}

#[test]
fn should_ignore_nan_inputs_in_topk() {
    // given: 5 valid inputs; s0 and s4 are NaN. topk(2) on real
    // values picks the two largest reals (s2=3, s3=4). NaNs rank
    // "worst" per the engine's `compare_k_values` and are only
    // selected when K exceeds the non-NaN count.
    let in_schema = mk_schema("in", 5);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![f64::NAN, 2.0, 3.0, 4.0, f64::NAN];
    let valid = vec![true; 5];
    let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), Some(3.0));
    assert_eq!(b.get(0, 3), Some(4.0));
    assert_eq!(b.get(0, 4), None);
}

#[test]
fn should_tiebreak_topk_deterministically_by_series_index() {
    // given: 4 series all valued 5.0; topk(2) must pick 2. The
    // engine's tie-break (`evaluator.rs:687-694`) breaks ties by
    // lower index wins — so s0 and s1 are the survivors.
    let in_schema = mk_schema("in", 4);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let values = vec![5.0; 4];
    let valid = vec![true; 4];
    let batch = mk_batch(in_schema.clone(), 1, 4, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 4], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(5.0));
    assert_eq!(b.get(0, 1), Some(5.0));
    assert_eq!(b.get(0, 2), None);
    assert_eq!(b.get(0, 3), None);
}

#[test]
fn should_use_absolute_series_indices_for_topk_batches() {
    // given: a filter-shaped topk over the tail slice of a 6-series roster.
    // The global lower-index tie-break and output `series_range` must both
    // be based on the child's absolute roster position, not batch-local 0..n.
    // The breaker kinds emit a single filter-shape output covering
    // the full input roster, so the selected cells land at their
    // global indices (series 3 and 4) and the leading 0..3 cells
    // are all invalid.
    let in_schema = mk_schema("in", 6);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let batch = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        3..6,
        vec![5.0, 5.0, 1.0],
        vec![true, true, true],
    );
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 6], 1);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.series_range, 0..6);
    // Leading slots (0..3) were never emitted by the child — invalid.
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), None);
    // Global series 3 and 4 tie at value 5.0 and are selected
    // (lower-index tie-break keeps the first two); series 5 has
    // value 1.0 and drops out.
    assert_eq!(b.get(0, 3), Some(5.0));
    assert_eq!(b.get(0, 4), Some(5.0));
    assert_eq!(b.get(0, 5), None);
}

#[test]
fn should_compute_quantile_with_linear_interpolation() {
    // given: 5 input series in one group, values 1..=5.
    //   q=0.5 → median = 3.0
    //   q=0.75 → between 4 and 5 → rank = 0.75 * 4 = 3.0 → exactly 4.0
    //   q=0.25 → rank 1.0 → exactly 2.0
    for (q, expected) in [(0.5_f64, 3.0_f64), (0.75, 4.0), (0.25, 2.0)] {
        let in_schema = mk_schema("in", 5);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let valid = vec![true; 5];
        let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Quantile(q),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let b = &outs[0];
        assert!(
            approx_eq(b.get(0, 0).unwrap(), expected, 1e-12),
            "q={q}: got {:?} expected {expected}",
            b.get(0, 0),
        );
    }
}

#[test]
fn should_handle_quantile_out_of_range() {
    // given: matches rollup_fns::quantile — q<0 → -inf, q>1 → +inf.
    for (q, expected) in [(-0.5_f64, f64::NEG_INFINITY), (1.5, f64::INFINITY)] {
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 3], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Quantile(q),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let b = &outs[0];
        let got = b.get(0, 0).expect("valid when group non-empty");
        assert_eq!(got, expected, "q={q}");
    }
}

#[test]
fn should_compute_quantile_per_group() {
    // given: 6 series, 2 groups (s0..s2 → g0; s3..s5 → g1).
    //   group 0 values: 1, 2, 3 → q=0.5 median = 2.0
    //   group 1 values: 10, 20, 30 → q=0.5 median = 20.0
    let in_schema = mk_schema("in", 6);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let values = vec![1.0, 2.0, 3.0, 10.0, 20.0, 30.0];
    let valid = vec![true; 6];
    let batch = mk_batch(in_schema.clone(), 1, 6, values, valid);

    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(
        vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)],
        2,
    );

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Quantile(0.5),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 2.0, 1e-12));
    assert!(approx_eq(b.get(0, 1).unwrap(), 20.0, 1e-12));
}

#[test]
fn should_use_absolute_series_indices_for_quantile_batches() {
    // given: only the tail slice of a 5-series roster is present in this
    // batch; quantile must still bucket values by their absolute series ids.
    let in_schema = mk_schema("in", 5);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let batch = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        2..5,
        vec![7.0, 11.0, 13.0],
        vec![true, true, true],
    );
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(0), Some(1), Some(1)], 2);

    // when
    let mut op = AggregateOp::new(
        child,
        AggregateKind::Quantile(0.5),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(7.0));
    assert_eq!(b.get(0, 1), Some(12.0));
}

#[test]
fn should_respect_memory_reservation_for_topk_heap() {
    // given: 4 input series, K=2, but reservation only holds 1 byte
    // — nowhere near the per-group heap scratch.
    let in_schema = mk_schema("in", 4);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 4, vec![1.0; 4], vec![true; 4]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0); 4], 1);

    let result = AggregateOp::new(
        child,
        AggregateKind::Topk(2),
        gmap,
        out_schema,
        MemoryReservation::new(1),
    );
    match result {
        Err(QueryError::MemoryLimit { .. }) => {}
        _ => panic!("expected MemoryLimit on constructor"),
    }
}

#[test]
fn should_preserve_input_schema_for_topk_bottomk() {
    // given: the operator's published schema for topk/bottomk is the
    // input shape (filter-semantics), not group_count.
    let in_schema = mk_schema("in", 5);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 5, vec![1.0; 5], vec![true; 5]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    // 5 inputs into 1 group; topk preserves 5 output series.
    let gmap = GroupMap::new(vec![Some(0); 5], 1);

    let op = AggregateOp::new(
        child,
        AggregateKind::Topk(3),
        gmap,
        out_schema.clone(),
        MemoryReservation::new(1 << 20),
    )
    .unwrap();

    // when/then
    let static_schema = op
        .schema()
        .series
        .as_static()
        .expect("topk publishes a static schema");
    assert_eq!(static_schema.len(), 5);
}

#[test]
fn should_use_groups_schema_for_quantile() {
    // given: quantile is a reducer — output series = group_count.
    let in_schema = mk_schema("in", 6);
    let out_schema = mk_schema("out", 2);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 6, vec![1.0; 6], vec![true; 6]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(
        vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)],
        2,
    );

    let op = AggregateOp::new(
        child,
        AggregateKind::Quantile(0.5),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();

    let static_schema = op
        .schema()
        .series
        .as_static()
        .expect("quantile publishes a static schema");
    assert_eq!(static_schema.len(), 2);
}

// ========================================================================
// 6.3.9 — breaker tile-boundary stress (topk / bottomk / quantile)
// ========================================================================

#[test]
fn should_select_topk_globally_across_multiple_series_tile_batches() {
    // given: 6 input series in one group with values 1..=6, emitted
    // as two series-tile batches (0..3 and 3..6) covering the same
    // step range — same shape `VectorSelectorOp` produces when the
    // roster exceeds the default 512-series tile. topk(3) must
    // pick the three globally-largest (values 4, 5, 6), not the
    // per-tile top-3.
    let in_schema = mk_schema("in", 6);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let batch_a = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        0..3,
        vec![1.0, 2.0, 3.0],
        vec![true, true, true],
    );
    let batch_b = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        3..6,
        vec![4.0, 5.0, 6.0],
        vec![true, true, true],
    );
    let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
    let gmap = GroupMap::new(vec![Some(0); 6], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Topk(3),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .expect("operator constructs");
    let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

    // then: reassemble the filter-shape output into a global series ×
    // step matrix and assert only global series 3, 4, 5 are selected.
    // Multiple output batches are allowed (one per input tile was the
    // old shape); the invariant checked is cell-level.
    let mut valid: [Option<f64>; 6] = [None; 6];
    for r in outs {
        let b = r.unwrap();
        for s in 0..b.series_count() {
            let gs = b.series_range.start + s;
            if b.validity.get(s) {
                valid[gs] = Some(b.values[s]);
            }
        }
    }
    assert_eq!(valid[0], None, "series 0 not in top-3 globally");
    assert_eq!(valid[1], None, "series 1 not in top-3 globally");
    assert_eq!(valid[2], None, "series 2 not in top-3 globally");
    assert_eq!(valid[3], Some(4.0));
    assert_eq!(valid[4], Some(5.0));
    assert_eq!(valid[5], Some(6.0));
}

#[test]
fn should_select_bottomk_globally_across_multiple_series_tile_batches() {
    // given: same shape as the topk test; bottomk(3) must pick the
    // three globally-smallest values (1, 2, 3 — all in tile A).
    let in_schema = mk_schema("in", 6);
    let out_schema = in_schema.clone();
    let grid = mk_grid(1);
    let batch_a = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        0..3,
        vec![1.0, 2.0, 3.0],
        vec![true, true, true],
    );
    let batch_b = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        3..6,
        vec![4.0, 5.0, 6.0],
        vec![true, true, true],
    );
    let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
    let gmap = GroupMap::new(vec![Some(0); 6], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Bottomk(3),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

    let mut valid: [Option<f64>; 6] = [None; 6];
    for r in outs {
        let b = r.unwrap();
        for s in 0..b.series_count() {
            let gs = b.series_range.start + s;
            if b.validity.get(s) {
                valid[gs] = Some(b.values[s]);
            }
        }
    }
    assert_eq!(valid[0], Some(1.0));
    assert_eq!(valid[1], Some(2.0));
    assert_eq!(valid[2], Some(3.0));
    assert_eq!(valid[3], None);
    assert_eq!(valid[4], None);
    assert_eq!(valid[5], None);
}

#[test]
fn should_compute_quantile_globally_across_multiple_series_tile_batches() {
    // given: 6 input series in one group, values 1..=6 split across
    // two tiles. median (q=0.5) over 1..=6 = 3.5; a per-tile median
    // path would compute 2.0 (tile A) and 5.0 (tile B) and emit two
    // cells for the one group — wrong both in value and in
    // duplicate-coverage.
    let in_schema = mk_schema("in", 6);
    let out_schema = mk_schema("out", 1);
    let grid = mk_grid(1);
    let batch_a = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        0..3,
        vec![1.0, 2.0, 3.0],
        vec![true, true, true],
    );
    let batch_b = mk_batch_with_series_range(
        in_schema.clone(),
        1,
        3..6,
        vec![4.0, 5.0, 6.0],
        vec![true, true, true],
    );
    let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
    let gmap = GroupMap::new(vec![Some(0); 6], 1);

    let mut op = AggregateOp::new(
        child,
        AggregateKind::Quantile(0.5),
        gmap,
        out_schema,
        MemoryReservation::new(1 << 20),
    )
    .unwrap();
    let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

    // then: exactly one output batch / one cell carrying the global
    // median 3.5. Reducer-shape output (one cell per group per step).
    let batches: Vec<StepBatch> = outs.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        batches.len(),
        1,
        "expected one output batch covering one group-cell, got {}",
        batches.len(),
    );
    let b = &batches[0];
    assert_eq!(b.series_count(), 1);
    assert_eq!(b.step_count(), 1);
    assert_eq!(b.get(0, 0), Some(3.5));
}
