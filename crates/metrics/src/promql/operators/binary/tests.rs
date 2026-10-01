use super::*;
use crate::model::{Label, Labels};
use std::sync::Arc;
use std::task::Waker;

// ---- helpers ------------------------------------------------------------

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

fn mk_labels(prefix: &str, i: usize) -> Labels {
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
}

fn mk_schema(prefix: &str, n: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..n).map(|i| mk_labels(prefix, i)).collect();
    let fps: Vec<u128> = (0..n as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

fn mk_schema_single() -> Arc<SeriesSchema> {
    let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
    let fps: Arc<[u128]> = Arc::from(vec![0u128]);
    Arc::new(SeriesSchema::new(labels, fps))
}

fn mk_grid(step_count: usize) -> StepGrid {
    StepGrid {
        start_ms: 1_000,
        end_ms: 1_000 + ((step_count as i64 - 1).max(0)) * 1_000,
        step_ms: 1_000,
        step_count,
    }
}

/// Build a StepBatch from flat row-major values + parallel validity.
fn mk_batch(
    schema: Arc<SeriesSchema>,
    step_count: usize,
    series_count: usize,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    assert_eq!(values.len(), step_count * series_count);
    assert_eq!(validity.len(), values.len());
    let ts: Arc<[i64]> = Arc::from(
        (0..step_count)
            .map(|i| 1_000 + (i as i64) * 1_000)
            .collect::<Vec<_>>(),
    );
    let mut bits = BitSet::with_len(validity.len());
    for (i, &b) in validity.iter().enumerate() {
        if b {
            bits.set(i);
        }
    }
    StepBatch::new(
        ts,
        0..step_count,
        SchemaRef::Static(schema),
        0..series_count,
        values,
        bits,
    )
}

/// Mock operator yielding a scripted queue of batches.
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

fn drive<L: Operator, R: Operator>(op: &mut BinaryOp<L, R>) -> Vec<Result<StepBatch, QueryError>> {
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

// ========================================================================
// required tests
// ========================================================================

#[test]
fn should_apply_add_vector_vector_one_to_one() {
    // given: two vectors of 2 series, step_count=2, matched 0↔1, 1↔0
    let lschema = mk_schema("l", 2);
    let rschema = mk_schema("r", 2);
    let grid = mk_grid(2);
    let lhs_batch = mk_batch(
        lschema.clone(),
        2,
        2,
        vec![1.0, 2.0, 3.0, 4.0],
        vec![true; 4],
    );
    let rhs_batch = mk_batch(
        rschema.clone(),
        2,
        2,
        vec![10.0, 20.0, 30.0, 40.0],
        vec![true; 4],
    );
    let match_table = MatchTable::OneToOne(vec![Some(1), Some(0)]);

    // when
    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: out row 0 = lhs[0] + rhs[1]; out row 1 = lhs[1] + rhs[0]
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.series_count(), 2);
    assert_eq!(b.get(0, 0), Some(1.0 + 20.0)); // step 0, out_row 0 = lhs[0,0]+rhs[0,1]
    assert_eq!(b.get(0, 1), Some(2.0 + 10.0));
    assert_eq!(b.get(1, 0), Some(3.0 + 40.0));
    assert_eq!(b.get(1, 1), Some(4.0 + 30.0));
}

#[test]
fn should_apply_sub_vector_scalar() {
    // given: vector of 2 series × 2 steps, scalar=100
    let vschema = mk_schema("v", 2);
    let grid = mk_grid(2);
    let vec_batch = mk_batch(
        vschema.clone(),
        2,
        2,
        vec![1.0, 2.0, 3.0, 4.0],
        vec![true; 4],
    );
    let scalar_batch = mk_batch(mk_schema_single(), 2, 1, vec![100.0, 100.0], vec![true; 2]);

    // when: v - 100
    let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
    let mut op =
        BinaryOp::new_vector_scalar(lhs, rhs, BinaryOpKind::Sub, MemoryReservation::new(1 << 20));
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &outs[0];
    assert_eq!(b.series_count(), 2);
    assert_eq!(b.get(0, 0), Some(-99.0));
    assert_eq!(b.get(0, 1), Some(-98.0));
    assert_eq!(b.get(1, 0), Some(-97.0));
    assert_eq!(b.get(1, 1), Some(-96.0));
}

#[test]
fn should_apply_div_scalar_scalar() {
    // given: two scalars, 3 steps
    let grid = mk_grid(3);
    let lhs_batch = mk_batch(mk_schema_single(), 3, 1, vec![6.0, 9.0, 1.0], vec![true; 3]);
    let rhs_batch = mk_batch(mk_schema_single(), 3, 1, vec![2.0, 3.0, 0.0], vec![true; 3]);

    // when: scalar/scalar, 1/0 → +Inf per IEEE 754 (Prometheus; see
    // module docs — legacy engine would emit NaN).
    let lhs = MockOp::new(mk_schema_single(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![rhs_batch]);
    let mut op =
        BinaryOp::new_scalar_scalar(lhs, rhs, BinaryOpKind::Div, MemoryReservation::new(1 << 20));
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &outs[0];
    assert_eq!(b.series_count(), 1);
    assert_eq!(b.step_count(), 3);
    assert_eq!(b.get(0, 0), Some(3.0));
    assert_eq!(b.get(1, 0), Some(3.0));
    let inf = b.get(2, 0).unwrap();
    assert!(inf.is_infinite() && inf > 0.0);
}

#[test]
fn should_set_validity_zero_when_rhs_missing_in_one_to_one() {
    // given: lhs[0] has no match (None), lhs[1]↔rhs[0]
    let lschema = mk_schema("l", 2);
    let rschema = mk_schema("r", 1);
    let grid = mk_grid(1);
    let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
    let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![10.0], vec![true]);
    let match_table = MatchTable::OneToOne(vec![None, Some(0)]);

    // when
    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: out_row 0 invalid; out_row 1 = 2 + 10
    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), Some(12.0));
}

#[test]
fn should_apply_comparison_without_bool_modifier_skipping_false_cells() {
    // given: v > 10 with values [5, 15, 20]
    let vschema = mk_schema("v", 3);
    let grid = mk_grid(1);
    let vec_batch = mk_batch(vschema.clone(), 1, 3, vec![5.0, 15.0, 20.0], vec![true; 3]);
    let scalar_batch = mk_batch(mk_schema_single(), 1, 1, vec![10.0], vec![true]);

    // when
    let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
    let mut op = BinaryOp::new_vector_scalar(
        lhs,
        rhs,
        BinaryOpKind::Gt {
            bool_modifier: false,
        },
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: 5>10 false → dropped; 15 / 20 passed through with LHS value
    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), Some(15.0));
    assert_eq!(b.get(0, 2), Some(20.0));
}

#[test]
fn should_apply_comparison_with_bool_modifier_producing_0_1() {
    // given: v > bool 10 with values [5, 15, 20]
    let vschema = mk_schema("v", 3);
    let grid = mk_grid(1);
    let vec_batch = mk_batch(vschema.clone(), 1, 3, vec![5.0, 15.0, 20.0], vec![true; 3]);
    let scalar_batch = mk_batch(mk_schema_single(), 1, 1, vec![10.0], vec![true]);

    // when
    let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
    let mut op = BinaryOp::new_vector_scalar(
        lhs,
        rhs,
        BinaryOpKind::Gt {
            bool_modifier: true,
        },
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: all three matched, with 0.0/1.0
    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(0.0));
    assert_eq!(b.get(0, 1), Some(1.0));
    assert_eq!(b.get(0, 2), Some(1.0));
}

#[test]
fn should_handle_pow_mod_atan2() {
    // given: two scalars, step_count=3
    let grid = mk_grid(3);
    let l = mk_batch(mk_schema_single(), 3, 1, vec![2.0, 7.0, 1.0], vec![true; 3]);
    let r = mk_batch(mk_schema_single(), 3, 1, vec![3.0, 4.0, 0.0], vec![true; 3]);

    let cases: [(BinaryOpKind, [f64; 3]); 3] = [
        (BinaryOpKind::Pow, [8.0, 2401.0, 1.0]),
        (BinaryOpKind::Mod, [2.0 % 3.0, 7.0 % 4.0, 1.0_f64 % 0.0_f64]),
        (
            BinaryOpKind::Atan2,
            [2.0f64.atan2(3.0), 7.0f64.atan2(4.0), 1.0f64.atan2(0.0)],
        ),
    ];
    for (kind, expect) in cases {
        let lhs = MockOp::new(mk_schema_single(), grid, vec![l.clone()]);
        let rhs = MockOp::new(mk_schema_single(), grid, vec![r.clone()]);
        let mut op = BinaryOp::new_scalar_scalar(lhs, rhs, kind, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|rr| rr.unwrap()).collect();
        let b = &outs[0];
        for (step, e) in expect.iter().enumerate() {
            let got = b.get(step, 0).unwrap();
            if e.is_nan() {
                assert!(got.is_nan(), "{kind:?} step {step}: expected NaN got {got}");
            } else {
                assert!(
                    (got - e).abs() < 1e-9 || got == *e,
                    "{kind:?} step {step}: expected {e} got {got}"
                );
            }
        }
    }
}

#[test]
fn should_apply_and_returning_lhs_when_rhs_present() {
    // given: 2 lhs series, 2 rhs series, 1↔1 match
    let lschema = mk_schema("l", 2);
    let rschema = mk_schema("r", 2);
    let grid = mk_grid(1);
    // lhs valid on both; rhs has series 1 valid, series 0 invalid.
    let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
    let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![99.0, 99.0], vec![false, true]);
    let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

    // when: lhs and rhs
    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::And,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: row 0 → rhs invalid so drop; row 1 → both valid, emit LHS.
    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), Some(2.0));
}

#[test]
fn should_apply_or_falling_back_to_rhs_when_lhs_missing() {
    let lschema = mk_schema("l", 2);
    let rschema = mk_schema("r", 2);
    let grid = mk_grid(1);
    // lhs: series 0 invalid, series 1 valid; rhs: series 0 valid, series 1 invalid.
    let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![false, true]);
    let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![10.0, 20.0], vec![true, false]);
    let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Or,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: row 0 lhs missing, rhs present → emit rhs (10); row 1 lhs present → emit lhs (2)
    let b = &outs[0];
    assert_eq!(b.get(0, 0), Some(10.0));
    assert_eq!(b.get(0, 1), Some(2.0));
}

#[test]
fn should_apply_unless_dropping_lhs_when_rhs_present() {
    let lschema = mk_schema("l", 2);
    let rschema = mk_schema("r", 2);
    let grid = mk_grid(1);
    let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
    // rhs: series 0 present → drop lhs[0]; series 1 missing → keep lhs[1]
    let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![99.0, 99.0], vec![true, false]);
    let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Unless,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.get(0, 0), None);
    assert_eq!(b.get(0, 1), Some(2.0));
}

#[test]
fn should_propagate_error_from_upstream() {
    // given: LHS yields an error; op short-circuits.
    let lschema = mk_schema("l", 1);
    let rschema = mk_schema("r", 1);
    let grid = mk_grid(1);
    let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![3.0], vec![true]);
    let lhs = MockOp::with_queue(
        lschema.clone(),
        grid,
        vec![Err(QueryError::MemoryLimit {
            requested: 8,
            cap: 0,
            already_reserved: 0,
        })],
    );
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        MatchTable::OneToOne(vec![Some(0)]),
        lschema,
        MemoryReservation::new(1 << 20),
    );

    // when
    let outs = drive(&mut op);

    // then
    assert_eq!(outs.len(), 1);
    assert!(matches!(outs[0], Err(QueryError::MemoryLimit { .. })));
    // After error, subsequent polls return None.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match op.next(&mut cx) {
        Poll::Ready(None) => {}
        other => panic!("expected None after error, got {other:?}"),
    }
}

#[test]
fn should_respect_memory_reservation() {
    // given: reservation of 0 bytes.
    let lschema = mk_schema("l", 1);
    let rschema = mk_schema("r", 1);
    let grid = mk_grid(1);
    let lhs_batch = mk_batch(lschema.clone(), 1, 1, vec![1.0], vec![true]);
    let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![2.0], vec![true]);
    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        MatchTable::OneToOne(vec![Some(0)]),
        lschema,
        MemoryReservation::new(1),
    );

    // when
    let outs = drive(&mut op);

    // then
    assert_eq!(outs.len(), 1);
    assert!(matches!(outs[0], Err(QueryError::MemoryLimit { .. })));
}

#[test]
fn should_return_static_schema_matching_match_table_output() {
    // given: vector/vector binop with OneToOne
    let lschema = mk_schema("l", 3);
    let rschema = mk_schema("r", 3);
    let grid = mk_grid(2);
    let lhs = MockOp::new(lschema.clone(), grid, vec![]);
    let rhs = MockOp::new(rschema, grid, vec![]);
    let op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        MatchTable::OneToOne(vec![Some(0), Some(1), Some(2)]),
        lschema.clone(),
        MemoryReservation::new(1 << 20),
    );

    // when / then
    assert!(!op.schema().series.is_deferred());
    assert_eq!(op.schema().series.as_static().unwrap().len(), 3);
    assert_eq!(op.schema().step_grid.step_count, 2);
}

#[test]
fn should_handle_child_end_of_stream() {
    // given: both children yield None from the start
    let lschema = mk_schema("l", 1);
    let rschema = mk_schema("r", 1);
    let grid = mk_grid(1);
    let lhs = MockOp::new(lschema.clone(), grid, vec![]);
    let rhs = MockOp::new(rschema, grid, vec![]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        MatchTable::OneToOne(vec![Some(0)]),
        lschema,
        MemoryReservation::new(1 << 20),
    );

    // when
    let outs = drive(&mut op);

    // then: no results, no errors — just end-of-stream.
    assert!(outs.is_empty());
}

#[test]
fn should_stitch_aligned_step_ranges_from_children() {
    // given: two children each emitting a [0..2) then [2..4) batch.
    // Vector/vector is a pipeline-breaker: children are drained into
    // a full-grid buffer and a single output batch covering 0..4 is
    // emitted on EOS (correct for cross-tile matches; see module
    // docs).
    let lschema = mk_schema("l", 1);
    let rschema = mk_schema("r", 1);
    let grid = StepGrid {
        start_ms: 1_000,
        end_ms: 4_000,
        step_ms: 1_000,
        step_count: 4,
    };
    let ts: Arc<[i64]> = Arc::from(vec![1_000, 2_000, 3_000, 4_000]);

    let mk = |start: usize, end: usize, values: Vec<f64>, schema: Arc<SeriesSchema>| {
        let mut v = BitSet::with_len(values.len());
        for i in 0..values.len() {
            v.set(i);
        }
        StepBatch::new(
            ts.clone(),
            start..end,
            SchemaRef::Static(schema),
            0..1,
            values,
            v,
        )
    };

    let lhs_batches = vec![
        mk(0, 2, vec![1.0, 2.0], lschema.clone()),
        mk(2, 4, vec![3.0, 4.0], lschema.clone()),
    ];
    let rhs_batches = vec![
        mk(0, 2, vec![10.0, 20.0], rschema.clone()),
        mk(2, 4, vec![30.0, 40.0], rschema.clone()),
    ];

    // when
    let lhs = MockOp::new(lschema.clone(), grid, lhs_batches);
    let rhs = MockOp::new(rschema, grid, rhs_batches);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        MatchTable::OneToOne(vec![Some(0)]),
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one output batch covering the full grid with correct
    // per-(step, series) sums.
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].step_range, 0..4);
    assert_eq!(outs[0].get(0, 0), Some(11.0));
    assert_eq!(outs[0].get(1, 0), Some(22.0));
    assert_eq!(outs[0].get(2, 0), Some(33.0));
    assert_eq!(outs[0].get(3, 0), Some(44.0));
}

#[test]
fn should_apply_group_left_with_shared_rhs() {
    // given: LHS has 3 "many" series all matching the same single RHS
    // series (e.g. `http_requests + on(env) group_left single_rhs`).
    let lschema = mk_schema("l", 3);
    let rschema = mk_schema("r", 1);
    let grid = mk_grid(1);
    let lhs_batch = mk_batch(
        lschema.clone(),
        1,
        3,
        vec![1.0, 2.0, 3.0],
        vec![true, true, true],
    );
    let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![100.0], vec![true]);
    let match_table = MatchTable::GroupLeft(vec![Some(0), Some(0), Some(0)]);

    // when
    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: all three lhs series were paired with rhs[0].
    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    assert_eq!(b.get(0, 0), Some(101.0));
    assert_eq!(b.get(0, 1), Some(102.0));
    assert_eq!(b.get(0, 2), Some(103.0));
}

#[test]
fn should_apply_group_right_with_shared_lhs() {
    // given: RHS has 2 "many" series, both matching LHS[0].
    let lschema = mk_schema("l", 1);
    let rschema = mk_schema("r", 2);
    let grid = mk_grid(1);
    let lhs_batch = mk_batch(lschema.clone(), 1, 1, vec![5.0], vec![true]);
    let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![10.0, 20.0], vec![true; 2]);
    let match_table = MatchTable::GroupRight(vec![Some(0), Some(0)]);

    // when
    let lhs = MockOp::new(lschema, grid, vec![lhs_batch]);
    let rhs = MockOp::new(rschema.clone(), grid, vec![rhs_batch]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Mul,
        match_table,
        rschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: output schema is RHS-shaped; each output = lhs * rhs[j]
    let b = &outs[0];
    assert_eq!(b.series_count(), 2);
    assert_eq!(b.get(0, 0), Some(50.0));
    assert_eq!(b.get(0, 1), Some(100.0));
}

// ========================================================================
// tile-boundary stress (RFC 0007 6.3.9)
// ========================================================================

/// Build a batch over an arbitrary `series_range` slice of a `roster`
/// schema. Values are row-major; `values[step * series_count + s]`
/// holds the sample for `(step, series_range.start + s)`.
fn mk_tile_batch(
    roster: Arc<SeriesSchema>,
    step_count: usize,
    series_range: std::ops::Range<usize>,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    let series_count = series_range.len();
    assert_eq!(values.len(), step_count * series_count);
    assert_eq!(validity.len(), values.len());
    let ts: Arc<[i64]> = Arc::from(
        (0..step_count)
            .map(|i| 1_000 + (i as i64) * 1_000)
            .collect::<Vec<_>>(),
    );
    let mut bits = BitSet::with_len(validity.len());
    for (i, &b) in validity.iter().enumerate() {
        if b {
            bits.set(i);
        }
    }
    StepBatch::new(
        ts,
        0..step_count,
        SchemaRef::Static(roster),
        series_range,
        values,
        bits,
    )
}

#[test]
fn should_apply_vector_vector_binary_across_multi_series_tile_batches() {
    // given: vector/vector over 1024 matched series (identity match:
    // lhs[i] ↔ rhs[i]) with both sides tiled into 2 × 512-series
    // batches — this is exactly the shape the default
    // `VectorSelectorOp` emits for rosters >512 series. Values are
    // chosen so the correct per-cell answer is
    // `value[step, series] = 1000 * step + series` (lhs) + same
    // shifted to `10_000 * step + series` (rhs); add-combined sum per
    // cell is therefore `11_000 * step + 2 * series`.
    const SERIES: usize = 1024;
    const TILE: usize = 512;
    const STEP_COUNT: usize = 2;

    let lschema = mk_schema("m", SERIES);
    let rschema = mk_schema("m", SERIES);
    let grid = mk_grid(STEP_COUNT);

    let build_tile = |prefix_scale: f64, series_range: std::ops::Range<usize>| -> Vec<f64> {
        let mut v = Vec::with_capacity(STEP_COUNT * series_range.len());
        for step in 0..STEP_COUNT {
            for s in series_range.clone() {
                v.push(prefix_scale * step as f64 + s as f64);
            }
        }
        v
    };

    let lhs_a = mk_tile_batch(
        lschema.clone(),
        STEP_COUNT,
        0..TILE,
        build_tile(1_000.0, 0..TILE),
        vec![true; STEP_COUNT * TILE],
    );
    let lhs_b = mk_tile_batch(
        lschema.clone(),
        STEP_COUNT,
        TILE..SERIES,
        build_tile(1_000.0, TILE..SERIES),
        vec![true; STEP_COUNT * TILE],
    );
    let rhs_a = mk_tile_batch(
        rschema.clone(),
        STEP_COUNT,
        0..TILE,
        build_tile(10_000.0, 0..TILE),
        vec![true; STEP_COUNT * TILE],
    );
    let rhs_b = mk_tile_batch(
        rschema.clone(),
        STEP_COUNT,
        TILE..SERIES,
        build_tile(10_000.0, TILE..SERIES),
        vec![true; STEP_COUNT * TILE],
    );

    // OneToOne identity match: output row i pulls lhs[i] + rhs[i].
    let match_table: Vec<Option<u32>> = (0..SERIES as u32).map(Some).collect();
    let match_table = MatchTable::OneToOne(match_table);

    let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_a, lhs_b]);
    let rhs = MockOp::new(rschema, grid, vec![rhs_a, rhs_b]);
    let mut op = BinaryOp::new_vector_vector(
        lhs,
        rhs,
        BinaryOpKind::Add,
        match_table,
        lschema,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: reassemble a global (step, series) cell matrix from
    // whatever batch shapes the op produced, then verify every cell
    // equals the single-tile reference `11_000 * step + 2 * series`.
    // Also verify no duplicate coverage: each global cell must be
    // written exactly once.
    let mut covered = vec![vec![false; SERIES]; STEP_COUNT];
    let mut computed = vec![vec![f64::NAN; SERIES]; STEP_COUNT];
    for batch in &outs {
        let sc = batch.series_count();
        for step_off in 0..batch.step_count() {
            let gs = batch.step_range.start + step_off;
            for so in 0..sc {
                let cell = step_off * sc + so;
                if !batch.validity.get(cell) {
                    continue;
                }
                let global_series = batch.series_range.start + so;
                assert!(
                    !covered[gs][global_series],
                    "cell (step={gs}, series={global_series}) covered twice",
                );
                covered[gs][global_series] = true;
                computed[gs][global_series] = batch.values[cell];
            }
        }
    }
    for step in 0..STEP_COUNT {
        for series in 0..SERIES {
            let expected = 11_000.0 * step as f64 + 2.0 * series as f64;
            assert!(
                covered[step][series],
                "missing cell (step={step}, series={series})",
            );
            assert!(
                (computed[step][series] - expected).abs() < 1e-9,
                "cell (step={step}, series={series}): expected {expected}, got {}",
                computed[step][series],
            );
        }
    }
}

#[test]
fn should_broadcast_scalar_across_series_tiled_vector_batches() {
    // given: `v * 2` where the vector side emits two series tiles for the
    // same step range but the scalar side emits a single batch
    let vschema = mk_schema("v", 4);
    let grid = mk_grid(2);
    let tile_a = mk_tile_batch(
        vschema.clone(),
        2,
        0..2,
        vec![1.0, 2.0, 5.0, 6.0],
        vec![true; 4],
    );
    let tile_b = mk_tile_batch(
        vschema.clone(),
        2,
        2..4,
        vec![3.0, 4.0, 7.0, 8.0],
        vec![true; 4],
    );
    let scalar_batch = mk_batch(mk_schema_single(), 2, 1, vec![2.0, 10.0], vec![true; 2]);

    // when
    let lhs = MockOp::new(vschema, grid, vec![tile_a, tile_b]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
    let mut op =
        BinaryOp::new_vector_scalar(lhs, rhs, BinaryOpKind::Mul, MemoryReservation::new(1 << 20));
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: each tile is scaled by the scalar at its own step
    assert_eq!(outs.len(), 2);
    assert_eq!(outs[0].series_range, 0..2);
    assert_eq!(outs[0].get(0, 1), Some(4.0));
    assert_eq!(outs[0].get(1, 0), Some(50.0));
    assert_eq!(outs[1].series_range, 2..4);
    assert_eq!(outs[1].get(0, 0), Some(6.0));
    assert_eq!(outs[1].get(1, 1), Some(80.0));
}

#[test]
fn should_return_empty_for_scalar_op_on_empty_vector() {
    // given: `v > 1` where the vector side has no series and emits nothing
    let grid = mk_grid(2);
    let scalar_batch = mk_batch(mk_schema_single(), 2, 1, vec![1.0, 1.0], vec![true; 2]);
    let lhs = MockOp::new(mk_schema("v", 0), grid, vec![]);
    let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
    let mut op = BinaryOp::new_vector_scalar(
        lhs,
        rhs,
        BinaryOpKind::Gt {
            bool_modifier: false,
        },
        MemoryReservation::new(1 << 20),
    );

    // when + then: no batches and no error
    assert!(drive(&mut op).is_empty());
}
