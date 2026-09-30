use super::*;
use crate::model::{Label, Labels};
use crate::promql::batch::{SchemaRef, SeriesSchema};
use crate::promql::operator::StepGrid;
use crate::promql::operators::matrix_selector::CellIndex;
use std::sync::Arc;
use std::task::Waker;

// ---- mock WindowStream --------------------------------------------------

struct MockWindows {
    schema: OperatorSchema,
    queue: Vec<Result<MatrixWindowBatch, QueryError>>,
}

impl MockWindows {
    fn new(schema: OperatorSchema, batches: Vec<MatrixWindowBatch>) -> Self {
        Self {
            schema,
            queue: batches.into_iter().map(Ok).collect(),
        }
    }
}

impl WindowStream for MockWindows {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn poll_windows(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        if self.queue.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(self.queue.remove(0)))
        }
    }
}

// ---- fixtures -----------------------------------------------------------

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

fn mk_schema(n: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..n)
        .map(|i| {
            Labels::new(vec![
                Label {
                    name: "__name__".to_string(),
                    value: "m".to_string(),
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

/// Build a single-tile `MatrixWindowBatch` from scratch.
///
/// `step_timestamps` — absolute step timestamps (ms).
/// `cells` — row-major by step (outer = step, inner = series). Each
/// inner vec is the packed `(ts, v)` sample list for that cell.
fn build_window(
    step_timestamps: Vec<i64>,
    series_count: usize,
    cells: Vec<Vec<(i64, f64)>>,
) -> MatrixWindowBatch {
    let step_count = step_timestamps.len();
    assert_eq!(cells.len(), step_count * series_count);
    let schema = mk_schema(series_count);

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut cell_idx = Vec::with_capacity(cells.len());
    for samples in cells {
        let offset = timestamps.len() as u32;
        let len = samples.len() as u32;
        for (t, v) in samples {
            timestamps.push(t);
            values.push(v);
        }
        cell_idx.push(CellIndex { offset, len });
    }

    let ts_arc: Arc<[i64]> = Arc::from(step_timestamps);
    MatrixWindowBatch {
        step_timestamps: ts_arc.clone(),
        step_range: 0..step_count,
        series: SchemaRef::Static(schema),
        series_range: 0..series_count,
        timestamps,
        values,
        cells: cell_idx,
        effective_times: None,
    }
}

fn build_schema(step_timestamps: Vec<i64>, step_ms: i64, series_count: usize) -> OperatorSchema {
    let step_count = step_timestamps.len();
    let start_ms = step_timestamps.first().copied().unwrap_or(0);
    let end_ms = step_timestamps.last().copied().unwrap_or(0);
    let series = SchemaRef::Static(mk_schema(series_count));
    OperatorSchema::new(
        series,
        StepGrid {
            start_ms,
            end_ms,
            step_ms,
            step_count,
        },
    )
}

fn drive<W: WindowStream>(op: &mut RollupOp<W>) -> Vec<Result<StepBatch, QueryError>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(result)) => out.push(result),
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
fn should_compute_rate_over_simple_counter() {
    // given: single step t=40, range=40 → window (0, 40]; samples
    // 10, 20, 30, 40 at timestamps 10, 20, 30, 40. Result should be
    // extrapolated_rate, matching the v1 reference in functions.rs.
    let step_ts = vec![40];
    let series = 1;
    let cells = vec![vec![(10, 10.0), (20, 20.0), (30, 30.0), (40, 40.0)]];
    let window = build_window(step_ts.clone(), series, cells);
    let schema = build_schema(step_ts, 10, series);

    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));

    // when
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one cell, validity=1.
    // Prometheus extrapolated_rate math:
    //   last-first = 30; no resets; time_diff = 30ms = 0.03s
    //   avg_interval = 0.01s; extrapolation_threshold = 0.011s
    //   duration_to_start = 10ms = 0.01s (< threshold → keep)
    //   duration_to_end = 0 (< threshold → keep)
    //   factor_unit = (0.03 + 0.01 + 0) / 0.03 = 4/3
    //   scaled = 30 * 4/3 = 40
    //   rate = 40 / 0.04 = 1000/s
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    let v = b.get(0, 0).expect("cell should be valid");
    assert!(approx_eq(v, 1000.0, 1e-9), "rate = {v}");
}

#[test]
fn should_handle_counter_reset_in_rate() {
    // given: counter values 10,20,5,15 at ts 10,20,30,40 — one reset
    // (20 → 5). `counter_increase_correction` returns 20.
    //   last-first = 15-10 = 5; +correction 20 → 25
    //   time_diff = 30ms = 0.03s; avg_interval = 0.01s
    //   duration_to_start = 10ms = 0.01s; duration_to_end = 0
    //   rate = 25 * (0.04/0.03) / 0.04 = 25/0.03 = 833.333.../s
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(10, 10.0), (20, 20.0), (30, 5.0), (40, 15.0)]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));

    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let v = batches[0].get(0, 0).unwrap();
    assert!(approx_eq(v, 25.0 / 0.03, 1e-9), "rate = {v}");
}

#[test]
fn should_compute_increase_matching_pipeline() {
    // given: strict counter 0,10,20,30 at ts 10,20,30,40.
    // Same math as rate but without the per-second divide.
    //   last-first = 30; time_diff 30ms = 0.03s; avg_interval 0.01s
    //   duration_to_start = 10ms (< threshold → keep)
    //     BUT: counter-zero clip: first_v=0 ≥ 0 and result>0, so
    //          duration_to_zero = 0 * 30/30 = 0; duration_to_start = 0.
    //   duration_to_end = 0.
    //   factor = 0.03/0.03 = 1.0
    //   increase = 30 * 1 = 30
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(10, 0.0), (20, 10.0), (30, 20.0), (40, 30.0)]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::Increase,
        40,
        MemoryReservation::new(1 << 20),
    );

    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let v = batches[0].get(0, 0).unwrap();
    assert!(approx_eq(v, 30.0, 1e-9), "increase = {v}");
}

#[test]
fn should_compute_avg_over_time() {
    // given: 2,4,6,8 → avg = 5.0
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(10, 2.0), (20, 4.0), (30, 6.0), (40, 8.0)]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::AvgOverTime,
        40,
        MemoryReservation::new(1 << 20),
    );

    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(batches[0].get(0, 0).unwrap(), 5.0, 1e-9));
}

#[test]
fn should_compute_sum_min_max_count_over_time() {
    // given: values 3,1,4,1,5 — sum=14, min=1, max=5, count=5
    let step_ts = vec![50];
    let cells = || vec![vec![(10, 3.0), (20, 1.0), (30, 4.0), (40, 1.0), (50, 5.0)]];

    let schema = build_schema(step_ts.clone(), 10, 1);

    for (kind, expected) in [
        (RollupKind::SumOverTime, 14.0),
        (RollupKind::MinOverTime, 1.0),
        (RollupKind::MaxOverTime, 5.0),
        (RollupKind::CountOverTime, 5.0),
    ] {
        let window = build_window(step_ts.clone(), 1, cells());
        let mock = MockWindows::new(schema.clone(), vec![window]);
        let mut op = RollupOp::new(mock, kind, 50, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let v = batches[0].get(0, 0).unwrap();
        assert!(
            approx_eq(v, expected, 1e-9),
            "{kind:?}: got {v}, want {expected}"
        );
    }
}

#[test]
fn should_compute_irate_from_last_two_samples() {
    // given: 5 samples; irate uses only the last two.
    //   last two: (30, 100), (40, 160) → (160-100)/0.01s = 6000/s
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![
            (0, 1.0),
            (10, 10.0),
            (20, 50.0),
            (30, 100.0),
            (40, 160.0),
        ]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Irate, 40, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(batches[0].get(0, 0).unwrap(), 6000.0, 1e-9));
}

#[test]
fn should_set_validity_zero_when_too_few_samples_for_rate() {
    // given: one sample — rate requires 2.
    let step_ts = vec![40];
    let window = build_window(step_ts.clone(), 1, vec![vec![(30, 5.0)]]);
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), None);
}

#[test]
fn should_set_validity_zero_when_window_empty() {
    // given: empty window — every rollup returns None (except
    // count_over_time which returns 0? Prometheus actually returns
    // "no result" for empty — absent cell. Our min_samples=1 enforces it.)
    let step_ts = vec![40];
    let window = build_window(step_ts.clone(), 1, vec![vec![]]);
    let schema = build_schema(step_ts.clone(), 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::SumOverTime,
        40,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), None);

    // Also verify for present_over_time specifically (which emits
    // 1 if any sample, else absent).
    let window = build_window(step_ts.clone(), 1, vec![vec![]]);
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::PresentOverTime,
        40,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), None);
}

#[test]
fn should_compute_quantile_over_time() {
    // given: 1..=5. quantile(0.5) = 3; quantile(0.75) = 4.
    let step_ts = vec![50];
    let cells = || vec![vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0), (50, 5.0)]];
    let schema = build_schema(step_ts.clone(), 10, 1);

    for (q, expected) in [(0.5_f64, 3.0), (0.75_f64, 4.0), (0.0, 1.0), (1.0, 5.0)] {
        let window = build_window(step_ts.clone(), 1, cells());
        let mock = MockWindows::new(schema.clone(), vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::QuantileOverTime(q),
            50,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let v = batches[0].get(0, 0).unwrap();
        assert!(
            approx_eq(v, expected, 1e-9),
            "q={q}: got {v}, want {expected}"
        );
    }
}

#[test]
fn should_compute_stddev_and_stdvar_over_time() {
    // given: 2,4,4,4,5,5,7,9 — population mean = 5, variance = 4, stddev = 2.
    let step_ts = vec![80];
    let cells = || {
        vec![vec![
            (10, 2.0),
            (20, 4.0),
            (30, 4.0),
            (40, 4.0),
            (50, 5.0),
            (60, 5.0),
            (70, 7.0),
            (80, 9.0),
        ]]
    };
    let schema = build_schema(step_ts.clone(), 10, 1);

    let window = build_window(step_ts.clone(), 1, cells());
    let mock = MockWindows::new(schema.clone(), vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::StdvarOverTime,
        80,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(batches[0].get(0, 0).unwrap(), 4.0, 1e-9));

    let window = build_window(step_ts.clone(), 1, cells());
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::StddevOverTime,
        80,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(batches[0].get(0, 0).unwrap(), 2.0, 1e-9));
}

#[test]
fn should_compute_changes_and_resets() {
    // given: 1,1,2,3,3,2,5 — changes=5 (1→2, 2→3, 3→2, 2→5, also 3→3 not a change),
    //        let's recount: pairs (1,1),(1,2),(2,3),(3,3),(3,2),(2,5)
    //         changes at positions 1→2 (yes), 2→3 (yes), 3→2 (yes), 2→5 (yes) = 4
    //         resets: only decreases: 3→2 = 1
    let step_ts = vec![70];
    let values = vec![
        (10, 1.0),
        (20, 1.0),
        (30, 2.0),
        (40, 3.0),
        (50, 3.0),
        (60, 2.0),
        (70, 5.0),
    ];
    let schema = build_schema(step_ts.clone(), 10, 1);

    let window = build_window(step_ts.clone(), 1, vec![values.clone()]);
    let mock = MockWindows::new(schema.clone(), vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::Changes,
        70,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), Some(4.0));

    let window = build_window(step_ts.clone(), 1, vec![values]);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::Resets,
        70,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), Some(1.0));
}

#[test]
fn should_return_static_schema() {
    // given: mock with a Static schema.
    let step_ts = vec![10];
    let window = build_window(step_ts.clone(), 1, vec![vec![(10, 1.0)]]);
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let op = RollupOp::new(
        mock,
        RollupKind::SumOverTime,
        10,
        MemoryReservation::new(1 << 20),
    );
    // when/then
    assert!(!op.schema().series.is_deferred());
    assert!(op.schema().series.as_static().is_some());
}

#[test]
fn should_respect_memory_reservation() {
    // given: cap too small for even a single-cell output.
    let step_ts = vec![10];
    let window = build_window(step_ts.clone(), 1, vec![vec![(10, 1.0)]]);
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::SumOverTime, 10, MemoryReservation::new(1));

    let results = drive(&mut op);
    let err = results
        .into_iter()
        .find_map(|r| r.err())
        .expect("expected MemoryLimit");
    assert!(matches!(err, QueryError::MemoryLimit { .. }));
}

#[test]
fn should_pass_through_step_grid() {
    // given: upstream schema with a specific grid
    let step_ts = vec![100, 110, 120];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(100, 1.0)], vec![(110, 2.0)], vec![(120, 3.0)]],
    );
    let schema = build_schema(step_ts.clone(), 10, 1);

    let expected_grid = schema.step_grid;
    let mock = MockWindows::new(schema, vec![window]);
    let op = RollupOp::new(
        mock,
        RollupKind::LastOverTime,
        10,
        MemoryReservation::new(1 << 20),
    );
    assert_eq!(op.schema().step_grid, expected_grid);
}

#[test]
fn should_drive_two_pointer_walk_across_many_steps() {
    // given: 16 steps, one series per step with a single sample at
    // the step timestamp. last_over_time of a window covering just
    // that step = the step value.
    let step_ms = 10;
    let n = 16;
    let step_ts: Vec<i64> = (0..n).map(|k| 10 + k as i64 * step_ms).collect();
    let cells: Vec<Vec<(i64, f64)>> = step_ts
        .iter()
        .map(|&t| vec![(t, (t / 10) as f64)])
        .collect();
    let window = build_window(step_ts.clone(), 1, cells);
    let schema = build_schema(step_ts.clone(), step_ms, 1);

    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::LastOverTime,
        step_ms,
        MemoryReservation::new(1 << 20),
    );

    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    // then: one batch with n cells, each holding value = step_ts[i]/10.
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.step_count(), n);
    for (i, &t) in step_ts.iter().enumerate().take(n) {
        let v = b.get(i, 0).expect("valid cell");
        assert!(approx_eq(v, (t / 10) as f64, 1e-9));
    }
}

// ---- extra coverage (not strictly required but trivially cheap) --------

#[test]
fn should_compute_idelta_from_last_two_samples() {
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(10, 1.0), (20, 5.0), (30, 7.0), (40, 12.0)]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::Idelta,
        40,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(batches[0].get(0, 0).unwrap(), 5.0, 1e-9));
}

#[test]
fn should_compute_delta_for_gauge() {
    // given: gauge values — delta does not correct for resets.
    //   10, 20, 5, 15 at ts 10,20,30,40
    //   last-first = 15-10 = 5 (NO correction)
    //   time_diff = 30ms; avg_interval = 0.01s
    //   duration_to_start = 10ms (< threshold → keep)
    //   NO counter-zero clip (Gauge path)
    //   duration_to_end = 0
    //   factor_unit = (0.03 + 0.01 + 0)/0.03 = 4/3
    //   delta = 5 * 4/3 ≈ 6.6667
    let step_ts = vec![40];
    let window = build_window(
        step_ts.clone(),
        1,
        vec![vec![(10, 10.0), (20, 20.0), (30, 5.0), (40, 15.0)]],
    );
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Delta, 40, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert!(approx_eq(
        batches[0].get(0, 0).unwrap(),
        5.0 * 4.0 / 3.0,
        1e-9
    ));
}

#[test]
fn should_emit_present_over_time_as_one_when_any_sample() {
    let step_ts = vec![40];
    let window = build_window(step_ts.clone(), 1, vec![vec![(30, 42.0)]]);
    let schema = build_schema(step_ts, 10, 1);
    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(
        mock,
        RollupKind::PresentOverTime,
        40,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(batches[0].get(0, 0), Some(1.0));
}

#[test]
fn should_use_effective_times_for_window_math_when_present() {
    // given: outer step is t=25 (= `step_timestamps[0]`) but samples
    // live in the `(0, 100]` window (folded `@ 100`) — same shape as
    // `rate(metric[100s] @ 100)` at outer t=25s. With
    // `effective_times = Some([100])` the rollup must compute over
    // `(0, 100]`, not `(-75, 25]`.
    let step_ts = vec![25i64];
    let effective = vec![100i64];
    let mut window = build_window(
        step_ts.clone(),
        1,
        vec![vec![
            (10, 1.0),
            (20, 2.0),
            (30, 3.0),
            (40, 4.0),
            (50, 5.0),
            (60, 6.0),
            (70, 7.0),
            (80, 8.0),
            (90, 9.0),
            (100, 10.0),
        ]],
    );
    window.effective_times = Some(Arc::from(effective));
    let schema = build_schema(step_ts, 10, 1);

    let mock = MockWindows::new(schema, vec![window]);
    let mut op = RollupOp::new(mock, RollupKind::Rate, 100, MemoryReservation::new(1 << 20));

    // when
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: Prometheus extrapolated rate over (0, 100] with samples
    // at 10..100 (avg interval 10ms):
    //   last - first = 9; time_diff = 90ms = 0.09s
    //   dur_to_start = 10ms = 0.01s (< 1.1 * avg = 0.011 → keep)
    //   dur_to_end = 0
    //   factor = (0.09 + 0.01 + 0) / 0.09 = 10/9
    //   scaled = 9 * 10/9 = 10
    //   rate = 10 / 0.1 = 100/s  (legacy v1 evaluator matches)
    let v = batches[0].get(0, 0).expect("rate valid");
    assert!(
        approx_eq(v, 100.0, 1e-9),
        "rate over effective window (0, 100]: {v}"
    );
}

#[test]
fn should_propagate_error_from_upstream() {
    // given: upstream yields an error — Rollup should pass it through
    // and refuse further polls.
    let schema = build_schema(vec![10], 10, 1);
    let mock = MockWindows {
        schema,
        queue: vec![Err(QueryError::MemoryLimit {
            requested: 10,
            cap: 0,
            already_reserved: 0,
        })],
    };
    let mut op = RollupOp::new(
        mock,
        RollupKind::SumOverTime,
        10,
        MemoryReservation::new(1 << 20),
    );

    let results = drive(&mut op);
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
}

/// Build a tile [`MatrixWindowBatch`] over `series_range` in a roster
/// of `total_series`. `cells` is row-major across the tile.
fn build_window_tile(
    step_timestamps: Vec<i64>,
    total_series: usize,
    series_range: std::ops::Range<usize>,
    cells: Vec<Vec<(i64, f64)>>,
) -> MatrixWindowBatch {
    let step_count = step_timestamps.len();
    let tile_sc = series_range.len();
    assert_eq!(cells.len(), step_count * tile_sc);
    let schema = mk_schema(total_series);

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut cell_idx = Vec::with_capacity(cells.len());
    for samples in cells {
        let offset = timestamps.len() as u32;
        let len = samples.len() as u32;
        for (t, v) in samples {
            timestamps.push(t);
            values.push(v);
        }
        cell_idx.push(CellIndex { offset, len });
    }

    let ts_arc: Arc<[i64]> = Arc::from(step_timestamps);
    MatrixWindowBatch {
        step_timestamps: ts_arc.clone(),
        step_range: 0..step_count,
        series: SchemaRef::Static(schema),
        series_range,
        timestamps,
        values,
        cells: cell_idx,
        effective_times: None,
    }
}

#[test]
fn should_reduce_multi_series_tile_window_batches_over_512_series() {
    // given: `RollupOp` consumes `MatrixWindowBatch`es from its child
    // `MatrixSelectorOp`, which tiles the same step range into
    // per-512-series batches for rosters >512 series. Each input tile
    // should round-trip to one output `StepBatch` tile with the same
    // series_range — the operator is per-batch stateless.
    const SERIES: usize = 1024;
    const TILE: usize = 512;
    let step_ts = vec![10_i64];
    let schema = build_schema(step_ts.clone(), 10, SERIES);

    // Each cell: one sample at t=10 with value = global series idx.
    let mut cells_a: Vec<Vec<(i64, f64)>> = Vec::with_capacity(TILE);
    for s in 0..TILE {
        cells_a.push(vec![(10, s as f64)]);
    }
    let mut cells_b: Vec<Vec<(i64, f64)>> = Vec::with_capacity(TILE);
    for s in TILE..SERIES {
        cells_b.push(vec![(10, s as f64)]);
    }
    let tile_a = build_window_tile(step_ts.clone(), SERIES, 0..TILE, cells_a);
    let tile_b = build_window_tile(step_ts, SERIES, TILE..SERIES, cells_b);

    let mock = MockWindows::new(schema, vec![tile_a, tile_b]);

    // when: sum_over_time emits the value as-is (one sample per cell).
    let mut op = RollupOp::new(
        mock,
        RollupKind::SumOverTime,
        10,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: two output batches preserving the child's tile shape,
    // each cell carrying the global series index it covers.
    assert_eq!(outs.len(), 2);
    assert_eq!(outs[0].series_range, 0..TILE);
    assert_eq!(outs[1].series_range, TILE..SERIES);
    for s in 0..TILE {
        assert_eq!(outs[0].get(0, s), Some(s as f64));
    }
    for s in 0..(SERIES - TILE) {
        let global = TILE + s;
        assert_eq!(outs[1].get(0, s), Some(global as f64));
    }
}
