use super::*;
use futures::stream;
use promql_parser::parser::VectorSelector;
use std::future::ready;
use std::task::Waker;
use std::time::Duration;

use crate::model::{Label, Labels, STALE_NAN};
use crate::promql::source::{ResolvedSeriesChunk, SampleBlock};

// ---- mock source ----------------------------------------------------

/// In-memory [`SeriesSource`] stub backed by per-series sample vectors.
/// Bucket/series bookkeeping is deliberately trivial — the operator
/// doesn't care about bucket IDs, it just threads them back from the
/// request. All test series share `bucket_id = 1`.
struct MockSource {
    /// Indexed by `series_id` (equal to the series' position in the
    /// roster). Each column is `(timestamps, values)`.
    data: Vec<(Vec<i64>, Vec<f64>)>,
}

impl MockSource {
    fn new(data: Vec<(Vec<i64>, Vec<f64>)>) -> Self {
        Self { data }
    }
}

impl SeriesSource for MockSource {
    fn resolve(
        &self,
        _selector: &VectorSelector,
        _time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        // Operator tests pre-resolve series externally; resolve is
        // exercised in 2.3. Return an empty stream to satisfy the
        // trait.
        stream::empty()
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let mut block = SampleBlock::with_series_count(request.series.len());
        for (col_idx, sref) in request.series.iter().enumerate() {
            let col = &self.data[sref.series_id as usize];
            // Honour the inclusive-exclusive time_range. The source
            // contract says samples in `[start, end)` are returned
            // in timestamp order.
            for (t, v) in col.0.iter().zip(col.1.iter()) {
                if *t >= request.time_range.start_ms && *t < request.time_range.end_ms_exclusive {
                    block.timestamps[col_idx].push(*t);
                    block.values[col_idx].push(*v);
                }
            }
        }
        stream::once(ready(Ok(SampleBatch {
            series_range: 0..request.series.len(),
            samples: block,
        })))
    }
}

// ---- fixture builders -----------------------------------------------

fn mk_label(metric: &str, suffix: usize) -> Labels {
    Labels::new(vec![
        Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        },
        Label {
            name: "i".to_string(),
            value: suffix.to_string(),
        },
    ])
}

fn mk_schema(n: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..n).map(|i| mk_label("m", i)).collect();
    let fps: Vec<u128> = (0..n as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

fn mk_request_series(n: usize) -> Arc<[Arc<[ResolvedSeriesRef]>]> {
    let name: Arc<str> = Arc::from("m");
    Arc::from(
        (0..n)
            .map(|i| {
                Arc::<[ResolvedSeriesRef]>::from(vec![ResolvedSeriesRef::new(
                    1,
                    i as u32,
                    name.clone(),
                )])
            })
            .collect::<Vec<_>>(),
    )
}

fn mk_grid(start_ms: i64, step_ms: i64, step_count: usize) -> StepGrid {
    let end_ms = start_ms + (step_count as i64 - 1) * step_ms;
    StepGrid {
        start_ms,
        end_ms,
        step_ms,
        step_count,
    }
}

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

/// Drain the operator to exhaustion, collecting all batches. Since
/// `MockSource` futures are ready-immediately, `Poll::Pending` is a
/// test failure.
fn drain<S: SeriesSource + Send + Sync + 'static>(
    op: &mut VectorSelectorOp<'static, S>,
) -> Vec<Result<StepBatch, QueryError>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(result)) => out.push(result),
            Poll::Pending => panic!("unexpected Pending from sync MockSource"),
        }
    }
}

fn make_op(
    source: MockSource,
    grid: StepGrid,
    at: Option<AtModifier>,
    offset: Option<Offset>,
    lookback_ms: i64,
    reservation: MemoryReservation,
    shape: BatchShape,
) -> VectorSelectorOp<'static, MockSource> {
    let n = source.data.len();
    let schema = mk_schema(n);
    let request = mk_request_series(n);
    VectorSelectorOp::new(
        Arc::new(source),
        schema,
        request,
        grid,
        at,
        offset,
        lookback_ms,
        reservation,
        shape,
    )
}

// ---- assertions over full drains ------------------------------------

/// Materialise the full [step x series] result grid from a stream of
/// batches. `None` cells mean "validity bit clear". The resulting
/// vector is indexed `[step][series]`.
fn materialise(
    batches: &[StepBatch],
    step_count: usize,
    series_count: usize,
) -> Vec<Vec<Option<f64>>> {
    let mut out = vec![vec![None; series_count]; step_count];
    for batch in batches {
        for step_off in 0..batch.step_count() {
            let step_global = batch.step_range.start + step_off;
            for series_off in 0..batch.series_count() {
                let series_global = batch.series_range.start + series_off;
                out[step_global][series_global] = batch.get(step_off, series_off);
            }
        }
    }
    out
}

// ====================================================================
// required tests
// ====================================================================

#[test]
fn should_emit_last_sample_within_lookback_window() {
    // given: one series with samples at t=10,20,30, step_ms=10,
    // step_count=4 starting at t=10, lookback=5.
    let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0])]);
    let grid = mk_grid(10, 10, 4);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::new(4, 1),
    );

    // when
    let results = drain(&mut op);
    let batches: Vec<StepBatch> = results.into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 4, 1);

    // then: step 10 → 1.0; step 20 → 2.0; step 30 → 3.0; step 40 → none
    assert_eq!(cells[0][0], Some(1.0));
    assert_eq!(cells[1][0], Some(2.0));
    assert_eq!(cells[2][0], Some(3.0));
    assert_eq!(cells[3][0], None, "no sample in (35, 40]");
}

#[test]
fn should_merge_cross_bucket_refs_for_one_logical_series() {
    // given: one logical output series backed by two bucket-local refs.
    // The older bucket contributes the earlier sample and the newer bucket
    // contributes the later one.
    let source = Arc::new(MockSource::new(vec![
        (vec![10], vec![1.0]),
        (vec![20], vec![2.0]),
    ]));
    let schema = mk_schema(1);
    let name: Arc<str> = Arc::from("m");
    let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![Arc::from(vec![
        ResolvedSeriesRef::new(1, 0, name.clone()),
        ResolvedSeriesRef::new(2, 1, name.clone()),
    ])]);
    let grid = mk_grid(10, 10, 2);
    let reservation = MemoryReservation::new(1 << 20);
    let mut op = VectorSelectorOp::new(
        source,
        schema,
        request_series,
        grid,
        None,
        None,
        15,
        reservation,
        BatchShape::new(2, 1),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 2, 1);

    // then
    assert_eq!(cells[0][0], Some(1.0));
    assert_eq!(cells[1][0], Some(2.0));
}

#[test]
fn should_set_validity_zero_when_no_sample_in_lookback() {
    // given: series with a hole between t=10 and t=100
    let source = MockSource::new(vec![(vec![10, 100], vec![1.0, 2.0])]);
    // 4 steps: 20, 40, 60, 80 — all inside the hole with lookback=15
    let grid = mk_grid(20, 20, 4);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        15,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 4, 1);

    // then: step 20 → 1.0 (in (5,20]); step 40 → none (hole);
    // step 60/80 → none (hole)
    assert_eq!(cells[0][0], Some(1.0));
    assert_eq!(cells[1][0], None);
    assert_eq!(cells[2][0], None);
    assert_eq!(cells[3][0], None);
}

#[test]
fn should_treat_stale_nan_as_absence() {
    // given: sample at t=10 (good), t=20 (STALE_NAN). Step at t=25
    // with lookback=30 would reach back to t=10 — but the intervening
    // STALE_NAN terminates the lookback.
    let stale = f64::from_bits(STALE_NAN);
    let source = MockSource::new(vec![(vec![10, 20], vec![1.0, stale])]);
    let grid = mk_grid(25, 10, 1);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        30,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 1, 1);

    // then: cell absent — the STALE_NAN at t=20 is the most recent
    // in-window sample and is treated as a staleness marker.
    assert_eq!(cells[0][0], None);
}

#[test]
fn should_apply_offset_shifting_lookup_window() {
    // given: samples at t=10,20,30 and offset=10ms. For step t,
    // window = (t - 10 - lookback, t - 10].
    let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0])]);
    // step=20 → window = (15, 20] → pick 2.0 without offset, or
    // (5, 10] with offset=10 → pick 1.0
    let grid = mk_grid(20, 10, 3);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        Some(Offset::Pos(Duration::from_millis(10))),
        5,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 3, 1);

    // then:
    //   step 20, effective=10, window=(5,10] → 1.0
    //   step 30, effective=20, window=(15,20] → 2.0
    //   step 40, effective=30, window=(25,30] → 3.0
    assert_eq!(cells[0][0], Some(1.0));
    assert_eq!(cells[1][0], Some(2.0));
    assert_eq!(cells[2][0], Some(3.0));
}

#[test]
fn should_apply_at_timestamp_pinning() {
    // given: samples at t=50, t=100, t=150. `@ 100` pins every step
    // to eval-time 100; lookback=60 → window (40, 100] → latest is 100.
    let source = MockSource::new(vec![(vec![50, 100, 150], vec![1.0, 2.0, 3.0])]);
    let grid = mk_grid(0, 10, 5); // steps 0, 10, 20, 30, 40
    let at_time = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(100);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        Some(AtModifier::At(at_time)),
        None,
        60,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 5, 1);

    // then: every step emits the same value (the sample at t=100)
    for (k, row) in cells.iter().enumerate().take(5) {
        assert_eq!(row[0], Some(2.0), "step {k} must match @-pin");
    }
}

#[test]
fn should_respect_memory_reservation() {
    // given: a tiny reservation cap that cannot fit a batch.
    let source = MockSource::new(vec![(vec![10], vec![1.0]); 4]);
    let grid = mk_grid(10, 10, 8);
    // cell_bytes for 8 steps × 4 series = 32 cells * (8 + bit) ≈ 264
    // bytes. Cap at 16 → try_grow rejects.
    let tiny = MemoryReservation::new(16);
    let mut op = make_op(source, grid, None, None, 5, tiny, BatchShape::new(8, 4));

    // when
    let results = drain(&mut op);

    // then: at least one result is a MemoryLimit error.
    let err = results
        .into_iter()
        .find_map(|r| r.err())
        .expect("expected a MemoryLimit error");
    assert!(matches!(err, QueryError::MemoryLimit { .. }));

    // and-given: a reasonable cap lets it emit
    let source = MockSource::new(vec![(vec![10], vec![1.0]); 4]);
    let reservation = MemoryReservation::new(1_000_000);
    let grid = mk_grid(10, 10, 8);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::new(8, 4),
    );
    let results = drain(&mut op);
    assert!(results.iter().all(|r| r.is_ok()));
}

#[test]
fn should_emit_batches_covering_full_step_grid() {
    // given: 3 series × 5 steps, batch shape 2 × 2 → tiles:
    // [0..2, 0..2], [0..2, 2..3], [2..4, 0..2], [2..4, 2..3],
    // [4..5, 0..2], [4..5, 2..3]
    let source = MockSource::new(vec![
        (vec![0, 10, 20, 30, 40], vec![1.0, 2.0, 3.0, 4.0, 5.0]),
        (vec![0, 10, 20, 30, 40], vec![10.0, 20.0, 30.0, 40.0, 50.0]),
        (
            vec![0, 10, 20, 30, 40],
            vec![100.0, 200.0, 300.0, 400.0, 500.0],
        ),
    ]);
    let grid = mk_grid(0, 10, 5);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::new(2, 2),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: every (step, series) cell is accounted for in exactly one
    // batch. Materialising yields a full 5×3 grid.
    let cells = materialise(&batches, 5, 3);
    for (k, row) in cells.iter().enumerate().take(5) {
        assert_eq!(row[0], Some(k as f64 + 1.0));
        assert_eq!(row[1], Some((k as f64 + 1.0) * 10.0));
        assert_eq!(row[2], Some((k as f64 + 1.0) * 100.0));
    }
    // total cells covered equals step_count × series_count
    let total: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total, 5 * 3);
}

#[test]
fn should_yield_end_of_stream_when_grid_exhausted() {
    // given: a two-step, one-series operator
    let source = MockSource::new(vec![(vec![0, 10], vec![1.0, 2.0])]);
    let grid = mk_grid(0, 10, 2);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::default(),
    );

    // when: drain, then poll one more time
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut count = 0usize;
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(Ok(_))) => count += 1,
            Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
            Poll::Pending => panic!("unexpected Pending"),
        }
    }
    // then: at least one batch was emitted, and the next poll after
    // end-of-stream is still Ready(None).
    assert!(count >= 1);
    match op.next(&mut cx) {
        Poll::Ready(None) => {}
        other => panic!("expected Ready(None) after exhaustion, got {other:?}"),
    }
}

#[test]
fn should_return_static_schema() {
    // given
    let source = MockSource::new(vec![(vec![0], vec![1.0])]);
    let grid = mk_grid(0, 10, 1);
    let reservation = MemoryReservation::new(1_000_000);
    let op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::default(),
    );

    // when
    let schema = op.schema();

    // then
    assert!(!schema.series.is_deferred());
    assert!(schema.series.as_static().is_some());
    assert_eq!(schema.step_grid.step_count, 1);
}

// ---- extra coverage ------------------------------------------------

#[test]
fn should_repeat_at_start_across_all_steps() {
    // given: @ start() pins every step to grid.start_ms
    let source = MockSource::new(vec![(vec![0, 10, 20], vec![10.0, 20.0, 30.0])]);
    let grid = mk_grid(10, 10, 3);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        Some(AtModifier::Start),
        None,
        5,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
    let cells = materialise(&batches, 3, 1);

    // then: every step picks the sample at t=10 (grid.start_ms)
    for row in cells.iter().take(3) {
        assert_eq!(row[0], Some(20.0));
    }
}

#[test]
fn should_handle_empty_series_roster() {
    // given: zero series
    let source = MockSource::new(vec![]);
    let grid = mk_grid(0, 10, 4);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        5,
        reservation,
        BatchShape::default(),
    );

    // when / then: end-of-stream on first poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match op.next(&mut cx) {
        Poll::Ready(None) => {}
        other => panic!("expected Ready(None), got {other:?}"),
    }
}
