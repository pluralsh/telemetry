use super::*;
use futures::stream;
use promql_parser::parser::VectorSelector;
use std::future::ready;
use std::task::Waker;
use std::time::Duration;

use crate::model::{Label, Labels, STALE_NAN};
use crate::promql::source::{ResolvedSeriesChunk, SampleBlock};

// ---- mock source (same shape 3a.1 uses) -----------------------------

struct MockSource {
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
        stream::empty()
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let mut block = SampleBlock::with_series_count(request.series.len());
        for (col_idx, sref) in request.series.iter().enumerate() {
            let col = &self.data[sref.series_id as usize];
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

fn drain_windows<S: SeriesSource + Send + Sync + 'static>(
    op: &mut MatrixSelectorOp<'static, S>,
) -> Vec<Result<MatrixWindowBatch, QueryError>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.windows(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(result)) => out.push(result),
            Poll::Pending => panic!("unexpected Pending from sync MockSource"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn make_op(
    source: MockSource,
    grid: StepGrid,
    at: Option<AtModifier>,
    offset: Option<Offset>,
    range_ms: i64,
    reservation: MemoryReservation,
    shape: BatchShape,
) -> MatrixSelectorOp<'static, MockSource> {
    let n = source.data.len();
    let schema = mk_schema(n);
    let request = mk_request_series(n);
    MatrixSelectorOp::new(
        Arc::new(source),
        schema,
        request,
        grid,
        at,
        offset,
        range_ms,
        reservation,
        shape,
    )
}

/// Collect all `(t, v)` pairs for a specific `(step_global, series_global)`
/// cell from a full drain of window batches.
fn cell_samples(
    batches: &[MatrixWindowBatch],
    step_global: usize,
    series_global: usize,
) -> Vec<(i64, f64)> {
    for batch in batches {
        if !batch.step_range.contains(&step_global) || !batch.series_range.contains(&series_global)
        {
            continue;
        }
        let step_off = step_global - batch.step_range.start;
        let series_off = series_global - batch.series_range.start;
        let (ts, vs) = batch.cell_samples(step_off, series_off);
        return ts.iter().zip(vs.iter()).map(|(t, v)| (*t, *v)).collect();
    }
    Vec::new()
}

// ====================================================================
// required tests
// ====================================================================

#[test]
fn should_include_samples_within_range_window() {
    // given: one series with samples 10..=40 at step 10; range=20ms,
    // step_ms=10, a single step t=30 → window (10, 30] → {20, 30}.
    let source = MockSource::new(vec![(
        vec![10, 20, 30, 40, 50],
        vec![1.0, 2.0, 3.0, 4.0, 5.0],
    )]);
    let grid = StepGrid {
        start_ms: 30,
        end_ms: 30,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        20,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: cell (step 0, series 0) = [(20, 2.0), (30, 3.0)]
    let cell = cell_samples(&batches, 0, 0);
    assert_eq!(cell, vec![(20, 2.0), (30, 3.0)]);
}

#[test]
fn should_merge_cross_bucket_refs_into_one_window_series() {
    // given: one logical output series backed by two bucket-local refs.
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
    let grid = StepGrid {
        start_ms: 20,
        end_ms: 20,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1 << 20);
    let mut op = MatrixSelectorOp::new(
        source,
        schema,
        request_series,
        grid,
        None,
        None,
        15,
        reservation,
        BatchShape::new(1, 1),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then
    assert_eq!(cell_samples(&batches, 0, 0), vec![(10, 1.0), (20, 2.0)]);
}

#[test]
fn should_exclude_samples_at_exclusive_window_start() {
    // given: sample at exactly `t - range` (the exclusive lower
    // bound) must NOT appear in the cell.
    let source = MockSource::new(vec![(vec![10, 15, 30], vec![1.0, 2.0, 3.0])]);
    // step t=30, range=20 → window (10, 30] — sample at t=10 excluded
    let grid = StepGrid {
        start_ms: 30,
        end_ms: 30,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        20,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then
    let cell = cell_samples(&batches, 0, 0);
    assert_eq!(cell, vec![(15, 2.0), (30, 3.0)]);
}

#[test]
fn should_include_sample_at_inclusive_window_end() {
    // given: sample at exactly `t` (the inclusive upper bound) must
    // appear in the cell.
    let source = MockSource::new(vec![(vec![25, 30], vec![1.0, 2.0])]);
    let grid = StepGrid {
        start_ms: 30,
        end_ms: 30,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        10,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: window (20, 30] → {25, 30}
    let cell = cell_samples(&batches, 0, 0);
    assert_eq!(cell, vec![(25, 1.0), (30, 2.0)]);
}

#[test]
fn should_apply_offset_shifting_range_window() {
    // given: offset=10ms positive — the window pins at (t - range -
    // 10, t - 10]. Sample at t=20 must appear for step=30 with
    // range=15 and offset=10 (window (5, 20]).
    let source = MockSource::new(vec![(vec![5, 20, 30], vec![1.0, 2.0, 3.0])]);
    let grid = StepGrid {
        start_ms: 30,
        end_ms: 30,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        Some(Offset::Pos(Duration::from_millis(10))),
        15,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: effective = 30 - 10 = 20, window (5, 20] → {20}
    let cell = cell_samples(&batches, 0, 0);
    assert_eq!(cell, vec![(20, 2.0)]);
}

#[test]
fn should_apply_at_timestamp_pinning() {
    // given: @ 100 — all steps pin to 100; range=50 → window (50,
    // 100] for every step.
    let source = MockSource::new(vec![(vec![30, 60, 90, 120], vec![1.0, 2.0, 3.0, 4.0])]);
    let grid = mk_grid(0, 10, 5); // steps 0,10,20,30,40
    let at_time = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(100);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        Some(AtModifier::At(at_time)),
        None,
        50,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: every step's cell = {60, 90} (sample at 30 excluded
    // by exclusive start; 120 excluded by inclusive end).
    for step in 0..5 {
        let cell = cell_samples(&batches, step, 0);
        assert_eq!(
            cell,
            vec![(60, 2.0), (90, 3.0)],
            "step {step} must see the @-pinned window"
        );
    }
}

#[test]
fn should_yield_empty_window_when_no_samples_in_range() {
    // given: samples far outside the window — hole in data for the
    // step we're asking about.
    let source = MockSource::new(vec![(vec![1000, 2000], vec![1.0, 2.0])]);
    let grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        50,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: cell empty; batch shape still reports the step & series
    assert_eq!(batches.len(), 1);
    let cell = cell_samples(&batches, 0, 0);
    assert!(cell.is_empty());
    assert_eq!(batches[0].cells[0], CellIndex::EMPTY);
}

#[test]
fn should_treat_stale_nan_as_absence() {
    // given: mix of good and STALE_NAN samples — STALE_NAN must not
    // appear in the window. Matches 3a.1's stricter policy.
    let stale = f64::from_bits(STALE_NAN);
    let source = MockSource::new(vec![(vec![10, 20, 30, 40], vec![1.0, stale, 3.0, stale])]);
    // step t=40, range=40 → window (0, 40] → all four source
    // samples, but filter drops the STALE_NANs.
    let grid = StepGrid {
        start_ms: 40,
        end_ms: 40,
        step_ms: 10,
        step_count: 1,
    };
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        40,
        reservation,
        BatchShape::default(),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: only the two good samples make it through
    let cell = cell_samples(&batches, 0, 0);
    assert_eq!(cell, vec![(10, 1.0), (30, 3.0)]);
}

#[test]
fn should_respect_memory_reservation() {
    // given: a tiny cap that cannot fit even the cell-index array.
    let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0]); 4]);
    let grid = mk_grid(10, 10, 8);
    let tiny = MemoryReservation::new(16);
    let mut op = make_op(source, grid, None, None, 20, tiny, BatchShape::new(8, 4));

    // when
    let results = drain_windows(&mut op);

    // then: at least one result is a MemoryLimit error.
    let err = results
        .into_iter()
        .find_map(|r| r.err())
        .expect("expected a MemoryLimit error");
    assert!(matches!(err, QueryError::MemoryLimit { .. }));
}

#[test]
fn should_return_static_schema() {
    // given
    let source = MockSource::new(vec![(vec![10], vec![1.0])]);
    let grid = mk_grid(10, 10, 1);
    let reservation = MemoryReservation::new(1_000_000);
    let op = make_op(
        source,
        grid,
        None,
        None,
        10,
        reservation,
        BatchShape::default(),
    );

    // when
    let schema = op.schema();

    // then
    assert!(!schema.series.is_deferred());
    assert!(schema.series.as_static().is_some());
}

#[test]
fn should_yield_end_of_windows_stream() {
    // given: a small operator — drive to exhaustion then re-poll.
    let source = MockSource::new(vec![(vec![10, 20], vec![1.0, 2.0])]);
    let grid = mk_grid(20, 10, 2);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        20,
        reservation,
        BatchShape::default(),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut count = 0usize;
    loop {
        match op.windows(&mut cx) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(Ok(_))) => count += 1,
            Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
            Poll::Pending => panic!("unexpected Pending"),
        }
    }
    // then
    assert!(count >= 1);
    match op.windows(&mut cx) {
        Poll::Ready(None) => {}
        other => panic!("expected Ready(None) after exhaustion, got {other:?}"),
    }
}

// ---- extra coverage ------------------------------------------------

#[test]
fn should_report_degenerate_next_as_end_of_stream() {
    // given: the `Operator::next` entry point is degenerate — matrix
    // cells cannot fit `StepBatch`'s single-float layout.
    let source = MockSource::new(vec![(vec![10, 20], vec![1.0, 2.0])]);
    let grid = mk_grid(20, 10, 2);
    let reservation = MemoryReservation::new(1_000_000);
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        20,
        reservation,
        BatchShape::default(),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let polled = <MatrixSelectorOp<'static, MockSource> as Operator>::next(&mut op, &mut cx);

    // then: immediate Ready(None) — consumers use `windows()` instead
    match polled {
        Poll::Ready(None) => {}
        other => panic!("expected Ready(None) from Operator::next, got {other:?}"),
    }
}

#[test]
fn should_slide_window_monotonically_across_steps() {
    // given: three steps walk a window of 20ms forward — two-pointer
    // cursor must advance monotonically without dropping samples that
    // slide in or re-emitting samples that slide out.
    let source = MockSource::new(vec![(
        vec![0, 5, 15, 25, 35, 45, 55],
        vec![0.0, 5.0, 15.0, 25.0, 35.0, 45.0, 55.0],
    )]);
    let grid = mk_grid(20, 10, 3); // steps 20, 30, 40
    let reservation = MemoryReservation::new(1_000_000);
    // One-step-chunk forces a fresh `build_window_batch` call per step,
    // exercising the cursor across multiple emit cycles.
    let mut op = make_op(
        source,
        grid,
        None,
        None,
        20,
        reservation,
        BatchShape::new(1, 1),
    );

    // when
    let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // then: step 20 → (0, 20] = {5, 15}; step 30 → (10, 30] = {15, 25};
    // step 40 → (20, 40] = {25, 35}
    assert_eq!(cell_samples(&batches, 0, 0), vec![(5, 5.0), (15, 15.0)]);
    assert_eq!(cell_samples(&batches, 1, 0), vec![(15, 15.0), (25, 25.0)]);
    assert_eq!(cell_samples(&batches, 2, 0), vec![(25, 25.0), (35, 35.0)]);
}
