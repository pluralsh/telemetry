use super::*;
use crate::model::{Label, Labels, STALE_NAN};
use crate::promql::batch::BitSet;
use crate::promql::operators::rollup::{RollupKind, RollupOp};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

fn noop_waker() -> Waker {
    futures::task::noop_waker()
}

fn mk_labels(n: usize) -> Arc<SeriesSchema> {
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

/// Minimal mock "inner plan" operator: returns a pre-scripted queue of
/// `StepBatch`es (or errors), then end-of-stream.
struct ScriptedChild {
    schema: OperatorSchema,
    queue: Vec<Result<StepBatch, QueryError>>,
}

impl ScriptedChild {
    fn new(schema: OperatorSchema, batches: Vec<StepBatch>) -> Self {
        Self {
            schema,
            queue: batches.into_iter().map(Ok).collect(),
        }
    }

    fn with_error(schema: OperatorSchema, err: QueryError) -> Self {
        Self {
            schema,
            queue: vec![Err(err)],
        }
    }
}

impl Operator for ScriptedChild {
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

/// Build a single-step batch covering one inner step with `series_count`
/// series. `values[i]` is the value for series i; `None` → validity 0.
fn mk_batch(
    step_timestamps: Arc<[i64]>,
    step_idx: usize,
    series: Arc<SeriesSchema>,
    values: &[Option<f64>],
) -> StepBatch {
    let n = values.len();
    let mut vs = Vec::with_capacity(n);
    let mut validity = BitSet::with_len(n);
    for (i, v) in values.iter().enumerate() {
        match v {
            Some(x) => {
                vs.push(*x);
                validity.set(i);
            }
            None => vs.push(0.0),
        }
    }
    StepBatch::new(
        step_timestamps,
        step_idx..(step_idx + 1),
        SchemaRef::Static(series),
        0..n,
        vs,
        validity,
    )
}

/// Build an inner-grid schema + timestamps for `[outer_t - range, outer_t]`
/// at `inner_step`.
fn mk_inner_grid(outer_t: i64, range_ms: i64, inner_step_ms: i64) -> (Arc<[i64]>, StepGrid) {
    // Inner grid: first step strictly > outer_t - range, spaced by
    // inner_step, up to and including outer_t. Mirrors the production
    // `[range:step]` semantics.
    let mut ts = Vec::new();
    let mut t = outer_t
        .saturating_sub(range_ms)
        .saturating_add(inner_step_ms);
    while t <= outer_t {
        ts.push(t);
        t += inner_step_ms;
    }
    let start_ms = ts.first().copied().unwrap_or(outer_t);
    let end_ms = ts.last().copied().unwrap_or(outer_t);
    let step_count = ts.len();
    (
        Arc::from(ts),
        StepGrid {
            start_ms,
            end_ms,
            step_ms: inner_step_ms,
            step_count,
        },
    )
}

// -----------------------------------------------------------------------
// required tests
// -----------------------------------------------------------------------

#[test]
fn should_regrid_child_onto_inner_step() {
    // given: outer step at t=100, range=30ms, inner_step=10ms, outer
    // step=60ms (single outer step here). Window: (70, 100] ⇒ inner
    // ts should be 80, 90, 100.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();

    let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
        // assert inner contract
        assert_eq!(tr.start_ms, 71); // 100 - 30 + 1
        assert_eq!(tr.end_ms_exclusive, 101); // 100 + 1
        assert_eq!(step_ms, 10);
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, 10);
        let mut batches = Vec::new();
        for (i, &t) in ts_arc.iter().enumerate() {
            batches.push(mk_batch(
                ts_arc.clone(),
                i,
                series_clone.clone(),
                &[Some(t as f64)],
            ));
        }
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let batch = match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected poll: {other:?}"),
    };

    // then: single cell for the outer step contains ts/vs 80, 90, 100.
    assert_eq!(batch.step_count(), 1);
    assert_eq!(batch.series_count(), 1);
    let (ts, vs) = batch.cell_samples(0, 0);
    assert_eq!(ts, &[80, 90, 100]);
    assert_eq!(vs, &[80.0, 90.0, 100.0]);
}

#[test]
fn should_emit_one_matrix_batch_per_outer_step() {
    // given: outer grid with 3 steps
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 220,
        step_ms: 60,
        step_count: 3,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();

    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches: Vec<StepBatch> = ts_arc
            .iter()
            .enumerate()
            .map(|(i, &t)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(t as f64)]))
            .collect();
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut count = 0;
    loop {
        match op.windows(&mut cx) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(Ok(_))) => count += 1,
            Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
            Poll::Pending => panic!("unexpected Pending"),
        }
    }
    // then: exactly one batch per outer step (3).
    assert_eq!(count, 3);
}

#[test]
fn should_invoke_factory_once_per_outer_step() {
    // given: outer grid with 4 steps; factory counter increments per call.
    let outer_grid = StepGrid {
        start_ms: 60,
        end_ms: 240,
        step_ms: 60,
        step_count: 4,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();

    let counter = Arc::new(Mutex::new(0usize));
    let counter_clone = counter.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        *counter_clone.lock().unwrap() += 1;
        let (ts_arc, inner_grid) = mk_inner_grid(60, 30, step_ms);
        let batches: Vec<StepBatch> = ts_arc
            .iter()
            .enumerate()
            .map(|(i, _)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(1.0)]))
            .collect();
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when: drive to completion
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    while let Poll::Ready(Some(Ok(_))) = op.windows(&mut cx) {}

    // then: factory called once per outer step.
    assert_eq!(*counter.lock().unwrap(), 4);
}

/// Returns `Pending` on its first poll, like a child waiting on storage.
struct PendingOnce {
    inner: ScriptedChild,
    pending: bool,
}

impl Operator for PendingOnce {
    fn schema(&self) -> &OperatorSchema {
        self.inner.schema()
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if std::mem::take(&mut self.pending) {
            return Poll::Pending;
        }
        self.inner.next(cx)
    }
}

#[test]
fn should_resume_a_pending_child_instead_of_replanning() {
    // given: one outer step whose child is pending on its first poll
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();
    let calls = Arc::new(Mutex::new(0usize));
    let calls_clone = calls.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        *calls_clone.lock().unwrap() += 1;
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = (0..ts_arc.len())
            .map(|i| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(i as f64)]))
            .collect();
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(PendingOnce {
            inner: ScriptedChild::new(schema, batches),
            pending: true,
        }) as Box<dyn Operator + Send>)
    });
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // when
    assert!(op.windows(&mut cx).is_pending());
    let batch = match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected: {other:?}"),
    };

    // then: the same child finished the step
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(batch.cell_samples(0, 0).1, &[0.0, 1.0, 2.0]);
}

#[test]
fn should_pack_samples_into_matrix_window_batch_layout() {
    // given: 2 series, outer t=100, range=30, inner=10. Inner ts 80, 90, 100.
    // Series 0: [1.0, 2.0, 3.0]; Series 1: [10.0, 20.0, 30.0].
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(2);
    let series_clone = series.clone();

    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = vec![
            mk_batch(
                ts_arc.clone(),
                0,
                series_clone.clone(),
                &[Some(1.0), Some(10.0)],
            ),
            mk_batch(
                ts_arc.clone(),
                1,
                series_clone.clone(),
                &[Some(2.0), Some(20.0)],
            ),
            mk_batch(
                ts_arc.clone(),
                2,
                series_clone.clone(),
                &[Some(3.0), Some(30.0)],
            ),
        ];
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let batch = match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected: {other:?}"),
    };

    // then: `cells` length = step_count * series_count = 2; per-cell
    // indexing yields the right samples.
    assert_eq!(batch.cells.len(), 2);
    let (ts0, vs0) = batch.cell_samples(0, 0);
    assert_eq!(ts0, &[80, 90, 100]);
    assert_eq!(vs0, &[1.0, 2.0, 3.0]);
    let (ts1, vs1) = batch.cell_samples(0, 1);
    assert_eq!(ts1, &[80, 90, 100]);
    assert_eq!(vs1, &[10.0, 20.0, 30.0]);
}

#[test]
fn should_yield_end_of_stream_when_outer_grid_exhausted() {
    // given: 2 outer steps
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 160,
        step_ms: 60,
        step_count: 2,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = vec![mk_batch(
            ts_arc.clone(),
            0,
            series_clone.clone(),
            &[Some(1.0)],
        )];
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when: drain exactly 2 batches, then one more poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
    assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
    // then: end-of-stream
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
    // idempotent
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_preserve_series_order_from_child() {
    // given: 3 series. Inner emits values that encode the series index.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(3);
    let series_clone = series.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = vec![mk_batch(
            ts_arc.clone(),
            0,
            series_clone.clone(),
            &[Some(0.0), Some(1.0), Some(2.0)],
        )];
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });
    // Inner step large enough to produce only one inner step inside
    // the window (80), so the scripted child's single batch stands alone.
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        30,
        MemoryReservation::new(1 << 20),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let batch = match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected: {other:?}"),
    };

    // then: per-series cells carry the expected value in the right slot.
    for s in 0..3 {
        let (_, vs) = batch.cell_samples(0, s);
        assert_eq!(vs, &[s as f64], "series {s} value in wrong slot");
    }
}

#[test]
fn should_skip_invalid_cells_in_inner_output() {
    // given: one series, 3 inner steps; the middle step's cell has
    // validity=0 and one cell carries STALE_NAN.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();
    let stale = f64::from_bits(STALE_NAN);
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = vec![
            mk_batch(ts_arc.clone(), 0, series_clone.clone(), &[Some(1.0)]),
            mk_batch(ts_arc.clone(), 1, series_clone.clone(), &[None]),
            mk_batch(ts_arc.clone(), 2, series_clone.clone(), &[Some(stale)]),
        ];
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let batch = match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected: {other:?}"),
    };

    // then: only the single good sample at ts=80 made it through.
    let (ts, vs) = batch.cell_samples(0, 0);
    assert_eq!(ts, &[80]);
    assert_eq!(vs, &[1.0]);
}

#[test]
fn should_propagate_error_from_child() {
    // given: child errors out on first poll.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (_ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::with_error(
            schema,
            QueryError::Internal("boom".into()),
        )) as Box<dyn Operator + Send>)
    });
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // then: error propagated verbatim.
    match op.windows(&mut cx) {
        Poll::Ready(Some(Err(QueryError::Internal(msg)))) => assert_eq!(msg, "boom"),
        other => panic!("expected Internal error, got {other:?}"),
    }
    // Once errored, subsequent polls return end-of-stream.
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_respect_memory_reservation() {
    // given: tiny cap cannot fit even the cell-index allocation.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(4);
    let series_clone = series.clone();
    let factory: ChildFactory = Box::new(move |_tr, step_ms| {
        let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
        let batches = vec![mk_batch(
            ts_arc.clone(),
            0,
            series_clone.clone(),
            &[Some(1.0); 4],
        )];
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });
    let mut op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(4),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let err = match op.windows(&mut cx) {
        Poll::Ready(Some(Err(e))) => e,
        other => panic!("expected MemoryLimit, got {other:?}"),
    };
    assert!(matches!(err, QueryError::MemoryLimit { .. }));
}

#[test]
fn should_return_static_schema() {
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let factory: ChildFactory = Box::new(move |_tr, _step_ms| {
        // Factory is never invoked in this test (no poll).
        unreachable!("factory not invoked for schema-only test");
    });
    let op = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // when/then
    let schema = <SubqueryOp as Operator>::schema(&op);
    assert!(!schema.series.is_deferred());
    assert!(schema.series.as_static().is_some());
}

#[test]
fn should_plug_into_rollup_end_to_end() {
    // given: analog of `rate(expr[3s:1s])` — 2 outer steps, range=30ms,
    // inner step=10ms. Inner values are a perfect 10/s counter:
    // at ts t (ms) the value is t/10. Each inner window carries 3
    // samples spanning the range.
    //
    // First outer step (t=100): window (70, 100] → inner ts {80, 90, 100},
    // values {8, 9, 10}. Second outer step (t=130): window (100, 130]
    // → inner ts {110, 120, 130}, values {11, 12, 13}.
    //
    // Rollup's `rate` extrapolation:
    //   result = last - first; time_diff = (last_t - first_t)/1000
    //   duration_to_start = (first_t - window_start)/1000
    //   duration_to_end = (window_end - last_t)/1000
    //   range_seconds = range_ms/1000 = 0.03
    //
    // Step 1: first=8@80, last=10@100; time_diff = 0.02; avg_interval=0.01;
    //   duration_to_start = (80 - 70)/1000 = 0.01 (< threshold 0.011 → keep).
    //   Counter-zero clip: result>0 && first>=0 → duration_to_zero = 8*(0.02/2) = 0.08;
    //   duration_to_zero >= duration_to_start so no clip applied.
    //   duration_to_end = 0 → factor_unit = (0.02+0.01+0)/0.02 = 1.5
    //   rate = 2 * 1.5 / 0.03 = 100/s.
    //
    // Step 2: same geometry, result = 2. rate = 100/s.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 130,
        step_ms: 30,
        step_count: 2,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();

    let call_idx = Rc::new(RefCell::new(0u64));
    // The factory is `Send` but we want the counter for debugging; keep
    // it side-effect-free so the callback can stay `Send`.
    let _ = call_idx;

    let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
        // Compute outer_t from the window range: end_ms_exclusive - 1.
        let outer_t = tr.end_ms_exclusive - 1;
        let (ts_arc, inner_grid) = mk_inner_grid(outer_t, 30, step_ms);
        let batches: Vec<StepBatch> = ts_arc
            .iter()
            .enumerate()
            .map(|(i, &t)| {
                mk_batch(
                    ts_arc.clone(),
                    i,
                    series_clone.clone(),
                    &[Some((t / 10) as f64)],
                )
            })
            .collect();
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let subquery = SubqueryOp::new(
        factory,
        series,
        outer_grid,
        30,
        10,
        MemoryReservation::new(1 << 20),
    );

    // Feed subquery directly into RollupOp<SubqueryOp> via WindowStream.
    let mut rollup = RollupOp::new(
        subquery,
        RollupKind::Rate,
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut batches = Vec::new();
    loop {
        match rollup.next(&mut cx) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(Ok(b))) => batches.push(b),
            Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
            Poll::Pending => panic!("unexpected Pending"),
        }
    }

    // then: two step batches, each with rate = 100/s on cell (0, 0).
    assert_eq!(batches.len(), 2);
    let a = batches[0].get(0, 0).expect("rate valid");
    let b = batches[1].get(0, 0).expect("rate valid");
    assert!((a - 100.0).abs() < 1e-9, "first rate = {a}");
    assert!((b - 100.0).abs() < 1e-9, "second rate = {b}");
}

#[test]
fn should_use_effective_times_for_inner_window() {
    // given: one outer step with `effective_times[0] = 200` — the
    // inner window must cover `(200 - range, 200]` even though the
    // outer step timestamp is `100`. This models an `@ 200` subquery
    // evaluated at `outer_t = 100`.
    let outer_grid = StepGrid {
        start_ms: 100,
        end_ms: 100,
        step_ms: 60,
        step_count: 1,
    };
    let series = mk_labels(1);
    let series_clone = series.clone();
    let received_range: Arc<Mutex<Option<TimeRange>>> = Arc::new(Mutex::new(None));
    let received_range_clone = received_range.clone();

    let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
        *received_range_clone.lock().unwrap() = Some(tr);
        let (ts_arc, inner_grid) = mk_inner_grid(tr.end_ms_exclusive - 1, 30, step_ms);
        let batches: Vec<StepBatch> = ts_arc
            .iter()
            .enumerate()
            .map(|(i, _)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(1.0)]))
            .collect();
        let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
        Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
    });

    let mut op = SubqueryOp::with_effective_times(
        factory,
        series,
        outer_grid,
        30,
        10,
        Arc::from(vec![200i64]),
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let _ = op.windows(&mut cx);

    // then: inner window covers `(170, 200]` — i.e. `start=171,
    // end_exclusive=201` — ignoring the outer step timestamp entirely.
    let tr = received_range.lock().unwrap().expect("factory invoked");
    assert_eq!(tr.start_ms, 171);
    assert_eq!(tr.end_ms_exclusive, 201);
}
