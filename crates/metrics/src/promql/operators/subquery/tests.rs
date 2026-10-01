use super::*;
use crate::model::{Label, Labels, STALE_NAN};
use crate::promql::batch::BitSet;
use crate::promql::operators::rollup::{RollupKind, RollupOp};
use std::sync::Arc;
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

/// A child evaluated at `ts`, one batch per inner step in the order
/// given; `value(t, series)` is the cell at `(t, series)`.
fn scripted_child(
    series: &Arc<SeriesSchema>,
    ts: &[i64],
    value: impl Fn(i64, usize) -> Option<f64>,
) -> ScriptedChild {
    let ts_arc: Arc<[i64]> = Arc::from(ts);
    let queue = ts
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let values: Vec<Option<f64>> = (0..series.len()).map(|s| value(t, s)).collect();
            Ok(mk_batch(ts_arc.clone(), i, series.clone(), &values))
        })
        .collect();
    let grid = StepGrid {
        start_ms: ts.iter().copied().min().unwrap_or(0),
        end_ms: ts.iter().copied().max().unwrap_or(0),
        step_ms: 10,
        step_count: ts.len(),
    };
    ScriptedChild {
        schema: OperatorSchema::new(SchemaRef::Static(series.clone()), grid),
        queue,
    }
}

fn boxed(child: impl Operator + 'static) -> Box<dyn Operator + Send> {
    Box::new(child)
}

fn outer(start_ms: i64, step_ms: i64, step_count: usize) -> StepGrid {
    StepGrid {
        start_ms,
        end_ms: start_ms + step_ms * (step_count as i64 - 1),
        step_ms,
        step_count,
    }
}

fn next_window(op: &mut SubqueryOp) -> MatrixWindowBatch {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match op.windows(&mut cx) {
        Poll::Ready(Some(Ok(b))) => b,
        other => panic!("unexpected poll: {other:?}"),
    }
}

fn drain_windows(op: &mut SubqueryOp) -> Vec<MatrixWindowBatch> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    loop {
        match op.windows(&mut cx) {
            Poll::Ready(None) => return out,
            Poll::Ready(Some(Ok(b))) => out.push(b),
            Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
            Poll::Pending => panic!("unexpected Pending"),
        }
    }
}

#[test]
fn should_slice_outer_window_from_inner_samples() {
    // given: outer step at t=100, range=30 → window (70, 100]; the child
    // also emits a point at 70, outside every window.
    let series = mk_labels(1);
    let child = scripted_child(&series, &[70, 80, 90, 100], |t, _| Some(t as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then
    assert_eq!(batch.step_count(), 1);
    assert_eq!(batch.series_count(), 1);
    let (ts, vs) = batch.cell_samples(0, 0);
    assert_eq!(ts, &[80, 90, 100]);
    assert_eq!(vs, &[80.0, 90.0, 100.0]);
}

#[test]
fn should_emit_one_matrix_batch_per_outer_step() {
    // given: outer grid with 3 steps
    let series = mk_labels(1);
    let ts: Vec<i64> = (8..=22).map(|k| k * 10).collect();
    let child = scripted_child(&series, &ts, |t, _| Some(t as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 3),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when / then: exactly one batch per outer step
    assert_eq!(drain_windows(&mut op).len(), 3);
}

#[test]
fn should_slice_every_outer_window_from_one_evaluation() {
    // given: 4 outer steps 60s apart with a 30ms range; one child covers
    // them all.
    let series = mk_labels(1);
    let ts: Vec<i64> = (4..=24).map(|k| k * 10).collect();
    let child = scripted_child(&series, &ts, |t, _| Some(t as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(60, 60, 4),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let windows = drain_windows(&mut op);

    // then: each window holds exactly its own (t - 30, t] points
    let got: Vec<Vec<i64>> = windows
        .iter()
        .map(|b| b.cell_samples(0, 0).0.to_vec())
        .collect();
    assert_eq!(
        got,
        vec![
            vec![40, 50, 60],
            vec![100, 110, 120],
            vec![160, 170, 180],
            vec![220, 230, 240],
        ]
    );
}

#[test]
fn should_share_samples_between_overlapping_windows() {
    // given: range (30) wider than the outer step (10)
    let series = mk_labels(1);
    let child = scripted_child(&series, &[80, 90, 100, 110], |t, _| Some(t as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 10, 2),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let windows = drain_windows(&mut op);

    // then
    assert_eq!(windows[0].cell_samples(0, 0).0, &[80, 90, 100]);
    assert_eq!(windows[1].step_range, 1..2);
    assert_eq!(windows[1].cell_samples(0, 0).0, &[90, 100, 110]);
}

#[test]
fn should_order_samples_when_child_emits_steps_out_of_order() {
    // given: the child's step chunks arrive newest-first
    let series = mk_labels(1);
    let child = scripted_child(&series, &[100, 90, 80], |t, _| Some(t as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then
    assert_eq!(
        batch.cell_samples(0, 0),
        (&[80, 90, 100][..], &[80.0, 90.0, 100.0][..])
    );
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
fn should_resume_a_pending_child() {
    // given: a child pending on its first poll
    let series = mk_labels(1);
    let child = PendingOnce {
        inner: scripted_child(&series, &[80, 90, 100], |t, _| Some((t / 10 - 8) as f64)),
        pending: true,
    };
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // when
    assert!(op.windows(&mut cx).is_pending());
    let batch = next_window(&mut op);

    // then
    assert_eq!(batch.cell_samples(0, 0).1, &[0.0, 1.0, 2.0]);
}

#[test]
fn should_pack_samples_into_matrix_window_batch_layout() {
    // given: 2 series; series 0 is t/80-ish 1,2,3 and series 1 ten times that
    let series = mk_labels(2);
    let child = scripted_child(&series, &[80, 90, 100], |t, s| {
        let base = (t / 10 - 7) as f64;
        Some(if s == 0 { base } else { base * 10.0 })
    });
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then: `cells` length = step_count * series_count = 2
    assert_eq!(batch.cells.len(), 2);
    assert_eq!(
        batch.cell_samples(0, 0),
        (&[80, 90, 100][..], &[1.0, 2.0, 3.0][..])
    );
    assert_eq!(
        batch.cell_samples(0, 1),
        (&[80, 90, 100][..], &[10.0, 20.0, 30.0][..])
    );
}

#[test]
fn should_yield_end_of_stream_when_outer_grid_exhausted() {
    // given: 2 outer steps
    let series = mk_labels(1);
    let child = scripted_child(&series, &[80], |_, _| Some(1.0));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 2),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when: drain exactly 2 batches, then one more poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
    assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
    // then: end-of-stream, idempotently
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_preserve_series_order_from_child() {
    // given: 3 series whose value encodes the series index
    let series = mk_labels(3);
    let child = scripted_child(&series, &[90], |_, s| Some(s as f64));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then
    for s in 0..3 {
        assert_eq!(
            batch.cell_samples(0, s).1,
            &[s as f64],
            "series {s} value in wrong slot"
        );
    }
}

#[test]
fn should_skip_invalid_cells_in_inner_output() {
    // given: 3 inner steps; the middle is invalid, the last STALE_NAN
    let series = mk_labels(1);
    let stale = f64::from_bits(STALE_NAN);
    let child = scripted_child(&series, &[80, 90, 100], |t, _| match t {
        80 => Some(1.0),
        90 => None,
        _ => Some(stale),
    });
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then: only the good sample at ts=80 made it through
    assert_eq!(batch.cell_samples(0, 0), (&[80][..], &[1.0][..]));
}

#[test]
fn should_propagate_error_from_child() {
    // given: child errors out on first poll
    let series = mk_labels(1);
    let mut child = scripted_child(&series, &[], |_, _| None);
    child.queue.push(Err(QueryError::Internal("boom".into())));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // then: error propagated verbatim, then end-of-stream
    match op.windows(&mut cx) {
        Poll::Ready(Some(Err(QueryError::Internal(msg)))) => assert_eq!(msg, "boom"),
        other => panic!("expected Internal error, got {other:?}"),
    }
    assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_respect_memory_reservation() {
    // given: a cap too small for the drained inner samples
    let series = mk_labels(4);
    let child = scripted_child(&series, &[80], |_, _| Some(1.0));
    let mut op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
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
fn should_release_inner_samples_on_drop() {
    // given
    let reservation = MemoryReservation::new(1 << 20);
    let series = mk_labels(2);
    let child = scripted_child(&series, &[80, 90, 100], |_, _| Some(1.0));
    let mut op = SubqueryOp::new(boxed(child), outer(100, 60, 1), 30, reservation.clone());
    drop(next_window(&mut op));
    assert!(reservation.reserved() > 0);

    // when
    drop(op);

    // then
    assert_eq!(reservation.reserved(), 0);
}

#[test]
fn should_take_static_schema_from_child() {
    let series = mk_labels(2);
    let child = scripted_child(&series, &[], |_, _| None);
    let op = SubqueryOp::new(
        boxed(child),
        outer(100, 60, 1),
        30,
        MemoryReservation::new(1 << 20),
    );

    let schema = <SubqueryOp as Operator>::schema(&op);
    assert_eq!(schema.series.as_static().expect("static").len(), 2);
}

#[test]
fn should_plug_into_rollup_end_to_end() {
    // given: analog of `rate(expr[3s:1s])` — 2 outer steps, range=30ms,
    // inner step=10ms. Inner values are a perfect 10/s counter: at ts t
    // (ms) the value is t/10.
    //
    // First outer step (t=100): window (70, 100] → inner ts {80, 90, 100},
    // values {8, 9, 10}. Second outer step (t=130): window (100, 130]
    // → inner ts {110, 120, 130}, values {11, 12, 13}.
    //
    // Rollup's `rate` extrapolation per step: first=8@80, last=10@100;
    // time_diff = 0.02; duration_to_start = 0.01 (< threshold 0.011 →
    // keep); duration_to_end = 0 → factor = 1.5; rate = 2 * 1.5 / 0.03
    // = 100/s. Step 2 has the same geometry.
    let series = mk_labels(1);
    let ts: Vec<i64> = (8..=13).map(|k| k * 10).collect();
    let child = scripted_child(&series, &ts, |t, _| Some((t / 10) as f64));
    let subquery = SubqueryOp::new(
        boxed(child),
        outer(100, 30, 2),
        30,
        MemoryReservation::new(1 << 20),
    );
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

    // then: two step batches, each with rate = 100/s on cell (0, 0)
    assert_eq!(batches.len(), 2);
    let a = batches[0].get(0, 0).expect("rate valid");
    let b = batches[1].get(0, 0).expect("rate valid");
    assert!((a - 100.0).abs() < 1e-9, "first rate = {a}");
    assert!((b - 100.0).abs() < 1e-9, "second rate = {b}");
}

#[test]
fn should_use_effective_times_for_inner_window() {
    // given: one outer step at t=100 with `effective_times[0] = 200` —
    // an `@ 200` subquery. The window must be (170, 200].
    let series = mk_labels(1);
    let child = scripted_child(&series, &[90, 100, 180, 190, 200], |t, _| Some(t as f64));
    let mut op = SubqueryOp::with_effective_times(
        boxed(child),
        outer(100, 60, 1),
        30,
        Arc::from(vec![200i64]),
        MemoryReservation::new(1 << 20),
    );

    // when
    let batch = next_window(&mut op);

    // then: sliced at the effective time, not the outer step timestamp
    assert_eq!(batch.cell_samples(0, 0).0, &[180, 190, 200]);
    assert_eq!(batch.effective_times.as_deref(), Some(&[200i64][..]));
}

#[test]
fn should_span_union_of_outer_windows() {
    assert_eq!(subquery_span(&[100, 160, 220], 30), (71, 220));
    assert_eq!(subquery_span(&[200, 200], 30), (171, 200));
}
