use super::*;
use crate::model::{Label, Labels};
use crate::promql::batch::{BitSet, SchemaRef, SeriesSchema};
use crate::promql::operator::StepGrid;
use std::sync::Arc;
use std::task::Waker;

// ---- test fixtures ------------------------------------------------------

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

fn mk_operator_schema(step_timestamps: &[i64], step_ms: i64, series: usize) -> OperatorSchema {
    let start_ms = step_timestamps.first().copied().unwrap_or(0);
    let end_ms = step_timestamps.last().copied().unwrap_or(0);
    OperatorSchema::new(
        SchemaRef::Static(mk_schema(series)),
        StepGrid {
            start_ms,
            end_ms,
            step_ms,
            step_count: step_timestamps.len(),
        },
    )
}

/// Build a single `StepBatch` from a row-major `values` vector and a
/// parallel `validity` vector of `bool`s.
fn mk_batch(
    step_timestamps: Vec<i64>,
    series_count: usize,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    let step_count = step_timestamps.len();
    assert_eq!(values.len(), step_count * series_count);
    assert_eq!(validity.len(), values.len());
    let schema = mk_schema(series_count);
    let mut bits = BitSet::with_len(validity.len());
    for (i, &b) in validity.iter().enumerate() {
        if b {
            bits.set(i);
        }
    }
    StepBatch::new(
        Arc::from(step_timestamps),
        0..step_count,
        SchemaRef::Static(schema),
        0..series_count,
        values,
        bits,
    )
}

/// Mock upstream operator. Yields a pre-built queue of batches (or
/// errors) in order, then `Ready(None)` forever.
struct MockChild {
    schema: OperatorSchema,
    queue: Vec<Result<StepBatch, QueryError>>,
}

impl MockChild {
    fn new(schema: OperatorSchema, batches: Vec<StepBatch>) -> Self {
        Self {
            schema,
            queue: batches.into_iter().map(Ok).collect(),
        }
    }

    fn with_queue(schema: OperatorSchema, queue: Vec<Result<StepBatch, QueryError>>) -> Self {
        Self { schema, queue }
    }
}

impl Operator for MockChild {
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

fn drive<C: Operator>(op: &mut InstantFnOp<C>) -> Vec<Result<StepBatch, QueryError>> {
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
fn should_apply_abs_to_each_cell() {
    // given: two steps × two series, mixed signs, all valid
    let ts = vec![1_000, 2_000];
    let values = vec![-1.0, 2.0, -3.5, 4.5];
    let validity = vec![true; 4];
    let batch = mk_batch(ts.clone(), 2, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 2);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.get(0, 0), Some(1.0));
    assert_eq!(b.get(0, 1), Some(2.0));
    assert_eq!(b.get(1, 0), Some(3.5));
    assert_eq!(b.get(1, 1), Some(4.5));
}

#[test]
fn should_preserve_validity_bits_from_child() {
    // given: 1 step × 3 series, middle cell invalid
    let ts = vec![1_000];
    let values = vec![-1.0, 999.0, 3.0];
    let validity = vec![true, false, true];
    let batch = mk_batch(ts.clone(), 3, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 3);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: invalid cell stays invalid; valid cells computed
    let b = &batches[0];
    assert_eq!(b.get(0, 0), Some(1.0));
    assert_eq!(b.get(0, 1), None);
    assert_eq!(b.get(0, 2), Some(3.0));
}

#[test]
fn should_apply_clamp_with_plan_time_bounds() {
    // given: values span below / within / above [2.0, 5.0]
    let ts = vec![1_000];
    let values = vec![1.0, 3.0, 6.0, f64::NAN];
    let validity = vec![true; 4];
    let batch = mk_batch(ts.clone(), 4, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 4);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Clamp { min: 2.0, max: 5.0 },
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &batches[0];
    assert_eq!(b.get(0, 0), Some(2.0));
    assert_eq!(b.get(0, 1), Some(3.0));
    assert_eq!(b.get(0, 2), Some(5.0));
    assert!(b.get(0, 3).unwrap().is_nan(), "NaN should propagate");
}

#[test]
fn should_emit_no_samples_for_clamp_when_min_exceeds_max() {
    // given
    let ts = vec![1_000];
    let values = vec![1.0, 3.0, 6.0];
    let validity = vec![true; 3];
    let batch = mk_batch(ts.clone(), 3, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 3);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Clamp {
            min: 5.0,
            max: -5.0,
        },
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let batch = &batches[0];
    assert_eq!(batch.get(0, 0), None);
    assert_eq!(batch.get(0, 1), None);
    assert_eq!(batch.get(0, 2), None);
}

#[test]
fn should_apply_clamp_min_and_clamp_max() {
    // given: 4 values exercising both tails
    let ts = vec![1_000];
    let values = vec![-1.0, 0.0, 1.0, 2.0];
    let validity = vec![true; 4];
    let schema = mk_operator_schema(&ts, 1_000, 4);

    let child = MockChild::new(
        schema.clone(),
        vec![mk_batch(ts.clone(), 4, values.clone(), validity.clone())],
    );
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::ClampMin { min: 0.0 },
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(0.0));
    assert_eq!(b.get(0, 1), Some(0.0));
    assert_eq!(b.get(0, 2), Some(1.0));
    assert_eq!(b.get(0, 3), Some(2.0));

    let child = MockChild::new(schema, vec![mk_batch(ts, 4, values, validity)]);
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::ClampMax { max: 1.0 },
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(-1.0));
    assert_eq!(b.get(0, 1), Some(0.0));
    assert_eq!(b.get(0, 2), Some(1.0));
    assert_eq!(b.get(0, 3), Some(1.0));
}

#[test]
fn should_handle_ln_of_nonpositive() {
    // given: ln(0) → -inf; ln(-1) → NaN (per Prometheus UnaryFunction
    // at timeseries/src/promql/functions.rs:203-211 which delegates to
    // f64::ln directly). Validity stays set in both cases — the legacy
    // engine writes `sample.value = self.op(...)` without flipping the
    // sample's presence.
    let ts = vec![1_000];
    let values = vec![0.0_f64, -1.0, 1.0, std::f64::consts::E];
    let validity = vec![true; 4];
    let batch = mk_batch(ts.clone(), 4, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 4);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Ln, MemoryReservation::new(1 << 20));
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &batches[0];
    let ln_zero = b.get(0, 0).expect("validity preserved across ln(0)");
    assert!(ln_zero.is_infinite() && ln_zero.is_sign_negative());
    let ln_neg = b.get(0, 1).expect("validity preserved across ln(-1)");
    assert!(ln_neg.is_nan());
    assert!(approx_eq(b.get(0, 2).unwrap(), 0.0, 1e-9));
    assert!(approx_eq(b.get(0, 3).unwrap(), 1.0, 1e-9));
}

#[test]
fn should_apply_timestamp_falling_back_to_step_time_without_source_column() {
    // given: two steps at t=1_500 ms and 3_000 ms; the input batch
    // carries no `source_timestamps` column (the producer was a
    // derived operator, e.g. `rate` or a binary op). `timestamp()`
    // therefore returns the step timestamp in seconds, matching
    // Prometheus' behaviour for derived inputs.
    let ts = vec![1_500, 3_000];
    let values = vec![7.0, -42.0];
    let validity = vec![true, true];
    let batch = mk_batch(ts.clone(), 1, values, validity);
    let schema = mk_operator_schema(&ts, 1_500, 1);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Timestamp,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &batches[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 1.5, 1e-9));
    assert!(approx_eq(b.get(1, 0).unwrap(), 3.0, 1e-9));
}

#[test]
fn should_apply_timestamp_returning_source_sample_timestamp_when_available() {
    // given: two steps at step_ms 1_500 / 3_000, but the input
    // batch was produced by a bare vector selector which stamped
    // per-cell source timestamps (the actual matching-sample times,
    // e.g. from an `@ t` pin). `timestamp()` must prefer those over
    // the step timestamp.
    let ts = vec![1_500, 3_000];
    let values = vec![7.0, -42.0];
    let validity = vec![true, true];
    let batch = mk_batch(ts.clone(), 1, values, validity)
        .with_source_timestamps(Arc::from(vec![10_000i64, 10_000]));
    let schema = mk_operator_schema(&ts, 1_500, 1);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Timestamp,
        MemoryReservation::new(1 << 20),
    );
    let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: both cells return the source sample timestamp (10s),
    // not the step time.
    let b = &batches[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 10.0, 1e-9));
    assert!(approx_eq(b.get(1, 0).unwrap(), 10.0, 1e-9));
    // and: the operator's own output does NOT carry source
    // timestamps — derived values have no source-timestamp concept,
    // so a wrapping `timestamp()` falls back to step time.
    assert!(b.source_timestamps.is_none());
}

#[test]
fn should_apply_calendar_extractions_from_epoch_seconds() {
    // given: 2006-01-02 22:04:05 UTC, the canonical timestamp used by the
    // legacy function tests.
    let ts = vec![0];
    let values = vec![1_136_239_445.0];
    let validity = vec![true];
    let schema = mk_operator_schema(&ts, 0, 1);

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut year = InstantFnOp::new(child, InstantFnKind::Year, MemoryReservation::new(1 << 20));
    let b = &drive(&mut year)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(2006.0));

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut month = InstantFnOp::new(child, InstantFnKind::Month, MemoryReservation::new(1 << 20));
    let b = &drive(&mut month)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(1.0));

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut dom = InstantFnOp::new(
        child,
        InstantFnKind::DayOfMonth,
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut dom)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(2.0));

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut dow = InstantFnOp::new(
        child,
        InstantFnKind::DayOfWeek,
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut dow)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(1.0));

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut hour = InstantFnOp::new(child, InstantFnKind::Hour, MemoryReservation::new(1 << 20));
    let b = &drive(&mut hour)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(22.0));

    let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut minute = InstantFnOp::new(
        child,
        InstantFnKind::Minute,
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut minute)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(4.0));

    let batch = mk_batch(ts, 1, values, validity);
    let child = MockChild::new(schema, vec![batch]);
    let mut dim = InstantFnOp::new(
        child,
        InstantFnKind::DaysInMonth,
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut dim)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(31.0));
}

#[test]
fn should_respect_memory_reservation() {
    // given: reservation too small for even a single-cell output
    let ts = vec![1_000];
    let batch = mk_batch(ts.clone(), 1, vec![1.0], vec![true]);
    let schema = mk_operator_schema(&ts, 1_000, 1);
    let child = MockChild::new(schema, vec![batch]);

    // when: 1-byte cap → can't fit any f64
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1));
    let results = drive(&mut op);

    // then
    let err = results
        .into_iter()
        .find_map(|r| r.err())
        .expect("expected MemoryLimit");
    assert!(matches!(err, QueryError::MemoryLimit { .. }));
}

#[test]
fn should_passthrough_static_schema() {
    // given: child with a Static schema
    let ts = vec![1_000];
    let batch = mk_batch(ts.clone(), 2, vec![1.0, 2.0], vec![true, true]);
    let schema = mk_operator_schema(&ts, 1_000, 2);
    let expected_grid = schema.step_grid;
    let child = MockChild::new(schema, vec![batch]);

    // when
    let op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));

    // then: schema passes through unchanged — same step grid, same
    // Static series handle, same underlying roster pointer.
    assert!(!op.schema().series.is_deferred());
    assert_eq!(op.schema().step_grid, expected_grid);
    assert_eq!(op.schema().series.as_static().unwrap().len(), 2);
}

#[test]
fn should_drain_child_and_end_stream() {
    // given: two batches then end-of-stream from the child.
    let ts = vec![1_000];
    let b1 = mk_batch(ts.clone(), 1, vec![1.0], vec![true]);
    let b2 = mk_batch(ts.clone(), 1, vec![2.0], vec![true]);
    let schema = mk_operator_schema(&ts, 1_000, 1);
    let child = MockChild::new(schema, vec![b1, b2]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
    let results = drive(&mut op);

    // then: exactly two batches, then the operator yields nothing
    // further. Re-polling after None stays None (idempotent).
    assert_eq!(results.len(), 2);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    matches!(op.next(&mut cx), Poll::Ready(None));
    matches!(op.next(&mut cx), Poll::Ready(None));
}

#[test]
fn should_apply_trig_function() {
    // given: values at 0, π/2, π — sin(0)=0, sin(π/2)=1, sin(π)≈0
    let ts = vec![1_000];
    let values = vec![0.0, std::f64::consts::FRAC_PI_2, std::f64::consts::PI];
    let validity = vec![true; 3];
    let schema = mk_operator_schema(&ts, 1_000, 3);

    let batch = mk_batch(ts.clone(), 3, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut op = InstantFnOp::new(child, InstantFnKind::Sin, MemoryReservation::new(1 << 20));
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 0.0, 1e-12));
    assert!(approx_eq(b.get(0, 1).unwrap(), 1.0, 1e-12));
    assert!(approx_eq(b.get(0, 2).unwrap(), 0.0, 1e-12));

    // and cos(0)=1, cos(π/2)≈0, cos(π)=-1
    let batch = mk_batch(ts, 3, values, validity);
    let child = MockChild::new(schema, vec![batch]);
    let mut op = InstantFnOp::new(child, InstantFnKind::Cos, MemoryReservation::new(1 << 20));
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert!(approx_eq(b.get(0, 0).unwrap(), 1.0, 1e-12));
    assert!(approx_eq(b.get(0, 1).unwrap(), 0.0, 1e-12));
    assert!(approx_eq(b.get(0, 2).unwrap(), -1.0, 1e-12));
}

#[test]
fn should_apply_round_with_to_nearest() {
    // given: values that round differently for to_nearest=1.0 vs 0.5
    // Prometheus tie-breaking: (v*inv + 0.5).floor() / inv — half-up
    // toward +∞. round(1.5, 1.0) = 2.0; round(2.5, 1.0) = 3.0;
    // round(1.25, 0.5) = 1.5 (since 1.25/0.5 + 0.5 = 3.0 → floor 3 →
    // 1.5).
    let ts = vec![1_000];
    let values = vec![1.5, 2.5, 1.25, -1.5];
    let validity = vec![true; 4];
    let schema = mk_operator_schema(&ts, 1_000, 4);

    let batch = mk_batch(ts.clone(), 4, values.clone(), validity.clone());
    let child = MockChild::new(schema.clone(), vec![batch]);
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Round { to_nearest: 1.0 },
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    assert_eq!(b.get(0, 0), Some(2.0));
    assert_eq!(b.get(0, 1), Some(3.0));
    assert_eq!(b.get(0, 2), Some(1.0));
    // -1.5 * 1 + 0.5 = -1.0 → floor(-1.0) = -1.0 → result -1.0
    assert_eq!(b.get(0, 3), Some(-1.0));

    let batch = mk_batch(ts, 4, values, validity);
    let child = MockChild::new(schema, vec![batch]);
    let mut op = InstantFnOp::new(
        child,
        InstantFnKind::Round { to_nearest: 0.5 },
        MemoryReservation::new(1 << 20),
    );
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];
    // round(1.5, 0.5) = 1.5; round(2.5, 0.5) = 2.5; round(1.25, 0.5) = 1.5
    assert!(approx_eq(b.get(0, 0).unwrap(), 1.5, 1e-12));
    assert!(approx_eq(b.get(0, 1).unwrap(), 2.5, 1e-12));
    assert!(approx_eq(b.get(0, 2).unwrap(), 1.5, 1e-12));
}

#[test]
fn should_apply_sgn() {
    // given: -3, 0, 4, NaN
    let ts = vec![1_000];
    let values = vec![-3.0, 0.0, 4.0, f64::NAN];
    let validity = vec![true; 4];
    let batch = mk_batch(ts.clone(), 4, values, validity);
    let schema = mk_operator_schema(&ts, 1_000, 4);
    let child = MockChild::new(schema, vec![batch]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Sgn, MemoryReservation::new(1 << 20));
    let b = &drive(&mut op)
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>()[0];

    // then
    assert_eq!(b.get(0, 0), Some(-1.0));
    assert_eq!(b.get(0, 1), Some(0.0));
    assert_eq!(b.get(0, 2), Some(1.0));
    assert!(b.get(0, 3).unwrap().is_nan());
}

#[test]
fn should_propagate_error_from_upstream() {
    // given: child yields an error. Op passes it through and stops.
    let ts = vec![1_000];
    let schema = mk_operator_schema(&ts, 1_000, 1);
    let child = MockChild::with_queue(
        schema,
        vec![Err(QueryError::MemoryLimit {
            requested: 10,
            cap: 0,
            already_reserved: 0,
        })],
    );

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
    let results = drive(&mut op);

    // then
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
}

// ========================================================================
// tile-boundary stress (RFC 0007 6.3.9)
// ========================================================================

/// Build one tile batch with an explicit `series_range` (not `0..N`).
fn mk_tile_batch(
    step_timestamps: Vec<i64>,
    total_series_count: usize,
    series_range: std::ops::Range<usize>,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    let step_count = step_timestamps.len();
    let tile_series_count = series_range.len();
    assert_eq!(values.len(), step_count * tile_series_count);
    assert_eq!(validity.len(), values.len());
    let schema = mk_schema(total_series_count);
    let mut bits = BitSet::with_len(validity.len());
    for (i, &b) in validity.iter().enumerate() {
        if b {
            bits.set(i);
        }
    }
    StepBatch::new(
        Arc::from(step_timestamps),
        0..step_count,
        SchemaRef::Static(schema),
        series_range,
        values,
        bits,
    )
}

#[test]
fn should_apply_instant_fn_across_multi_tile_inputs_over_512_series() {
    // given: 1024 series split into two series-tile batches covering the
    // same step range (mirroring `VectorSelectorOp`'s `series_chunk=512`
    // emission for rosters >512 series).
    const SERIES: usize = 1024;
    const TILE: usize = 512;
    let ts = vec![10_i64, 20_i64];
    let schema = mk_operator_schema(&ts, 10, SERIES);

    let vals_a: Vec<f64> = (0..(TILE * 2)).map(|i| -(i as f64)).collect();
    let vals_b: Vec<f64> = (0..(TILE * 2)).map(|i| -((TILE + i / 2) as f64)).collect();
    let batch_a = mk_tile_batch(ts.clone(), SERIES, 0..TILE, vals_a, vec![true; TILE * 2]);
    let batch_b = mk_tile_batch(
        ts.clone(),
        SERIES,
        TILE..SERIES,
        vals_b,
        vec![true; TILE * 2],
    );
    let child = MockChild::new(schema, vec![batch_a, batch_b]);

    // when
    let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: each output batch preserves its child's series_range and
    // every cell is the abs of the input — invariant under tiling.
    assert_eq!(outs.len(), 2, "InstantFn emits one output per input batch");
    assert_eq!(outs[0].series_range, 0..TILE);
    assert_eq!(outs[1].series_range, TILE..SERIES);
    // Spot-check a handful of cells across both tiles.
    assert_eq!(outs[0].get(0, 0), Some(0.0));
    assert_eq!(outs[0].get(0, 1), Some(1.0));
    assert_eq!(outs[0].get(1, TILE - 1), Some((TILE * 2 - 1) as f64));
    // tile B: original vals_b[i] = -(TILE + i/2), so abs = TILE + i/2.
    assert_eq!(outs[1].get(0, 0), Some(TILE as f64));
    assert_eq!(
        outs[1].get(1, TILE - 1),
        Some((TILE + (TILE * 2 - 1) / 2) as f64)
    );
}

// ========================================================================
// end-to-end pipeline: VectorSelectorOp -> InstantFnOp(Abs)
// ========================================================================

#[test]
fn should_apply_abs_over_vector_selector_pipeline() {
    use crate::promql::operators::vector_selector::{BatchShape, VectorSelectorOp};
    use crate::promql::source::{
        ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest,
        SeriesSource, TimeRange,
    };
    use futures::Stream;
    use futures::stream::{self};
    use promql_parser::parser::VectorSelector;
    use std::future::ready;

    /// Sync-on-poll mock — single-batch, honours `(start, end]` via
    /// TimeRange's `[start, end)` convention. Samples are in
    /// ascending timestamp order.
    struct MockSource {
        data: Vec<(Vec<i64>, Vec<f64>)>,
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
                    if *t >= request.time_range.start_ms && *t < request.time_range.end_ms_exclusive
                    {
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

    // given: two series with negative values; selector walks them at
    // ts=10,20 with lookback=5.
    let source = Arc::new(MockSource {
        data: vec![
            (vec![10, 20], vec![-1.0, -2.0]),
            (vec![10, 20], vec![3.0, -4.0]),
        ],
    });
    let schema = mk_schema(2);
    let name: Arc<str> = Arc::from("m");
    let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![
        Arc::from(vec![ResolvedSeriesRef::new(1, 0, name.clone())]),
        Arc::from(vec![ResolvedSeriesRef::new(1, 1, name.clone())]),
    ]);
    let grid = StepGrid {
        start_ms: 10,
        end_ms: 20,
        step_ms: 10,
        step_count: 2,
    };
    let reservation = MemoryReservation::new(1 << 20);
    let selector = VectorSelectorOp::new(
        source,
        schema,
        request_series,
        grid,
        None,
        None,
        5,
        reservation.clone(),
        BatchShape::new(2, 2),
    );

    // when: wire through InstantFn(Abs)
    let mut op = InstantFnOp::new(selector, InstantFnKind::Abs, reservation);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut batches = Vec::new();
    loop {
        match op.next(&mut cx) {
            Poll::Ready(None) => break,
            Poll::Ready(Some(Ok(b))) => batches.push(b),
            Poll::Ready(Some(Err(err))) => panic!("unexpected error: {err:?}"),
            Poll::Pending => panic!("unexpected pending"),
        }
    }

    // then: all four cells are the abs() of the input values.
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.get(0, 0), Some(1.0)); // abs(-1)
    assert_eq!(b.get(0, 1), Some(3.0)); // abs( 3)
    assert_eq!(b.get(1, 0), Some(2.0)); // abs(-2)
    assert_eq!(b.get(1, 1), Some(4.0)); // abs(-4)
}
