use super::*;
use crate::model::{Label, Labels};
use crate::promql::batch::SeriesSchema;
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

fn mk_labels(pairs: &[(&str, &str)]) -> Labels {
    Labels::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
}

fn mk_input_schema(count: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..count)
        .map(|i| mk_labels(&[("__name__", "version"), ("inst", &i.to_string())]))
        .collect();
    let fps: Vec<u128> = (0..count as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

fn mk_grid(step_count: usize) -> StepGrid {
    StepGrid {
        start_ms: 0,
        end_ms: 10 * ((step_count as i64) - 1).max(0),
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
        0..series_count,
        values,
        bits,
    )
}

fn drive<C: Operator>(op: &mut CountValuesOp<C>) -> Vec<Result<StepBatch, QueryError>> {
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

/// Collect the `__name__`/custom-label pairs from every output series
/// as `(metric, value_label)` tuples to make assertions compact.
fn value_labels(schema: &Arc<SeriesSchema>, label_name: &str) -> Vec<String> {
    schema
        .labels_slice()
        .iter()
        .map(|l| l.get(label_name).unwrap_or("").to_string())
        .collect()
}

// ========================================================================
// tests
// ========================================================================

#[test]
fn should_produce_one_series_per_distinct_value() {
    // given: 4 input series, 1 step. Values [6, 6, 7, 8] ⇒ three
    // distinct buckets.
    let in_schema = mk_input_schema(4);
    let grid = mk_grid(1);
    let batch = mk_batch(
        in_schema.clone(),
        1,
        4,
        vec![6.0, 6.0, 7.0, 8.0],
        vec![true; 4],
    );
    let child = MockOp::new(in_schema, grid, vec![batch]);

    // when
    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one batch, three output series
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    let finalised = op.finalized_schema().expect("finalised after drain");
    let labels = value_labels(finalised, "version");
    let mut sorted = labels.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["6", "7", "8"]);
}

#[test]
fn should_count_series_per_value_per_step() {
    // given: 3 input series, 2 steps.
    //   step 0: [6, 6, 7] ⇒ {6:2, 7:1}
    //   step 1: [7, 7, 8] ⇒ {7:2, 8:1}
    let in_schema = mk_input_schema(3);
    let grid = mk_grid(2);
    let values = vec![6.0, 6.0, 7.0, 7.0, 7.0, 8.0];
    let valid = vec![true; 6];
    let batch = mk_batch(in_schema.clone(), 2, 3, values, valid);
    let child = MockOp::new(in_schema, grid, vec![batch]);

    // when
    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then
    let b = &outs[0];
    assert_eq!(b.step_count(), 2);
    assert_eq!(b.series_count(), 3);
    let finalised = op.finalized_schema().unwrap();
    let labels = value_labels(finalised, "version");
    // find each series's column by its label
    let col_for = |needle: &str| -> usize {
        labels
            .iter()
            .position(|s| s == needle)
            .unwrap_or_else(|| panic!("no column for {needle}, labels = {labels:?}"))
    };
    let c6 = col_for("6");
    let c7 = col_for("7");
    let c8 = col_for("8");

    assert_eq!(b.get(0, c6), Some(2.0));
    assert_eq!(b.get(0, c7), Some(1.0));
    assert_eq!(b.get(0, c8), None); // no inputs hit 8 at step 0
    assert_eq!(b.get(1, c6), None);
    assert_eq!(b.get(1, c7), Some(2.0));
    assert_eq!(b.get(1, c8), Some(1.0));
}

#[test]
fn should_respect_by_grouping() {
    // given: 4 inputs split into two groups.
    //   group 0: series 0,1 values [6, 7]
    //   group 1: series 2,3 values [6, 6]
    // Expected output series:
    //   {group="a", version="6"} step0 = 1
    //   {group="a", version="7"} step0 = 1
    //   {group="b", version="6"} step0 = 2
    let in_schema = mk_input_schema(4);
    let grid = mk_grid(1);
    let batch = mk_batch(
        in_schema.clone(),
        1,
        4,
        vec![6.0, 7.0, 6.0, 6.0],
        vec![true; 4],
    );
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);
    let group_labels: Arc<[Labels]> = Arc::from(
        vec![mk_labels(&[("group", "a")]), mk_labels(&[("group", "b")])].into_boxed_slice(),
    );

    let mut op = CountValuesOp::new(
        child,
        "version",
        Some(gmap),
        group_labels,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    let finalised = op.finalized_schema().unwrap();

    // Locate each expected series by its full label set.
    let find = |group: &str, version: &str| -> usize {
        finalised
            .labels_slice()
            .iter()
            .position(|l| l.get("group") == Some(group) && l.get("version") == Some(version))
            .unwrap_or_else(|| {
                panic!(
                    "no series with group={group} version={version}: {:?}",
                    finalised
                )
            })
    };
    let a6 = find("a", "6");
    let a7 = find("a", "7");
    let b6 = find("b", "6");
    assert_eq!(b.get(0, a6), Some(1.0));
    assert_eq!(b.get(0, a7), Some(1.0));
    assert_eq!(b.get(0, b6), Some(2.0));
}

#[test]
fn should_respect_without_grouping() {
    // given: 3 inputs, each in its own group (mimics `without`).
    //   group 0 (inst=0): value 6
    //   group 1 (inst=1): value 6
    //   group 2 (inst=2): value 7
    // Output: three serieses all with different `{inst=..., version=...}`.
    let in_schema = mk_input_schema(3);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 3, vec![6.0, 6.0, 7.0], vec![true; 3]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2)], 3);
    let group_labels: Arc<[Labels]> = Arc::from(
        vec![
            mk_labels(&[("inst", "0")]),
            mk_labels(&[("inst", "1")]),
            mk_labels(&[("inst", "2")]),
        ]
        .into_boxed_slice(),
    );

    let mut op = CountValuesOp::new(
        child,
        "version",
        Some(gmap),
        group_labels,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    let finalised = op.finalized_schema().unwrap();
    for l in finalised.labels_slice() {
        assert!(l.get("inst").is_some());
        assert!(l.get("version").is_some());
    }
    // every output cell should equal 1 (each input is alone in its group)
    for s in 0..3 {
        assert_eq!(b.get(0, s), Some(1.0));
    }
}

#[test]
fn should_expose_deferred_schema_before_drain() {
    // given: fresh operator
    let in_schema = mk_input_schema(1);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 1, vec![6.0], vec![true]);
    let child = MockOp::new(in_schema, grid, vec![batch]);

    let op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );

    // when / then: schema is Deferred even before polling
    assert!(op.schema().series.is_deferred());
    assert!(op.finalized_schema().is_none());
}

#[test]
fn should_expose_static_schema_after_drain() {
    // given
    let in_schema = mk_input_schema(2);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 2, vec![6.0, 7.0], vec![true, true]);
    let child = MockOp::new(in_schema, grid, vec![batch]);

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );

    // when: drive to completion
    let _ = drive(&mut op);

    // then: schema() still reports Deferred, but finalized_schema() is Some
    assert!(op.schema().series.is_deferred());
    let finalised = op.finalized_schema().expect("finalised after drain");
    assert_eq!(finalised.len(), 2);
}

#[test]
fn should_distinguish_nan_and_value_by_bit_pattern() {
    // given: two inputs with NaN values that have *different* bit
    // patterns — they should end up in two separate buckets in the
    // intermediate map even though both render to "NaN".
    // To keep the test deterministic and user-facing, we check that a
    // NaN-valued input produces a `{version="NaN"}` output series and
    // the count is correct for repeated NaN inputs.
    let in_schema = mk_input_schema(3);
    let grid = mk_grid(1);
    let nan_a = f64::NAN;
    // Construct a different NaN bit pattern.
    let nan_b = f64::from_bits(f64::NAN.to_bits() ^ 0x1);
    assert!(nan_b.is_nan());
    assert_ne!(nan_a.to_bits(), nan_b.to_bits());
    let batch = mk_batch(
        in_schema.clone(),
        1,
        3,
        vec![nan_a, nan_b, 6.0],
        vec![true; 3],
    );
    let child = MockOp::new(in_schema, grid, vec![batch]);

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    let b = &outs[0];
    let finalised = op.finalized_schema().unwrap();
    // Three distinct buckets by value_bits: two NaN variants + 6.
    assert_eq!(b.series_count(), 3);
    // All NaN buckets render with label `"NaN"` even though the bit
    // patterns differ.
    let labels = value_labels(finalised, "version");
    let nan_count = labels.iter().filter(|s| *s == "NaN").count();
    assert_eq!(nan_count, 2);
    // The sixth bucket is present.
    assert!(labels.iter().any(|s| s == "6"));
    // Every output cell at step 0 equals 1 (each bucket saw exactly
    // one input).
    for s in 0..3 {
        assert_eq!(b.get(0, s), Some(1.0));
    }
}

#[test]
fn should_propagate_error_from_upstream() {
    // given: child queue has an error before EOS
    let in_schema = mk_input_schema(1);
    let grid = mk_grid(1);
    let child = MockOp::with_queue(
        in_schema,
        grid,
        vec![Err(QueryError::Internal("boom".into()))],
    );

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );

    // when
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let first = op.next(&mut cx);

    // then
    match first {
        Poll::Ready(Some(Err(QueryError::Internal(msg)))) => assert!(msg.contains("boom")),
        other => panic!("expected upstream error, got {other:?}"),
    }
    // subsequent polls return None (errored)
    assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_respect_memory_reservation() {
    // given: a tiny cap that cannot fit even one bucket
    let in_schema = mk_input_schema(1);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 1, vec![6.0], vec![true]);
    let child = MockOp::new(in_schema, grid, vec![batch]);

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(8), // 8-byte cap, far below any reasonable bucket
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let r = op.next(&mut cx);
    match r {
        Poll::Ready(Some(Err(QueryError::MemoryLimit { .. }))) => {}
        other => panic!("expected MemoryLimit error, got {other:?}"),
    }
}

#[test]
fn should_handle_child_end_of_stream() {
    // given: child produces no batches at all
    let in_schema = mk_input_schema(0);
    let grid = mk_grid(2);
    let child = MockOp::new(in_schema, grid, vec![]);

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one batch with zero series, finalised schema is empty.
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].series_count(), 0);
    let finalised = op.finalized_schema().unwrap();
    assert_eq!(finalised.len(), 0);
}

#[test]
fn should_format_value_label_matching_existing_engine() {
    // Reference behaviour cited from Prometheus' strconv.FormatFloat(-1, 64):
    //   6.0 -> "6", 6.5 -> "6.5", -0.0 -> "-0",
    //   NaN -> "NaN", +Inf -> "+Inf", -Inf -> "-Inf"
    // Additional finite cases agree with Rust's f64::to_string.
    assert_eq!(format_value_label(6.0), "6");
    assert_eq!(format_value_label(6.5), "6.5");
    assert_eq!(format_value_label(0.0), "0");
    assert_eq!(format_value_label(-0.0), "-0");
    assert_eq!(format_value_label(f64::NAN), "NaN");
    assert_eq!(format_value_label(f64::INFINITY), "+Inf");
    assert_eq!(format_value_label(f64::NEG_INFINITY), "-Inf");
    assert_eq!(format_value_label(-3.25), "-3.25");
    assert_eq!(format_value_label(1e20), "100000000000000000000");
}

#[test]
fn should_return_none_on_subsequent_polls_after_emission() {
    // given: valid input; drive once to collect the emit, then poll
    // a second time to confirm idempotent EOS.
    let in_schema = mk_input_schema(2);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 2, vec![6.0, 7.0], vec![true; 2]);
    let child = MockOp::new(in_schema, grid, vec![batch]);

    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // first: batch
    let first = op.next(&mut cx);
    assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
    // second: none
    assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
    // third: still none
    assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
}

#[test]
fn should_drop_inputs_with_none_group_assignment() {
    // given: 3 inputs, second one unassigned (group = None).
    let in_schema = mk_input_schema(3);
    let grid = mk_grid(1);
    let batch = mk_batch(in_schema.clone(), 1, 3, vec![6.0, 7.0, 6.0], vec![true; 3]);
    let child = MockOp::new(in_schema, grid, vec![batch]);
    let gmap = GroupMap::new(vec![Some(0), None, Some(0)], 1);
    let group_labels: Arc<[Labels]> =
        Arc::from(vec![mk_labels(&[("group", "a")])].into_boxed_slice());

    let mut op = CountValuesOp::new(
        child,
        "version",
        Some(gmap),
        group_labels,
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: only value 6 emerges (the 7 was dropped).
    let b = &outs[0];
    assert_eq!(b.series_count(), 1);
    let finalised = op.finalized_schema().unwrap();
    assert_eq!(value_labels(finalised, "version"), vec!["6"]);
    assert_eq!(b.get(0, 0), Some(2.0));
}

fn mk_tile_batch(
    schema: Arc<SeriesSchema>,
    step_count: usize,
    series_range: std::ops::Range<usize>,
    values: Vec<f64>,
    validity: Vec<bool>,
) -> StepBatch {
    let sc = series_range.len();
    assert_eq!(values.len(), step_count * sc);
    assert_eq!(validity.len(), values.len());
    let ts: Arc<[i64]> = Arc::from((0..step_count).map(|i| (i as i64) * 10).collect::<Vec<_>>());
    let mut bits = BitSet::with_len(values.len());
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

#[test]
fn should_count_values_across_multi_series_tile_batches_over_512_series() {
    // given: 1024 input series emitted as two series-tile batches
    // mirroring `VectorSelectorOp`'s default `series_chunk=512`
    // emission. Values follow a round-robin: input `i` takes value
    // `i % 3` (three distinct output buckets, each with ~341 or ~342
    // contributors). CountValuesOp must bucket across tiles so the
    // per-step counts sum over both tiles.
    const INPUTS: usize = 1024;
    const TILE: usize = 512;
    const STEPS: usize = 1;

    let in_schema = mk_input_schema(INPUTS);
    let grid = mk_grid(STEPS);

    let build_tile = |series_range: std::ops::Range<usize>| -> StepBatch {
        let sc = series_range.len();
        let mut values = Vec::with_capacity(STEPS * sc);
        for _step in 0..STEPS {
            for s in series_range.clone() {
                values.push((s % 3) as f64);
            }
        }
        mk_tile_batch(
            in_schema.clone(),
            STEPS,
            series_range,
            values,
            vec![true; STEPS * sc],
        )
    };
    let batch_a = build_tile(0..TILE);
    let batch_b = build_tile(TILE..INPUTS);
    let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);

    // when
    let mut op = CountValuesOp::new(
        child,
        "version",
        None,
        Arc::from(vec![Labels::empty()].into_boxed_slice()),
        MemoryReservation::new(1 << 20),
    );
    let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

    // then: one output batch with three series (values 0, 1, 2).
    // Counts: i in 0..1024 with i % 3 == 0 → 342; == 1 → 341; == 2
    // → 341. Verify via finalised schema + per-bucket count.
    assert_eq!(outs.len(), 1);
    let b = &outs[0];
    assert_eq!(b.series_count(), 3);
    let finalised = op.finalized_schema().unwrap();
    let labels = value_labels(finalised, "version");
    let col_for = |v: &str| -> usize {
        labels
            .iter()
            .position(|s| s == v)
            .unwrap_or_else(|| panic!("no column for {v}, labels = {labels:?}"))
    };
    let count_for =
        |modulus: usize| -> f64 { (0..INPUTS).filter(|i| i % 3 == modulus).count() as f64 };
    assert_eq!(b.get(0, col_for("0")), Some(count_for(0)));
    assert_eq!(b.get(0, col_for("1")), Some(count_for(1)));
    assert_eq!(b.get(0, col_for("2")), Some(count_for(2)));
}
