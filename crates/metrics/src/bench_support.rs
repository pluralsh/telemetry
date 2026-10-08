//! Thin wrappers over crate internals for the criterion benches. Not a
//! stable API; compiled only with the `bench-internals` feature.

use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;

use crate::model::{Label, Labels, Sample, SeriesData};
use crate::promql::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use crate::promql::memory::{MemoryReservation, QueryError};
use crate::promql::operator::{Operator, OperatorSchema, StepGrid};
use crate::promql::operators::binary::{BinaryOp, BinaryOpKind, ConstScalarOp, MatchTable};
use crate::promql::operators::instant_fn::{InstantFnKind, InstantFnOp};
use crate::serde::timeseries::merge_batch_time_series;

/// The columnar chunk codec float samples are stored in.
pub use crate::serde::chunk;

/// A float-only series value as writers store it.
pub fn encode_series(points: &[Sample]) -> Bytes {
    SeriesData::new(points.to_vec(), Vec::new())
        .encode()
        .expect("encode")
}

/// The query read path: a ranged decode landing in timestamp / value
/// columns.
pub fn decode_series_columns(
    bytes: &[u8],
    start_ms: i64,
    end_ms: i64,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) {
    let data = SeriesData::decode_range(bytes, start_ms, end_ms).expect("decode");
    timestamps.extend(data.timestamps);
    values.extend(data.values);
}

/// The merge operator over one key's entries, oldest first.
pub fn merge_series(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    merge_batch_time_series(existing, operands).expect("merge")
}

/// [`crate::TimeSeriesDb::query_range`] with a trace collector attached:
/// the result series count and the trace as JSON (`?trace=true` shape).
pub async fn query_range_traced(
    db: &crate::TimeSeriesDb,
    namespace: &crate::Namespace,
    query: &str,
    range: std::ops::RangeInclusive<std::time::SystemTime>,
    step: std::time::Duration,
) -> (usize, serde_json::Value) {
    use crate::tsdb::TsdbReadEngine;
    let tsdb = db.read_engine(namespace).await;
    let outcome = tsdb
        .eval_query_range_traced(
            query,
            range,
            step,
            &crate::model::QueryOptions::default(),
            Some(crate::promql::trace::TraceCollector::new()),
        )
        .await
        .expect("query");
    let series = outcome.value.into_matrix().len();
    let trace = serde_json::to_value(outcome.trace.expect("trace")).expect("trace json");
    (series, trace)
}

/// The read path a traced query runs on.
#[derive(Clone, Copy)]
pub enum Engine<'a> {
    Writer(&'a crate::TimeSeriesDb),
    Reader(&'a crate::TimeSeriesDbReader),
}

/// An instant query at a time, or a range query over an interval.
pub enum Evaluation {
    Instant(std::time::SystemTime),
    Range(
        std::ops::RangeInclusive<std::time::SystemTime>,
        std::time::Duration,
    ),
}

/// A query on `engine` with a trace collector attached: the result series
/// count and the trace as JSON (`?trace=true` shape).
pub async fn query_traced(
    engine: Engine<'_>,
    namespace: &crate::Namespace,
    query: &str,
    evaluation: Evaluation,
) -> std::result::Result<(usize, serde_json::Value), crate::QueryError> {
    match engine {
        Engine::Writer(db) => {
            let tsdb = db.read_engine(namespace).await;
            evaluate_traced(tsdb.as_ref(), query, evaluation).await
        }
        Engine::Reader(reader) => {
            let scoped = crate::reader::ScopedReader { namespace, reader };
            evaluate_traced(&scoped, query, evaluation).await
        }
    }
}

async fn evaluate_traced<E>(
    engine: &E,
    query: &str,
    evaluation: Evaluation,
) -> std::result::Result<(usize, serde_json::Value), crate::QueryError>
where
    E: crate::tsdb::TsdbReadEngine + Sync,
    E::QR: 'static,
{
    let options = crate::model::QueryOptions::default();
    let collector = Some(crate::promql::trace::TraceCollector::new());
    let outcome = match evaluation {
        Evaluation::Instant(time) => {
            engine
                .eval_query_traced(query, Some(time), &options, collector)
                .await?
        }
        Evaluation::Range(range, step) => {
            engine
                .eval_query_range_traced(query, range, step, &options, collector)
                .await?
        }
    };
    let series = match outcome.value {
        crate::model::QueryValue::Vector(samples) => samples.len(),
        crate::model::QueryValue::Matrix(series) => series.len(),
        crate::model::QueryValue::Scalar { .. } => 1,
    };
    let trace = serde_json::to_value(outcome.trace.expect("trace")).expect("trace json");
    Ok((series, trace))
}

/// Metrics' default SlateDB `(l0_sst_size_bytes, min_compaction_sources)`,
/// applied wherever the settings file leaves them unset.
pub fn default_slatedb_tuning() -> (usize, usize) {
    (
        crate::storage::slate::DEFAULT_L0_SST_SIZE_BYTES,
        crate::storage::slate::DEFAULT_MIN_COMPACTION_SOURCES,
    )
}

/// One `StepBatch` tile.
#[derive(Clone)]
pub struct Tile(StepBatch);

impl Tile {
    /// `values` row-major by step; `validity[i]` marks `values[i]` present.
    pub fn new(
        step_count: usize,
        series_count: usize,
        values: Vec<f64>,
        validity: &[bool],
    ) -> Self {
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), values.len());
        let mut bits = BitSet::with_len(values.len());
        for (i, _) in validity.iter().enumerate().filter(|(_, v)| **v) {
            bits.set(i);
        }
        let steps: Arc<[i64]> = (0..step_count as i64)
            .map(|i| 1_000_000 + i * 15_000)
            .collect();
        let source_ts: Arc<[i64]> = (0..values.len() as i64)
            .map(|i| 1_000_000 + (i / series_count.max(1) as i64) * 15_000 - (i % 7) * 100)
            .collect();
        Self(
            StepBatch::new(
                steps,
                0..step_count,
                SchemaRef::Static(schema(series_count)),
                0..series_count,
                values,
                bits,
            )
            .with_source_timestamps(source_ts),
        )
    }

    pub fn values(&self) -> &[f64] {
        &self.0.values
    }
}

fn schema(series_count: usize) -> Arc<SeriesSchema> {
    let labels: Vec<Labels> = (0..series_count)
        .map(|i| Labels::new(vec![Label::new("i", i.to_string())]))
        .collect();
    let fps: Vec<u128> = (0..series_count as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

fn grid(batch: &StepBatch) -> StepGrid {
    let steps = batch.step_timestamps_slice();
    StepGrid {
        start_ms: steps.first().copied().unwrap_or(0),
        end_ms: steps.last().copied().unwrap_or(0),
        step_ms: 15_000,
        step_count: steps.len(),
    }
}

struct OnceOp {
    schema: OperatorSchema,
    batch: Option<StepBatch>,
}

impl OnceOp {
    fn new(batch: StepBatch) -> Self {
        Self {
            schema: OperatorSchema::new(batch.series.clone(), grid(&batch)),
            batch: Some(batch),
        }
    }
}

impl Operator for OnceOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

fn drain_one(op: &mut impl Operator) -> StepBatch {
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    match op.next(&mut cx) {
        Poll::Ready(Some(Ok(batch))) => batch,
        _ => panic!("expected one batch"),
    }
}

fn reservation() -> MemoryReservation {
    MemoryReservation::new(usize::MAX)
}

#[derive(Debug, Clone, Copy)]
pub enum InstantFn {
    Abs,
    Ceil,
    Sqrt,
    Ln,
    Clamp { min: f64, max: f64 },
    Timestamp,
}

pub fn instant_fn(kind: InstantFn, tile: Tile) -> Tile {
    let kind = match kind {
        InstantFn::Abs => InstantFnKind::Abs,
        InstantFn::Ceil => InstantFnKind::Ceil,
        InstantFn::Sqrt => InstantFnKind::Sqrt,
        InstantFn::Ln => InstantFnKind::Ln,
        InstantFn::Clamp { min, max } => InstantFnKind::Clamp { min, max },
        InstantFn::Timestamp => InstantFnKind::Timestamp,
    };
    let mut op = InstantFnOp::new(OnceOp::new(tile.0), kind, reservation());
    Tile(drain_one(&mut op))
}

#[derive(Debug, Clone, Copy)]
pub enum Arith {
    Add,
    Mul,
    Div,
}

/// Comparisons; `GtBool` carries the `bool` modifier.
#[derive(Debug, Clone, Copy)]
pub enum Compare {
    Gt,
    Ne,
    GtBool,
}

#[derive(Debug, Clone, Copy)]
pub struct BinaryKind(BinaryOpKind);

impl From<Arith> for BinaryKind {
    fn from(op: Arith) -> Self {
        Self(match op {
            Arith::Add => BinaryOpKind::Add,
            Arith::Mul => BinaryOpKind::Mul,
            Arith::Div => BinaryOpKind::Div,
        })
    }
}

impl From<Compare> for BinaryKind {
    fn from(op: Compare) -> Self {
        Self(match op {
            Compare::Gt => BinaryOpKind::Gt {
                bool_modifier: false,
            },
            Compare::Ne => BinaryOpKind::Ne {
                bool_modifier: false,
            },
            Compare::GtBool => BinaryOpKind::Gt {
                bool_modifier: true,
            },
        })
    }
}

/// Which `BinaryOp` loop evaluates the cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryPath {
    /// The per-cell reference loop.
    Generic,
    Fast,
}

/// `lhs OP rhs`, LHS series `i` matched with RHS series `map[i]`:
/// one-to-one, or `group_left` (several LHS series sharing a partner).
pub fn binary_vector_vector(
    op: impl Into<BinaryKind>,
    lhs: Tile,
    rhs: Tile,
    map: &[Option<u32>],
    group_left: bool,
    path: BinaryPath,
) -> Tile {
    let output_schema = lhs.0.series.as_static().expect("static").clone();
    let table = if group_left {
        MatchTable::GroupLeft(map.to_vec())
    } else {
        MatchTable::OneToOne(map.to_vec())
    };
    let op = BinaryOp::new_vector_vector(
        OnceOp::new(lhs.0),
        OnceOp::new(rhs.0),
        op.into().0,
        table,
        output_schema,
        reservation(),
    );
    Tile(drain_binary(op, path))
}

/// `lhs OP scalar`.
pub fn binary_vector_scalar(
    op: impl Into<BinaryKind>,
    lhs: Tile,
    scalar: f64,
    path: BinaryPath,
) -> Tile {
    let reservation = reservation();
    let scalar = ConstScalarOp::new(scalar, grid(&lhs.0), reservation.clone());
    let op = BinaryOp::new_vector_scalar(OnceOp::new(lhs.0), scalar, op.into().0, reservation);
    Tile(drain_binary(op, path))
}

fn drain_binary<L: Operator, R: Operator>(op: BinaryOp<L, R>, path: BinaryPath) -> StepBatch {
    let mut op = match path {
        BinaryPath::Generic => op.with_generic_path(),
        BinaryPath::Fast => op,
    };
    drain_one(&mut op)
}

#[derive(Debug, Clone, Copy)]
pub enum Aggregate {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Group,
}

impl From<Aggregate> for crate::promql::operators::aggregate::AggregateKind {
    fn from(kind: Aggregate) -> Self {
        match kind {
            Aggregate::Sum => Self::Sum,
            Aggregate::Avg => Self::Avg,
            Aggregate::Min => Self::Min,
            Aggregate::Max => Self::Max,
            Aggregate::Count => Self::Count,
            Aggregate::Stddev => Self::Stddev,
            Aggregate::Stdvar => Self::Stdvar,
            Aggregate::Group => Self::Group,
        }
    }
}

/// `kind by (…)` over `tile`, input series `i` landing in group
/// `groups[i]`.
pub fn aggregate(kind: Aggregate, tile: Tile, groups: &[Option<u32>], group_count: usize) -> Tile {
    use crate::promql::operators::aggregate::{AggregateOp, GroupMap};
    let mut op = AggregateOp::new(
        OnceOp::new(tile.0),
        kind.into(),
        GroupMap::new(groups.to_vec(), group_count),
        schema(group_count),
        reservation(),
    )
    .expect("aggregate");
    Tile(drain_one(&mut op))
}

/// [`aggregate`] through the cell-at-a-time all-lanes accumulator it
/// replaced; returns the step-major output values.
pub fn aggregate_reference(
    kind: Aggregate,
    tile: Tile,
    groups: &[Option<u32>],
    group_count: usize,
) -> Vec<f64> {
    use crate::promql::operators::aggregate::{GroupMap, reference};
    let map = GroupMap::new(groups.to_vec(), group_count);
    let step_count = tile.0.step_count();
    reference::aggregate(kind.into(), step_count, &map, std::slice::from_ref(&tile.0)).0
}
