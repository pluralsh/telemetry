//! A leaf that replays a subtree a shard already evaluated.
//!
//! Pushdown evaluates the aggregations that can be computed from partial
//! results on the shards, and the coordinator runs the rest of the query over
//! what they returned. Each partial series carries samples at the query's
//! step timestamps; a step with no sample is absent, never zero.

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::histogram::FloatHistogram;
use crate::model::{Labels, RangeSample};

use super::super::batch::{BitSet, HistogramCells, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::QueryError;
use super::super::operator::{Operator, OperatorSchema, StepGrid};

/// Partial series from every shard for one pushed-down subtree. Series from
/// different shards may share labels; the aggregation above combines them.
#[derive(Debug)]
pub struct PrecomputedSeries {
    pub series: Vec<RangeSample>,
}

/// A shared handle that compares by identity, so plans carrying one stay
/// comparable without comparing their data.
#[derive(Debug, Clone)]
pub struct PrecomputedRef(pub Arc<PrecomputedSeries>);

impl PartialEq for PrecomputedRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub struct PrecomputedOp {
    schema: OperatorSchema,
    batch: Option<StepBatch>,
}

impl PrecomputedOp {
    pub fn new(data: &PrecomputedSeries, grid: StepGrid) -> Result<Self, QueryError> {
        let step_count = grid.step_count;
        let step_timestamps: Arc<[i64]> = (0..step_count)
            .map(|i| grid.start_ms + i as i64 * grid.step_ms)
            .collect();
        let series_count = data.series.len();
        let labels: Arc<[Labels]> = data.series.iter().map(|s| s.labels.clone()).collect();
        // Rows are identified by position, as other derived schemas are.
        let fingerprints: Arc<[u128]> = (0..series_count as u128).collect();
        let schema_series = Arc::new(SeriesSchema::new(labels, fingerprints));
        let schema = OperatorSchema::new(SchemaRef::Static(schema_series.clone()), grid);
        let cells = step_count * series_count;
        let mut values = vec![0.0; cells];
        let mut validity = BitSet::with_len(cells);
        let mut histograms: HistogramCells = vec![None; cells];
        let index_of = |timestamp: i64| -> Result<usize, QueryError> {
            let offset = timestamp - grid.start_ms;
            if offset < 0 || offset % grid.step_ms.max(1) != 0 {
                return Err(QueryError::Internal(format!(
                    "pushed-down sample at {timestamp} is off the step grid"
                )));
            }
            let step = (offset / grid.step_ms.max(1)) as usize;
            if step >= step_count {
                return Err(QueryError::Internal(format!(
                    "pushed-down sample at {timestamp} is past the step grid"
                )));
            }
            Ok(step)
        };
        for (series, sample) in data.series.iter().enumerate() {
            for &(timestamp, value) in &sample.samples {
                let cell = index_of(timestamp)? * series_count + series;
                values[cell] = value;
                validity.set(cell);
            }
            for (timestamp, histogram) in &sample.histograms {
                let cell = index_of(*timestamp)? * series_count + series;
                histograms[cell] = Some(Arc::<FloatHistogram>::clone(histogram));
            }
        }
        let batch = (step_count > 0 && series_count > 0).then(|| {
            StepBatch::new(
                step_timestamps,
                0..step_count,
                SchemaRef::Static(schema_series),
                0..series_count,
                values,
                validity,
            )
            .with_histograms(histograms)
        });
        Ok(Self { schema, batch })
    }
}

impl Operator for PrecomputedOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}
