//! `AbsentOp` implements PromQL's `absent(v)` and, over a
//! `count_over_time` child, `absent_over_time(m)`: a one-series vector
//! valued `1` at every step where the child has no samples, and empty
//! elsewhere.
//!
//! A step is only known to be empty once every series chunk covering it
//! has arrived, so the operator drains its child before emitting one
//! full-grid batch. State is a single presence bit per step.
//!
//! The output labels are a plan-time constant derived from the argument's
//! selector matchers (see `absent_labels` in the lowering).

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::Labels;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};
use super::histogram::{cell_bytes, grid_timestamps};

pub struct AbsentOp<C: Operator> {
    child: C,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    present: BitSet,
    step_timestamps: Option<Arc<[i64]>>,
    done: bool,
}

impl<C: Operator> AbsentOp<C> {
    pub fn new(child: C, labels: Labels, reservation: MemoryReservation) -> Self {
        let step_grid = child.schema().step_grid;
        let output = Arc::new(SeriesSchema::new(
            Arc::from(vec![labels]),
            Arc::from(vec![0]),
        ));
        Self {
            child,
            reservation,
            schema: OperatorSchema::new(SchemaRef::Static(output), step_grid),
            present: BitSet::with_len(step_grid.step_count),
            step_timestamps: None,
            done: false,
        }
    }

    fn absorb(&mut self, batch: &StepBatch) {
        if self.step_timestamps.is_none() {
            self.step_timestamps = Some(batch.step_timestamps.clone());
        }
        let series_count = batch.series_count();
        for step_off in 0..batch.step_count() {
            let row = step_off * series_count;
            if (row..row + series_count).any(|cell| batch.is_present(cell)) {
                self.present.set(batch.step_range.start + step_off);
            }
        }
    }

    fn finalise(&mut self) -> Result<StepBatch, QueryError> {
        let step_count = self.schema.step_grid.step_count;
        let bytes = cell_bytes(step_count);
        self.reservation.try_grow(bytes)?;
        let mut validity = BitSet::with_len(step_count);
        for step in 0..step_count {
            if !self.present.get(step) {
                validity.set(step);
            }
        }
        self.reservation.release(bytes);
        let step_timestamps = self
            .step_timestamps
            .take()
            .unwrap_or_else(|| grid_timestamps(&self.schema));
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            self.schema.series.clone(),
            0..1,
            vec![1.0; step_count],
            validity,
        ))
    }
}

impl<C: Operator> Operator for AbsentOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match self.child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(batch))) => self.absorb(&batch),
                Poll::Ready(Some(Err(err))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Some(self.finalise()));
                }
            }
        }
    }
}
