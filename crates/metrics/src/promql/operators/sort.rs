//! `sort` and `sort_desc`. The planner records the order and applies it to
//! the final result, so per step these only keep float samples, as
//! Prometheus' `filterFloats` does. The output is a function result rather
//! than a selector's samples, so `timestamp()` over it sees step timestamps.

use std::task::{Context, Poll};

use crate::promql::batch::StepBatch;
use crate::promql::memory::QueryError;
use crate::promql::operator::{Operator, OperatorSchema};

pub struct SortOp<C: Operator> {
    child: C,
}

impl<C: Operator> SortOp<C> {
    pub fn new(child: C) -> Self {
        Self { child }
    }
}

impl<C: Operator> Operator for SortOp<C> {
    fn schema(&self) -> &OperatorSchema {
        self.child.schema()
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        self.child.next(cx).map(|batch| {
            batch.map(|batch| {
                batch.map(|mut batch| {
                    // Histogram cells already have a clear validity bit.
                    batch.histograms = None;
                    batch.histograms = None;
                    batch.source_timestamps = None;
                    batch
                })
            })
        })
    }
}
