//! Drops `__name__` from every output series, as Prometheus does for
//! arithmetic, `bool` comparisons and all functions but `last_over_time`
//! and the label functions.
//!
//! Only used when the stripped labelsets stay distinct, so it is a
//! streaming relabel: each batch passes through restamped with the
//! stripped schema. Collisions instead go through
//! [`super::label_manip::LabelManipOp`], which merges the colliding series
//! the way Prometheus does.

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::promql::batch::{SchemaRef, SeriesSchema, StepBatch};
use crate::promql::memory::QueryError;
use crate::promql::operator::{Operator, OperatorSchema};

pub struct DropNameOp<C: Operator> {
    child: C,
    schema: OperatorSchema,
}

impl<C: Operator> DropNameOp<C> {
    /// `output_schema` lists the child's series in order, minus `__name__`.
    pub fn new(child: C, output_schema: Arc<SeriesSchema>) -> Self {
        debug_assert_eq!(
            child.schema().series.as_static().map(|schema| schema.len()),
            Some(output_schema.len()),
            "drop-name output must map the child's series one to one",
        );
        let step_grid = child.schema().step_grid;
        Self {
            child,
            schema: OperatorSchema::new(SchemaRef::Static(output_schema), step_grid),
        }
    }
}

impl<C: Operator> Operator for DropNameOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        self.child.next(cx).map(|batch| {
            batch.map(|batch| {
                batch.map(|mut batch| {
                    batch.series = self.schema.series.clone();
                    batch
                })
            })
        })
    }
}
