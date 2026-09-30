//! `CountValuesOp` implements PromQL `count_values` — the one aggregation
//! whose output labelset depends on the sample values it sees, not just
//! the query's matchers. It buckets input series by their per-step value
//! and emits one output series per distinct observed value, labelled
//! `{<label>="<value>"}` plus any `by` / `without` grouping labels.
//!
//! Because the output roster isn't known until the samples have been
//! read, this is a pipeline breaker (buffers its child fully before
//! emitting): it drains the child, discovers the distinct values,
//! finalises an output [`SeriesSchema`], and only then emits batches.
//! [`Operator::schema`] publishes [`SchemaRef::Deferred`] for the
//! operator's whole life; downstream consumers read the concrete schema
//! off each emitted batch (internally memoised and exposed via
//! [`CountValuesOp::finalized_schema`]).
//!
//! Value-to-label formatting matches Prometheus' Go
//! `FormatFloat(v, 'f', -1, 64)`: NaN → `"NaN"`, ±Inf → `"±Inf"`, `-0.0`
//! → `"-0"`, finite values use the shortest round-trip decimal. Buckets
//! key on `f64::to_bits()` internally so `+0.0` / `-0.0` and individual
//! NaN bit patterns don't collide during hashing; label formatting then
//! collapses all NaN patterns to `"NaN"` so the user-facing roster stays
//! consistent.
//!
//! The optional [`GroupMap`] partitions inputs into `(group, value)`
//! buckets; absent a group map, all inputs feed one synthetic group. The
//! planner passes one `Labels` per group for composing output rows.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::{Label, Labels};

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};
use super::aggregate::GroupMap;

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

pub use common::display::prometheus_float as format_value_label;

// ---------------------------------------------------------------------------
// Intermediate bucket state
// ---------------------------------------------------------------------------

/// Key into the `(group, value-bits)` intermediate map.
///
/// `value_bits = f64::to_bits(v)`: distinguishes `+0.0` from `-0.0` and
/// every NaN bit pattern. Label formatting later collapses all NaNs to
/// the literal `"NaN"`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BucketKey {
    group: u32,
    value_bits: u64,
}

/// Per-bucket counts array — one entry per step in the output grid.
#[derive(Debug)]
struct BucketCounts {
    /// Length = `step_count`. Each entry is the number of input series
    /// in `(group)` that observed this value at that step.
    per_step: Vec<u32>,
}

// ---------------------------------------------------------------------------
// Memory accounting
// ---------------------------------------------------------------------------

/// Conservative per-bucket byte cost: one `HashMap` entry (~48 B on
/// 64-bit for `(BucketKey, BucketCounts)` with hasher overhead) + the
/// `Vec<u32>` per-step counter (`4 * step_count` bytes). Overshoots
/// slightly to stay safely on the upper-bound side of the reservation.
#[inline]
fn bucket_bytes(step_count: usize) -> usize {
    const ENTRY_OVERHEAD: usize = 64;
    ENTRY_OVERHEAD.saturating_add(step_count.saturating_mul(std::mem::size_of::<u32>()))
}

#[inline]
fn schema_bytes(series_count: usize, avg_labels_per_series: usize) -> usize {
    // `Arc<[Labels]>` (`Vec<Label>` per series) + `Arc<[u128]>`
    // fingerprints. Per-series cost: one `Vec<Label>` header (24 B) +
    // `avg_labels_per_series * (sizeof(Label) + avg label string bytes)`.
    // We conservatively assume 32 bytes per label string tail on top of
    // the 48-byte `Label` struct. Plus 16 B for the fingerprint.
    const VEC_HDR: usize = 24;
    const LABEL_STRUCT: usize = std::mem::size_of::<Label>();
    const LABEL_STRING_TAIL: usize = 32;
    let per_label = LABEL_STRUCT.saturating_add(LABEL_STRING_TAIL);
    let per_series = VEC_HDR
        .saturating_add(avg_labels_per_series.saturating_mul(per_label))
        .saturating_add(std::mem::size_of::<u128>());
    series_count.saturating_mul(per_series)
}

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// RAII guard for the per-batch output `Vec<f64>` + `BitSet`. Mirrors the
/// pattern in `aggregate.rs` / `rollup.rs`.
struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl OutBuffers {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![0.0; cells],
            validity: BitSet::with_len(cells),
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::replace(&mut self.validity, BitSet::with_len(0));
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for OutBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// CountValuesOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's `count_values`. The one operator whose output
/// series roster depends on sample values — so it publishes
/// [`SchemaRef::Deferred`] from [`Self::schema`] and only binds a concrete
/// [`SeriesSchema`] after it has drained its child.
///
/// Life cycle:
/// 1. Constructor stores the child and grouping configuration.
///    [`Self::schema`] returns [`SchemaRef::Deferred`].
/// 2. The first call to [`Self::next`] drains the entire child, builds
///    the intermediate `(group, value)` counts table, finalises the
///    output [`SeriesSchema`] (which [`Self::finalized_schema`] then
///    exposes), and emits one [`StepBatch`] covering the whole grid.
/// 3. Subsequent calls to [`Self::next`] return `Poll::Ready(None)`.
pub struct CountValuesOp<C: Operator> {
    child: Option<C>,
    label_name: String,
    /// Optional grouping. `None` ⇒ all inputs fall into a single
    /// synthetic group (group_count = 1, group labels empty).
    group_map: Option<GroupMap>,
    /// Per-group label sets (planner-supplied). Length equals
    /// `group_count` when grouping is used; length = 1 with empty labels
    /// when grouping is absent.
    group_labels: Arc<[Labels]>,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Finalised output schema, populated once the child has been drained
    /// and the `(group, value)` roster has been computed. Exposed via
    /// [`Self::finalized_schema`] after draining.
    finalized: Option<Arc<SeriesSchema>>,
    /// Bytes reserved for the intermediate buckets + finalised schema;
    /// released on `Drop`.
    scratch_bytes: usize,
    done: bool,
    errored: bool,
}

impl<C: Operator> CountValuesOp<C> {
    /// Construct a `count_values` operator.
    ///
    /// * `child` — upstream operator producing the value stream.
    /// * `label_name` — label to attach to output series (e.g. `"version"`).
    ///   The planner is responsible for validating the label name is a
    ///   legal Prometheus label identifier.
    /// * `group_map` — optional `by`/`without` grouping. `None` ⇒ every
    ///   input is aggregated into one group (no group labels).
    /// * `group_labels` — per-group label sets. Required to have
    ///   `group_map.group_count` entries when `group_map` is `Some`; must
    ///   be `[Labels::empty()]` when `group_map` is `None`.
    /// * `reservation` — per-query reservation.
    /// * `step_grid` — outer step grid the emitted batch lands on.
    pub fn new(
        child: C,
        label_name: impl Into<String>,
        group_map: Option<GroupMap>,
        group_labels: Arc<[Labels]>,
        reservation: MemoryReservation,
    ) -> Self {
        let step_grid = child.schema().step_grid;

        debug_assert!(
            match &group_map {
                Some(g) => group_labels.len() == g.group_count,
                None => group_labels.len() == 1,
            },
            "group_labels length must match group_count (or be 1 when ungrouped)",
        );

        Self {
            child: Some(child),
            label_name: label_name.into(),
            group_map,
            group_labels,
            reservation,
            schema: OperatorSchema::new(SchemaRef::Deferred, step_grid),
            finalized: None,
            scratch_bytes: 0,
            done: false,
            errored: false,
        }
    }

    /// Returns the finalised output schema once the child has been drained
    /// and the first emit has happened. Before that, returns `None`.
    ///
    /// Downstream operators that need the concrete roster (a planner
    /// concern) should call this after polling their deferred child to
    /// completion of its first batch.
    pub fn finalized_schema(&self) -> Option<&Arc<SeriesSchema>> {
        self.finalized.as_ref()
    }

    /// Drain the child synchronously under the supplied context. Returns
    /// `Ok(Some(batches))` on full drain, `Ok(None)` on pending, or
    /// `Err` on upstream error.
    #[allow(clippy::type_complexity)]
    fn drain_child(&mut self, cx: &mut Context<'_>) -> Result<Option<Vec<StepBatch>>, QueryError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(Some(Vec::new()));
        };
        let mut batches = Vec::new();
        loop {
            match child.next(cx) {
                Poll::Pending => return Ok(None),
                Poll::Ready(None) => {
                    self.child = None;
                    return Ok(Some(batches));
                }
                Poll::Ready(Some(Err(err))) => return Err(err),
                Poll::Ready(Some(Ok(batch))) => batches.push(batch),
            }
        }
    }

    /// Adds one occurrence per valid cell of `batch` to its
    /// `(group, value_bits)` bucket at the cell's global step.
    fn count_batch(
        &mut self,
        batch: &StepBatch,
        step_count: usize,
        buckets: &mut HashMap<BucketKey, BucketCounts>,
        bucket_order: &mut Vec<BucketKey>,
    ) -> Result<(), QueryError> {
        let in_series_count = batch.series_count();
        for step_off in 0..batch.step_count() {
            let global_step = batch.step_range.start + step_off;
            // Guard against mismatched child grid — `global_step` must land
            // inside the outer grid. Planner bug otherwise.
            if global_step >= step_count {
                debug_assert!(
                    false,
                    "global_step {global_step} exceeds outer grid step_count {step_count}"
                );
                continue;
            }
            for s in 0..in_series_count {
                let cell = step_off * in_series_count + s;
                if !batch.validity.get(cell) {
                    continue;
                }
                let Some(group) = self.group_of(batch.series_range.start + s) else {
                    continue;
                };
                let key = BucketKey {
                    group,
                    value_bits: batch.values[cell].to_bits(),
                };
                let counts = match buckets.entry(key) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        // Reserve one bucket worth of bytes before inserting
                        // a fresh entry.
                        let bytes = bucket_bytes(step_count);
                        self.reservation.try_grow(bytes)?;
                        self.scratch_bytes = self.scratch_bytes.saturating_add(bytes);
                        bucket_order.push(key);
                        entry.insert(BucketCounts {
                            per_step: vec![0u32; step_count],
                        })
                    }
                };
                counts.per_step[global_step] = counts.per_step[global_step].saturating_add(1);
            }
        }
        Ok(())
    }

    /// The output group of an input series, or `None` if the series is
    /// excluded from grouping.
    fn group_of(&self, global_series: usize) -> Option<u32> {
        let Some(group_map) = &self.group_map else {
            return Some(0);
        };
        match group_map.input_to_group.get(global_series) {
            Some(group) => *group,
            // Out-of-range series index ⇒ planner bug. Drop defensively
            // (debug_assert catches tests).
            None => {
                debug_assert!(
                    false,
                    "series index {global_series} out of group_map bounds"
                );
                None
            }
        }
    }

    /// After the child has been fully drained, build the counts table,
    /// finalise the schema, and emit the full-grid `StepBatch`.
    fn finalise(&mut self, batches: Vec<StepBatch>) -> Result<StepBatch, QueryError> {
        let step_count = self.schema.step_grid.step_count;

        // The query grid's `step_timestamps: Arc<[i64]>`. Shared by every
        // input batch; take the first one and reuse it. If the child
        // produced no batches we synthesise a fresh `Arc<[i64]>` sized to
        // the grid — output is an empty-roster batch (no series), which
        // the downstream treats as "no series emitted".
        let step_timestamps: Arc<[i64]> = batches
            .first()
            .map(|b| b.step_timestamps.clone())
            .unwrap_or_else(|| Arc::from(vec![0i64; step_count].into_boxed_slice()));

        // Bucket every `(group, value_bits)` occurrence per step.
        let mut buckets: HashMap<BucketKey, BucketCounts> = HashMap::new();
        // Stable emission order: insertion order of bucket keys. Keeps
        // tests deterministic and matches "first-seen wins" semantics
        // found elsewhere in the engine.
        let mut bucket_order: Vec<BucketKey> = Vec::new();

        for batch in &batches {
            self.count_batch(batch, step_count, &mut buckets, &mut bucket_order)?;
        }

        // Build the output roster in insertion order.
        let out_series_count = bucket_order.len();
        let mut labels_vec: Vec<Labels> = Vec::with_capacity(out_series_count);
        let mut fps_vec: Vec<u128> = Vec::with_capacity(out_series_count);
        let avg_labels_per_series = self
            .group_labels
            .first()
            .map(|l| l.len())
            .unwrap_or(0)
            .saturating_add(1);
        let schema_budget = schema_bytes(out_series_count, avg_labels_per_series);
        self.reservation.try_grow(schema_budget)?;
        self.scratch_bytes = self.scratch_bytes.saturating_add(schema_budget);

        for (fp, key) in bucket_order.iter().enumerate() {
            let value = f64::from_bits(key.value_bits);
            let group_idx = key.group as usize;
            let base_labels = self
                .group_labels
                .get(group_idx)
                .cloned()
                .unwrap_or_else(Labels::empty);
            let mut label_vec: Vec<Label> = base_labels
                .iter()
                .filter(|l| l.name != self.label_name)
                .cloned()
                .collect();
            label_vec.push(Label {
                name: self.label_name.clone(),
                value: format_value_label(value),
            });
            label_vec.sort();
            labels_vec.push(Labels::new(label_vec));
            // Simple stable fingerprint — deterministic but not
            // cross-query comparable. The planner's fingerprint function
            // can replace this in Phase 4; for now insertion order is
            // sufficient for downstream identity checks.
            fps_vec.push(fp as u128);
        }

        let output_schema = Arc::new(SeriesSchema::new(
            Arc::from(labels_vec.into_boxed_slice()),
            Arc::from(fps_vec.into_boxed_slice()),
        ));
        self.finalized = Some(output_schema.clone());

        // Emit a single StepBatch spanning the full grid × all output
        // series. Values are the counts; validity=1 iff any input
        // contributed for that (step, output-series).
        let cells = step_count.saturating_mul(out_series_count);
        let mut out = OutBuffers::allocate(&self.reservation, cells)?;
        for (out_idx, key) in bucket_order.iter().enumerate() {
            let counts = buckets
                .get(key)
                .expect("bucket present in order vec must be in map");
            for step in 0..step_count {
                let n = counts.per_step[step];
                if n == 0 {
                    continue;
                }
                let cell = step * out_series_count + out_idx;
                out.values[cell] = n as f64;
                out.validity.set(cell);
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(output_schema),
            0..out_series_count,
            values,
            validity,
        ))
    }
}

impl<C: Operator> Operator for CountValuesOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        if self.finalized.is_some() {
            // Already emitted the one batch; transition to done.
            self.done = true;
            return Poll::Ready(None);
        }
        match self.drain_child(cx) {
            Ok(None) => Poll::Pending,
            Ok(Some(batches)) => match self.finalise(batches) {
                Ok(batch) => {
                    self.done = true;
                    Poll::Ready(Some(Ok(batch)))
                }
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
            Err(err) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

impl<C: Operator> Drop for CountValuesOp<C> {
    fn drop(&mut self) {
        if self.scratch_bytes > 0 {
            self.reservation.release(self.scratch_bytes);
            self.scratch_bytes = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
