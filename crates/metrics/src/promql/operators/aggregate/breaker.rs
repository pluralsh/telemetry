//! Pipeline-breaker reducers (`topk`, `bottomk`, `quantile`) that buffer the full
//! step × series grid before selecting.

use super::*;

// ---------------------------------------------------------------------------
// Topk / Bottomk heap entry
// ---------------------------------------------------------------------------
//
// Mirrors `evaluator.rs::KHeapEntry` (lines 664-694): a max-heap whose
// ordering treats "greater" as "more likely to be evicted". For topk,
// `SmallestFirst` = min-heap on value (so `peek()` is the smallest
// currently-selected). For bottomk, `LargestFirst` = max-heap on value.
// Ties break by **larger index first** so the lower index wins selection
// (first-seen preference — matches legacy).

#[derive(Clone, Copy, Debug)]
enum KOrder {
    /// For `topk`: peek() returns the smallest value in the K winners.
    SmallestFirst,
    /// For `bottomk`: peek() returns the largest value in the K winners.
    LargestFirst,
}

/// NaN-aware comparison mirroring `evaluator.rs:652-662`.
///
/// Returns the [`Ordering`] used inside the `BinaryHeap`: `Greater`
/// means "more evictable" (pops first). The heap's `peek()` therefore
/// returns the worst currently-kept candidate, and a new candidate
/// replaces it iff the new cmp vs peek is `Less` ("strictly better").
///
/// NaN always ranks "worse" than any real value — a NaN entry will be
/// the first to be evicted when a real candidate arrives.
///
/// | order              | operation   | "worse" = more evictable |
/// |--------------------|-------------|--------------------------|
/// | `SmallestFirst`    | topk (keep K largest)  | smaller values  |
/// | `LargestFirst`     | bottomk (keep K smallest) | larger values |
#[inline]
fn k_cmp(a: f64, b: f64, order: KOrder) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match order {
            // topk: smaller value ⇒ "worse" ⇒ ordered greater.
            KOrder::SmallestFirst => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
            // bottomk: larger value ⇒ "worse" ⇒ ordered greater.
            KOrder::LargestFirst => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        },
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct KHeapEntry {
    value: f64,
    /// Input-series index in the full child roster. Used for deterministic
    /// tie-breaks and group-map lookup under tiled input batches.
    global_series_idx: u32,
    /// Input-series offset within the current batch's `series_range`.
    /// Filter-shaped outputs (`topk` / `bottomk`) write back into this local
    /// slice, not the full input roster.
    local_series_idx: u32,
    order: KOrder,
}

impl PartialEq for KHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.global_series_idx == other.global_series_idx
            && self.value.to_bits() == other.value.to_bits()
    }
}
impl Eq for KHeapEntry {}
impl PartialOrd for KHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for KHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap. "Greater" = more evictable.
        // Equal values: larger index is more evictable so the smaller
        // index stays in the winners (first-seen preference; matches
        // `evaluator.rs:687-694`).
        k_cmp(self.value, other.value, self.order)
            .then_with(|| self.global_series_idx.cmp(&other.global_series_idx))
    }
}

impl<C: Operator> AggregateOp<C> {
    /// Absorb one child batch into the breaker full-grid buffer.
    /// Row-major by step: `breaker_values[global_step * input_series +
    /// global_series]`. Idempotent across arbitrary batch ordering —
    /// step tiles × series tiles × Concurrent/Coalesce interleaving all
    /// funnel into the same grid.
    pub(super) fn absorb_batch_breaker(&mut self, input: &StepBatch) {
        debug_assert!(self.kind.is_breaker());
        self.debug_assert_batch_within_input_roster(input);

        // Capture the step-timestamp `Arc<[i64]>` once (first non-empty
        // batch is authoritative; the outer grid is shared).
        if self.breaker_step_timestamps.is_none() {
            self.breaker_step_timestamps = Some(input.step_timestamps.clone());
        }

        let step_count_in = input.step_count();
        let in_series_count = input.series_count();
        let input_series_total = self.group_map.input_series_count();
        for step_off in 0..step_count_in {
            let global_step = input.step_range.start + step_off;
            let step_base = step_off * in_series_count;
            let grid_base = global_step * input_series_total;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                if global_series >= input_series_total {
                    continue;
                }
                let grid_cell = grid_base + global_series;
                self.breaker_values[grid_cell] = input.values[cell];
                self.breaker_validity.set(grid_cell);
            }
        }
    }

    /// Finalise breaker output from the buffered full grid. Routes to
    /// the kind-specific `finalise_topk_or_bottomk` / `finalise_quantile`
    /// helpers, each of which runs its per-step selection / quantile
    /// against the complete `(step_count × input_series_count)` grid.
    pub(super) fn finalise_breaker(&mut self) -> Result<StepBatch, QueryError> {
        match self.kind {
            AggregateKind::Topk(k) => self.finalise_topk_or_bottomk(k, KOrder::SmallestFirst),
            AggregateKind::Bottomk(k) => self.finalise_topk_or_bottomk(k, KOrder::LargestFirst),
            AggregateKind::Quantile(q) => self.finalise_quantile(q),
            _ => unreachable!("non-breaker kind routed to finalise_breaker"),
        }
    }

    /// Global-scope topk / bottomk: for each step, push every valid
    /// `(group, series, value)` triple into the per-group heap, then
    /// emit the survivors' input cells with validity=1. Output shape =
    /// `(step_count × input_series_count)` filter batch.
    fn finalise_topk_or_bottomk(&mut self, k: i64, order: KOrder) -> Result<StepBatch, QueryError> {
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let in_series_count = self.group_map.input_series_count();
        let out_cells = step_count * in_series_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for global_step in 0..step_count {
            let k_usize = self.k_for_step(global_step, in_series_count, k);
            for heap in &mut self.heaps {
                heap.clear();
            }
            if k_usize == 0 {
                continue;
            }
            let grid_base = global_step * in_series_count;
            for global_series in 0..in_series_count {
                let cell = grid_base + global_series;
                if !self.breaker_validity.get(cell) {
                    continue;
                }
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = self.breaker_values[cell];
                let entry = KHeapEntry {
                    value: v,
                    global_series_idx: global_series as u32,
                    local_series_idx: global_series as u32,
                    order,
                };
                let heap = &mut self.heaps[group];
                if heap.len() < k_usize {
                    heap.push(entry);
                } else if let Some(worst) = heap.peek()
                    && entry.cmp(worst) == Ordering::Less
                {
                    heap.pop();
                    heap.push(entry);
                }
            }
            let out_base = global_step * in_series_count;
            for heap in &mut self.heaps {
                for entry in heap.drain() {
                    let idx = out_base + entry.local_series_idx as usize;
                    out.values[idx] = entry.value;
                    out.validity.set(idx);
                }
            }
        }

        let step_timestamps = self.breaker_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });
        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..in_series_count,
            values,
            validity,
        ))
    }

    /// Global-scope quantile: for each step, collect every valid
    /// `(group, value)` pair into the per-group sort buffer, then emit
    /// one reducer-shape cell per group with the q-th quantile.
    /// Output shape = `(step_count × group_count)`.
    fn finalise_quantile(&mut self, q: f64) -> Result<StepBatch, QueryError> {
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let in_series_count = self.group_map.input_series_count();
        let group_count = self.group_map.group_count;
        let out_cells = step_count * group_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for global_step in 0..step_count {
            for buf in &mut self.sort_bufs {
                buf.clear();
            }
            let grid_base = global_step * in_series_count;
            for global_series in 0..in_series_count {
                let cell = grid_base + global_series;
                if !self.breaker_validity.get(cell) {
                    continue;
                }
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                self.sort_bufs[group].push(self.breaker_values[cell]);
            }
            let out_base = global_step * group_count;
            for (g, buf) in self.sort_bufs.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }
                let idx = out_base + g;
                out.values[idx] = quantile_linear_interp(q, buf);
                out.validity.set(idx);
            }
        }

        let step_timestamps = self.breaker_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });
        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }

    /// Buffered per-step reduction for `Topk` / `Bottomk` / `Quantile`.
    ///
    /// Memory: the per-step scratch (heap or sort buffer) was reserved
    /// up front in `new`. The per-batch output column is charged here
    /// via `OutBuffers`.
    #[allow(dead_code)]
    fn reduce_batch_breaker(&mut self, input: &StepBatch) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        self.debug_assert_batch_within_input_roster(input);

        match self.kind {
            AggregateKind::Topk(k) => self.reduce_topk_or_bottomk(input, k, KOrder::SmallestFirst),
            AggregateKind::Bottomk(k) => {
                self.reduce_topk_or_bottomk(input, k, KOrder::LargestFirst)
            }
            AggregateKind::Quantile(q) => self.reduce_quantile(input, q),
            _ => {
                debug_assert!(false, "non-breaker kind routed to breaker reducer");
                // Unreachable: the top-level `next()` dispatch branches
                // on `kind.is_breaker()` and the streaming kinds never
                // reach this method. If a new variant is added without
                // updating the dispatch, surface a planner bug loudly
                // in release builds rather than silently misrouting.
                let _ = step_count;
                unreachable!("non-breaker kind routed to breaker reducer")
            }
        }
    }

    /// `topk(k)` / `bottomk(k)` per-step selection.
    ///
    /// Output schema is the **input series** — cells not in the top-K
    /// (bottom-K) of their group get `validity = 0`. Selected cells
    /// carry the input value through unchanged.
    fn reduce_topk_or_bottomk(
        &mut self,
        input: &StepBatch,
        k: i64,
        order: KOrder,
    ) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        let in_series_count = input.series_count();
        // Output column width equals input column width (filter-shape).
        let out_cells = step_count * in_series_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step_off in 0..step_count {
            let k_usize = self.k_for_step(input.step_range.start + step_off, in_series_count, k);
            // Reset heaps for this step. `clear()` retains capacity.
            for heap in &mut self.heaps {
                heap.clear();
            }
            if k_usize == 0 {
                continue;
            }

            let step_base = step_off * in_series_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = input.values[cell];
                let entry = KHeapEntry {
                    value: v,
                    global_series_idx: global_series as u32,
                    local_series_idx: in_series as u32,
                    order,
                };
                let heap = &mut self.heaps[group];
                if heap.len() < k_usize {
                    heap.push(entry);
                } else if let Some(worst) = heap.peek() {
                    // Only displace when the candidate is strictly
                    // better than the current worst (stable-ish —
                    // equal values keep first-seen preference via the
                    // index tie-break already baked into `KHeapEntry`).
                    if entry.cmp(worst) == Ordering::Less {
                        heap.pop();
                        heap.push(entry);
                    }
                }
            }

            // Emit the survivors' cells with validity=1; all other
            // cells for this step stay at validity=0.
            let out_base = step_off * in_series_count;
            for heap in &mut self.heaps {
                for entry in heap.drain() {
                    let idx = out_base + entry.local_series_idx as usize;
                    out.values[idx] = entry.value;
                    out.validity.set(idx);
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            input.step_timestamps.clone(),
            input.step_range.clone(),
            SchemaRef::Static(self.output_schema.clone()),
            input.series_range.clone(),
            values,
            validity,
        ))
    }

    /// `quantile(q)` per-step reduction — one cell per group per step,
    /// linear interpolation between ranks.
    fn reduce_quantile(&mut self, input: &StepBatch, q: f64) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        let in_series_count = input.series_count();
        let group_count = self.group_map.group_count;
        let out_cells = step_count * group_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step_off in 0..step_count {
            for buf in &mut self.sort_bufs {
                buf.clear();
            }

            let step_base = step_off * in_series_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                self.sort_bufs[group].push(input.values[cell]);
            }

            let out_base = step_off * group_count;
            for (g, buf) in self.sort_bufs.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }
                let idx = out_base + g;
                out.values[idx] = quantile_linear_interp(q, buf);
                out.validity.set(idx);
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            input.step_timestamps.clone(),
            input.step_range.clone(),
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }
}

fn quantile_linear_interp(q: f64, buf: &mut [f64]) -> f64 {
    crate::util::quantile_in_place(q, buf)
}
