//! Memory-accounted output buffers and drained full-grid scratch for vector/vector matching.

use super::*;

// ---------------------------------------------------------------------------
// Output buffer helpers
// ---------------------------------------------------------------------------

#[inline]
pub(super) fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// RAII output-buffer pair charged to a [`MemoryReservation`].
pub(super) struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    pub(super) values: Vec<f64>,
    pub(super) validity: BitSet,
}

impl OutBuffers {
    pub(super) fn allocate(
        reservation: &MemoryReservation,
        cells: usize,
    ) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![f64::NAN; cells],
            validity: BitSet::with_len(cells),
        })
    }

    pub(super) fn finish(mut self) -> (Vec<f64>, BitSet) {
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
// BufferedSide — drained full-grid scratch for the vector/vector path
// ---------------------------------------------------------------------------

/// Dense `(step_count × total_series_count)` buffer for one side of a
/// vector/vector binary operation. Populated by absorbing every input
/// batch from the child into the cell grid, regardless of the child's
/// tile shape (step × series). Used for cross-tile matching — see
/// [`BinaryOp::apply_vv`].
///
/// The grid is row-major by step: `values[step * total_series + series]`
/// (matching `StepBatch`'s layout). `validity` runs parallel.
///
/// Memory is reserved up front from the shared reservation and released
/// on drop; an undersize reservation surfaces `QueryError::MemoryLimit`
/// at construction.
pub(super) struct BufferedSide {
    reservation: MemoryReservation,
    bytes: usize,
    pub(super) step_count: usize,
    total_series: usize,
    values: Vec<f64>,
    validity: BitSet,
    /// Allocated on the first absorbed batch carrying histograms.
    histograms: Option<HistogramCells>,
    pub(super) step_timestamps: Arc<[i64]>,
}

impl BufferedSide {
    pub(super) fn allocate(
        reservation: &MemoryReservation,
        step_count: usize,
        total_series: usize,
        step_timestamps: Arc<[i64]>,
    ) -> Result<Self, QueryError> {
        let cells = step_count.saturating_mul(total_series);
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            step_count,
            total_series,
            values: vec![f64::NAN; cells],
            validity: BitSet::with_len(cells),
            histograms: None,
            step_timestamps,
        })
    }

    /// Absorb one child batch into the grid using the batch's global
    /// `(step_range, series_range)` as its destination; idempotent across
    /// arbitrary child tiling (step × series) and interleaved emission.
    pub(super) fn absorb(&mut self, batch: &StepBatch) {
        let step_count_in = batch.step_count();
        let series_count_in = batch.series_count();
        for step_off in 0..step_count_in {
            let global_step = batch.step_range.start + step_off;
            if global_step >= self.step_count {
                continue;
            }
            let in_base = step_off * series_count_in;
            let out_base = global_step * self.total_series;
            for s in 0..series_count_in {
                let in_cell = in_base + s;
                if !batch.validity.get(in_cell) {
                    continue;
                }
                let global_series = batch.series_range.start + s;
                if global_series >= self.total_series {
                    continue;
                }
                let out_cell = out_base + global_series;
                self.values[out_cell] = batch.values[in_cell];
                self.validity.set(out_cell);
            }
        }
        let Some(cells) = &batch.histograms else {
            return;
        };
        let total = self.step_count * self.total_series;
        let out = self.histograms.get_or_insert_with(|| vec![None; total]);
        for step_off in 0..step_count_in {
            let global_step = batch.step_range.start + step_off;
            if global_step >= self.step_count {
                continue;
            }
            for s in 0..series_count_in {
                let global_series = batch.series_range.start + s;
                if global_series >= self.total_series {
                    continue;
                }
                if let Some(h) = &cells[step_off * series_count_in + s] {
                    out[global_step * self.total_series + global_series] = Some(h.clone());
                }
            }
        }
    }

    #[inline]
    pub(super) fn has_histograms(&self) -> bool {
        self.histograms.is_some()
    }

    #[inline]
    pub(super) fn get_histogram(
        &self,
        step: usize,
        global_series: usize,
    ) -> Option<&Arc<FloatHistogram>> {
        if step >= self.step_count || global_series >= self.total_series {
            return None;
        }
        self.histograms.as_ref()?[step * self.total_series + global_series].as_ref()
    }

    /// Fetch `(step, global_series)` as `Option<f64>`: `Some(v)` iff the
    /// cell was written by at least one absorbed batch.
    #[inline]
    pub(super) fn get(&self, step: usize, global_series: usize) -> Option<f64> {
        if step >= self.step_count || global_series >= self.total_series {
            return None;
        }
        let cell = step * self.total_series + global_series;
        if self.validity.get(cell) {
            Some(self.values[cell])
        } else {
            None
        }
    }
}

impl Drop for BufferedSide {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
            self.bytes = 0;
        }
    }
}
