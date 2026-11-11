//! Joins a deferred leaf's rows — one per series per bucket, ordered by
//! bucket — where a series' samples meet at a bucket edge.
//!
//! A step's window `(t - window, t]` reaches into the bucket before `t`'s
//! own only for `t` within `window` after the edge between them, and then
//! only that bucket's samples within `window` before the edge. So a row
//! emits the steps before its bucket's end on its own; the steps from the
//! end on belong to the series' row in the next bucket, which reads the
//! row's samples near the edge (its tail) ahead of its own. A tail no row
//! takes up emits those steps alone once every row has been read.
//!
//! Only rows the source reports a [`SeriesEdge`] for take part, and their
//! fingerprints say which rows are one series. A row without samples near
//! an edge has none a step across it could read.

use std::collections::HashMap;
use std::sync::Arc;

use crate::histogram::FloatHistogram;

use super::super::memory::{MemoryReservation, QueryError};
use super::super::source::SeriesEdge;
use super::vector_selector::histogram_sample_bytes;

/// How many times more rows a selector loads per chunk when it reads their
/// fingerprints with their samples: a bucket's row holds that bucket's
/// samples alone, and each chunk takes a fingerprint read per bucket.
pub(crate) const EDGE_READ_CHUNK_SCALE: usize = 4;

/// A chunk's per-row sample columns, indexed by chunk-local row.
pub(crate) struct Columns<'a> {
    pub(crate) timestamps: &'a mut [Vec<i64>],
    pub(crate) values: &'a mut [Vec<f64>],
    pub(crate) histogram_timestamps: &'a mut [Vec<i64>],
    pub(crate) histograms: &'a mut [Vec<Arc<FloatHistogram>>],
}

/// Effective times `[from_ms, until_ms)` a row emits at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowSpan {
    pub(crate) from_ms: i64,
    pub(crate) until_ms: i64,
}

impl RowSpan {
    pub(crate) const ALL: Self = Self {
        from_ms: i64::MIN,
        until_ms: i64::MAX,
    };
    pub(crate) const NONE: Self = Self {
        from_ms: i64::MAX,
        until_ms: i64::MIN,
    };

    #[inline]
    pub(crate) fn contains(&self, effective_ms: i64) -> bool {
        self.from_ms <= effective_ms && effective_ms < self.until_ms
    }
}

/// A row's samples within the window before its bucket's end.
pub(crate) struct Tail {
    pub(crate) row: usize,
    pub(crate) edge_ms: i64,
    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: Vec<f64>,
    pub(crate) histogram_timestamps: Vec<i64>,
    pub(crate) histograms: Vec<Arc<FloatHistogram>>,
    pub(crate) bytes: usize,
}

pub(crate) struct Stitcher {
    window_ms: i64,
    reservation: MemoryReservation,
    /// Tails no row has taken up, by the edge they end at and fingerprint.
    tails: HashMap<(i64, u128), Tail>,
}

impl Stitcher {
    pub(crate) fn new(window_ms: i64, reservation: MemoryReservation) -> Self {
        Self {
            window_ms,
            reservation,
            tails: HashMap::new(),
        }
    }

    pub(crate) fn window_ms(&self) -> i64 {
        self.window_ms
    }

    /// Joins the rows of the chunk at `chunk_start`, read in row order, to
    /// the tails before them and holds back their own. Returns each row's
    /// span (empty when every row emits all its steps) and the bytes of
    /// tail samples moved into `columns`, whose reservation passes to the
    /// chunk.
    pub(crate) fn stitch(
        &mut self,
        chunk_start: usize,
        columns: Columns<'_>,
        edges: &[(usize, SeriesEdge)],
    ) -> Result<(Vec<RowSpan>, usize), QueryError> {
        if edges.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut spans = vec![RowSpan::ALL; columns.timestamps.len()];
        let mut moved = 0;
        for &(row, edge) in edges {
            let span = &mut spans[row];
            if let Some(head_ms) = edge.head_ms
                && let Some(tail) = self.tails.remove(&(head_ms, edge.fingerprint))
            {
                moved += tail.bytes;
                prepend(&mut columns.timestamps[row], tail.timestamps);
                prepend(&mut columns.values[row], tail.values);
                prepend(
                    &mut columns.histogram_timestamps[row],
                    tail.histogram_timestamps,
                );
                prepend(&mut columns.histograms[row], tail.histograms);
                span.from_ms = head_ms;
            }
            if let Some(edge_ms) = edge.tail_ms {
                let after = edge_ms - self.window_ms;
                let from = columns.timestamps[row].partition_point(|&ts| ts <= after);
                let histograms_from =
                    columns.histogram_timestamps[row].partition_point(|&ts| ts <= after);
                let tail = Tail {
                    row: chunk_start + row,
                    edge_ms,
                    timestamps: columns.timestamps[row][from..].to_vec(),
                    values: columns.values[row][from..].to_vec(),
                    histogram_timestamps: columns.histogram_timestamps[row][histograms_from..]
                        .to_vec(),
                    histograms: columns.histograms[row][histograms_from..].to_vec(),
                    bytes: 0,
                };
                let bytes = tail_bytes(&tail);
                self.reservation.try_grow(bytes)?;
                self.tails
                    .insert((edge_ms, edge.fingerprint), Tail { bytes, ..tail });
                span.until_ms = edge_ms;
            }
        }
        Ok((spans, moved))
    }

    /// Every tail no row took up, by row; their reservations pass to the
    /// caller.
    pub(crate) fn take_orphans(&mut self) -> Vec<Tail> {
        let mut tails: Vec<Tail> = self.tails.drain().map(|(_, tail)| tail).collect();
        tails.sort_unstable_by_key(|tail| tail.row);
        tails
    }
}

impl Drop for Stitcher {
    fn drop(&mut self) {
        let bytes: usize = self.tails.values().map(|tail| tail.bytes).sum();
        if bytes > 0 {
            self.reservation.release(bytes);
        }
    }
}

fn tail_bytes(tail: &Tail) -> usize {
    let floats = tail
        .timestamps
        .len()
        .saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>());
    tail.histograms
        .iter()
        .map(|h| histogram_sample_bytes(h))
        .fold(floats, usize::saturating_add)
}

fn prepend<T>(column: &mut Vec<T>, mut head: Vec<T>) {
    head.append(column);
    *column = head;
}

/// The next run of `orphans` (by row) a chunk of at most `max_rows` rows
/// covers: its first row, its tails and each of its rows' span, which is
/// empty for rows without a tail.
pub(crate) fn orphan_run(
    orphans: &mut std::collections::VecDeque<Tail>,
    max_rows: usize,
    window_ms: i64,
) -> Option<(usize, Vec<Tail>, Vec<RowSpan>)> {
    let first = orphans.front()?.row;
    let mut run = Vec::new();
    while let Some(tail) = orphans.front()
        && tail.row < first + max_rows
    {
        run.push(orphans.pop_front().expect("front"));
    }
    let rows = run.last().expect("non-empty run").row - first + 1;
    let mut spans = vec![RowSpan::NONE; rows];
    for tail in &run {
        spans[tail.row - first] = RowSpan {
            from_ms: tail.edge_ms,
            until_ms: tail.edge_ms.saturating_add(window_ms),
        };
    }
    Some((first, run, spans))
}

/// The steps `[lo, hi)` of `effective_times` (non-decreasing) at which some
/// row can emit: each row only within `window_ms` after a sample of its
/// own, and within its span.
pub(crate) fn step_bounds(
    effective_times: &[i64],
    window_ms: i64,
    timestamps: &[Vec<i64>],
    histogram_timestamps: &[Vec<i64>],
    spans: &[RowSpan],
) -> (usize, usize) {
    let (mut lo, mut hi) = (i64::MAX, i64::MIN);
    for (row, (floats, histograms)) in timestamps.iter().zip(histogram_timestamps).enumerate() {
        let first = floats.first().into_iter().chain(histograms.first()).min();
        let last = floats.last().into_iter().chain(histograms.last()).max();
        let (Some(&first), Some(&last)) = (first, last) else {
            continue;
        };
        let span = spans.get(row).copied().unwrap_or(RowSpan::ALL);
        let from = first.max(span.from_ms);
        let until = last.saturating_add(window_ms).min(span.until_ms);
        if from < until {
            lo = lo.min(from);
            hi = hi.max(until);
        }
    }
    if lo >= hi {
        return (0, 0);
    }
    (
        effective_times.partition_point(|&t| t < lo),
        effective_times.partition_point(|&t| t < hi),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(
        index: usize,
        fingerprint: u128,
        head_ms: Option<i64>,
        tail_ms: Option<i64>,
    ) -> SeriesEdge {
        SeriesEdge {
            index,
            fingerprint,
            head_ms,
            tail_ms,
        }
    }

    struct Chunk {
        timestamps: Vec<Vec<i64>>,
        values: Vec<Vec<f64>>,
        histogram_timestamps: Vec<Vec<i64>>,
        histograms: Vec<Vec<Arc<FloatHistogram>>>,
    }

    impl Chunk {
        fn new(rows: Vec<Vec<i64>>) -> Self {
            let values = rows
                .iter()
                .map(|ts| ts.iter().map(|&t| t as f64).collect())
                .collect();
            let n = rows.len();
            Self {
                timestamps: rows,
                values,
                histogram_timestamps: vec![Vec::new(); n],
                histograms: vec![Vec::new(); n],
            }
        }

        fn columns(&mut self) -> Columns<'_> {
            Columns {
                timestamps: &mut self.timestamps,
                values: &mut self.values,
                histogram_timestamps: &mut self.histogram_timestamps,
                histograms: &mut self.histograms,
            }
        }
    }

    #[test]
    fn should_join_a_series_across_the_edge_its_samples_meet_at() {
        // given: a series near the end of the bucket ending at 100, a
        // second series ending early, and the first series again after 100
        let reservation = MemoryReservation::new(1 << 20);
        let mut stitcher = Stitcher::new(10, reservation.clone());
        let mut before = Chunk::new(vec![vec![50, 95, 99], vec![40, 60]]);
        let mut after = Chunk::new(vec![vec![102, 150]]);

        // when
        let (before_spans, _) = stitcher
            .stitch(0, before.columns(), &[(0, edge(0, 7, None, Some(100)))])
            .unwrap();
        let (after_spans, moved) = stitcher
            .stitch(2, after.columns(), &[(0, edge(0, 7, Some(100), None))])
            .unwrap();

        // then: the first row stops at the edge, the next takes over with
        // the samples a window after the edge can read
        assert_eq!(
            before_spans[0],
            RowSpan {
                from_ms: i64::MIN,
                until_ms: 100
            }
        );
        assert_eq!(before_spans[1], RowSpan::ALL);
        assert_eq!(
            after_spans[0],
            RowSpan {
                from_ms: 100,
                until_ms: i64::MAX
            }
        );
        assert_eq!(after.timestamps[0], vec![95, 99, 102, 150]);
        assert_eq!(moved, 2 * 16);
        assert!(stitcher.take_orphans().is_empty());
    }

    #[test]
    fn should_leave_a_tail_no_row_takes_up_to_emit_alone() {
        // given: a series ending near the edge and another series after it
        let reservation = MemoryReservation::new(1 << 20);
        let mut stitcher = Stitcher::new(10, reservation.clone());
        let mut before = Chunk::new(vec![vec![95]]);
        let mut after = Chunk::new(vec![vec![101]]);

        // when
        stitcher
            .stitch(0, before.columns(), &[(0, edge(0, 7, None, Some(100)))])
            .unwrap();
        let (spans, moved) = stitcher
            .stitch(1, after.columns(), &[(0, edge(0, 8, Some(100), None))])
            .unwrap();
        let mut orphans: std::collections::VecDeque<Tail> = stitcher.take_orphans().into();

        // then
        assert_eq!(spans[0], RowSpan::ALL);
        assert_eq!(moved, 0);
        let (first, run, spans) = orphan_run(&mut orphans, 512, 10).unwrap();
        assert_eq!((first, run.len()), (0, 1));
        assert_eq!(run[0].timestamps, vec![95]);
        assert_eq!(
            spans,
            vec![RowSpan {
                from_ms: 100,
                until_ms: 110
            }]
        );
        assert!(orphans.is_empty());
    }

    #[test]
    fn should_bound_steps_by_samples_window_and_spans() {
        // given: steps every 10ms and rows emitting within 5ms of samples
        let effective: Vec<i64> = (0..20).map(|k| k * 10).collect();
        let timestamps = vec![vec![42, 61], vec![], vec![120]];
        let histograms = vec![vec![], vec![], vec![]];
        let spans = vec![
            RowSpan::ALL,
            RowSpan::ALL,
            RowSpan {
                from_ms: i64::MIN,
                until_ms: 100,
            },
        ];

        // when
        let bounds = step_bounds(&effective, 5, &timestamps, &histograms, &spans);

        // then: from step 50 (first ≥ 42) to the step after 61 + 5
        assert_eq!(bounds, (5, 7));
    }
}
