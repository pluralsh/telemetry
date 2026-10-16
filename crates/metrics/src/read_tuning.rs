//! Cold-read strategy and write-layout switches. Every combination returns
//! the same results; only object-store round trips and requests differ.
//! Constant outside tests, where profiles and differential tests override
//! them.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadTuning {
    /// Range-scan each dense cluster of a sparse batch instead of falling
    /// back to point gets for the whole batch.
    pub(crate) cluster_scans: bool,
    /// In-flight point gets per forward-index or fingerprint batch.
    pub(crate) point_get_concurrency: usize,
    /// Series chunks a selector loads ahead of the one it emits.
    pub(crate) chunk_prefetch: usize,
    /// Find a replica bucket's postings watermark with one descending scan
    /// instead of galloping point gets.
    pub(crate) scan_watermark: bool,
    /// Resolve a label-projected selector in full when its candidates' IDs
    /// are dense, where cluster scans read their forward-index entries in
    /// fewer round trips than scanning every projected label's postings.
    pub(crate) unproject_dense: bool,
}

impl ReadTuning {
    pub(crate) const DEFAULT: Self = Self {
        cluster_scans: true,
        point_get_concurrency: 256,
        chunk_prefetch: 4,
        scan_watermark: true,
        unproject_dense: true,
    };
}

#[cfg(not(test))]
pub(crate) fn read_tuning() -> ReadTuning {
    ReadTuning::DEFAULT
}

#[cfg(test)]
static OVERRIDE: std::sync::RwLock<Option<ReadTuning>> = std::sync::RwLock::new(None);
#[cfg(test)]
static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) fn read_tuning() -> ReadTuning {
    OVERRIDE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .unwrap_or(ReadTuning::DEFAULT)
}

/// Holds [`read_tuning`] at `tuning` process-wide until dropped. Overrides
/// exclude each other, so tests holding one take turns.
#[cfg(test)]
pub(crate) async fn override_read_tuning(tuning: ReadTuning) -> TuningOverride {
    let exclusive = EXCLUSIVE.lock().await;
    *OVERRIDE
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(tuning);
    TuningOverride {
        _exclusive: exclusive,
    }
}

/// [`override_read_tuning`] to the defaults, but resolving projected labels
/// from the index however dense the candidates, for tests of that path.
#[cfg(test)]
pub(crate) async fn override_index_projection() -> TuningOverride {
    override_read_tuning(ReadTuning {
        unproject_dense: false,
        ..ReadTuning::DEFAULT
    })
    .await
}

#[cfg(test)]
pub(crate) struct TuningOverride {
    _exclusive: tokio::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for TuningOverride {
    fn drop(&mut self) {
        *OVERRIDE
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// How series IDs are assigned, for the cold reads above: a metric's
/// forward-index and fingerprint records are contiguous only if its IDs
/// are. Every combination stores the same series and samples, and IDs stay
/// dense and visible in order, which [`crate::minitsdb::ReplicaPostings`]
/// relies on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteLayout {
    /// Renumber each flush's new series in (metric, labels) order.
    pub(crate) sorted_series_ids: bool,
    /// Register every series a rollup copies before any of its samples, so
    /// series first seen in a later source are numbered with their metric's
    /// rather than in a tail.
    pub(crate) rollup_presort: bool,
}

impl WriteLayout {
    pub(crate) const DEFAULT: Self = Self {
        sorted_series_ids: true,
        rollup_presort: true,
    };
}

#[cfg(not(test))]
pub(crate) fn write_layout() -> WriteLayout {
    WriteLayout::DEFAULT
}

#[cfg(test)]
static LAYOUT_OVERRIDE: std::sync::RwLock<Option<WriteLayout>> = std::sync::RwLock::new(None);
#[cfg(test)]
static LAYOUT_EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) fn write_layout() -> WriteLayout {
    LAYOUT_OVERRIDE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .unwrap_or(WriteLayout::DEFAULT)
}

/// Holds [`write_layout`] at `layout` process-wide until dropped, like
/// [`override_read_tuning`].
#[cfg(test)]
pub(crate) async fn override_write_layout(layout: WriteLayout) -> LayoutOverride {
    let exclusive = LAYOUT_EXCLUSIVE.lock().await;
    *LAYOUT_OVERRIDE
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(layout);
    LayoutOverride {
        _exclusive: exclusive,
    }
}

#[cfg(test)]
pub(crate) struct LayoutOverride {
    _exclusive: tokio::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl LayoutOverride {
    /// Switches the held layout, as a writer restarted on another binary.
    pub(crate) fn set(&self, layout: WriteLayout) {
        *LAYOUT_OVERRIDE
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(layout);
    }
}

#[cfg(test)]
impl Drop for LayoutOverride {
    fn drop(&mut self) {
        *LAYOUT_OVERRIDE
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// Every combination of the layout switches.
#[cfg(test)]
pub(crate) fn every_layout() -> Vec<WriteLayout> {
    let mut out = Vec::new();
    for sorted_series_ids in [false, true] {
        for rollup_presort in [false, true] {
            out.push(WriteLayout {
                sorted_series_ids,
                rollup_presort,
            });
        }
    }
    out
}

/// Every combination of the switches, at point-get concurrencies of one and
/// the default.
#[cfg(test)]
pub(crate) fn every_tuning() -> Vec<ReadTuning> {
    let mut out = Vec::new();
    for cluster_scans in [false, true] {
        for point_get_concurrency in [1, ReadTuning::DEFAULT.point_get_concurrency] {
            for chunk_prefetch in [0, 1, ReadTuning::DEFAULT.chunk_prefetch] {
                for scan_watermark in [false, true] {
                    for unproject_dense in [false, true] {
                        out.push(ReadTuning {
                            cluster_scans,
                            point_get_concurrency,
                            chunk_prefetch,
                            scan_watermark,
                            unproject_dense,
                        });
                    }
                }
            }
        }
    }
    out
}
