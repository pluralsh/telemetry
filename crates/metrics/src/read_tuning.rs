//! Cold-read strategy switches. Every combination returns the same results;
//! only object-store round trips and requests differ. Constant outside
//! tests, where profiles and differential tests override them.

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
}

impl ReadTuning {
    pub(crate) const DEFAULT: Self = Self {
        cluster_scans: true,
        point_get_concurrency: 256,
        chunk_prefetch: 2,
        scan_watermark: true,
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

/// Every combination of the switches, at point-get concurrencies of one and
/// the default.
#[cfg(test)]
pub(crate) fn every_tuning() -> Vec<ReadTuning> {
    let mut out = Vec::new();
    for cluster_scans in [false, true] {
        for point_get_concurrency in [1, ReadTuning::DEFAULT.point_get_concurrency] {
            for chunk_prefetch in [0, 1, ReadTuning::DEFAULT.chunk_prefetch] {
                for scan_watermark in [false, true] {
                    out.push(ReadTuning {
                        cluster_scans,
                        point_get_concurrency,
                        chunk_prefetch,
                        scan_watermark,
                    });
                }
            }
        }
    }
    out
}
