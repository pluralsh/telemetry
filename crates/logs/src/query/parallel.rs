//! The pool log pipelines and flush encoding run on, sized to the CPUs this
//! process may use, so parsing overlaps reading and one database's rows
//! spread across cores.

use std::num::NonZero;
use std::sync::LazyLock;

use crate::Error;

/// Rows per pipeline job: enough that dispatch is noise next to parsing,
/// few enough that a database's rows spread across the pool.
pub(super) const BATCH_ROWS: usize = 2048;

struct Pool {
    threads: usize,
    pool: rayon::ThreadPool,
}

/// `None` with a single usable CPU, where batching only adds overhead.
/// `available_parallelism` honours CPU affinity and cgroup quotas.
static POOL: LazyLock<Option<Pool>> = LazyLock::new(|| {
    let threads = std::thread::available_parallelism().map_or(1, NonZero::get);
    if threads < 2 {
        return None;
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("logs-pipeline-{index}"))
        // A panicking job drops its sender, which fails its query instead of
        // aborting the process.
        .panic_handler(|_| {})
        .build()
        .ok()
        .map(|pool| Pool { threads, pool })
});

pub(super) fn enabled() -> bool {
    POOL.is_some()
}

/// The pipeline pool and its thread count.
pub(crate) fn pool() -> Option<(&'static rayon::ThreadPool, usize)> {
    POOL.as_ref().map(|pool| (&pool.pool, pool.threads))
}

pub(super) fn panicked() -> Error {
    Error::Query("log pipeline worker panicked".into())
}

/// `f` applied to each of `items`, in order, on the pool when there is one,
/// off the async runtime either way.
pub(crate) async fn map_blocking<T, U, F>(items: Vec<T>, f: F) -> crate::Result<Vec<U>>
where
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> crate::Result<U> + Send + Sync + 'static,
{
    use rayon::prelude::*;
    tokio::task::spawn_blocking(move || match pool() {
        Some((pool, _)) if items.len() > 1 => {
            pool.install(|| items.into_par_iter().map(&f).collect())
        }
        _ => items.into_iter().map(f).collect(),
    })
    .await
    .map_err(|error| Error::Invalid(format!("flush encoding task failed: {error}")))?
}
