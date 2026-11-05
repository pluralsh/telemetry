//! A pool for CPU-bound write work, sized to the CPUs this process may use,
//! so one writer's flush encoding spreads across cores instead of holding
//! one async worker.

use std::num::NonZero;
use std::sync::LazyLock;

use rayon::prelude::*;

/// `None` with a single usable CPU, where fanning out only adds overhead.
/// `available_parallelism` honours CPU affinity and cgroup quotas.
static POOL: LazyLock<Option<rayon::ThreadPool>> = LazyLock::new(|| {
    let threads = std::thread::available_parallelism().map_or(1, NonZero::get);
    if threads < 2 {
        return None;
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("write-encode-{index}"))
        .build()
        .ok()
});

/// `f` applied to each of `items`, in order: across the pool when there is
/// one and more than one item, and off the async runtime either way. A
/// panicking `f` fails the join.
pub async fn map_blocking<T, U, F>(items: Vec<T>, f: F) -> Result<Vec<U>, tokio::task::JoinError>
where
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    spawn_map(items, f).await
}

/// [`map_blocking`], started now, so the caller can work alongside it.
pub fn spawn_map<T, U, F>(items: Vec<T>, f: F) -> tokio::task::JoinHandle<Vec<U>>
where
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    tokio::task::spawn_blocking(move || match POOL.as_ref() {
        Some(pool) if items.len() > 1 => pool.install(|| items.into_par_iter().map(&f).collect()),
        _ => items.into_iter().map(f).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_in_order() {
        let mapped = map_blocking((0..1_000).collect(), |item: u32| item * 2)
            .await
            .unwrap();
        assert_eq!(mapped, (0..1_000).map(|item| item * 2).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_panicking_job_fails_the_join() {
        let result = map_blocking(vec![1, 2, 3], |item: u32| {
            assert_ne!(item, 2);
            item
        })
        .await;
        assert!(result.is_err());
    }
}
