//! Background loops shared by the product servers.

use std::{future::Future, time::Duration};

use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Runs `tick` every `period`, starting one period from now, until
/// `cancellation` fires. A run that overruns its period delays the next one
/// instead of triggering a burst of catch-up runs. A run already in progress
/// finishes before the loop exits; `tick` races `cancellation` itself if it
/// should be abandoned instead.
pub fn spawn_periodic<F, Fut>(
    period: Duration,
    cancellation: CancellationToken,
    mut tick: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return,
                _ = interval.tick() => tick().await,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn ticks_after_each_period_until_cancelled() {
        let runs = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationToken::new();
        let task = spawn_periodic(Duration::from_secs(10), cancellation.clone(), {
            let runs = Arc::clone(&runs);
            move || {
                runs.fetch_add(1, Ordering::Relaxed);
                async {}
            }
        });
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(runs.load(Ordering::Relaxed), 0);
        tokio::time::sleep(Duration::from_secs(21)).await;
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        cancellation.cancel();
        task.await.unwrap();
    }
}
