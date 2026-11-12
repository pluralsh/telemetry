//! Shares one evaluation among identical concurrent requests.
//!
//! Dashboards fire the same queries from several panels and viewers at once;
//! each would otherwise scan the same data and hold its own share of the
//! query memory budget. The first caller for a key runs it and every caller
//! that arrives while it runs gets a clone of its result. Nothing is kept
//! once it finishes.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Mutex, PoisonError};

use tokio::sync::watch;

pub struct SingleFlight<K, V> {
    in_flight: Mutex<HashMap<K, watch::Receiver<Option<V>>>>,
    name: &'static str,
}

/// Removes the leader's entry however its evaluation ends, so a cancelled
/// leader hands the key to one of its waiters.
struct Lead<'a, K: Eq + Hash, V> {
    flights: &'a SingleFlight<K, V>,
    key: Option<K>,
}

impl<K: Eq + Hash, V> Drop for Lead<'_, K, V> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.flights
                .in_flight
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone> SingleFlight<K, V> {
    /// `name` labels `telemetry_single_flight_total`.
    pub fn new(name: &'static str) -> Self {
        Self {
            in_flight: Mutex::default(),
            name,
        }
    }

    /// Runs `evaluate` for `key`, or waits for the run already in flight.
    pub async fn run<F, Fut>(&self, key: K, evaluate: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V>,
    {
        loop {
            let joined = {
                let mut in_flight = self
                    .in_flight
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                match in_flight.get(&key) {
                    Some(receiver) => Err(receiver.clone()),
                    None => {
                        let (sender, receiver) = watch::channel(None);
                        in_flight.insert(key.clone(), receiver);
                        Ok(sender)
                    }
                }
            };
            match joined {
                Ok(sender) => {
                    metrics::counter!("telemetry_single_flight_total", "db" => self.name, "outcome" => "evaluated")
                        .increment(1);
                    let lead = Lead {
                        flights: self,
                        key: Some(key),
                    };
                    let value = evaluate().await;
                    sender.send_replace(Some(value.clone()));
                    drop(lead);
                    return value;
                }
                Err(mut receiver) => {
                    if let Ok(value) = receiver.wait_for(Option::is_some).await {
                        metrics::counter!("telemetry_single_flight_total", "db" => self.name, "outcome" => "shared")
                            .increment(1);
                        return value.clone().expect("waited for a value");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    #[tokio::test]
    async fn concurrent_callers_share_one_evaluation() {
        // given
        let flights = SingleFlight::<&str, usize>::new("test");
        let runs = AtomicUsize::new(0);
        let release = Notify::new();
        let evaluate = || async {
            runs.fetch_add(1, Ordering::SeqCst);
            release.notified().await;
            7
        };

        // when: a second caller arrives while the first runs
        let both = futures::future::join(flights.run("q", evaluate), async {
            tokio::task::yield_now().await;
            flights.run("q", evaluate).await
        });
        let release = async {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            release.notify_waiters();
        };
        let ((first, second), ()) = futures::future::join(both, release).await;

        // then
        assert_eq!((first, second), (7, 7));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn finished_results_are_not_kept() {
        let flights = SingleFlight::<&str, usize>::new("test");
        assert_eq!(flights.run("q", || async { 1 }).await, 1);
        assert_eq!(flights.run("q", || async { 2 }).await, 2);
    }

    #[tokio::test]
    async fn a_waiter_takes_over_from_a_cancelled_leader() {
        // given: a leader that never finishes and a waiter behind it
        let flights = Arc::new(SingleFlight::<&str, usize>::new("test"));
        let leader = tokio::spawn({
            let flights = Arc::clone(&flights);
            async move { flights.run("q", std::future::pending).await }
        });
        while flights.in_flight.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let waiter = tokio::spawn({
            let flights = Arc::clone(&flights);
            async move { flights.run("q", || async { 3 }).await }
        });
        tokio::task::yield_now().await;

        // when
        leader.abort();

        // then
        assert_eq!(waiter.await.unwrap(), 3);
    }
}
