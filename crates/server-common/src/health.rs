//! `/-/healthy` and `/-/ready` responses, and a throttle for errors that
//! repeat every tick while storage stays broken.

use std::time::Duration;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use common::storage::health::StorageHealth;
use tokio::time::Instant;

/// Liveness: fails once a SlateDB handle the process holds has closed with
/// an error, which only a restart recovers from.
pub fn liveness(storage: &StorageHealth) -> Response {
    if storage.is_healthy() {
        return StatusCode::OK.into_response();
    }
    let closed = storage
        .closed()
        .into_iter()
        .map(|closed| format!("{} ({})", closed.db, closed.reason))
        .collect::<Vec<_>>()
        .join(", ");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("storage closed: {closed}\n"),
    )
        .into_response()
}

/// Readiness: the server's own readiness, and never while liveness fails.
pub fn readiness(storage: &StorageHealth, ready: bool) -> StatusCode {
    if ready && storage.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// How often [`ErrorThrottle::flush`] lets a repeating error through.
pub const REPEATED_ERROR_INTERVAL: Duration = Duration::from_secs(60);

/// Lets one occurrence of a repeating error through per interval and
/// counts the ones it holds back.
#[derive(Debug)]
pub struct ErrorThrottle {
    every: Duration,
    last: Option<Instant>,
    suppressed: u64,
}

impl ErrorThrottle {
    pub fn new(every: Duration) -> Self {
        Self {
            every,
            last: None,
            suppressed: 0,
        }
    }

    /// A throttle for the periodic durable flush.
    pub fn flush() -> Self {
        Self::new(REPEATED_ERROR_INTERVAL)
    }

    /// `Some(suppressed)` when this occurrence should be logged, with how
    /// many were held back since the last one logged; `None` to skip it.
    pub fn admit(&mut self) -> Option<u64> {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < self.every)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }

    /// Forgets past occurrences, so the next one is logged at once.
    pub fn reset(&mut self) {
        self.last = None;
        self.suppressed = 0;
    }
}

#[cfg(test)]
mod tests {
    use common::storage::health::CloseReason;

    use super::*;

    #[test]
    fn probes_fail_while_storage_is_closed() {
        let storage = StorageHealth::new();
        assert_eq!(liveness(&storage).status(), StatusCode::OK);
        assert_eq!(readiness(&storage, true), StatusCode::OK);
        assert_eq!(readiness(&storage, false), StatusCode::SERVICE_UNAVAILABLE);

        let closed = storage.record_closed("logs/0", CloseReason::Panic);
        assert_eq!(liveness(&storage).status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(readiness(&storage, true), StatusCode::SERVICE_UNAVAILABLE);

        drop(closed);
        assert_eq!(liveness(&storage).status(), StatusCode::OK);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_logs_once_per_interval_with_a_count() {
        let mut throttle = ErrorThrottle::new(Duration::from_secs(60));
        assert_eq!(throttle.admit(), Some(0));
        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(10)).await;
            assert_eq!(throttle.admit(), None);
        }
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(throttle.admit(), Some(5));
        assert_eq!(throttle.admit(), None);
        throttle.reset();
        assert_eq!(throttle.admit(), Some(0));
    }
}
