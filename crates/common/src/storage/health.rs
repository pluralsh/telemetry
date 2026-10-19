//! Whether every SlateDB handle a process depends on is still open.
//!
//! SlateDB closes a handle for good when a background task (compactor,
//! WAL or memtable flusher, manifest poller) panics or fails, or when a
//! newer writer fences it. Every later operation then returns
//! `Error::Closed`, so a process holding such a handle can no longer serve
//! and should be restarted. [`StorageHealth`] follows each registered
//! handle's status watch and keeps a count of the ones that closed with an
//! error, so a liveness probe is an atomic load with no I/O.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

pub use slatedb::CloseReason;
use slatedb::DbStatus;
use tokio::sync::watch;

/// Gauge set to 1 while a SlateDB handle is closed with an error, and back
/// to 0 once it is dropped. Labels: `db`, `reason` ∈ {`panic`, `fenced`,
/// `unknown`}.
pub const STORAGE_CLOSED: &str = "storage_closed";

/// A SlateDB handle that closed with an error and is still held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosedStorage {
    /// The database's path, or `<kind>-<n>` when it is not known.
    pub db: String,
    pub reason: &'static str,
}

/// Tracks the SlateDB handles of a process; see the module docs. Clones
/// share state.
#[derive(Clone, Default)]
pub struct StorageHealth {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    closed_count: AtomicUsize,
    next_id: AtomicU64,
    closed: Mutex<BTreeMap<u64, ClosedStorage>>,
}

/// Holds a [`ClosedStorage`] entry; dropping it clears the entry.
#[must_use = "dropping the guard clears the closed entry"]
pub struct ClosedGuard {
    health: StorageHealth,
    id: u64,
    entry: ClosedStorage,
}

impl Drop for ClosedGuard {
    fn drop(&mut self) {
        let removed = self
            .health
            .inner
            .closed
            .lock()
            .expect("storage health lock")
            .remove(&self.id);
        if removed.is_some() {
            self.health
                .inner
                .closed_count
                .fetch_sub(1, Ordering::AcqRel);
            closed_gauge(&self.entry).set(0.0);
        }
    }
}

impl StorageHealth {
    /// A registry tracking no handles; tests use one to stay isolated from
    /// [`Self::global`].
    pub fn new() -> Self {
        Self::default()
    }

    /// The registry every storage handle opened by this process registers
    /// with, and the one the servers' health probes read.
    pub fn global() -> &'static StorageHealth {
        static GLOBAL: OnceLock<StorageHealth> = OnceLock::new();
        GLOBAL.get_or_init(StorageHealth::new)
    }

    /// Whether no registered handle is closed with an error.
    pub fn is_healthy(&self) -> bool {
        self.inner.closed_count.load(Ordering::Acquire) == 0
    }

    /// The handles currently closed with an error.
    pub fn closed(&self) -> Vec<ClosedStorage> {
        self.inner
            .closed
            .lock()
            .expect("storage health lock")
            .values()
            .cloned()
            .collect()
    }

    /// Follows a handle's `subscribe()` watch until the handle is dropped.
    /// Once it closes with an error it counts as closed until then; a clean
    /// close does not. Outside a Tokio runtime the handle goes untracked.
    pub fn watch(&self, db: impl Into<String>, mut status: watch::Receiver<DbStatus>) {
        let health = self.clone();
        let db = db.into();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(%db, "no Tokio runtime; storage health does not track this handle");
            return;
        };
        runtime.spawn(async move {
            let Ok(reason) = status
                .wait_for(|status| status.close_reason.is_some())
                .await
                .map(|status| status.close_reason)
            else {
                return;
            };
            let Some(reason) = reason.filter(|reason| *reason != CloseReason::Clean) else {
                return;
            };
            let _closed = health.record_closed(db, reason);
            // The handle stays unusable while it is held; a dropped one no
            // longer counts against the process.
            while status.changed().await.is_ok() {}
        });
    }

    /// Records `db` as closed with `reason` until the guard is dropped,
    /// logging the transition and setting [`STORAGE_CLOSED`].
    pub fn record_closed(&self, db: impl Into<String>, reason: CloseReason) -> ClosedGuard {
        let entry = ClosedStorage {
            db: db.into(),
            reason: reason_label(reason),
        };
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .closed
            .lock()
            .expect("storage health lock")
            .insert(id, entry.clone());
        self.inner.closed_count.fetch_add(1, Ordering::AcqRel);
        closed_gauge(&entry).set(1.0);
        tracing::error!(
            db = %entry.db,
            reason = entry.reason,
            "SlateDB handle closed with an error; it rejects every operation \
             until the process restarts, so liveness and readiness now fail"
        );
        ClosedGuard {
            health: self.clone(),
            id,
            entry,
        }
    }
}

/// A `<kind>-<n>` name, unique in the process, for a handle whose path is
/// not known where it registers.
pub fn unnamed(kind: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{kind}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn reason_label(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::Clean => "clean",
        CloseReason::Fenced => "fenced",
        CloseReason::Panic => "panic",
        _ => "unknown",
    }
}

fn closed_gauge(entry: &ClosedStorage) -> metrics::Gauge {
    metrics::gauge!(STORAGE_CLOSED, "db" => entry.db.clone(), "reason" => entry.reason)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use slatedb::Db;
    use slatedb::object_store::memory::InMemory;

    use super::*;

    async fn wait_until(mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition not reached within 10s");
    }

    #[test]
    fn closed_guard_counts_until_dropped() {
        let health = StorageHealth::new();
        assert!(health.is_healthy());
        let first = health.record_closed("a", CloseReason::Panic);
        let second = health.record_closed("b", CloseReason::Fenced);
        assert!(!health.is_healthy());
        assert_eq!(
            health.closed(),
            vec![
                ClosedStorage {
                    db: "a".into(),
                    reason: "panic"
                },
                ClosedStorage {
                    db: "b".into(),
                    reason: "fenced"
                },
            ]
        );
        drop(first);
        assert!(!health.is_healthy());
        drop(second);
        assert!(health.is_healthy());
        assert!(health.closed().is_empty());
    }

    #[tokio::test]
    async fn fenced_writer_fails_health_until_dropped() {
        let store = Arc::new(InMemory::new());
        let health = StorageHealth::new();
        let writer = Db::builder("/fenced", store.clone()).build().await.unwrap();
        health.watch("/fenced", writer.subscribe());
        writer.put(b"key", b"before").await.unwrap();
        assert!(health.is_healthy());

        // A second writer on the same path takes over and fences the first.
        let successor = Db::builder("/fenced", store).build().await.unwrap();
        let error = writer.put(b"key", b"after").await.unwrap_err();
        assert!(
            matches!(error.kind(), slatedb::ErrorKind::Closed(_)),
            "{error:?}"
        );
        wait_until(|| !health.is_healthy()).await;
        assert_eq!(
            health.closed(),
            vec![ClosedStorage {
                db: "/fenced".into(),
                reason: "fenced"
            }]
        );

        drop(writer);
        wait_until(|| health.is_healthy()).await;
        successor.close().await.unwrap();
    }

    #[tokio::test]
    async fn clean_close_stays_healthy() {
        let health = StorageHealth::new();
        let db = Db::builder("/clean", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let mut status = db.subscribe();
        health.watch("/clean", db.subscribe());
        db.close().await.unwrap();
        status
            .wait_for(|status| status.close_reason.is_some())
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(health.is_healthy());
    }
}
