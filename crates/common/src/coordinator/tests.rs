use super::*;
use crate::BytesRange;
use crate::coordinator::Durability;
use crate::storage::in_memory::{InMemoryStorage, InMemoryStorageSnapshot};
use crate::storage::{PutRecordOp, Record, StorageSnapshot};
use crate::{Storage, StorageRead};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Mutex;
// ============================================================================
// Test Infrastructure
// ============================================================================

#[derive(Clone, Debug)]
struct TestWrite {
    key: String,
    value: u64,
    size: usize,
}

/// Context carries state that must persist across deltas (like sequence allocation)
#[derive(Clone, Debug, Default)]
struct TestContext {
    next_seq: u64,
    error: Option<String>,
}

/// A shared reader that sees writes as they are applied to the delta.
#[derive(Clone, Debug, Default)]
struct TestDeltaReader {
    data: Arc<Mutex<HashMap<String, u64>>>,
}

impl TestDeltaReader {
    fn get(&self, key: &str) -> Option<u64> {
        self.data.lock().unwrap().get(key).copied()
    }
}

/// Delta accumulates writes with sequence numbers.
/// Stores the context directly and updates it in place.
#[derive(Debug)]
struct TestDelta {
    context: TestContext,
    writes: HashMap<String, (u64, u64)>,
    key_values: Arc<Mutex<HashMap<String, u64>>>,
    total_size: usize,
}

#[derive(Clone, Debug)]
struct FrozenTestDelta {
    writes: HashMap<String, (u64, u64)>,
}

impl std::fmt::Debug for View<TestDelta> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("View<TestDelta>")
    }
}

impl Delta for TestDelta {
    type Context = TestContext;
    type Write = TestWrite;
    type DeltaView = TestDeltaReader;
    type Frozen = FrozenTestDelta;
    type FrozenView = Arc<HashMap<String, u64>>;
    type ApplyResult = ();
    type Snapshot = Arc<dyn StorageSnapshot>;

    fn init(context: Self::Context) -> Self {
        Self {
            context,
            writes: HashMap::default(),
            key_values: Arc::new(Mutex::new(HashMap::default())),
            total_size: 0,
        }
    }

    fn apply(&mut self, write: Self::Write) -> Result<(), String> {
        if let Some(error) = &self.context.error {
            return Err(error.clone());
        }

        let seq = self.context.next_seq;
        self.context.next_seq += 1;

        self.writes.insert(write.key.clone(), (seq, write.value));
        self.total_size += write.size;
        self.key_values
            .lock()
            .unwrap()
            .insert(write.key, write.value);
        Ok(())
    }

    fn estimate_size(&self) -> usize {
        self.total_size
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        let frozen = FrozenTestDelta {
            writes: self.writes,
        };
        let frozen_view = Arc::new(self.key_values.lock().unwrap().clone());
        (frozen, frozen_view, self.context)
    }

    fn reader(&self) -> Self::DeltaView {
        TestDeltaReader {
            data: self.key_values.clone(),
        }
    }
}

/// Shared state for TestFlusher - allows test to inspect and control behavior
#[derive(Default)]
struct TestFlusherState {
    flushed_events: Vec<Arc<EpochStamped<FrozenTestDelta>>>,
    delta_error: Option<String>,
    storage_error: Option<String>,
    /// Signals when a flush starts (before blocking)
    flush_started_tx: Option<oneshot::Sender<()>>,
    /// Blocks flush until signaled
    unblock_rx: Option<mpsc::Receiver<()>>,
}

#[derive(Clone)]
struct TestFlusher {
    state: Arc<Mutex<TestFlusherState>>,
    storage: Arc<InMemoryStorage>,
}

impl Default for TestFlusher {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(TestFlusherState::default())),
            storage: Arc::new(InMemoryStorage::new()),
        }
    }
}

impl TestFlusher {
    /// Create a flusher that blocks until signaled, with a notification when flush starts.
    /// Returns (flusher, flush_started_rx, unblock_tx).
    fn with_flush_control() -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel(1);
        let flusher = Self {
            state: Arc::new(Mutex::new(TestFlusherState {
                flushed_events: Vec::new(),
                delta_error: None,
                storage_error: None,
                flush_started_tx: Some(started_tx),
                unblock_rx: Some(unblock_rx),
            })),
            storage: Arc::new(InMemoryStorage::new()),
        };
        (flusher, started_rx, unblock_tx)
    }

    fn failing_delta(error: &str) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestFlusherState {
                delta_error: Some(error.to_string()),
                ..Default::default()
            })),
            storage: Arc::new(InMemoryStorage::new()),
        }
    }

    fn failing_storage_control(error: &str) -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel(1);
        let flusher = Self {
            state: Arc::new(Mutex::new(TestFlusherState {
                storage_error: Some(error.to_string()),
                flush_started_tx: Some(started_tx),
                unblock_rx: Some(unblock_rx),
                ..Default::default()
            })),
            storage: Arc::new(InMemoryStorage::new()),
        };
        (flusher, started_rx, unblock_tx)
    }

    fn flushed_events(&self) -> Vec<Arc<EpochStamped<FrozenTestDelta>>> {
        self.state.lock().unwrap().flushed_events.clone()
    }

    async fn initial_snapshot(&self) -> Arc<dyn StorageSnapshot> {
        self.storage.snapshot().await.unwrap()
    }
}

#[async_trait]
impl Flusher<TestDelta> for TestFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenTestDelta,
        epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        // Signal that flush has started
        let flush_started_tx = {
            let mut state = self.state.lock().unwrap();
            if state.storage_error.is_none() {
                state.flush_started_tx.take()
            } else {
                None
            }
        };
        if let Some(tx) = flush_started_tx {
            let _ = tx.send(());
        }

        // Block if test wants to control timing
        let unblock_rx = {
            let mut state = self.state.lock().unwrap();
            if state.storage_error.is_none() {
                state.unblock_rx.take()
            } else {
                None
            }
        };
        if let Some(mut rx) = unblock_rx {
            rx.recv().await;
        }
        if let Some(error) = self.state.lock().unwrap().delta_error.clone() {
            return Err(error);
        }

        // Write records to storage
        let records: Vec<PutRecordOp> = frozen
            .writes
            .iter()
            .map(|(key, (seq, value))| {
                let mut buf = Vec::with_capacity(16);
                buf.extend_from_slice(&seq.to_le_bytes());
                buf.extend_from_slice(&value.to_le_bytes());
                Record::new(Bytes::from(key.clone()), Bytes::from(buf)).into()
            })
            .collect();
        self.storage
            .put(records)
            .await
            .map_err(|e| format!("{}", e))?;

        // Record the flush
        {
            let mut state = self.state.lock().unwrap();
            state
                .flushed_events
                .push(Arc::new(EpochStamped::new(frozen, epoch_range.clone())));
        }

        self.storage.snapshot().await.map_err(|e| format!("{}", e))
    }

    async fn flush_storage(&self) -> Result<(), String> {
        // Signal that flush has started
        let flush_started_tx = {
            let mut state = self.state.lock().unwrap();
            state.flush_started_tx.take()
        };
        if let Some(tx) = flush_started_tx {
            let _ = tx.send(());
        }

        // Block if test wants to control timing
        let unblock_rx = {
            let mut state = self.state.lock().unwrap();
            state.unblock_rx.take()
        };
        if let Some(mut rx) = unblock_rx {
            rx.recv().await;
        }
        if let Some(error) = self.state.lock().unwrap().storage_error.clone() {
            return Err(error);
        }

        Ok(())
    }
}

fn test_config() -> WriteCoordinatorConfig {
    WriteCoordinatorConfig {
        queue_capacity: 100,
        flush_interval: Duration::from_secs(3600), // Long interval to avoid timer flushes
        flush_size_threshold: usize::MAX,
    }
}

async fn assert_snapshot_has_rows(
    snapshot: &Arc<dyn StorageSnapshot>,
    expected: &[(&str, u64, u64)],
) {
    let records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
    assert_eq!(
        records.len(),
        expected.len(),
        "expected {} rows but snapshot has {}",
        expected.len(),
        records.len()
    );
    let mut actual: Vec<(String, u64, u64)> = records
        .iter()
        .map(|r| {
            let key = String::from_utf8(r.key.to_vec()).unwrap();
            let seq = u64::from_le_bytes(r.value[0..8].try_into().unwrap());
            let value = u64::from_le_bytes(r.value[8..16].try_into().unwrap());
            (key, seq, value)
        })
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected: Vec<(&str, u64, u64)> = expected.to_vec();
    expected.sort_by(|a, b| a.0.cmp(b.0));
    for (actual, expected) in actual.iter().zip(expected.iter()) {
        assert_eq!(
            actual.0, expected.0,
            "key mismatch: got {:?}, expected {:?}",
            actual.0, expected.0
        );
        assert_eq!(
            actual.1, expected.1,
            "seq mismatch for key {:?}: got {}, expected {}",
            actual.0, actual.1, expected.1
        );
        assert_eq!(
            actual.2, expected.2,
            "value mismatch for key {:?}: got {}, expected {}",
            actual.0, actual.2, expected.2
        );
    }
}

// ============================================================================
// Basic Write Flow Tests
// ============================================================================

#[tokio::test]
async fn should_assign_monotonic_epochs() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let write3 = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();

    let epoch1 = write1.epoch().await.unwrap();
    let epoch2 = write2.epoch().await.unwrap();
    let epoch3 = write3.epoch().await.unwrap();

    // then
    assert!(epoch1 < epoch2);
    assert!(epoch2 < epoch3);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_apply_writes_in_order() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let mut last_write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();

    handle.flush(false).await.unwrap();
    // Wait for flush to complete via watermark
    last_write.wait(Durability::Written).await.unwrap();

    // then
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 1);
    let frozen_delta = &events[0];
    let delta = &frozen_delta.val;
    // Writing key "a" 3x overwrites; last write wins with seq=2 (0-indexed)
    let (seq, value) = delta.writes.get("a").unwrap();
    assert_eq!(*value, 3);
    assert_eq!(*seq, 2);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test]
async fn should_update_applied_watermark_after_each_write() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut write_handle = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();

    // then - wait should succeed immediately after write is applied
    let result = write_handle.wait(Durability::Applied).await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

#[tokio::test]
async fn should_propagate_apply_error_to_handle() {
    // given
    let flusher = TestFlusher::default();
    let context = TestContext {
        error: Some("apply error".to_string()),
        ..Default::default()
    };
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        context,
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();

    let result = write.epoch().await;

    // then
    assert!(
        matches!(result, Err(WriteError::ApplyError(epoch, msg)) if epoch == 1 && msg == "apply error")
    );

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Manual Flush Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_flush_on_command() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write.wait(Durability::Written).await.unwrap();

    // then
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_wait_on_flush_handle() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let mut flush_handle = handle.flush(false).await.unwrap();

    // then - can wait directly on the flush handle
    flush_handle.wait(Durability::Written).await.unwrap();
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_return_correct_epoch_from_flush_handle() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let flush_handle = handle.flush(false).await.unwrap();

    // then - flush handle epoch should be the last write's epoch
    let flush_epoch = flush_handle.epoch().await.unwrap();
    let write2_epoch = write2.epoch().await.unwrap();
    assert_eq!(flush_epoch, write2_epoch);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_include_all_pending_writes_in_flush() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let mut last_write = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();

    handle.flush(false).await.unwrap();
    last_write.wait(Durability::Written).await.unwrap();

    // then
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 1);
    let frozen_delta = &events[0];
    assert_eq!(frozen_delta.val.writes.len(), 3);
    let snapshot = flusher.storage.snapshot().await.unwrap();
    assert_snapshot_has_rows(&snapshot, &[("a", 0, 1), ("b", 1, 2), ("c", 2, 3)]).await;

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_skip_flush_when_no_new_writes() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write.wait(Durability::Written).await.unwrap();

    // Second flush with no new writes
    handle.flush(false).await.unwrap();

    // Synchronization: write and wait for applied to ensure the flush command
    // has been processed (commands are processed in order)
    let sync_write = handle
        .try_write(TestWrite {
            key: "sync".into(),
            value: 0,
            size: 1,
        })
        .await
        .unwrap();
    sync_write.epoch().await.unwrap();

    // then - only one flush should have occurred (the second flush was a no-op)
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_update_written_watermark_after_flush() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut write_handle = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();

    handle.flush(false).await.unwrap();

    // then - wait for Written should succeed after flush completes
    let result = write_handle.wait(Durability::Written).await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Timer-Based Flush Tests
// ============================================================================

#[tokio::test(start_paused = true)]
async fn should_flush_on_flush_interval() {
    // given - create coordinator with short flush interval
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 100,
        flush_interval: Duration::from_millis(100),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - ensure coordinator task runs and then write something
    tokio::task::yield_now().await;
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    write.wait(Durability::Applied).await.unwrap();

    // then - no flush should have happened yet (interval was reset in run())
    assert_eq!(flusher.flushed_events().len(), 0);

    // when - advance time past the flush interval from when run() was called
    tokio::time::advance(Duration::from_millis(150)).await;
    tokio::task::yield_now().await;

    // then - flush should have happened
    assert_eq!(flusher.flushed_events().len(), 1);
    let snapshot = flusher.storage.snapshot().await.unwrap();
    assert_snapshot_has_rows(&snapshot, &[("a", 0, 1)]).await;

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Size-Threshold Flush Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_flush_when_size_threshold_exceeded() {
    // given
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 100,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: 100, // Low threshold for testing
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - write that exceeds threshold
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 150,
        })
        .await
        .unwrap();
    write.wait(Durability::Written).await.unwrap();

    // then
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_accumulate_until_threshold() {
    // given
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 100,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: 100,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - small writes that accumulate
    for i in 0..5 {
        let mut w = handle
            .try_write(TestWrite {
                key: format!("key{}", i),
                value: i,
                size: 15,
            })
            .await
            .unwrap();
        w.wait(Durability::Applied).await.unwrap();
    }

    // then - no flush yet (75 bytes < 100 threshold)
    assert_eq!(flusher.flushed_events().len(), 0);

    // when - write that pushes over threshold
    let mut final_write = handle
        .try_write(TestWrite {
            key: "final".into(),
            value: 999,
            size: 30,
        })
        .await
        .unwrap();
    final_write.wait(Durability::Written).await.unwrap();

    // then - should have flushed (105 bytes > 100 threshold)
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Non-Blocking Flush (Concurrency) Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_accept_writes_during_flush() {
    // given
    let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when: trigger a flush and wait for it to start (proving it's in progress)
    let write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    flush_started_rx.await.unwrap(); // wait until flush is actually in progress

    // then: writes during blocked flush still succeed
    let write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    assert!(write2.epoch().await.unwrap() > write1.epoch().await.unwrap());

    // cleanup
    unblock_tx.send(()).await.unwrap();
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_assign_new_epochs_during_flush() {
    // given
    let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when: write, flush, then write more during blocked flush
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    flush_started_rx.await.unwrap(); // wait until flush is actually in progress

    // Writes during blocked flush get new epochs
    let w1 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let w2 = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();

    // then: epochs continue incrementing
    let e1 = w1.epoch().await.unwrap();
    let e2 = w2.epoch().await.unwrap();
    assert!(e1 < e2);

    // cleanup
    unblock_tx.send(()).await.unwrap();
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_keep_applying_writes_when_flush_queue_is_full() {
    // given: the first flush blocks inside the flusher
    let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();
    let write = |key: String| TestWrite {
        key,
        value: 1,
        size: 1,
    };
    handle.try_write(write("blocked".into())).await.unwrap();
    handle.flush(false).await.unwrap();
    flush_started_rx.await.unwrap();

    // when: more flushes are requested than the queue holds
    for i in 0..FLUSH_QUEUE_CAPACITY + 2 {
        handle.try_write(write(format!("queued{i}"))).await.unwrap();
        handle.flush(false).await.unwrap();
    }

    // then: the write loop is not stalled on the flush task
    let mut last = handle.try_write(write("last".into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), last.wait(Durability::Applied))
        .await
        .expect("write loop stalled behind a full flush queue")
        .unwrap();

    // and: once flushing resumes, every write is flushed exactly once
    unblock_tx.send(()).await.unwrap();
    handle.flush(false).await.unwrap();
    last.wait(Durability::Written).await.unwrap();
    let events = flusher.flushed_events();
    assert!(events.len() <= FLUSH_QUEUE_CAPACITY + 3);
    for pair in events.windows(2) {
        assert_eq!(pair[0].epoch_range.end, pair[1].epoch_range.start);
    }
    let keys: HashSet<_> = events
        .iter()
        .flat_map(|event| event.val.writes.keys().cloned())
        .collect();
    assert_eq!(keys.len(), FLUSH_QUEUE_CAPACITY + 4);

    coordinator.stop().await;
}

// ============================================================================
// Backpressure Tests
// ============================================================================

#[tokio::test]
async fn should_return_backpressure_when_queue_full() {
    // given
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 2,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    // Don't start coordinator - queue will fill

    // when - fill the queue
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;
    let _ = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await;

    // Third write should fail with backpressure
    let result = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await;

    // then
    assert!(matches!(result, Err(WriteError::Backpressure(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_accept_writes_after_queue_drains() {
    // given
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 2,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");

    // Fill queue without processing
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;
    let mut write_b = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();

    // when - start coordinator to drain queue and wait for it to process writes
    coordinator.start();
    write_b.wait(Durability::Applied).await.unwrap();

    // then - writes should succeed now
    let result = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Terminal Flush Failure Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_propagate_flush_delta_failure() {
    let flusher = TestFlusher::failing_delta("flush delta failed");
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["active", "queued"],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let active = coordinator.handle("active");
    let queued = coordinator.handle("queued");
    coordinator.pause_handle("queued").pause();
    let mut queued_write = queued
        .try_write(TestWrite {
            key: "queued".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();
    coordinator.start();

    let mut written = active
        .try_write(TestWrite {
            key: "written".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let mut durable = active
        .try_write(TestWrite {
            key: "durable".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    written.wait(Durability::Applied).await.unwrap();
    durable.wait(Durability::Applied).await.unwrap();
    active.flush(false).await.unwrap();

    let written_error =
        tokio::time::timeout(Duration::from_secs(5), written.wait(Durability::Written))
            .await
            .expect("Written waiter hung")
            .unwrap_err();
    assert!(matches!(written_error, WriteError::FlushError(msg) if msg == "flush delta failed"));
    assert!(
        matches!(durable.wait(Durability::Durable).await, Err(WriteError::FlushError(msg)) if msg == "flush delta failed")
    );
    assert!(
        matches!(queued_write.wait(Durability::Applied).await, Err(WriteError::FlushError(msg)) if msg == "flush delta failed")
    );
    assert!(matches!(
        active.try_write(TestWrite {
            key: "new".into(),
            value: 4,
            size: 10,
        }).await,
        Err(WriteError::FlushError(msg)) if msg == "flush delta failed"
    ));
    assert!(
        coordinator
            .stop()
            .await
            .unwrap_err()
            .contains("flush delta failed")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_propagate_flush_storage_failure() {
    let (flusher, storage_started, unblock_storage) =
        TestFlusher::failing_storage_control("flush storage failed");
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["active", "queued"],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let active = coordinator.handle("active");
    let queued = coordinator.handle("queued");
    coordinator.pause_handle("queued").pause();
    let mut queued_write = queued
        .try_write(TestWrite {
            key: "queued".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();
    coordinator.start();

    let mut durable = active
        .try_write(TestWrite {
            key: "durable".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    durable.wait(Durability::Applied).await.unwrap();
    active.flush(true).await.unwrap();
    storage_started.await.unwrap();

    let mut written = active
        .try_write(TestWrite {
            key: "written".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    written.wait(Durability::Applied).await.unwrap();
    active.flush(false).await.unwrap().epoch().await.unwrap();
    unblock_storage.send(()).await.unwrap();

    assert!(
        matches!(durable.wait(Durability::Durable).await, Err(WriteError::FlushError(msg)) if msg == "flush storage failed")
    );
    assert!(
        matches!(written.wait(Durability::Written).await, Err(WriteError::FlushError(msg)) if msg == "flush storage failed")
    );
    assert!(
        matches!(queued_write.wait(Durability::Applied).await, Err(WriteError::FlushError(msg)) if msg == "flush storage failed")
    );
    assert!(matches!(
        active.try_write(TestWrite {
            key: "new".into(),
            value: 4,
            size: 10,
        }).await,
        Err(WriteError::FlushError(msg)) if msg == "flush storage failed"
    ));
    assert!(
        coordinator
            .stop()
            .await
            .unwrap_err()
            .contains("flush storage failed")
    );
}

// ============================================================================
// Shutdown Tests
// ============================================================================

#[tokio::test]
async fn should_shutdown_cleanly_when_stop_called() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let result = coordinator.stop().await;

    // then - coordinator should return Ok
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_flush_pending_writes_on_shutdown() {
    // given
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 100,
        flush_interval: Duration::from_secs(3600), // Long interval - won't trigger
        flush_size_threshold: usize::MAX,          // High threshold - won't trigger
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - write without explicit flush, then shutdown
    let write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let epoch = write.epoch().await.unwrap();

    // Drop handle to trigger shutdown
    coordinator.stop().await;

    // then - pending writes should have been flushed
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 1);
    let epoch_range = &events[0].epoch_range;
    assert!(epoch_range.contains(&epoch));
}

#[tokio::test]
async fn should_return_shutdown_error_after_coordinator_stops() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // Stop coordinator
    coordinator.stop().await;

    // when
    let result = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;

    // then
    assert!(matches!(result, Err(WriteError::Shutdown)));
}

// ============================================================================
// Epoch Range Tracking Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_track_epoch_range_in_flush_event() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let mut last_write = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();

    handle.flush(false).await.unwrap();
    last_write.wait(Durability::Written).await.unwrap();

    // then
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 1);
    let epoch_range = &events[0].epoch_range;
    assert_eq!(epoch_range.start, 1);
    assert_eq!(epoch_range.end, 4); // exclusive: one past the last epoch (3)

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_have_contiguous_epoch_ranges() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - first batch
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let mut write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write2.wait(Durability::Written).await.unwrap();

    // when - second batch
    let mut write3 = handle
        .try_write(TestWrite {
            key: "c".into(),
            value: 3,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write3.wait(Durability::Written).await.unwrap();

    // then
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 2);

    let range1 = &events[0].epoch_range;
    let range2 = &events[1].epoch_range;

    // Ranges should be contiguous (end of first == start of second)
    assert_eq!(range1.end, range2.start);
    assert_eq!(range1, &(1..3));
    assert_eq!(range2, &(3..4));

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_include_exact_epochs_in_range() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - write and capture the assigned epochs
    let write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let epoch1 = write1.epoch().await.unwrap();

    let mut write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    let epoch2 = write2.epoch().await.unwrap();

    handle.flush(false).await.unwrap();
    write2.wait(Durability::Written).await.unwrap();

    // then - the epoch_range should contain exactly the epochs assigned to writes
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 1);
    let epoch_range = &events[0].epoch_range;

    // The range should start at the first write's epoch
    assert_eq!(epoch_range.start, epoch1);
    // The range end should be one past the last write's epoch (exclusive)
    assert_eq!(epoch_range.end, epoch2 + 1);
    // Both epochs should be contained in the range
    assert!(epoch_range.contains(&epoch1));
    assert!(epoch_range.contains(&epoch2));

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// State Carryover (ID Allocation) Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_preserve_context_across_flushes() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - write key "a" in first batch (seq 0)
    let mut write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write1.wait(Durability::Written).await.unwrap();

    // Write to key "a" again in second batch (seq 1)
    let mut write2 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();
    write2.wait(Durability::Written).await.unwrap();

    // then
    let events = flusher.flushed_events();
    assert_eq!(events.len(), 2);

    // Batch 1: "a" with seq 0
    let (seq1, _) = events[0].val.writes.get("a").unwrap();
    assert_eq!(*seq1, 0);

    // Batch 2: "a" with seq 1 (sequence continues)
    let (seq2, _) = events[1].val.writes.get("a").unwrap();
    assert_eq!(*seq2, 1);

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Subscribe Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_receive_view_on_subscribe() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // then - first broadcast is on freeze (delta added to frozen)
    let result = subscriber.recv().await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_include_snapshot_in_view_after_flush() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // First broadcast: freeze (frozen delta added)
    let _ = subscriber.recv().await.unwrap();
    // Second broadcast: flush complete (snapshot updated)
    let result = subscriber.recv().await.unwrap();

    // then - snapshot should contain the flushed data
    assert_snapshot_has_rows(&result.snapshot, &[("a", 0, 1)]).await;

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_include_delta_in_view_after_flush() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 42,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // First broadcast: freeze
    let _ = subscriber.recv().await.unwrap();
    // Second broadcast: flush complete
    let result = subscriber.recv().await.unwrap();

    // then - last_written_delta should contain the write we made
    let flushed = result.last_written_delta.as_ref().unwrap();
    assert_eq!(flushed.val.get("a"), Some(&42));

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_include_epoch_range_in_view_after_flush() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    let write1 = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let write2 = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // First broadcast: freeze
    let _ = subscriber.recv().await.unwrap();
    // Second broadcast: flush complete
    let result = subscriber.recv().await.unwrap();

    // then - epoch range from last_written_delta should contain the epochs
    let flushed = result.last_written_delta.as_ref().unwrap();
    let epoch1 = write1.epoch().await.unwrap();
    let epoch2 = write2.epoch().await.unwrap();
    assert!(flushed.epoch_range.contains(&epoch1));
    assert!(flushed.epoch_range.contains(&epoch2));
    assert_eq!(flushed.epoch_range.start, epoch1);
    assert_eq!(flushed.epoch_range.end, epoch2 + 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_broadcast_frozen_delta_on_freeze() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // then - first broadcast should have the frozen delta in the frozen vec
    let state = subscriber.recv().await.unwrap();
    assert_eq!(state.frozen.len(), 1);
    assert!(state.frozen[0].val.contains_key("a"));

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_remove_frozen_delta_after_flush_complete() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when
    handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    handle.flush(false).await.unwrap();

    // First broadcast: freeze (frozen has 1 entry)
    let state1 = subscriber.recv().await.unwrap();
    assert_eq!(state1.frozen.len(), 1);

    // Second broadcast: flush complete (frozen is empty, last_written_delta set)
    let state2 = subscriber.recv().await.unwrap();
    assert_eq!(state2.frozen.len(), 0);
    assert!(state2.last_written_delta.is_some());

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_recover_from_message_lost_subscriber() {
    // given - a coordinator with a small broadcast buffer (16)
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    let (mut subscriber, _) = coordinator.subscribe();
    subscriber.initialize();
    coordinator.start();

    // when - write and flush enough times to overflow the broadcast buffer
    // (capacity is 16, so ~20 flushes should cause lag)
    // The subscriber never called recv(), so all broadcasts accumulate and overflow the buffer
    for i in 0..20 {
        let _write_handle = handle
            .try_write(TestWrite {
                key: format!("key_{}", i),
                value: i as u64,
                size: 10,
            })
            .await
            .unwrap();
        // Wait so flushes are not coalesced while the flush queue is full.
        let mut flushed = handle.flush(false).await.unwrap();
        flushed.wait(Durability::Written).await.unwrap();
    }

    // make changes durable
    let mut watermark = handle
        .flush(true)
        .await
        .expect("flush(true) should succeed");

    // wait for durability watermark
    watermark.wait(Durability::Durable).await;

    // expect SubscribeError to be MessageLost
    let result = subscriber
        .recv()
        .await
        .expect_err("expected recv() to yield an error");
    assert!(matches!(result, SubscribeError::MessageLost));

    // when - resubscribe to recover
    let (rx, initial_view) = handle.subscribe();
    (subscriber, _) = ViewSubscriber::new(rx, initial_view);
    let view = subscriber.initialize();

    // then - the fresh view should reflect the current state
    // (all 20 writes should be in the snapshot after all the flushes)
    let records = view.snapshot.scan(BytesRange::unbounded()).await.unwrap();
    assert!(
        records.len() >= 20,
        "expected at least 20 rows, got {}",
        records.len()
    );

    // and - we should be able to receive future broadcasts
    let _write_handle = handle
        .try_write(TestWrite {
            key: "post_recovery".into(),
            value: 100,
            size: 10,
        })
        .await
        .unwrap();
    let _ = handle.flush(false).await.unwrap();

    let result = subscriber.recv().await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Durable Flush Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_flush_even_when_no_writes_if_flush_storage() {
    // given
    let flusher = TestFlusher::default();
    let storage = Arc::new(InMemoryStorage::new());
    let snapshot = storage.snapshot().await.unwrap();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        snapshot,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - flush with flush_storage but no pending writes
    let mut flush_handle = handle.flush(true).await.unwrap();
    flush_handle.wait(Durability::Durable).await.unwrap();

    // then - flusher was called (durable event sent) but no delta was recorded
    // (TestFlusher only records events with deltas)
    assert_eq!(flusher.flushed_events().len(), 0);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_advance_durable_watermark() {
    // given
    let flusher = TestFlusher::default();
    let storage = Arc::new(InMemoryStorage::new());
    let snapshot = storage.snapshot().await.unwrap();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        snapshot,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when - write and flush with durable
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await
        .unwrap();
    let mut flush_handle = handle.flush(true).await.unwrap();

    // then - can wait for Durable durability level
    flush_handle.wait(Durability::Durable).await.unwrap();
    write.wait(Durability::Durable).await.unwrap();
    assert_eq!(flusher.flushed_events().len(), 1);

    // cleanup
    coordinator.stop().await;
}

#[tokio::test]
async fn should_see_applied_write_via_view() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher,
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut write = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 42,
            size: 10,
        })
        .await
        .unwrap();
    write.wait(Durability::Applied).await.unwrap();

    // then
    let view = coordinator.view();
    assert_eq!(view.current.get("a"), Some(42));

    // cleanup
    coordinator.stop().await;
}

// ============================================================================
// Multi-Channel Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_flush_writes_from_multiple_channels() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["ch1".to_string(), "ch2".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let ch1 = coordinator.handle("ch1");
    let ch2 = coordinator.handle("ch2");
    coordinator.start();

    // when - write to both channels, waiting for each to be applied
    // to ensure deterministic ordering
    let mut w1 = ch1
        .try_write(TestWrite {
            key: "a".into(),
            value: 10,
            size: 10,
        })
        .await
        .unwrap();
    w1.wait(Durability::Applied).await.unwrap();

    let mut w2 = ch2
        .try_write(TestWrite {
            key: "b".into(),
            value: 20,
            size: 10,
        })
        .await
        .unwrap();
    w2.wait(Durability::Applied).await.unwrap();

    let mut w3 = ch1
        .try_write(TestWrite {
            key: "c".into(),
            value: 30,
            size: 10,
        })
        .await
        .unwrap();
    w3.wait(Durability::Applied).await.unwrap();

    ch1.flush(false).await.unwrap();
    w3.wait(Durability::Written).await.unwrap();

    // then - snapshot should contain writes from both channels
    let snapshot = flusher.storage.snapshot().await.unwrap();
    assert_snapshot_has_rows(&snapshot, &[("a", 0, 10), ("b", 1, 20), ("c", 2, 30)]).await;

    // cleanup
    coordinator.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_succeed_with_write_timeout_when_queue_has_space() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    let mut wh = handle
        .write_timeout(
            TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    // then
    let result = wh.wait(Durability::Applied).await;
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

#[tokio::test]
async fn should_timeout_when_queue_full() {
    // given - queue_capacity=2, coordinator NOT started so nothing drains
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 2,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");

    // fill the queue
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;
    let _ = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await;

    // when - third write should time out
    let result = handle
        .write_timeout(
            TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            },
            Duration::from_millis(10),
        )
        .await;

    // then
    assert!(matches!(result, Err(WriteError::TimeoutError(_))));
}

#[tokio::test]
async fn should_return_write_in_timeout_error() {
    // given - queue_capacity=1, coordinator NOT started
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 1,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");

    // fill the queue
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;

    // when
    let result = handle
        .write_timeout(
            TestWrite {
                key: "retry_me".into(),
                value: 42,
                size: 10,
            },
            Duration::from_millis(10),
        )
        .await;
    let Err(err) = result else {
        panic!("expected TimeoutError");
    };

    // then - original write is returned inside the error
    let write = err.into_inner().expect("should contain the write");
    assert_eq!(write.key, "retry_me");
    assert_eq!(write.value, 42);
}

#[tokio::test]
async fn should_return_write_in_backpressure_error() {
    // given - queue_capacity=1, coordinator NOT started
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 1,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");

    // fill the queue
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;

    // when
    let result = handle
        .try_write(TestWrite {
            key: "retry_me".into(),
            value: 42,
            size: 10,
        })
        .await;
    let Err(err) = result else {
        panic!("expected Backpressure");
    };

    // then - original write is returned inside the error
    let write = err.into_inner().expect("should contain the write");
    assert_eq!(write.key, "retry_me");
    assert_eq!(write.value, 42);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_succeed_when_queue_drains_within_timeout() {
    // given - queue_capacity=2, coordinator started so it drains
    let flusher = TestFlusher::default();
    let config = WriteCoordinatorConfig {
        queue_capacity: 2,
        flush_interval: Duration::from_secs(3600),
        flush_size_threshold: usize::MAX,
    };
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");

    // fill the queue before starting
    let _ = handle
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 10,
        })
        .await;
    let _ = handle
        .try_write(TestWrite {
            key: "b".into(),
            value: 2,
            size: 10,
        })
        .await;

    // when - start coordinator (begins draining) and write with generous timeout
    coordinator.start();
    let result = handle
        .write_timeout(
            TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            },
            Duration::from_secs(5),
        )
        .await;

    // then
    assert!(result.is_ok());

    // cleanup
    coordinator.stop().await;
}

#[tokio::test]
async fn should_return_shutdown_on_write_timeout_after_coordinator_stops() {
    // given
    let flusher = TestFlusher::default();
    let mut coordinator = WriteCoordinator::new(
        test_config(),
        vec!["default".to_string()],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle = coordinator.handle("default");
    coordinator.start();

    // when
    coordinator.stop().await;
    let result = handle
        .write_timeout(
            TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            },
            Duration::from_secs(1),
        )
        .await;

    // then
    assert!(matches!(result, Err(WriteError::Shutdown)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_pause_and_resume_write_channel() {
    // given
    let flusher = TestFlusher::default();
    let mut config = test_config();
    config.flush_size_threshold = usize::MAX;
    config.flush_interval = Duration::from_hours(24);
    let mut coordinator = WriteCoordinator::new(
        config,
        vec!["a", "b"],
        TestContext::default(),
        flusher.initial_snapshot().await,
        flusher.clone(),
    );
    let handle_a = coordinator.handle("a");
    let handle_b = coordinator.handle("b");
    let pause_handle = coordinator.pause_handle("a");

    // pause before starting the coordinator. otherwise
    // there's a race condition where the PausableReceiver
    // makes it past pause_rx.wait_for(|v| !*v).await; and
    // waits for recv before we pause the handle - we could
    // change the behavior of the PausableReceiver to first
    // wait on recv and then check pause before returning
    // but that feels weird and is not necessary for the use
    // cases we have in mind
    pause_handle.pause();
    coordinator.start();

    // when
    let mut result_a = handle_a
        .try_write(TestWrite {
            key: "a".into(),
            value: 1,
            size: 1,
        })
        .await
        .unwrap();
    for i in 0..1000 {
        handle_b
            .try_write(TestWrite {
                key: format!("b{}", i),
                value: i,
                size: 1,
            })
            .await
            .unwrap()
            .wait(Durability::Applied)
            .await
            .unwrap();
    }

    // then
    // make sure the data only contains keys from b (so a was never processed)
    let data = coordinator.view().current.data.lock().unwrap().clone();
    let mut expected = (0..1000).map(|i| format!("b{}", i)).collect::<HashSet<_>>();
    assert_eq!(data.keys().cloned().collect::<HashSet<_>>(), expected);
    pause_handle.unpause();
    // after resuming, wait for the data to include keys from a
    result_a.wait(Durability::Applied).await.unwrap();
    let data = coordinator.view().current.data.lock().unwrap().clone();
    expected.insert("a".into());
    assert_eq!(data.keys().cloned().collect::<HashSet<_>>(), expected);
}
