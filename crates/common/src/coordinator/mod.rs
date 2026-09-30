#![allow(unused)]

mod error;
mod handle;
pub(crate) mod metrics;
pub mod subscriber;
mod traits;

use std::collections::HashMap;
use std::ops::Range;
use std::ops::{Deref, DerefMut};

pub use error::{WriteError, WriteResult};
use futures::stream::{self, SelectAll, StreamExt};
pub use handle::{View, WriteCoordinatorHandle, WriteHandle};
pub use metrics::describe_coordinator_metrics;
pub use subscriber::{SubscribeError, ViewMonitor, ViewSubscriber};
pub use traits::{Delta, Durability, EpochStamped, Flusher};

/// Event sent from the write coordinator task to the flush task.
enum FlushEvent<D: Delta> {
    /// Flush a frozen delta to storage.
    FlushDelta { frozen: EpochStamped<D::Frozen> },
    /// Ensure storage durability (e.g. call storage.flush()).
    FlushStorage,
}

// Internal use only
use crate::StorageRead;
use async_trait::async_trait;
pub use handle::EpochWatcher;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, Interval, interval_at};
use tokio_util::sync::CancellationToken;

/// Configuration for the write coordinator.
#[derive(Debug, Clone)]
pub struct WriteCoordinatorConfig {
    /// Maximum number of pending writes in the queue.
    pub queue_capacity: usize,
    /// Interval at which to trigger automatic flushes.
    pub flush_interval: Duration,
    /// Delta size threshold at which to trigger a flush.
    pub flush_size_threshold: usize,
}

impl Default for WriteCoordinatorConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            flush_interval: Duration::from_secs(10),
            flush_size_threshold: 64 * 1024 * 1024, // 64 MB
        }
    }
}

pub(crate) enum WriteCommand<D: Delta> {
    Write {
        write: D::Write,
        result_tx: oneshot::Sender<handle::EpochResult<D::ApplyResult>>,
    },
    Flush {
        epoch_tx: oneshot::Sender<handle::EpochResult<()>>,
        flush_storage: bool,
    },
}

/// The write coordinator manages write ordering, batching, and durability.
///
/// It accepts writes through `WriteCoordinatorHandle`, applies them to a `Delta`,
/// and coordinates flushing through a `Flusher`.
pub struct WriteCoordinator<D: Delta, F: Flusher<D>> {
    handles: HashMap<String, WriteCoordinatorHandle<D>>,
    pause_handles: HashMap<String, PauseHandle>,
    stop_tok: CancellationToken,
    tasks: Option<(WriteCoordinatorTask<D>, FlushTask<D, F>)>,
    write_task_jh: Option<tokio::task::JoinHandle<Result<(), String>>>,
    view: Arc<BroadcastedView<D>>,
}

impl<D: Delta, F: Flusher<D>> WriteCoordinator<D, F> {
    pub fn new(
        config: WriteCoordinatorConfig,
        channels: Vec<impl ToString>,
        initial_context: D::Context,
        initial_snapshot: D::Snapshot,
        flusher: F,
    ) -> WriteCoordinator<D, F> {
        let (watermarks, watcher) = EpochWatermarks::new();
        let watermarks = Arc::new(watermarks);

        // Create a write channel per named input
        let mut write_rxs = Vec::with_capacity(channels.len());
        let mut write_txs = HashMap::new();
        let mut pause_handles = HashMap::new();
        for name in &channels {
            let (write_tx, write_rx, pause_hdl) = pausable_channel(config.queue_capacity);
            write_rxs.push(write_rx);
            write_txs.insert(name.to_string(), write_tx);
            pause_handles.insert(name.to_string(), pause_hdl);
        }

        // FlushEvents go to a background task so converting deltas to storage
        // operations never runs on the write path. When this queue is full
        // the write task defers freezing (see `PendingFlush`) rather than
        // blocking, so writes keep being applied during slow flushes.
        let (flush_tx, flush_rx) = mpsc::channel(FLUSH_QUEUE_CAPACITY);

        let flush_stop_tok = CancellationToken::new();
        let stop_tok = CancellationToken::new();
        let write_task = WriteCoordinatorTask::new(
            config,
            initial_context,
            initial_snapshot,
            write_rxs,
            flush_tx,
            watermarks.clone(),
            stop_tok.clone(),
            flush_stop_tok.clone(),
        );

        let view = write_task.view.clone();

        let handles = write_txs
            .into_iter()
            .map(|(name, write_tx)| {
                let handle = WriteCoordinatorHandle::new(
                    name.clone(),
                    write_tx,
                    watcher.clone(),
                    view.clone(),
                );
                (name, handle)
            })
            .collect();

        let flush_task = FlushTask {
            flusher,
            stop_tok: flush_stop_tok,
            flush_rx,
            watermarks: watermarks.clone(),
            view: view.clone(),
            last_flushed_epoch: 0,
        };

        Self {
            handles,
            pause_handles,
            tasks: Some((write_task, flush_task)),
            write_task_jh: None,
            stop_tok,
            view,
        }
    }

    pub fn handle(&self, name: &str) -> WriteCoordinatorHandle<D> {
        self.handles
            .get(name)
            .expect("unknown channel name")
            .clone()
    }

    pub fn pause_handle(&self, name: &str) -> PauseHandle {
        self.pause_handles
            .get(name)
            .expect("unknown channel name")
            .clone()
    }

    pub fn start(&mut self) {
        let Some((write_task, flush_task)) = self.tasks.take() else {
            // already started
            return;
        };
        let flush_task_jh = flush_task.run();
        let write_task_jh = write_task.run(flush_task_jh);
        self.write_task_jh = Some(write_task_jh);
    }

    pub async fn stop(mut self) -> Result<(), String> {
        let Some(write_task_jh) = self.write_task_jh.take() else {
            return Ok(());
        };
        self.stop_tok.cancel();
        write_task_jh.await.map_err(|e| e.to_string())?
    }

    pub fn view(&self) -> Arc<View<D>> {
        self.view.current()
    }

    pub fn subscribe(&self) -> (ViewSubscriber<D>, ViewMonitor) {
        let (view_rx, initial_view) = self.view.subscribe();
        ViewSubscriber::new(view_rx, initial_view)
    }
}

/// Frozen deltas (or storage flushes) queued for the flush task. Each queued
/// delta is also held in the view, so this bounds frozen-delta memory.
const FLUSH_QUEUE_CAPACITY: usize = 2;

/// Flush work requested while the flush queue was full, sent in order
/// (delta first, then storage) as soon as the queue has room. Deferring the
/// freeze keeps the write loop applying writes to the live delta instead of
/// stalling on the flush task.
#[derive(Default)]
struct PendingFlush {
    delta: Option<FlushReason>,
    storage: bool,
}

impl PendingFlush {
    fn is_pending(&self) -> bool {
        self.delta.is_some() || self.storage
    }
}

struct WriteCoordinatorTask<D: Delta> {
    config: WriteCoordinatorConfig,
    delta: CurrentDelta<D>,
    flush_tx: mpsc::Sender<FlushEvent<D>>,
    pending: PendingFlush,
    write_rxs: Vec<PausableReceiver<D>>,
    watermarks: Arc<EpochWatermarks>,
    view: Arc<BroadcastedView<D>>,
    epoch: u64,
    delta_start_epoch: u64,
    flush_interval: Interval,
    stop_tok: CancellationToken,
    flush_stop_tok: CancellationToken,
}

impl<D: Delta> WriteCoordinatorTask<D> {
    /// Create a new write coordinator with the given flusher.
    ///
    /// This is useful for testing with mock flushers.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: WriteCoordinatorConfig,
        initial_context: D::Context,
        initial_snapshot: D::Snapshot,
        write_rxs: Vec<PausableReceiver<D>>,
        flush_tx: mpsc::Sender<FlushEvent<D>>,
        watermarks: Arc<EpochWatermarks>,
        stop_tok: CancellationToken,
        flush_stop_tok: CancellationToken,
    ) -> Self {
        let delta = D::init(initial_context);

        let initial_view = View {
            current: delta.reader(),
            frozen: vec![],
            snapshot: initial_snapshot,
            last_written_delta: None,
        };
        let initial_view = Arc::new(BroadcastedView::new(initial_view));

        let flush_interval = interval_at(
            Instant::now() + config.flush_interval,
            config.flush_interval,
        );
        Self {
            config,
            delta: CurrentDelta::new(delta),
            write_rxs,
            flush_tx,
            pending: PendingFlush::default(),
            watermarks,
            view: initial_view,
            // Epochs start at 1 because watch channels initialize to 0 (meaning "nothing
            // processed yet"). If the first write had epoch 0, wait() would return
            // immediately since the condition `watermark < epoch` would be `0 < 0` = false.
            epoch: 1,
            delta_start_epoch: 1,
            flush_interval,
            stop_tok,
            flush_stop_tok,
        }
    }

    /// Run the coordinator event loop.
    pub fn run(
        mut self,
        flush_task_jh: tokio::task::JoinHandle<WriteResult<()>>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        tokio::task::spawn(async move { self.run_coordinator(flush_task_jh).await })
    }

    async fn run_coordinator(
        mut self,
        mut flush_task_jh: tokio::task::JoinHandle<WriteResult<()>>,
    ) -> Result<(), String> {
        // Reset the interval to start fresh from when run() is called
        self.flush_interval.reset();

        // Merge all write receivers into a single stream
        let mut write_stream: SelectAll<_> = SelectAll::new();
        for rx in self.write_rxs.drain(..) {
            write_stream.push(
                stream::unfold(
                    rx,
                    |mut rx| async move { rx.recv().await.map(|cmd| (cmd, rx)) },
                )
                .boxed(),
            );
        }

        loop {
            tokio::select! {
                cmd = write_stream.next() => {
                    match cmd {
                        Some(WriteCommand::Write { write, result_tx }) => {
                            self.handle_write(write, result_tx).await?;
                        }
                        Some(WriteCommand::Flush { epoch_tx, flush_storage }) => {
                            // Send back the epoch of the last processed write
                            let _ = epoch_tx.send(Ok(handle::WriteApplied {
                                epoch: self.epoch.saturating_sub(1),
                                result: (),
                            }));
                            self.request_flush(FlushReason::Explicit, flush_storage);
                        }
                        None => {
                            // All write channels closed
                            break;
                        }
                    }
                }

                _ = self.flush_interval.tick() => {
                    self.request_flush(FlushReason::Interval, false);
                }

                _ = has_room(&self.flush_tx), if self.pending.is_pending() => {
                    self.send_pending();
                }

                _ = self.stop_tok.cancelled() => {
                    break;
                }

                result = &mut flush_task_jh => {
                    return result
                        .map_err(|e| format!("flush task panicked: {}", e))?
                        .map_err(|e| format!("flush task error: {}", e));
                }
            }
        }

        // Flush any remaining pending writes before shutdown, waiting for room.
        self.request_flush(FlushReason::Shutdown, false);
        while self.pending.is_pending() && has_room(&self.flush_tx).await {
            self.send_pending();
        }

        // Signal the flush task to stop
        self.flush_stop_tok.cancel();
        // Wait for the flush task to complete and propagate any errors
        flush_task_jh
            .await
            .map_err(|e| format!("flush task panicked: {}", e))?
            .map_err(|e| format!("flush task error: {}", e))
    }

    async fn handle_write(
        &mut self,
        op: D::Write,
        result_tx: oneshot::Sender<handle::EpochResult<D::ApplyResult>>,
    ) -> Result<(), String> {
        let write_epoch = self.epoch;
        self.epoch += 1;

        let apply_start = std::time::Instant::now();
        let result = self.delta.apply(op);
        ::metrics::histogram!(metrics::COORDINATOR_DELTA_APPLY_DURATION_SECONDS)
            .record(apply_start.elapsed().as_secs_f64());

        // Ignore error if receiver was dropped (fire-and-forget write)
        let _ = result_tx.send(
            result
                .map(|apply_result| handle::WriteApplied {
                    epoch: write_epoch,
                    result: apply_result,
                })
                .map_err(|e| handle::WriteFailed {
                    epoch: write_epoch,
                    error: e,
                }),
        );

        // Ignore error if no watchers are listening - this is non-fatal
        self.watermarks.update_applied(write_epoch);

        let estimated = self.delta.estimate_size();
        ::metrics::gauge!(metrics::COORDINATOR_DELTA_ESTIMATED_BYTES).set(estimated as f64);
        if estimated >= self.config.flush_size_threshold {
            self.request_flush(FlushReason::SizeThreshold, false);
        }

        Ok(())
    }

    /// Queues a delta flush (if the delta has writes) and optionally a
    /// storage flush, sending whatever the flush queue has room for now.
    fn request_flush(&mut self, reason: FlushReason, flush_storage: bool) {
        let deferred = self.pending.is_pending();
        if self.epoch != self.delta_start_epoch && self.pending.delta.is_none() {
            self.pending.delta = Some(reason);
        }
        self.pending.storage |= flush_storage;
        // Already-deferred work is sent by the run loop once there is room.
        if !deferred {
            self.send_pending();
        }
    }

    /// Sends pending flush work in order until done or the queue is full.
    /// A deferred delta keeps absorbing writes, so it is frozen only once it
    /// can be sent.
    fn send_pending(&mut self) {
        while self.pending.is_pending() {
            let permit = match self.flush_tx.clone().try_reserve_owned() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    ::metrics::counter!(metrics::COORDINATOR_FLUSH_DEFERRED_TOTAL).increment(1);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.pending = PendingFlush::default();
                    return;
                }
            };
            ::metrics::gauge!(metrics::COORDINATOR_FLUSH_EVENT_QUEUE_DEPTH)
                .set(FLUSH_QUEUE_CAPACITY.saturating_sub(self.flush_tx.capacity()) as f64);
            let event = match self.pending.delta.take() {
                Some(reason) => self.freeze(reason),
                None => {
                    self.pending.storage = false;
                    FlushEvent::FlushStorage
                }
            };
            permit.send(event);
        }
    }

    fn freeze(&mut self, reason: FlushReason) -> FlushEvent<D> {
        self.flush_interval.reset();

        ::metrics::counter!(metrics::COORDINATOR_FLUSH_TOTAL, "reason" => reason.as_str())
            .increment(1);

        let epoch_range = self.delta_start_epoch..self.epoch;
        self.delta_start_epoch = self.epoch;

        let freeze_start = std::time::Instant::now();
        let (frozen, frozen_reader) = self.delta.freeze_and_init();
        ::metrics::histogram!(metrics::COORDINATOR_DELTA_FREEZE_DURATION_SECONDS)
            .record(freeze_start.elapsed().as_secs_f64());
        // Reset estimated bytes gauge: the new delta starts empty.
        ::metrics::gauge!(metrics::COORDINATOR_DELTA_ESTIMATED_BYTES).set(0.0);

        let stamped_frozen = EpochStamped::new(frozen, epoch_range.clone());
        let stamped_frozen_reader = EpochStamped::new(frozen_reader, epoch_range.clone());
        let reader = self.delta.reader();
        // update the view before sending the flush msg to ensure the flusher sees
        // the frozen reader when updating the view post-flush
        self.view.update_delta_frozen(stamped_frozen_reader, reader);
        FlushEvent::FlushDelta {
            frozen: stamped_frozen,
        }
    }
}

/// Resolves once the flush queue has a free slot (`false` if it closed).
/// Only the write task sends, so the slot is still free when it acts.
async fn has_room<T>(flush_tx: &mpsc::Sender<T>) -> bool {
    flush_tx.reserve().await.is_ok()
}

/// Reason a flush was triggered. Used as a low-cardinality `reason` label.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FlushReason {
    SizeThreshold,
    Interval,
    Explicit,
    Shutdown,
}

impl FlushReason {
    fn as_str(self) -> &'static str {
        match self {
            FlushReason::SizeThreshold => "size_threshold",
            FlushReason::Interval => "interval",
            FlushReason::Explicit => "explicit",
            FlushReason::Shutdown => "shutdown",
        }
    }
}

struct FlushTask<D: Delta, F: Flusher<D>> {
    flusher: F,
    stop_tok: CancellationToken,
    flush_rx: mpsc::Receiver<FlushEvent<D>>,
    watermarks: Arc<EpochWatermarks>,
    view: Arc<BroadcastedView<D>>,
    last_flushed_epoch: u64,
}

impl<D: Delta, F: Flusher<D>> FlushTask<D, F> {
    fn run(mut self) -> tokio::task::JoinHandle<WriteResult<()>> {
        tokio::spawn(async move {
            let result = async {
                loop {
                    tokio::select! {
                        event = self.flush_rx.recv() => {
                            let Some(event) = event else {
                                break;
                            };
                            self.handle_event(event).await?;
                        }
                        _ = self.stop_tok.cancelled() => {
                            break;
                        }
                    }
                }
                // drain all remaining flush events
                while let Ok(event) = self.flush_rx.try_recv() {
                    self.handle_event(event).await?;
                }
                Ok(())
            }
            .await;
            if let Err(WriteError::FlushError(error)) = &result {
                self.watermarks.fail_flush(error.clone());
            }
            result
        })
    }

    async fn handle_event(&mut self, event: FlushEvent<D>) -> WriteResult<()> {
        match event {
            FlushEvent::FlushDelta { frozen } => self.handle_flush(frozen).await,
            FlushEvent::FlushStorage => {
                let start = std::time::Instant::now();
                let result = self
                    .flusher
                    .flush_storage()
                    .await
                    .map_err(|e| WriteError::FlushError(e.to_string()));
                ::metrics::histogram!(metrics::COORDINATOR_FLUSH_STORAGE_DURATION_SECONDS)
                    .record(start.elapsed().as_secs_f64());
                result?;
                self.watermarks.update_durable(self.last_flushed_epoch);
                Ok(())
            }
        }
    }

    async fn handle_flush(&mut self, frozen: EpochStamped<D::Frozen>) -> WriteResult<()> {
        let delta = frozen.val;
        let epoch_range = frozen.epoch_range;
        let start = std::time::Instant::now();
        let result = self
            .flusher
            .flush_delta(delta, &epoch_range)
            .await
            .map_err(|e| WriteError::FlushError(e.to_string()));
        ::metrics::histogram!(metrics::COORDINATOR_FLUSH_DELTA_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());
        let snapshot = result?;
        self.last_flushed_epoch = epoch_range.end - 1;
        // Publish the refreshed view first so any waiter awoken by the
        // watermark notification below observes a view that already includes
        // this flush. Reversing the order races on multi-thread runtimes:
        // a waiter on `Written` can schedule on another worker and read
        // `view.snapshot` before this task completes the view update.
        self.view.update_flush_finished(snapshot, epoch_range);
        self.watermarks.update_written(self.last_flushed_epoch);
        Ok(())
    }
}

struct CurrentDelta<D: Delta> {
    delta: Option<D>,
}

impl<D: Delta> Deref for CurrentDelta<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        match &self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> DerefMut for CurrentDelta<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> CurrentDelta<D> {
    fn new(delta: D) -> Self {
        Self { delta: Some(delta) }
    }

    fn freeze_and_init(&mut self) -> (D::Frozen, D::FrozenView) {
        let Some(delta) = self.delta.take() else {
            panic!("delta not initialized");
        };
        let (frozen, frozen_reader, context) = delta.freeze();
        let new_delta = D::init(context);
        self.delta = Some(new_delta);
        (frozen, frozen_reader)
    }
}

pub struct EpochWatermarks {
    applied_tx: tokio::sync::watch::Sender<u64>,
    written_tx: tokio::sync::watch::Sender<u64>,
    durable_tx: tokio::sync::watch::Sender<u64>,
    terminal_tx: tokio::sync::watch::Sender<Option<String>>,
}

impl EpochWatermarks {
    pub fn new() -> (Self, EpochWatcher) {
        let (applied_tx, applied_rx) = tokio::sync::watch::channel(0);
        let (written_tx, written_rx) = tokio::sync::watch::channel(0);
        let (durable_tx, durable_rx) = tokio::sync::watch::channel(0);
        let (terminal_tx, terminal_rx) = tokio::sync::watch::channel(None);
        let watcher = EpochWatcher {
            applied_rx,
            written_rx,
            durable_rx,
            terminal_rx,
        };
        let watermarks = EpochWatermarks {
            applied_tx,
            written_tx,
            durable_tx,
            terminal_tx,
        };
        (watermarks, watcher)
    }

    pub fn update_applied(&self, epoch: u64) {
        let _ = self.applied_tx.send(epoch);
    }

    pub fn update_written(&self, epoch: u64) {
        let _ = self.written_tx.send(epoch);
    }

    pub fn update_durable(&self, epoch: u64) {
        let _ = self.durable_tx.send(epoch);
    }

    fn fail_flush(&self, error: String) {
        let _ = self.terminal_tx.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(error);
            true
        });
    }
}

/// The latest [`View`], readable without locking, plus a broadcast of every
/// view transition.
///
/// Readers load `view` lock-free. Updates and [`subscribe`](Self::subscribe)
/// hold `publish`, so a subscriber's initial view is exactly the view that
/// precedes the first broadcast it receives.
pub(crate) struct BroadcastedView<D: Delta> {
    view: arc_swap::ArcSwap<View<D>>,
    publish: Mutex<broadcast::Sender<Arc<View<D>>>>,
}

impl<D: Delta> BroadcastedView<D> {
    fn new(initial_view: View<D>) -> Self {
        let (view_tx, _) = broadcast::channel(16);
        Self {
            view: arc_swap::ArcSwap::from_pointee(initial_view),
            publish: Mutex::new(view_tx),
        }
    }

    fn update_flush_finished(&self, snapshot: D::Snapshot, epoch_range: Range<u64>) {
        self.update(|view| {
            let mut frozen = view.frozen.clone();
            let last = frozen
                .pop()
                .expect("frozen should not be empty when flush completes");
            assert_eq!(last.epoch_range, epoch_range);
            View {
                current: view.current.clone(),
                frozen,
                snapshot,
                last_written_delta: Some(last),
            }
        });
    }

    fn update_delta_frozen(&self, frozen: EpochStamped<D::FrozenView>, reader: D::DeltaView) {
        self.update(|view| {
            let mut new_frozen = vec![frozen];
            new_frozen.extend(view.frozen.iter().cloned());
            View {
                current: reader,
                frozen: new_frozen,
                snapshot: view.snapshot.clone(),
                last_written_delta: view.last_written_delta.clone(),
            }
        });
    }

    fn update(&self, next: impl FnOnce(&View<D>) -> View<D>) {
        let view_tx = self.publish.lock().unwrap_or_else(PoisonError::into_inner);
        let view = Arc::new(next(&self.view.load()));
        self.view.store(Arc::clone(&view));
        let _ = view_tx.send(view);
    }

    fn current(&self) -> Arc<View<D>> {
        self.view.load_full()
    }

    fn subscribe(&self) -> (broadcast::Receiver<Arc<View<D>>>, Arc<View<D>>) {
        let view_tx = self.publish.lock().unwrap_or_else(PoisonError::into_inner);
        (view_tx.subscribe(), self.view.load_full())
    }
}

struct PausableReceiver<D: Delta> {
    pause_rx: Option<watch::Receiver<bool>>,
    rx: mpsc::Receiver<WriteCommand<D>>,
}

impl<D: Delta> PausableReceiver<D> {
    async fn recv(&mut self) -> Option<WriteCommand<D>> {
        if let Some(pause_rx) = self.pause_rx.as_mut() {
            pause_rx.wait_for(|v| !*v).await;
        }
        self.rx.recv().await
    }
}

/// A handle that can be used to pause/unpause the coordinator's consumption from a write queue
#[derive(Clone)]
pub struct PauseHandle {
    pause_tx: tokio::sync::watch::Sender<bool>,
}

impl PauseHandle {
    pub fn pause(&self) {
        self.pause_tx.send_replace(true);
    }

    pub fn unpause(&self) {
        self.pause_tx.send_replace(false);
    }
}

fn pausable_channel<D: Delta>(
    capacity: usize,
) -> (
    mpsc::Sender<WriteCommand<D>>,
    PausableReceiver<D>,
    PauseHandle,
) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (tx, rx) = mpsc::channel(capacity);
    (
        tx,
        PausableReceiver {
            pause_rx: Some(pause_rx),
            rx,
        },
        PauseHandle { pause_tx },
    )
}

#[cfg(test)]
mod tests;
