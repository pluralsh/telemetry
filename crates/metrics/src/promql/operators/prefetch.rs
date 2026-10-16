//! Read-ahead for the selectors' series-chunk loads. A selector emits one
//! chunk while the next ones load, so a cold query pays for its chunks'
//! object-store round trips together instead of one chunk after another.
//!
//! Prefetched chunks hold their memory reservation early, so a failed load
//! is never final while prefetches overlap it: the selector drops them and
//! retries that chunk alone, without read-ahead, as it would have loaded.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::super::memory::QueryError;

pub(crate) type ChunkLoad<'a, T> = Pin<Box<dyn Future<Output = Result<T, QueryError>> + Send + 'a>>;

struct Load<'a, T> {
    chunk_start: usize,
    future: Option<ChunkLoad<'a, T>>,
    done: Option<Result<T, QueryError>>,
}

pub(crate) struct Prefetch<'a, T> {
    depth: usize,
    loads: VecDeque<Load<'a, T>>,
}

impl<'a, T: Send + 'a> Prefetch<'a, T> {
    pub(crate) fn new(depth: usize) -> Self {
        Self {
            depth,
            loads: VecDeque::new(),
        }
    }

    /// Whether a load failing now may be an artifact of read-ahead.
    pub(crate) fn active(&self) -> bool {
        self.depth > 0
    }

    /// Drops every queued load, releasing what they reserved, and stops
    /// reading ahead.
    pub(crate) fn disable(&mut self) {
        self.depth = 0;
        self.loads.clear();
    }

    /// The prefetched load of the chunk at `chunk_start`, unless it was not
    /// queued or failed.
    pub(crate) fn take(&mut self, chunk_start: usize) -> Option<ChunkLoad<'a, T>> {
        while let Some(front) = self.loads.front()
            && front.chunk_start < chunk_start
        {
            self.loads.pop_front();
        }
        if self.loads.front()?.chunk_start != chunk_start {
            return None;
        }
        let taken = self.loads.pop_front()?;
        match (taken.done, taken.future) {
            (Some(Ok(value)), _) => Some(Box::pin(std::future::ready(Ok(value)))),
            (None, Some(future)) => Some(future),
            _ => None,
        }
    }

    /// Which of `starts` (ascending) to queue next: those past the queued
    /// loads, up to the read-ahead depth.
    pub(crate) fn wanted(&self, starts: impl IntoIterator<Item = usize>) -> Vec<usize> {
        let queued_to = self.loads.back().map(|load| load.chunk_start);
        starts
            .into_iter()
            .filter(|&start| queued_to.is_none_or(|queued| start > queued))
            .take(self.depth.saturating_sub(self.loads.len()))
            .collect()
    }

    pub(crate) fn push(&mut self, chunk_start: usize, future: ChunkLoad<'a, T>) {
        self.loads.push_back(Load {
            chunk_start,
            future: Some(future),
            done: None,
        });
    }

    /// Advances every queued load, keeping what completes.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) {
        for load in &mut self.loads {
            if let Some(future) = load.future.as_mut()
                && let Poll::Ready(result) = future.as_mut().poll(cx)
            {
                load.done = Some(result);
                load.future = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ready(value: usize) -> ChunkLoad<'static, usize> {
        Box::pin(std::future::ready(Ok(value)))
    }

    fn poll_all(prefetch: &mut Prefetch<'static, usize>) {
        let waker = futures::task::noop_waker();
        prefetch.poll(&mut Context::from_waker(&waker));
    }

    #[test]
    fn should_queue_up_to_the_depth_past_queued_chunks() {
        // given
        let mut prefetch = Prefetch::new(2);
        prefetch.push(10, ready(10));

        // when
        let wanted = prefetch.wanted([10, 20, 30]);

        // then
        assert_eq!(wanted, vec![20]);
    }

    #[test]
    fn should_serve_prefetched_chunks_and_skip_passed_ones() {
        // given
        let mut prefetch = Prefetch::new(2);
        prefetch.push(10, ready(10));
        prefetch.push(20, ready(20));
        poll_all(&mut prefetch);

        // when
        let later = prefetch.take(20).expect("prefetched");

        // then: 10 was passed over and dropped
        assert_eq!(futures::executor::block_on(later).unwrap(), 20);
        assert!(prefetch.take(10).is_none());
        assert!(prefetch.loads.is_empty());
    }

    #[test]
    fn should_not_serve_a_failed_prefetch() {
        // given
        let mut prefetch: Prefetch<'static, usize> = Prefetch::new(1);
        prefetch.push(
            5,
            Box::pin(std::future::ready(Err(QueryError::Internal("full".into())))),
        );
        poll_all(&mut prefetch);

        // then
        assert!(prefetch.take(5).is_none());
    }

    #[test]
    fn should_drop_queued_loads_once_disabled() {
        // given
        let dropped = Arc::new(AtomicUsize::new(0));
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let mut prefetch: Prefetch<'static, Counted> = Prefetch::new(2);
        for start in [1, 2] {
            prefetch.push(
                start,
                Box::pin(std::future::ready(Ok(Counted(dropped.clone())))),
            );
        }
        let waker = futures::task::noop_waker();
        prefetch.poll(&mut Context::from_waker(&waker));

        // when
        prefetch.disable();

        // then
        assert!(!prefetch.active());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert!(prefetch.wanted([3, 4]).is_empty());
    }
}
