//! SlateDB's foyer hybrid cache, with block warming kept out of its memory
//! tier.
//!
//! Inside a [`warming_payloads`] scope, the cache inserts blocks into its
//! disk tier only, finds blocks without promoting a disk hit into memory,
//! and waits for the disk tier to write what is queued instead of letting
//! a burst of inserts overflow its submit queue, where foyer drops them. So
//! warming fills the disk tier at the speed it writes, and the memory tier
//! keeps what queries read. Outside a scope it is SlateDB's
//! `FoyerHybridCache`.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use foyer::{HybridCache, HybridCachePolicy, HybridCacheProperties, Load, Location};
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};

tokio::task_local! {
    static WARMING: Arc<AtomicU64>;
}

/// Runs `future` as block warming, adding to `bytes` the size of every data
/// block it finds in or inserts into a [`WarmingHybridCache`].
pub async fn warming_payloads<F: Future>(bytes: Arc<AtomicU64>, future: F) -> F::Output {
    WARMING.scope(bytes, future).await
}

pub struct WarmingHybridCache {
    inner: HybridCache<CachedKey, CachedEntry>,
    /// Bytes warming queued for the disk tier since it last waited for it.
    queued: AtomicU64,
    /// Queued bytes past which warming waits for the disk tier: half its
    /// submit queue, whose overflow foyer drops.
    queue_limit: u64,
    /// Times foyer queues a disk-only insert: under `WriteOnInsertion` both
    /// the insert and the release of its phantom memory entry queue it.
    copies: u64,
}

impl WarmingHybridCache {
    pub fn new(
        inner: HybridCache<CachedKey, CachedEntry>,
        submit_queue_bytes: u64,
        policy: HybridCachePolicy,
    ) -> Self {
        Self {
            inner,
            queued: AtomicU64::new(0),
            queue_limit: (submit_queue_bytes / 2).max(1),
            copies: match policy {
                HybridCachePolicy::WriteOnInsertion => 2,
                HybridCachePolicy::WriteOnEviction => 1,
            },
        }
    }

    async fn get(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        let Ok(warmed) = WARMING.try_with(Arc::clone) else {
            return self
                .inner
                .get(key)
                .await
                .map(|entry| entry.map(|entry| entry.value().clone()))
                .map_err(|error| slatedb::Error::internal(format!("hybrid cache get: {error}")));
        };
        let found = match self.inner.memory().get(key) {
            Some(entry) => Some(entry.value().clone()),
            None => match self.inner.storage().load(key).await {
                Ok(Load::Entry { value, .. }) => Some(value),
                Ok(Load::Piece { piece, .. }) => Some(piece.value().clone()),
                Ok(Load::Throttled | Load::Miss) | Err(_) => None,
            },
        };
        if let Some(entry) = &found {
            warmed.fetch_add(entry.size() as u64, Ordering::Relaxed);
        }
        Ok(found)
    }
}

#[async_trait]
impl DbCache for WarmingHybridCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get(key).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get(key).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get(key).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get(key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        let Ok(warmed) = WARMING.try_with(Arc::clone) else {
            self.inner.insert(key, value);
            return;
        };
        let size = value.size() as u64;
        let queued = size * self.copies;
        if self.queued.fetch_add(queued, Ordering::AcqRel) + queued > self.queue_limit {
            self.inner.storage().wait().await;
            self.queued.store(queued, Ordering::Release);
        }
        warmed.fetch_add(size, Ordering::Relaxed);
        self.inner.insert_with_properties(
            key,
            value,
            HybridCacheProperties::default().with_location(Location::OnDisk),
        );
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key);
    }

    fn entry_count(&self) -> u64 {
        0
    }

    async fn close(&self) -> Result<(), slatedb::Error> {
        tracing::info!(
            memory_bytes = self.inner.memory().usage(),
            "foyer hybrid cache: closing, flushing memory to disk"
        );
        self.inner
            .close()
            .await
            .map_err(|error| slatedb::Error::internal(format!("hybrid cache close: {error}")))
    }
}
