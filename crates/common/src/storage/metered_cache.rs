//! An in-memory SlateDB cache weighed by the memory its entries hold.
//!
//! SlateDB's own in-memory cache weighs an entry by its payload alone
//! ([`CachedEntry::size`]: the bloom filter bits or the index flatbuffer) and
//! inserts what a fetch loaded without copying it out of the read buffer it
//! may share. This cache copies every fetched entry into an allocation of its
//! own and adds [`ENTRY_OVERHEAD_BYTES`] to its weight, so its capacity bounds
//! the memory its entries keep alive.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use foyer::{Cache, CacheBuilder, Event, EventListener};
use slatedb::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache};
use slatedb::manifest::SsTableId;

use super::metrics_recorder::MixtricsBridge;

/// Memory an entry holds beyond its payload: the foyer record with its key
/// and eviction links, the index slot, and the `Arc`ed index or filter
/// structs around the payload.
pub const ENTRY_OVERHEAD_BYTES: usize = 320;

/// The bytes a [`MeteredCache`] charges for `entry`.
pub fn entry_weight(entry: &CachedEntry) -> usize {
    entry.size() + ENTRY_OVERHEAD_BYTES
}

tokio::task_local! {
    static ATTRIBUTION: Arc<AtomicU64>;
}

/// Runs `future`, adding to `bytes` the weight of every entry it fetches
/// through a metadata [`MeteredCache`], whether the fetch loaded the entry or
/// found it cached.
pub async fn attributing<F: Future>(bytes: Arc<AtomicU64>, future: F) -> F::Output {
    ATTRIBUTION.scope(bytes, future).await
}

/// Usage of a [`MeteredCache`], readable without the cache itself.
#[derive(Clone)]
pub struct CacheUsage {
    cache: Cache<CachedKey, CachedEntry>,
    evictions: Arc<AtomicU64>,
    /// Present on a metadata cache; see [`Self::holds`].
    resident: Option<Arc<Residency>>,
}

impl CacheUsage {
    /// Whether the cache holds the block at `offset` of SST `id` for any
    /// database sharing it, such as an SST's filter at its `filter_offset`.
    /// Always false for a data cache, which does not track its entries.
    pub fn holds(&self, id: SsTableId, offset: u64) -> bool {
        self.resident.as_ref().is_some_and(|resident| {
            resident
                .keys(id, offset)
                .iter()
                .any(|key| self.cache.contains(key))
        })
    }

    pub fn usage_bytes(&self) -> u64 {
        self.cache.usage() as u64
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.cache.capacity() as u64
    }

    /// Entries evicted to make room since the cache was built.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for CacheUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheUsage")
            .field("usage_bytes", &self.usage_bytes())
            .field("capacity_bytes", &self.capacity_bytes())
            .field("evictions", &self.evictions())
            .finish()
    }
}

/// The keys of the entries a cache was given, by SST and block offset.
///
/// SlateDB scopes every key to the database that made it and keeps the
/// scope private, so a key cannot be built from an SST id alone; this keeps
/// the keys the cache saw, and [`CacheUsage::holds`] asks the cache about
/// them. Keys leave when their entries are evicted or removed, so it holds
/// about as many keys as the cache does entries.
#[derive(Default)]
struct Residency(Mutex<HashMap<(SsTableId, u64), Vec<CachedKey>>>);

impl Residency {
    fn record(&self, key: &CachedKey) {
        let Some(block) = sst_block(key) else {
            return;
        };
        let mut keys = self.0.lock().expect("residency lock");
        let keys = keys.entry(block).or_default();
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }

    fn forget(&self, key: &CachedKey) {
        let Some(block) = sst_block(key) else {
            return;
        };
        let mut keys = self.0.lock().expect("residency lock");
        if let Some(scoped) = keys.get_mut(&block) {
            scoped.retain(|scoped| scoped != key);
            if scoped.is_empty() {
                keys.remove(&block);
            }
        }
    }

    /// Copied out so the cache is never asked while this is locked: foyer
    /// may call [`EvictionCounter::on_leave`] under a lock of its own.
    fn keys(&self, id: SsTableId, offset: u64) -> Vec<CachedKey> {
        self.0
            .lock()
            .expect("residency lock")
            .get(&(id, offset))
            .cloned()
            .unwrap_or_default()
    }
}

/// The SST and block offset of `key`, read from SlateDB's serialized key
/// format, which disk caches persist and so keeps stable.
fn sst_block(key: &CachedKey) -> Option<(SsTableId, u64)> {
    #[derive(serde::Deserialize)]
    enum Id {
        Wal(u64),
        Compacted(String),
    }
    #[derive(serde::Deserialize)]
    enum Key {
        V1(Id, u64),
        V2(serde::de::IgnoredAny, Id, u64),
    }
    let (Key::V1(id, block) | Key::V2(_, id, block)) =
        serde_json::from_value(serde_json::to_value(key).ok()?).ok()?;
    let id = match id {
        Id::Wal(id) => SsTableId::Wal(id),
        Id::Compacted(id) => SsTableId::Compacted(ulid::Ulid::from_string(&id).ok()?),
    };
    Some((id, block))
}

struct EvictionCounter {
    evictions: Arc<AtomicU64>,
    counter: metrics::Counter,
    resident: Option<Arc<Residency>>,
}

impl EventListener for EvictionCounter {
    type Key = CachedKey;
    type Value = CachedEntry;

    fn on_leave(&self, reason: Event, key: &CachedKey, _value: &CachedEntry) {
        if reason == Event::Evict {
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.counter.increment(1);
        }
        // A replaced entry's key stays in the cache.
        if reason != Event::Replace
            && let Some(resident) = &self.resident
        {
            resident.forget(key);
        }
    }
}

pub struct MeteredCache {
    usage: CacheUsage,
    /// Whether fetches are added to an [`attributing`] scope's bytes.
    attributes: bool,
}

impl MeteredCache {
    /// A cache of `capacity` bytes named `slatedb_{label}_cache` in foyer's
    /// metrics. Only a `meta` cache attributes its fetches and tracks which
    /// SST blocks it holds ([`CacheUsage::holds`]).
    pub fn new(label: &str, capacity: u64, shards: usize) -> Self {
        let evictions = Arc::new(AtomicU64::new(0));
        let meta = label == "meta";
        let resident = meta.then(|| Arc::new(Residency::default()));
        let name = format!("slatedb_{label}_cache");
        let cache = CacheBuilder::new(usize::try_from(capacity).unwrap_or(usize::MAX))
            .with_name(name.clone())
            .with_shards(shards.max(1))
            .with_weighter(|_, entry: &CachedEntry| entry_weight(entry))
            .with_event_listener(Arc::new(EvictionCounter {
                evictions: Arc::clone(&evictions),
                counter: metrics::counter!("telemetry_slatedb_cache_evictions_total", "cache" => name.clone()),
                resident: resident.clone(),
            }))
            .with_metrics_registry(Box::new(MixtricsBridge))
            .build();
        metrics::gauge!("telemetry_slatedb_cache_capacity_bytes", "cache" => name)
            .set(capacity as f64);
        Self {
            usage: CacheUsage {
                cache,
                evictions,
                resident,
            },
            attributes: meta,
        }
    }

    pub fn usage(&self) -> CacheUsage {
        self.usage.clone()
    }

    fn get(&self, key: &CachedKey) -> Option<CachedEntry> {
        self.usage.cache.get(key).map(|entry| entry.value().clone())
    }

    async fn fetch(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        let attribution = self
            .attributes
            .then(|| ATTRIBUTION.try_with(Arc::clone).ok())
            .flatten();
        let resident = self.usage.resident.clone();
        let loaded = key.clone();
        let entry = self
            .usage
            .cache
            .get_or_fetch(&key, move || async move {
                let entry = loader().await.map(|entry| entry.clamp_allocated_size());
                if entry.is_ok()
                    && let Some(resident) = resident
                {
                    resident.record(&loaded);
                }
                entry
            })
            .await
            .map(|entry| entry.value().clone())
            .map_err(|error| slatedb::Error::internal(format!("metered cache fetch: {error}")))?;
        if let Some(bytes) = attribution {
            bytes.fetch_add(entry_weight(&entry) as u64, Ordering::Relaxed);
        }
        Ok(entry)
    }
}

#[async_trait]
impl DbCache for MeteredCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        if let Some(resident) = &self.usage.resident {
            resident.record(&key);
        }
        self.usage.cache.insert(key, value);
    }

    async fn remove(&self, key: &CachedKey) {
        self.usage.cache.remove(key);
    }

    fn entry_count(&self) -> u64 {
        0
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.fetch(key, loader).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_sst_and_block_of_slatedb_cache_keys() {
        for id in [
            SsTableId::Compacted(ulid::Ulid::new()),
            SsTableId::Wal(7),
        ] {
            assert_eq!(sst_block(&CachedKey::from((id, 42))), Some((id, 42)));
        }
    }
}
