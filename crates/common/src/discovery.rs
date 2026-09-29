//! Shared durable discovery catalog for product metadata.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};

use crate::serde::sortable::{
    decode_f64_sortable, decode_i64_sortable, encode_f64_sortable, encode_i64_sortable,
};
use crate::serde::terminated_bytes;
use crate::storage::{PutOptions, RecordOp};
use crate::{BytesRange, PutRecordOp, Record, StorageError, StorageRead, StorageResult, Ttl};

/// Routing-slot sentinel reserved for partition-level catalog records.
pub const CATALOG_SLOT: u16 = u16::MAX;
/// On-disk format version for every discovery catalog key and value.
pub const CATALOG_FORMAT_VERSION: u8 = 1;

const NAME_RECORD: u8 = 1;
const VALUE_RECORD: u8 = 2;
const METADATA_RECORD: u8 = 3;

struct CacheEntry<V> {
    value: Arc<V>,
    expires_at: Option<Instant>,
}

/// Small per-reader cache for immutable partition catalog scans.
///
/// Closed partitions can be retained until capacity eviction. Active
/// partitions use a short TTL so newly written terms become visible.
pub struct DiscoveryCache<K, V> {
    entries: Mutex<HashMap<K, CacheEntry<V>>>,
    capacity: usize,
    active_ttl: Duration,
}

impl<K, V> DiscoveryCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn new(capacity: usize, active_ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
            active_ttl,
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let expired = entries
            .get(key)
            .is_some_and(|entry| entry.expires_at.is_some_and(|expiry| expiry <= now));
        if expired {
            entries.remove(key);
        }
        let value = entries.get(key).map(|entry| entry.value.clone());
        metrics::counter!(
            "telemetry_discovery_cache_requests_total",
            "result" => if value.is_some() { "hit" } else { "miss" }
        )
        .increment(1);
        value
    }

    pub fn insert(&self, key: K, value: V, active: bool) -> Arc<V> {
        let now = Instant::now();
        let value = Arc::new(value);
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, entry| entry.expires_at.is_none_or(|expiry| expiry > now));
        if entries.len() >= self.capacity
            && let Some(oldest) = entries.keys().next().cloned()
        {
            entries.remove(&oldest);
        }
        entries.insert(
            key,
            CacheEntry {
                value: value.clone(),
                expires_at: active.then(|| now + self.active_ttl),
            },
        );
        value
    }

    pub fn clear(&self) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Scalar value supported by the cross-product discovery catalog.
#[derive(Clone, Debug)]
pub enum DiscoveryValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

impl PartialEq for DiscoveryValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int(left), Self::Int(right)) => left == right,
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            _ => false,
        }
    }
}

impl Eq for DiscoveryValue {}

impl Ord for DiscoveryValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded().cmp(&other.encoded())
    }
}

impl PartialOrd for DiscoveryValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl DiscoveryValue {
    fn estimated_size(&self) -> usize {
        match self {
            Self::String(value) => 2 + value.len(),
            Self::Bool(_) => 2,
            Self::Int(_) | Self::Double(_) => 9,
        }
    }

    fn encode(&self, out: &mut BytesMut) {
        match self {
            Self::String(value) => {
                out.put_u8(1);
                terminated_bytes::serialize(value.as_bytes(), out);
            }
            Self::Bool(value) => {
                out.put_u8(2);
                out.put_u8(u8::from(*value));
            }
            Self::Int(value) => {
                out.put_u8(3);
                out.put_u64(encode_i64_sortable(*value));
            }
            Self::Double(value) => {
                out.put_u8(4);
                out.put_u64(encode_f64_sortable(*value));
            }
        }
    }

    fn encoded(&self) -> Bytes {
        let mut out = BytesMut::new();
        self.encode(&mut out);
        out.freeze()
    }

    fn decode(input: &mut &[u8]) -> StorageResult<Self> {
        let Some((&kind, rest)) = input.split_first() else {
            return Err(corrupt("discovery value is missing its type"));
        };
        *input = rest;
        match kind {
            1 => {
                let value = terminated_bytes::deserialize(input)
                    .map_err(|error| corrupt(error.to_string()))?;
                String::from_utf8(value.to_vec())
                    .map(Self::String)
                    .map_err(|error| corrupt(error.to_string()))
            }
            2 => {
                let Some((&value, rest)) = input.split_first() else {
                    return Err(corrupt("boolean discovery value is truncated"));
                };
                *input = rest;
                match value {
                    0 => Ok(Self::Bool(false)),
                    1 => Ok(Self::Bool(true)),
                    _ => Err(corrupt("boolean discovery value is invalid")),
                }
            }
            3 | 4 => {
                if input.len() < 8 {
                    return Err(corrupt("numeric discovery value is truncated"));
                }
                let encoded = u64::from_be_bytes(input[..8].try_into().unwrap());
                *input = &input[8..];
                if kind == 3 {
                    Ok(Self::Int(decode_i64_sortable(encoded)))
                } else {
                    Ok(Self::Double(decode_f64_sortable(encoded)))
                }
            }
            _ => Err(corrupt(format!("unknown discovery value type {kind}"))),
        }
    }
}

/// Deduplicating collection of catalog records for one time partition.
#[derive(Default)]
pub struct CatalogBatch {
    names: BTreeSet<(String, String)>,
    values: BTreeSet<(String, String, DiscoveryValue)>,
    metadata: BTreeMap<String, Bytes>,
}

impl CatalogBatch {
    pub fn insert(
        &mut self,
        scope: impl Into<String>,
        name: impl Into<String>,
        value: DiscoveryValue,
    ) {
        let scope = scope.into();
        let name = name.into();
        self.names.insert((scope.clone(), name.clone()));
        self.values.insert((scope, name, value));
    }

    pub fn insert_metadata(&mut self, name: impl Into<String>, value: Bytes) {
        self.metadata.insert(name.into(), value);
    }

    pub fn len(&self) -> usize {
        self.names.len() + self.values.len() + self.metadata.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn estimated_size(&self) -> usize {
        let names = self
            .names
            .iter()
            .map(|(scope, name)| scope.len() + name.len() + 2)
            .sum::<usize>();
        let values = self
            .values
            .iter()
            .map(|(scope, name, value)| scope.len() + name.len() + value.estimated_size() + 2)
            .sum::<usize>();
        let metadata = self
            .metadata
            .iter()
            .map(|(name, value)| name.len() + value.len() + 1)
            .sum::<usize>();
        names + values + metadata
    }

    pub fn into_ops(self, partition_prefix: &[u8], ttl: Ttl) -> Vec<RecordOp> {
        let term_count = self.len();
        let estimated_bytes = self.estimated_size();
        let options = PutOptions { ttl };
        let mut ops = Vec::with_capacity(term_count);
        ops.extend(
            self.names
                .into_iter()
                .map(|(scope, name)| put_empty(name_key(partition_prefix, &scope, &name), options)),
        );
        ops.extend(self.values.into_iter().map(|(scope, name, value)| {
            put_empty(value_key(partition_prefix, &scope, &name, &value), options)
        }));
        ops.extend(self.metadata.into_iter().map(|(name, value)| {
            RecordOp::Put(PutRecordOp::new_with_options(
                Record::new(metadata_key(partition_prefix, &name), value),
                options,
            ))
        }));
        metrics::counter!("telemetry_discovery_catalog_terms_written_total")
            .increment(term_count as u64);
        metrics::histogram!("telemetry_discovery_catalog_batch_estimated_bytes")
            .record(estimated_bytes as f64);
        ops
    }
}

pub async fn names(
    storage: &dyn StorageRead,
    partition_prefix: &[u8],
    scope: Option<&str>,
) -> StorageResult<Vec<String>> {
    let started = std::time::Instant::now();
    let prefix = name_prefix(partition_prefix, scope);
    let mut iter = storage
        .scan_prefix_iter(prefix.clone(), BytesRange::unbounded(), None)
        .await?;
    let mut names = BTreeSet::new();
    while let Some(record) = iter.next().await? {
        let mut suffix = &record.key[prefix.len()..];
        if scope.is_none() {
            terminated_bytes::deserialize(&mut suffix)
                .map_err(|error| corrupt(error.to_string()))?;
        }
        let name = terminated_bytes::deserialize(&mut suffix)
            .map_err(|error| corrupt(error.to_string()))?;
        if !suffix.is_empty() {
            return Err(corrupt("discovery name key has a trailing suffix"));
        }
        names.insert(String::from_utf8(name.to_vec()).map_err(|error| corrupt(error.to_string()))?);
    }
    let names: Vec<_> = names.into_iter().collect();
    record_scan("names", names.len(), started);
    Ok(names)
}

pub async fn values(
    storage: &dyn StorageRead,
    partition_prefix: &[u8],
    scope: &str,
    name: &str,
) -> StorageResult<Vec<DiscoveryValue>> {
    let started = std::time::Instant::now();
    let prefix = value_prefix(partition_prefix, scope, name);
    let mut iter = storage
        .scan_prefix_iter(prefix.clone(), BytesRange::unbounded(), None)
        .await?;
    let mut values = BTreeSet::new();
    while let Some(record) = iter.next().await? {
        let mut suffix = &record.key[prefix.len()..];
        let value = DiscoveryValue::decode(&mut suffix)?;
        if !suffix.is_empty() {
            return Err(corrupt("discovery value key has a trailing suffix"));
        }
        values.insert(value);
    }
    let values: Vec<_> = values.into_iter().collect();
    record_scan("values", values.len(), started);
    Ok(values)
}

pub async fn metadata(
    storage: &dyn StorageRead,
    partition_prefix: &[u8],
) -> StorageResult<Vec<(String, Bytes)>> {
    let started = std::time::Instant::now();
    let prefix = record_prefix(partition_prefix, METADATA_RECORD).freeze();
    let mut iter = storage
        .scan_prefix_iter(prefix.clone(), BytesRange::unbounded(), None)
        .await?;
    let mut metadata = BTreeMap::new();
    while let Some(record) = iter.next().await? {
        let mut suffix = &record.key[prefix.len()..];
        let name = terminated_bytes::deserialize(&mut suffix)
            .map_err(|error| corrupt(error.to_string()))?;
        if !suffix.is_empty() {
            return Err(corrupt("discovery metadata key has a trailing suffix"));
        }
        metadata.insert(
            String::from_utf8(name.to_vec()).map_err(|error| corrupt(error.to_string()))?,
            record.value,
        );
    }
    let metadata: Vec<_> = metadata.into_iter().collect();
    record_scan("metadata", metadata.len(), started);
    Ok(metadata)
}

fn name_prefix(partition_prefix: &[u8], scope: Option<&str>) -> Bytes {
    let mut prefix = record_prefix(partition_prefix, NAME_RECORD);
    if let Some(scope) = scope {
        terminated_bytes::serialize(scope.as_bytes(), &mut prefix);
    }
    prefix.freeze()
}

fn value_prefix(partition_prefix: &[u8], scope: &str, name: &str) -> Bytes {
    let mut prefix = record_prefix(partition_prefix, VALUE_RECORD);
    terminated_bytes::serialize(scope.as_bytes(), &mut prefix);
    terminated_bytes::serialize(name.as_bytes(), &mut prefix);
    prefix.freeze()
}

fn name_key(partition_prefix: &[u8], scope: &str, name: &str) -> Bytes {
    let mut key = BytesMut::from(name_prefix(partition_prefix, None).as_ref());
    terminated_bytes::serialize(scope.as_bytes(), &mut key);
    terminated_bytes::serialize(name.as_bytes(), &mut key);
    key.freeze()
}

fn value_key(partition_prefix: &[u8], scope: &str, name: &str, value: &DiscoveryValue) -> Bytes {
    let mut key = BytesMut::from(value_prefix(partition_prefix, scope, name).as_ref());
    value.encode(&mut key);
    key.freeze()
}

fn metadata_key(partition_prefix: &[u8], name: &str) -> Bytes {
    let mut key = record_prefix(partition_prefix, METADATA_RECORD);
    terminated_bytes::serialize(name.as_bytes(), &mut key);
    key.freeze()
}

fn record_prefix(partition_prefix: &[u8], record: u8) -> BytesMut {
    let mut prefix = BytesMut::with_capacity(partition_prefix.len() + 4);
    prefix.extend_from_slice(partition_prefix);
    prefix.put_u16(CATALOG_SLOT);
    prefix.put_u8(CATALOG_FORMAT_VERSION);
    prefix.put_u8(record);
    prefix
}

fn put_empty(key: Bytes, options: PutOptions) -> RecordOp {
    RecordOp::Put(PutRecordOp::new_with_options(Record::empty(key), options))
}

fn corrupt(message: impl Into<String>) -> StorageError {
    StorageError::Internal(message.into())
}

fn record_scan(kind: &'static str, records: usize, started: std::time::Instant) {
    metrics::counter!(
        "telemetry_discovery_catalog_records_scanned_total",
        "kind" => kind
    )
    .increment(records as u64);
    metrics::histogram!(
        "telemetry_discovery_catalog_scan_duration_seconds",
        "kind" => kind
    )
    .record(started.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::Storage;
    use crate::storage::in_memory::{Clock, InMemoryStorage};

    struct TestClock(AtomicI64);

    impl Clock for TestClock {
        fn now(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[tokio::test]
    async fn catalog_round_trips_and_deduplicates_terms() {
        let storage = InMemoryStorage::new();
        let prefix = b"tenant/partition";
        let mut batch = CatalogBatch::default();
        batch.insert("", "job", DiscoveryValue::String("api".into()));
        batch.insert("", "job", DiscoveryValue::String("api".into()));
        batch.insert("", "job", DiscoveryValue::String("worker".into()));
        batch.insert("span", "error", DiscoveryValue::Bool(true));
        batch.insert("span", "latency", DiscoveryValue::Int(-7));
        batch.insert("span", "ratio", DiscoveryValue::Double(0.5));
        batch.insert_metadata("up", Bytes::from_static(b"gauge"));
        storage
            .apply(batch.into_ops(prefix, Ttl::NoExpiry))
            .await
            .unwrap();

        assert_eq!(
            names(&storage, prefix, Some("")).await.unwrap(),
            vec!["job"]
        );
        assert_eq!(
            values(&storage, prefix, "", "job").await.unwrap(),
            vec![
                DiscoveryValue::String("api".into()),
                DiscoveryValue::String("worker".into())
            ]
        );
        assert_eq!(
            names(&storage, prefix, Some("span")).await.unwrap(),
            vec!["error", "latency", "ratio"]
        );
        assert_eq!(
            metadata(&storage, prefix).await.unwrap(),
            vec![("up".to_owned(), Bytes::from_static(b"gauge"))]
        );
    }

    #[test]
    fn catalog_slot_is_outside_the_routing_space() {
        let slot = std::hint::black_box(CATALOG_SLOT);
        assert!(slot > 4095);
    }

    #[test]
    fn catalog_keys_include_the_independent_format_version() {
        let partition = b"tenant/partition";
        let prefix = record_prefix(partition, NAME_RECORD).freeze();

        assert_eq!(
            &prefix[partition.len()..],
            &[
                (CATALOG_SLOT >> 8) as u8,
                CATALOG_SLOT as u8,
                CATALOG_FORMAT_VERSION,
                NAME_RECORD,
            ]
        );
    }

    #[tokio::test]
    async fn catalog_records_preserve_partition_ttl() {
        let clock = Arc::new(TestClock(AtomicI64::new(100)));
        let storage = InMemoryStorage::new().with_clock(clock.clone());
        let prefix = b"expiring/partition";
        let mut batch = CatalogBatch::default();
        batch.insert("", "job", DiscoveryValue::String("api".into()));
        storage
            .apply(batch.into_ops(prefix, Ttl::ExpireAfter(10)))
            .await
            .unwrap();
        assert_eq!(
            names(&storage, prefix, Some("")).await.unwrap(),
            vec!["job"]
        );

        clock.0.store(110, Ordering::Relaxed);
        assert!(names(&storage, prefix, Some("")).await.unwrap().is_empty());
    }

    #[test]
    fn discovery_cache_expires_only_active_partitions() {
        let cache = DiscoveryCache::new(2, Duration::ZERO);
        cache.insert("closed", vec!["stable"], false);
        cache.insert("active", vec!["refresh"], true);

        assert_eq!(cache.get(&"closed").unwrap().as_slice(), &["stable"]);
        assert!(cache.get(&"active").is_none());
    }
}
