//! Meter adapter for the shared durable discovery catalog.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use common::discovery::{self, DiscoveryCache, DiscoveryValue};
use common::storage::{Record, StorageError, StorageIterator, StorageResult};

use crate::Namespace;
use crate::model::{MetricMetadata, MetricType, Temporality, TimeBucket};
use crate::serde::{Decode, Encode, EncodingError};
use crate::storage::StorageRead as MeterStorageRead;

const CATALOG_SCOPE: &str = "";

pub(crate) struct MeterDiscoveryCache {
    names: DiscoveryCache<(Namespace, TimeBucket), Vec<String>>,
    values: DiscoveryCache<(Namespace, TimeBucket, String), Vec<String>>,
    metadata: DiscoveryCache<(Namespace, TimeBucket), Vec<MetricMetadata>>,
}

impl MeterDiscoveryCache {
    pub(crate) fn new() -> Self {
        Self {
            names: DiscoveryCache::new(1_024, Duration::from_secs(5)),
            values: DiscoveryCache::new(4_096, Duration::from_secs(5)),
            metadata: DiscoveryCache::new(1_024, Duration::from_secs(5)),
        }
    }

    pub(crate) fn clear(&self) {
        self.names.clear();
        self.values.clear();
        self.metadata.clear();
    }
}

pub(crate) fn partition_prefix(namespace: &Namespace, bucket: TimeBucket) -> Bytes {
    let mut out = BytesMut::new();
    crate::serde::write_bucket_prefix(&mut out, namespace, &bucket);
    out.freeze()
}

struct CatalogStorage<T>(T);

struct CatalogIterator(slatedb::DbIterator);

#[async_trait]
impl StorageIterator for CatalogIterator {
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        self.0
            .next()
            .await
            .map(|entry| entry.map(|entry| Record::new(entry.key, entry.value)))
            .map_err(|error| StorageError::Storage(error.to_string()))
    }
}

#[async_trait]
impl<T> common::StorageRead for CatalogStorage<T>
where
    T: MeterStorageRead + Clone + Send + Sync + 'static,
{
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        MeterStorageRead::get(&self.0, key.clone())
            .await
            .map(|value| value.map(|value| Record::new(key, value)))
    }

    async fn scan_iter(
        &self,
        range: common::BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = MeterStorageRead::scan(&self.0, range).await?;
        Ok(Box::new(CatalogIterator(iter)))
    }
}

pub(crate) async fn names<T>(
    storage: T,
    namespace: &Namespace,
    buckets: &[TimeBucket],
    cache: &MeterDiscoveryCache,
) -> StorageResult<Vec<String>>
where
    T: MeterStorageRead + Clone + Send + Sync + 'static,
{
    let storage = CatalogStorage(storage);
    let mut found = BTreeSet::new();
    for bucket in buckets {
        let key = (namespace.clone(), *bucket);
        let names = if let Some(names) = cache.names.get(&key) {
            names
        } else {
            let names = discovery::names(
                &storage,
                &partition_prefix(namespace, *bucket),
                Some(CATALOG_SCOPE),
            )
            .await?;
            cache.names.insert(key, names, is_active_bucket(*bucket))
        };
        found.extend(names.iter().cloned());
    }
    Ok(found.into_iter().collect())
}

pub(crate) async fn values<T>(
    storage: T,
    namespace: &Namespace,
    buckets: &[TimeBucket],
    name: &str,
    cache: &MeterDiscoveryCache,
) -> StorageResult<Vec<String>>
where
    T: MeterStorageRead + Clone + Send + Sync + 'static,
{
    let storage = CatalogStorage(storage);
    let mut found = BTreeSet::new();
    for bucket in buckets {
        let key = (namespace.clone(), *bucket, name.to_owned());
        let values = if let Some(values) = cache.values.get(&key) {
            values
        } else {
            let mut strings = Vec::new();
            for value in discovery::values(
                &storage,
                &partition_prefix(namespace, *bucket),
                CATALOG_SCOPE,
                name,
            )
            .await?
            {
                if let DiscoveryValue::String(value) = value {
                    strings.push(value);
                }
            }
            cache.values.insert(key, strings, is_active_bucket(*bucket))
        };
        found.extend(values.iter().cloned());
    }
    Ok(found.into_iter().collect())
}

pub(crate) async fn metadata<T>(
    storage: T,
    namespace: &Namespace,
    buckets: &[TimeBucket],
    metric: Option<&str>,
    cache: &MeterDiscoveryCache,
) -> StorageResult<Vec<MetricMetadata>>
where
    T: MeterStorageRead + Clone + Send + Sync + 'static,
{
    let storage = CatalogStorage(storage);
    let mut found = Vec::new();
    for bucket in buckets {
        let key = (namespace.clone(), *bucket);
        let entries = if let Some(entries) = cache.metadata.get(&key) {
            entries
        } else {
            let mut entries = Vec::new();
            for (name, value) in
                discovery::metadata(&storage, &partition_prefix(namespace, *bucket)).await?
            {
                entries.push(
                    decode_metadata(&name, &value)
                        .map_err(|error| StorageError::Internal(error.to_string()))?,
                );
            }
            cache
                .metadata
                .insert(key, entries, is_active_bucket(*bucket))
        };
        for entry in entries.iter() {
            if metric.is_some_and(|filter| filter != entry.metric_name) {
                continue;
            }
            if !found.contains(entry) {
                found.push(entry.clone());
            }
        }
    }
    found.sort_by(|left, right| left.metric_name.cmp(&right.metric_name));
    Ok(found)
}

fn is_active_bucket(bucket: TimeBucket) -> bool {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX);
    let start_secs = i64::from(bucket.start) * 60;
    let end_secs = start_secs.saturating_add(i64::from(bucket.size) * 60 * 60);
    now_secs >= start_secs && now_secs < end_secs
}

pub(crate) fn encode_metadata(metadata: &MetricMetadata) -> Bytes {
    let mut out = BytesMut::new();
    crate::serde::forward_index::MetricMeta::from(metadata.metric_type).encode(&mut out);
    crate::serde::encode_optional_utf8(metadata.description.as_deref(), &mut out);
    crate::serde::encode_optional_utf8(metadata.unit.as_deref(), &mut out);
    out.freeze()
}

pub(crate) fn decode_metadata(
    metric_name: &str,
    bytes: &[u8],
) -> Result<MetricMetadata, EncodingError> {
    let mut input = bytes;
    let encoded = crate::serde::forward_index::MetricMeta::decode(&mut input)?;
    let temporality = match encoded.temporality() {
        0 => Temporality::Unspecified,
        1 => Temporality::Cumulative,
        2 => Temporality::Delta,
        value => {
            return Err(EncodingError {
                message: format!("invalid metadata temporality {value}"),
            });
        }
    };
    let metric_type = match encoded.metric_type {
        0 => None,
        1 => Some(MetricType::Gauge),
        2 => Some(MetricType::Sum {
            monotonic: encoded.monotonic(),
            temporality,
        }),
        3 => Some(MetricType::Histogram { temporality }),
        4 => Some(MetricType::ExponentialHistogram { temporality }),
        5 => Some(MetricType::Summary),
        value => {
            return Err(EncodingError {
                message: format!("invalid metadata metric type {value}"),
            });
        }
    };
    let description = crate::serde::decode_optional_utf8(&mut input)?;
    let unit = crate::serde::decode_optional_utf8(&mut input)?;
    if !input.is_empty() {
        return Err(EncodingError {
            message: "metadata value has a trailing suffix".to_owned(),
        });
    }
    Ok(MetricMetadata {
        metric_name: metric_name.to_owned(),
        metric_type,
        description,
        unit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_codec_round_trips_descriptions() {
        let metadata = MetricMetadata {
            metric_name: "requests_total".to_owned(),
            metric_type: Some(MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            }),
            description: Some("Total requests".to_owned()),
            unit: Some("requests".to_owned()),
        };

        let encoded = encode_metadata(&metadata);
        assert_eq!(
            decode_metadata(&metadata.metric_name, &encoded).unwrap(),
            metadata
        );
    }

    #[test]
    fn partition_prefix_stops_before_catalog_slot() {
        let namespace = Namespace::new("tenant").unwrap();
        let bucket = TimeBucket {
            start: 12_345,
            size: 2,
        };
        let prefix = partition_prefix(&namespace, bucket);

        assert_eq!(prefix[0], crate::serde::SUBSYSTEM);
        assert_eq!(prefix[1], crate::serde::KEY_VERSION);
        assert_eq!(prefix.len(), 2 + "tenant".len() + 1 + 4 + 1);
    }
}
