// Key structures with big-endian encoding

use super::*;
use crate::Namespace;
use crate::model::{SeriesFingerprint, SeriesId, TimeBucket};
use bytes::{Bytes, BytesMut};
use common::serde::terminated_bytes;

/// SeriesDictionary key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDictionaryKey {
    pub namespace: Namespace,
    pub bucket: TimeBucket,
    pub series_fingerprint: SeriesFingerprint,
}

impl SeriesDictionaryKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &self.namespace,
            &self.bucket,
            RecordType::SeriesDictionary,
        );
        buf.extend_from_slice(&self.series_fingerprint.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let (namespace, bucket, record_type, offset) = parse_record_prefix(buf)?;
        if record_type != RecordType::SeriesDictionary {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected SeriesDictionary, got {:?}",
                    record_type
                ),
            });
        }
        let suffix = &buf[offset..];
        if suffix.len() < 16 {
            return Err(EncodingError {
                message: "Buffer too short for SeriesDictionaryKey".to_string(),
            });
        }
        let series_fingerprint = u128::from_be_bytes(suffix[..16].try_into().unwrap());

        Ok(SeriesDictionaryKey {
            namespace,
            bucket,
            series_fingerprint,
        })
    }
}

impl RecordKey for SeriesDictionaryKey {
    const RECORD_TYPE: RecordType = RecordType::SeriesDictionary;
}

impl TimeBucketScoped for SeriesDictionaryKey {
    fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    fn bucket(&self) -> TimeBucket {
        self.bucket
    }
}

/// ForwardIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardIndexKey {
    pub namespace: Namespace,
    pub bucket: TimeBucket,
    pub series_id: SeriesId,
}

impl ForwardIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &self.namespace,
            &self.bucket,
            RecordType::ForwardIndex,
        );
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let (namespace, bucket, record_type, offset) = parse_record_prefix(buf)?;
        if record_type != RecordType::ForwardIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected ForwardIndex, got {:?}",
                    record_type
                ),
            });
        }
        let suffix = &buf[offset..];
        if suffix.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for ForwardIndexKey".to_string(),
            });
        }
        let series_id = u32::from_be_bytes(suffix[..4].try_into().unwrap());

        Ok(ForwardIndexKey {
            namespace,
            bucket,
            series_id,
        })
    }
}

impl RecordKey for ForwardIndexKey {
    const RECORD_TYPE: RecordType = RecordType::ForwardIndex;
}

impl TimeBucketScoped for ForwardIndexKey {
    fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    fn bucket(&self) -> TimeBucket {
        self.bucket
    }
}

/// InvertedIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvertedIndexKey {
    pub namespace: Namespace,
    pub bucket: TimeBucket,
    pub attribute: String,
    pub value: String,
}

impl InvertedIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &self.namespace,
            &self.bucket,
            RecordType::InvertedIndex,
        );
        // Attribute uses terminated encoding to delimit from value
        terminated_bytes::serialize(self.attribute.as_bytes(), &mut buf);
        // Value is raw UTF-8 (no terminator needed - end of key acts as delimiter)
        buf.extend_from_slice(self.value.as_bytes());
        buf.freeze()
    }

    /// Key prefix shared by all entries for a specific attribute (label name)
    /// within a given bucket. This allows efficient scanning for all values of a label.
    pub fn attribute_prefix(namespace: &Namespace, bucket: &TimeBucket, attribute: &str) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(&mut buf, namespace, bucket, RecordType::InvertedIndex);
        terminated_bytes::serialize(attribute.as_bytes(), &mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let (namespace, bucket, record_type, offset) = parse_record_prefix(buf)?;
        if record_type != RecordType::InvertedIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected InvertedIndex, got {:?}",
                    record_type
                ),
            });
        }
        let mut slice = &buf[offset..];

        // Attribute uses terminated encoding
        let attribute_bytes = terminated_bytes::deserialize(&mut slice)?;
        let attribute = String::from_utf8(attribute_bytes.to_vec()).map_err(|e| EncodingError {
            message: format!("Invalid UTF-8 in attribute: {}", e),
        })?;

        // Value is the remaining bytes (raw UTF-8, no terminator)
        let value = String::from_utf8(slice.to_vec()).map_err(|e| EncodingError {
            message: format!("Invalid UTF-8 in value: {}", e),
        })?;

        Ok(InvertedIndexKey {
            namespace,
            bucket,
            attribute,
            value,
        })
    }
}

impl RecordKey for InvertedIndexKey {
    const RECORD_TYPE: RecordType = RecordType::InvertedIndex;
}

impl TimeBucketScoped for InvertedIndexKey {
    fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    fn bucket(&self) -> TimeBucket {
        self.bucket
    }
}

/// TimeSeries key — metric-name-prefixed layout.
///
/// Physical key: `<prefix, bucket, metric_name, series_id>`
///
/// The metric name is encoded with a terminated-bytes delimiter so that keys
/// sort by `(bucket, metric_name, series_id)`. This groups all sample records
/// for the same metric together in storage, improving locality for single-metric
/// PromQL range queries on the cold path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesKey {
    pub namespace: Namespace,
    pub bucket: TimeBucket,
    pub metric_name: String,
    pub series_id: SeriesId,
}

impl TimeSeriesKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &self.namespace,
            &self.bucket,
            RecordType::TimeSeries,
        );
        terminated_bytes::serialize(self.metric_name.as_bytes(), &mut buf);
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let (namespace, bucket, record_type, offset) = parse_record_prefix(buf)?;
        if record_type != RecordType::TimeSeries {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected TimeSeries, got {:?}",
                    record_type
                ),
            });
        }

        let mut slice = &buf[offset..];
        let metric_name_bytes = terminated_bytes::deserialize(&mut slice)?;
        let metric_name =
            String::from_utf8(metric_name_bytes.to_vec()).map_err(|e| EncodingError {
                message: format!("Invalid UTF-8 in metric_name: {}", e),
            })?;

        if slice.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for series_id in TimeSeriesKey".to_string(),
            });
        }
        let series_id = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);

        Ok(TimeSeriesKey {
            namespace,
            bucket,
            metric_name,
            series_id,
        })
    }
}

impl RecordKey for TimeSeriesKey {
    const RECORD_TYPE: RecordType = RecordType::TimeSeries;
}

impl TimeBucketScoped for TimeSeriesKey {
    fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    fn bucket(&self) -> TimeBucket {
        self.bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_series_dictionary_key() {
        // given
        let key = SeriesDictionaryKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 2,
            },
            series_fingerprint: 67890,
        };

        // when
        let encoded = key.encode();
        let decoded = SeriesDictionaryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_record_type_directly_after_bucket_header() {
        let key = ForwardIndexKey {
            namespace: Namespace::new("tenant").unwrap(),
            bucket: TimeBucket {
                start: 12345,
                size: 1,
            },
            series_id: 7,
        };

        let encoded = key.encode();
        let namespace_end = encoded[2..].iter().position(|byte| *byte == 0).unwrap() + 2;
        let record_type_offset = namespace_end + 1 + 4 + 1;

        assert_eq!(encoded[record_type_offset], RecordType::ForwardIndex.id());
        assert_eq!(ForwardIndexKey::decode(&encoded).unwrap(), key);
    }

    #[test]
    fn should_encode_and_decode_forward_index_key() {
        // given
        let key = ForwardIndexKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 3,
            },
            series_id: 42,
        };

        // when
        let encoded = key.encode();
        let decoded = ForwardIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_inverted_index_key() {
        // given
        let key = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 1,
            },
            attribute: "host".to_string(),
            value: "server1".to_string(),
        };

        // when
        let encoded = key.encode();
        let decoded = InvertedIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_time_series_key() {
        // given
        let key = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 4,
            },
            metric_name: "http_requests_total".to_string(),
            series_id: 99,
        };

        // when
        let encoded = key.encode();
        let decoded = TimeSeriesKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_time_series_key_with_empty_metric_name() {
        let key = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 1,
            },
            metric_name: "".to_string(),
            series_id: 7,
        };

        let encoded = key.encode();
        let decoded = TimeSeriesKey::decode(&encoded).unwrap();

        assert_eq!(decoded, key);
    }

    #[test]
    fn should_sort_time_series_keys_by_bucket_then_metric_then_series() {
        // Keys with the same bucket should sort by metric name, then series id
        let bucket_a = TimeBucket {
            start: 100,
            size: 1,
        };
        let bucket_d = TimeBucket {
            start: 200,
            size: 1,
        };
        let key_a = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: bucket_a,
            metric_name: "cpu_usage".to_string(),
            series_id: 2,
        };
        let key_b = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: bucket_a,
            metric_name: "cpu_usage".to_string(),
            series_id: 10,
        };
        let key_c = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: bucket_a,
            metric_name: "memory_usage".to_string(),
            series_id: 1,
        };
        let key_d = TimeSeriesKey {
            namespace: crate::Namespace::default(),
            bucket: bucket_d,
            metric_name: "cpu_usage".to_string(),
            series_id: 1,
        };

        let enc_a = key_a.encode();
        let enc_b = key_b.encode();
        let enc_c = key_c.encode();
        let enc_d = key_d.encode();

        // Same bucket, same metric: series_id ordering
        assert!(enc_a < enc_b, "cpu_usage/2 < cpu_usage/10");
        // Same bucket, different metric: alphabetical
        assert!(enc_b < enc_c, "cpu_usage < memory_usage");
        // Different bucket: bucket ordering dominates
        assert!(enc_c < enc_d, "bucket 100 < bucket 200");
    }

    #[test]
    fn should_create_attribute_prefix_that_matches_same_attribute_keys() {
        // given
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };
        let key1 = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket,
            attribute: "host".to_string(),
            value: "server1".to_string(),
        };
        let key2 = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket,
            attribute: "host".to_string(),
            value: "server2".to_string(),
        };
        let key3 = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket,
            attribute: "env".to_string(),
            value: "prod".to_string(),
        };

        // when
        let prefix =
            InvertedIndexKey::attribute_prefix(&crate::Namespace::default(), &bucket, "host");

        // then
        assert!(key1.encode().starts_with(&prefix));
        assert!(key2.encode().starts_with(&prefix));
        assert!(!key3.encode().starts_with(&prefix));
    }

    #[test]
    fn should_not_match_shorter_attribute_with_value_that_looks_like_suffix() {
        // given - searching for "hostname" should NOT match a key with
        // attribute "host" and value "name" even though "host" + "name" = "hostname"
        // The tuple-style delimiter encoding should prevent this collision.
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };
        let host_name_key = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket,
            attribute: "host".to_string(),
            value: "name".to_string(),
        };

        // when - search for "hostname"
        let prefix =
            InvertedIndexKey::attribute_prefix(&crate::Namespace::default(), &bucket, "hostname");

        // then - should NOT match the "host":"name" key
        assert!(
            !host_name_key.encode().starts_with(&prefix),
            "attribute_prefix for 'hostname' should not match key with attribute='host' value='name'. \
             The delimiter-based encoding should differentiate them."
        );
    }

    #[test]
    fn should_not_match_when_value_bytes_could_mimic_attribute_continuation() {
        // given - test a more contrived case where naive concatenation
        // might produce a collision
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };

        // Key with short attribute and value that concatenates to a different attribute
        let short_attr_key = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket,
            attribute: "ab".to_string(),
            value: "cdef".to_string(),
        };

        // Search for "abcdef"
        // If encoding was naive concatenation, "ab" + "cdef" might look like "abcdef"
        // But with tuple-style encoding, each element is delimited separately

        // when
        let prefix =
            InvertedIndexKey::attribute_prefix(&crate::Namespace::default(), &bucket, "abcdef");

        // then
        assert!(
            !short_attr_key.encode().starts_with(&prefix),
            "attribute_prefix for 'abcdef' should not match key with attribute='ab' value='cdef'"
        );
    }

    #[test]
    fn should_encode_attribute_with_terminator_and_value_without() {
        // This test demonstrates and verifies that:
        // - Only the attribute uses terminated encoding (with 0x00 delimiter)
        // - The value uses raw UTF-8 (no terminator, delimited by end of key)
        //
        // This is sufficient because the attribute terminator separates attribute
        // from value, and the value extends to the end of the key.

        let key = InvertedIndexKey {
            namespace: crate::Namespace::default(),
            bucket: TimeBucket {
                start: 12345,
                size: 1,
            },
            attribute: "host".to_string(),
            value: "server1".to_string(),
        };

        let encoded = key.encode();

        // Verify it round-trips correctly
        let decoded = InvertedIndexKey::decode(&encoded).unwrap();
        assert_eq!(decoded.attribute, "host");
        assert_eq!(decoded.value, "server1");

        // Verify the key ends with raw "server1" bytes (no trailing 0x00)
        let value_bytes = b"server1";
        assert!(
            encoded.ends_with(value_bytes),
            "Encoded key should end with raw value bytes (no terminator)"
        );

        // The encoded key should contain the 0x00 terminator after the attribute
        // but NOT after the value. We can verify by checking the byte before "server1"
        // is 0x00 (the attribute terminator).
        let value_start = encoded.len() - value_bytes.len();
        assert_eq!(
            encoded[value_start - 1],
            0x00,
            "Byte before value should be 0x00 (attribute terminator)"
        );
    }
}
