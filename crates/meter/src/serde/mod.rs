pub mod dictionary;
pub mod forward_index;
pub mod inverted_index;
pub mod key;
pub mod timeseries;

use crate::Namespace;
use crate::model::{BucketSize, BucketStart, TimeBucket};
use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::{KEY_PREFIX_LEN, KeyPrefix};
use std::ops::Bound;

// Re-export encoding utilities from common
pub use common::serde::encoding::{
    EncodingError, decode_optional_utf8, decode_utf8, encode_optional_utf8, encode_utf8,
};

/// Trait for types that can be encoded to bytes
pub trait Encode {
    fn encode(&self, buf: &mut BytesMut);
}

/// Trait for types that can be decoded from bytes
pub trait Decode: Sized {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError>;
}

/// Encode an array of encodable items
///
/// Format: `count: u16` (little-endian) + `count` serialized elements
pub fn encode_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    let count = items.len();
    if count > u16::MAX as usize {
        panic!("Array too long: {} items", count);
    }
    buf.extend_from_slice(&(count as u16).to_le_bytes());
    for item in items {
        item.encode(buf);
    }
}

/// Decode an array of decodable items
///
/// Format: `count: u16` (little-endian) + `count` serialized elements
pub fn decode_array<T: Decode>(buf: &mut &[u8]) -> Result<Vec<T>, EncodingError> {
    if buf.len() < 2 {
        return Err(EncodingError {
            message: "Buffer too short for array count".to_string(),
        });
    }
    let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];

    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf)?);
    }
    Ok(items)
}

/// Encode a fixed-element array (no count prefix)
///
/// Format: Serialized elements back-to-back with no additional padding
pub fn encode_fixed_element_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    for item in items {
        item.encode(buf);
    }
}

/// Decode a fixed-element array (no count prefix)
///
/// The number of elements is computed by dividing the buffer length by the element size.
/// This function validates that the buffer length is divisible by the element size.
pub fn decode_fixed_element_array<T: Decode>(
    buf: &mut &[u8],
    element_size: usize,
) -> Result<Vec<T>, EncodingError> {
    if !buf.len().is_multiple_of(element_size) {
        return Err(EncodingError {
            message: format!(
                "Buffer length {} is not divisible by element size {}",
                buf.len(),
                element_size
            ),
        });
    }

    let count = buf.len() / element_size;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf)?);
    }
    Ok(items)
}

/// Key format version.
pub const KEY_VERSION: u8 = 0x03;

/// Subsystem byte for timeseries storage (see [`common::serde::subsystem`]).
pub const SUBSYSTEM: u8 = common::serde::subsystem::TIMESERIES;

pub(crate) fn write_bucket_prefix(buf: &mut BytesMut, namespace: &Namespace, bucket: &TimeBucket) {
    assert!(bucket.size != 0, "bucket_size 0 is reserved");
    KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(buf);
    common::serde::terminated_bytes::serialize(namespace.as_bytes(), buf);
    buf.put_u32(bucket.start);
    buf.put_u8(bucket.size);
}

/// Range containing every record of one bucket.
pub(crate) fn bucket_records_range(namespace: &Namespace, bucket: &TimeBucket) -> BytesRange {
    let mut prefix = BytesMut::new();
    write_bucket_prefix(&mut prefix, namespace, bucket);
    BytesRange::prefix(prefix.freeze())
}

/// Minimum header length, including the terminated namespace.
pub const MIN_PREFIX_AND_RECORD_TYPE_LEN: usize = KEY_PREFIX_LEN + 1 + 4 + 1 + 1;

/// Record type enumeration for timeseries storage.
///
/// Encoded as the single byte after the bucket header in every bucket-scoped key.
/// `0x01` is reserved (formerly `BucketList`, now superseded by SlateDB
/// segments).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    SeriesDictionary = 0x02,
    ForwardIndex = 0x03,
    InvertedIndex = 0x04,
    TimeSeries = 0x05,
}

impl RecordType {
    /// Returns the record type ID.
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a record type ID back to a RecordType.
    pub fn from_id(id: u8) -> Result<Self, EncodingError> {
        match id {
            0x02 => Ok(RecordType::SeriesDictionary),
            0x03 => Ok(RecordType::ForwardIndex),
            0x04 => Ok(RecordType::InvertedIndex),
            0x05 => Ok(RecordType::TimeSeries),
            _ => Err(EncodingError {
                message: format!("invalid record type: 0x{:02x}", id),
            }),
        }
    }
}

/// Writes the bucket-scoped header `[subsystem, version, namespace\0,
/// time_bucket(4 BE), bucket_size, record_type]` to `buf`.
///
/// `bucket.size == 0` is reserved and panics.
pub fn write_record_prefix(
    buf: &mut BytesMut,
    namespace: &Namespace,
    bucket: &TimeBucket,
    record_type: RecordType,
) {
    write_bucket_prefix(buf, namespace, bucket);
    buf.put_u8(record_type.id());
}

/// Range containing all index metadata records for one bucket, while
/// excluding time-series sample records.
pub(crate) fn bucket_metadata_range(namespace: &Namespace, bucket: &TimeBucket) -> BytesRange {
    let mut start = BytesMut::new();
    write_record_prefix(&mut start, namespace, bucket, RecordType::SeriesDictionary);
    let mut end = BytesMut::new();
    write_record_prefix(&mut end, namespace, bucket, RecordType::TimeSeries);
    BytesRange::new(
        Bound::Included(start.freeze()),
        Bound::Excluded(end.freeze()),
    )
}

/// Reads the bucket-scoped header from `buf`, validating subsystem, version,
/// `bucket_size`, and the record type ID. Returns the parsed `TimeBucket` and
/// `RecordType`.
pub fn parse_record_prefix(
    buf: &[u8],
) -> Result<(Namespace, TimeBucket, RecordType, usize), EncodingError> {
    KeyPrefix::from_bytes_with_validation(buf, SUBSYSTEM, KEY_VERSION)?;
    if buf.len() < MIN_PREFIX_AND_RECORD_TYPE_LEN {
        return Err(EncodingError {
            message: "Buffer too short for namespace-scoped bucket header".to_string(),
        });
    }
    let mut suffix = &buf[KEY_PREFIX_LEN..];
    let namespace_bytes = common::serde::terminated_bytes::deserialize(&mut suffix)?;
    let namespace = Namespace::new(String::from_utf8(namespace_bytes.to_vec()).map_err(
        |error| EncodingError {
            message: format!("Invalid namespace UTF-8: {error}"),
        },
    )?)
    .map_err(|error| EncodingError {
        message: error.to_string(),
    })?;
    let offset = buf.len() - suffix.len();
    if suffix.len() < 6 {
        return Err(EncodingError {
            message: "Buffer too short for bucket fields".to_string(),
        });
    }
    let start = BucketStart::from_be_bytes([suffix[0], suffix[1], suffix[2], suffix[3]]);
    let size: BucketSize = suffix[4];
    if size == 0 {
        return Err(EncodingError {
            message: "bucket_size 0 is reserved".to_string(),
        });
    }
    let record_type = RecordType::from_id(suffix[5])?;
    Ok((
        namespace,
        TimeBucket { start, size },
        record_type,
        offset + 6,
    ))
}

/// Trait for record keys that have a record type
pub trait RecordKey {
    const RECORD_TYPE: RecordType;
}

/// Trait for record keys that are scoped to a specific time bucket.
/// Provides methods to create scan ranges and decode bucket prefixes.
pub trait TimeBucketScoped: RecordKey {
    fn namespace(&self) -> &Namespace;
    /// Returns the time bucket for this record
    fn bucket(&self) -> TimeBucket;

    /// Decodes and validates the bucket-scoped header of a key.
    /// Returns the `TimeBucket` when the encoded record type matches
    /// `Self::RECORD_TYPE`.
    fn decode_bucket_prefix(bytes: &[u8]) -> Result<(Namespace, TimeBucket), EncodingError> {
        let (namespace, bucket, record_type, _) = parse_record_prefix(bytes)?;
        if record_type != Self::RECORD_TYPE {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected {:?}, got {:?}",
                    Self::RECORD_TYPE,
                    record_type
                ),
            });
        }
        Ok((namespace, bucket))
    }

    /// Key prefix shared by all records of this type in the given time bucket.
    fn bucket_prefix(namespace: &Namespace, bucket: &TimeBucket) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(&mut buf, namespace, bucket, Self::RECORD_TYPE);
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_bucket_scoped_header() {
        // given
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };
        let namespace = Namespace::new("tenant-a").unwrap();
        let mut buf = BytesMut::new();

        // when
        write_record_prefix(&mut buf, &namespace, &bucket, RecordType::TimeSeries);
        let (decoded_namespace, decoded_bucket, decoded_type, _) =
            parse_record_prefix(&buf).unwrap();

        // then
        assert_eq!(decoded_namespace, namespace);
        assert_eq!(decoded_bucket, bucket);
        assert_eq!(decoded_type, RecordType::TimeSeries);
    }

    #[test]
    #[should_panic(expected = "bucket_size 0 is reserved")]
    fn should_panic_when_writing_zero_bucket_size() {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &Namespace::default(),
            &TimeBucket { start: 0, size: 0 },
            RecordType::TimeSeries,
        );
    }

    #[test]
    fn parse_time_bucket_and_record_type_rejects_zero_bucket_size() {
        let mut buf = BytesMut::new();
        KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(&mut buf);
        common::serde::terminated_bytes::serialize(Namespace::default().as_bytes(), &mut buf);
        buf.put_u32(0);
        buf.put_u8(0);
        buf.put_u8(RecordType::TimeSeries.id());

        let err = parse_record_prefix(&buf).unwrap_err();

        assert!(err.message.contains("bucket_size 0 is reserved"));
    }

    #[test]
    fn parse_time_bucket_and_record_type_rejects_short_buffer() {
        let buf = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 0, 1];
        let err = parse_record_prefix(&buf).unwrap_err();
        assert!(err.message.contains("Buffer too short"));
    }

    #[test]
    fn bucket_metadata_range_excludes_samples_and_adjacent_buckets() {
        let namespace = Namespace::new("tenant-a").unwrap();
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };
        let range = bucket_metadata_range(&namespace, &bucket);
        for record_type in [
            RecordType::SeriesDictionary,
            RecordType::ForwardIndex,
            RecordType::InvertedIndex,
        ] {
            let mut key = BytesMut::new();
            write_record_prefix(&mut key, &namespace, &bucket, record_type);
            key.put_u8(1);
            assert!(range.contains(&key));
        }

        let mut samples = BytesMut::new();
        write_record_prefix(&mut samples, &namespace, &bucket, RecordType::TimeSeries);
        assert!(!range.contains(&samples));

        let mut adjacent = BytesMut::new();
        write_record_prefix(
            &mut adjacent,
            &namespace,
            &TimeBucket {
                start: 12346,
                size: 1,
            },
            RecordType::SeriesDictionary,
        );
        assert!(!range.contains(&adjacent));
    }
}
