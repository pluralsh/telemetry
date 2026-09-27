// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use serde::{Deserialize, Serialize};

use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Error, Namespace, Result, SegmentId, TraceId,
};

pub(crate) const KEY_VERSION: u8 = 1;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::TRACE;
pub(crate) const METADATA_VERSION: u8 = 1;

/// Locator records deliberately live in one fixed routing segment per
/// namespace. This makes trace-by-ID a single-shard lookup after data pages are
/// time-sharded. The tradeoff is concentrated locator traffic; immutable
/// sequence-suffixed fragments avoid a hot read/modify/write key.
pub(crate) const LOCATOR_SEGMENT: SegmentId = i64::MIN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RecordType {
    NextPageSequence = 1,
    PageMetadata = 2,
    PagePayload = 3,
    TraceLocator = 4,
    AttributePosting = 5,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StoredPageMetadata {
    pub version: u8,
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub trace_count: u32,
    pub payload_bytes: u32,
}

impl StoredPageMetadata {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TraceLocator {
    pub version: u8,
    pub segment: SegmentId,
    pub page_sequence: u64,
    pub trace_index: u32,
    pub expires_at_unix_ms: Option<u64>,
}

impl TraceLocator {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }
}

pub(crate) fn segment_for(timestamp_ns: u64, segment_ns: u64) -> SegmentId {
    i64::try_from(timestamp_ns / segment_ns).unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(crate) fn segment_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    let mut bytes = BytesMut::new();
    write_scope(&mut bytes, namespace, segment);
    bytes.freeze()
}

pub(crate) fn next_sequence_key(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::NextPageSequence).freeze()
}

pub(crate) fn metadata_key(namespace: &Namespace, segment: SegmentId, sequence: u64) -> Bytes {
    sequence_key(namespace, segment, RecordType::PageMetadata, sequence)
}

pub(crate) fn payload_key(namespace: &Namespace, segment: SegmentId, sequence: u64) -> Bytes {
    sequence_key(namespace, segment, RecordType::PagePayload, sequence)
}

pub(crate) fn metadata_range(namespace: &Namespace, segment: SegmentId) -> BytesRange {
    BytesRange::prefix(record_prefix(namespace, segment, RecordType::PageMetadata).freeze())
}

pub(crate) fn decode_metadata_sequence(key: &[u8]) -> Result<u64> {
    let (_, _, record_type, offset) = parse_record_prefix(key)?;
    if record_type != RecordType::PageMetadata || key.len() != offset + 8 {
        return Err(Error::Corrupt("invalid trace page metadata key".to_owned()));
    }
    Ok(u64::from_be_bytes(key[offset..].try_into().unwrap()))
}

pub(crate) fn locator_key(
    namespace: &Namespace,
    trace_id: TraceId,
    segment: SegmentId,
    sequence: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, LOCATOR_SEGMENT, RecordType::TraceLocator);
    bytes.extend_from_slice(trace_id.as_bytes());
    bytes.put_u64(encode_sortable_i64(segment));
    bytes.put_u64(sequence);
    bytes.freeze()
}

pub(crate) fn locator_range(namespace: &Namespace, trace_id: TraceId) -> BytesRange {
    let mut bytes = record_prefix(namespace, LOCATOR_SEGMENT, RecordType::TraceLocator);
    bytes.extend_from_slice(trace_id.as_bytes());
    BytesRange::prefix(bytes.freeze())
}

pub(crate) fn locator_namespace_range(namespace: &Namespace) -> BytesRange {
    BytesRange::prefix(record_prefix(namespace, LOCATOR_SEGMENT, RecordType::TraceLocator).freeze())
}

pub(crate) fn decode_locator_trace_id(key: &[u8]) -> Result<TraceId> {
    let (_, segment, record_type, offset) = parse_record_prefix(key)?;
    if segment != LOCATOR_SEGMENT
        || record_type != RecordType::TraceLocator
        || key.len() != offset + 16 + 8 + 8
    {
        return Err(Error::Corrupt("invalid trace locator key".to_owned()));
    }
    TraceId::new(key[offset..offset + 16].try_into().unwrap())
}

pub(crate) fn posting_key(
    namespace: &Namespace,
    segment: SegmentId,
    matcher: &AttributeMatcher,
    sequence: u64,
) -> Bytes {
    let mut bytes = posting_prefix(namespace, segment, matcher);
    bytes.put_u64(sequence);
    bytes.freeze()
}

pub(crate) fn posting_range(
    namespace: &Namespace,
    segment: SegmentId,
    matcher: &AttributeMatcher,
) -> BytesRange {
    BytesRange::prefix(posting_prefix(namespace, segment, matcher).freeze())
}

pub(crate) fn decode_posting_sequence(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::Corrupt("truncated attribute posting key".to_owned()));
    }
    Ok(u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
}

pub(crate) fn encode_sequence(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

pub(crate) fn decode_sequence(value: &[u8]) -> Result<u64> {
    value
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| Error::Corrupt("page sequence must contain eight bytes".to_owned()))
}

pub(crate) fn encode_metadata(value: &StoredPageMetadata) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}

pub(crate) fn decode_metadata(value: &[u8]) -> Result<StoredPageMetadata> {
    let metadata: StoredPageMetadata = serde_json::from_slice(value)?;
    if metadata.version != METADATA_VERSION
        || metadata.trace_count == 0
        || metadata.min_timestamp_ns > metadata.max_timestamp_ns
    {
        return Err(Error::Corrupt(
            "invalid trace page metadata version or bounds".to_owned(),
        ));
    }
    Ok(metadata)
}

pub(crate) fn encode_locator(value: &TraceLocator) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}

pub(crate) fn decode_locator(value: &[u8]) -> Result<TraceLocator> {
    let locator: TraceLocator = serde_json::from_slice(value)?;
    if locator.version != METADATA_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported trace locator version {}",
            locator.version
        )));
    }
    Ok(locator)
}

pub(crate) fn encode_indices(indices: &[u32]) -> Result<Bytes> {
    let mut bytes = BytesMut::with_capacity(4 + indices.len() * 4);
    bytes.put_u32(
        u32::try_from(indices.len())
            .map_err(|_| Error::Invalid("attribute posting is too large".to_owned()))?,
    );
    for index in indices {
        bytes.put_u32(*index);
    }
    Ok(bytes.freeze())
}

pub(crate) fn decode_indices(value: &[u8]) -> Result<Vec<u32>> {
    if value.len() < 4 {
        return Err(Error::Corrupt("truncated attribute posting".to_owned()));
    }
    let count = u32::from_be_bytes(value[..4].try_into().unwrap()) as usize;
    let expected = 4usize
        .checked_add(
            count
                .checked_mul(4)
                .ok_or_else(|| Error::Corrupt("attribute posting overflow".to_owned()))?,
        )
        .ok_or_else(|| Error::Corrupt("attribute posting overflow".to_owned()))?;
    if value.len() != expected {
        return Err(Error::Corrupt(
            "attribute posting length mismatch".to_owned(),
        ));
    }
    Ok(value[4..]
        .chunks_exact(4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().unwrap()))
        .collect())
}

fn posting_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    matcher: &AttributeMatcher,
) -> BytesMut {
    let mut bytes = record_prefix(namespace, segment, RecordType::AttributePosting);
    bytes.put_u8(match matcher.scope {
        AttributeScope::Resource => 1,
        AttributeScope::Span => 2,
    });
    common::serde::terminated_bytes::serialize(matcher.name.as_bytes(), &mut bytes);
    match &matcher.value {
        AttributeValue::String(value) => {
            bytes.put_u8(1);
            common::serde::terminated_bytes::serialize(value.as_bytes(), &mut bytes);
        }
        AttributeValue::Bool(value) => {
            bytes.put_u8(2);
            bytes.put_u8(u8::from(*value));
        }
        AttributeValue::Int(value) => {
            bytes.put_u8(3);
            bytes.put_u64(encode_sortable_i64(*value));
        }
        AttributeValue::Double(value) => {
            bytes.put_u8(4);
            bytes.put_u64(value.to_bits());
        }
    }
    bytes
}

fn sequence_key(
    namespace: &Namespace,
    segment: SegmentId,
    record_type: RecordType,
    sequence: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, record_type);
    bytes.put_u64(sequence);
    bytes.freeze()
}

fn record_prefix(namespace: &Namespace, segment: SegmentId, record_type: RecordType) -> BytesMut {
    let mut bytes = BytesMut::new();
    write_scope(&mut bytes, namespace, segment);
    bytes.put_u8(record_type as u8);
    bytes
}

fn write_scope(bytes: &mut BytesMut, namespace: &Namespace, segment: SegmentId) {
    bytes.put_u8(SUBSYSTEM);
    bytes.put_u8(KEY_VERSION);
    common::serde::terminated_bytes::serialize(namespace.as_bytes(), bytes);
    bytes.put_u64(encode_sortable_i64(segment));
}

fn parse_record_prefix(bytes: &[u8]) -> Result<(Namespace, SegmentId, RecordType, usize)> {
    if bytes.len() < 12 || bytes[0] != SUBSYSTEM || bytes[1] != KEY_VERSION {
        return Err(Error::Corrupt("invalid Track key prefix".to_owned()));
    }
    let mut suffix = &bytes[2..];
    let namespace_bytes = common::serde::terminated_bytes::deserialize(&mut suffix)
        .map_err(|error| Error::Corrupt(error.to_string()))?;
    if suffix.len() < 9 {
        return Err(Error::Corrupt("truncated Track key scope".to_owned()));
    }
    let namespace = Namespace::new(
        String::from_utf8(namespace_bytes.to_vec())
            .map_err(|error| Error::Corrupt(format!("namespace is not UTF-8: {error}")))?,
    )
    .map_err(|error| Error::Corrupt(error.to_string()))?;
    let segment = decode_sortable_i64(&suffix[..8]);
    let record_type = match suffix[8] {
        1 => RecordType::NextPageSequence,
        2 => RecordType::PageMetadata,
        3 => RecordType::PagePayload,
        4 => RecordType::TraceLocator,
        5 => RecordType::AttributePosting,
        value => return Err(Error::Corrupt(format!("unknown Track record type {value}"))),
    };
    Ok((
        namespace,
        segment,
        record_type,
        bytes.len() - suffix.len() + 9,
    ))
}

pub(crate) fn routing_prefix_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 12 || bytes[0] != SUBSYSTEM || bytes[1] != KEY_VERSION {
        return None;
    }
    let namespace_end = bytes[2..].iter().position(|byte| *byte == 0)? + 2;
    let length = namespace_end + 1 + 8;
    (bytes.len() >= length).then_some(length)
}

fn encode_sortable_i64(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

fn decode_sortable_i64(bytes: &[u8]) -> i64 {
    (u64::from_be_bytes(bytes.try_into().unwrap()) ^ (1_u64 << 63)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_route_by_namespace_and_time_segment() {
        let namespace = Namespace::new("tenant").unwrap();
        let key = metadata_key(&namespace, -2, 7);
        assert!(key.starts_with(&segment_prefix(&namespace, -2)));
        assert_eq!(decode_metadata_sequence(&key).unwrap(), 7);
        assert_eq!(
            routing_prefix_len(&key),
            Some(segment_prefix(&namespace, -2).len())
        );
    }

    #[test]
    fn locator_uses_fixed_segment_and_unique_fragments() {
        let namespace = Namespace::default();
        let id = TraceId::new([1; 16]).unwrap();
        let first = locator_key(&namespace, id, 2, 3);
        let second = locator_key(&namespace, id, 4, 5);
        assert_ne!(first, second);
        assert!(first.starts_with(&segment_prefix(&namespace, LOCATOR_SEGMENT)));
    }

    #[test]
    fn typed_posting_keys_are_distinct() {
        let namespace = Namespace::default();
        let string = AttributeMatcher {
            scope: AttributeScope::Span,
            name: "value".to_owned(),
            value: AttributeValue::String("7".to_owned()),
        };
        let integer = AttributeMatcher {
            scope: AttributeScope::Span,
            name: "value".to_owned(),
            value: AttributeValue::Int(7),
        };
        assert_ne!(
            posting_key(&namespace, 0, &string, 1),
            posting_key(&namespace, 0, &integer, 1)
        );
    }
}
