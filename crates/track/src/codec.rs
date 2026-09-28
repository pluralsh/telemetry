// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::scope::{KeyScope, ScopedSegmentExtractor};
use common::serde::sortable::encode_i64_sortable;
use common::serde::varint::{var_u32, var_u64};

use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Error, Namespace, Result, SegmentId, TraceId,
};

pub(crate) const KEY_VERSION: u8 = 2;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::TRACE;
const KEY_SCOPE: KeyScope = KeyScope::new(SUBSYSTEM, KEY_VERSION);
/// Persisted by SlateDB; renaming it makes existing databases unopenable.
pub(crate) const SEGMENT_EXTRACTOR: ScopedSegmentExtractor =
    ScopedSegmentExtractor::new("track-trace/v2", KEY_SCOPE);
/// Leading byte of page metadata and locator values.
const VALUE_VERSION: u8 = 1;
const HAS_EXPIRY: u8 = 1;

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

/// Page metadata carries a compact copy of the page's trace directory so
/// search can resolve candidate trace IDs without fetching payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredPageMetadata {
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    /// Strictly ordered by trace ID, index-aligned with the payload directory.
    pub traces: Vec<PageTrace>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageTrace {
    pub trace_id: TraceId,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
}

impl PageTrace {
    pub(crate) fn overlaps(&self, start_ns: u64, end_ns: u64) -> bool {
        self.max_timestamp_ns >= start_ns && self.min_timestamp_ns <= end_ns
    }
}

impl StoredPageMetadata {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }

    pub(crate) fn overlaps(&self, start_ns: u64, end_ns: u64) -> bool {
        self.max_timestamp_ns >= start_ns && self.min_timestamp_ns <= end_ns
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TraceLocator {
    pub segment: SegmentId,
    pub page_sequence: u64,
    pub trace_index: u32,
    pub expires_at_unix_ms: Option<u64>,
}

/// A page's `(segment, routing slot, sequence)` address.
pub(crate) type PageRef = (SegmentId, u16, u64);

impl TraceLocator {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }

    pub(crate) fn page(&self, slot: u16) -> PageRef {
        (self.segment, slot, self.page_sequence)
    }
}

pub(crate) fn segment_for(timestamp_ns: u64, segment_ns: u64) -> SegmentId {
    i64::try_from(timestamp_ns / segment_ns).unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(crate) fn segment_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    let mut bytes = BytesMut::new();
    KEY_SCOPE.write(&mut bytes, namespace, segment);
    bytes.freeze()
}

pub(crate) fn next_sequence_key(namespace: &Namespace, segment: SegmentId, slot: u16) -> Bytes {
    record_prefix(namespace, segment, slot, RecordType::NextPageSequence).freeze()
}

pub(crate) fn metadata_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    sequence: u64,
) -> Bytes {
    sequence_key(namespace, segment, slot, RecordType::PageMetadata, sequence)
}

pub(crate) fn payload_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    sequence: u64,
) -> Bytes {
    sequence_key(namespace, segment, slot, RecordType::PagePayload, sequence)
}

pub(crate) fn metadata_range(namespace: &Namespace, segment: SegmentId, slot: u16) -> BytesRange {
    BytesRange::prefix(record_prefix(namespace, segment, slot, RecordType::PageMetadata).freeze())
}

#[cfg(test)]
pub(crate) fn decode_metadata_sequence(key: &[u8]) -> Result<u64> {
    let (_, _, _, record_type, offset) = parse_record_prefix(key)?;
    if record_type != RecordType::PageMetadata || key.len() != offset + 8 {
        return Err(Error::Corrupt("invalid trace page metadata key".to_owned()));
    }
    Ok(u64::from_be_bytes(key[offset..].try_into().unwrap()))
}

pub(crate) fn locator_key(
    namespace: &Namespace,
    slot: u16,
    trace_id: TraceId,
    segment: SegmentId,
    sequence: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, LOCATOR_SEGMENT, slot, RecordType::TraceLocator);
    bytes.extend_from_slice(trace_id.as_bytes());
    bytes.put_u64(encode_i64_sortable(segment));
    bytes.put_u64(sequence);
    bytes.freeze()
}

pub(crate) fn locator_range(namespace: &Namespace, slot: u16, trace_id: TraceId) -> BytesRange {
    let mut bytes = record_prefix(namespace, LOCATOR_SEGMENT, slot, RecordType::TraceLocator);
    bytes.extend_from_slice(trace_id.as_bytes());
    BytesRange::prefix(bytes.freeze())
}

pub(crate) fn locator_slot_range(namespace: &Namespace, slot: u16) -> BytesRange {
    BytesRange::prefix(
        record_prefix(namespace, LOCATOR_SEGMENT, slot, RecordType::TraceLocator).freeze(),
    )
}

pub(crate) fn decode_locator_trace_id(key: &[u8]) -> Result<(u16, TraceId)> {
    let (_, segment, slot, record_type, offset) = parse_record_prefix(key)?;
    if segment != LOCATOR_SEGMENT
        || record_type != RecordType::TraceLocator
        || key.len() != offset + 16 + 8 + 8
    {
        return Err(Error::Corrupt("invalid trace locator key".to_owned()));
    }
    Ok((
        slot,
        TraceId::new(key[offset..offset + 16].try_into().unwrap())?,
    ))
}

pub(crate) fn posting_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    matcher: &AttributeMatcher,
    sequence: u64,
) -> Bytes {
    let mut bytes = posting_prefix(namespace, segment, slot, matcher);
    bytes.put_u64(sequence);
    bytes.freeze()
}

pub(crate) fn posting_range(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    matcher: &AttributeMatcher,
) -> BytesRange {
    BytesRange::prefix(posting_prefix(namespace, segment, slot, matcher).freeze())
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

/// Trace timestamps are stored relative to the page minimum, so the
/// directory costs about 20 bytes per trace.
pub(crate) fn encode_metadata(value: &StoredPageMetadata) -> Result<Bytes> {
    let page_span = value
        .max_timestamp_ns
        .checked_sub(value.min_timestamp_ns)
        .ok_or_else(|| Error::Invalid("page max timestamp precedes min timestamp".to_owned()))?;
    let mut bytes = value_header(value.expires_at_unix_ms, 24 + value.traces.len() * 24);
    var_u64::serialize(value.min_timestamp_ns, &mut bytes);
    var_u64::serialize(page_span, &mut bytes);
    var_u32::serialize(
        u32::try_from(value.traces.len())
            .map_err(|_| Error::Invalid("page trace count exceeds u32".to_owned()))?,
        &mut bytes,
    );
    for trace in &value.traces {
        let (Some(offset), Some(span)) = (
            trace.min_timestamp_ns.checked_sub(value.min_timestamp_ns),
            trace.max_timestamp_ns.checked_sub(trace.min_timestamp_ns),
        ) else {
            return Err(Error::Invalid(
                "trace timestamps fall outside page bounds".to_owned(),
            ));
        };
        bytes.extend_from_slice(trace.trace_id.as_bytes());
        var_u64::serialize(offset, &mut bytes);
        var_u64::serialize(span, &mut bytes);
    }
    Ok(bytes.freeze())
}

pub(crate) fn decode_metadata(value: &[u8]) -> Result<StoredPageMetadata> {
    let corrupt = || Error::Corrupt("invalid trace page metadata bounds".to_owned());
    let (expires_at_unix_ms, mut buf) = value_body(value, "trace page metadata")?;
    let min_timestamp_ns = read_u64(&mut buf)?;
    let max_timestamp_ns = min_timestamp_ns
        .checked_add(read_u64(&mut buf)?)
        .ok_or_else(|| Error::Corrupt("page max timestamp overflows".to_owned()))?;
    let count = read_u32(&mut buf)? as usize;
    if count == 0 {
        return Err(corrupt());
    }
    let mut traces = Vec::with_capacity(count.min(buf.len() / 18));
    let (mut observed_min, mut observed_max) = (u64::MAX, 0);
    for _ in 0..count {
        let (id, rest) = buf
            .split_first_chunk::<16>()
            .ok_or_else(|| Error::Corrupt("truncated trace page metadata".to_owned()))?;
        buf = rest;
        let trace_id = TraceId::new(*id).map_err(|error| Error::Corrupt(error.to_string()))?;
        let trace_min = min_timestamp_ns
            .checked_add(read_u64(&mut buf)?)
            .ok_or_else(corrupt)?;
        let trace_max = trace_min
            .checked_add(read_u64(&mut buf)?)
            .ok_or_else(corrupt)?;
        if trace_max > max_timestamp_ns
            || traces
                .last()
                .is_some_and(|prior: &PageTrace| prior.trace_id >= trace_id)
        {
            return Err(corrupt());
        }
        observed_min = observed_min.min(trace_min);
        observed_max = observed_max.max(trace_max);
        traces.push(PageTrace {
            trace_id,
            min_timestamp_ns: trace_min,
            max_timestamp_ns: trace_max,
        });
    }
    expect_consumed(buf, "trace page metadata")?;
    if observed_min != min_timestamp_ns || observed_max != max_timestamp_ns {
        return Err(corrupt());
    }
    Ok(StoredPageMetadata {
        expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        traces,
    })
}

pub(crate) fn encode_locator(value: &TraceLocator) -> Result<Bytes> {
    let mut bytes = value_header(value.expires_at_unix_ms, 20);
    bytes.put_i64(value.segment);
    var_u64::serialize(value.page_sequence, &mut bytes);
    var_u32::serialize(value.trace_index, &mut bytes);
    Ok(bytes.freeze())
}

pub(crate) fn decode_locator(value: &[u8]) -> Result<TraceLocator> {
    let (expires_at_unix_ms, mut buf) = value_body(value, "trace locator")?;
    let (segment, rest) = buf
        .split_first_chunk::<8>()
        .ok_or_else(|| Error::Corrupt("truncated trace locator".to_owned()))?;
    buf = rest;
    let locator = TraceLocator {
        segment: i64::from_be_bytes(*segment),
        page_sequence: read_u64(&mut buf)?,
        trace_index: read_u32(&mut buf)?,
        expires_at_unix_ms,
    };
    expect_consumed(buf, "trace locator")?;
    Ok(locator)
}

fn value_header(expires_at_unix_ms: Option<u64>, capacity: usize) -> BytesMut {
    let mut bytes = BytesMut::with_capacity(capacity);
    bytes.put_u8(VALUE_VERSION);
    match expires_at_unix_ms {
        Some(expires_at) => {
            bytes.put_u8(HAS_EXPIRY);
            var_u64::serialize(expires_at, &mut bytes);
        }
        None => bytes.put_u8(0),
    }
    bytes
}

/// Returns the logical expiry and the remaining payload.
fn value_body<'a>(value: &'a [u8], what: &str) -> Result<(Option<u64>, &'a [u8])> {
    match value {
        [VALUE_VERSION, flags, rest @ ..] => {
            let mut buf = rest;
            let expires_at = match *flags {
                0 => None,
                HAS_EXPIRY => Some(read_u64(&mut buf)?),
                flags => return Err(Error::Corrupt(format!("unknown {what} flags {flags}"))),
            };
            Ok((expires_at, buf))
        }
        [version, ..] => Err(Error::Corrupt(format!(
            "unsupported {what} version {version}"
        ))),
        [] => Err(Error::Corrupt(format!("empty {what}"))),
    }
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    var_u32::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    var_u64::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn expect_consumed(buf: &[u8], what: &str) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(Error::Corrupt(format!("trailing bytes in {what}")))
    }
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
    slot: u16,
    matcher: &AttributeMatcher,
) -> BytesMut {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::AttributePosting);
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
            bytes.put_u64(encode_i64_sortable(*value));
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
    slot: u16,
    record_type: RecordType,
    sequence: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, record_type);
    bytes.put_u64(sequence);
    bytes.freeze()
}

fn record_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    record_type: RecordType,
) -> BytesMut {
    assert!(slot < sharding::ROUTING_SLOT_COUNT);
    let mut bytes = BytesMut::new();
    KEY_SCOPE.write(&mut bytes, namespace, segment);
    bytes.put_u16(slot);
    bytes.put_u8(record_type as u8);
    bytes
}

fn parse_record_prefix(bytes: &[u8]) -> Result<(Namespace, SegmentId, u16, RecordType, usize)> {
    let (namespace, segment, scope_len) = KEY_SCOPE.parse(bytes)?;
    let slot_bytes = bytes
        .get(scope_len..scope_len + 2)
        .ok_or_else(|| Error::Corrupt("key is missing its routing slot".to_owned()))?;
    let slot = u16::from_be_bytes(slot_bytes.try_into().unwrap());
    if slot >= sharding::ROUTING_SLOT_COUNT {
        return Err(Error::Corrupt(format!(
            "routing slot exceeds 12 bits: {slot}"
        )));
    }
    let Some(&record_type) = bytes.get(scope_len + 2) else {
        return Err(Error::Corrupt("key is missing its record type".to_owned()));
    };
    let record_type = match record_type {
        1 => RecordType::NextPageSequence,
        2 => RecordType::PageMetadata,
        3 => RecordType::PagePayload,
        4 => RecordType::TraceLocator,
        5 => RecordType::AttributePosting,
        value => return Err(Error::Corrupt(format!("unknown Track record type {value}"))),
    };
    Ok((namespace, segment, slot, record_type, scope_len + 3))
}
#[cfg(test)]
mod tests {
    use slatedb::PrefixExtractor;

    use super::*;

    #[test]
    fn segment_extractor_name_is_stable() {
        assert_eq!(SEGMENT_EXTRACTOR.name(), "track-trace/v2");
    }

    #[test]
    fn keys_route_by_namespace_time_segment_and_slot() {
        let namespace = Namespace::new("tenant").unwrap();
        let key = metadata_key(&namespace, -2, 17, 7);
        assert!(key.starts_with(&segment_prefix(&namespace, -2)));
        assert_eq!(decode_metadata_sequence(&key).unwrap(), 7);
        assert_eq!(parse_record_prefix(&key).unwrap().2, 17);
        assert_eq!(
            KEY_SCOPE.prefix_len(&key),
            Some(segment_prefix(&namespace, -2).len())
        );
    }

    #[test]
    fn locator_uses_fixed_segment_and_unique_fragments() {
        let namespace = Namespace::default();
        let id = TraceId::new([1; 16]).unwrap();
        let slot = crate::routing::routing_slot(&namespace, id);
        let first = locator_key(&namespace, slot, id, 2, 3);
        let second = locator_key(&namespace, slot, id, 4, 5);
        assert_ne!(first, second);
        assert!(first.starts_with(&segment_prefix(&namespace, LOCATOR_SEGMENT)));
        assert_eq!(decode_locator_trace_id(&first).unwrap(), (slot, id));
    }

    #[test]
    fn metadata_and_locators_roundtrip_in_binary() {
        for expires_at_unix_ms in [None, Some(0), Some(u64::MAX)] {
            let metadata = StoredPageMetadata {
                expires_at_unix_ms,
                min_timestamp_ns: 5,
                max_timestamp_ns: u64::MAX,
                traces: vec![
                    PageTrace {
                        trace_id: TraceId::new([1; 16]).unwrap(),
                        min_timestamp_ns: 9,
                        max_timestamp_ns: u64::MAX,
                    },
                    PageTrace {
                        trace_id: TraceId::new([2; 16]).unwrap(),
                        min_timestamp_ns: 5,
                        max_timestamp_ns: 5,
                    },
                ],
            };
            assert_eq!(
                decode_metadata(&encode_metadata(&metadata).unwrap()).unwrap(),
                metadata
            );
            let locator = TraceLocator {
                segment: i64::MIN,
                page_sequence: u64::MAX,
                trace_index: 12,
                expires_at_unix_ms,
            };
            assert_eq!(
                decode_locator(&encode_locator(&locator).unwrap()).unwrap(),
                locator
            );
        }
    }

    #[test]
    fn rejects_unknown_value_versions() {
        let mut locator = encode_locator(&TraceLocator {
            segment: -4,
            page_sequence: 2,
            trace_index: 1,
            expires_at_unix_ms: None,
        })
        .unwrap()
        .to_vec();
        locator[0] = VALUE_VERSION + 1;
        assert!(decode_locator(&locator).is_err());
        assert!(decode_metadata(br#"{"version":1}"#).is_err());
    }

    #[test]
    fn metadata_rejects_inconsistent_directories() {
        let trace = |id: u8, min: u64, max: u64| PageTrace {
            trace_id: TraceId::new([id; 16]).unwrap(),
            min_timestamp_ns: min,
            max_timestamp_ns: max,
        };
        let metadata = |traces| StoredPageMetadata {
            expires_at_unix_ms: None,
            min_timestamp_ns: 10,
            max_timestamp_ns: 20,
            traces,
        };
        for invalid in [
            metadata(vec![trace(2, 10, 20), trace(1, 10, 20)]),
            metadata(vec![trace(1, 10, 15)]),
            metadata(vec![trace(1, 12, 20)]),
            metadata(vec![trace(1, 10, 25)]),
        ] {
            let encoded = encode_metadata(&invalid).unwrap();
            assert!(decode_metadata(&encoded).is_err(), "{invalid:?}");
        }
        assert!(encode_metadata(&metadata(vec![trace(1, 5, 20)])).is_err());
        assert!(
            encode_metadata(&metadata(Vec::new()))
                .is_ok_and(|bytes| decode_metadata(&bytes).is_err())
        );
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
            posting_key(&namespace, 0, 17, &string, 1),
            posting_key(&namespace, 0, 17, &integer, 1)
        );
    }

    #[test]
    fn routing_slot_follows_segment_prefix_and_precedes_record_type() {
        let namespace = Namespace::new("tenant").unwrap();
        let prefix = segment_prefix(&namespace, 9);
        let key = metadata_key(&namespace, 9, 0x0abc, 7);
        assert_eq!(
            &key[prefix.len()..prefix.len() + 2],
            &0x0abcu16.to_be_bytes()
        );
        assert_eq!(key[prefix.len() + 2], RecordType::PageMetadata as u8);
        assert_eq!(KEY_SCOPE.prefix_len(&key), Some(prefix.len()));
    }
}
