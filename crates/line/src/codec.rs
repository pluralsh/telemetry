// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::scope::{KeyScope, ScopedSegmentExtractor};
use common::serde::sortable::{decode_i64_sortable, encode_i64_sortable};
use common::serde::varint::{var_u32, var_u64};

use crate::Namespace;
use crate::error::{Error, Result};
use crate::model::{Label, Labels, SegmentId, StreamFingerprint, StreamId};

pub(crate) const KEY_VERSION: u8 = 2;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::LOG;
const KEY_SCOPE: KeyScope = KeyScope::new(SUBSYSTEM, KEY_VERSION);
/// Persisted by SlateDB; renaming it makes existing databases unopenable.
pub(crate) const SEGMENT_EXTRACTOR: ScopedSegmentExtractor =
    ScopedSegmentExtractor::new("line-log/v2", KEY_SCOPE);
const PAGE_METADATA_VERSION: u8 = 1;
const PAGE_METADATA_HAS_EXPIRY: u8 = 1;
/// Leading byte of forward-label values.
const LABELS_FORMAT: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RecordType {
    NextStreamId = 1,
    StreamDictionary = 2,
    ForwardLabels = 3,
    LabelPostings = 4,
    PageMetadata = 5,
    PagePayload = 6,
    NextPageSequence = 7,
    SearchFieldStats = 8,
    SearchTermStats = 9,
    SearchTermDirectory = 10,
    SearchPostingBlock = 11,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageId {
    pub timestamp_ns: i64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredPageMetadata {
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub row_count: u32,
    pub payload_bytes: u32,
}

impl StoredPageMetadata {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }
}

pub(crate) fn segment_for(timestamp_ns: i64, segment_ns: i64) -> SegmentId {
    timestamp_ns.div_euclid(segment_ns) * segment_ns
}

/// Namespace and time-segment prefix shared by routed records and the
/// partition-level discovery catalog.
pub(crate) fn segment_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    let mut bytes = BytesMut::new();
    KEY_SCOPE.write(&mut bytes, namespace, segment);
    bytes.freeze()
}

pub(crate) fn next_stream_id_key(namespace: &Namespace, segment: SegmentId, slot: u16) -> Bytes {
    record_prefix(namespace, segment, slot, RecordType::NextStreamId).freeze()
}

pub(crate) fn next_page_sequence_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::NextPageSequence);
    bytes.put_u32(stream_id);
    bytes.freeze()
}

pub(crate) fn dictionary_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    fingerprint: StreamFingerprint,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::StreamDictionary);
    bytes.extend_from_slice(&fingerprint);
    bytes.freeze()
}

pub(crate) fn forward_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::ForwardLabels);
    bytes.put_u32(stream_id);
    bytes.freeze()
}

pub(crate) fn forward_range(namespace: &Namespace, segment: SegmentId, slot: u16) -> BytesRange {
    BytesRange::prefix(record_prefix(namespace, segment, slot, RecordType::ForwardLabels).freeze())
}

pub(crate) fn decode_forward_key(bytes: &[u8]) -> Result<(u16, StreamId)> {
    let (_, _, slot, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::ForwardLabels || bytes.len() != offset + 4 {
        return Err(Error::Corrupt("invalid forward-label key".to_owned()));
    }
    Ok((
        slot,
        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()),
    ))
}

pub(crate) fn posting_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    label: &Label,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::LabelPostings);
    common::serde::terminated_bytes::serialize(label.name.as_bytes(), &mut bytes);
    bytes.extend_from_slice(label.value.as_bytes());
    bytes.freeze()
}

pub(crate) fn field_stats_key(namespace: &Namespace, segment: SegmentId, slot: u16) -> Bytes {
    record_prefix(namespace, segment, slot, RecordType::SearchFieldStats).freeze()
}

pub(crate) fn term_stats_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    term: &str,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::SearchTermStats);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.freeze()
}

pub(crate) fn term_directory_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    term: &str,
    ordinal: u32,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::SearchTermDirectory);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.put_u32(ordinal);
    bytes.freeze()
}

pub(crate) fn term_posting_block_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    term: &str,
    ordinal: u32,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, RecordType::SearchPostingBlock);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.put_u32(ordinal);
    bytes.freeze()
}

pub(crate) fn metadata_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    page_key(
        namespace,
        segment,
        slot,
        RecordType::PageMetadata,
        stream_id,
        page_id,
    )
}

pub(crate) fn payload_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    page_key(
        namespace,
        segment,
        slot,
        RecordType::PagePayload,
        stream_id,
        page_id,
    )
}

pub(crate) fn metadata_range(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    stream_id: StreamId,
) -> BytesRange {
    let mut prefix = record_prefix(namespace, segment, slot, RecordType::PageMetadata);
    prefix.put_u32(stream_id);
    BytesRange::prefix(prefix.freeze())
}

pub(crate) fn decode_metadata_key(bytes: &[u8]) -> Result<(u16, StreamId, PageId)> {
    let (_, _, slot, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::PageMetadata || bytes.len() != offset + 20 {
        return Err(Error::Corrupt("invalid page metadata key".to_owned()));
    }
    let stream_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
    let timestamp_ns = decode_sortable_i64(&bytes[offset + 4..offset + 12]);
    let sequence = u64::from_be_bytes(bytes[offset + 12..offset + 20].try_into().unwrap());
    Ok((
        slot,
        stream_id,
        PageId {
            timestamp_ns,
            sequence,
        },
    ))
}

pub(crate) fn encode_stream_id(value: StreamId) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

pub(crate) fn decode_stream_id(bytes: &[u8]) -> Result<StreamId> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| Error::Corrupt("stream id must contain four bytes".to_owned()))
}

pub(crate) fn encode_page_sequence(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

pub(crate) fn decode_page_sequence(bytes: &[u8]) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| Error::Corrupt("page sequence must contain eight bytes".to_owned()))
}

pub(crate) fn encode_labels(labels: &Labels) -> Result<Bytes> {
    let mut bytes = BytesMut::new();
    bytes.put_u8(LABELS_FORMAT);
    var_u32::serialize(value_len(labels.iter().count())?, &mut bytes);
    for label in labels.iter() {
        put_str(&label.name, &mut bytes)?;
        put_str(&label.value, &mut bytes)?;
    }
    Ok(bytes.freeze())
}

pub(crate) fn decode_labels(bytes: &[u8]) -> Result<Labels> {
    if bytes.first() != Some(&LABELS_FORMAT) {
        return Err(Error::Corrupt("unknown forward-label format".to_owned()));
    }
    let mut buf = &bytes[1..];
    let count = read_u32(&mut buf)? as usize;
    let mut labels = Vec::with_capacity(count.min(buf.len() / 2));
    for _ in 0..count {
        let name = read_str(&mut buf)?;
        let value = read_str(&mut buf)?;
        labels.push(Label { name, value });
    }
    expect_consumed(buf, "forward labels")?;
    Labels::new(labels)
}

pub(crate) fn encode_metadata(metadata: &StoredPageMetadata) -> Result<Bytes> {
    if metadata.max_timestamp_ns < metadata.min_timestamp_ns {
        return Err(Error::Invalid(
            "page max timestamp precedes min timestamp".to_owned(),
        ));
    }
    let mut bytes = BytesMut::with_capacity(32);
    bytes.put_u8(PAGE_METADATA_VERSION);
    match metadata.expires_at_unix_ms {
        Some(expires_at) => {
            bytes.put_u8(PAGE_METADATA_HAS_EXPIRY);
            var_u64::serialize(expires_at, &mut bytes);
        }
        None => bytes.put_u8(0),
    }
    bytes.put_i64(metadata.min_timestamp_ns);
    var_u64::serialize(
        metadata
            .max_timestamp_ns
            .abs_diff(metadata.min_timestamp_ns),
        &mut bytes,
    );
    var_u32::serialize(metadata.row_count, &mut bytes);
    var_u32::serialize(metadata.payload_bytes, &mut bytes);
    Ok(bytes.freeze())
}

pub(crate) fn decode_metadata(bytes: &[u8]) -> Result<StoredPageMetadata> {
    let Some(&version) = bytes.first() else {
        return Err(Error::Corrupt("empty page metadata".to_owned()));
    };
    if version != PAGE_METADATA_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported page metadata version {version}"
        )));
    }
    let mut buf = &bytes[1..];
    let (&flags, rest) = buf
        .split_first()
        .ok_or_else(|| Error::Corrupt("truncated page metadata".to_owned()))?;
    buf = rest;
    let expires_at_unix_ms = match flags {
        0 => None,
        PAGE_METADATA_HAS_EXPIRY => Some(read_u64(&mut buf)?),
        _ => {
            return Err(Error::Corrupt(format!(
                "unknown page metadata flags {flags}"
            )));
        }
    };
    let (min, rest) = buf
        .split_first_chunk::<8>()
        .ok_or_else(|| Error::Corrupt("truncated page metadata".to_owned()))?;
    buf = rest;
    let min_timestamp_ns = i64::from_be_bytes(*min);
    let max_timestamp_ns = min_timestamp_ns
        .checked_add_unsigned(read_u64(&mut buf)?)
        .ok_or_else(|| Error::Corrupt("page max timestamp overflows".to_owned()))?;
    let metadata = StoredPageMetadata {
        expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        row_count: read_u32(&mut buf)?,
        payload_bytes: read_u32(&mut buf)?,
    };
    expect_consumed(buf, "page metadata")?;
    Ok(metadata)
}

fn put_str(value: &str, bytes: &mut BytesMut) -> Result<()> {
    var_u32::serialize(value_len(value.len())?, bytes);
    bytes.put_slice(value.as_bytes());
    Ok(())
}

fn read_str(buf: &mut &[u8]) -> Result<String> {
    let len = read_u32(buf)? as usize;
    if buf.len() < len {
        return Err(Error::Corrupt("truncated string".to_owned()));
    }
    let (value, rest) = buf.split_at(len);
    *buf = rest;
    String::from_utf8(value.to_vec())
        .map_err(|error| Error::Corrupt(format!("string is not UTF-8: {error}")))
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    var_u32::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    var_u64::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn value_len(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::Invalid("value length exceeds u32".to_owned()))
}

fn expect_consumed(buf: &[u8], what: &str) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(Error::Corrupt(format!("trailing bytes in {what}")))
    }
}

pub(crate) fn encode_postings(postings: &roaring::RoaringBitmap) -> Result<Bytes> {
    let mut bytes = Vec::with_capacity(postings.serialized_size());
    postings
        .serialize_into(&mut bytes)
        .map_err(|error| Error::Corrupt(format!("failed to encode postings: {error}")))?;
    Ok(Bytes::from(bytes))
}

pub(crate) fn decode_postings(bytes: &[u8]) -> Result<roaring::RoaringBitmap> {
    roaring::RoaringBitmap::deserialize_from(bytes)
        .map_err(|error| Error::Corrupt(format!("failed to decode postings: {error}")))
}

fn page_key(
    namespace: &Namespace,
    segment: SegmentId,
    slot: u16,
    record_type: RecordType,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, slot, record_type);
    bytes.put_u32(stream_id);
    bytes.put_u64(encode_i64_sortable(page_id.timestamp_ns));
    bytes.put_u64(page_id.sequence);
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
        1 => RecordType::NextStreamId,
        2 => RecordType::StreamDictionary,
        3 => RecordType::ForwardLabels,
        4 => RecordType::LabelPostings,
        5 => RecordType::PageMetadata,
        6 => RecordType::PagePayload,
        7 => RecordType::NextPageSequence,
        8 => RecordType::SearchFieldStats,
        9 => RecordType::SearchTermStats,
        10 => RecordType::SearchTermDirectory,
        11 => RecordType::SearchPostingBlock,
        value => return Err(Error::Corrupt(format!("unknown record type {value}"))),
    };
    Ok((namespace, segment, slot, record_type, scope_len + 3))
}

fn decode_sortable_i64(bytes: &[u8]) -> i64 {
    decode_i64_sortable(u64::from_be_bytes(bytes.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use slatedb::PrefixExtractor;

    use super::*;

    #[test]
    fn segment_extractor_name_is_stable() {
        assert_eq!(SEGMENT_EXTRACTOR.name(), "line-log/v2");
    }

    #[test]
    fn keys_group_by_namespace_then_segment() {
        let namespace = Namespace::new("tenant").unwrap();
        let first = metadata_key(
            &namespace,
            -10,
            17,
            2,
            PageId {
                timestamp_ns: -4,
                sequence: 8,
            },
        );
        let second = metadata_key(
            &namespace,
            10,
            18,
            1,
            PageId {
                timestamp_ns: 4,
                sequence: 7,
            },
        );
        assert!(first < second);
        assert!(first.starts_with(&segment_prefix(&namespace, -10)));
        assert_eq!(
            decode_metadata_key(&first).unwrap(),
            (
                17,
                2,
                PageId {
                    timestamp_ns: -4,
                    sequence: 8
                }
            )
        );
    }

    #[test]
    fn page_sequence_breaks_equal_timestamp_ties() {
        let namespace = Namespace::default();
        let first = metadata_key(
            &namespace,
            0,
            17,
            3,
            PageId {
                timestamp_ns: 42,
                sequence: 10,
            },
        );
        let second = metadata_key(
            &namespace,
            0,
            17,
            3,
            PageId {
                timestamp_ns: 42,
                sequence: 11,
            },
        );
        assert!(first < second);
    }

    #[test]
    fn slot_is_between_segment_prefix_and_record_type() {
        let namespace = Namespace::new("tenant").unwrap();
        let key = forward_key(&namespace, 42, 0x0abc, 7);
        let prefix = segment_prefix(&namespace, 42);
        assert_eq!(&key[prefix.len()..prefix.len() + 2], &[0x0a, 0xbc]);
        assert_eq!(key[prefix.len() + 2], RecordType::ForwardLabels as u8);
        assert_eq!(decode_forward_key(&key).unwrap(), (0x0abc, 7));
    }

    #[test]
    fn page_metadata_roundtrips_in_binary() {
        for expires_at_unix_ms in [None, Some(0), Some(u64::MAX)] {
            let metadata = StoredPageMetadata {
                expires_at_unix_ms,
                min_timestamp_ns: i64::MIN,
                max_timestamp_ns: i64::MAX,
                row_count: 7,
                payload_bytes: 4096,
            };
            let encoded = encode_metadata(&metadata).unwrap();
            assert_eq!(encoded[0], PAGE_METADATA_VERSION);
            assert_eq!(decode_metadata(&encoded).unwrap(), metadata);
        }
        let mut unknown = encode_metadata(&StoredPageMetadata {
            expires_at_unix_ms: None,
            min_timestamp_ns: 1,
            max_timestamp_ns: 2,
            row_count: 1,
            payload_bytes: 1,
        })
        .unwrap()
        .to_vec();
        unknown[0] = PAGE_METADATA_VERSION + 1;
        assert!(decode_metadata(&unknown).is_err());
    }

    #[test]
    fn labels_roundtrip() {
        let labels = Labels::new(vec![
            Label::new("service", "api"),
            Label::new("environment", "prod ✓"),
            Label::new("empty", ""),
        ])
        .unwrap();
        let encoded = encode_labels(&labels).unwrap();
        assert_eq!(decode_labels(&encoded).unwrap(), labels);
        assert!(decode_labels(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode_labels(&serde_json::to_vec(&labels).unwrap()).is_err());
    }

    #[test]
    fn page_metadata_rejects_unknown_versions() {
        let error = decode_metadata(&[PAGE_METADATA_VERSION + 1, 0]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported page metadata version")
        );
    }
}
