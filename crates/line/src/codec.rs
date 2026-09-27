// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::model::{Label, Labels, SegmentId, StreamFingerprint, StreamId};
use crate::namespace::Namespace;
use crate::page::BlockMetadata;

pub(crate) const KEY_VERSION: u8 = 2;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::LOG;
pub(crate) const CURRENT_PAGE_METADATA_VERSION: u8 = 2;

const fn legacy_page_metadata_version() -> u8 {
    1
}

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PageId {
    pub timestamp_ns: i64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StoredPageMetadata {
    #[serde(default = "legacy_page_metadata_version")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub row_count: u32,
    pub payload_bytes: u32,
    pub blocks: Vec<BlockMetadata>,
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

#[cfg(test)]
pub(crate) fn segment_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    let mut bytes = BytesMut::new();
    write_scope(&mut bytes, namespace, segment);
    bytes.freeze()
}

pub(crate) fn next_stream_id_key(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::NextStreamId).freeze()
}

pub(crate) fn next_page_sequence_key(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::NextPageSequence);
    bytes.put_u32(stream_id);
    bytes.freeze()
}

pub(crate) fn dictionary_key(
    namespace: &Namespace,
    segment: SegmentId,
    fingerprint: StreamFingerprint,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::StreamDictionary);
    bytes.extend_from_slice(&fingerprint);
    bytes.freeze()
}

pub(crate) fn forward_key(namespace: &Namespace, segment: SegmentId, stream_id: StreamId) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::ForwardLabels);
    bytes.put_u32(stream_id);
    bytes.freeze()
}

pub(crate) fn forward_range(namespace: &Namespace, segment: SegmentId) -> BytesRange {
    BytesRange::prefix(record_prefix(namespace, segment, RecordType::ForwardLabels).freeze())
}

pub(crate) fn decode_forward_key(bytes: &[u8]) -> Result<StreamId> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::ForwardLabels || bytes.len() != offset + 4 {
        return Err(Error::Corrupt("invalid forward-label key".to_owned()));
    }
    Ok(u32::from_be_bytes(
        bytes[offset..offset + 4].try_into().unwrap(),
    ))
}

pub(crate) fn posting_key(namespace: &Namespace, segment: SegmentId, label: &Label) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::LabelPostings);
    common::serde::terminated_bytes::serialize(label.name.as_bytes(), &mut bytes);
    bytes.extend_from_slice(label.value.as_bytes());
    bytes.freeze()
}

pub(crate) fn field_stats_key(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::SearchFieldStats).freeze()
}

pub(crate) fn term_stats_key(namespace: &Namespace, segment: SegmentId, term: &str) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::SearchTermStats);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.freeze()
}

pub(crate) fn term_directory_key(
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    ordinal: u32,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::SearchTermDirectory);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.put_u32(ordinal);
    bytes.freeze()
}

pub(crate) fn term_posting_block_key(
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    ordinal: u32,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::SearchPostingBlock);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.put_u32(ordinal);
    bytes.freeze()
}

pub(crate) fn metadata_key(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    page_key(
        namespace,
        segment,
        RecordType::PageMetadata,
        stream_id,
        page_id,
    )
}

pub(crate) fn payload_key(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    page_key(
        namespace,
        segment,
        RecordType::PagePayload,
        stream_id,
        page_id,
    )
}

pub(crate) fn metadata_range(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
) -> BytesRange {
    let mut prefix = record_prefix(namespace, segment, RecordType::PageMetadata);
    prefix.put_u32(stream_id);
    BytesRange::prefix(prefix.freeze())
}

pub(crate) fn decode_metadata_key(bytes: &[u8]) -> Result<(StreamId, PageId)> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::PageMetadata || bytes.len() != offset + 20 {
        return Err(Error::Corrupt("invalid page metadata key".to_owned()));
    }
    let stream_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
    let timestamp_ns = decode_sortable_i64(&bytes[offset + 4..offset + 12]);
    let sequence = u64::from_be_bytes(bytes[offset + 12..offset + 20].try_into().unwrap());
    Ok((
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
    Ok(Bytes::from(serde_json::to_vec(labels)?))
}

pub(crate) fn decode_labels(bytes: &[u8]) -> Result<Labels> {
    let labels: Labels = serde_json::from_slice(bytes)?;
    Labels::new(labels.iter().cloned().collect())
}

pub(crate) fn encode_metadata(metadata: &StoredPageMetadata) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(metadata)?))
}

pub(crate) fn decode_metadata(bytes: &[u8]) -> Result<StoredPageMetadata> {
    let metadata: StoredPageMetadata = serde_json::from_slice(bytes)?;
    if metadata.version == 0 || metadata.version > CURRENT_PAGE_METADATA_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported page metadata version {}",
            metadata.version
        )));
    }
    Ok(metadata)
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
    record_type: RecordType,
    stream_id: StreamId,
    page_id: PageId,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, record_type);
    bytes.put_u32(stream_id);
    bytes.put_u64(encode_sortable_i64(page_id.timestamp_ns));
    bytes.put_u64(page_id.sequence);
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
        return Err(Error::Corrupt("invalid Line key prefix".to_owned()));
    }
    let mut suffix = &bytes[2..];
    let namespace_bytes = common::serde::terminated_bytes::deserialize(&mut suffix)
        .map_err(|error| Error::Corrupt(error.to_string()))?;
    if suffix.len() < 9 {
        return Err(Error::Corrupt("truncated Line key scope".to_owned()));
    }
    let namespace = Namespace::new(
        String::from_utf8(namespace_bytes.to_vec())
            .map_err(|error| Error::Corrupt(format!("namespace is not UTF-8: {error}")))?,
    )
    .map_err(|error| Error::Corrupt(error.to_string()))?;
    let segment = decode_sortable_i64(&suffix[..8]);
    let record_type = match suffix[8] {
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
    fn keys_group_by_namespace_then_segment() {
        let namespace = Namespace::new("tenant").unwrap();
        let first = metadata_key(
            &namespace,
            -10,
            2,
            PageId {
                timestamp_ns: -4,
                sequence: 8,
            },
        );
        let second = metadata_key(
            &namespace,
            10,
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
            3,
            PageId {
                timestamp_ns: 42,
                sequence: 10,
            },
        );
        let second = metadata_key(
            &namespace,
            0,
            3,
            PageId {
                timestamp_ns: 42,
                sequence: 11,
            },
        );
        assert!(first < second);
    }

    #[test]
    fn page_metadata_decodes_legacy_records_without_logical_expiry() {
        let metadata = decode_metadata(
            br#"{"min_timestamp_ns":1,"max_timestamp_ns":2,"row_count":1,"payload_bytes":3,"blocks":[]}"#,
        )
        .unwrap();
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.expires_at_unix_ms, None);
        assert!(!metadata.is_expired_at(u64::MAX));
    }

    #[test]
    fn page_metadata_rejects_unknown_versions() {
        let error = decode_metadata(
            br#"{"version":3,"min_timestamp_ns":1,"max_timestamp_ns":2,"row_count":1,"payload_bytes":3,"blocks":[]}"#,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported page metadata version")
        );
    }
}
