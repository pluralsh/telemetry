// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::serde::ensure_consumed;
use common::serde::scope::{KeyScope, ScopedSegmentExtractor};
use common::serde::sortable::{decode_i64_sortable, encode_i64_sortable};
use common::serde::varint::{var_u32, var_u64};

use crate::Namespace;
use crate::error::{Error, Result};
use crate::model::{Label, Labels, SegmentId, StreamFingerprint, StreamId};

pub(crate) const KEY_VERSION: u8 = 3;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::LOG;
const KEY_SCOPE: KeyScope = KeyScope::new(SUBSYSTEM, KEY_VERSION);
/// Persisted by SlateDB; renaming it makes existing databases unopenable.
pub(crate) const SEGMENT_EXTRACTOR_NAME: &str = "logs-log/v3";
pub(crate) const SEGMENT_EXTRACTOR: ScopedSegmentExtractor =
    ScopedSegmentExtractor::new(SEGMENT_EXTRACTOR_NAME, KEY_SCOPE);
const PAGE_METADATA_VERSION: u8 = 2;
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
    PageTombstone = 12,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageId {
    pub timestamp_ns: i64,
    pub sequence: u64,
}

/// Page metadata. A compacted page keeps the key of the first page it
/// replaced and covers the consecutive written pages ("leaves") starting at
/// its sequence; full-text postings keep addressing those leaves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredPageMetadata {
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub row_count: u32,
    pub payload_bytes: u32,
    /// Zero for written pages; a merge produces one more than its inputs'
    /// maximum, so `(sequence, level)` never names two different payloads.
    pub level: u8,
    pub written_at_unix_ms: u64,
    /// Row count of each covered leaf in order; empty for a single leaf.
    pub leaf_rows: Vec<u32>,
}

impl StoredPageMetadata {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        self.expires_at_unix_ms
            .is_some_and(|expires_at| unix_ms >= expires_at)
    }

    pub(crate) fn leaf_count(&self) -> u64 {
        self.leaf_rows.len().max(1) as u64
    }

    /// `(leaf sequence, first row)` for every leaf of a page at `sequence`.
    pub(crate) fn leaves(&self, sequence: u64) -> Vec<(u64, u32)> {
        if self.leaf_rows.is_empty() {
            return vec![(sequence, 0)];
        }
        let mut offset = 0u32;
        (sequence..)
            .zip(&self.leaf_rows)
            .map(|(leaf, rows)| {
                let first = offset;
                offset = offset.saturating_add(*rows);
                (leaf, first)
            })
            .collect()
    }

    /// Row counts of the leaves, including a single-leaf page's own count.
    pub(crate) fn leaf_row_counts(&self) -> Vec<u32> {
        if self.leaf_rows.is_empty() {
            vec![self.row_count]
        } else {
            self.leaf_rows.clone()
        }
    }
}

pub(crate) fn segment_for(timestamp_ns: i64, segment_ns: i64) -> SegmentId {
    timestamp_ns.div_euclid(segment_ns) * segment_ns
}

/// Namespace and time-segment prefix shared by stream records and the
/// partition-level discovery catalog.
pub(crate) fn segment_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    let mut bytes = BytesMut::new();
    KEY_SCOPE.write(&mut bytes, namespace, segment);
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

pub(crate) fn forward_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::ForwardLabels).freeze()
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
    level: u8,
) -> Bytes {
    leveled_page_key(
        namespace,
        segment,
        RecordType::PagePayload,
        stream_id,
        page_id,
        level,
    )
}

/// Marks a replaced payload for deletion once in-flight readers are done.
pub(crate) fn tombstone_key(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
    page_id: PageId,
    level: u8,
) -> Bytes {
    leveled_page_key(
        namespace,
        segment,
        RecordType::PageTombstone,
        stream_id,
        page_id,
        level,
    )
}

pub(crate) fn tombstone_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::PageTombstone).freeze()
}

pub(crate) fn decode_tombstone_key(bytes: &[u8]) -> Result<(StreamId, PageId, u8)> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::PageTombstone || bytes.len() != offset + 21 {
        return Err(Error::Corrupt("invalid page tombstone key".to_owned()));
    }
    let (stream_id, page_id) = decode_page_suffix(&bytes[offset..offset + 20]);
    Ok((stream_id, page_id, bytes[offset + 20]))
}

pub(crate) fn encode_deadline(unix_ms: u64) -> Bytes {
    Bytes::copy_from_slice(&unix_ms.to_be_bytes())
}

pub(crate) fn decode_deadline(bytes: &[u8]) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| Error::Corrupt("tombstone deadline must contain eight bytes".to_owned()))
}

/// Page metadata of every stream in a segment.
pub(crate) fn segment_metadata_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::PageMetadata).freeze()
}

pub(crate) fn metadata_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
) -> Bytes {
    let mut prefix = record_prefix(namespace, segment, RecordType::PageMetadata);
    prefix.put_u32(stream_id);
    prefix.freeze()
}

pub(crate) fn decode_metadata_key(bytes: &[u8]) -> Result<(StreamId, PageId)> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::PageMetadata || bytes.len() != offset + 20 {
        return Err(Error::Corrupt("invalid page metadata key".to_owned()));
    }
    Ok(decode_page_suffix(&bytes[offset..]))
}

fn decode_page_suffix(bytes: &[u8]) -> (StreamId, PageId) {
    let stream_id = u32::from_be_bytes(bytes[..4].try_into().unwrap());
    let timestamp_ns = decode_sortable_i64(&bytes[4..12]);
    let sequence = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    (
        stream_id,
        PageId {
            timestamp_ns,
            sequence,
        },
    )
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
    let count = var_u32::deserialize(&mut buf)? as usize;
    let mut labels = Vec::with_capacity(count.min(buf.len() / 2));
    for _ in 0..count {
        let name = read_str(&mut buf)?;
        let value = read_str(&mut buf)?;
        labels.push(Label { name, value });
    }
    ensure_consumed(buf, "forward labels")?;
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
    bytes.put_u8(metadata.level);
    var_u64::serialize(metadata.written_at_unix_ms, &mut bytes);
    var_u32::serialize(value_len(metadata.leaf_rows.len())?, &mut bytes);
    for rows in &metadata.leaf_rows {
        var_u32::serialize(*rows, &mut bytes);
    }
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
        PAGE_METADATA_HAS_EXPIRY => Some(var_u64::deserialize(&mut buf)?),
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
        .checked_add_unsigned(var_u64::deserialize(&mut buf)?)
        .ok_or_else(|| Error::Corrupt("page max timestamp overflows".to_owned()))?;
    let row_count = var_u32::deserialize(&mut buf)?;
    let payload_bytes = var_u32::deserialize(&mut buf)?;
    let (&level, rest) = buf
        .split_first()
        .ok_or_else(|| Error::Corrupt("truncated page metadata".to_owned()))?;
    buf = rest;
    let written_at_unix_ms = var_u64::deserialize(&mut buf)?;
    let leaves = var_u32::deserialize(&mut buf)? as usize;
    let mut leaf_rows = Vec::with_capacity(leaves.min(buf.len()));
    for _ in 0..leaves {
        leaf_rows.push(var_u32::deserialize(&mut buf)?);
    }
    ensure_consumed(buf, "page metadata")?;
    if !leaf_rows.is_empty()
        && leaf_rows
            .iter()
            .try_fold(0u32, |sum, rows| sum.checked_add(*rows))
            != Some(row_count)
    {
        return Err(Error::Corrupt(
            "page leaf rows do not sum to its row count".to_owned(),
        ));
    }
    Ok(StoredPageMetadata {
        expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        row_count,
        payload_bytes,
        level,
        written_at_unix_ms,
        leaf_rows,
    })
}

fn put_str(value: &str, bytes: &mut BytesMut) -> Result<()> {
    var_u32::serialize(value_len(value.len())?, bytes);
    bytes.put_slice(value.as_bytes());
    Ok(())
}

fn read_str(buf: &mut &[u8]) -> Result<String> {
    let len = var_u32::deserialize(buf)? as usize;
    if buf.len() < len {
        return Err(Error::Corrupt("truncated string".to_owned()));
    }
    let (value, rest) = buf.split_at(len);
    *buf = rest;
    String::from_utf8(value.to_vec())
        .map_err(|error| Error::Corrupt(format!("string is not UTF-8: {error}")))
}

fn value_len(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::Invalid("value length exceeds u32".to_owned()))
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
    bytes.put_u64(encode_i64_sortable(page_id.timestamp_ns));
    bytes.put_u64(page_id.sequence);
    bytes.freeze()
}

fn leveled_page_key(
    namespace: &Namespace,
    segment: SegmentId,
    record_type: RecordType,
    stream_id: StreamId,
    page_id: PageId,
    level: u8,
) -> Bytes {
    let key = page_key(namespace, segment, record_type, stream_id, page_id);
    let mut bytes = BytesMut::with_capacity(key.len() + 1);
    bytes.extend_from_slice(&key);
    bytes.put_u8(level);
    bytes.freeze()
}

#[cfg(test)]
pub(crate) fn record_type_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    record_type: RecordType,
) -> Bytes {
    record_prefix(namespace, segment, record_type).freeze()
}

fn record_prefix(namespace: &Namespace, segment: SegmentId, record_type: RecordType) -> BytesMut {
    let mut bytes = BytesMut::new();
    KEY_SCOPE.write(&mut bytes, namespace, segment);
    bytes.put_u8(record_type as u8);
    bytes
}

fn parse_record_prefix(bytes: &[u8]) -> Result<(Namespace, SegmentId, RecordType, usize)> {
    let (namespace, segment, scope_len) = KEY_SCOPE.parse(bytes)?;
    let Some(&record_type) = bytes.get(scope_len) else {
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
        12 => RecordType::PageTombstone,
        value => return Err(Error::Corrupt(format!("unknown record type {value}"))),
    };
    Ok((namespace, segment, record_type, scope_len + 1))
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
        assert_eq!(SEGMENT_EXTRACTOR.name(), "logs-log/v3");
    }

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
    fn record_type_directly_follows_segment_prefix() {
        let namespace = Namespace::new("tenant").unwrap();
        let key = forward_key(&namespace, 42, 7);
        let prefix = segment_prefix(&namespace, 42);
        assert_eq!(key[prefix.len()], RecordType::ForwardLabels as u8);
        assert_eq!(decode_forward_key(&key).unwrap(), 7);
    }

    #[test]
    fn page_metadata_roundtrips_in_binary() {
        for (expires_at_unix_ms, leaf_rows) in [
            (None, Vec::new()),
            (Some(0), vec![3, 4]),
            (Some(u64::MAX), vec![7]),
        ] {
            let metadata = StoredPageMetadata {
                expires_at_unix_ms,
                min_timestamp_ns: i64::MIN,
                max_timestamp_ns: i64::MAX,
                row_count: 7,
                payload_bytes: 4096,
                level: 2,
                written_at_unix_ms: 1_234,
                leaf_rows,
            };
            let encoded = encode_metadata(&metadata).unwrap();
            assert_eq!(encoded[0], PAGE_METADATA_VERSION);
            assert_eq!(decode_metadata(&encoded).unwrap(), metadata);
        }
        let single = StoredPageMetadata {
            expires_at_unix_ms: None,
            min_timestamp_ns: 1,
            max_timestamp_ns: 2,
            row_count: 1,
            payload_bytes: 1,
            level: 0,
            written_at_unix_ms: 0,
            leaf_rows: Vec::new(),
        };
        let mut unknown = encode_metadata(&single).unwrap().to_vec();
        unknown[0] = PAGE_METADATA_VERSION + 1;
        assert!(decode_metadata(&unknown).is_err());
        let mismatched = StoredPageMetadata {
            leaf_rows: vec![2],
            ..single
        };
        assert!(decode_metadata(&encode_metadata(&mismatched).unwrap()).is_err());
    }

    #[test]
    fn leaves_address_consecutive_sequences_with_row_offsets() {
        let metadata = StoredPageMetadata {
            expires_at_unix_ms: None,
            min_timestamp_ns: 0,
            max_timestamp_ns: 0,
            row_count: 6,
            payload_bytes: 1,
            level: 1,
            written_at_unix_ms: 0,
            leaf_rows: vec![1, 2, 3],
        };
        assert_eq!(metadata.leaves(10), vec![(10, 0), (11, 1), (12, 3)]);
        assert_eq!(metadata.leaf_count(), 3);
        let single = StoredPageMetadata {
            leaf_rows: Vec::new(),
            ..metadata
        };
        assert_eq!(single.leaves(10), vec![(10, 0)]);
        assert_eq!(single.leaf_row_counts(), vec![6]);
    }

    #[test]
    fn tombstone_keys_roundtrip_and_differ_by_level() {
        let namespace = Namespace::new("tenant").unwrap();
        let page_id = PageId {
            timestamp_ns: -5,
            sequence: 9,
        };
        let key = tombstone_key(&namespace, 7, 3, page_id, 2);
        assert!(key.starts_with(&tombstone_prefix(&namespace, 7)));
        assert_eq!(decode_tombstone_key(&key).unwrap(), (3, page_id, 2));
        assert_ne!(
            payload_key(&namespace, 7, 3, page_id, 0),
            payload_key(&namespace, 7, 3, page_id, 1)
        );
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
