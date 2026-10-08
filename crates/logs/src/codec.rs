// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use common::serde::ensure_consumed;
use common::serde::scope::{KeyScope, ScopedSegmentExtractor};
use common::serde::varint::{var_u32, var_u64};

use crate::Namespace;
use crate::error::{Error, Result};
use crate::model::{Label, Labels, SegmentId, StreamFingerprint, StreamId};

pub(crate) const KEY_VERSION: u8 = 5;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::LOG;
const KEY_SCOPE: KeyScope = KeyScope::new(SUBSYSTEM, KEY_VERSION);
/// Persisted by SlateDB; renaming it makes existing databases unopenable.
pub(crate) const SEGMENT_EXTRACTOR_NAME: &str = "logs-log/v5";
pub(crate) const SEGMENT_EXTRACTOR: ScopedSegmentExtractor =
    ScopedSegmentExtractor::new(SEGMENT_EXTRACTOR_NAME, KEY_SCOPE);
/// Leading byte of run and object-directory values.
const OBJECT_FORMAT: u8 = 1;
const HAS_EXPIRY: u8 = 1;
/// Leading byte of forward-label values.
const LABELS_FORMAT: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RecordType {
    NextStreamId = 1,
    StreamDictionary = 2,
    ForwardLabels = 3,
    LabelPostings = 4,
    Run = 5,
    ObjectBlock = 6,
    NextObjectId = 7,
    SearchFieldStats = 8,
    SearchTermStats = 9,
    SearchTermDirectory = 10,
    SearchPostingBlock = 11,
    ObjectTombstone = 12,
    /// Discovery records of a whole rollup period; see [`rollup_prefix`].
    Rollup = 13,
    ObjectDirectory = 14,
}

/// Records under [`rollup_prefix`]. The period's discovery catalog shares
/// the prefix and starts with `CATALOG_RECORD_TYPE`, which none of these use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RollupRecord {
    NextStreamId = 1,
    StreamDictionary = 2,
    ForwardLabels = 3,
    LabelPostings = 4,
}

/// One stored object: the blocks written together by a flush or a merge.
/// IDs are allocated per segment in write order. A merge reuses its first
/// input's ID one level higher, so `(id, level)` never names two objects.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ObjectRef {
    pub id: u64,
    pub level: u8,
}

/// A written (level-0) object holding some of a stream's rows. Full-text
/// postings address rows by `(stream, leaf object, row in leaf)` and keep
/// doing so after merges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Leaf {
    pub object_id: u64,
    pub rows: u32,
}

/// One stream's contiguous blocks inside one object, read by queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StoredRun {
    pub level: u8,
    pub expires_at_unix_ms: Option<u64>,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub rows: u32,
    /// Encoded size of the run's blocks.
    pub bytes: u32,
    /// Total length of the run's log lines.
    pub line_bytes: u64,
    /// See [`ObjectRun::duplicate_free`].
    pub duplicate_free: bool,
    /// See [`ObjectRun::error_metadata`].
    pub error_metadata: bool,
    pub first_block: u32,
    pub blocks: u32,
}

impl StoredRun {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        is_expired(self.expires_at_unix_ms, unix_ms)
    }
}

/// A run as listed in its object's directory. Runs are ordered by stream ID
/// and their blocks follow each other, so a run's first block is the sum of
/// its predecessors' block counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectRun {
    pub stream_id: StreamId,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub rows: u32,
    pub bytes: u32,
    pub line_bytes: u64,
    /// No two of the run's rows share a timestamp, line and structured
    /// metadata, the rows queries deduplicate. False when that is unknown.
    pub duplicate_free: bool,
    /// Some row carries `__error__` structured metadata, which fails metric
    /// queries that keep it.
    pub error_metadata: bool,
    pub blocks: u32,
    /// The leaves whose rows the run concatenates, in order. Empty in a
    /// level-0 object, whose runs are their own single leaf.
    pub leaves: Vec<Leaf>,
}

/// Directory of one object, read by compaction and by full-text queries
/// that must map postings onto merged objects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredObject {
    pub expires_at_unix_ms: Option<u64>,
    pub written_at_unix_ms: u64,
    /// Count of consecutive level-0 IDs the object covers from its own.
    pub span: u64,
    pub runs: Vec<ObjectRun>,
}

impl StoredObject {
    pub(crate) fn is_expired_at(&self, unix_ms: u64) -> bool {
        is_expired(self.expires_at_unix_ms, unix_ms)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.runs.iter().map(|run| u64::from(run.rows)).sum()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.runs.iter().map(|run| u64::from(run.bytes)).sum()
    }

    pub(crate) fn blocks(&self) -> u32 {
        self.runs
            .iter()
            .fold(0u32, |sum, run| sum.saturating_add(run.blocks))
    }

    /// The leaves of `run` in an object with ID `object_id`.
    pub(crate) fn leaves_of(run: &ObjectRun, object_id: u64) -> Vec<Leaf> {
        if run.leaves.is_empty() {
            vec![Leaf {
                object_id,
                rows: run.rows,
            }]
        } else {
            run.leaves.clone()
        }
    }

    /// The query-side record of every run, with its first block.
    pub(crate) fn stored_runs(&self, level: u8) -> Vec<(StreamId, StoredRun)> {
        let mut first_block = 0u32;
        self.runs
            .iter()
            .map(|run| {
                let stored = StoredRun {
                    level,
                    expires_at_unix_ms: self.expires_at_unix_ms,
                    min_timestamp_ns: run.min_timestamp_ns,
                    max_timestamp_ns: run.max_timestamp_ns,
                    rows: run.rows,
                    bytes: run.bytes,
                    line_bytes: run.line_bytes,
                    duplicate_free: run.duplicate_free,
                    error_metadata: run.error_metadata,
                    first_block,
                    blocks: run.blocks,
                };
                first_block = first_block.saturating_add(run.blocks);
                (run.stream_id, stored)
            })
            .collect()
    }
}

fn is_expired(expires_at_unix_ms: Option<u64>, unix_ms: u64) -> bool {
    expires_at_unix_ms.is_some_and(|expires_at| unix_ms >= expires_at)
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

/// Discovery records summarising every segment of the rollup period starting
/// at `period`. They live in the scope of the period's first segment, so the
/// segment extractor routes them like any other record, and use period-local
/// stream IDs unrelated to that segment's.
pub(crate) fn rollup_prefix(namespace: &Namespace, period: SegmentId) -> Bytes {
    record_prefix(namespace, period, RecordType::Rollup).freeze()
}

fn rollup_record_prefix(
    namespace: &Namespace,
    period: SegmentId,
    record: RollupRecord,
) -> BytesMut {
    let mut bytes = record_prefix(namespace, period, RecordType::Rollup);
    bytes.put_u8(record as u8);
    bytes
}

pub(crate) fn rollup_next_stream_id_key(namespace: &Namespace, period: SegmentId) -> Bytes {
    rollup_record_prefix(namespace, period, RollupRecord::NextStreamId).freeze()
}

pub(crate) fn rollup_dictionary_key(
    namespace: &Namespace,
    period: SegmentId,
    fingerprint: StreamFingerprint,
) -> Bytes {
    let mut bytes = rollup_record_prefix(namespace, period, RollupRecord::StreamDictionary);
    bytes.extend_from_slice(&fingerprint);
    bytes.freeze()
}

pub(crate) fn rollup_dictionary_prefix(namespace: &Namespace, period: SegmentId) -> Bytes {
    rollup_record_prefix(namespace, period, RollupRecord::StreamDictionary).freeze()
}

pub(crate) fn rollup_forward_key(
    namespace: &Namespace,
    period: SegmentId,
    stream_id: StreamId,
) -> Bytes {
    let mut bytes = rollup_record_prefix(namespace, period, RollupRecord::ForwardLabels);
    bytes.put_u32(stream_id);
    bytes.freeze()
}

pub(crate) fn rollup_forward_prefix(namespace: &Namespace, period: SegmentId) -> Bytes {
    rollup_record_prefix(namespace, period, RollupRecord::ForwardLabels).freeze()
}

/// Stream ID of a key under [`rollup_forward_prefix`] of length `prefix_len`.
pub(crate) fn decode_rollup_forward_key(bytes: &[u8], prefix_len: usize) -> Result<StreamId> {
    bytes
        .get(prefix_len..)
        .and_then(|suffix| <[u8; 4]>::try_from(suffix).ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| Error::Corrupt("invalid rollup forward-label key".to_owned()))
}

pub(crate) fn rollup_posting_key(namespace: &Namespace, period: SegmentId, label: &Label) -> Bytes {
    let mut bytes = rollup_record_prefix(namespace, period, RollupRecord::LabelPostings);
    common::serde::terminated_bytes::serialize(label.name.as_bytes(), &mut bytes);
    bytes.extend_from_slice(label.value.as_bytes());
    bytes.freeze()
}

pub(crate) fn next_stream_id_key(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::NextStreamId).freeze()
}

pub(crate) fn next_object_id_key(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::NextObjectId).freeze()
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

pub(crate) fn dictionary_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::StreamDictionary).freeze()
}

/// Fingerprint of a key under a stream-dictionary prefix of length
/// `prefix_len`, segment or rollup.
pub(crate) fn decode_dictionary_key(bytes: &[u8], prefix_len: usize) -> Result<StreamFingerprint> {
    bytes
        .get(prefix_len..)
        .and_then(|suffix| StreamFingerprint::try_from(suffix).ok())
        .ok_or_else(|| Error::Corrupt("invalid stream dictionary key".to_owned()))
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

/// One directory fragment of `term`, keyed by its first block's ID.
pub(crate) fn term_directory_key(
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    first_block: u64,
) -> Bytes {
    let mut bytes = term_directory_prefix_buf(namespace, segment, term);
    bytes.put_u64(first_block);
    bytes.freeze()
}

/// Every directory fragment of `term`, in block order.
pub(crate) fn term_directory_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
) -> Bytes {
    term_directory_prefix_buf(namespace, segment, term).freeze()
}

fn term_directory_prefix_buf(namespace: &Namespace, segment: SegmentId, term: &str) -> BytesMut {
    let mut bytes = record_prefix(namespace, segment, RecordType::SearchTermDirectory);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes
}

pub(crate) fn term_posting_block_key(
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    block: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::SearchPostingBlock);
    common::serde::terminated_bytes::serialize(term.as_bytes(), &mut bytes);
    bytes.put_u64(block);
    bytes.freeze()
}

/// The run of `stream_id` in object `object_id`. A merged object reuses its
/// first input's ID, so its runs replace that input's runs in place.
pub(crate) fn run_key(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
    object_id: u64,
) -> Bytes {
    let mut bytes = record_prefix(namespace, segment, RecordType::Run);
    bytes.put_u32(stream_id);
    bytes.put_u64(object_id);
    bytes.freeze()
}

/// Runs of every stream in a segment.
pub(crate) fn segment_run_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::Run).freeze()
}

pub(crate) fn stream_run_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    stream_id: StreamId,
) -> Bytes {
    let mut prefix = record_prefix(namespace, segment, RecordType::Run);
    prefix.put_u32(stream_id);
    prefix.freeze()
}

/// `(stream ID, object ID)` of a run key.
pub(crate) fn decode_run_key(bytes: &[u8]) -> Result<(StreamId, u64)> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != RecordType::Run || bytes.len() != offset + 12 {
        return Err(Error::Corrupt("invalid run key".to_owned()));
    }
    Ok((
        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()),
        u64::from_be_bytes(bytes[offset + 4..offset + 12].try_into().unwrap()),
    ))
}

fn object_key(
    namespace: &Namespace,
    segment: SegmentId,
    record_type: RecordType,
    object: ObjectRef,
) -> BytesMut {
    let mut bytes = record_prefix(namespace, segment, record_type);
    bytes.put_u64(object.id);
    bytes.put_u8(object.level);
    bytes
}

fn decode_object_key(bytes: &[u8], expected: RecordType) -> Result<ObjectRef> {
    let (_, _, record_type, offset) = parse_record_prefix(bytes)?;
    if record_type != expected || bytes.len() != offset + 9 {
        return Err(Error::Corrupt(format!("invalid {expected:?} key")));
    }
    Ok(ObjectRef {
        id: u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap()),
        level: bytes[offset + 8],
    })
}

/// The two values a block is stored as. Every meta value of an object sorts
/// before its line values, so a read that needs no lines scans one compact
/// key range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum BlockGroup {
    /// Timestamps, line lengths and structured metadata.
    Meta = 0,
    Lines = 1,
}

impl BlockGroup {
    pub(crate) const ALL: [Self; 2] = [Self::Meta, Self::Lines];
}

/// One group of an object's blocks, ordered by index.
pub(crate) fn object_blocks_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    object: ObjectRef,
    group: BlockGroup,
) -> Bytes {
    let mut bytes = object_key(namespace, segment, RecordType::ObjectBlock, object);
    bytes.put_u8(group as u8);
    bytes.freeze()
}

pub(crate) fn block_key(
    namespace: &Namespace,
    segment: SegmentId,
    object: ObjectRef,
    group: BlockGroup,
    block: u32,
) -> Bytes {
    let mut bytes = object_key(namespace, segment, RecordType::ObjectBlock, object);
    bytes.put_u8(group as u8);
    bytes.put_u32(block);
    bytes.freeze()
}

/// Block index of a key under an [`object_blocks_prefix`] of `prefix_len`.
pub(crate) fn decode_block_index(bytes: &[u8], prefix_len: usize) -> Result<u32> {
    bytes
        .get(prefix_len..)
        .and_then(|suffix| <[u8; 4]>::try_from(suffix).ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| Error::Corrupt("invalid object block key".to_owned()))
}

pub(crate) fn directory_key(namespace: &Namespace, segment: SegmentId, object: ObjectRef) -> Bytes {
    object_key(namespace, segment, RecordType::ObjectDirectory, object).freeze()
}

pub(crate) fn directory_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::ObjectDirectory).freeze()
}

pub(crate) fn decode_directory_key(bytes: &[u8]) -> Result<ObjectRef> {
    decode_object_key(bytes, RecordType::ObjectDirectory)
}

/// Marks a replaced object for deletion once in-flight readers are done.
pub(crate) fn tombstone_key(namespace: &Namespace, segment: SegmentId, object: ObjectRef) -> Bytes {
    object_key(namespace, segment, RecordType::ObjectTombstone, object).freeze()
}

pub(crate) fn tombstone_prefix(namespace: &Namespace, segment: SegmentId) -> Bytes {
    record_prefix(namespace, segment, RecordType::ObjectTombstone).freeze()
}

pub(crate) fn decode_tombstone_key(bytes: &[u8]) -> Result<ObjectRef> {
    decode_object_key(bytes, RecordType::ObjectTombstone)
}

/// Tombstone value: the deletion deadline and the object's block count.
pub(crate) fn encode_tombstone(deadline_unix_ms: u64, blocks: u32) -> Bytes {
    let mut bytes = BytesMut::with_capacity(12);
    bytes.put_u64(deadline_unix_ms);
    bytes.put_u32(blocks);
    bytes.freeze()
}

pub(crate) fn decode_tombstone(bytes: &[u8]) -> Result<(u64, u32)> {
    let bytes: &[u8; 12] = bytes
        .try_into()
        .map_err(|_| Error::Corrupt("tombstone must contain twelve bytes".to_owned()))?;
    Ok((
        u64::from_be_bytes(bytes[..8].try_into().unwrap()),
        u32::from_be_bytes(bytes[8..].try_into().unwrap()),
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

pub(crate) fn encode_object_id(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}

pub(crate) fn decode_object_id(bytes: &[u8]) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| Error::Corrupt("object id must contain eight bytes".to_owned()))
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

pub(crate) fn encode_run(run: &StoredRun) -> Result<Bytes> {
    let mut bytes = BytesMut::with_capacity(32);
    bytes.put_u8(OBJECT_FORMAT);
    put_expiry(run.expires_at_unix_ms, &mut bytes);
    put_time_range(run.min_timestamp_ns, run.max_timestamp_ns, &mut bytes)?;
    var_u32::serialize(run.rows, &mut bytes);
    var_u32::serialize(run.bytes, &mut bytes);
    var_u64::serialize(run.line_bytes, &mut bytes);
    put_run_flags(run.duplicate_free, run.error_metadata, &mut bytes);
    bytes.put_u8(run.level);
    var_u32::serialize(run.first_block, &mut bytes);
    var_u32::serialize(run.blocks, &mut bytes);
    Ok(bytes.freeze())
}

pub(crate) fn decode_run(bytes: &[u8]) -> Result<StoredRun> {
    let mut buf = object_body(bytes, "run")?;
    let expires_at_unix_ms = read_expiry(&mut buf)?;
    let (min_timestamp_ns, max_timestamp_ns) = read_time_range(&mut buf)?;
    let rows = var_u32::deserialize(&mut buf)?;
    let bytes = var_u32::deserialize(&mut buf)?;
    let line_bytes = var_u64::deserialize(&mut buf)?;
    let (duplicate_free, error_metadata) = read_run_flags(&mut buf, "run")?;
    let level = read_u8(&mut buf)?;
    let first_block = var_u32::deserialize(&mut buf)?;
    let blocks = var_u32::deserialize(&mut buf)?;
    ensure_consumed(buf, "run")?;
    if blocks == 0 || first_block.checked_add(blocks).is_none() {
        return Err(Error::Corrupt("run has invalid block bounds".to_owned()));
    }
    Ok(StoredRun {
        level,
        expires_at_unix_ms,
        min_timestamp_ns,
        max_timestamp_ns,
        rows,
        bytes,
        line_bytes,
        duplicate_free,
        error_metadata,
        first_block,
        blocks,
    })
}

/// Runs must be ordered by strictly increasing stream ID; IDs are
/// delta-encoded.
pub(crate) fn encode_object(object: &StoredObject) -> Result<Bytes> {
    let mut bytes = BytesMut::with_capacity(16 + object.runs.len() * 24);
    bytes.put_u8(OBJECT_FORMAT);
    put_expiry(object.expires_at_unix_ms, &mut bytes);
    var_u64::serialize(object.written_at_unix_ms, &mut bytes);
    var_u64::serialize(object.span, &mut bytes);
    var_u32::serialize(value_len(object.runs.len())?, &mut bytes);
    let mut previous: Option<StreamId> = None;
    for run in &object.runs {
        let delta = match previous {
            None => run.stream_id,
            Some(previous) if run.stream_id > previous => run.stream_id - previous,
            Some(_) => {
                return Err(Error::Invalid(
                    "object runs must be ordered by stream".to_owned(),
                ));
            }
        };
        previous = Some(run.stream_id);
        var_u32::serialize(delta, &mut bytes);
        put_time_range(run.min_timestamp_ns, run.max_timestamp_ns, &mut bytes)?;
        var_u32::serialize(run.rows, &mut bytes);
        var_u32::serialize(run.bytes, &mut bytes);
        var_u64::serialize(run.line_bytes, &mut bytes);
        put_run_flags(run.duplicate_free, run.error_metadata, &mut bytes);
        var_u32::serialize(run.blocks, &mut bytes);
        var_u32::serialize(value_len(run.leaves.len())?, &mut bytes);
        for leaf in &run.leaves {
            var_u64::serialize(leaf.object_id, &mut bytes);
            var_u32::serialize(leaf.rows, &mut bytes);
        }
    }
    Ok(bytes.freeze())
}

pub(crate) fn decode_object(bytes: &[u8]) -> Result<StoredObject> {
    let mut buf = object_body(bytes, "object directory")?;
    let expires_at_unix_ms = read_expiry(&mut buf)?;
    let written_at_unix_ms = var_u64::deserialize(&mut buf)?;
    let span = var_u64::deserialize(&mut buf)?;
    let count = var_u32::deserialize(&mut buf)? as usize;
    let mut runs = Vec::with_capacity(count.min(buf.len() / 8));
    let mut stream_id = 0u32;
    for index in 0..count {
        let delta = var_u32::deserialize(&mut buf)?;
        stream_id = if index == 0 {
            delta
        } else {
            stream_id
                .checked_add(delta)
                .filter(|_| delta > 0)
                .ok_or_else(|| Error::Corrupt("object runs are out of order".to_owned()))?
        };
        let (min_timestamp_ns, max_timestamp_ns) = read_time_range(&mut buf)?;
        let rows = var_u32::deserialize(&mut buf)?;
        let bytes = var_u32::deserialize(&mut buf)?;
        let line_bytes = var_u64::deserialize(&mut buf)?;
        let (duplicate_free, error_metadata) = read_run_flags(&mut buf, "object directory")?;
        let blocks = var_u32::deserialize(&mut buf)?;
        let leaf_count = var_u32::deserialize(&mut buf)? as usize;
        let mut leaves = Vec::with_capacity(leaf_count.min(buf.len() / 2));
        for _ in 0..leaf_count {
            leaves.push(Leaf {
                object_id: var_u64::deserialize(&mut buf)?,
                rows: var_u32::deserialize(&mut buf)?,
            });
        }
        if !leaves.is_empty()
            && leaves
                .iter()
                .try_fold(0u32, |sum, leaf| sum.checked_add(leaf.rows))
                != Some(rows)
        {
            return Err(Error::Corrupt(
                "run leaf rows do not sum to its row count".to_owned(),
            ));
        }
        runs.push(ObjectRun {
            stream_id,
            min_timestamp_ns,
            max_timestamp_ns,
            rows,
            bytes,
            line_bytes,
            duplicate_free,
            error_metadata,
            blocks,
            leaves,
        });
    }
    ensure_consumed(buf, "object directory")?;
    Ok(StoredObject {
        expires_at_unix_ms,
        written_at_unix_ms,
        span,
        runs,
    })
}

fn object_body<'a>(bytes: &'a [u8], what: &str) -> Result<&'a [u8]> {
    match bytes.first() {
        Some(&OBJECT_FORMAT) => Ok(&bytes[1..]),
        Some(format) => Err(Error::Corrupt(format!(
            "unsupported {what} format {format}"
        ))),
        None => Err(Error::Corrupt(format!("empty {what}"))),
    }
}

fn put_expiry(expires_at_unix_ms: Option<u64>, bytes: &mut BytesMut) {
    match expires_at_unix_ms {
        Some(expires_at) => {
            bytes.put_u8(HAS_EXPIRY);
            var_u64::serialize(expires_at, bytes);
        }
        None => bytes.put_u8(0),
    }
}

fn read_expiry(buf: &mut &[u8]) -> Result<Option<u64>> {
    match read_u8(buf)? {
        0 => Ok(None),
        HAS_EXPIRY => Ok(Some(var_u64::deserialize(buf)?)),
        flags => Err(Error::Corrupt(format!("unknown expiry flags {flags}"))),
    }
}

fn put_time_range(min: i64, max: i64, bytes: &mut BytesMut) -> Result<()> {
    if max < min {
        return Err(Error::Invalid(
            "max timestamp precedes min timestamp".to_owned(),
        ));
    }
    bytes.put_i64(min);
    var_u64::serialize(max.abs_diff(min), bytes);
    Ok(())
}

fn read_time_range(buf: &mut &[u8]) -> Result<(i64, i64)> {
    let (min, rest) = buf
        .split_first_chunk::<8>()
        .ok_or_else(|| Error::Corrupt("truncated timestamp".to_owned()))?;
    *buf = rest;
    let min = i64::from_be_bytes(*min);
    let max = min
        .checked_add_unsigned(var_u64::deserialize(buf)?)
        .ok_or_else(|| Error::Corrupt("max timestamp overflows".to_owned()))?;
    Ok((min, max))
}

fn read_u8(buf: &mut &[u8]) -> Result<u8> {
    let (&value, rest) = buf
        .split_first()
        .ok_or_else(|| Error::Corrupt("truncated value".to_owned()))?;
    *buf = rest;
    Ok(value)
}

const DUPLICATE_FREE: u8 = 1;
const ERROR_METADATA: u8 = 2;

fn put_run_flags(duplicate_free: bool, error_metadata: bool, bytes: &mut BytesMut) {
    bytes.put_u8(
        if duplicate_free { DUPLICATE_FREE } else { 0 }
            | if error_metadata { ERROR_METADATA } else { 0 },
    );
}

fn read_run_flags(buf: &mut &[u8], what: &str) -> Result<(bool, bool)> {
    match read_u8(buf)? {
        flags if flags & !(DUPLICATE_FREE | ERROR_METADATA) == 0 => {
            Ok((flags & DUPLICATE_FREE != 0, flags & ERROR_METADATA != 0))
        }
        flags => Err(Error::Corrupt(format!("invalid {what} flags {flags}"))),
    }
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

/// Records written as merge operands, combined by [`crate::merge::LogsMergeOperator`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MergeKind {
    /// Segment and rollup label postings: stream-ID bitmaps, unioned.
    StreamPostings,
    /// Search field statistics, summed.
    FieldStats,
    /// Search term statistics, summed.
    TermStats,
}

pub(crate) fn merge_kind(key: &[u8]) -> Option<MergeKind> {
    let (_, _, record_type, offset) = parse_record_prefix(key).ok()?;
    match record_type {
        RecordType::LabelPostings => Some(MergeKind::StreamPostings),
        RecordType::Rollup if key.get(offset) == Some(&(RollupRecord::LabelPostings as u8)) => {
            Some(MergeKind::StreamPostings)
        }
        RecordType::SearchFieldStats => Some(MergeKind::FieldStats),
        RecordType::SearchTermStats => Some(MergeKind::TermStats),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn record_type_prefix(
    namespace: &Namespace,
    segment: SegmentId,
    record_type: RecordType,
) -> Bytes {
    record_prefix(namespace, segment, record_type).freeze()
}

/// The record-type byte after `key`'s scope, if it has one.
#[cfg(test)]
pub(crate) fn key_record_type(key: &[u8]) -> Option<u8> {
    key.get(KEY_SCOPE.prefix_len(key)?).copied()
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
        5 => RecordType::Run,
        6 => RecordType::ObjectBlock,
        7 => RecordType::NextObjectId,
        8 => RecordType::SearchFieldStats,
        9 => RecordType::SearchTermStats,
        10 => RecordType::SearchTermDirectory,
        11 => RecordType::SearchPostingBlock,
        12 => RecordType::ObjectTombstone,
        13 => RecordType::Rollup,
        14 => RecordType::ObjectDirectory,
        value => return Err(Error::Corrupt(format!("unknown record type {value}"))),
    };
    Ok((namespace, segment, record_type, scope_len + 1))
}

#[cfg(test)]
mod tests {
    use slatedb::{PrefixExtractor, PrefixTarget};

    use super::*;

    #[test]
    fn segment_extractor_name_is_stable() {
        assert_eq!(SEGMENT_EXTRACTOR.name(), "logs-log/v5");
    }

    #[test]
    fn merge_records_are_classified_by_key() {
        let namespace = Namespace::new("tenant").unwrap();
        let label = Label::new("service", "api");
        assert_eq!(
            merge_kind(&posting_key(&namespace, 0, &label)),
            Some(MergeKind::StreamPostings)
        );
        assert_eq!(
            merge_kind(&rollup_posting_key(&namespace, 0, &label)),
            Some(MergeKind::StreamPostings)
        );
        assert_eq!(
            merge_kind(&field_stats_key(&namespace, 0)),
            Some(MergeKind::FieldStats)
        );
        assert_eq!(
            merge_kind(&term_stats_key(&namespace, 0, "error")),
            Some(MergeKind::TermStats)
        );
        assert_eq!(
            merge_kind(&rollup_dictionary_key(&namespace, 0, [1; 16])),
            None
        );
        assert_eq!(merge_kind(&forward_key(&namespace, 0, 1)), None);
    }

    #[test]
    fn dictionary_keys_decode_their_fingerprint() {
        let namespace = Namespace::new("tenant").unwrap();
        let prefix = dictionary_prefix(&namespace, 3);
        let key = dictionary_key(&namespace, 3, [7; 16]);
        assert!(key.starts_with(&prefix));
        assert_eq!(decode_dictionary_key(&key, prefix.len()).unwrap(), [7; 16]);
        let rollup = rollup_dictionary_prefix(&namespace, 3);
        let key = rollup_dictionary_key(&namespace, 3, [9; 16]);
        assert!(key.starts_with(&rollup));
        assert_eq!(decode_dictionary_key(&key, rollup.len()).unwrap(), [9; 16]);
        assert!(decode_dictionary_key(&key[..key.len() - 1], rollup.len()).is_err());
    }

    #[test]
    fn rollup_records_route_with_their_first_segment_and_stay_disjoint() {
        let namespace = Namespace::new("tenant").unwrap();
        let period = 7_200;
        let prefix = rollup_prefix(&namespace, period);
        let segment = segment_prefix(&namespace, period);
        let label = Label::new("service", "api");
        let keys = [
            rollup_next_stream_id_key(&namespace, period),
            rollup_dictionary_key(&namespace, period, [3; 16]),
            rollup_forward_key(&namespace, period, 9),
            rollup_posting_key(&namespace, period, &label),
        ];
        for key in &keys {
            assert!(key.starts_with(&prefix));
            assert_eq!(
                SEGMENT_EXTRACTOR.prefix_len(&PrefixTarget::Point(key.clone())),
                Some(segment.len())
            );
        }
        // The catalog record type byte follows the rollup prefix directly.
        assert!(keys.iter().all(|key| key[prefix.len()] != u8::MAX));
        assert!(!forward_key(&namespace, period, 9).starts_with(&prefix));
        assert!(!posting_key(&namespace, period, &label).starts_with(&prefix));
        let forward = rollup_forward_prefix(&namespace, period);
        assert_eq!(
            decode_rollup_forward_key(&keys[2], forward.len()).unwrap(),
            9
        );
        assert!(decode_rollup_forward_key(&keys[3], forward.len()).is_err());
    }

    #[test]
    fn keys_group_by_namespace_then_segment() {
        let namespace = Namespace::new("tenant").unwrap();
        let first = run_key(&namespace, -10, 2, 8);
        let second = run_key(&namespace, 10, 1, 7);
        assert!(first < second);
        assert!(first.starts_with(&segment_prefix(&namespace, -10)));
        assert!(first.starts_with(&stream_run_prefix(&namespace, -10, 2)));
        assert_eq!(decode_run_key(&first).unwrap(), (2, 8));
        // A stream's runs sort by object ID.
        assert!(run_key(&namespace, 0, 3, 10) < run_key(&namespace, 0, 3, 11));
    }

    #[test]
    fn object_blocks_are_contiguous_and_ordered() {
        let namespace = Namespace::default();
        let object = ObjectRef { id: 5, level: 1 };
        for group in BlockGroup::ALL {
            let prefix = object_blocks_prefix(&namespace, 0, object, group);
            let keys = [0, 1, 256].map(|block| block_key(&namespace, 0, object, group, block));
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
            for (key, block) in keys.iter().zip([0, 1, 256]) {
                assert!(key.starts_with(&prefix));
                assert_eq!(decode_block_index(key, prefix.len()).unwrap(), block);
            }
            let other_level = block_key(&namespace, 0, ObjectRef { id: 5, level: 2 }, group, 0);
            assert!(!other_level.starts_with(&prefix));
        }
        // Every meta block precedes every lines block of the same object.
        assert!(
            block_key(&namespace, 0, object, BlockGroup::Meta, u32::MAX)
                < block_key(&namespace, 0, object, BlockGroup::Lines, 0)
        );
        let key = directory_key(&namespace, 0, object);
        assert!(key.starts_with(&directory_prefix(&namespace, 0)));
        assert_eq!(decode_directory_key(&key).unwrap(), object);
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
    fn runs_roundtrip_in_binary() {
        for expires_at_unix_ms in [None, Some(0), Some(u64::MAX)] {
            let run = StoredRun {
                level: 2,
                expires_at_unix_ms,
                min_timestamp_ns: i64::MIN,
                max_timestamp_ns: i64::MAX,
                rows: 7,
                bytes: 4096,
                line_bytes: u64::MAX,
                duplicate_free: expires_at_unix_ms.is_some(),
                error_metadata: expires_at_unix_ms.is_none(),
                first_block: 3,
                blocks: 2,
            };
            let encoded = encode_run(&run).unwrap();
            assert_eq!(encoded[0], OBJECT_FORMAT);
            assert_eq!(decode_run(&encoded).unwrap(), run);
        }
        let run = StoredRun {
            level: 0,
            expires_at_unix_ms: None,
            min_timestamp_ns: 1,
            max_timestamp_ns: 2,
            rows: 1,
            bytes: 1,
            line_bytes: 1,
            duplicate_free: true,
            error_metadata: false,
            first_block: 0,
            blocks: 1,
        };
        let mut unknown = encode_run(&run).unwrap().to_vec();
        unknown[0] = OBJECT_FORMAT + 1;
        assert!(decode_run(&unknown).is_err());
        let empty = StoredRun { blocks: 0, ..run };
        assert!(decode_run(&encode_run(&empty).unwrap()).is_err());
    }

    #[test]
    fn object_directories_roundtrip_and_derive_runs() {
        let run = |stream_id, rows, blocks, leaves| ObjectRun {
            stream_id,
            min_timestamp_ns: -5,
            max_timestamp_ns: 9,
            rows,
            bytes: rows * 10,
            line_bytes: u64::from(rows) * 40,
            duplicate_free: stream_id % 2 == 0,
            error_metadata: stream_id % 3 == 0,
            blocks,
            leaves,
        };
        let object = StoredObject {
            expires_at_unix_ms: Some(77),
            written_at_unix_ms: 1_234,
            span: 3,
            runs: vec![
                run(2, 3, 1, Vec::new()),
                run(
                    9,
                    5,
                    2,
                    vec![
                        Leaf {
                            object_id: 4,
                            rows: 2,
                        },
                        Leaf {
                            object_id: 6,
                            rows: 3,
                        },
                    ],
                ),
            ],
        };
        let encoded = encode_object(&object).unwrap();
        assert_eq!(decode_object(&encoded).unwrap(), object);
        assert_eq!((object.rows(), object.bytes(), object.blocks()), (8, 80, 3));
        let stored = object.stored_runs(1);
        assert_eq!(
            stored
                .iter()
                .map(|(stream, run)| (*stream, run.first_block, run.blocks, run.level))
                .collect::<Vec<_>>(),
            [(2, 0, 1, 1), (9, 1, 2, 1)]
        );
        assert_eq!(
            StoredObject::leaves_of(&object.runs[0], 4),
            [Leaf {
                object_id: 4,
                rows: 3
            }]
        );

        let unordered = StoredObject {
            runs: vec![run(9, 1, 1, Vec::new()), run(2, 1, 1, Vec::new())],
            ..object.clone()
        };
        assert!(encode_object(&unordered).is_err());
        let mismatched = StoredObject {
            runs: vec![run(
                1,
                4,
                1,
                vec![Leaf {
                    object_id: 0,
                    rows: 3,
                }],
            )],
            ..object
        };
        assert!(decode_object(&encode_object(&mismatched).unwrap()).is_err());
    }

    #[test]
    fn tombstones_roundtrip_and_differ_by_level() {
        let namespace = Namespace::new("tenant").unwrap();
        let object = ObjectRef { id: 9, level: 2 };
        let key = tombstone_key(&namespace, 7, object);
        assert!(key.starts_with(&tombstone_prefix(&namespace, 7)));
        assert_eq!(decode_tombstone_key(&key).unwrap(), object);
        assert_ne!(
            key,
            tombstone_key(&namespace, 7, ObjectRef { id: 9, level: 1 })
        );
        assert_eq!(
            decode_tombstone(&encode_tombstone(123, 4)).unwrap(),
            (123, 4)
        );
        assert!(decode_tombstone(&[0; 8]).is_err());
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
    fn values_reject_unknown_formats() {
        let error = decode_run(&[OBJECT_FORMAT + 1, 0]).unwrap_err();
        assert!(error.to_string().contains("unsupported run format"));
        let error = decode_object(&[]).unwrap_err();
        assert!(error.to_string().contains("empty object directory"));
    }
}
