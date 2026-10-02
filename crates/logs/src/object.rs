// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Row blocks and the multi-stream objects built from them.
//!
//! A block holds rows of one stream and is stored under its own key, so a
//! query reads exactly the blocks of the streams it selects. An object packs
//! the blocks of many streams, ordered by stream, so a query selecting most
//! of a segment's streams reads each object with one range scan.

use std::collections::BTreeMap;
use std::ops::{Bound, Range};

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::storage::{RecordOp, StorageRead, Ttl};

use crate::Namespace;
use crate::codec::{
    Leaf, ObjectRef, ObjectRun, StoredObject, block_key, decode_block_index, directory_key,
    encode_object, encode_run, object_blocks_prefix, run_key,
};
use crate::config::PageConfig;
use crate::error::{Error, Result};
use crate::model::{Field, Fields, LogEntry, SegmentId, StreamId};

const BLOCK_FORMAT: u8 = 1;
const BLOCK_HEADER_LEN: usize = 25;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockHeader {
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub rows: u32,
    uncompressed_len: u32,
}

impl BlockHeader {
    pub(crate) fn overlaps(&self, start_ns: i64, end_ns: i64) -> bool {
        self.max_timestamp_ns >= start_ns && self.min_timestamp_ns <= end_ns
    }
}

/// `0x01 │ min ts │ max ts │ rows │ uncompressed length │ Snappy rows`.
pub(crate) fn encode_block(entries: &[LogEntry]) -> Result<Bytes> {
    let (Some(min), Some(max)) = (
        entries.iter().map(|entry| entry.timestamp_ns).min(),
        entries.iter().map(|entry| entry.timestamp_ns).max(),
    ) else {
        return Err(Error::Invalid("cannot encode an empty block".to_owned()));
    };
    let raw = encode_rows(entries)?;
    let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
    let mut bytes = BytesMut::with_capacity(BLOCK_HEADER_LEN + compressed.len());
    bytes.put_u8(BLOCK_FORMAT);
    bytes.put_i64(min);
    bytes.put_i64(max);
    bytes.put_u32(to_u32(entries.len(), "block row count")?);
    bytes.put_u32(to_u32(raw.len(), "uncompressed block length")?);
    bytes.extend_from_slice(&compressed);
    Ok(bytes.freeze())
}

pub(crate) fn block_header(bytes: &[u8]) -> Result<BlockHeader> {
    if bytes.len() < BLOCK_HEADER_LEN {
        return Err(Error::Corrupt("truncated block header".to_owned()));
    }
    if bytes[0] != BLOCK_FORMAT {
        return Err(Error::Corrupt(format!(
            "unsupported block format {}",
            bytes[0]
        )));
    }
    let i64_at = |offset: usize| i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
    let u32_at = |offset: usize| u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
    Ok(BlockHeader {
        min_timestamp_ns: i64_at(1),
        max_timestamp_ns: i64_at(9),
        rows: u32_at(17),
        uncompressed_len: u32_at(21),
    })
}

/// Rows of a block in stored order.
pub(crate) fn decode_block(bytes: &[u8]) -> Result<Vec<LogEntry>> {
    let header = block_header(bytes)?;
    let raw = snap::raw::Decoder::new().decompress_vec(&bytes[BLOCK_HEADER_LEN..])?;
    if raw.len() != header.uncompressed_len as usize {
        return Err(Error::Corrupt(
            "decompressed block length mismatch".to_owned(),
        ));
    }
    decode_rows(&raw, header.rows)
}

/// One stream's run within a [`BuiltObject`].
#[derive(Debug)]
pub(crate) struct BuiltRun {
    pub stream_id: StreamId,
    /// The run's rows in stored order, for objects built from written rows;
    /// empty for merged objects.
    pub entries: Vec<LogEntry>,
    pub rows: u32,
    /// See [`crate::codec::ObjectRun::leaves`].
    pub leaves: Vec<Leaf>,
    pub blocks: u32,
    pub bytes: u32,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
}

#[derive(Debug, Default)]
pub(crate) struct BuiltObject {
    /// Encoded blocks, run after run.
    pub blocks: Vec<Bytes>,
    pub runs: Vec<BuiltRun>,
}

/// Packs written rows, pushed in increasing stream order, into objects, cut
/// once an object reaches the page size or row limit, so one large stream
/// may span objects.
pub(crate) struct ObjectBuilder {
    config: PageConfig,
    objects: Vec<BuiltObject>,
    current: BuiltObject,
    rows: usize,
    estimated_bytes: usize,
}

impl ObjectBuilder {
    pub(crate) fn new(config: PageConfig) -> Result<Self> {
        validate(&config)?;
        Ok(Self {
            config,
            objects: Vec::new(),
            current: BuiltObject::default(),
            rows: 0,
            estimated_bytes: 0,
        })
    }

    pub(crate) fn push_run(&mut self, stream_id: StreamId, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if self
            .current
            .runs
            .last()
            .is_some_and(|last| last.stream_id >= stream_id)
        {
            return Err(Error::Invalid(
                "object runs must be pushed in stream order".to_owned(),
            ));
        }
        let mut run = Vec::new();
        for entry in entries {
            let size = estimated_row_size(&entry);
            if self.rows > 0
                && (self.rows >= self.config.max_rows
                    || self.estimated_bytes.saturating_add(size) > self.config.target_size_bytes)
            {
                if !run.is_empty() {
                    self.close_run(stream_id, std::mem::take(&mut run))?;
                }
                self.cut();
            }
            self.rows += 1;
            self.estimated_bytes = self.estimated_bytes.saturating_add(size);
            run.push(entry);
        }
        if !run.is_empty() {
            self.close_run(stream_id, run)?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Vec<BuiltObject> {
        self.cut();
        self.objects
    }

    fn close_run(&mut self, stream_id: StreamId, entries: Vec<LogEntry>) -> Result<()> {
        let mut run = RunBlocks::new(stream_id);
        for chunk in entries.chunks(self.config.rows_per_block) {
            run.push(encode_block(chunk)?)?;
        }
        run.finish_into(&mut self.current, entries, Vec::new())
    }

    fn cut(&mut self) {
        if !self.current.runs.is_empty() {
            self.objects.push(std::mem::take(&mut self.current));
        }
        self.rows = 0;
        self.estimated_bytes = 0;
    }
}

fn validate(config: &PageConfig) -> Result<()> {
    if config.target_size_bytes == 0 || config.max_rows == 0 || config.rows_per_block == 0 {
        return Err(Error::Invalid("page limits must be positive".to_owned()));
    }
    Ok(())
}

/// Encoded blocks of one run, with the totals of their headers.
struct RunBlocks {
    stream_id: StreamId,
    blocks: Vec<Bytes>,
    rows: u32,
    bytes: u32,
    min_timestamp_ns: i64,
    max_timestamp_ns: i64,
}

impl RunBlocks {
    fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            blocks: Vec::new(),
            rows: 0,
            bytes: 0,
            min_timestamp_ns: i64::MAX,
            max_timestamp_ns: i64::MIN,
        }
    }

    fn push(&mut self, block: Bytes) -> Result<()> {
        let header = block_header(&block)?;
        let overflow = || Error::Invalid("run exceeds u32 rows or bytes".to_owned());
        self.rows = self.rows.checked_add(header.rows).ok_or_else(overflow)?;
        self.bytes = self
            .bytes
            .checked_add(to_u32(block.len(), "block length")?)
            .ok_or_else(overflow)?;
        self.min_timestamp_ns = self.min_timestamp_ns.min(header.min_timestamp_ns);
        self.max_timestamp_ns = self.max_timestamp_ns.max(header.max_timestamp_ns);
        self.blocks.push(block);
        Ok(())
    }

    /// Appends the run's blocks and its description to `object`.
    fn finish_into(
        self,
        object: &mut BuiltObject,
        entries: Vec<LogEntry>,
        leaves: Vec<Leaf>,
    ) -> Result<()> {
        object.runs.push(BuiltRun {
            stream_id: self.stream_id,
            entries,
            rows: self.rows,
            leaves,
            blocks: to_u32(self.blocks.len(), "run block count")?,
            bytes: self.bytes,
            min_timestamp_ns: self.min_timestamp_ns,
            max_timestamp_ns: self.max_timestamp_ns,
        });
        object.blocks.extend(self.blocks);
        Ok(())
    }
}

/// One input of [`merge_objects`]: an object's directory and all its blocks.
pub(crate) struct MergeInput {
    pub object_id: u64,
    pub stored: StoredObject,
    pub blocks: Vec<Bytes>,
}

/// Builds the one object replacing `inputs` (given in ID order): each
/// stream's rows are concatenated in input order, so they stay in write
/// order. A block already holding `rows_per_block` rows is copied unchanged
/// while no partial block's rows precede it; other blocks are decoded and
/// their rows re-blocked.
pub(crate) fn merge_objects(config: &PageConfig, inputs: Vec<MergeInput>) -> Result<BuiltObject> {
    validate(config)?;
    let mut streams = BTreeMap::<StreamId, (RunBlocks, Vec<LogEntry>, Vec<Leaf>)>::new();
    for input in inputs {
        let mut blocks = input.blocks.into_iter();
        for run in &input.stored.runs {
            let (merged, pending, leaves) = streams
                .entry(run.stream_id)
                .or_insert_with(|| (RunBlocks::new(run.stream_id), Vec::new(), Vec::new()));
            let mut rows = 0u32;
            for _ in 0..run.blocks {
                let block = blocks.next().ok_or_else(|| {
                    Error::Corrupt("object has fewer blocks than its directory".to_owned())
                })?;
                let header = block_header(&block)?;
                rows = rows.saturating_add(header.rows);
                if pending.is_empty() && header.rows as usize >= config.rows_per_block {
                    merged.push(block)?;
                    continue;
                }
                pending.extend(decode_block(&block)?);
                while pending.len() >= config.rows_per_block {
                    let rest = pending.split_off(config.rows_per_block);
                    merged.push(encode_block(&std::mem::replace(pending, rest))?)?;
                }
            }
            if rows != run.rows {
                return Err(Error::Corrupt(
                    "merge input row count differs from its directory".to_owned(),
                ));
            }
            leaves.extend(StoredObject::leaves_of(run, input.object_id));
        }
        if blocks.next().is_some() {
            return Err(Error::Corrupt(
                "object has more blocks than its directory".to_owned(),
            ));
        }
    }
    let mut object = BuiltObject::default();
    for (_, (mut merged, pending, leaves)) in streams {
        if !pending.is_empty() {
            merged.push(encode_block(&pending)?)?;
        }
        merged.finish_into(&mut object, Vec::new(), leaves)?;
    }
    Ok(object)
}

#[derive(Clone, Copy)]
pub(crate) struct ObjectLocation<'a> {
    pub namespace: &'a Namespace,
    pub segment: SegmentId,
    pub object: ObjectRef,
}

#[derive(Clone, Copy)]
pub(crate) struct ObjectProperties {
    pub expires_at_unix_ms: Option<u64>,
    pub written_at_unix_ms: u64,
    pub span: u64,
    pub ttl: Ttl,
}

/// Appends the puts of an object's blocks, run records and directory.
pub(crate) fn object_records(
    ops: &mut Vec<RecordOp>,
    location: ObjectLocation<'_>,
    built: BuiltObject,
    properties: ObjectProperties,
) -> Result<StoredObject> {
    let ObjectLocation {
        namespace,
        segment,
        object,
    } = location;
    if built.blocks.is_empty() {
        return Err(Error::Invalid("cannot write an empty object".to_owned()));
    }
    for (index, block) in built.blocks.into_iter().enumerate() {
        let index = to_u32(index, "object block count")?;
        ops.push(RecordOp::put_with_ttl(
            block_key(namespace, segment, object, index),
            block,
            properties.ttl,
        ));
    }
    let stored = StoredObject {
        expires_at_unix_ms: properties.expires_at_unix_ms,
        written_at_unix_ms: properties.written_at_unix_ms,
        span: properties.span,
        runs: built
            .runs
            .into_iter()
            .map(|run| {
                Ok(ObjectRun {
                    stream_id: run.stream_id,
                    min_timestamp_ns: run.min_timestamp_ns,
                    max_timestamp_ns: run.max_timestamp_ns,
                    rows: run.rows,
                    bytes: run.bytes,
                    blocks: run.blocks,
                    leaves: run.leaves,
                })
            })
            .collect::<Result<_>>()?,
    };
    for (stream_id, run) in stored.stored_runs(object.level) {
        ops.push(RecordOp::put_with_ttl(
            run_key(namespace, segment, stream_id, object.id),
            encode_run(&run)?,
            properties.ttl,
        ));
    }
    ops.push(RecordOp::put_with_ttl(
        directory_key(namespace, segment, object),
        encode_object(&stored)?,
        properties.ttl,
    ));
    Ok(stored)
}

/// Blocks `range` of `object`, in order: one point read for a single block,
/// otherwise one range scan.
pub(crate) async fn read_blocks<S: StorageRead + ?Sized>(
    storage: &S,
    namespace: &Namespace,
    segment: SegmentId,
    object: ObjectRef,
    range: Range<u32>,
) -> Result<Vec<Bytes>> {
    let missing = || Error::Corrupt("run references missing object blocks".to_owned());
    if range.len() == 1 {
        let record = storage
            .get(block_key(namespace, segment, object, range.start))
            .await?
            .ok_or_else(missing)?;
        return Ok(vec![record.value]);
    }
    let prefix = object_blocks_prefix(namespace, segment, object);
    let prefix_len = prefix.len();
    let bounds = BytesRange::new(
        Bound::Included(Bytes::copy_from_slice(&range.start.to_be_bytes())),
        Bound::Excluded(Bytes::copy_from_slice(&range.end.to_be_bytes())),
    );
    let mut iterator = storage.scan_prefix_iter(prefix, bounds, None).await?;
    let mut blocks = Vec::with_capacity(range.len());
    while let Some(record) = iterator.next().await? {
        let expected = range.start as usize + blocks.len();
        if decode_block_index(&record.key, prefix_len)? as usize != expected {
            return Err(missing());
        }
        blocks.push(record.value);
    }
    if blocks.len() != range.len() {
        return Err(missing());
    }
    Ok(blocks)
}

fn encode_rows(entries: &[LogEntry]) -> Result<Vec<u8>> {
    let capacity = entries
        .iter()
        .try_fold(0usize, |size, entry| {
            size.checked_add(estimated_row_size(entry))
        })
        .ok_or_else(|| Error::Invalid("row block is too large".to_owned()))?;
    let mut raw = BytesMut::with_capacity(capacity);
    for entry in entries {
        raw.put_i64(entry.timestamp_ns);
        raw.put_u32(to_u32(entry.line.len(), "log line length")?);
        raw.extend_from_slice(entry.line.as_bytes());
        raw.put_u32(to_u32(
            entry.structured_metadata.len(),
            "structured metadata field count",
        )?);
        for field in entry.structured_metadata.iter() {
            raw.put_u32(to_u32(field.name.len(), "structured metadata name length")?);
            raw.extend_from_slice(field.name.as_bytes());
            raw.put_u32(to_u32(
                field.value.len(),
                "structured metadata value length",
            )?);
            raw.extend_from_slice(field.value.as_bytes());
        }
    }
    Ok(raw.to_vec())
}

fn decode_rows(mut raw: &[u8], expected_rows: u32) -> Result<Vec<LogEntry>> {
    let mut entries = Vec::with_capacity(expected_rows as usize);
    for _ in 0..expected_rows {
        if raw.len() < 12 {
            return Err(Error::Corrupt("truncated row header".to_owned()));
        }
        let timestamp_ns = i64::from_be_bytes(raw[..8].try_into().unwrap());
        let line_len = u32::from_be_bytes(raw[8..12].try_into().unwrap()) as usize;
        raw = &raw[12..];
        if raw.len() < line_len {
            return Err(Error::Corrupt("truncated log line".to_owned()));
        }
        let line = std::str::from_utf8(&raw[..line_len])
            .map_err(|error| Error::Corrupt(format!("log line is not UTF-8: {error}")))?
            .to_owned();
        raw = &raw[line_len..];
        if raw.len() < 4 {
            return Err(Error::Corrupt(
                "truncated structured metadata count".to_owned(),
            ));
        }
        let field_count = u32::from_be_bytes(raw[..4].try_into().unwrap()) as usize;
        raw = &raw[4..];
        if field_count > raw.len() / 8 {
            return Err(Error::Corrupt(
                "structured metadata count exceeds remaining row bytes".to_owned(),
            ));
        }
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let name = take_len_prefixed_utf8(&mut raw, "structured metadata name")?;
            let value = take_len_prefixed_utf8(&mut raw, "structured metadata value")?;
            fields.push(Field { name, value });
        }
        let structured_metadata = Fields::from_canonical(fields).map_err(|error| match error {
            Error::Invalid(message) => Error::Corrupt(message),
            other => other,
        })?;
        entries.push(LogEntry {
            timestamp_ns,
            line,
            structured_metadata,
        });
    }
    if !raw.is_empty() {
        return Err(Error::Corrupt("trailing bytes in row block".to_owned()));
    }
    Ok(entries)
}

fn estimated_row_size(entry: &LogEntry) -> usize {
    entry
        .structured_metadata
        .iter()
        .fold(16usize.saturating_add(entry.line.len()), |size, field| {
            size.saturating_add(8 + field.name.len() + field.value.len())
        })
}

fn take_len_prefixed_utf8(raw: &mut &[u8], field: &str) -> Result<String> {
    if raw.len() < 4 {
        return Err(Error::Corrupt(format!("truncated {field} length")));
    }
    let length = u32::from_be_bytes(raw[..4].try_into().unwrap()) as usize;
    *raw = &raw[4..];
    if raw.len() < length {
        return Err(Error::Corrupt(format!("truncated {field}")));
    }
    let value = std::str::from_utf8(&raw[..length])
        .map_err(|error| Error::Corrupt(format!("{field} is not UTF-8: {error}")))?
        .to_owned();
    *raw = &raw[length..];
    Ok(value)
}

fn to_u32(value: usize, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::Invalid(format!("{field} exceeds u32")))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn entries(range: std::ops::Range<i64>) -> Vec<LogEntry> {
        range
            .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
            .collect()
    }

    #[test]
    fn blocks_carry_their_bounds_and_decode_independently() {
        let mut rows = entries(0..5);
        rows.swap(0, 4);
        let block = encode_block(&rows).unwrap();
        let header = block_header(&block).unwrap();
        assert_eq!((header.min_timestamp_ns, header.max_timestamp_ns), (0, 4));
        assert_eq!(header.rows, 5);
        assert!(header.overlaps(4, 9) && !header.overlaps(5, 9));
        assert_eq!(decode_block(&block).unwrap(), rows);
        let mut unknown = block.to_vec();
        unknown[0] = BLOCK_FORMAT + 1;
        assert!(block_header(&unknown).is_err());
        assert!(encode_block(&[]).is_err());
    }

    #[test]
    fn structured_metadata_round_trips_in_canonical_order() {
        let fields = Fields::new(vec![
            Field::new("trace_id", "abc"),
            Field::new("severity", "info"),
        ])
        .unwrap();
        let entry = LogEntry::with_structured_metadata(42, "hello", fields);
        let block = encode_block(std::slice::from_ref(&entry)).unwrap();
        assert_eq!(decode_block(&block).unwrap(), vec![entry]);
    }

    #[test]
    fn decoder_rejects_noncanonical_structured_metadata() {
        let mut raw = BytesMut::new();
        raw.put_i64(42);
        raw.put_u32(1);
        raw.extend_from_slice(b"x");
        raw.put_u32(2);
        for (name, value) in [("z", "last"), ("a", "first")] {
            raw.put_u32(name.len() as u32);
            raw.extend_from_slice(name.as_bytes());
            raw.put_u32(value.len() as u32);
            raw.extend_from_slice(value.as_bytes());
        }

        let error = decode_rows(&raw, 1).unwrap_err();
        assert!(error.to_string().contains("canonical order"));
    }

    #[test]
    fn splitting_builder_packs_streams_and_cuts_on_each_limit() {
        let config = PageConfig {
            target_size_bytes: 1024,
            max_rows: 5,
            rows_per_block: 2,
        };
        let mut builder = ObjectBuilder::new(config).unwrap();
        builder.push_run(1, entries(0..3)).unwrap();
        builder.push_run(4, entries(10..14)).unwrap();
        builder.push_run(7, entries(20..21)).unwrap();
        assert!(builder.push_run(7, entries(30..31)).is_err());
        let objects = builder.finish();
        // Five rows fill the first object; stream 4 continues in the next.
        let layout = objects
            .iter()
            .map(|object| {
                object
                    .runs
                    .iter()
                    .map(|run| (run.stream_id, run.entries.len(), run.blocks))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            layout,
            [vec![(1, 3, 2), (4, 2, 1)], vec![(4, 2, 1), (7, 1, 1)]]
        );
        assert_eq!(objects[0].blocks.len(), 3);
        assert_eq!(
            objects[0].runs[0].bytes as usize,
            objects[0].blocks[..2].iter().map(Bytes::len).sum::<usize>()
        );

        let mut builder = ObjectBuilder::new(PageConfig {
            target_size_bytes: 40,
            ..PageConfig::default()
        })
        .unwrap();
        builder
            .push_run(0, vec![LogEntry::new(1, "x".repeat(100))])
            .unwrap();
        builder.push_run(1, vec![LogEntry::new(2, "y")]).unwrap();
        assert_eq!(builder.finish().len(), 2);
    }

    /// A written object of `runs`, as compaction reads it back.
    fn written(
        object_id: u64,
        config: &PageConfig,
        runs: Vec<(StreamId, Vec<LogEntry>)>,
    ) -> MergeInput {
        let mut builder = ObjectBuilder::new(PageConfig {
            target_size_bytes: usize::MAX,
            max_rows: usize::MAX,
            ..config.clone()
        })
        .unwrap();
        for (stream_id, entries) in runs {
            builder.push_run(stream_id, entries).unwrap();
        }
        let built = builder.finish().pop().unwrap();
        let stored = StoredObject {
            expires_at_unix_ms: None,
            written_at_unix_ms: 0,
            span: 1,
            runs: built
                .runs
                .iter()
                .map(|run| ObjectRun {
                    stream_id: run.stream_id,
                    min_timestamp_ns: run.min_timestamp_ns,
                    max_timestamp_ns: run.max_timestamp_ns,
                    rows: run.rows,
                    bytes: run.bytes,
                    blocks: run.blocks,
                    leaves: Vec::new(),
                })
                .collect(),
        };
        MergeInput {
            object_id,
            stored,
            blocks: built.blocks,
        }
    }

    fn run_rows(object: &BuiltObject) -> Vec<(StreamId, Vec<String>)> {
        let mut blocks = object.blocks.iter();
        object
            .runs
            .iter()
            .map(|run| {
                let lines = (&mut blocks)
                    .take(run.blocks as usize)
                    .flat_map(|block| decode_block(block).unwrap())
                    .map(|entry| entry.line)
                    .collect();
                (run.stream_id, lines)
            })
            .collect()
    }

    #[test]
    fn merges_concatenate_streams_in_input_order_and_copy_full_blocks() {
        let config = PageConfig {
            target_size_bytes: 1,
            max_rows: 1,
            rows_per_block: 2,
        };
        let line = |timestamp, text: &str| LogEntry::new(timestamp, text);
        let first = written(
            3,
            &config,
            vec![
                (2, vec![line(5, "a"), line(6, "b")]),
                (7, vec![line(1, "p")]),
            ],
        );
        let full_block = first.blocks[0].clone();
        let second = written(
            4,
            &config,
            vec![
                (2, vec![line(1, "c")]),
                (5, vec![line(9, "x"), line(9, "y"), line(9, "z")]),
                (7, vec![line(2, "q"), line(3, "r")]),
            ],
        );
        let merged = merge_objects(&config, vec![first, second]).unwrap();
        // Rows keep write order even where it is not timestamp order.
        assert_eq!(
            run_rows(&merged),
            [
                (2, vec!["a".into(), "b".into(), "c".into()]),
                (5, vec!["x".into(), "y".into(), "z".into()]),
                (7, vec!["p".into(), "q".into(), "r".into()]),
            ]
        );
        let stream_2 = &merged.runs[0];
        assert_eq!(
            (stream_2.min_timestamp_ns, stream_2.max_timestamp_ns),
            (1, 6)
        );
        assert_eq!(stream_2.rows, 3);
        assert_eq!(
            stream_2.leaves,
            [
                Leaf {
                    object_id: 3,
                    rows: 2
                },
                Leaf {
                    object_id: 4,
                    rows: 1
                }
            ]
        );
        // Stream 2's full first block is reused byte for byte; stream 7's
        // partial block forces its later full block to be re-blocked.
        assert!(merged.blocks[0].as_ptr() == full_block.as_ptr());
        assert_eq!(
            merged.runs.iter().map(|run| run.blocks).collect::<Vec<_>>(),
            [2, 2, 2]
        );
        assert_eq!(
            merged
                .runs
                .iter()
                .map(|run| run.bytes as usize)
                .sum::<usize>(),
            merged.blocks.iter().map(Bytes::len).sum::<usize>()
        );
    }

    #[test]
    fn merges_reject_directories_that_disagree_with_their_blocks() {
        let config = PageConfig {
            rows_per_block: 2,
            ..PageConfig::default()
        };
        let mut input = written(0, &config, vec![(1, entries(0..3))]);
        input.stored.runs[0].rows = 4;
        assert!(merge_objects(&config, vec![input]).is_err());
        let mut input = written(0, &config, vec![(1, entries(0..3))]);
        input.blocks.pop();
        assert!(merge_objects(&config, vec![input]).is_err());
    }

    proptest! {
        #[test]
        fn blocks_round_trip(lines in prop::collection::vec("[ -~]{0,80}", 1..200)) {
            let rows: Vec<_> = lines.into_iter().enumerate()
                .map(|(index, line)| LogEntry::new(index as i64, line))
                .collect();
            prop_assert_eq!(decode_block(&encode_block(&rows).unwrap()).unwrap(), rows);
        }
    }
}
