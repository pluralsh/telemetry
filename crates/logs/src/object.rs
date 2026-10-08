// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Row blocks and the multi-stream objects built from them.
//!
//! A block holds rows of one stream and is stored under its own keys, one
//! for its meta value and one for its lines, so a query reads exactly the
//! blocks of the streams it selects and only the lines it needs. An object packs
//! the blocks of many streams, ordered by stream, so a query selecting most
//! of a segment's streams reads each object with one range scan.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::{Bound, Range};
use std::sync::{Arc, OnceLock};

use bytes::{BufMut, Bytes};
use common::BytesRange;
use common::storage::{ReadHints, RecordOp, StorageRead, Ttl};

use crate::Namespace;
use crate::codec::{
    BlockGroup, Leaf, ObjectRef, ObjectRun, StoredObject, block_key, decode_block_index,
    directory_key, encode_object, encode_run, object_blocks_prefix, run_key,
};
use crate::config::PageConfig;
use crate::error::{Error, Result};
use crate::model::{Field, Fields, LogEntry, SegmentId, StreamId};

/// Not 1 or 2, retired single-value layouts, so stale blocks fail as corrupt
/// rather than misparse.
const META_FORMAT: u8 = 3;
const LINES_FORMAT: u8 = 4;
const META_HEADER_LEN: usize = 29;
const LINES_HEADER_LEN: usize = 5;
const ZSTD_LEVEL: i32 = 3;
const ERROR_METADATA: &str = "__error__";

thread_local! {
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
    static DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockHeader {
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub rows: u32,
    /// Total length of the block's lines.
    pub line_bytes: u32,
    uncompressed_len: u32,
}

impl BlockHeader {
    pub(crate) fn overlaps(&self, start_ns: i64, end_ns: i64) -> bool {
        self.max_timestamp_ns >= start_ns && self.min_timestamp_ns <= end_ns
    }
}

/// A block's two stored values. `lines` is absent when the read that
/// fetched the block needed no lines.
#[derive(Clone, Debug)]
pub(crate) struct Block {
    pub meta: Bytes,
    pub lines: Option<Bytes>,
    /// Decompressed bodies, kept once a decode needs them and shared by
    /// clones, so a cached block is decompressed at most once.
    bodies: Arc<Bodies>,
}

#[derive(Debug, Default)]
struct Bodies {
    meta: OnceLock<Box<[u8]>>,
    lines: OnceLock<Box<[u8]>>,
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.meta == other.meta && self.lines == other.lines
    }
}

impl Eq for Block {}

impl Block {
    pub(crate) fn new(meta: Bytes, lines: Option<Bytes>) -> Self {
        Self {
            meta,
            lines,
            bodies: Arc::default(),
        }
    }

    /// Stored size of the values held.
    pub(crate) fn encoded_len(&self) -> usize {
        self.meta.len() + self.lines.as_ref().map_or(0, Bytes::len)
    }

    /// Memory the block may come to hold, with both bodies decompressed.
    pub(crate) fn resident_len(&self) -> Result<usize> {
        let header = block_header(&self.meta)?;
        let lines = if self.lines.is_some() {
            header.line_bytes as usize
        } else {
            0
        };
        Ok(self.encoded_len() + header.uncompressed_len as usize + lines)
    }

    fn meta_body(&self, header: &BlockHeader) -> Result<&[u8]> {
        if let Some(body) = self.bodies.meta.get() {
            return Ok(body);
        }
        let mut body = Vec::new();
        decompress_zstd(
            &self.meta[META_HEADER_LEN..],
            header.uncompressed_len,
            &mut body,
        )?;
        Ok(self.bodies.meta.get_or_init(|| body.into_boxed_slice()))
    }

    fn lines_body(&self, header: &BlockHeader) -> Result<&[u8]> {
        if let Some(body) = self.bodies.lines.get() {
            return Ok(body);
        }
        let lines = self
            .lines
            .as_deref()
            .ok_or_else(|| Error::Invalid("block lines were not read".to_owned()))?;
        let mut body = Vec::new();
        decompress_lines(lines, header.line_bytes, &mut body)?;
        Ok(self.bodies.lines.get_or_init(|| body.into_boxed_slice()))
    }
}

/// What a block read fetches besides each block's meta value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadNeeds {
    pub lines: bool,
}

/// A row read without its line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowSample {
    pub timestamp_ns: i64,
    pub line_len: u32,
    /// Empty unless the decode asked for structured metadata.
    pub structured_metadata: Fields,
}

/// A block's meta value is `0x03 │ min ts │ max ts │ rows │ line bytes │
/// body length │ zstd body`, the body holding, with every integer an
/// unsigned LEB128 varint:
///
/// - each row's timestamp less the block's minimum;
/// - each row's line length;
/// - the count of distinct structured metadata names, then each name's
///   length and bytes;
/// - for each row, its field count, then each field's name index and value
///   length;
/// - the field values, concatenated.
///
/// Its lines value is `0x04 │ line bytes │ zstd frame of the lines,
/// concatenated`. Rows keep their order, so a block need not be in timestamp
/// order.
pub(crate) fn encode_block(entries: &[LogEntry]) -> Result<Block> {
    let (min, max) = timestamp_bounds(entries)?;
    let (meta_body, lines_body) = encode_columns(entries, min)?;
    let line_bytes = to_u32(lines_body.len(), "block line bytes")?;
    let mut meta =
        Vec::with_capacity(META_HEADER_LEN + zstd::zstd_safe::compress_bound(meta_body.len()));
    put_header(
        &mut meta,
        META_FORMAT,
        (min, max),
        entries.len(),
        line_bytes,
        meta_body.len(),
    )?;
    let mut lines =
        Vec::with_capacity(LINES_HEADER_LEN + zstd::zstd_safe::compress_bound(lines_body.len()));
    lines.put_u8(LINES_FORMAT);
    lines.put_u32(line_bytes);
    Ok(Block::new(
        compress_after(meta, &meta_body)?,
        Some(compress_after(lines, &lines_body)?),
    ))
}

/// `header` followed by the zstd frame of `body`.
fn compress_after(header: Vec<u8>, body: &[u8]) -> Result<Bytes> {
    let header_len = header.len() as u64;
    let mut output = std::io::Cursor::new(header);
    output.set_position(header_len);
    COMPRESSOR
        .with_borrow_mut(|compressor| {
            let compressor = match compressor {
                Some(compressor) => compressor,
                None => compressor.insert(zstd::bulk::Compressor::new(ZSTD_LEVEL)?),
            };
            compressor.compress_to_buffer(body, &mut output)
        })
        .map_err(|error| Error::Invalid(format!("block compression failed: {error}")))?;
    let mut bytes = output.into_inner();
    // Blocks are held until their object is written; `Bytes` keeps all capacity.
    bytes.shrink_to_fit();
    Ok(Bytes::from(bytes))
}

fn timestamp_bounds(entries: &[LogEntry]) -> Result<(i64, i64)> {
    let (Some(min), Some(max)) = (
        entries.iter().map(|entry| entry.timestamp_ns).min(),
        entries.iter().map(|entry| entry.timestamp_ns).max(),
    ) else {
        return Err(Error::Invalid("cannot encode an empty block".to_owned()));
    };
    Ok((min, max))
}

fn put_header(
    bytes: &mut Vec<u8>,
    format: u8,
    (min, max): (i64, i64),
    rows: usize,
    line_bytes: u32,
    uncompressed_len: usize,
) -> Result<()> {
    bytes.put_u8(format);
    bytes.put_i64(min);
    bytes.put_i64(max);
    bytes.put_u32(to_u32(rows, "block row count")?);
    bytes.put_u32(line_bytes);
    bytes.put_u32(to_u32(uncompressed_len, "uncompressed block length")?);
    Ok(())
}

/// Header of a block's meta value.
pub(crate) fn block_header(meta: &[u8]) -> Result<BlockHeader> {
    if meta.len() < META_HEADER_LEN {
        return Err(Error::Corrupt("truncated block header".to_owned()));
    }
    if meta[0] != META_FORMAT {
        return Err(Error::Corrupt(format!(
            "unsupported block format {}",
            meta[0]
        )));
    }
    let i64_at = |offset: usize| i64::from_be_bytes(meta[offset..offset + 8].try_into().unwrap());
    let u32_at = |offset: usize| u32::from_be_bytes(meta[offset..offset + 4].try_into().unwrap());
    Ok(BlockHeader {
        min_timestamp_ns: i64_at(1),
        max_timestamp_ns: i64_at(9),
        rows: u32_at(17),
        line_bytes: u32_at(21),
        uncompressed_len: u32_at(25),
    })
}

/// Rows of a block in stored order.
pub(crate) fn decode_block(block: &Block) -> Result<Vec<LogEntry>> {
    let mut entries = Vec::new();
    decode_block_where(
        block,
        (i64::MIN, i64::MAX),
        |_| true,
        |_, entry| entries.push(entry),
    )?;
    Ok(entries)
}

/// Passes `emit` the rows of a block in `[start_ns, end_ns]` whose index in
/// the block passes `keep`, in stored order, with their index. Other rows'
/// lines and structured metadata are not materialized, and the lines value
/// is not decompressed when no row is kept.
pub(crate) fn decode_block_where(
    block: &Block,
    range: (i64, i64),
    keep: impl FnMut(u32) -> bool,
    mut emit: impl FnMut(u32, LogEntry),
) -> Result<()> {
    if block.lines.is_none() {
        return Err(Error::Invalid("block lines were not read".to_owned()));
    }
    with_meta(block, range, |header, columns| {
        let kept = columns.kept(range, keep);
        if kept.is_empty() {
            return Ok(());
        }
        columns.emit_entries(block.lines_body(header)?, &kept, &mut emit)
    })
}

/// [`decode_block_where`], lending `emit` each kept row's timestamp, line
/// and structured metadata (in canonical order) rather than building them.
pub(crate) fn decode_block_rows(
    block: &Block,
    range: (i64, i64),
    keep: impl FnMut(u32) -> bool,
    mut emit: impl FnMut(u32, i64, &str, &[(&str, &str)]),
) -> Result<()> {
    if block.lines.is_none() {
        return Err(Error::Invalid("block lines were not read".to_owned()));
    }
    with_meta(block, range, |header, columns| {
        let kept = columns.kept(range, keep);
        if kept.is_empty() {
            return Ok(());
        }
        columns.lend_rows(block.lines_body(header)?, &kept, &mut emit)
    })
}

/// Passes `emit` the rows of a block in `[start_ns, end_ns]` whose index in
/// the block passes `keep`, without their lines, reading only the block's
/// meta value. Structured metadata is materialized only with `metadata`.
pub(crate) fn decode_samples_where(
    block: &Block,
    range: (i64, i64),
    keep: impl FnMut(u32) -> bool,
    metadata: bool,
    mut emit: impl FnMut(u32, RowSample),
) -> Result<()> {
    with_meta(block, range, |_, columns| {
        let kept = columns.kept(range, keep);
        columns.emit_samples(&kept, metadata, &mut emit)
    })
}

/// Runs `f` with the parsed columns of a block's meta value.
fn with_meta<T>(
    block: &Block,
    range: (i64, i64),
    f: impl FnOnce(&BlockHeader, &MetaColumns<'_>) -> Result<T>,
) -> Result<T> {
    let header = block_header(&block.meta)?;
    let columns = MetaColumns::parse(block.meta_body(&header)?, &header, range)?;
    f(&header, &columns)
}

/// Decompresses a block's lines value, which must hold `line_bytes` bytes.
fn decompress_lines(lines: &[u8], line_bytes: u32, buffer: &mut Vec<u8>) -> Result<()> {
    if lines.len() < LINES_HEADER_LEN {
        return Err(Error::Corrupt("truncated block lines header".to_owned()));
    }
    if lines[0] != LINES_FORMAT {
        return Err(Error::Corrupt(format!(
            "unsupported block lines format {}",
            lines[0]
        )));
    }
    if u32::from_be_bytes(lines[1..LINES_HEADER_LEN].try_into().unwrap()) != line_bytes {
        return Err(Error::Corrupt(
            "block lines length differs from its header".to_owned(),
        ));
    }
    decompress_zstd(&lines[LINES_HEADER_LEN..], line_bytes, buffer)
}

fn decompress_zstd(compressed: &[u8], expected_len: u32, buffer: &mut Vec<u8>) -> Result<()> {
    let mismatch = || Error::Corrupt("decompressed block length mismatch".to_owned());
    // Checked before allocating, so a corrupt length cannot size the buffer.
    match zstd::zstd_safe::get_frame_content_size(compressed) {
        Ok(Some(len)) if len == u64::from(expected_len) => {}
        Ok(_) => return Err(mismatch()),
        Err(_) => return Err(Error::Corrupt("corrupt block compression frame".to_owned())),
    }
    buffer.clear();
    buffer.reserve(expected_len as usize);
    let len = DECOMPRESSOR
        .with_borrow_mut(|decompressor| {
            let decompressor = match decompressor {
                Some(decompressor) => decompressor,
                None => decompressor.insert(zstd::bulk::Decompressor::new()?),
            };
            decompressor.decompress_to_buffer(compressed, buffer)
        })
        .map_err(|error| Error::Corrupt(format!("block decompression failed: {error}")))?;
    if len != expected_len as usize {
        return Err(mismatch());
    }
    Ok(())
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
    pub line_bytes: u64,
    /// See [`crate::codec::ObjectRun::duplicate_free`].
    pub duplicate_free: bool,
    /// See [`crate::codec::ObjectRun::error_metadata`].
    pub error_metadata: bool,
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
}

#[derive(Debug, Default)]
pub(crate) struct BuiltObject {
    /// Encoded blocks, run after run.
    pub blocks: Vec<Block>,
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
        let duplicate_free = duplicate_free(&entries);
        let error_metadata = entries.iter().any(|entry| {
            entry
                .structured_metadata
                .iter()
                .any(|field| field.name == ERROR_METADATA)
        });
        run.finish_into(
            &mut self.current,
            entries,
            Vec::new(),
            duplicate_free,
            error_metadata,
        )
    }

    fn cut(&mut self) {
        if !self.current.runs.is_empty() {
            self.objects.push(std::mem::take(&mut self.current));
        }
        self.rows = 0;
        self.estimated_bytes = 0;
    }
}

/// Whether no two of `entries` share a timestamp, line and structured
/// metadata.
fn duplicate_free(entries: &[LogEntry]) -> bool {
    let distinct = |group: &[&LogEntry]| {
        if group.len() < 2 {
            return true;
        }
        let mut group = group.to_vec();
        group.sort_by(|a, b| {
            a.line.cmp(&b.line).then_with(|| {
                a.structured_metadata
                    .iter()
                    .cmp(b.structured_metadata.iter())
            })
        });
        group.windows(2).all(|pair| pair[0] != pair[1])
    };
    let mut sorted = entries.iter().collect::<Vec<_>>();
    if !entries.is_sorted_by_key(|entry| entry.timestamp_ns) {
        sorted.sort_by_key(|entry| entry.timestamp_ns);
    }
    sorted
        .chunk_by(|a, b| a.timestamp_ns == b.timestamp_ns)
        .all(distinct)
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
    blocks: Vec<Block>,
    rows: u32,
    bytes: u32,
    line_bytes: u64,
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
            line_bytes: 0,
            min_timestamp_ns: i64::MAX,
            max_timestamp_ns: i64::MIN,
        }
    }

    fn push(&mut self, block: Block) -> Result<()> {
        if block.lines.is_none() {
            return Err(Error::Invalid(
                "object blocks must hold their lines".to_owned(),
            ));
        }
        let header = block_header(&block.meta)?;
        let overflow = || Error::Invalid("run exceeds u32 rows or bytes".to_owned());
        self.rows = self.rows.checked_add(header.rows).ok_or_else(overflow)?;
        self.bytes = self
            .bytes
            .checked_add(to_u32(block.encoded_len(), "block length")?)
            .ok_or_else(overflow)?;
        self.line_bytes = self.line_bytes.saturating_add(header.line_bytes.into());
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
        duplicate_free: bool,
        error_metadata: bool,
    ) -> Result<()> {
        object.runs.push(BuiltRun {
            stream_id: self.stream_id,
            entries,
            rows: self.rows,
            leaves,
            blocks: to_u32(self.blocks.len(), "run block count")?,
            bytes: self.bytes,
            line_bytes: self.line_bytes,
            duplicate_free,
            error_metadata,
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
    pub blocks: Vec<Block>,
}

/// Builds the one object replacing `inputs` (given in ID order): each
/// stream's rows are concatenated in input order, so they stay in write
/// order. A block already holding `rows_per_block` rows is copied unchanged
/// while no partial block's rows precede it; other blocks are decoded and
/// their rows re-blocked. A merged run is duplicate-free when its inputs are
/// and their time ranges are disjoint, so no two inputs share a timestamp.
pub(crate) fn merge_objects(config: &PageConfig, inputs: Vec<MergeInput>) -> Result<BuiltObject> {
    validate(config)?;
    let mut streams = BTreeMap::<StreamId, MergedStream>::new();
    for input in inputs {
        let mut blocks = input.blocks.into_iter();
        for run in &input.stored.runs {
            let MergedStream {
                blocks: merged,
                pending,
                leaves,
                inputs,
            } = streams
                .entry(run.stream_id)
                .or_insert_with(|| MergedStream::new(run.stream_id));
            inputs.push(InputRun {
                min_timestamp_ns: run.min_timestamp_ns,
                max_timestamp_ns: run.max_timestamp_ns,
                duplicate_free: run.duplicate_free,
                error_metadata: run.error_metadata,
            });
            let mut rows = 0u32;
            for _ in 0..run.blocks {
                let block = blocks.next().ok_or_else(|| {
                    Error::Corrupt("object has fewer blocks than its directory".to_owned())
                })?;
                let header = block_header(&block.meta)?;
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
    for (_, stream) in streams {
        let MergedStream {
            blocks: mut merged,
            pending,
            leaves,
            mut inputs,
        } = stream;
        if !pending.is_empty() {
            merged.push(encode_block(&pending)?)?;
        }
        inputs.sort_unstable_by_key(|run| run.min_timestamp_ns);
        let duplicate_free = inputs.iter().all(|run| run.duplicate_free)
            && inputs
                .windows(2)
                .all(|pair| pair[0].max_timestamp_ns < pair[1].min_timestamp_ns);
        let error_metadata = inputs.iter().any(|run| run.error_metadata);
        merged.finish_into(
            &mut object,
            Vec::new(),
            leaves,
            duplicate_free,
            error_metadata,
        )?;
    }
    Ok(object)
}

/// One stream's state while [`merge_objects`] concatenates its runs.
struct MergedStream {
    blocks: RunBlocks,
    /// Rows of partial blocks not yet re-blocked.
    pending: Vec<LogEntry>,
    leaves: Vec<Leaf>,
    inputs: Vec<InputRun>,
}

/// What [`merge_objects`] keeps of each input run to flag the merged run.
#[derive(Clone, Copy)]
struct InputRun {
    min_timestamp_ns: i64,
    max_timestamp_ns: i64,
    duplicate_free: bool,
    error_metadata: bool,
}

impl MergedStream {
    fn new(stream_id: StreamId) -> Self {
        Self {
            blocks: RunBlocks::new(stream_id),
            pending: Vec::new(),
            leaves: Vec::new(),
            inputs: Vec::new(),
        }
    }
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
        let lines = block
            .lines
            .ok_or_else(|| Error::Invalid("object blocks must hold their lines".to_owned()))?;
        ops.push(RecordOp::put_with_ttl(
            block_key(namespace, segment, object, BlockGroup::Meta, index),
            block.meta,
            properties.ttl,
        ));
        ops.push(RecordOp::put_with_ttl(
            block_key(namespace, segment, object, BlockGroup::Lines, index),
            lines,
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
                    line_bytes: run.line_bytes,
                    duplicate_free: run.duplicate_free,
                    error_metadata: run.error_metadata,
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

/// Blocks `range` of `object`, in order, with their lines only when `needs`
/// them. Each group is one point read for a single block, otherwise one range
/// scan; the two groups are read concurrently.
pub(crate) async fn read_blocks<S: StorageRead + ?Sized>(
    storage: &S,
    namespace: &Namespace,
    segment: SegmentId,
    object: ObjectRef,
    range: Range<u32>,
    needs: ReadNeeds,
    hints: ReadHints,
) -> Result<Vec<Block>> {
    let read = |group| {
        read_group(
            storage,
            namespace,
            segment,
            object,
            group,
            range.clone(),
            hints,
        )
    };
    if !needs.lines {
        let meta = read(BlockGroup::Meta).await?;
        return Ok(meta
            .into_iter()
            .map(|meta| Block::new(meta, None))
            .collect());
    }
    let (meta, lines) = futures::try_join!(read(BlockGroup::Meta), read(BlockGroup::Lines))?;
    Ok(meta
        .into_iter()
        .zip(lines)
        .map(|(meta, lines)| Block::new(meta, Some(lines)))
        .collect())
}

async fn read_group<S: StorageRead + ?Sized>(
    storage: &S,
    namespace: &Namespace,
    segment: SegmentId,
    object: ObjectRef,
    group: BlockGroup,
    range: Range<u32>,
    hints: ReadHints,
) -> Result<Vec<Bytes>> {
    let missing = || Error::Corrupt("run references missing object blocks".to_owned());
    if range.len() == 1 {
        let record = storage
            .get_with(
                block_key(namespace, segment, object, group, range.start),
                hints,
            )
            .await?
            .ok_or_else(missing)?;
        return Ok(vec![record.value]);
    }
    let prefix = object_blocks_prefix(namespace, segment, object, group);
    let prefix_len = prefix.len();
    let bounds = BytesRange::new(
        Bound::Included(Bytes::copy_from_slice(&range.start.to_be_bytes())),
        Bound::Excluded(Bytes::copy_from_slice(&range.end.to_be_bytes())),
    );
    let mut iterator = storage
        .scan_prefix_iter_with(prefix, bounds, None, hints)
        .await?;
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

/// The bodies of a block's meta and lines values.
fn encode_columns(entries: &[LogEntry], min_timestamp_ns: i64) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut names = Vec::<&str>::new();
    let mut name_ids = foldhash::HashMap::<&str, u64>::default();
    let mut descriptors = Vec::new();
    let (mut values_len, mut lines_len) = (0usize, 0usize);
    for entry in entries {
        put_varint(
            &mut descriptors,
            to_u32(
                entry.structured_metadata.len(),
                "structured metadata field count",
            )?
            .into(),
        );
        for field in entry.structured_metadata.iter() {
            let id = *name_ids.entry(&field.name).or_insert_with(|| {
                names.push(&field.name);
                names.len() as u64 - 1
            });
            put_varint(&mut descriptors, id);
            put_varint(
                &mut descriptors,
                to_u32(field.value.len(), "structured metadata value length")?.into(),
            );
            values_len = values_len.saturating_add(field.value.len());
        }
        lines_len = lines_len.saturating_add(entry.line.len());
    }
    let names_len = names.iter().map(|name| name.len() + 5).sum::<usize>();
    let mut body = Vec::with_capacity(
        (entries.len() * 15)
            .saturating_add(names_len)
            .saturating_add(descriptors.len())
            .saturating_add(values_len),
    );
    for entry in entries {
        put_varint(
            &mut body,
            entry.timestamp_ns.wrapping_sub(min_timestamp_ns) as u64,
        );
    }
    for entry in entries {
        put_varint(
            &mut body,
            to_u32(entry.line.len(), "log line length")?.into(),
        );
    }
    put_varint(&mut body, names.len() as u64);
    for name in &names {
        put_varint(
            &mut body,
            to_u32(name.len(), "structured metadata name length")?.into(),
        );
        body.extend_from_slice(name.as_bytes());
    }
    body.extend_from_slice(&descriptors);
    for entry in entries {
        for field in entry.structured_metadata.iter() {
            body.extend_from_slice(field.value.as_bytes());
        }
    }
    let mut lines = Vec::with_capacity(lines_len);
    for entry in entries {
        lines.extend_from_slice(entry.line.as_bytes());
    }
    Ok((body, lines))
}

/// A row's timestamp and where its columns lie in a decoded meta body.
struct RowColumns {
    timestamp_ns: i64,
    line_end: usize,
    fields_at: usize,
    values_end: usize,
}

/// A decoded meta body, validated, with the rows a decode's time range
/// selects. Timestamps and line lengths are parsed eagerly; structured
/// metadata is materialized per emitted row.
struct MetaColumns<'a> {
    body: &'a [u8],
    rows: Vec<RowColumns>,
    names: Vec<&'a str>,
    values: &'a [u8],
    selected: Range<usize>,
}

impl<'a> MetaColumns<'a> {
    fn parse(body: &'a [u8], header: &BlockHeader, (start_ns, end_ns): (i64, i64)) -> Result<Self> {
        let row_count = header.rows as usize;
        // Each row takes at least a byte each for its timestamp, line length and
        // field count.
        if row_count > body.len() / 3 {
            return Err(Error::Corrupt(
                "block row count exceeds its body".to_owned(),
            ));
        }
        if header.max_timestamp_ns < header.min_timestamp_ns {
            return Err(Error::Corrupt(
                "block timestamp bounds are inverted".to_owned(),
            ));
        }
        let span = header
            .max_timestamp_ns
            .wrapping_sub(header.min_timestamp_ns) as u64;
        let mut reader = ColumnReader::new(body);
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let delta = reader.varint("row timestamp")?;
            if delta > span {
                return Err(Error::Corrupt(
                    "row timestamp outside block bounds".to_owned(),
                ));
            }
            rows.push(RowColumns {
                timestamp_ns: header.min_timestamp_ns.wrapping_add(delta as i64),
                line_end: 0,
                fields_at: 0,
                values_end: 0,
            });
        }
        let selected = if rows.is_sorted_by_key(|row| row.timestamp_ns) {
            rows.partition_point(|row| row.timestamp_ns < start_ns)
                ..rows.partition_point(|row| row.timestamp_ns <= end_ns)
        } else {
            0..row_count
        };
        let mut lines_len = 0usize;
        for row in &mut rows {
            lines_len = lines_len
                .checked_add(reader.length("log line length")?)
                .ok_or_else(|| Error::Corrupt("block lines exceed usize".to_owned()))?;
            row.line_end = lines_len;
        }
        let name_count = reader.length("structured metadata name count")?;
        if name_count > reader.remaining() {
            return Err(Error::Corrupt(
                "structured metadata name count exceeds block body".to_owned(),
            ));
        }
        let mut names = Vec::with_capacity(name_count);
        for _ in 0..name_count {
            let length = reader.length("structured metadata name length")?;
            let name = reader.take(length, "structured metadata name")?;
            names.push(utf8(name).map_err(|error| {
                Error::Corrupt(format!("structured metadata name is not UTF-8: {error}"))
            })?);
        }
        let mut values_len = 0usize;
        for row in &mut rows {
            row.fields_at = reader.position();
            let field_count = reader.length("structured metadata count")?;
            if field_count > reader.remaining() / 2 {
                return Err(Error::Corrupt(
                    "structured metadata count exceeds remaining row bytes".to_owned(),
                ));
            }
            for _ in 0..field_count {
                if reader.length("structured metadata name index")? >= names.len() {
                    return Err(Error::Corrupt(
                        "structured metadata name index out of range".to_owned(),
                    ));
                }
                values_len = values_len
                    .checked_add(reader.length("structured metadata value length")?)
                    .ok_or_else(|| Error::Corrupt("block values exceed usize".to_owned()))?;
            }
            row.values_end = values_len;
        }
        let values = reader.take(values_len, "structured metadata value")?;
        if reader.remaining() != 0 {
            return Err(Error::Corrupt("trailing bytes in row block".to_owned()));
        }
        if lines_len != header.line_bytes as usize {
            return Err(Error::Corrupt(
                "block line lengths differ from its header".to_owned(),
            ));
        }
        Ok(Self {
            body,
            rows,
            names,
            values,
            selected,
        })
    }

    /// Indexes of selected rows in `[start_ns, end_ns]` that pass `keep`.
    fn kept(&self, (start_ns, end_ns): (i64, i64), mut keep: impl FnMut(u32) -> bool) -> Vec<u32> {
        self.selected
            .clone()
            .filter(|&index| {
                let timestamp_ns = self.rows[index].timestamp_ns;
                timestamp_ns >= start_ns && timestamp_ns <= end_ns && keep(index as u32)
            })
            .map(|index| index as u32)
            .collect()
    }

    fn line_start(&self, index: usize) -> usize {
        index
            .checked_sub(1)
            .map_or(0, |prior| self.rows[prior].line_end)
    }

    fn values_start(&self, index: usize) -> usize {
        index
            .checked_sub(1)
            .map_or(0, |prior| self.rows[prior].values_end)
    }

    /// Values of rows `first..=last`, checked as UTF-8 once.
    fn values_between(&self, first: usize, last: usize) -> Result<(usize, &'a str)> {
        let base = self.values_start(first);
        let values = utf8(&self.values[base..self.rows[last].values_end]).map_err(|error| {
            Error::Corrupt(format!("structured metadata value is not UTF-8: {error}"))
        })?;
        Ok((base, values))
    }

    /// Structured metadata of row `index`, whose values lie in `values` from
    /// `values_base`.
    fn fields(&self, index: usize, values: &str, values_base: usize) -> Result<Fields> {
        let mut reader = ColumnReader::at(self.body, self.rows[index].fields_at);
        let field_count = reader.length("structured metadata count")?;
        let mut fields = Vec::with_capacity(field_count);
        let mut value_end = self.values_start(index) - values_base;
        for _ in 0..field_count {
            let name = self.names[reader.length("structured metadata name index")?];
            let value_start = value_end;
            value_end += reader.length("structured metadata value length")?;
            // A value whose bounds split a character is caught by `get`.
            let value = values.get(value_start..value_end).ok_or_else(|| {
                Error::Corrupt(
                    "structured metadata value is not UTF-8: splits a character".to_owned(),
                )
            })?;
            fields.push(Field {
                name: name.to_owned(),
                value: value.to_owned(),
            });
        }
        canonical_fields(fields)
    }

    /// Emits rows `kept` with their lines from the decoded `lines` body.
    fn emit_entries(
        &self,
        lines: &[u8],
        kept: &[u32],
        emit: &mut impl FnMut(u32, LogEntry),
    ) -> Result<()> {
        let (Some(&first), Some(&last)) = (kept.first(), kept.last()) else {
            return Ok(());
        };
        let (first, last) = (first as usize, last as usize);
        let lines_base = self.line_start(first);
        let lines = utf8(&lines[lines_base..self.rows[last].line_end])
            .map_err(|error| Error::Corrupt(format!("log line is not UTF-8: {error}")))?;
        let (values_base, values) = self.values_between(first, last)?;
        for &index in kept {
            let index = index as usize;
            let row = &self.rows[index];
            let line = lines
                .get(self.line_start(index) - lines_base..row.line_end - lines_base)
                .ok_or_else(|| {
                    Error::Corrupt("log line is not UTF-8: splits a character".to_owned())
                })?;
            emit(
                index as u32,
                LogEntry {
                    timestamp_ns: row.timestamp_ns,
                    line: line.to_owned(),
                    structured_metadata: self.fields(index, values, values_base)?,
                },
            );
        }
        Ok(())
    }

    /// [`Self::emit_entries`], lending each row instead of building it.
    fn lend_rows(
        &self,
        lines: &[u8],
        kept: &[u32],
        emit: &mut impl FnMut(u32, i64, &str, &[(&str, &str)]),
    ) -> Result<()> {
        let (Some(&first), Some(&last)) = (kept.first(), kept.last()) else {
            return Ok(());
        };
        let (first, last) = (first as usize, last as usize);
        let lines_base = self.line_start(first);
        let lines = utf8(&lines[lines_base..self.rows[last].line_end])
            .map_err(|error| Error::Corrupt(format!("log line is not UTF-8: {error}")))?;
        let (values_base, values) = self.values_between(first, last)?;
        let mut fields = Vec::new();
        for &index in kept {
            let index = index as usize;
            let row = &self.rows[index];
            let line = lines
                .get(self.line_start(index) - lines_base..row.line_end - lines_base)
                .ok_or_else(|| {
                    Error::Corrupt("log line is not UTF-8: splits a character".to_owned())
                })?;
            self.lent_fields(index, values, values_base, &mut fields)?;
            emit(index as u32, row.timestamp_ns, line, &fields);
        }
        Ok(())
    }

    /// [`Self::fields`] as borrowed pairs, checked to be canonical.
    fn lent_fields(
        &self,
        index: usize,
        values: &'a str,
        values_base: usize,
        fields: &mut Vec<(&'a str, &'a str)>,
    ) -> Result<()> {
        fields.clear();
        let mut reader = ColumnReader::at(self.body, self.rows[index].fields_at);
        let field_count = reader.length("structured metadata count")?;
        let mut value_end = self.values_start(index) - values_base;
        for _ in 0..field_count {
            let name = self.names[reader.length("structured metadata name index")?];
            let value_start = value_end;
            value_end += reader.length("structured metadata value length")?;
            let value = values.get(value_start..value_end).ok_or_else(|| {
                Error::Corrupt(
                    "structured metadata value is not UTF-8: splits a character".to_owned(),
                )
            })?;
            if name.is_empty() {
                return Err(Error::Corrupt(
                    "structured metadata names cannot be empty".to_owned(),
                ));
            }
            match fields.last() {
                Some(&(previous, _)) if previous == name => {
                    return Err(Error::Corrupt(
                        "structured metadata names must be unique".to_owned(),
                    ));
                }
                Some(&(previous, _)) if previous > name => {
                    return Err(Error::Corrupt(
                        "structured metadata is not in canonical order".to_owned(),
                    ));
                }
                _ => fields.push((name, value)),
            }
        }
        Ok(())
    }

    /// Emits rows `kept` without their lines.
    fn emit_samples(
        &self,
        kept: &[u32],
        metadata: bool,
        emit: &mut impl FnMut(u32, RowSample),
    ) -> Result<()> {
        let (Some(&first), Some(&last)) = (kept.first(), kept.last()) else {
            return Ok(());
        };
        let values = if metadata {
            Some(self.values_between(first as usize, last as usize)?)
        } else {
            None
        };
        for &index in kept {
            let index = index as usize;
            let row = &self.rows[index];
            let structured_metadata = match values {
                Some((values_base, values)) => self.fields(index, values, values_base)?,
                None => Fields::default(),
            };
            emit(
                index as u32,
                RowSample {
                    timestamp_ns: row.timestamp_ns,
                    line_len: (row.line_end - self.line_start(index)) as u32,
                    structured_metadata,
                },
            );
        }
        Ok(())
    }
}

fn canonical_fields(fields: Vec<Field>) -> Result<Fields> {
    Fields::from_canonical(fields).map_err(|error| match error {
        Error::Invalid(message) => Error::Corrupt(message),
        other => other,
    })
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push(value as u8 | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

struct ColumnReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ColumnReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self::at(bytes, 0)
    }

    fn at(bytes: &'a [u8], position: usize) -> Self {
        Self { bytes, position }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn varint(&mut self, field: &str) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let Some(&byte) = self.bytes.get(self.position) else {
                return Err(Error::Corrupt(format!("truncated {field}")));
            };
            self.position += 1;
            if shift == 63 && byte > 1 {
                break;
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Error::Corrupt(format!("{field} overflows u64")))
    }

    fn length(&mut self, field: &str) -> Result<usize> {
        usize::try_from(self.varint(field)?)
            .map_err(|_| Error::Corrupt(format!("{field} overflows usize")))
    }

    fn take(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        if length > self.remaining() {
            return Err(Error::Corrupt(format!("truncated {field}")));
        }
        let bytes = &self.bytes[self.position..self.position + length];
        self.position += length;
        Ok(bytes)
    }
}

fn estimated_row_size(entry: &LogEntry) -> usize {
    entry
        .structured_metadata
        .iter()
        .fold(16usize.saturating_add(entry.line.len()), |size, field| {
            size.saturating_add(8 + field.name.len() + field.value.len())
        })
}

/// `simdutf8` for speed, `std` for its error's position.
fn utf8(bytes: &[u8]) -> std::result::Result<&str, std::str::Utf8Error> {
    simdutf8::basic::from_utf8(bytes).or_else(|_| std::str::from_utf8(bytes))
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
        let header = block_header(&block.meta).unwrap();
        assert_eq!((header.min_timestamp_ns, header.max_timestamp_ns), (0, 4));
        assert_eq!(header.rows, 5);
        assert!(header.overlaps(4, 9) && !header.overlaps(5, 9));
        assert_eq!(decode_block(&block).unwrap(), rows);
        assert!(encode_block(&[]).is_err());
    }

    #[test]
    fn blocks_of_other_formats_are_corrupt() {
        let block = encode_block(&entries(0..5)).unwrap();
        assert_eq!(block.meta[0], META_FORMAT);
        assert_eq!(block.lines.as_ref().unwrap()[0], LINES_FORMAT);
        for format in [0, 1, 2, LINES_FORMAT, LINES_FORMAT + 1] {
            let mut other = block.meta.to_vec();
            other[0] = format;
            assert!(
                matches!(block_header(&other), Err(Error::Corrupt(_))),
                "{format}"
            );
            assert!(
                matches!(
                    decode_block(&with_meta_value(&block, &other)),
                    Err(Error::Corrupt(_))
                ),
                "{format}"
            );
        }
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
        // One row with line "x" whose fields are named "z" then "a".
        let mut meta = vec![0, 1, 2, 1, b'z', 1, b'a', 2, 0, 4, 1, 5];
        meta.extend_from_slice(b"lastfirst");
        let block = columns_block((42, 42), 1, &meta, b"x");

        let error = decode_block(&block).unwrap_err();
        assert!(matches!(error, Error::Corrupt(_)), "{error}");
        assert!(error.to_string().contains("canonical order"), "{error}");
    }

    /// A block of `rows` rows bounded by `bounds` around a meta body and
    /// lines.
    fn columns_block(bounds: (i64, i64), rows: usize, meta_body: &[u8], lines: &[u8]) -> Block {
        let line_bytes = lines.len() as u32;
        let mut meta = Vec::new();
        put_header(
            &mut meta,
            META_FORMAT,
            bounds,
            rows,
            line_bytes,
            meta_body.len(),
        )
        .unwrap();
        meta.extend(zstd::bulk::compress(meta_body, ZSTD_LEVEL).unwrap());
        let mut lines_value = vec![LINES_FORMAT];
        lines_value.extend(line_bytes.to_be_bytes());
        lines_value.extend(zstd::bulk::compress(lines, ZSTD_LEVEL).unwrap());
        Block::new(meta.into(), Some(lines_value.into()))
    }

    fn with_meta_value(block: &Block, meta: &[u8]) -> Block {
        Block::new(Bytes::copy_from_slice(meta), block.lines.clone())
    }

    fn with_lines_value(block: &Block, lines: &[u8]) -> Block {
        Block::new(block.meta.clone(), Some(Bytes::copy_from_slice(lines)))
    }

    fn samples_of(block: &Block, range: (i64, i64), metadata: bool) -> Vec<(u32, RowSample)> {
        let mut samples = Vec::new();
        decode_samples_where(
            block,
            range,
            |_| true,
            metadata,
            |index, sample| samples.push((index, sample)),
        )
        .unwrap();
        samples
    }

    fn decode_where(
        block: &Block,
        range: (i64, i64),
        keep: impl Fn(u32) -> bool,
    ) -> Result<Vec<(u32, LogEntry)>> {
        let mut rows = Vec::new();
        decode_block_where(block, range, keep, |index, entry| rows.push((index, entry)))?;
        Ok(rows)
    }

    fn with_metadata(timestamp: i64, line: &str, fields: &[(&str, &str)]) -> LogEntry {
        let fields = fields
            .iter()
            .map(|(name, value)| Field::new(*name, *value))
            .collect();
        LogEntry::with_structured_metadata(timestamp, line, Fields::new(fields).unwrap())
    }

    fn mixed_rows() -> Vec<LogEntry> {
        vec![
            with_metadata(10, "first", &[("trace_id", "a1"), ("pod", "api-1")]),
            with_metadata(11, "", &[]),
            with_metadata(13, "dritte Zeile — ünïcödé", &[("pod", "api-2")]),
            with_metadata(13, "fourth", &[("span_id", "ß"), ("trace_id", "b2")]),
            with_metadata(20, "fifth", &[("pod", "")]),
        ]
    }

    #[test]
    fn mixed_rows_and_extreme_timestamps_round_trip() {
        let rows = mixed_rows();
        let block = encode_block(&rows).unwrap();
        let header = block_header(&block.meta).unwrap();
        assert_eq!((header.min_timestamp_ns, header.max_timestamp_ns), (10, 20));
        assert_eq!(
            header.line_bytes as usize,
            rows.iter().map(|row| row.line.len()).sum::<usize>()
        );
        assert_eq!(decode_block(&block).unwrap(), rows);
        let extremes = vec![
            LogEntry::new(i64::MAX, "late"),
            LogEntry::new(i64::MIN, "early"),
        ];
        assert_eq!(
            decode_block(&encode_block(&extremes).unwrap()).unwrap(),
            extremes
        );
    }

    #[test]
    fn partial_decodes_materialize_only_rows_in_range_and_kept() {
        let sorted = mixed_rows();
        let mut unsorted = sorted.clone();
        unsorted.swap(0, 4);
        for rows in [sorted, unsorted] {
            let block = encode_block(&rows).unwrap();
            for range in [
                (11, 13),
                (12, 12),
                (13, 13),
                (0, 10),
                (20, 99),
                (i64::MIN, i64::MAX),
            ] {
                let keep = |index: u32| index != 3;
                let expected = rows
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, entry)| (index as u32, entry))
                    .filter(|(index, entry)| {
                        (range.0..=range.1).contains(&entry.timestamp_ns) && keep(*index)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    decode_where(&block, range, keep).unwrap(),
                    expected,
                    "{range:?}"
                );
            }
        }
    }

    #[test]
    fn samples_read_only_the_meta_value() {
        let rows = mixed_rows();
        let block = encode_block(&rows).unwrap();
        let meta_only = Block::new(block.meta.clone(), None);
        let expected = |metadata: bool| {
            rows.iter()
                .enumerate()
                .filter(|(_, row)| (11..=13).contains(&row.timestamp_ns))
                .map(|(index, row)| {
                    (
                        index as u32,
                        RowSample {
                            timestamp_ns: row.timestamp_ns,
                            line_len: row.line.len() as u32,
                            structured_metadata: if metadata {
                                row.structured_metadata.clone()
                            } else {
                                Fields::default()
                            },
                        },
                    )
                })
                .collect::<Vec<_>>()
        };
        for metadata in [false, true] {
            assert_eq!(
                samples_of(&meta_only, (11, 13), metadata),
                expected(metadata)
            );
        }
        assert!(matches!(decode_block(&meta_only), Err(Error::Invalid(_))));
    }

    #[test]
    fn corrupt_columnar_blocks_are_rejected_without_panicking() {
        let block = encode_block(&mixed_rows()).unwrap();
        let lines = block.lines.clone().unwrap();
        for len in 0..block.meta.len() {
            let error = decode_block(&with_meta_value(&block, &block.meta[..len])).unwrap_err();
            assert!(matches!(error, Error::Corrupt(_)), "meta {len}: {error}");
        }
        for len in 0..lines.len() {
            let error = decode_block(&with_lines_value(&block, &lines[..len])).unwrap_err();
            assert!(matches!(error, Error::Corrupt(_)), "lines {len}: {error}");
        }
        for offset in [17, 21, 25] {
            let mut meta = block.meta.to_vec();
            meta[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
            assert!(
                matches!(
                    decode_block(&with_meta_value(&block, &meta)),
                    Err(Error::Corrupt(_))
                ),
                "{offset}"
            );
        }
        let mut format = lines.to_vec();
        format[0] = META_FORMAT;
        assert!(matches!(
            decode_block(&with_lines_value(&block, &format)),
            Err(Error::Corrupt(_))
        ));
        let mut length = lines.to_vec();
        length[1..5].copy_from_slice(&1u32.to_be_bytes());
        assert!(matches!(
            decode_block(&with_lines_value(&block, &length)),
            Err(Error::Corrupt(_))
        ));

        let rows = [LogEntry::new(5, "ab")];
        let cases: [(&str, &[u8], &[u8]); 6] = [
            ("timestamp outside bounds", &[9, 2, 0, 0], b"ab"),
            ("trailing bytes", &[0, 2, 0, 0, 7], b"ab"),
            ("lengths exceed lines", &[0, 3, 0, 0], b"ab"),
            ("name index", &[0, 2, 0, 1, 0, 0], b"ab"),
            ("invalid UTF-8", &[0, 2, 0, 0], &[0xc3, b'b']),
            ("overlong varint", &[0xff; 12], b""),
        ];
        for (case, meta, lines) in cases {
            let block = columns_block((5, 5), rows.len(), meta, lines);
            assert!(
                matches!(decode_block(&block), Err(Error::Corrupt(_))),
                "{case}"
            );
        }
        let split = columns_block((5, 6), 2, &[0, 1, 1, 1, 0, 0, 0], &[0xc3, 0xa9]);
        assert!(matches!(decode_block(&split), Err(Error::Corrupt(_))));
        let valid = columns_block((5, 5), 1, &[0, 2, 0, 0], b"ab");
        assert_eq!(decode_block(&valid).unwrap(), rows);
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
            objects[0].blocks[..2]
                .iter()
                .map(Block::encoded_len)
                .sum::<usize>()
        );
        assert_eq!(objects[0].runs[0].line_bytes, 18);

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
                    line_bytes: run.line_bytes,
                    duplicate_free: run.duplicate_free,
                    error_metadata: run.error_metadata,
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
        assert!(merged.blocks[0].meta.as_ptr() == full_block.meta.as_ptr());
        assert_eq!(merged.blocks[0].lines, full_block.lines);
        assert_eq!(
            merged
                .runs
                .iter()
                .map(|run| run.line_bytes)
                .collect::<Vec<_>>(),
            [3, 3, 3]
        );
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
            merged.blocks.iter().map(Block::encoded_len).sum::<usize>()
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

    #[test]
    fn runs_record_whether_they_hold_duplicates() {
        let line = |timestamp, text: &str| LogEntry::new(timestamp, text);
        assert!(duplicate_free(&[line(2, "a"), line(1, "a"), line(2, "b")]));
        assert!(!duplicate_free(&[line(2, "a"), line(1, "a"), line(2, "a")]));
        assert!(duplicate_free(&[
            with_metadata(1, "a", &[("pod", "x")]),
            with_metadata(1, "a", &[("pod", "y")]),
        ]));

        let config = PageConfig {
            rows_per_block: 2,
            ..PageConfig::default()
        };
        let merge = |first: Vec<LogEntry>, second: Vec<LogEntry>| {
            let inputs = vec![
                written(1, &config, vec![(1, first)]),
                written(2, &config, vec![(1, second)]),
            ];
            merge_objects(&config, inputs).unwrap().runs[0].duplicate_free
        };
        assert!(merge(vec![line(5, "a")], vec![line(1, "a")]));
        // Overlapping inputs might share a row, so the merge cannot vouch.
        assert!(!merge(vec![line(1, "a"), line(5, "b")], vec![line(3, "c")]));
        assert!(!merge(vec![line(1, "a"), line(1, "a")], vec![line(3, "c")]));
    }

    proptest! {
        #[test]
        fn blocks_round_trip(lines in prop::collection::vec("[ -~]{0,80}", 1..200)) {
            let rows: Vec<_> = lines.into_iter().enumerate()
                .map(|(index, line)| LogEntry::new(index as i64, line))
                .collect();
            prop_assert_eq!(decode_block(&encode_block(&rows).unwrap()).unwrap(), rows);
        }

        #[test]
        fn columnar_blocks_round_trip_and_decode_ranges(
            rows in prop::collection::vec(
                (
                    -50i64..50,
                    "\\PC{0,40}",
                    prop::collection::btree_map("[a-c]{1,3}", "\\PC{0,8}", 0..4),
                ),
                1..64,
            ),
            sorted in any::<bool>(),
            range in (-60i64..60, 0i64..60),
        ) {
            let mut rows: Vec<_> = rows
                .into_iter()
                .map(|(timestamp, line, fields)| {
                    let fields = fields
                        .into_iter()
                        .map(|(name, value)| Field::new(name, value))
                        .collect();
                    LogEntry::with_structured_metadata(timestamp, line, Fields::new(fields).unwrap())
                })
                .collect();
            if sorted {
                rows.sort_by_key(|row| row.timestamp_ns);
            }
            let block = encode_block(&rows).unwrap();
            prop_assert_eq!(&decode_block(&block).unwrap(), &rows);
            let (start, end) = (range.0, range.0 + range.1);
            let expected: Vec<_> = rows
                .iter()
                .cloned()
                .enumerate()
                .filter(|(index, row)| (start..=end).contains(&row.timestamp_ns) && index % 3 != 1)
                .map(|(index, row)| (index as u32, row))
                .collect();
            let samples: Vec<_> = expected
                .iter()
                .map(|(index, row)| (*index, row.timestamp_ns, row.line.len() as u32))
                .collect();
            prop_assert_eq!(decode_where(&block, (start, end), |index| index % 3 != 1).unwrap(), expected);
            let mut sampled = Vec::new();
            decode_samples_where(&block, (start, end), |index| index % 3 != 1, false, |index, sample| {
                sampled.push((index, sample.timestamp_ns, sample.line_len));
            }).unwrap();
            prop_assert_eq!(sampled, samples);
        }

        #[test]
        fn mutated_columnar_blocks_never_panic(
            position in any::<prop::sample::Index>(),
            byte in any::<u8>(),
        ) {
            let rows = mixed_rows();
            let min = rows.iter().map(|row| row.timestamp_ns).min().unwrap();
            let (mut meta, mut lines) = encode_columns(&rows, min).unwrap();
            let position = position.index(meta.len() + lines.len());
            match meta.get_mut(position) {
                Some(slot) => *slot = byte,
                None => lines[position - meta.len()] = byte,
            }
            let block = columns_block((10, 20), rows.len(), &meta, &lines);
            if let Err(error) = decode_block(&block) {
                prop_assert!(matches!(error, Error::Corrupt(_)), "{}", error);
            }
        }
    }
}
