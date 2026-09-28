// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::config::PageConfig;
use crate::error::{Error, Result};
use crate::model::{Field, Fields, LogEntry};

const MAGIC: &[u8; 4] = b"LINE";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = 13;
const BLOCK_DIRECTORY_ENTRY_LEN: usize = 32;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockMetadata {
    pub min_timestamp_ns: i64,
    pub max_timestamp_ns: i64,
    pub row_count: u32,
    pub offset: u32,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

/// Immutable compressed page. Every row block is a separate Snappy stream.
#[derive(Clone, Debug)]
pub struct Page {
    bytes: Bytes,
    blocks: Vec<BlockMetadata>,
    row_count: u32,
}

impl Page {
    pub fn from_entries(entries: &[LogEntry], rows_per_block: usize) -> Result<Self> {
        if entries.is_empty() {
            return Err(Error::Invalid("cannot build an empty page".to_owned()));
        }
        if rows_per_block == 0 {
            return Err(Error::Invalid("rows_per_block must be positive".to_owned()));
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].timestamp_ns > pair[1].timestamp_ns)
        {
            return Err(Error::Invalid(
                "page entries must be timestamp ordered".to_owned(),
            ));
        }

        let chunks: Vec<&[LogEntry]> = entries.chunks(rows_per_block).collect();
        let data_offset = HEADER_LEN
            .checked_add(chunks.len() * BLOCK_DIRECTORY_ENTRY_LEN)
            .ok_or_else(|| Error::Invalid("page directory is too large".to_owned()))?;
        let mut compressed_blocks = Vec::with_capacity(chunks.len());
        let mut blocks = Vec::with_capacity(chunks.len());
        let mut offset = data_offset;

        for chunk in chunks {
            let raw = encode_rows(chunk)?;
            let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
            let metadata = BlockMetadata {
                min_timestamp_ns: chunk.first().unwrap().timestamp_ns,
                max_timestamp_ns: chunk.last().unwrap().timestamp_ns,
                row_count: to_u32(chunk.len(), "block row count")?,
                offset: to_u32(offset, "block offset")?,
                compressed_len: to_u32(compressed.len(), "compressed block length")?,
                uncompressed_len: to_u32(raw.len(), "uncompressed block length")?,
            };
            offset = offset
                .checked_add(compressed.len())
                .ok_or_else(|| Error::Invalid("page is too large".to_owned()))?;
            blocks.push(metadata);
            compressed_blocks.push(compressed);
        }

        let mut bytes = BytesMut::with_capacity(offset);
        bytes.extend_from_slice(MAGIC);
        bytes.put_u8(FORMAT_VERSION);
        bytes.put_u32(to_u32(blocks.len(), "block count")?);
        bytes.put_u32(to_u32(entries.len(), "page row count")?);
        for block in &blocks {
            bytes.put_i64(block.min_timestamp_ns);
            bytes.put_i64(block.max_timestamp_ns);
            bytes.put_u32(block.row_count);
            bytes.put_u32(block.offset);
            bytes.put_u32(block.compressed_len);
            bytes.put_u32(block.uncompressed_len);
        }
        for compressed in compressed_blocks {
            bytes.extend_from_slice(&compressed);
        }
        Ok(Self {
            bytes: bytes.freeze(),
            blocks,
            row_count: to_u32(entries.len(), "page row count")?,
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
            return Err(Error::Corrupt("invalid page magic or header".to_owned()));
        }
        if bytes[4] != FORMAT_VERSION {
            return Err(Error::Corrupt(format!(
                "unsupported page version {}",
                bytes[4]
            )));
        }
        let block_count = read_u32(&bytes, 5)? as usize;
        let row_count = read_u32(&bytes, 9)?;
        let directory_end = HEADER_LEN
            .checked_add(block_count * BLOCK_DIRECTORY_ENTRY_LEN)
            .ok_or_else(|| Error::Corrupt("page directory overflow".to_owned()))?;
        if directory_end > bytes.len() {
            return Err(Error::Corrupt("truncated page directory".to_owned()));
        }

        let mut blocks = Vec::with_capacity(block_count);
        let mut cursor = HEADER_LEN;
        let mut rows = 0_u32;
        for _ in 0..block_count {
            let block = BlockMetadata {
                min_timestamp_ns: read_i64(&bytes, cursor)?,
                max_timestamp_ns: read_i64(&bytes, cursor + 8)?,
                row_count: read_u32(&bytes, cursor + 16)?,
                offset: read_u32(&bytes, cursor + 20)?,
                compressed_len: read_u32(&bytes, cursor + 24)?,
                uncompressed_len: read_u32(&bytes, cursor + 28)?,
            };
            let end = block.offset as usize + block.compressed_len as usize;
            if (block.offset as usize) < directory_end || end > bytes.len() {
                return Err(Error::Corrupt("invalid page block bounds".to_owned()));
            }
            rows = rows
                .checked_add(block.row_count)
                .ok_or_else(|| Error::Corrupt("page row count overflow".to_owned()))?;
            blocks.push(block);
            cursor += BLOCK_DIRECTORY_ENTRY_LEN;
        }
        if rows != row_count {
            return Err(Error::Corrupt("page row count mismatch".to_owned()));
        }
        Ok(Self {
            bytes,
            blocks,
            row_count,
        })
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn blocks(&self) -> &[BlockMetadata] {
        &self.blocks
    }

    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Decodes exactly one block without decompressing any neighboring block.
    pub fn decode_block(&self, index: usize) -> Result<Vec<LogEntry>> {
        let block = self
            .blocks
            .get(index)
            .ok_or_else(|| Error::Invalid(format!("block index {index} is out of range")))?;
        let start = block.offset as usize;
        let end = start + block.compressed_len as usize;
        let raw = snap::raw::Decoder::new().decompress_vec(&self.bytes[start..end])?;
        if raw.len() != block.uncompressed_len as usize {
            return Err(Error::Corrupt(
                "decompressed block length mismatch".to_owned(),
            ));
        }
        decode_rows(&raw, block.row_count)
    }

    pub fn decode_range(&self, start_ns: i64, end_ns: i64) -> Result<Vec<LogEntry>> {
        Ok(self
            .decode_range_with_ids(start_ns, end_ns)?
            .into_iter()
            .map(|(_, entry)| entry)
            .collect())
    }

    pub(crate) fn decode_range_with_ids(
        &self,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<(u32, LogEntry)>> {
        self.decode_rows_where(start_ns, end_ns, |_| true)
    }

    /// Like [`decode_range_with_ids`](Self::decode_range_with_ids), limited
    /// to row IDs accepted by `wanted`. Blocks holding no wanted row are never
    /// decompressed.
    pub(crate) fn decode_rows_where(
        &self,
        start_ns: i64,
        end_ns: i64,
        wanted: impl Fn(u32) -> bool,
    ) -> Result<Vec<(u32, LogEntry)>> {
        let mut result = Vec::new();
        let mut first_row = 0u32;
        for (index, block) in self.blocks.iter().enumerate() {
            let rows = first_row..first_row.saturating_add(block.row_count);
            first_row = rows.end;
            if block.max_timestamp_ns < start_ns
                || block.min_timestamp_ns > end_ns
                || !rows.clone().any(&wanted)
            {
                continue;
            }
            result.extend(
                self.decode_block(index)?
                    .into_iter()
                    .zip(rows)
                    .filter(|(entry, row_id)| {
                        entry.timestamp_ns >= start_ns
                            && entry.timestamp_ns <= end_ns
                            && wanted(*row_id)
                    })
                    .map(|(entry, row_id)| (row_id, entry)),
            );
        }
        Ok(result)
    }
}

/// Accumulates ordered rows and cuts immutable pages by size, rows, or age.
pub struct PageBuilder {
    config: PageConfig,
    entries: Vec<LogEntry>,
    estimated_bytes: usize,
    created_at: Instant,
}

impl PageBuilder {
    pub fn new(config: PageConfig, now: Instant) -> Result<Self> {
        if config.target_size_bytes == 0 || config.max_rows == 0 || config.rows_per_block == 0 {
            return Err(Error::Invalid("page limits must be positive".to_owned()));
        }
        Ok(Self {
            config,
            entries: Vec::new(),
            estimated_bytes: 0,
            created_at: now,
        })
    }

    pub fn append(&mut self, entry: LogEntry, now: Instant) -> Result<Option<Page>> {
        Ok(self.append_with_rows(entry, now)?.map(|(page, _)| page))
    }

    pub fn finish(&mut self) -> Result<Option<Page>> {
        Ok(self.finish_with_rows()?.map(|(page, _)| page))
    }

    /// Like [`Self::append`], but also returns the rows of a completed page.
    pub(crate) fn append_with_rows(
        &mut self,
        entry: LogEntry,
        now: Instant,
    ) -> Result<Option<(Page, Vec<LogEntry>)>> {
        if self
            .entries
            .last()
            .is_some_and(|last| last.timestamp_ns > entry.timestamp_ns)
        {
            return Err(Error::Invalid(
                "page builder entries must be timestamp ordered".to_owned(),
            ));
        }
        let row_size = estimated_row_size(&entry);
        let should_cut = !self.entries.is_empty()
            && (self.entries.len() >= self.config.max_rows
                || self.estimated_bytes.saturating_add(row_size) > self.config.target_size_bytes
                || now.duration_since(self.created_at) >= self.config.max_age);
        let completed = if should_cut {
            let page = Page::from_entries(&self.entries, self.config.rows_per_block)?;
            let capacity = self.entries.len();
            let rows = std::mem::replace(&mut self.entries, Vec::with_capacity(capacity));
            self.estimated_bytes = 0;
            self.created_at = now;
            Some((page, rows))
        } else {
            None
        };
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_size);
        self.entries.push(entry);
        Ok(completed)
    }

    pub(crate) fn finish_with_rows(&mut self) -> Result<Option<(Page, Vec<LogEntry>)>> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        let page = Page::from_entries(&self.entries, self.config.rows_per_block)?;
        self.estimated_bytes = 0;
        Ok(Some((page, std::mem::take(&mut self.entries))))
    }
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

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    bytes
        .get(offset..offset + 4)
        .map(|value| u32::from_be_bytes(value.try_into().unwrap()))
        .ok_or_else(|| Error::Corrupt("truncated page integer".to_owned()))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64> {
    bytes
        .get(offset..offset + 8)
        .map(|value| i64::from_be_bytes(value.try_into().unwrap()))
        .ok_or_else(|| Error::Corrupt("truncated page timestamp".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn blocks_are_independently_decodable() {
        let entries: Vec<_> = (0..10)
            .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
            .collect();
        let page = Page::from_entries(&entries, 3).unwrap();
        assert_eq!(page.blocks().len(), 4);
        assert_eq!(page.decode_block(1).unwrap(), entries[3..6]);
        assert_eq!(page.decode_range(4, 7).unwrap(), entries[4..=7]);
    }

    #[test]
    fn selected_rows_match_a_full_decode() {
        let entries: Vec<_> = (0..10)
            .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
            .collect();
        let page = Page::from_entries(&entries, 3).unwrap();
        let wanted = [1u32, 7, 8, 9];
        let expected: Vec<_> = page
            .decode_range_with_ids(2, 8)
            .unwrap()
            .into_iter()
            .filter(|(row_id, _)| wanted.contains(row_id))
            .collect();
        let selected = page
            .decode_rows_where(2, 8, |row_id| wanted.contains(&row_id))
            .unwrap();
        assert_eq!(selected, expected);
        assert_eq!(
            selected
                .iter()
                .map(|(row_id, _)| *row_id)
                .collect::<Vec<_>>(),
            [7, 8]
        );
    }

    #[test]
    fn structured_metadata_round_trips_in_canonical_order() {
        let fields = Fields::new(vec![
            Field::new("trace_id", "abc"),
            Field::new("severity", "info"),
        ])
        .unwrap();
        let entry = LogEntry::with_structured_metadata(42, "hello", fields);
        let page = Page::from_entries(std::slice::from_ref(&entry), 1).unwrap();

        assert_eq!(page.decode_block(0).unwrap(), vec![entry]);
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
    fn builder_cuts_on_each_policy() {
        let config = PageConfig {
            target_size_bytes: 100,
            max_rows: 2,
            max_age: Duration::from_secs(1),
            rows_per_block: 2,
        };
        let now = Instant::now();
        let mut builder = PageBuilder::new(config, now).unwrap();
        assert!(
            builder
                .append(LogEntry::new(1, "a"), now)
                .unwrap()
                .is_none()
        );
        assert!(
            builder
                .append(LogEntry::new(2, "b"), now)
                .unwrap()
                .is_none()
        );
        assert!(
            builder
                .append(LogEntry::new(3, "c"), now)
                .unwrap()
                .is_some()
        );
        assert!(
            builder
                .append(LogEntry::new(4, "d"), now + Duration::from_secs(2))
                .unwrap()
                .is_some()
        );
    }

    proptest! {
        #[test]
        fn page_round_trips(lines in prop::collection::vec("[ -~]{0,80}", 1..200)) {
            let entries: Vec<_> = lines.into_iter().enumerate()
                .map(|(index, line)| LogEntry::new(index as i64, line))
                .collect();
            let encoded = Page::from_entries(&entries, 17).unwrap().bytes();
            let decoded = Page::decode(encoded).unwrap();
            prop_assert_eq!(decoded.decode_range(i64::MIN, i64::MAX).unwrap(), entries);
        }
    }
}
