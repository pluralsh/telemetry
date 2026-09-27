// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;

use crate::{Error, PageConfig, Result, Trace, TraceId};

const MAGIC: &[u8; 4] = b"TRAK";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = 13;
const DIRECTORY_ENTRY_LEN: usize = 48;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceDirectoryEntry {
    pub trace_id: TraceId,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub resource_spans_count: u32,
    pub offset: u32,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

/// Immutable bounded page with one independent Snappy stream per trace.
#[derive(Clone, Debug)]
pub struct Page {
    bytes: Bytes,
    directory: Vec<TraceDirectoryEntry>,
}

impl Page {
    pub fn from_traces(traces: &[Trace], max_size_bytes: usize) -> Result<Self> {
        if traces.is_empty() {
            return Err(Error::Invalid(
                "cannot build an empty trace page".to_owned(),
            ));
        }
        if max_size_bytes < HEADER_LEN + DIRECTORY_ENTRY_LEN {
            return Err(Error::Invalid(
                "trace page size bound is too small".to_owned(),
            ));
        }
        let mut traces = traces.to_vec();
        traces.sort_by_key(|trace| trace.trace_id);
        if traces
            .windows(2)
            .any(|pair| pair[0].trace_id == pair[1].trace_id)
        {
            return Err(Error::Invalid(
                "a page cannot contain duplicate trace IDs".to_owned(),
            ));
        }
        let data_offset = HEADER_LEN
            .checked_add(
                traces
                    .len()
                    .checked_mul(DIRECTORY_ENTRY_LEN)
                    .ok_or_else(|| Error::Invalid("trace page directory overflow".to_owned()))?,
            )
            .ok_or_else(|| Error::Invalid("trace page directory overflow".to_owned()))?;
        let mut offset = data_offset;
        let mut directory = Vec::with_capacity(traces.len());
        let mut payloads = Vec::with_capacity(traces.len());
        for trace in &traces {
            let raw = encode_trace(trace)?;
            let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
            let (min_timestamp_ns, max_timestamp_ns) = trace.timestamp_range();
            directory.push(TraceDirectoryEntry {
                trace_id: trace.trace_id,
                min_timestamp_ns,
                max_timestamp_ns,
                resource_spans_count: to_u32(trace.resource_spans.len(), "resource spans count")?,
                offset: to_u32(offset, "trace payload offset")?,
                compressed_len: to_u32(compressed.len(), "compressed trace length")?,
                uncompressed_len: to_u32(raw.len(), "uncompressed trace length")?,
            });
            offset = offset
                .checked_add(compressed.len())
                .ok_or_else(|| Error::Invalid("trace page size overflow".to_owned()))?;
            payloads.push(compressed);
        }
        if offset > max_size_bytes {
            return Err(Error::Invalid(format!(
                "encoded trace page is {offset} bytes, exceeding {max_size_bytes}"
            )));
        }
        let mut bytes = BytesMut::with_capacity(offset);
        bytes.extend_from_slice(MAGIC);
        bytes.put_u8(FORMAT_VERSION);
        bytes.put_u32(to_u32(directory.len(), "trace count")?);
        bytes.put_u32(to_u32(offset, "page length")?);
        for entry in &directory {
            bytes.extend_from_slice(entry.trace_id.as_bytes());
            bytes.put_u64(entry.min_timestamp_ns);
            bytes.put_u64(entry.max_timestamp_ns);
            bytes.put_u32(entry.resource_spans_count);
            bytes.put_u32(entry.offset);
            bytes.put_u32(entry.compressed_len);
            bytes.put_u32(entry.uncompressed_len);
        }
        for payload in payloads {
            bytes.extend_from_slice(&payload);
        }
        Ok(Self {
            bytes: bytes.freeze(),
            directory,
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
            return Err(Error::Corrupt(
                "invalid trace page magic or header".to_owned(),
            ));
        }
        if bytes[4] != FORMAT_VERSION {
            return Err(Error::Corrupt(format!(
                "unsupported trace page version {}",
                bytes[4]
            )));
        }
        let trace_count = read_u32(&bytes, 5)? as usize;
        let declared_len = read_u32(&bytes, 9)? as usize;
        if declared_len != bytes.len() {
            return Err(Error::Corrupt("trace page length mismatch".to_owned()));
        }
        let directory_end = HEADER_LEN
            .checked_add(
                trace_count
                    .checked_mul(DIRECTORY_ENTRY_LEN)
                    .ok_or_else(|| Error::Corrupt("trace page directory overflow".to_owned()))?,
            )
            .ok_or_else(|| Error::Corrupt("trace page directory overflow".to_owned()))?;
        if directory_end > bytes.len() {
            return Err(Error::Corrupt("truncated trace page directory".to_owned()));
        }
        let mut directory = Vec::with_capacity(trace_count);
        let mut cursor = HEADER_LEN;
        let mut prior_id = None;
        let mut expected_offset = directory_end;
        for _ in 0..trace_count {
            let trace_id = TraceId::from_slice(&bytes[cursor..cursor + 16])
                .map_err(|error| Error::Corrupt(error.to_string()))?;
            let entry = TraceDirectoryEntry {
                trace_id,
                min_timestamp_ns: read_u64(&bytes, cursor + 16)?,
                max_timestamp_ns: read_u64(&bytes, cursor + 24)?,
                resource_spans_count: read_u32(&bytes, cursor + 32)?,
                offset: read_u32(&bytes, cursor + 36)?,
                compressed_len: read_u32(&bytes, cursor + 40)?,
                uncompressed_len: read_u32(&bytes, cursor + 44)?,
            };
            if prior_id.is_some_and(|prior| prior >= trace_id) {
                return Err(Error::Corrupt(
                    "trace page directory is not strictly ordered".to_owned(),
                ));
            }
            if entry.min_timestamp_ns > entry.max_timestamp_ns || entry.resource_spans_count == 0 {
                return Err(Error::Corrupt(
                    "invalid trace page directory metadata".to_owned(),
                ));
            }
            let start = entry.offset as usize;
            let end = start
                .checked_add(entry.compressed_len as usize)
                .ok_or_else(|| Error::Corrupt("trace payload bounds overflow".to_owned()))?;
            if start != expected_offset || end > bytes.len() {
                return Err(Error::Corrupt("invalid trace payload bounds".to_owned()));
            }
            expected_offset = end;
            prior_id = Some(trace_id);
            directory.push(entry);
            cursor += DIRECTORY_ENTRY_LEN;
        }
        if expected_offset != bytes.len() {
            return Err(Error::Corrupt("trailing bytes in trace page".to_owned()));
        }
        Ok(Self { bytes, directory })
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn directory(&self) -> &[TraceDirectoryEntry] {
        &self.directory
    }

    /// Decodes one trace without decompressing any neighboring trace.
    pub fn get_trace(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        let Ok(index) = self
            .directory
            .binary_search_by_key(&trace_id, |entry| entry.trace_id)
        else {
            return Ok(None);
        };
        self.decode_trace(index).map(Some)
    }

    pub fn decode_trace(&self, index: usize) -> Result<Trace> {
        let entry = self
            .directory
            .get(index)
            .ok_or_else(|| Error::Invalid(format!("trace index {index} is out of range")))?;
        let start = entry.offset as usize;
        let end = start + entry.compressed_len as usize;
        let raw = snap::raw::Decoder::new().decompress_vec(&self.bytes[start..end])?;
        if raw.len() != entry.uncompressed_len as usize {
            return Err(Error::Corrupt(
                "decompressed trace length mismatch".to_owned(),
            ));
        }
        let trace = decode_trace(&raw, entry.trace_id, entry.resource_spans_count)?;
        if trace.timestamp_range() != (entry.min_timestamp_ns, entry.max_timestamp_ns) {
            return Err(Error::Corrupt(
                "trace timestamps disagree with page directory".to_owned(),
            ));
        }
        Ok(trace)
    }
}

pub struct PageBuilder {
    config: PageConfig,
    traces: Vec<Trace>,
}

impl PageBuilder {
    pub fn new(config: PageConfig) -> Result<Self> {
        if config.target_size_bytes == 0
            || config.max_size_bytes == 0
            || config.max_traces == 0
            || config.target_size_bytes > config.max_size_bytes
        {
            return Err(Error::Invalid("invalid trace page limits".to_owned()));
        }
        Ok(Self {
            config,
            traces: Vec::new(),
        })
    }

    pub fn append(&mut self, trace: Trace) -> Result<Option<Page>> {
        if self.traces.is_empty() {
            Page::from_traces(std::slice::from_ref(&trace), self.config.max_size_bytes)?;
            self.traces.push(trace);
            return Ok(None);
        }
        let duplicate = self
            .traces
            .iter()
            .any(|existing| existing.trace_id == trace.trace_id);
        let mut prospective = self.traces.clone();
        prospective.push(trace.clone());
        let candidate = Page::from_traces(&prospective, self.config.max_size_bytes);
        let should_cut = duplicate
            || self.traces.len() >= self.config.max_traces
            || candidate
                .as_ref()
                .is_ok_and(|page| page.bytes.len() > self.config.target_size_bytes)
            || candidate.is_err();
        if should_cut {
            let page = Page::from_traces(&self.traces, self.config.max_size_bytes)?;
            self.traces.clear();
            Page::from_traces(std::slice::from_ref(&trace), self.config.max_size_bytes)?;
            self.traces.push(trace);
            Ok(Some(page))
        } else {
            self.traces.push(trace);
            Ok(None)
        }
    }

    pub fn finish(&mut self) -> Result<Option<Page>> {
        if self.traces.is_empty() {
            return Ok(None);
        }
        let page = Page::from_traces(&self.traces, self.config.max_size_bytes)?;
        self.traces.clear();
        Ok(Some(page))
    }
}

fn encode_trace(trace: &Trace) -> Result<Vec<u8>> {
    let mut raw = BytesMut::new();
    raw.put_u32(to_u32(trace.resource_spans.len(), "resource spans count")?);
    for resource_spans in &trace.resource_spans {
        raw.put_u32(to_u32(
            resource_spans.encoded_len(),
            "resource spans length",
        )?);
        resource_spans
            .encode(&mut raw)
            .map_err(|error| Error::Invalid(format!("failed to encode OTLP trace: {error}")))?;
    }
    Ok(raw.to_vec())
}

fn decode_trace(raw: &[u8], trace_id: TraceId, expected_count: u32) -> Result<Trace> {
    if raw.len() < 4 {
        return Err(Error::Corrupt("truncated trace payload".to_owned()));
    }
    let count = u32::from_be_bytes(raw[..4].try_into().unwrap());
    if count != expected_count {
        return Err(Error::Corrupt(
            "resource spans count does not match directory".to_owned(),
        ));
    }
    let mut remaining = &raw[4..];
    let mut resource_spans = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if remaining.len() < 4 {
            return Err(Error::Corrupt("truncated resource spans length".to_owned()));
        }
        let length = u32::from_be_bytes(remaining[..4].try_into().unwrap()) as usize;
        remaining = &remaining[4..];
        if remaining.len() < length {
            return Err(Error::Corrupt("truncated resource spans".to_owned()));
        }
        resource_spans.push(ResourceSpans::decode(&remaining[..length])?);
        remaining = &remaining[length..];
    }
    if !remaining.is_empty() {
        return Err(Error::Corrupt("trailing bytes in trace payload".to_owned()));
    }
    Trace::new(trace_id, resource_spans).map_err(|error| Error::Corrupt(error.to_string()))
}

fn to_u32(value: usize, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::Invalid(format!("{field} exceeds u32")))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    bytes
        .get(offset..offset + 4)
        .map(|value| u32::from_be_bytes(value.try_into().unwrap()))
        .ok_or_else(|| Error::Corrupt("truncated trace page integer".to_owned()))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    bytes
        .get(offset..offset + 8)
        .map(|value| u64::from_be_bytes(value.try_into().unwrap()))
        .ok_or_else(|| Error::Corrupt("truncated trace page integer".to_owned()))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use proptest::prelude::*;

    use super::*;

    fn trace(id: u8, start: u64, name: impl Into<String>) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        Trace::new(
            trace_id,
            vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        name: name.into(),
                        start_time_unix_nano: start,
                        end_time_unix_nano: start + 10,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        )
        .unwrap()
    }

    #[test]
    fn page_round_trip_and_selective_decode() {
        let traces = vec![trace(1, 10, "one"), trace(2, 20, "two")];
        let page = Page::from_traces(&traces, 64 * 1024).unwrap();
        let decoded = Page::decode(page.bytes()).unwrap();
        assert_eq!(decoded.directory().len(), 2);
        assert_eq!(
            decoded.get_trace(traces[1].trace_id).unwrap(),
            Some(traces[1].clone())
        );
    }

    #[test]
    fn neighboring_corruption_does_not_prevent_selective_decode() {
        let first = trace(1, 10, "one");
        let second = trace(2, 20, "two");
        let page = Page::from_traces(&[first.clone(), second.clone()], 64 * 1024).unwrap();
        let mut bytes = page.bytes().to_vec();
        *bytes.last_mut().unwrap() ^= 0xff;
        let decoded = Page::decode(Bytes::from(bytes)).unwrap();
        assert_eq!(decoded.get_trace(first.trace_id).unwrap(), Some(first));
        assert!(decoded.get_trace(second.trace_id).is_err());
    }

    #[test]
    fn rejects_bad_version_bounds_and_size_limit() {
        let trace = trace(1, 10, "one");
        let page = Page::from_traces(std::slice::from_ref(&trace), 64 * 1024).unwrap();
        let mut version = page.bytes().to_vec();
        version[4] = 99;
        assert!(Page::decode(Bytes::from(version)).is_err());

        let mut bounds = page.bytes().to_vec();
        bounds[HEADER_LEN + 36..HEADER_LEN + 40].copy_from_slice(&0_u32.to_be_bytes());
        assert!(Page::decode(Bytes::from(bounds)).is_err());
        assert!(Page::from_traces(&[trace], HEADER_LEN + DIRECTORY_ENTRY_LEN).is_err());
    }

    proptest! {
        #[test]
        fn page_property_round_trips(
            starts in prop::collection::vec(0_u64..1_000_000, 1..30)
        ) {
            let traces: Vec<_> = starts.into_iter().enumerate()
                .map(|(index, start)| trace((index + 1) as u8, start, format!("span-{index}")))
                .collect();
            let page = Page::decode(
                Page::from_traces(&traces, 1024 * 1024).unwrap().bytes()
            ).unwrap();
            for trace in traces {
                prop_assert_eq!(page.get_trace(trace.trace_id).unwrap(), Some(trace));
            }
        }
    }
}
