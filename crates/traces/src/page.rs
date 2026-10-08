// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use bytes::{BufMut, Bytes, BytesMut};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;

use crate::sidecar::{self, PageColumns};
use crate::{Error, PageConfig, Result, Trace, TraceId};

const MAGIC: &[u8; 4] = b"TRAK";
const FORMAT_VERSION: u8 = 3;
const HEADER_LEN: usize = 13;
const DIRECTORY_ENTRY_LEN: usize = 48;
/// Compressed and uncompressed sidecar lengths, ahead of the sidecar.
const SIDECAR_HEADER_LEN: usize = 8;

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

/// Immutable bounded page with one independent Snappy stream per trace and a
/// Snappy column sidecar of span intrinsics.
#[derive(Clone, Debug)]
pub struct Page {
    bytes: Bytes,
    directory: Vec<TraceDirectoryEntry>,
    sidecar: SidecarBounds,
    columns: OnceLock<Arc<PageColumns>>,
}

#[derive(Clone, Copy, Debug)]
struct SidecarBounds {
    /// Compressed bytes within the page.
    start: usize,
    end: usize,
    uncompressed_len: usize,
}

impl SidecarBounds {
    fn range(self) -> Range<usize> {
        self.start..self.end
    }
}

/// One trace encoded and compressed exactly once, ready to be laid out.
struct EncodedTrace {
    trace: Trace,
    compressed: Vec<u8>,
    uncompressed_len: usize,
}

impl EncodedTrace {
    fn new(trace: Trace) -> Result<Self> {
        let raw = encode_trace(&trace)?;
        let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
        Ok(Self {
            trace,
            compressed,
            uncompressed_len: raw.len(),
        })
    }
}

fn page_len(traces: usize, payload_bytes: usize) -> Option<usize> {
    HEADER_LEN
        .checked_add(traces.checked_mul(DIRECTORY_ENTRY_LEN)?)?
        .checked_add(payload_bytes)
}

impl Page {
    pub fn from_traces(traces: &[Trace], max_size_bytes: usize) -> Result<Self> {
        let encoded = traces
            .iter()
            .cloned()
            .map(EncodedTrace::new)
            .collect::<Result<Vec<_>>>()?;
        Self::assemble(encoded, max_size_bytes).map(|(page, _)| page)
    }

    /// Lays out already-encoded traces; returns the traces in directory order.
    ///
    /// `max_size_bytes` bounds the trace data; the column sidecar is written
    /// in addition.
    fn assemble(
        mut traces: Vec<EncodedTrace>,
        max_size_bytes: usize,
    ) -> Result<(Self, Vec<Trace>)> {
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
        traces.sort_by_key(|encoded| encoded.trace.trace_id);
        if traces
            .windows(2)
            .any(|pair| pair[0].trace.trace_id == pair[1].trace.trace_id)
        {
            return Err(Error::Invalid(
                "a page cannot contain duplicate trace IDs".to_owned(),
            ));
        }
        let overflow = || Error::Invalid("trace page size overflow".to_owned());
        let payload_bytes = traces
            .iter()
            .try_fold(0usize, |sum, encoded| {
                sum.checked_add(encoded.compressed.len())
            })
            .ok_or_else(overflow)?;
        let data_len = page_len(traces.len(), payload_bytes).ok_or_else(overflow)?;
        if data_len > max_size_bytes {
            return Err(Error::Invalid(format!(
                "encoded trace page is {data_len} bytes, exceeding {max_size_bytes}"
            )));
        }
        let directory_end = data_len - payload_bytes;
        let raw = sidecar::encode(traces.iter().map(|encoded| &encoded.trace))?;
        let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
        let total = data_len
            .checked_add(SIDECAR_HEADER_LEN + compressed.len())
            .ok_or_else(overflow)?;
        let total_u32 = to_u32(total, "page length")?;
        let start = directory_end + SIDECAR_HEADER_LEN;
        let sidecar = SidecarBounds {
            start,
            end: start + compressed.len(),
            uncompressed_len: raw.len(),
        };
        let mut offset = sidecar.end;
        let mut directory = Vec::with_capacity(traces.len());
        for encoded in &traces {
            let (min_timestamp_ns, max_timestamp_ns) = encoded.trace.timestamp_range();
            directory.push(TraceDirectoryEntry {
                trace_id: encoded.trace.trace_id,
                min_timestamp_ns,
                max_timestamp_ns,
                resource_spans_count: to_u32(
                    encoded.trace.resource_spans.len(),
                    "resource spans count",
                )?,
                offset: to_u32(offset, "trace payload offset")?,
                compressed_len: to_u32(encoded.compressed.len(), "compressed trace length")?,
                uncompressed_len: to_u32(encoded.uncompressed_len, "uncompressed trace length")?,
            });
            offset += encoded.compressed.len();
        }
        let mut bytes = BytesMut::with_capacity(total);
        bytes.extend_from_slice(MAGIC);
        bytes.put_u8(FORMAT_VERSION);
        bytes.put_u32(to_u32(directory.len(), "trace count")?);
        bytes.put_u32(total_u32);
        for entry in &directory {
            bytes.extend_from_slice(entry.trace_id.as_bytes());
            bytes.put_u64(entry.min_timestamp_ns);
            bytes.put_u64(entry.max_timestamp_ns);
            bytes.put_u32(entry.resource_spans_count);
            bytes.put_u32(entry.offset);
            bytes.put_u32(entry.compressed_len);
            bytes.put_u32(entry.uncompressed_len);
        }
        bytes.put_u32(to_u32(compressed.len(), "compressed sidecar length")?);
        bytes.put_u32(to_u32(raw.len(), "sidecar length")?);
        bytes.extend_from_slice(&compressed);
        let mut ordered = Vec::with_capacity(traces.len());
        for encoded in traces {
            bytes.extend_from_slice(&encoded.compressed);
            ordered.push(encoded.trace);
        }
        Ok((
            Self {
                bytes: bytes.freeze(),
                directory,
                sidecar,
                columns: OnceLock::new(),
            },
            ordered,
        ))
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
        let compressed_len = read_u32(&bytes, directory_end)? as usize;
        let uncompressed_len = read_u32(&bytes, directory_end + 4)? as usize;
        let start = directory_end + SIDECAR_HEADER_LEN;
        let end = start
            .checked_add(compressed_len)
            .filter(|&end| end <= bytes.len())
            .ok_or_else(|| Error::Corrupt("invalid trace page sidecar bounds".to_owned()))?;
        let sidecar = SidecarBounds {
            start,
            end,
            uncompressed_len,
        };
        let mut directory = Vec::with_capacity(trace_count);
        let mut cursor = HEADER_LEN;
        let mut prior_id = None;
        let mut expected_offset = sidecar.end;
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
        Ok(Self {
            bytes,
            directory,
            sidecar,
            columns: OnceLock::new(),
        })
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    /// Compressed sidecar bytes, including their length header.
    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn sidecar_len(&self) -> usize {
        SIDECAR_HEADER_LEN + self.sidecar.range().len()
    }

    /// The page's span columns, decoded on first use.
    pub(crate) fn columns(&self) -> Result<Arc<PageColumns>> {
        if let Some(columns) = self.columns.get() {
            return Ok(Arc::clone(columns));
        }
        let compressed = &self.bytes[self.sidecar.range()];
        if snap::raw::decompress_len(compressed)? != self.sidecar.uncompressed_len {
            return Err(Error::Corrupt(
                "trace page sidecar length mismatch".to_owned(),
            ));
        }
        let raw = snap::raw::Decoder::new().decompress_vec(compressed)?;
        let columns = Arc::new(PageColumns::decode(&raw, self.directory.len())?);
        Ok(Arc::clone(self.columns.get_or_init(|| columns)))
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

/// Cuts pages by trace count and exact encoded size of the trace data; the
/// column sidecar counts toward neither size limit. Each trace is encoded and
/// compressed once, on append.
pub struct PageBuilder {
    config: PageConfig,
    traces: Vec<EncodedTrace>,
    trace_ids: HashSet<TraceId>,
    payload_bytes: usize,
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
            trace_ids: HashSet::new(),
            payload_bytes: 0,
        })
    }

    pub fn append(&mut self, trace: Trace) -> Result<Option<Page>> {
        Ok(self.append_with_traces(trace)?.map(|(page, _)| page))
    }

    pub fn finish(&mut self) -> Result<Option<Page>> {
        Ok(self.finish_with_traces()?.map(|(page, _)| page))
    }

    /// Like [`Self::append`], but also returns a completed page's traces in
    /// directory order.
    pub(crate) fn append_with_traces(
        &mut self,
        trace: Trace,
    ) -> Result<Option<(Page, Vec<Trace>)>> {
        let encoded = EncodedTrace::new(trace)?;
        let alone = page_len(1, encoded.compressed.len());
        if alone.is_none_or(|len| len > self.config.max_size_bytes) {
            return Err(Error::Invalid(format!(
                "encoded trace page exceeds {} bytes",
                self.config.max_size_bytes
            )));
        }
        let should_cut = !self.traces.is_empty()
            && (self.trace_ids.contains(&encoded.trace.trace_id)
                || self.traces.len() >= self.config.max_traces
                || self
                    .payload_bytes
                    .checked_add(encoded.compressed.len())
                    .and_then(|payload_bytes| page_len(self.traces.len() + 1, payload_bytes))
                    .is_none_or(|len| len > self.config.target_size_bytes));
        let completed = if should_cut {
            self.finish_with_traces()?
        } else {
            None
        };
        self.payload_bytes += encoded.compressed.len();
        self.trace_ids.insert(encoded.trace.trace_id);
        self.traces.push(encoded);
        Ok(completed)
    }

    pub(crate) fn finish_with_traces(&mut self) -> Result<Option<(Page, Vec<Trace>)>> {
        if self.traces.is_empty() {
            return Ok(None);
        }
        self.trace_ids.clear();
        self.payload_bytes = 0;
        Page::assemble(std::mem::take(&mut self.traces), self.config.max_size_bytes).map(Some)
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
    common::serde::be_u32_at(bytes, offset)
        .ok_or_else(|| Error::Corrupt("truncated trace page integer".to_owned()))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    common::serde::be_u64_at(bytes, offset)
        .ok_or_else(|| Error::Corrupt("truncated trace page integer".to_owned()))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
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
        for stale in [1, 99] {
            let mut version = page.bytes().to_vec();
            version[4] = stale;
            assert!(matches!(
                Page::decode(Bytes::from(version)),
                Err(Error::Corrupt(message))
                    if message == format!("unsupported trace page version {stale}")
            ));
        }

        let mut bounds = page.bytes().to_vec();
        bounds[HEADER_LEN + 36..HEADER_LEN + 40].copy_from_slice(&0_u32.to_be_bytes());
        assert!(Page::decode(Bytes::from(bounds)).is_err());
        assert!(Page::from_traces(&[trace], HEADER_LEN + DIRECTORY_ENTRY_LEN).is_err());
    }

    #[test]
    fn builder_cuts_by_exact_size_count_and_duplicate_ids() {
        let traces: Vec<_> = (1..=6)
            .map(|id| trace(id, 100 + u64::from(id), "span"))
            .collect();
        let single = Page::from_traces(&traces[..1], 64 * 1024)
            .unwrap()
            .bytes()
            .len();
        let two = Page::from_traces(&traces[..2], 64 * 1024)
            .unwrap()
            .bytes()
            .len();
        let mut builder = PageBuilder::new(PageConfig {
            target_size_bytes: two,
            max_size_bytes: 64 * 1024,
            max_traces: 3,
        })
        .unwrap();
        assert!(single < two);

        let mut pages = Vec::new();
        for trace in traces.iter().rev().cloned() {
            pages.extend(builder.append_with_traces(trace).unwrap());
        }
        pages.extend(builder.append_with_traces(traces[0].clone()).unwrap());
        pages.extend(builder.finish_with_traces().unwrap());

        let counts: Vec<_> = pages
            .iter()
            .map(|(page, _)| page.directory().len())
            .collect();
        assert_eq!(counts, vec![2, 2, 2, 1]);
        for (page, ordered) in &pages {
            assert!(page.bytes().len() <= two);
            let expected = Page::from_traces(ordered, 64 * 1024).unwrap();
            assert_eq!(page.bytes(), expected.bytes());
            for (entry, trace) in page.directory().iter().zip(ordered) {
                assert_eq!(entry.trace_id, trace.trace_id);
            }
        }
    }

    #[test]
    fn pages_carry_columns() {
        let traces = vec![trace(1, 10, "one"), trace(2, 20, "two")];
        let page = Page::decode(Page::from_traces(&traces, 64 * 1024).unwrap().bytes()).unwrap();
        assert_eq!(page.bytes()[4], FORMAT_VERSION);
        let columns = page.columns().unwrap();
        for (index, trace) in traces.iter().enumerate() {
            assert_eq!(
                page.get_trace(trace.trace_id).unwrap().as_ref(),
                Some(trace)
            );
            assert_eq!(columns.trace_spans(index), index..index + 1);
        }
    }

    #[test]
    fn size_bound_excludes_the_sidecar() {
        let traces = vec![trace(1, 10, "one")];
        let page = Page::from_traces(&traces, 64 * 1024).unwrap();
        let data_len = page.bytes().len() - page.sidecar_len();
        let bounded = Page::from_traces(&traces, data_len).unwrap();
        assert_eq!(bounded.bytes(), page.bytes());
        assert!(bounded.bytes().len() > data_len);
        assert!(Page::from_traces(&traces, data_len - 1).is_err());
    }

    #[test]
    fn corrupt_sidecars_are_errors() {
        let traces = vec![trace(1, 10, "one"), trace(2, 20, "two")];
        let page = Page::from_traces(&traces, 64 * 1024).unwrap();
        let sidecar = page.sidecar;
        let header = sidecar.start - SIDECAR_HEADER_LEN;

        let mut bounds = page.bytes().to_vec();
        bounds[header..header + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(Page::decode(Bytes::from(bounds)).is_err());

        let mut length = page.bytes().to_vec();
        length[header + 4..header + 8].copy_from_slice(&1_u32.to_be_bytes());
        let decoded = Page::decode(Bytes::from(length)).unwrap();
        assert!(decoded.columns().is_err());
        assert_eq!(
            decoded.get_trace(traces[1].trace_id).unwrap().as_ref(),
            Some(&traces[1])
        );

        for position in sidecar.range() {
            for flip in [0x01, 0x80, 0xff] {
                let mut bytes = page.bytes().to_vec();
                bytes[position] ^= flip;
                let decoded = Page::decode(Bytes::from(bytes)).unwrap();
                let _ = decoded.columns();
                assert_eq!(
                    decoded.get_trace(traces[0].trace_id).unwrap().as_ref(),
                    Some(&traces[0])
                );
            }
        }
        let raw = sidecar::encode(&traces).unwrap();
        for len in 0..raw.len() {
            assert!(PageColumns::decode(&raw[..len], 2).is_err());
        }
        assert!(PageColumns::decode(&raw, 3).is_err());
        assert!(PageColumns::decode(&raw, 2).is_ok());
    }

    #[test]
    fn corrupt_attribute_columns_fail_the_filters_reading_them() {
        let traces = [(1, "GET", 200), (2, "POST", 503)].map(|(id, method, status)| {
            let trace_id = TraceId::new([id; 16]).unwrap();
            let attribute = |key: &str, value| KeyValue {
                key: key.to_owned(),
                value: Some(AnyValue { value: Some(value) }),
            };
            let span = Span {
                trace_id: trace_id.as_bytes().to_vec(),
                attributes: vec![
                    attribute(
                        "http.method",
                        any_value::Value::StringValue(method.to_owned()),
                    ),
                    attribute("http.status_code", any_value::Value::IntValue(status)),
                ],
                ..Default::default()
            };
            let scope_spans = vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }];
            let resource_spans = vec![ResourceSpans {
                scope_spans,
                ..Default::default()
            }];
            Trace::new(trace_id, resource_spans).unwrap()
        });
        let query = crate::traceql::parse(
            r#"{ span.http.method = "GET" || span.http.status_code >= 500 }"#,
        )
        .unwrap();
        let filter = crate::traceql::prefilter(&query).unwrap();
        let raw = sidecar::encode(&traces).unwrap();
        assert!(
            PageColumns::decode(&raw, 2)
                .unwrap()
                .exists(&filter)
                .is_ok()
        );

        let mut lazily_caught = 0;
        for position in 0..raw.len() {
            for flip in [0x01, 0x80, 0xff] {
                let mut bytes = raw.to_vec();
                bytes[position] ^= flip;
                if let Ok(columns) = PageColumns::decode(&bytes, 2) {
                    lazily_caught += usize::from(columns.exists(&filter).is_err());
                }
            }
        }
        assert!(lazily_caught > 0);
    }

    proptest! {
        #[test]
        fn arbitrary_sidecar_bytes_never_panic(
            raw in prop::collection::vec(any::<u8>(), 0..256),
            traces in 0_usize..4,
        ) {
            let _ = PageColumns::decode(&raw, traces);
        }

        #[test]
        fn page_columns_never_rule_out_a_matching_trace(
            traces in (1_u8..6).prop_flat_map(|count| {
                (1..=count)
                    .map(crate::traceql::columns::testing::trace)
                    .collect::<Vec<_>>()
            }),
            source in crate::traceql::columns::testing::query(),
        ) {
            let query = crate::traceql::parse(&source).unwrap();
            let Some(filter) = crate::traceql::prefilter(&query) else {
                return Ok(());
            };
            let page = Page::decode(Page::from_traces(&traces, 1 << 20).unwrap().bytes()).unwrap();
            let columns = page.columns().unwrap();
            let exists = columns.exists(&filter).unwrap();
            for (index, entry) in page.directory().iter().enumerate() {
                let trace = page.get_trace(entry.trace_id).unwrap().unwrap();
                prop_assert_eq!(columns.trace_spans(index).len(), trace.spans().count());
                let satisfied = exists.iter().map(|leaf| leaf[index]).collect::<Vec<_>>();
                if !filter.matches(&satisfied) {
                    prop_assert!(
                        crate::traceql::execute(&trace, &query, 1_000).unwrap().is_none(),
                        "{} ruled out a matching trace", source
                    );
                }
            }
        }

        #[test]
        fn dedicated_columns_agree_with_the_interpreter(
            traces in (1_u8..6).prop_flat_map(|count| {
                (1..=count)
                    .map(crate::traceql::columns::testing::trace)
                    .collect::<Vec<_>>()
            }),
            leaf in crate::traceql::columns::testing::dedicated_leaf(),
            negate in any::<bool>(),
        ) {
            use crate::traceql::columns::{ColumnPredicate, DedicatedTest};

            let source = if negate {
                format!("{{ !({leaf}) }}")
            } else {
                format!("{{ {leaf} }}")
            };
            let query = crate::traceql::parse(&source).unwrap();
            let filter = crate::traceql::prefilter(&query);
            let exact = match &filter {
                Some(crate::traceql::TraceFilter::Exists(predicate)) => matches!(
                    predicate,
                    ColumnPredicate::Dedicated { test: DedicatedTest::String { .. }, .. }
                ) || matches!(
                    predicate,
                    ColumnPredicate::Not(inner) if matches!(
                        **inner,
                        ColumnPredicate::Dedicated { test: DedicatedTest::String { .. }, .. }
                    )
                ),
                _ => false,
            };
            let page = Page::decode(Page::from_traces(&traces, 1 << 20).unwrap().bytes()).unwrap();
            let columns = page.columns().unwrap();
            let exists = filter.as_ref().map(|filter| columns.exists(filter).unwrap());
            for (index, entry) in page.directory().iter().enumerate() {
                let trace = page.get_trace(entry.trace_id).unwrap().unwrap();
                let matched = crate::traceql::execute(&trace, &query, 1_000).unwrap().is_some();
                let satisfied = match (&filter, &exists) {
                    (Some(filter), Some(exists)) => filter.matches(
                        &exists.iter().map(|leaf| leaf[index]).collect::<Vec<_>>(),
                    ),
                    _ => true,
                };
                prop_assert!(satisfied || !matched, "{} ruled out a matching trace", source);
                if exact {
                    prop_assert_eq!(satisfied, matched, "{}", source);
                }
            }
        }

        #[test]
        fn column_summaries_agree_with_the_interpreter(
            traces in (1_u8..6).prop_flat_map(|count| {
                (1..=count)
                    .map(crate::traceql::columns::testing::trace)
                    .collect::<Vec<_>>()
            }),
            source in crate::traceql::columns::testing::query(),
        ) {
            let query = crate::traceql::parse(&source).unwrap();
            let Some(decider) = crate::traceql::columns::decider(&query) else {
                return Ok(());
            };
            let compiled = crate::traceql::CompiledQuery::new(query);
            let page = Page::decode(Page::from_traces(&traces, 1 << 20).unwrap().bytes()).unwrap();
            let columns = page.columns().unwrap();
            let indexes = (0..page.directory().len()).collect::<Vec<_>>();
            let decided = columns.decide(&decider).unwrap().unwrap();
            let summaries = columns.summaries(&indexes, &decider).unwrap().unwrap();
            for (index, entry) in page.directory().iter().enumerate() {
                let trace = page.get_trace(entry.trace_id).unwrap().unwrap();
                let expected =
                    crate::traceql::summarize_compiled(&trace, &compiled, 1_000).unwrap();
                prop_assert_eq!(decided[index], expected.is_some(), "{}", source);
                let Some(summary) = &summaries[index] else {
                    continue;
                };
                match expected {
                    Some(expected) => {
                        prop_assert_eq!((entry.min_timestamp_ns, entry.max_timestamp_ns),
                            (expected.start_ns, expected.end_ns));
                        prop_assert_eq!(&summary.root_service_name, &expected.root_service_name);
                        prop_assert_eq!(&summary.root_span_name, &expected.root_span_name);
                        prop_assert_eq!(summary.matched, expected.matched_spans, "{}", source);
                    }
                    None => prop_assert_eq!(summary.matched, 0, "{}", source),
                }
            }
        }

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
