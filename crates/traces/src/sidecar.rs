// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! The column sidecar of a version 3 trace page: intrinsic, structural and
//! common attribute columns for every span in the page, decodable without
//! touching any trace payload.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::OnceLock;

use bytes::BytesMut;
use common::serde::varint::{var_u32, var_u64};
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};

use crate::traceql::columns::{
    self, ColumnSource, DEDICATED_COLUMNS, Decider, DedicatedTest, DedicatedType, MAX_KIND_CODE,
    MAX_STATUS_CODE, ServiceName, TraceFilter,
};
use crate::traceql::{AttributeScope, FieldExpr};
use crate::{Error, Result, Trace};

/// Attribute codes: the attribute is missing, holds a value of another type
/// than its column's, or from here on holds a value of the column's type
/// (for string columns, the dictionary id offset by this).
const CODE_MISSING: u32 = 0;
const CODE_OTHER: u32 = 1;
const CODE_FIRST_VALUE: u32 = 2;
const KIND_SHIFT: u8 = 2;
const STATUS_MASK: u8 = (1 << KIND_SHIFT) - 1;
/// Trace flag: two of the trace's spans share a span ID.
const REPEATS_SPAN_ID: u8 = 1;
/// Parent codes: the span has no parent ID, its parent is not among the
/// trace's spans on the page, or from here on the parent's index among them.
const PARENT_NONE: u32 = 0;
const PARENT_ELSEWHERE: u32 = 1;
const PARENT_FIRST_INDEX: u32 = 2;

/// Decoded sidecar columns, indexed by page span: the spans of each trace in
/// directory order, each trace's spans in interpreter order.
#[derive(Debug)]
pub(crate) struct PageColumns {
    /// Exclusive end span of each trace.
    trace_ends: Vec<usize>,
    trace_flags: Vec<u8>,
    /// Each span's start after its trace's earliest start on the page.
    start_offsets: Vec<u64>,
    parents: Vec<u32>,
    durations: Vec<i64>,
    statuses: Vec<u8>,
    kinds: Vec<u8>,
    names: Vec<String>,
    name_ids: Vec<u32>,
    services: Vec<String>,
    service_codes: Vec<u32>,
    /// The encoded attribute columns, each decoded the first time a filter
    /// needs it.
    dedicated_bytes: Box<[u8]>,
    /// Index-aligned with [`DEDICATED_COLUMNS`]: each column's bytes within
    /// `dedicated_bytes`, and the column once decoded.
    dedicated: Vec<(Range<usize>, OnceLock<Dedicated>)>,
}

/// A trace's search summary from page columns.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnSummary {
    pub(crate) root_service_name: Option<String>,
    pub(crate) root_span_name: Option<String>,
    pub(crate) matched: usize,
}

/// One dedicated attribute column of every span.
#[derive(Debug)]
enum Dedicated {
    /// No span on the page holds the attribute.
    Missing,
    String {
        values: Vec<String>,
        codes: Vec<u32>,
    },
    /// `values` holds each span's integer, or zero without one.
    Int { codes: Vec<u8>, values: Vec<i64> },
}

/// A dedicated column as it is built.
enum DedicatedBuilder {
    String { values: Dictionary, codes: Vec<u32> },
    Int { codes: Vec<u8>, values: Vec<i64> },
}

impl DedicatedBuilder {
    fn new(kind: DedicatedType) -> Self {
        match kind {
            DedicatedType::String => Self::String {
                values: Dictionary::default(),
                codes: Vec::new(),
            },
            DedicatedType::Int => Self::Int {
                codes: Vec::new(),
                values: Vec::new(),
            },
        }
    }

    /// The code of `attributes`' value of `name`, with the integer it holds
    /// in an integer column, for [`Self::push`] to add to one or more spans.
    fn resolve(&mut self, attributes: &[KeyValue], name: &str) -> Result<(u32, i64)> {
        let value = columns::scalar(attributes, name);
        Ok(match (self, value) {
            (_, None) => (CODE_MISSING, 0),
            (Self::String { values, .. }, Some(any_value::Value::StringValue(value))) => (
                CODE_FIRST_VALUE
                    .checked_add(values.id(value)?)
                    .ok_or_else(|| Error::Invalid("too many attribute values".to_owned()))?,
                0,
            ),
            (Self::Int { .. }, Some(any_value::Value::IntValue(value))) => {
                (CODE_FIRST_VALUE, *value)
            }
            (_, Some(_)) => (CODE_OTHER, 0),
        })
    }

    fn push(&mut self, (code, value): (u32, i64)) {
        match self {
            Self::String { codes, .. } => codes.push(code),
            Self::Int { codes, values } => {
                codes.push(code as u8);
                if code == CODE_FIRST_VALUE {
                    values.push(value);
                }
            }
        }
    }

    /// The column's byte length, then when any span holds the attribute,
    /// how many do and the column.
    fn encode(&self, out: &mut BytesMut) -> Result<()> {
        let present = match self {
            Self::String { codes, .. } => {
                codes.iter().filter(|&&code| code != CODE_MISSING).count()
            }
            Self::Int { codes, .. } => codes.iter().filter(|&&code| code != 0).count(),
        };
        if present == 0 {
            var_u32::serialize(0, out);
            return Ok(());
        }
        let mut column = BytesMut::new();
        var_u32::serialize(to_u32(present)?, &mut column);
        match self {
            Self::String { values, codes } => {
                values.encode(&mut column)?;
                for &code in codes {
                    var_u32::serialize(code, &mut column);
                }
            }
            Self::Int { codes, values } => {
                column.extend_from_slice(codes);
                for &value in values {
                    var_u64::serialize(((value << 1) ^ (value >> 63)) as u64, &mut column);
                }
            }
        }
        var_u32::serialize(to_u32(column.len())?, out);
        out.extend_from_slice(&column);
        Ok(())
    }
}

#[derive(Default)]
struct Dictionary {
    ids: HashMap<String, u32>,
    values: Vec<String>,
}

impl Dictionary {
    fn id(&mut self, value: &str) -> Result<u32> {
        if let Some(&id) = self.ids.get(value) {
            return Ok(id);
        }
        let id = to_u32(self.values.len())?;
        self.ids.insert(value.to_owned(), id);
        self.values.push(value.to_owned());
        Ok(id)
    }

    fn encode(&self, out: &mut BytesMut) -> Result<()> {
        var_u32::serialize(to_u32(self.values.len())?, out);
        for value in &self.values {
            var_u32::serialize(to_u32(value.len())?, out);
            out.extend_from_slice(value.as_bytes());
        }
        Ok(())
    }
}

/// The uncompressed sidecar of `traces`, given in directory order.
///
/// ```text
/// var_u32 traces │ var_u32 spans │ (var_u32 spans │ u8 flags) × traces
///                                   (flag 1: two spans share a span ID)
/// name dictionary │ service.name dictionary   (var_u32 count, then
///                                              var_u32 length + UTF-8 each)
/// var_u64 duration ns × spans
/// var_u64 start ns after the trace's earliest start × spans
/// var_u32 parent × spans  (0 no parent ID, 1 parent not among the trace's
///                          spans, 2 + its index among them; a repeated
///                          span ID names its last span)
/// u8 status | kind << 2 × spans
/// var_u32 name id × spans
/// var_u32 service code × spans  (0 missing, 1 non-string, 2 + dictionary id)
/// dedicated column × 8, in DEDICATED_COLUMNS order:
///   var_u32 byte length, zero when no span holds the attribute; otherwise
///   var_u32 spans holding the attribute, then
///   string: dictionary │ var_u32 code × spans  (as for service.name)
///   int:    u8 code × spans (0 missing, 1 non-integer, 2 integer)
///           │ var_u64 zigzag integer × spans with code 2
/// ```
pub(crate) fn encode<'t>(traces: impl IntoIterator<Item = &'t Trace>) -> Result<Vec<u8>> {
    let mut counts = Vec::new();
    let mut trace_flags = Vec::new();
    let mut durations = Vec::new();
    let mut start_offsets = Vec::new();
    let mut parents = Vec::new();
    let mut flags = Vec::new();
    let mut name_ids = Vec::new();
    let mut service_codes = Vec::new();
    let mut names = Dictionary::default();
    let mut services = Dictionary::default();
    let mut dedicated = DEDICATED_COLUMNS.map(|column| DedicatedBuilder::new(column.kind));
    for trace in traces {
        let first = durations.len();
        let mut structure = Vec::new();
        for resource_spans in &trace.resource_spans {
            let attributes = resource_spans
                .resource
                .as_ref()
                .map_or(&[][..], |resource| resource.attributes.as_slice());
            let service = match columns::service_name(attributes) {
                ServiceName::Missing => CODE_MISSING,
                ServiceName::Other => CODE_OTHER,
                ServiceName::String(name) => CODE_FIRST_VALUE
                    .checked_add(services.id(name)?)
                    .ok_or_else(|| Error::Invalid("too many service names".to_owned()))?,
            };
            let mut resource_codes = [(CODE_MISSING, 0); DEDICATED_COLUMNS.len()];
            for ((code, column), builder) in resource_codes
                .iter_mut()
                .zip(&DEDICATED_COLUMNS)
                .zip(&mut dedicated)
            {
                if column.scope == AttributeScope::Resource {
                    *code = builder.resolve(attributes, column.name)?;
                }
            }
            for span in resource_spans
                .scope_spans
                .iter()
                .flat_map(|scope| &scope.spans)
            {
                structure.push(span);
                durations.push(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                flags.push(columns::status_code(span) | columns::kind_code(span) << KIND_SHIFT);
                name_ids.push(names.id(&span.name)?);
                service_codes.push(service);
                for ((&resource_code, column), builder) in resource_codes
                    .iter()
                    .zip(&DEDICATED_COLUMNS)
                    .zip(&mut dedicated)
                {
                    let code = match column.scope {
                        AttributeScope::Resource => resource_code,
                        _ => builder.resolve(&span.attributes, column.name)?,
                    };
                    builder.push(code);
                }
            }
        }
        counts.push(to_u32(durations.len() - first)?);
        let by_id: HashMap<&[u8], usize> = structure
            .iter()
            .enumerate()
            .map(|(index, span)| (span.span_id.as_slice(), index))
            .collect();
        trace_flags.push(if by_id.len() < structure.len() {
            REPEATS_SPAN_ID
        } else {
            0
        });
        let earliest = structure
            .iter()
            .map(|span| span.start_time_unix_nano)
            .min()
            .unwrap_or(0);
        for span in &structure {
            start_offsets.push(span.start_time_unix_nano - earliest);
            parents.push(if span.parent_span_id.is_empty() {
                PARENT_NONE
            } else {
                match by_id.get(span.parent_span_id.as_slice()) {
                    Some(&index) => PARENT_FIRST_INDEX
                        .checked_add(to_u32(index)?)
                        .ok_or_else(|| Error::Invalid("too many spans".to_owned()))?,
                    None => PARENT_ELSEWHERE,
                }
            });
        }
    }
    let mut out = BytesMut::new();
    var_u32::serialize(to_u32(counts.len())?, &mut out);
    var_u32::serialize(to_u32(durations.len())?, &mut out);
    for (count, flag) in counts.into_iter().zip(trace_flags) {
        var_u32::serialize(count, &mut out);
        out.extend_from_slice(&[flag]);
    }
    names.encode(&mut out)?;
    services.encode(&mut out)?;
    for duration in durations {
        var_u64::serialize(duration, &mut out);
    }
    for offset in start_offsets {
        var_u64::serialize(offset, &mut out);
    }
    for parent in parents {
        var_u32::serialize(parent, &mut out);
    }
    out.extend_from_slice(&flags);
    for id in name_ids {
        var_u32::serialize(id, &mut out);
    }
    for code in service_codes {
        var_u32::serialize(code, &mut out);
    }
    for column in &dedicated {
        column.encode(&mut out)?;
    }
    Ok(out.to_vec())
}

impl PageColumns {
    /// Decodes and validates an uncompressed sidecar for a page holding
    /// `trace_count` traces.
    pub(crate) fn decode(raw: &[u8], trace_count: usize) -> Result<Self> {
        let mut buf = raw;
        if var_u32::deserialize(&mut buf)? as usize != trace_count {
            return Err(corrupt("trace count disagrees with the page directory"));
        }
        let spans = var_u32::deserialize(&mut buf)? as usize;
        // Every trace and span takes at least a byte, which bounds
        // allocations by the input.
        if trace_count > buf.len() || spans > buf.len() {
            return Err(corrupt("counts exceed the sidecar length"));
        }
        let mut trace_ends = Vec::with_capacity(trace_count);
        let mut trace_flags = Vec::with_capacity(trace_count);
        let mut end = 0_usize;
        for _ in 0..trace_count {
            let count = var_u32::deserialize(&mut buf)? as usize;
            if count == 0 {
                return Err(corrupt("a trace has no spans"));
            }
            end = end
                .checked_add(count)
                .ok_or_else(|| corrupt("span count overflow"))?;
            trace_ends.push(end);
            let (&flag, rest) = buf
                .split_first()
                .ok_or_else(|| corrupt("truncated trace flags"))?;
            if flag & !REPEATS_SPAN_ID != 0 {
                return Err(corrupt("invalid trace flags"));
            }
            trace_flags.push(flag);
            buf = rest;
        }
        if end != spans {
            return Err(corrupt("trace span counts disagree with the span count"));
        }
        let names = decode_dictionary(&mut buf)?;
        let services = decode_dictionary(&mut buf)?;
        let mut durations = Vec::with_capacity(spans);
        for _ in 0..spans {
            durations.push(i64::try_from(var_u64::deserialize(&mut buf)?).unwrap_or(i64::MAX));
        }
        let mut start_offsets = Vec::with_capacity(spans);
        for _ in 0..spans {
            start_offsets.push(var_u64::deserialize(&mut buf)?);
        }
        let mut parents = Vec::with_capacity(spans);
        let mut start = 0;
        for &end in &trace_ends {
            let bound = end - start + PARENT_FIRST_INDEX as usize;
            parents.extend(decode_ids(&mut buf, end - start, bound)?);
            if !start_offsets[start..end].contains(&0) {
                return Err(corrupt("a trace has no span at its earliest start"));
            }
            start = end;
        }
        if buf.len() < spans {
            return Err(corrupt("truncated flags column"));
        }
        let (flags, rest) = buf.split_at(spans);
        buf = rest;
        let mut statuses = Vec::with_capacity(spans);
        let mut kinds = Vec::with_capacity(spans);
        for &flag in flags {
            let (status, kind) = (flag & STATUS_MASK, flag >> KIND_SHIFT);
            if status > MAX_STATUS_CODE || kind > MAX_KIND_CODE {
                return Err(corrupt("invalid span status or kind"));
            }
            statuses.push(status);
            kinds.push(kind);
        }
        let name_ids = decode_ids(&mut buf, spans, names.len())?;
        let service_codes =
            decode_ids(&mut buf, spans, services.len() + CODE_FIRST_VALUE as usize)?;
        let dedicated_bytes = Box::<[u8]>::from(buf);
        let mut dedicated = Vec::with_capacity(DEDICATED_COLUMNS.len());
        for _ in DEDICATED_COLUMNS {
            let len = var_u32::deserialize(&mut buf)? as usize;
            if len > buf.len() {
                return Err(corrupt("truncated attribute column"));
            }
            let start = dedicated_bytes.len() - buf.len();
            dedicated.push((start..start + len, OnceLock::new()));
            buf = &buf[len..];
        }
        if !buf.is_empty() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(Self {
            trace_ends,
            trace_flags,
            start_offsets,
            parents,
            durations,
            statuses,
            kinds,
            names,
            name_ids,
            services,
            service_codes,
            dedicated_bytes,
            dedicated,
        })
    }

    /// The attribute column at `column` of [`DEDICATED_COLUMNS`], decoded
    /// and validated on first use.
    fn dedicated_column(&self, column: usize) -> Result<&Dedicated> {
        let (range, decoded) = &self.dedicated[column];
        if let Some(decoded) = decoded.get() {
            return Ok(decoded);
        }
        let found = decode_dedicated(
            &self.dedicated_bytes[range.clone()],
            self.durations.len(),
            DEDICATED_COLUMNS[column].kind,
        )?;
        Ok(decoded.get_or_init(|| found))
    }

    pub(crate) fn trace_spans(&self, trace: usize) -> Range<usize> {
        let start = trace
            .checked_sub(1)
            .map_or(0, |previous| self.trace_ends[previous]);
        start..self.trace_ends[trace]
    }

    /// For each leaf of `filter`, in [`TraceFilter::leaves`] order, whether
    /// each trace has a span satisfying it. A leaf the columns cannot
    /// evaluate is taken to be satisfied. Fails when an attribute column the
    /// filter reads is corrupt.
    pub(crate) fn exists(&self, filter: &TraceFilter) -> Result<Vec<Vec<bool>>> {
        let leaves = filter.leaves();
        let mut wanted = Vec::new();
        for leaf in &leaves {
            leaf.dedicated_columns(&mut wanted);
        }
        for column in wanted {
            self.dedicated_column(column)?;
        }
        Ok(leaves
            .into_iter()
            .map(|leaf| match columns::evaluate(leaf, self) {
                Some(mask) => self.any_per_trace(&mask),
                None => vec![true; self.trace_ends.len()],
            })
            .collect())
    }

    /// Whether each trace matches the query `decider` decides; `None` when
    /// the columns cannot evaluate it.
    pub(crate) fn decide(&self, decider: &Decider) -> Result<Option<Vec<bool>>> {
        let Some(predicate) = decider else {
            return Ok(Some(vec![true; self.trace_ends.len()]));
        };
        let mut wanted = Vec::new();
        predicate.dedicated_columns(&mut wanted);
        for column in wanted {
            self.dedicated_column(column)?;
        }
        Ok(columns::evaluate(predicate, self).map(|mask| self.any_per_trace(&mask)))
    }

    /// The search summaries of `traces`, each wholly on this page, as the
    /// interpreter gives them for a query matching exactly the spans
    /// `decider` accepts. A trace's summary is `None` when the columns cannot
    /// tell its root apart: two of its spans share a span ID, or the earliest
    /// root candidates start together and only span IDs would order them.
    pub(crate) fn summaries(
        &self,
        traces: &[usize],
        decider: &Decider,
    ) -> Result<Option<Vec<Option<ColumnSummary>>>> {
        let mask = match decider {
            None => None,
            Some(predicate) => {
                let mut wanted = Vec::new();
                predicate.dedicated_columns(&mut wanted);
                for column in wanted {
                    self.dedicated_column(column)?;
                }
                match columns::evaluate(predicate, self) {
                    Some(mask) => Some(mask),
                    None => return Ok(None),
                }
            }
        };
        Ok(Some(
            traces
                .iter()
                .map(|&trace| {
                    let spans = self.trace_spans(trace);
                    if self.trace_flags[trace] & REPEATS_SPAN_ID != 0 {
                        return None;
                    }
                    let mut root = None::<(u64, usize, bool)>;
                    for span in spans.clone() {
                        if self.parents[span] >= PARENT_FIRST_INDEX {
                            continue;
                        }
                        let start = self.start_offsets[span];
                        root = match root {
                            Some((earliest, index, _)) if earliest < start => {
                                Some((earliest, index, false))
                            }
                            Some((earliest, index, _)) if earliest == start => {
                                Some((earliest, index, true))
                            }
                            _ => Some((start, span, false)),
                        };
                    }
                    if root.is_some_and(|(_, _, tied)| tied) {
                        return None;
                    }
                    let root = root.map(|(_, span, _)| span);
                    let matched = match &mask {
                        Some(mask) => mask[spans].iter().filter(|&&set| set).count(),
                        None => spans.len(),
                    };
                    Some(ColumnSummary {
                        root_service_name: root.and_then(|span| {
                            let code = self.service_codes[span].checked_sub(CODE_FIRST_VALUE)?;
                            Some(self.services[code as usize].clone())
                        }),
                        root_span_name: root
                            .map(|span| self.names[self.name_ids[span] as usize].clone()),
                        matched,
                    })
                })
                .collect(),
        ))
    }

    fn any_per_trace(&self, mask: &[bool]) -> Vec<bool> {
        let (mut rest, mut start) = (mask, 0);
        self.trace_ends
            .iter()
            .map(|&end| {
                let (spans, tail) = rest.split_at(end - start);
                (rest, start) = (tail, end);
                columns::any_set(spans)
            })
            .collect()
    }
}

impl ColumnSource for PageColumns {
    fn span_count(&self) -> usize {
        self.durations.len()
    }

    fn durations(&self) -> &[i64] {
        &self.durations
    }

    fn statuses(&self) -> &[u8] {
        &self.statuses
    }

    fn kinds(&self) -> &[u8] {
        &self.kinds
    }

    fn names_equal(&self, value: &str, out: &mut [bool]) {
        match self.names.iter().position(|name| name == value) {
            Some(id) => {
                let id = id as u32;
                for (out, &name) in out.iter_mut().zip(&self.name_ids) {
                    *out = name == id;
                }
            }
            None => out.fill(false),
        }
    }

    fn service_names(&self, equal: bool, value: &str, out: &mut [bool]) -> Option<()> {
        let wanted = self
            .services
            .iter()
            .position(|service| service == value)
            .map(|id| id as u32 + CODE_FIRST_VALUE);
        let pairs = out.iter_mut().zip(&self.service_codes);
        match (equal, wanted) {
            (true, Some(wanted)) => pairs.for_each(|(out, &code)| *out = code == wanted),
            (true, None) => out.fill(false),
            // `!=` is false for a missing attribute.
            (false, Some(wanted)) => {
                pairs.for_each(|(out, &code)| *out = code != CODE_MISSING && code != wanted);
            }
            (false, None) => pairs.for_each(|(out, &code)| *out = code != CODE_MISSING),
        }
        Some(())
    }

    fn dedicated(&self, column: usize, test: &DedicatedTest, out: &mut [bool]) -> Option<()> {
        match (self.dedicated.get(column)?.1.get()?, test) {
            (Dedicated::Missing, _) => out.fill(false),
            (Dedicated::String { values, codes }, DedicatedTest::String { op, literal }) => {
                let passes = columns::string_codes(*op, literal, values);
                for (out, &code) in out.iter_mut().zip(codes) {
                    *out = passes[code as usize];
                }
            }
            (Dedicated::Int { codes, values }, &DedicatedTest::Int { low, width, inside }) => {
                for ((out, &code), &value) in out.iter_mut().zip(codes).zip(values) {
                    let integer = (value.wrapping_sub(low) as u64 <= width) == inside;
                    *out = match u32::from(code) {
                        CODE_FIRST_VALUE => integer,
                        code => code == CODE_OTHER,
                    };
                }
            }
            _ => return None,
        }
        Some(())
    }

    fn constant(&self, _expression: &FieldExpr, _out: &mut [bool]) -> Option<()> {
        None
    }
}

fn decode_dictionary(buf: &mut &[u8]) -> Result<Vec<String>> {
    let count = var_u32::deserialize(buf)? as usize;
    if count > buf.len() {
        return Err(corrupt("dictionary count exceeds the sidecar length"));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = var_u32::deserialize(buf)? as usize;
        if len > buf.len() {
            return Err(corrupt("truncated dictionary entry"));
        }
        let (value, rest) = buf.split_at(len);
        *buf = rest;
        values.push(
            String::from_utf8(value.to_vec())
                .map_err(|_| corrupt("dictionary entry is not UTF-8"))?,
        );
    }
    Ok(values)
}

fn decode_dedicated(column: &[u8], spans: usize, kind: DedicatedType) -> Result<Dedicated> {
    if column.is_empty() {
        return Ok(Dedicated::Missing);
    }
    let mut column = column;
    let buf = &mut column;
    let present = var_u32::deserialize(buf)? as usize;
    if present == 0 || present > spans {
        return Err(corrupt("attribute presence count out of range"));
    }
    let (decoded, found) = match kind {
        DedicatedType::String => {
            let values = decode_dictionary(buf)?;
            let codes = decode_ids(buf, spans, values.len() + CODE_FIRST_VALUE as usize)?;
            let found = codes.iter().filter(|&&code| code != CODE_MISSING).count();
            (Dedicated::String { values, codes }, found)
        }
        DedicatedType::Int => {
            if buf.len() < spans {
                return Err(corrupt("truncated integer attribute codes"));
            }
            let (codes, rest) = buf.split_at(spans);
            *buf = rest;
            let mut values = Vec::with_capacity(spans);
            for &code in codes {
                values.push(match u32::from(code) {
                    CODE_MISSING | CODE_OTHER => 0,
                    CODE_FIRST_VALUE => {
                        let zigzag = var_u64::deserialize(buf)?;
                        (zigzag >> 1) as i64 ^ -((zigzag & 1) as i64)
                    }
                    _ => return Err(corrupt("invalid integer attribute code")),
                });
            }
            let found = codes.iter().filter(|&&code| code != 0).count();
            (
                Dedicated::Int {
                    codes: codes.to_vec(),
                    values,
                },
                found,
            )
        }
    };
    if found != present {
        return Err(corrupt("attribute presence count disagrees with its codes"));
    }
    if !buf.is_empty() {
        return Err(corrupt("trailing attribute column bytes"));
    }
    Ok(decoded)
}

fn decode_ids(buf: &mut &[u8], spans: usize, bound: usize) -> Result<Vec<u32>> {
    let mut ids = Vec::with_capacity(spans);
    for _ in 0..spans {
        let id = var_u32::deserialize(buf)?;
        if id as usize >= bound {
            return Err(corrupt("dictionary id out of range"));
        }
        ids.push(id);
    }
    Ok(ids)
}

fn corrupt(message: &str) -> Error {
    Error::Corrupt(format!("trace page column sidecar: {message}"))
}

fn to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::Invalid("page column sidecar exceeds u32".to_owned()))
}
