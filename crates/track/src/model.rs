// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::{fmt, str::FromStr};

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, any_value},
    trace::v1::{ResourceSpans, Span},
};

use crate::{Error, Result};

pub type SegmentId = i64;

/// A validated, non-zero W3C/OTLP 16-byte trace identifier.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub fn new(bytes: [u8; 16]) -> Result<Self> {
        if bytes == [0; 16] {
            return Err(Error::Invalid("trace ID cannot be all zeroes".to_owned()));
        }
        Ok(Self(bytes))
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| Error::Invalid("trace ID must contain exactly 16 bytes".to_owned()))?;
        Self::new(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TraceId({self})")
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for TraceId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 32 {
            return Err(Error::Invalid(
                "hex trace ID must contain exactly 32 characters".to_owned(),
            ));
        }
        let mut bytes = [0_u8; 16];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| Error::Invalid("trace ID must be hexadecimal".to_owned()))?;
        }
        Self::new(bytes)
    }
}

/// One trace continuation. Resource/scope grouping and all typed OTLP fields
/// are retained exactly as received.
#[derive(Clone, Debug, PartialEq)]
pub struct Trace {
    pub trace_id: TraceId,
    pub resource_spans: Vec<ResourceSpans>,
}

impl Trace {
    pub fn new(trace_id: TraceId, resource_spans: Vec<ResourceSpans>) -> Result<Self> {
        let trace = Self {
            trace_id,
            resource_spans,
        };
        let mut spans = 0usize;
        for span in trace.spans() {
            spans += 1;
            let found = TraceId::from_slice(&span.trace_id)?;
            if found != trace_id {
                return Err(Error::Invalid(format!(
                    "span trace ID {found} does not match trace {trace_id}"
                )));
            }
            if span.end_time_unix_nano < span.start_time_unix_nano {
                return Err(Error::Invalid(
                    "span end time precedes start time".to_owned(),
                ));
            }
        }
        if spans == 0 {
            return Err(Error::Invalid(
                "trace must contain at least one span".to_owned(),
            ));
        }
        Ok(trace)
    }

    pub fn spans(&self) -> impl Iterator<Item = &Span> {
        self.resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
    }

    pub fn timestamp_range(&self) -> (u64, u64) {
        let mut spans = self.spans();
        let first = spans.next().expect("validated Trace contains a span");
        spans.fold(
            (first.start_time_unix_nano, first.end_time_unix_nano),
            |(min, max), span| {
                (
                    min.min(span.start_time_unix_nano),
                    max.max(span.end_time_unix_nano),
                )
            },
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TraceBatch {
    pub traces: Vec<Trace>,
}

impl TraceBatch {
    pub fn new(traces: Vec<Trace>) -> Self {
        Self { traces }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttributeScope {
    Resource,
    Span,
}

/// Scalar OTLP attribute values supported by Track's exact index.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

impl AttributeValue {
    pub(crate) fn from_otlp(value: &AnyValue) -> Option<Self> {
        match value.value.as_ref()? {
            any_value::Value::StringValue(value) => Some(Self::String(value.clone())),
            any_value::Value::BoolValue(value) => Some(Self::Bool(*value)),
            any_value::Value::IntValue(value) => Some(Self::Int(*value)),
            any_value::Value::DoubleValue(value) => Some(Self::Double(*value)),
            any_value::Value::ArrayValue(_)
            | any_value::Value::KvlistValue(_)
            | any_value::Value::BytesValue(_) => None,
        }
    }

    pub(crate) fn exact_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int(left), Self::Int(right)) => left == right,
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttributeMatcher {
    pub scope: AttributeScope,
    pub name: String,
    pub value: AttributeValue,
}

impl AttributeMatcher {
    pub fn new(
        scope: AttributeScope,
        name: impl Into<String>,
        value: AttributeValue,
    ) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::Invalid("attribute name cannot be empty".to_owned()));
        }
        Ok(Self { scope, name, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_trace_ids() {
        let id = TraceId::new([0xabu8; 16]).unwrap();
        assert_eq!(id.to_string(), "abababababababababababababababab");
        assert_eq!(id.to_string().parse::<TraceId>().unwrap(), id);
        assert!(TraceId::new([0; 16]).is_err());
        assert!(TraceId::from_slice(&[1; 15]).is_err());
        assert!("not-a-trace-id".parse::<TraceId>().is_err());
    }
}
