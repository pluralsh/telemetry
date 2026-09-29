// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub type StreamId = u32;
pub type StreamFingerprint = [u8; 16];
pub type SegmentId = i64;

/// A canonical stream label.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Sorted, unique labels identifying one log stream.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Labels(Vec<Label>);

impl Labels {
    pub fn new(mut labels: Vec<Label>) -> Result<Self> {
        labels.sort();
        if labels.iter().any(|label| label.name.is_empty()) {
            return Err(Error::Invalid("label names cannot be empty".to_owned()));
        }
        if labels.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(Error::Invalid("label names must be unique".to_owned()));
        }
        Ok(Self(labels))
    }

    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn fingerprint(&self) -> StreamFingerprint {
        let mut hasher = blake3::Hasher::new();
        for label in &self.0 {
            hasher.update(&(label.name.len() as u32).to_be_bytes());
            hasher.update(label.name.as_bytes());
            hasher.update(&(label.value.len() as u32).to_be_bytes());
            hasher.update(label.value.as_bytes());
        }
        hasher.finalize().as_bytes()[..16].try_into().unwrap()
    }
}

/// One structured metadata field attached to a log entry.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Field {
    pub name: String,
    pub value: String,
}

impl Field {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Canonically sorted, unique structured metadata for Loki and OTLP records.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Fields(Vec<Field>);

impl Fields {
    pub fn new(mut fields: Vec<Field>) -> Result<Self> {
        fields.sort();
        Self::validate(&fields)?;
        Ok(Self(fields))
    }

    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn from_canonical(fields: Vec<Field>) -> Result<Self> {
        Self::validate(&fields)?;
        if fields.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Corrupt(
                "structured metadata is not in canonical order".to_owned(),
            ));
        }
        Ok(Self(fields))
    }

    fn validate(fields: &[Field]) -> Result<()> {
        if fields.iter().any(|field| field.name.is_empty()) {
            return Err(Error::Invalid(
                "structured metadata names cannot be empty".to_owned(),
            ));
        }
        if fields.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(Error::Invalid(
                "structured metadata names must be unique".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A timestamped UTF-8 log line with per-entry structured metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: Fields,
}

impl LogEntry {
    pub fn new(timestamp_ns: i64, line: impl Into<String>) -> Self {
        Self {
            timestamp_ns,
            line: line.into(),
            structured_metadata: Fields::default(),
        }
    }

    pub fn with_structured_metadata(
        timestamp_ns: i64,
        line: impl Into<String>,
        structured_metadata: Fields,
    ) -> Self {
        Self {
            timestamp_ns,
            line: line.into(),
            structured_metadata,
        }
    }
}

/// One stream and its entries for ingestion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogBatch {
    pub labels: Labels,
    pub entries: Vec<LogEntry>,
}

impl LogBatch {
    pub fn new(labels: Labels, entries: Vec<LogEntry>) -> Self {
        Self { labels, entries }
    }
}

/// A decoded query result with stream identity attached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRow {
    /// Shared by every row decoded from the same stream.
    pub labels: Arc<Labels>,
    pub entry: LogEntry,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_metadata_is_canonicalized() {
        let fields = Fields::new(vec![
            Field::new("trace_id", "abc"),
            Field::new("severity", "warn"),
        ])
        .unwrap();
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["severity", "trace_id"]
        );
    }

    #[test]
    fn structured_metadata_rejects_malformed_names() {
        assert!(Fields::new(vec![Field::new("", "value")]).is_err());
        assert!(
            Fields::new(vec![
                Field::new("trace_id", "first"),
                Field::new("trace_id", "second"),
            ])
            .is_err()
        );
    }
}
