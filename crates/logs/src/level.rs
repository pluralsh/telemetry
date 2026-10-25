// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Loki's `detected_level`: a log level found for every entry at ingest and
//! stored as structured metadata, so level filters and per-level counts need
//! no line parsing, and summaries count rows per level.

use crate::model::{Field, Fields, Labels, LogBatch};

/// The structured metadata field ingest sets.
pub const DETECTED_LEVEL: &str = "detected_level";

/// Every value [`detect`] yields, in summary code order (code `i + 1`; code
/// 0 is a row without the field).
pub const LEVELS: [&str; 8] = [
    "trace", "debug", "info", "warn", "error", "critical", "fatal", "unknown",
];

const UNKNOWN: &str = "unknown";

/// Label and field names a level is read from, as Loki reads them.
const LEVEL_NAMES: [&str; 6] = [
    "level",
    "lvl",
    "severity",
    "severity_text",
    "loglevel",
    "log_level",
];

/// Lines longer than this are only searched for level keywords this far in.
const KEYWORD_SCAN_BYTES: usize = 512;

/// The summary code of `value`: its position in [`LEVELS`] plus one.
pub(crate) fn code(value: &str) -> Option<u8> {
    LEVELS
        .iter()
        .position(|level| *level == value)
        .map(|index| index as u8 + 1)
}

/// Adds [`DETECTED_LEVEL`] to every entry of `batches` that lacks it.
pub fn add_detected_levels(batches: &mut [LogBatch]) {
    for batch in batches {
        if batch
            .labels
            .iter()
            .any(|label| label.name == DETECTED_LEVEL)
        {
            continue;
        }
        let from_labels = from_pairs(
            batch
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str())),
        );
        for entry in &mut batch.entries {
            if entry
                .structured_metadata
                .iter()
                .any(|field| field.name == DETECTED_LEVEL)
            {
                continue;
            }
            let level = from_pairs(
                entry
                    .structured_metadata
                    .iter()
                    .map(|field| (field.name.as_str(), field.value.as_str())),
            )
            .or(from_labels)
            .unwrap_or_else(|| from_line(&entry.line));
            let mut fields = entry
                .structured_metadata
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            fields.push(Field::new(DETECTED_LEVEL, level));
            if let Ok(fields) = Fields::new(fields) {
                entry.structured_metadata = fields;
            }
        }
    }
}

/// The level of an entry with stream `labels`, `metadata` and `line`.
pub fn detect<'a>(
    labels: &Labels,
    metadata: impl Iterator<Item = (&'a str, &'a str)>,
    line: &str,
) -> &'static str {
    from_pairs(metadata)
        .or_else(|| from_pairs(labels.iter().map(|l| (l.name.as_str(), l.value.as_str()))))
        .unwrap_or_else(|| from_line(line))
}

fn from_pairs<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> Option<&'static str> {
    let mut number = None;
    for (name, value) in pairs {
        if LEVEL_NAMES
            .iter()
            .any(|level| name.eq_ignore_ascii_case(level))
        {
            return Some(normalize(value));
        }
        if name.eq_ignore_ascii_case("severity_number") {
            number = value.parse::<u8>().ok().map(otlp_severity);
        }
    }
    number
}

/// The level of `line`: a JSON or logfmt level field, a klog header, or
/// else the first level keyword in it.
fn from_line(line: &str) -> &'static str {
    let trimmed = line.trim_start();
    if trimmed.starts_with('{')
        && let Some(level) = json_level(trimmed)
    {
        return normalize(level);
    }
    if let Some(level) = logfmt_level(line) {
        return normalize(level);
    }
    if let Some(level) = klog_level(line) {
        return level;
    }
    keyword_level(line).unwrap_or(UNKNOWN)
}

/// The value of the first `"<level name>": "value"` pair in a JSON line.
fn json_level(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut from = 0;
    while let Some(offset) = memchr::memchr(b'"', &bytes[from..]) {
        let start = from + offset + 1;
        let end = start + memchr::memchr(b'"', &bytes[start..])?;
        from = end + 1;
        let key = &line[start..end];
        let rest = line[from..].trim_start();
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        if !LEVEL_NAMES
            .iter()
            .any(|name| key.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let rest = rest.trim_start();
        if let Some(value) = rest.strip_prefix('"') {
            return value.find('"').map(|end| &value[..end]);
        }
        let end = rest
            .find(|c: char| !c.is_ascii_alphanumeric())
            .unwrap_or(rest.len());
        return (end > 0).then(|| &rest[..end]);
    }
    None
}

/// The value of a `level=value` pair, where the key starts the line or
/// follows whitespace.
fn logfmt_level(line: &str) -> Option<&str> {
    for name in LEVEL_NAMES {
        let mut from = 0;
        while let Some(offset) = find_ignore_ascii_case(&line[from..], name) {
            let start = from + offset;
            from = start + name.len();
            let bounded = start == 0 || line.as_bytes()[start - 1].is_ascii_whitespace();
            let Some(rest) = line[from..].strip_prefix('=') else {
                continue;
            };
            if !bounded {
                continue;
            }
            let value = match rest.strip_prefix('"') {
                Some(quoted) => &quoted[..quoted.find('"').unwrap_or(quoted.len())],
                None => &rest[..rest.find(char::is_whitespace).unwrap_or(rest.len())],
            };
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn find_ignore_ascii_case(haystack: &str, needle: &str) -> Option<usize> {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    haystack
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

/// klog's `Lmmdd hh:mm:ss` header, as Kubernetes components log.
fn klog_level(line: &str) -> Option<&'static str> {
    let bytes = line.as_bytes();
    let header = bytes.get(..15)?;
    let digits = |range: std::ops::Range<usize>| header[range].iter().all(u8::is_ascii_digit);
    if !(digits(1..5) && header[5] == b' ' && digits(6..8) && header[8] == b':') {
        return None;
    }
    match header[0] {
        b'I' => Some("info"),
        b'W' => Some("warn"),
        b'E' => Some("error"),
        b'F' => Some("fatal"),
        _ => None,
    }
}

/// The first whole word of `line` that names a level. Words that also read
/// as prose (`trace`, `alert`, `notice`) are not keywords.
fn keyword_level(line: &str) -> Option<&'static str> {
    const KEYWORDS: [&str; 10] = [
        "debug", "info", "warn", "warning", "error", "err", "critical", "crit", "fatal", "panic",
    ];
    let mut end = line.len().min(KEYWORD_SCAN_BYTES);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end]
        .split(|c: char| !c.is_ascii_alphabetic())
        .find(|word| {
            KEYWORDS
                .iter()
                .any(|keyword| word.eq_ignore_ascii_case(keyword))
        })
        .map(normalize)
}

/// A level name as Loki normalizes it, or `unknown`.
fn normalize(value: &str) -> &'static str {
    const NAMES: [(&str, &str); 22] = [
        ("trace", "trace"),
        ("trc", "trace"),
        ("debug", "debug"),
        ("dbg", "debug"),
        ("info", "info"),
        ("inf", "info"),
        ("information", "info"),
        ("notice", "info"),
        ("warn", "warn"),
        ("warning", "warn"),
        ("wrn", "warn"),
        ("error", "error"),
        ("err", "error"),
        ("eror", "error"),
        ("critical", "critical"),
        ("crit", "critical"),
        ("alert", "critical"),
        ("emerg", "critical"),
        ("emergency", "critical"),
        ("fatal", "fatal"),
        ("panic", "fatal"),
        ("dpanic", "fatal"),
    ];
    let value = value.trim();
    if let Ok(number) = value.parse::<u8>() {
        return otlp_severity(number);
    }
    NAMES
        .iter()
        .find(|(name, _)| value.eq_ignore_ascii_case(name))
        .map_or(UNKNOWN, |(_, level)| level)
}

/// OTLP's `SeverityNumber` ranges.
fn otlp_severity(number: u8) -> &'static str {
    match number {
        1..=4 => "trace",
        5..=8 => "debug",
        9..=12 => "info",
        13..=16 => "warn",
        17..=20 => "error",
        21..=24 => "fatal",
        _ => UNKNOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, LogEntry};

    fn level(line: &str) -> &'static str {
        detect(&Labels::new(Vec::new()).unwrap(), std::iter::empty(), line)
    }

    #[test]
    fn reads_structured_lines() {
        assert_eq!(level(r#"{"ts":1,"level":"ERROR","msg":"boom"}"#), "error");
        assert_eq!(
            level(r#"{"msg":"info here","severity": "Warning"}"#),
            "warn"
        );
        assert_eq!(level("ts=1 level=debug msg=\"error ignored\""), "debug");
        assert_eq!(level("ts=1 lvl=\"crit\" msg=x"), "critical");
        assert_eq!(
            level("E1025 12:00:00.123456       1 reflector.go:1] failed to list"),
            "error"
        );
        assert_eq!(level("W1025 12:00:00.123456 1 x.go:1] slow"), "warn");
    }

    #[test]
    fn falls_back_to_the_first_level_word() {
        assert_eq!(
            level("2026-10-25 12:00:00 INFO request failed with error"),
            "info"
        );
        assert_eq!(level("panic: runtime error: index out of range"), "fatal");
        assert_eq!(level("[WARN] disk almost full"), "warn");
        assert_eq!(level("GET /healthz 200"), "unknown");
        assert_eq!(level("errors=0 retrying"), "unknown");
    }

    #[test]
    fn prefers_metadata_then_labels() {
        let labels = Labels::new(vec![Label::new("level", "warn")]).unwrap();
        assert_eq!(detect(&labels, std::iter::empty(), "ERROR x"), "warn");
        assert_eq!(
            detect(&labels, [("severity_number", "17")].into_iter(), "x"),
            "error"
        );
    }

    #[test]
    fn adds_the_field_unless_present() {
        let labels = Labels::new(vec![Label::new("app", "api")]).unwrap();
        let kept = Fields::new(vec![Field::new(DETECTED_LEVEL, "info")]).unwrap();
        let mut batches = vec![LogBatch::new(
            labels,
            vec![
                LogEntry::new(1, "level=error boom"),
                LogEntry::with_structured_metadata(2, "ERROR", kept.clone()),
            ],
        )];
        add_detected_levels(&mut batches);
        let entries = &batches[0].entries;
        assert_eq!(
            entries[0].structured_metadata,
            Fields::new(vec![Field::new(DETECTED_LEVEL, "error")]).unwrap()
        );
        assert_eq!(entries[1].structured_metadata, kept);
    }

    #[test]
    fn every_level_has_a_code() {
        for (index, level) in LEVELS.iter().enumerate() {
            assert_eq!(code(level), Some(index as u8 + 1));
        }
        assert_eq!(code("notice"), None);
    }
}
