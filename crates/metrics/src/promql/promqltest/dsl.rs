use crate::histogram::{Bucket, CounterResetHint, FloatHistogram};
use crate::model::{Label, Labels, RangeSample, STALE_NAN};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Command Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct LoadCmd {
    pub interval: Duration,
    pub series: Vec<SeriesLoad>,
}

#[derive(Debug, Clone)]
pub struct EvalInstantCmd {
    pub time: SystemTime,
    pub query: String,
    pub expected: Vec<RangeSample>,
    pub expect_ordered: bool,
    /// `expect fail [msg:<text>]`: the query must error, and the error must
    /// contain `<text>` when given.
    pub expect_fail: Option<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct ClearCmd;

#[derive(Debug, Clone)]
pub struct IgnoreCmd;

#[derive(Debug, Clone)]
pub struct ResumeCmd;

#[derive(Debug, Clone)]
pub enum Command {
    Load(LoadCmd),
    EvalInstant(EvalInstantCmd),
    Clear(ClearCmd),
    Ignore(IgnoreCmd),
    Resume(ResumeCmd),
}

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone)]
pub struct SeriesLoad {
    pub labels: HashMap<String, String>, // includes __name__
    pub values: Vec<(i64, f64)>,         // (step_index, value)
    pub histograms: Vec<(i64, FloatHistogram)>,
}

// ============================================================================
// Parser
// ============================================================================

struct Parser;

impl Parser {
    /// Parse entire test file into commands
    pub fn parse_file(input: &str) -> Result<Vec<Command>, String> {
        let mut commands = Vec::new();
        let lines: Vec<&str> = input.lines().collect();
        let mut i = 0;
        let mut ignoring = false;

        while i < lines.len() {
            let line = lines[i].trim();
            i += 1;

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Check for ignore/resume first
            if line == "ignore" {
                commands.push(Command::Ignore(IgnoreCmd));
                ignoring = true;
                continue;
            } else if line == "resume" {
                commands.push(Command::Resume(ResumeCmd));
                ignoring = false;
                continue;
            } else if line == "clear" {
                commands.push(Command::Clear(ClearCmd));
                continue;
            }

            // Skip parsing other commands when ignoring
            if ignoring {
                continue;
            }

            // Try each parser dispatcher in order
            if let Some(cmd) = Self::try_parse_load(line, &lines, &mut i)? {
                commands.push(cmd);
            } else if let Some(cmd) = Self::try_parse_eval_instant(line, &lines, &mut i)? {
                commands.push(cmd);
            } else {
                return Err(format!("Unknown directive at line {}: {}", i, line));
            }
        }

        Ok(commands)
    }

    /// Try to parse "load" command (with indented series lines)
    fn try_parse_load(
        line: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<Option<Command>, String> {
        let Some(rest) = line.strip_prefix("load ") else {
            return Ok(None);
        };

        let interval = parse_duration(rest)?;
        let mut series = Vec::new();

        // Collect indented series lines
        while *i < lines.len() {
            let next_line = lines[*i];
            if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
                break;
            }

            let trimmed = next_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                *i += 1;
                continue;
            }

            series.push(parse_series(trimmed)?);
            *i += 1;
        }

        Ok(Some(Command::Load(LoadCmd { interval, series })))
    }

    /// Try to parse "eval instant at" command (with indented expected results)
    fn try_parse_eval_instant(
        line: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<Option<Command>, String> {
        let Some(rest) = line.strip_prefix("eval instant at ") else {
            return Ok(None);
        };

        // Parse time and query from same line or next line
        let (time_str, query) = Self::parse_time_and_query(rest, lines, i)?;
        let time = parse_time(&time_str)?;

        let mut expected = Vec::new();
        let mut expect_ordered = false;
        let mut expect_fail = None;

        // Collect indented expected result lines
        while *i < lines.len() {
            let next_line = lines[*i];
            if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
                break;
            }

            let trimmed = next_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                *i += 1;
                continue;
            }

            // Only ordering and failure are implemented; other directives remain no-ops.
            if trimmed.starts_with("expect ") {
                if trimmed == "expect ordered" {
                    expect_ordered = true;
                } else if let Some(rest) = trimmed.strip_prefix("expect fail") {
                    let msg = rest
                        .trim()
                        .strip_prefix("msg:")
                        .map(|m| m.trim().to_string());
                    expect_fail = Some(msg);
                }
                *i += 1;
                continue;
            }

            expected.push(parse_expected(trimmed)?);
            *i += 1;
        }

        Ok(Some(Command::EvalInstant(EvalInstantCmd {
            time,
            query,
            expected,
            expect_ordered,
            expect_fail,
        })))
    }

    /// Parse time and query from "eval instant at" line
    /// Format: "eval instant at <time> <query>"
    /// Query can span to next line if on same line it's empty
    fn parse_time_and_query(
        rest: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<(String, String), String> {
        // Split on any whitespace (space, tab, etc.)
        // Note: This means "eval instant at 10s\t\tmetric" is valid, which could be
        // confusing if test files have inconsistent whitespace formatting
        let parts: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
        let time_str = parts[0];

        let query = if parts.len() > 1 && !parts[1].trim().is_empty() {
            parts[1].trim().to_string()
        } else {
            // Query on next line
            if *i < lines.len() {
                let query_line = lines[*i];
                *i += 1;
                query_line.trim().to_string()
            } else {
                return Err("Missing query after 'eval instant at'".to_string());
            }
        };

        Ok((time_str.to_string(), query))
    }
}

// ============================================================================
// Parsing Helpers (Series, Metrics, Values, Durations)
// ============================================================================

fn parse_series(line: &str) -> Result<SeriesLoad, String> {
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in series: {}", line));
        }
    }

    let value_parts: String = chars.collect::<String>().trim().to_string();
    if value_parts.is_empty() {
        return Err(format!("Series missing values: {}", line));
    }

    let (metric, labels) = parse_metric(metric_part.trim())?;
    let (values, histograms) = parse_multiple_value_exprs(&value_parts)?;

    let mut all_labels = labels;
    if !metric.is_empty() {
        all_labels.insert("__name__".to_string(), metric);
    }

    Ok(SeriesLoad {
        labels: all_labels,
        values,
        histograms,
    })
}

fn parse_metric(s: &str) -> Result<(String, HashMap<String, String>), String> {
    if let Some((m, rest)) = s.split_once('{') {
        let rest = rest
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in metric: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((m.to_string(), labels))
    } else if s.starts_with('{') {
        // Label-only (no metric name, used in expected results)
        let rest = s
            .strip_prefix('{')
            .unwrap()
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in labels: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((String::new(), labels))
    } else {
        // Metric name only (no labels)
        Ok((s.to_string(), HashMap::new()))
    }
}

fn parse_labels(labels_str: &str, context: &str) -> Result<HashMap<String, String>, String> {
    let mut labels = HashMap::new();
    for kv in labels_str.split(',') {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("Invalid label '{}' (missing =) in: '{}'", kv, context))?;
        labels.insert(k.to_string(), v.trim_matches('"').to_string());
    }
    Ok(labels)
}

type ParsedValues = (Vec<(i64, f64)>, Vec<(i64, FloatHistogram)>);

fn parse_multiple_value_exprs(s: &str) -> Result<ParsedValues, String> {
    let mut floats = Vec::new();
    let mut histograms = Vec::new();
    let mut base_step = 0i64;

    // Multiple value expressions are sequential blocks: "0+10x3 100+20x2"
    // produces steps [0..=6] (x3 => 4 samples, x2 => 3 samples), and `_`
    // (or `_xN`) occupies steps without producing samples.
    for token in value_tokens(s)? {
        base_step += parse_value_token(&token, base_step, &mut floats, &mut histograms)?;
    }

    Ok((floats, histograms))
}

/// Split a value list on whitespace, keeping `{{ … }}` histogram
/// descriptors (which contain spaces) inside a single token.
fn value_tokens(s: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' && chars.peek() == Some(&'{') {
            chars.next();
            depth += 1;
            current.push_str("{{");
        } else if c == '}' && depth > 0 && chars.peek() == Some(&'}') {
            chars.next();
            depth -= 1;
            current.push_str("}}");
        } else if c.is_whitespace() && depth == 0 {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(c);
        }
    }
    if depth != 0 {
        return Err(format!("Unbalanced {{{{ }}}} in values: {s}"));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

/// Parse one value token starting at `base_step`, returning the number of
/// steps it occupies. Expansions are inclusive, as in Prometheus:
/// `0+10x5` => 6 samples `[0, 10, …, 50]`, and `{{a}}+{{b}}x2` =>
/// `a, a+b, a+2b`. `_xN` skips N steps.
fn parse_value_token(
    token: &str,
    base_step: i64,
    floats: &mut Vec<(i64, f64)>,
    histograms: &mut Vec<(i64, FloatHistogram)>,
) -> Result<i64, String> {
    if token == "_" {
        return Ok(1);
    }
    if let Some(count) = token.strip_prefix("_x") {
        return count
            .parse::<i64>()
            .map_err(|_| format!("Invalid count: {count}"));
    }
    if token == "stale" {
        floats.push((base_step, f64::from_bits(STALE_NAN)));
        return Ok(1);
    }
    if token.starts_with("{{") {
        return parse_histogram_token(token, base_step, histograms);
    }

    if let Some((lhs, count_str)) = token.split_once('x') {
        let (start_str, step) = match expansion_split(lhs) {
            Some((start_str, sign, step_str)) => {
                let step: f64 = step_str
                    .parse()
                    .map_err(|_| format!("Invalid step value: {}", step_str))?;
                (start_str, sign * step)
            }
            None => (lhs, 0.0),
        };
        let start: f64 = start_str
            .parse()
            .map_err(|_| format!("Invalid start value: {}", start_str))?;
        let count: i64 = count_str
            .parse()
            .map_err(|_| format!("Invalid count: {}", count_str))?;
        for i in 0..=count {
            floats.push((base_step + i, start + step * i as f64));
        }
        return Ok(count + 1);
    }

    let value = token
        .parse::<f64>()
        .map_err(|_| format!("Invalid value '{}'", token))?;
    floats.push((base_step, value));
    Ok(1)
}

/// `{{h}}`, `{{h}}xN`, or `{{a}}±{{b}}xN`.
fn parse_histogram_token(
    token: &str,
    base_step: i64,
    histograms: &mut Vec<(i64, FloatHistogram)>,
) -> Result<i64, String> {
    let first_end = token
        .find("}}")
        .ok_or_else(|| format!("Unterminated histogram: {token}"))?;
    let start = parse_histogram_desc(&token[2..first_end])?;
    let rest = &token[first_end + 2..];
    if rest.is_empty() {
        histograms.push((base_step, start));
        return Ok(1);
    }
    if let Some(count) = rest.strip_prefix('x') {
        let count: i64 = count
            .parse()
            .map_err(|_| format!("Invalid count in: {token}"))?;
        for i in 0..=count {
            histograms.push((base_step + i, start.clone()));
        }
        return Ok(count + 1);
    }
    let negate = match rest.as_bytes().first() {
        Some(b'+') => false,
        Some(b'-') => true,
        _ => return Err(format!("Invalid histogram expansion: {token}")),
    };
    let rest = &rest[1..];
    let inner = rest
        .strip_prefix("{{")
        .and_then(|r| r.split_once("}}x"))
        .ok_or_else(|| format!("Invalid histogram expansion: {token}"))?;
    let increment = parse_histogram_desc(inner.0)?;
    let count: i64 = inner
        .1
        .parse()
        .map_err(|_| format!("Invalid count in: {token}"))?;
    let mut current = start;
    for i in 0..=count {
        histograms.push((base_step + i, current.clone()));
        let combined = if negate {
            current.sub(&increment)
        } else {
            current.add(&increment)
        };
        combined.map_err(|e| format!("Cannot expand {token}: {e:?}"))?;
    }
    Ok(count + 1)
}

/// Parse the inside of `{{ … }}`: space-separated `key:value` pairs where
/// list values are bracketed (`buckets:[1 2 1]`). Bucket lists start at
/// index `offset` / `n_offset`.
fn parse_histogram_desc(desc: &str) -> Result<FloatHistogram, String> {
    let mut h = FloatHistogram::default();
    let (mut buckets, mut n_buckets) = (Vec::new(), Vec::new());
    let (mut offset, mut n_offset) = (0i32, 0i32);
    let mut rest = desc.trim();
    while !rest.is_empty() {
        let (key, after) = rest
            .split_once(':')
            .ok_or_else(|| format!("Invalid histogram field in: {desc}"))?;
        let key = key.trim();
        let after = after.trim_start();
        let (value, remaining) = if let Some(list) = after.strip_prefix('[') {
            let (inner, remaining) = list
                .split_once(']')
                .ok_or_else(|| format!("Unterminated list in: {desc}"))?;
            (inner, remaining)
        } else {
            after.split_once(char::is_whitespace).unwrap_or((after, ""))
        };
        rest = remaining.trim_start();
        let float = |v: &str| v.parse::<f64>().map_err(|_| format!("Invalid {key}: {v}"));
        let list = |v: &str| {
            v.split_whitespace()
                .map(float)
                .collect::<Result<Vec<_>, _>>()
        };
        let int = |v: &str| v.parse::<i32>().map_err(|_| format!("Invalid {key}: {v}"));
        match key {
            "schema" => h.schema = int(value)?,
            "sum" => h.sum = float(value)?,
            "count" => h.count = float(value)?,
            "z_bucket" => h.zero_count = float(value)?,
            "z_bucket_w" => h.zero_threshold = float(value)?,
            "buckets" => buckets = list(value)?,
            "n_buckets" => n_buckets = list(value)?,
            "offset" => offset = int(value)?,
            "n_offset" => n_offset = int(value)?,
            "custom_values" => h.custom_values = Arc::from(list(value)?),
            "counter_reset_hint" => {
                h.counter_reset_hint = match value {
                    "unknown" => CounterResetHint::Unknown,
                    "reset" => CounterResetHint::CounterReset,
                    "not_reset" => CounterResetHint::NotCounterReset,
                    "gauge" => CounterResetHint::Gauge,
                    other => return Err(format!("Invalid counter_reset_hint: {other}")),
                }
            }
            other => return Err(format!("Unknown histogram field '{other}' in: {desc}")),
        }
    }
    let to_buckets = |counts: Vec<f64>, offset: i32| -> Vec<Bucket> {
        counts
            .into_iter()
            .enumerate()
            .map(|(i, count)| Bucket {
                index: offset + i as i32,
                count,
            })
            .collect()
    };
    h.positive = to_buckets(buckets, offset);
    h.negative = to_buckets(n_buckets, n_offset);
    Ok(h)
}

/// Split `start±step` at the operator, skipping a leading sign and the
/// sign of an exponent (`1e-3`).
fn expansion_split(lhs: &str) -> Option<(&str, f64, &str)> {
    let bytes = lhs.as_bytes();
    (1..bytes.len()).find_map(|i| {
        let sign = match bytes[i] {
            b'+' => 1.0,
            b'-' => -1.0,
            _ => return None,
        };
        if matches!(bytes[i - 1], b'e' | b'E') {
            return None;
        }
        Some((&lhs[..i], sign, &lhs[i + 1..]))
    })
}

fn parse_expected(line: &str) -> Result<RangeSample, String> {
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name if present (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read full label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in expected: {}", line));
        }
    }

    let value_str: String = chars.collect::<String>().trim().to_string();
    if value_str.is_empty() {
        return Err(format!("Missing value in expected: {}", line));
    }

    let (metric_name, label_map) = parse_metric(metric_part.trim())?;
    let (samples, histograms) = if let Some(desc) = value_str
        .strip_prefix("{{")
        .and_then(|v| v.strip_suffix("}}"))
    {
        (Vec::new(), vec![(0, Arc::new(parse_histogram_desc(desc)?))])
    } else {
        let value = value_str
            .parse::<f64>()
            .map_err(|_| format!("Invalid value '{}' in expected: {}", value_str, line))?;
        (vec![(0, value)], Vec::new())
    };

    let mut labels: Vec<Label> = label_map
        .into_iter()
        .map(|(k, v)| Label::new(k, v))
        .collect();
    if !metric_name.is_empty() {
        labels.push(Label::metric_name(metric_name));
    }
    labels.sort();
    Ok(RangeSample {
        labels: Labels::new(labels),
        samples,
        histograms,
    })
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();

    // Try using promql_parser's duration parser which handles compound durations like "5m59s"
    if let Ok(duration) = promql_parser::util::parse_duration(s) {
        return Ok(duration);
    }

    // Fall back to suffix-based parsing (strict: requires units)
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(h) = s.strip_suffix('h') {
        h.parse::<u64>()
            .map(|v| Duration::from_secs(v * 3600))
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(m) = s.strip_suffix('m') {
        m.parse::<u64>()
            .map(|v| Duration::from_secs(v * 60))
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(sec) = s.strip_suffix('s') {
        sec.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else {
        Err(format!(
            "Invalid duration {}: missing unit (ms, s, m, h)",
            s
        ))
    }
}

fn parse_time(s: &str) -> Result<SystemTime, String> {
    // Matches durations with or without units
    // "10s", "5m", "100", "100.5" all valid

    // Try parsing as duration first (e.g., "10s", "5m")
    if let Ok(duration) = parse_duration(s) {
        return Ok(UNIX_EPOCH + duration);
    }

    // Fall back to unitless seconds (e.g., "100", "100.5")
    // This matches @ 100, @ 100s, and @ 1m40s are all equivalent
    match s.parse::<f64>() {
        Ok(secs) => Ok(UNIX_EPOCH + Duration::from_secs_f64(secs)),
        Err(_) => Err(format!(
            "Invalid time '{}': expected duration (10s, 5m) or seconds (100, 100.5)",
            s
        )),
    }
}

// ============================================================================
// Public API
// ============================================================================

pub fn parse_test_file(input: &str) -> Result<Vec<Command>, String> {
    Parser::parse_file(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_clear_command() {
        // given
        let input = "clear";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Clear(_)));
    }

    #[test]
    fn should_parse_ignore_command() {
        // given
        let input = "ignore";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Ignore(_)));
    }

    #[test]
    fn should_parse_resume_command() {
        // given
        let input = "resume";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Resume(_)));
    }

    #[test]
    fn should_parse_load_command() {
        // given
        let input = "load 10s\n  metric{job=\"1\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(cmd.interval, Duration::from_secs(10));
                assert_eq!(cmd.series.len(), 1);
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_eval_instant_command() {
        // given
        let input = "eval instant at 10s metric\n  {job=\"1\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert_eq!(cmd.expected.len(), 1);
                assert!(!cmd.expect_ordered);
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_parse_expect_ordered_directive() {
        // given
        let input = "eval instant at 10s metric\n  expect ordered\n  {job=\"1\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert!(cmd.expect_ordered);
                assert_eq!(cmd.expected.len(), 1);
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_parse_expansion_syntax() {
        // given
        let input = "0+10x5";

        // when
        let (vals, _) = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 40.0),
                (5, 50.0),
            ]
        );
    }

    #[test]
    fn should_advance_steps_over_blanks() {
        // given
        let input = "1 _ 3 _x2 5";

        // when
        let (vals, _) = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(vals, vec![(0, 1.0), (2, 3.0), (5, 5.0)]);
    }

    #[test]
    fn should_parse_histogram_descriptor() {
        // given
        let input = "{{schema:1 sum:5 count:4 z_bucket:1 z_bucket_w:0.01 buckets:[1 2] offset:-1 n_buckets:[1] counter_reset_hint:gauge}}";

        // when
        let (floats, hists) = parse_multiple_value_exprs(input).unwrap();

        // then
        assert!(floats.is_empty());
        assert_eq!(hists.len(), 1);
        let (step, h) = &hists[0];
        assert_eq!(*step, 0);
        assert_eq!((h.schema, h.sum, h.count), (1, 5.0, 4.0));
        assert_eq!((h.zero_count, h.zero_threshold), (1.0, 0.01));
        assert_eq!(
            h.positive,
            vec![
                Bucket {
                    index: -1,
                    count: 1.0
                },
                Bucket {
                    index: 0,
                    count: 2.0
                }
            ]
        );
        assert_eq!(
            h.negative,
            vec![Bucket {
                index: 0,
                count: 1.0
            }]
        );
        assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
    }

    #[test]
    fn should_expand_histogram_series() {
        // given
        let input = "{{sum:1 count:1 buckets:[1]}}+{{sum:2 count:2 buckets:[2]}}x2 _ {{count:7}}x1";

        // when
        let (_, hists) = parse_multiple_value_exprs(input).unwrap();

        // then
        let got: Vec<(i64, f64, f64)> = hists.iter().map(|(s, h)| (*s, h.count, h.sum)).collect();
        assert_eq!(
            got,
            vec![
                (0, 1.0, 1.0),
                (1, 3.0, 3.0),
                (2, 5.0, 5.0),
                (4, 7.0, 0.0),
                (5, 7.0, 0.0)
            ]
        );
    }

    #[test]
    fn should_use_absolute_step_indices_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+20x2";

        // when
        let (vals, _) = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 100.0),
                (5, 120.0),
                (6, 140.0),
            ]
        );
    }

    #[test]
    fn should_reject_invalid_values() {
        // given
        let input = "1 2 invalid 4";

        // when
        let result = parse_multiple_value_exprs(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid value 'invalid'"));
    }

    #[test]
    fn should_use_absolute_step_offsets_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+5x3";

        // when
        let (vals, _) = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 100.0),
                (5, 105.0),
                (6, 110.0),
                (7, 115.0),
            ]
        );
    }

    #[test]
    fn should_produce_sequential_steps_to_preserve_monotonic_timestamps() {
        // given - this demonstrates why we use sequential blocks
        let input = "0+10x3 0+20x2"; // Second expression also starts at step 0

        // when
        let (vals, _) = parse_multiple_value_exprs(input).unwrap();

        // then - we produce sequential steps [0,1,2,3,4,5,6]
        // not overlapping [0,1,2,3,0,1,2]
        // This guarantees strictly increasing timestamps when steps are converted
        // to wall-clock time, preventing Gorilla/tsz encoder panics.
        assert_eq!(
            vals,
            vec![
                (0, 0.0),  // First expression
                (1, 10.0), // First expression
                (2, 20.0), // First expression
                (3, 30.0), // First expression
                (4, 0.0),  // Second expression (step 4, not 0)
                (5, 20.0), // Second expression (step 5, not 1)
                (6, 40.0), // Second expression (step 6, not 2)
            ]
        );

        // Without sequential blocks, this would produce:
        // [(0, 0.0), (1, 10.0), (2, 20.0), (3, 30.0), (0, 0.0), (1, 20.0), (2, 40.0)]
        // which has backwards timestamps after sorting
    }

    #[test]
    fn should_parse_duration_with_units() {
        // given / when / then
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_duration("1000ms").unwrap(),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn should_parse_time_with_and_without_units() {
        // given
        let with_unit = "10s";
        let without_unit = "10";

        // when
        let t1 = parse_time(with_unit).unwrap();
        let t2 = parse_time(without_unit).unwrap();

        // then
        assert_eq!(t1, t2);
    }

    #[test]
    fn should_parse_metric_with_labels() {
        // given
        let input = "load 5m\n  metric{job=\"test\",instance=\"localhost\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(
                    cmd.series[0].labels.get("__name__"),
                    Some(&"metric".to_string())
                );
                assert_eq!(cmd.series[0].labels.get("job"), Some(&"test".to_string()));
                assert_eq!(
                    cmd.series[0].labels.get("instance"),
                    Some(&"localhost".to_string())
                );
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_label_only_selector() {
        // given
        let input = "eval instant at 10s {job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "{job=\"test\"}");
                assert_eq!(cmd.expected[0].labels.get("job"), Some("test"));
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_reject_unbalanced_braces() {
        // given
        let input = "load 5m\n  metric{job=\"test\" 1 2 3";

        // when
        let result = Parser::parse_file(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unbalanced"));
    }

    #[test]
    fn should_handle_multiple_tabs_between_time_and_query() {
        // given
        let input = "eval instant at 10s\t\tmetric\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert_eq!(cmd.time, UNIX_EPOCH + Duration::from_secs(10));
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_trim_leading_whitespace_from_query() {
        // given
        let input = "eval instant at 10s \t  metric{job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric{job=\"test\"}");
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }
}
