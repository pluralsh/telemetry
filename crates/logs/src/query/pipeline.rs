use std::ops::Range;

use super::*;
use common::display::{sanitize_label_name, sanitized_label_name};
use template::Context;

/// One row as pipeline stages see it: its line, its labels, and its
/// structured metadata, which a label lookup falls back to.
pub(super) trait StageRow: Context {
    fn set_line(&mut self, line: String);
    fn label(&self, name: &str) -> Option<&str>;
    fn metadata_value(&self, name: &str) -> Option<&str>;
    fn insert_label(&mut self, name: String, value: String);
    fn remove_label(&mut self, name: &str) -> Option<String>;
    fn remove_metadata(&mut self, name: &str) -> Option<String>;
    /// Removes every label and metadata field `keep` rejects.
    fn retain_fields(&mut self, keep: &mut dyn FnMut(&str, &str) -> bool);
    fn set_value(&mut self, value: f64);
}

/// `None` unless `stage` only tests the line, so it can run before a row's
/// labels or metadata are touched.
pub(super) fn line_test(line: &str, stage: &PipelineStage) -> Option<Result<bool>> {
    match stage {
        PipelineStage::LineFilter(filter) => Some(line_filter(line, filter)),
        // The index only produces candidates; the source line remains the
        // authority so stale/colliding postings cannot create false matches.
        PipelineStage::Match(query) => Some(Ok(source_matches(
            &DEFAULT_ANALYZER,
            line,
            &match_terms(query),
        ))),
        _ => None,
    }
}

pub(super) fn apply_stage<R: StageRow>(row: &mut R, stage: &PipelineStage) -> Result<bool> {
    match stage {
        PipelineStage::LineFilter(_) | PipelineStage::Match(_) => {
            line_test(row.line(), stage).expect("line filters and matches are line tests")
        }
        PipelineStage::Parser(parser) => {
            parse_stage(row, parser)?;
            Ok(true)
        }
        PipelineStage::LabelFilter(filter) => label_filter(row, &filter.value),
        PipelineStage::LineFormat(template) => {
            match template.compiled().render(row) {
                Ok(line) => row.set_line(line),
                Err(error) => set_error(row, "TemplateFormatErr", &error.to_string()),
            }
            Ok(true)
        }
        PipelineStage::LabelFormat(assignments) => {
            label_format(row, assignments);
            Ok(true)
        }
        // Applies to structured metadata as well as labels, as in Loki.
        PipelineStage::Drop(selections) => {
            row.retain_fields(&mut |name, value| !selected(selections, name, value));
            Ok(true)
        }
        PipelineStage::Keep(selections) => {
            row.retain_fields(&mut |name, value| {
                is_error_label(name) || selected(selections, name, value)
            });
            Ok(true)
        }
        PipelineStage::Decolorize => {
            let decolorized = match ANSI_ESCAPE.replace_all(row.line(), "") {
                Cow::Owned(line) => Some(line),
                Cow::Borrowed(_) => None,
            };
            if let Some(line) = decolorized {
                row.set_line(line);
            }
            Ok(true)
        }
        PipelineStage::Unwrap(unwrap) => apply_unwrap(row, unwrap),
    }
}

fn selected(selections: &[crate::logql::LabelSelection], name: &str, value: &str) -> bool {
    selections.iter().any(|selection| {
        selection.label == name
            && selection
                .matcher
                .as_ref()
                .is_none_or(|(op, expected)| string_match(*op, value, expected).unwrap_or(false))
    })
}

/// `or` operands share the first operand's operator: a positive filter keeps
/// a line matching any operand, a negative one drops it.
pub(super) fn line_filter(line: &str, filter: &LineFilter) -> Result<bool> {
    let negative = filter.branches.first().is_some_and(|branch| {
        matches!(
            branch.op,
            LineFilterOp::NotContains | LineFilterOp::NotRegex | LineFilterOp::NotPattern
        )
    });
    for branch in &filter.branches {
        let term = match &branch.term {
            LineFilterTerm::String(value) | LineFilterTerm::Ip(value) => value,
        };
        let found = match (&branch.op, &branch.term) {
            (LineFilterOp::Contains | LineFilterOp::NotContains, LineFilterTerm::Ip(value)) => {
                contains_ip(line, value)
            }
            (LineFilterOp::Contains | LineFilterOp::NotContains, _) => contains(line, term),
            (LineFilterOp::Regex | LineFilterOp::NotRegex, _) => {
                with_regex(RegexKind::Plain, term, |regex| regex.is_match(line))?
            }
            (LineFilterOp::Pattern | LineFilterOp::NotPattern, _) => {
                pattern_line_matches(term, line)
            }
        };
        if found {
            return Ok(!negative);
        }
    }
    Ok(negative)
}

pub(super) fn parse_stage<R: StageRow>(row: &mut R, parser: &ParserStage) -> Result<()> {
    if let ParserStage::Logfmt {
        strict,
        keep_empty,
        expressions,
    } = parser
    {
        let (labels, error) = parse_logfmt(row.line(), *strict, *keep_empty, expressions)?;
        merge_parsed(row, labels);
        if let Some(error) = error {
            set_error(row, "LogfmtParserErr", &error);
        }
        return Ok(());
    }
    let result = match parser {
        ParserStage::Json { expressions } => parse_json(row.line(), expressions),
        ParserStage::Logfmt { .. } => unreachable!(),
        ParserStage::Regexp(expression) => Ok(with_regex(RegexKind::Plain, expression, |regex| {
            named_captures(regex, row.line())
        })?),
        ParserStage::Pattern(pattern) => Ok(pattern_captures(pattern, row.line())),
        ParserStage::Unpack => unpack(row.line()).map(|(line, labels)| {
            row.set_line(line);
            labels
        }),
    };
    match result {
        Ok(labels) => {
            merge_parsed(row, labels);
            Ok(())
        }
        Err(error) => {
            let details = match (parser, &error) {
                // Loki reports jsonparser's message, not a position.
                (ParserStage::Json { .. } | ParserStage::Unpack, Error::Json(_)) => {
                    "Malformed JSON error".to_owned()
                }
                _ => error.to_string(),
            };
            set_error(row, parser_error_name(parser), &details);
            Ok(())
        }
    }
}

/// Whether a label filter names `__error__`, which asks for parser errors
/// to be kept in metric results (`__preserve_error__="true"`).
pub(super) fn filters_on_error(log: &LogExpr) -> bool {
    fn names_error(filter: &LabelFilterExpr) -> bool {
        match filter {
            LabelFilterExpr::And(lhs, rhs) | LabelFilterExpr::Or(lhs, rhs) => {
                names_error(&lhs.value) || names_error(&rhs.value)
            }
            LabelFilterExpr::Predicate(predicate) => predicate.label == ERROR_LABEL,
        }
    }
    log.stages.iter().any(|stage| match &stage.value {
        PipelineStage::LabelFilter(filter) => names_error(&filter.value),
        PipelineStage::Unwrap(Unwrap {
            post_filter: Some(filter),
            ..
        }) => names_error(&filter.value),
        _ => false,
    })
}

pub(super) fn parser_error_name(parser: &ParserStage) -> &'static str {
    match parser {
        ParserStage::Json { .. } => "JSONParserErr",
        ParserStage::Logfmt { .. } => "LogfmtParserErr",
        ParserStage::Regexp(_) => "RegexpParserErr",
        ParserStage::Pattern(_) => "PatternParserErr",
        ParserStage::Unpack => "UnpackParserErr",
    }
}

pub(super) fn merge_parsed<R: StageRow>(row: &mut R, labels: BTreeMap<String, String>) {
    for (mut name, value) in labels {
        if row.label(&name).is_some() || row.metadata_value(&name).is_some() {
            // An empty extraction never displaces a clashing label's `_extracted` copy.
            if value.is_empty() {
                continue;
            }
            name.push_str("_extracted");
        }
        row.insert_label(name, value);
    }
}

/// Labels that report a pipeline error; `keep` and grouping never remove them.
pub(super) fn is_error_label(name: &str) -> bool {
    matches!(
        name,
        ERROR_LABEL | ERROR_DETAILS_LABEL | PRESERVE_ERROR_LABEL
    )
}

pub(super) fn set_error<R: StageRow>(row: &mut R, kind: &str, details: &str) {
    row.insert_label(ERROR_LABEL.into(), kind.into());
    row.insert_label(ERROR_DETAILS_LABEL.into(), details.into());
}

pub(super) fn parse_json(
    line: &str,
    expressions: &[ParserExpression],
) -> Result<BTreeMap<String, String>> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    let mut flattened = BTreeMap::new();
    if expressions.is_empty() {
        flatten_json("", &value, &mut flattened);
    } else {
        for expression in expressions {
            let extracted =
                json_path(&value, &expression.expression)?.map_or_else(String::new, json_string);
            flattened.insert(expression.label.clone(), extracted);
        }
    }
    Ok(flattened)
}

pub(super) fn flatten_json(
    prefix: &str,
    value: &serde_json::Value,
    output: &mut BTreeMap<String, String>,
) {
    if let serde_json::Value::Object(object) = value {
        for (name, value) in object {
            let name = if prefix.is_empty() {
                sanitize_label_name(name)
            } else {
                sanitize_label_name(&format!("{prefix}_{name}"))
            };
            match value {
                serde_json::Value::Object(_) => flatten_json(&name, value, output),
                // Loki extracts neither nulls nor arrays.
                serde_json::Value::Null | serde_json::Value::Array(_) => {}
                _ => {
                    output.insert(name, json_string(value));
                }
            }
        }
    }
}

pub(super) fn json_path<'a>(
    mut value: &'a serde_json::Value,
    path: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let bytes = path.as_bytes();
    let mut cursor = 0usize;
    while cursor < path.len() {
        if bytes[cursor] == b'.' {
            cursor += 1;
            if cursor == path.len() {
                return Err(Error::Query("JSON path cannot end with '.'".into()));
            }
        }
        let next = if bytes[cursor] == b'[' {
            let (step, end) = json_path_bracket(path, cursor + 1)?;
            cursor = end;
            match step {
                JsonPathStep::Key(key) => value.get(key),
                JsonPathStep::Index(index) => value.get(index),
            }
        } else {
            let end = json_path_identifier(bytes, cursor)?;
            let next = value.get(&path[cursor..end]);
            cursor = end;
            next
        };
        let Some(next) = next else {
            return Ok(None);
        };
        value = next;
        if cursor < path.len() && !matches!(bytes[cursor], b'.' | b'[') {
            return Err(Error::Query("invalid JSON path separator".into()));
        }
    }
    Ok(Some(value))
}

enum JsonPathStep {
    Key(String),
    Index(usize),
}

/// Parses the bracketed step that starts just after `[`, returning it with
/// the position just past the closing `]`.
fn json_path_bracket(path: &str, cursor: usize) -> Result<(JsonPathStep, usize)> {
    let bytes = path.as_bytes();
    let start = skip_ascii_whitespace(bytes, cursor);
    if start == path.len() {
        return Err(Error::Query("unterminated JSON path bracket".into()));
    }
    let (step, end, missing_close) = if bytes[start] == b'"' {
        let end = quoted_end(bytes, start + 1);
        let key = serde_json::from_str(&path[start..end])?;
        (
            JsonPathStep::Key(key),
            end,
            "JSON path quoted key requires ']'",
        )
    } else {
        let end = start
            + bytes[start..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count();
        let index = path[start..end]
            .parse::<usize>()
            .map_err(|_| Error::Query("JSON array index must be an integer".into()))?;
        (
            JsonPathStep::Index(index),
            end,
            "JSON array index requires ']'",
        )
    };
    let close = skip_ascii_whitespace(bytes, end);
    if bytes.get(close) != Some(&b']') {
        return Err(Error::Query(missing_close.into()));
    }
    Ok((step, close + 1))
}

/// Returns the position just past the closing quote of a JSON string whose
/// body starts at `cursor`, or the end of input if it is unterminated.
pub(super) fn quoted_end(bytes: &[u8], mut cursor: usize) -> usize {
    let mut escaped = false;
    while let Some(&byte) = bytes.get(cursor) {
        cursor += 1;
        match byte {
            _ if escaped => escaped = false,
            b'\\' => escaped = true,
            b'"' => break,
            _ => {}
        }
    }
    cursor
}

fn json_path_identifier(bytes: &[u8], start: usize) -> Result<usize> {
    let first = bytes[start];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return Err(Error::Query("invalid JSON path identifier".into()));
    }
    let rest = bytes[start + 1..]
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
        .count();
    Ok(start + 1 + rest)
}

pub(super) fn skip_ascii_whitespace(bytes: &[u8], cursor: usize) -> usize {
    cursor
        + bytes[cursor..]
            .iter()
            .take_while(|b| b.is_ascii_whitespace())
            .count()
}

pub(super) fn json_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

pub(super) fn parse_logfmt(
    line: &str,
    strict: bool,
    keep_empty: bool,
    expressions: &[ParserExpression],
) -> Result<(BTreeMap<String, String>, Option<String>)> {
    let mut all = BTreeMap::new();
    let parse_error = logfmt_pairs(line, strict, |key, value| {
        if keep_empty || !value.is_empty() {
            all.entry(key.into_owned())
                .or_insert_with(|| value.into_string(line));
        }
    });
    if expressions.is_empty() {
        Ok((all, parse_error))
    } else {
        Ok((
            expressions
                .iter()
                .map(|expression| {
                    (
                        expression.label.clone(),
                        all.get(&*sanitized_label_name(&expression.expression))
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect(),
            parse_error,
        ))
    }
}

/// A logfmt value: bytes of its line, or a quoted value with escapes.
pub(super) enum LogfmtValue {
    Range(Range<usize>),
    Owned(String),
}

impl LogfmtValue {
    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Range(range) => range.is_empty(),
            Self::Owned(value) => value.is_empty(),
        }
    }

    fn into_string(self, line: &str) -> String {
        match self {
            Self::Range(range) => line[range].to_owned(),
            Self::Owned(value) => value,
        }
    }
}

/// Passes `pair` each sanitized key and value of a logfmt `line`, in line
/// order, and returns the syntax error that ended a strict parse; pairs
/// before it were passed.
pub(super) fn logfmt_pairs<'a>(
    line: &'a str,
    strict: bool,
    mut pair: impl FnMut(Cow<'a, str>, LogfmtValue),
) -> Option<String> {
    let bytes = line.as_bytes();
    let mut cursor = 0usize;
    while cursor < line.len() {
        while cursor < line.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == line.len() {
            break;
        }
        let token_start = cursor;
        let key_start = cursor;
        while cursor < line.len()
            && !bytes[cursor].is_ascii_whitespace()
            && !matches!(bytes[cursor], b'=' | b'"')
        {
            cursor += 1;
        }
        if cursor == key_start || bytes.get(cursor) == Some(&b'"') {
            if strict {
                return Some(format!(
                    "logfmt syntax error at pos {} : invalid key",
                    cursor + 1
                ));
            }
            skip_logfmt_token(line, &mut cursor);
            continue;
        }
        let key = sanitized_label_name(&line[key_start..cursor]);
        let value = match logfmt_value(line, &mut cursor, strict) {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(error) => return Some(error),
        };
        if key.is_empty() {
            if strict {
                return Some(format!(
                    "logfmt syntax error at pos {} : invalid key",
                    token_start + 1
                ));
            }
            continue;
        }
        pair(key, value);
    }
    None
}

/// Scans the value following a logfmt key. `Ok(None)` means a malformed
/// pair was skipped; errors are only reported in strict mode.
fn logfmt_value(
    line: &str,
    cursor: &mut usize,
    strict: bool,
) -> std::result::Result<Option<LogfmtValue>, String> {
    if line.as_bytes().get(*cursor) != Some(&b'=') {
        return Ok(Some(LogfmtValue::Range(*cursor..*cursor)));
    }
    *cursor += 1;
    if line.as_bytes().get(*cursor) == Some(&b'"') {
        return match scan_logfmt_quoted(line, cursor) {
            Ok(value) => Ok(Some(value)),
            Err(error) if strict => Err(error),
            Err(_) => {
                skip_logfmt_token(line, cursor);
                Ok(None)
            }
        };
    }
    let start = *cursor;
    skip_logfmt_token(line, cursor);
    let value = &line[start..*cursor];
    if strict && let Some(offset) = value.find(['=', '"']) {
        return Err(format!(
            "logfmt syntax error at pos {} : unexpected '{}'",
            start + offset + 1,
            char::from(value.as_bytes()[offset])
        ));
    }
    Ok(Some(LogfmtValue::Range(start..*cursor)))
}

pub(super) fn skip_logfmt_token(line: &str, cursor: &mut usize) {
    while *cursor < line.len() && !line.as_bytes()[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }
}

/// The quoted value opening at `cursor`, borrowed from the line unless it
/// holds escapes.
fn scan_logfmt_quoted(line: &str, cursor: &mut usize) -> std::result::Result<LogfmtValue, String> {
    let start = *cursor + 1;
    if let Some(offset) = memchr::memchr2(b'"', b'\\', &line.as_bytes()[start..])
        && line.as_bytes()[start + offset] == b'"'
    {
        *cursor = start + offset + 1;
        return Ok(LogfmtValue::Range(start..start + offset));
    }
    *cursor += 1;
    let mut output = String::new();
    let mut chars = line[*cursor..].char_indices();
    while let Some((relative, character)) = chars.next() {
        *cursor += character.len_utf8();
        match character {
            '"' => return Ok(LogfmtValue::Owned(output)),
            '\\' => {
                let Some((_, escaped)) = chars.next() else {
                    return Err(format!(
                        "logfmt syntax error at pos {} : invalid quoted value",
                        *cursor + relative + 1
                    ));
                };
                *cursor += escaped.len_utf8();
                output.push(match escaped {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '"' => '"',
                    '\\' => '\\',
                    other => other,
                });
            }
            value => output.push(value),
        }
    }
    Err(format!(
        "logfmt syntax error at pos {} : unterminated quoted value",
        *cursor + 1
    ))
}

pub(super) fn unpack(line: &str) -> Result<(String, BTreeMap<String, String>)> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::Query("packed value must be an object".into()))?;
    let unpacked = object
        .get("_entry")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(line)
        .to_owned();
    let mut labels = BTreeMap::new();
    // unpack only promotes string fields.
    for (name, value) in object {
        if let Some(value) = value.as_str()
            && name != "_entry"
        {
            labels.insert(sanitize_label_name(name), value.to_owned());
        }
    }
    Ok((unpacked, labels))
}

enum PatternPart<'a> {
    Literal(&'a str),
    Capture,
}

/// Splits a pattern as Loki's lexer does: only `<identifier>` is a capture,
/// any other `<` is literal text.
fn pattern_parts(pattern: &str) -> impl Iterator<Item = PatternPart<'_>> {
    let mut rest = pattern;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        if let Some(length) = capture_length(rest) {
            rest = &rest[length..];
            return Some(PatternPart::Capture);
        }
        let end = rest
            .match_indices('<')
            .map(|(index, _)| index)
            .find(|&index| index > 0 && capture_length(&rest[index..]).is_some())
            .unwrap_or(rest.len());
        let literal = &rest[..end];
        rest = &rest[end..];
        Some(PatternPart::Literal(literal))
    })
}

fn capture_length(input: &str) -> Option<usize> {
    let body = input.strip_prefix('<')?;
    let end = body.find('>')?;
    let mut name = body[..end].chars();
    let first = name.next()?;
    ((first.is_ascii_alphabetic() || first == '_')
        && name.all(|char| char.is_ascii_alphanumeric() || char == '_'))
    .then_some(end + 2)
}

/// Loki's `|>` matcher (`pattern.Matcher.Test`): each literal binds to its
/// first occurrence without backtracking, captures must be non-empty, and a
/// trailing literal must end the line while a trailing capture must not.
pub(super) fn pattern_line_matches(pattern: &str, line: &str) -> bool {
    let mut offset = 0;
    let mut ends_on_capture = None;
    for (index, part) in pattern_parts(pattern).enumerate() {
        match part {
            PatternPart::Capture => ends_on_capture = Some(true),
            PatternPart::Literal(literal) => {
                ends_on_capture = Some(false);
                let Some(found) = line[offset..].find(literal) else {
                    return false;
                };
                if index != 0 && found == 0 {
                    return false;
                }
                offset += found + literal.len();
            }
        }
    }
    match ends_on_capture {
        None => line.is_empty(),
        Some(_) if line.is_empty() => false,
        Some(ends_on_capture) => ends_on_capture == (offset != line.len()),
    }
}

/// Loki's pattern parser, which is lenient unlike the `|>` filter: a leading
/// literal must match, then each capture runs to the next literal, and a
/// missing literal ends the match with the capture taking the rest.
pub(super) fn pattern_captures(pattern: &str, line: &str) -> BTreeMap<String, String> {
    enum Part<'a> {
        Literal(&'a str),
        Capture(&'a str),
    }
    let mut parts = Vec::new();
    let mut rest = pattern;
    while let Some(start) = rest.find('<') {
        let Some(end) = rest[start..].find('>').map(|offset| start + offset) else {
            break;
        };
        if start > 0 {
            parts.push(Part::Literal(&rest[..start]));
        }
        parts.push(Part::Capture(&rest[start + 1..end]));
        rest = &rest[end + 1..];
    }
    if !rest.is_empty() {
        parts.push(Part::Literal(rest));
    }

    let mut labels = BTreeMap::new();
    let mut input = line;
    let mut parts = parts.into_iter().peekable();
    if let Some(Part::Literal(literal)) = parts.peek() {
        let Some(stripped) = input.strip_prefix(*literal) else {
            return labels;
        };
        input = stripped;
        parts.next();
    }
    if line.is_empty() {
        return labels;
    }
    while let Some(Part::Capture(name)) = parts.next() {
        let (value, remaining) = match parts.next() {
            Some(Part::Literal(literal)) => match input.find(literal) {
                Some(index) => (&input[..index], Some(&input[index + literal.len()..])),
                None => (input, None),
            },
            _ => (input, None),
        };
        if name != "_" {
            labels.insert(name.to_owned(), value.to_owned());
        }
        match remaining {
            Some(remaining) => input = remaining,
            None => break,
        }
    }
    labels
}

pub(super) fn named_captures(regex: &Regex, line: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if let Some(captures) = regex.captures(line) {
        for name in regex.capture_names().flatten() {
            if let Some(value) = captures.name(name) {
                labels.insert(name.to_owned(), value.as_str().to_owned());
            }
        }
    }
    labels
}

/// Loki's label filter semantics: string matchers see a missing label as
/// empty; typed filters drop rows missing the label and keep unparsable
/// values, marked `LabelFilterErr` unless an earlier error is reported;
/// `ip()` passes any row that already carries an error.
pub(super) fn label_filter<R: StageRow>(row: &mut R, expression: &LabelFilterExpr) -> Result<bool> {
    match expression {
        LabelFilterExpr::And(lhs, rhs) => {
            // Both sides run, so either can report an error.
            let lhs = label_filter(row, &lhs.value)?;
            Ok(label_filter(row, &rhs.value)? && lhs)
        }
        LabelFilterExpr::Or(lhs, rhs) => {
            Ok(label_filter(row, &lhs.value)? || label_filter(row, &rhs.value)?)
        }
        LabelFilterExpr::Predicate(predicate) => {
            let typed = matches!(
                predicate.value,
                FilterValue::Number(_)
                    | FilterValue::Bytes(_)
                    | FilterValue::Duration(_)
                    | FilterValue::Ip(_)
            ) && !matches!(predicate.op, ComparisonOp::Regex | ComparisonOp::NotRegex);
            if !typed {
                let actual = lookup(row, &predicate.label).unwrap_or("");
                return compare_filter(actual, predicate.op, &predicate.value);
            }
            if matches!(predicate.value, FilterValue::Ip(_)) && row.label(ERROR_LABEL).is_some() {
                return Ok(true);
            }
            let Some(actual) = lookup(row, &predicate.label) else {
                return Ok(false);
            };
            if let FilterValue::Ip(pattern) = &predicate.value {
                return Ok(compare_bool(contains_ip(actual, pattern), predicate.op));
            }
            let parsed = match &predicate.value {
                FilterValue::Number(_) => actual.parse::<f64>().ok(),
                FilterValue::Bytes(_) => parse_bytes(actual).ok(),
                _ => parse_go_duration_ns(actual),
            };
            if parsed.is_none() {
                if row.label(ERROR_LABEL).is_none() {
                    let details = format!(
                        "cannot parse {:?} as {}",
                        actual,
                        filter_kind(&predicate.value)
                    );
                    set_error(row, "LabelFilterErr", &details);
                }
                return Ok(true);
            }
            compare_filter(actual, predicate.op, &predicate.value)
        }
    }
}

fn filter_kind(value: &FilterValue) -> &'static str {
    match value {
        FilterValue::Bytes(_) => "bytes",
        FilterValue::Duration(_) => "a duration",
        _ => "a number",
    }
}

pub(super) fn compare_filter(
    actual: &str,
    op: ComparisonOp,
    expected: &FilterValue,
) -> Result<bool> {
    if matches!(op, ComparisonOp::Regex | ComparisonOp::NotRegex) {
        let (FilterValue::String(expected) | FilterValue::Identifier(expected)) = expected else {
            return Ok(false);
        };
        let matched = match label_regex_filter(expected) {
            Some(filter) => filter.matches(actual),
            None => with_regex(RegexKind::Anchored, expected, |regex| {
                regex.is_match(actual)
            })?,
        };
        return Ok(if op == ComparisonOp::Regex {
            matched
        } else {
            !matched
        });
    }
    let ordering = match expected {
        FilterValue::Number(value) => numeric_cmp(actual, value.parse().ok()),
        FilterValue::Bytes(value) => parse_bytes(actual)
            .ok()
            .and_then(|actual| actual.partial_cmp(&parse_bytes(value).ok()?)),
        FilterValue::Duration(value) => parse_go_duration_ns(actual)
            .and_then(|actual| actual.partial_cmp(&(parse_duration_ns(value).ok()? as f64))),
        FilterValue::Ip(value) => {
            return Ok(compare_bool(contains_ip(actual, value), op));
        }
        FilterValue::String(value) | FilterValue::Identifier(value) => Some(actual.cmp(value)),
    };
    Ok(ordering.is_some_and(|ordering| compare_ordering(ordering, op)))
}

pub(super) fn numeric_cmp(actual: &str, expected: Option<f64>) -> Option<Ordering> {
    actual.parse::<f64>().ok()?.partial_cmp(&expected?)
}

pub(super) fn compare_bool(equal: bool, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Equal => equal,
        ComparisonOp::NotEqual => !equal,
        _ => false,
    }
}

pub(super) fn compare_ordering(ordering: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterOrEqual => ordering != Ordering::Less,
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessOrEqual => ordering != Ordering::Greater,
        ComparisonOp::Regex | ComparisonOp::NotRegex => false,
    }
}

pub(super) fn label_format<R: StageRow>(row: &mut R, assignments: &[FormatAssignment]) {
    for assignment in assignments {
        // A reported error owns its labels; formatting cannot rewrite them.
        if matches!(assignment.label.as_str(), ERROR_LABEL | ERROR_DETAILS_LABEL)
            && row.label(ERROR_LABEL).is_some()
        {
            continue;
        }
        let value = match &assignment.value {
            FormatValue::Rename(source) => row
                .remove_label(source)
                .or_else(|| row.remove_metadata(source))
                .unwrap_or_default(),
            FormatValue::Template(template) => match template.compiled().render(row) {
                Ok(value) => value,
                Err(error) => {
                    set_error(row, "TemplateFormatErr", &error.to_string());
                    continue;
                }
            },
        };
        // An empty value leaves the label absent, as in Loki's label builder.
        if value.is_empty() {
            row.remove_label(&assignment.label);
        } else {
            row.insert_label(assignment.label.clone(), value);
        }
    }
}

pub(super) fn lookup<'a, R: StageRow>(row: &'a R, name: &str) -> Option<&'a str> {
    match name {
        "__line__" => Some(row.line()),
        _ => row.label(name).or_else(|| row.metadata_value(name)),
    }
}

/// A row without the unwrapped label is dropped; an unconvertible value keeps
/// the row, marked with `SampleExtractionErr`. `duration()` and
/// `duration_seconds()` both yield seconds.
pub(super) fn apply_unwrap<R: StageRow>(row: &mut R, unwrap: &Unwrap) -> Result<bool> {
    let Some(source) = lookup(row, &unwrap.label).map(str::to_owned) else {
        return Ok(false);
    };
    let parsed = match unwrap.conversion {
        None => source.parse::<f64>().ok(),
        Some(Conversion::Bytes) => parse_bytes(&source).ok(),
        Some(Conversion::Duration | Conversion::DurationSeconds) => {
            parse_go_duration_ns(&source).map(|value| value / 1_000_000_000.0)
        }
    };
    match parsed {
        Some(value) => row.set_value(value),
        None => set_error(
            row,
            "SampleExtractionErr",
            &format!("unable to convert unwrap value {source:?}"),
        ),
    }
    unwrap
        .post_filter
        .as_ref()
        .map_or(Ok(true), |filter| label_filter(row, &filter.value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_stage_free_counts_and_byte_sums_read_no_lines() {
        let read = |query: &str| {
            let query = logql::parse(query).unwrap();
            lineless_read(&query, &QueryRequest::range("", 0, 10, 1)).unwrap()
        };
        for query in [
            r#"count_over_time({a="b"}[1m])"#,
            r#"sum by (a) (rate({a="b"}[1m] offset 5s))"#,
            r#"bytes_rate({a="b"}[1m]) / bytes_over_time({c="d"}[5m])"#,
        ] {
            assert!(read(query).is_some(), "{query}");
        }
        for query in [
            r#"{a="b"}"#,
            r#"count_over_time({a="b"} |= "" [1m])"#,
            r#"sum_over_time({a="b"} | unwrap x [1m])"#,
            r#"absent_over_time({a="b"}[1m])"#,
            r#"count_over_time({a="b"}[1m]) / sum_over_time({a="b"} | unwrap x [1m])"#,
        ] {
            assert!(read(query).is_none(), "{query}");
        }
        assert!(
            !read(r#"sum(count_over_time({a="b"}[1m]))"#)
                .unwrap()
                .metadata
        );
        assert!(
            read(r#"sum by (a) (count_over_time({a="b"}[1m]))"#)
                .unwrap()
                .metadata
        );
        assert!(read(r#"count_over_time({a="b"}[1m])"#).unwrap().metadata);
    }

    fn logfmt(line: &str, strict: bool) -> (BTreeMap<String, String>, Option<String>) {
        parse_logfmt(line, strict, false, &[]).unwrap()
    }

    #[test]
    fn logfmt_parses_bare_quoted_and_valueless_pairs() {
        let (labels, error) = logfmt(r#"a=1 b="two words" c d=x=y"#, false);
        assert_eq!(error, None);
        assert_eq!(labels.get("a").map(String::as_str), Some("1"));
        assert_eq!(labels.get("b").map(String::as_str), Some("two words"));
        assert_eq!(labels.get("c"), None);
        assert_eq!(labels.get("d").map(String::as_str), Some("x=y"));
    }

    #[test]
    fn strict_logfmt_reports_unexpected_character_in_value() {
        let (labels, error) = logfmt("a=1 b=x=y c=3", true);
        assert_eq!(
            error.as_deref(),
            Some("logfmt syntax error at pos 8 : unexpected '='")
        );
        assert_eq!(labels.get("a").map(String::as_str), Some("1"));
        assert_eq!(labels.get("c"), None);
    }

    #[test]
    fn json_path_walks_keys_indexes_and_quoted_keys() {
        let value = serde_json::json!({"a": {"b c": [10, {"d": true}]}});
        let found = |path| json_path(&value, path).unwrap().cloned();
        assert_eq!(found(r#"a["b c"][0]"#), Some(serde_json::json!(10)));
        assert_eq!(found(r#"a[ "b c" ][1].d"#), Some(serde_json::json!(true)));
        assert_eq!(found("a.missing"), None);
        assert!(json_path(&value, "a.").is_err());
        assert!(json_path(&value, "a[x]").is_err());
        assert!(json_path(&value, "a[0").is_err());
        assert!(json_path(&value, "a-b").is_err());
    }
}
