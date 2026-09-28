use super::*;

pub(super) fn apply_stage(row: &mut Row, stage: &PipelineStage) -> Result<bool> {
    match stage {
        PipelineStage::LineFilter(filter) => line_filter(&row.line, filter),
        PipelineStage::Parser(parser) => {
            parse_stage(row, parser)?;
            Ok(true)
        }
        PipelineStage::LabelFilter(filter) => label_filter(row, &filter.value),
        PipelineStage::LineFormat(template) => {
            match render_template(template, row) {
                Ok(line) => row.line = line,
                Err(error) => set_error(row, "TemplateFormatErr", &error.to_string()),
            }
            Ok(true)
        }
        PipelineStage::LabelFormat(assignments) => {
            label_format(row, assignments);
            Ok(true)
        }
        PipelineStage::Drop(selections) => {
            retain_labels(row, |name, value| !selected(selections, name, value));
            Ok(true)
        }
        PipelineStage::Keep(selections) => {
            retain_labels(row, |name, value| selected(selections, name, value));
            Ok(true)
        }
        PipelineStage::Decolorize => {
            if let Cow::Owned(line) = ANSI_ESCAPE.replace_all(&row.line, "") {
                row.line = line;
            }
            Ok(true)
        }
        PipelineStage::Unwrap(unwrap) => apply_unwrap(row, unwrap),
        // The index only produces candidates; the source line remains the
        // authority so stale/colliding postings cannot create false matches.
        PipelineStage::Match(query) => Ok(source_matches(&row.line, &match_terms(query))),
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

/// Copies the stream's shared labels only if something is actually removed.
fn retain_labels(row: &mut Row, mut keep: impl FnMut(&str, &str) -> bool) {
    if row.labels.iter().all(|(name, value)| keep(name, value)) {
        return;
    }
    Arc::make_mut(&mut row.labels).retain(|name, value| keep(name, value));
}

pub(super) fn line_filter(line: &str, filter: &LineFilter) -> Result<bool> {
    for branch in &filter.branches {
        let term = match &branch.term {
            LineFilterTerm::String(value) | LineFilterTerm::Ip(value) => value,
        };
        let found = match (&branch.op, &branch.term) {
            (LineFilterOp::Contains | LineFilterOp::NotContains, LineFilterTerm::Ip(value)) => line
                .split(|character: char| {
                    !(character.is_ascii_hexdigit() || ".:/".contains(character))
                })
                .any(|candidate| ip_matches(candidate, value)),
            (LineFilterOp::Contains | LineFilterOp::NotContains, _) => line.contains(term),
            (LineFilterOp::Regex | LineFilterOp::NotRegex, _) => {
                with_regex(RegexKind::Plain, term, |regex| regex.is_match(line))?
            }
            (LineFilterOp::Pattern | LineFilterOp::NotPattern, _) => {
                with_regex(RegexKind::Pattern, term, |regex| regex.is_match(line))?
            }
        };
        let accepted = match branch.op {
            LineFilterOp::NotContains | LineFilterOp::NotRegex | LineFilterOp::NotPattern => !found,
            _ => found,
        };
        if accepted {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn parse_stage(row: &mut Row, parser: &ParserStage) -> Result<()> {
    if let ParserStage::Logfmt {
        strict,
        keep_empty,
        expressions,
    } = parser
    {
        let (labels, error) = parse_logfmt(&row.line, *strict, *keep_empty, expressions)?;
        merge_parsed(row, labels);
        if let Some(error) = error {
            set_error(row, "LogfmtParserErr", &error);
        }
        return Ok(());
    }
    let result = match parser {
        ParserStage::Json { expressions } => parse_json(&row.line, expressions),
        ParserStage::Logfmt { .. } => unreachable!(),
        ParserStage::Regexp(expression) => Ok(with_regex(RegexKind::Plain, expression, |regex| {
            named_captures(regex, &row.line)
        })?),
        ParserStage::Pattern(pattern) => {
            Ok(with_regex(RegexKind::PatternCaptures, pattern, |regex| {
                named_captures(regex, &row.line)
            })?)
        }
        ParserStage::Unpack => unpack(&row.line).map(|(line, labels)| {
            row.line = line;
            labels
        }),
    };
    match result {
        Ok(labels) => {
            merge_parsed(row, labels);
            Ok(())
        }
        Err(error) => {
            set_error(row, parser_error_name(parser), &error.to_string());
            Ok(())
        }
    }
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

pub(super) fn merge_parsed(row: &mut Row, labels: BTreeMap<String, String>) {
    if labels.is_empty() {
        return;
    }
    let row_labels = Arc::make_mut(&mut row.labels);
    for (mut name, value) in labels {
        if row_labels.contains_key(&name) {
            name.push_str("_extracted");
        }
        row_labels.insert(name, value);
    }
}

pub(super) fn set_error(row: &mut Row, kind: &str, details: &str) {
    let labels = Arc::make_mut(&mut row.labels);
    labels.insert("__error__".into(), kind.into());
    labels.insert("__error_details__".into(), details.into());
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
            let name = sanitize_label(if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}_{name}")
            });
            if value.is_object() {
                flatten_json(&name, value, output);
            } else {
                output.insert(name, json_string(value));
            }
        }
    }
}

pub(super) fn json_path<'a>(
    mut value: &'a serde_json::Value,
    path: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let mut cursor = 0usize;
    let bytes = path.as_bytes();
    while cursor < path.len() {
        if bytes[cursor] == b'.' {
            cursor += 1;
            if cursor == path.len() {
                return Err(Error::Query("JSON path cannot end with '.'".into()));
            }
        }
        if bytes[cursor] == b'[' {
            cursor += 1;
            while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor == path.len() {
                return Err(Error::Query("unterminated JSON path bracket".into()));
            }
            if bytes[cursor] == b'"' {
                let start = cursor;
                cursor += 1;
                let mut escaped = false;
                while cursor < path.len() {
                    let byte = bytes[cursor];
                    cursor += 1;
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        break;
                    }
                }
                let key: String = serde_json::from_str(&path[start..cursor])?;
                while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if bytes.get(cursor) != Some(&b']') {
                    return Err(Error::Query("JSON path quoted key requires ']'".into()));
                }
                cursor += 1;
                let Some(next) = value.get(&key) else {
                    return Ok(None);
                };
                value = next;
            } else {
                let start = cursor;
                while cursor < path.len() && bytes[cursor].is_ascii_digit() {
                    cursor += 1;
                }
                let index = path[start..cursor]
                    .parse::<usize>()
                    .map_err(|_| Error::Query("JSON array index must be an integer".into()))?;
                while cursor < path.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if bytes.get(cursor) != Some(&b']') {
                    return Err(Error::Query("JSON array index requires ']'".into()));
                }
                cursor += 1;
                let Some(next) = value.get(index) else {
                    return Ok(None);
                };
                value = next;
            }
        } else {
            let start = cursor;
            let first = bytes[cursor];
            if !(first.is_ascii_alphabetic() || first == b'_') {
                return Err(Error::Query("invalid JSON path identifier".into()));
            }
            cursor += 1;
            while cursor < path.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            let Some(next) = value.get(&path[start..cursor]) else {
                return Ok(None);
            };
            value = next;
        }
        if cursor < path.len() && bytes[cursor] != b'.' && bytes[cursor] != b'[' {
            return Err(Error::Query("invalid JSON path separator".into()));
        }
    }
    Ok(Some(value))
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
    let mut cursor = 0usize;
    let mut parse_error = None;
    while cursor < line.len() {
        while cursor < line.len() && line.as_bytes()[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor == line.len() {
            break;
        }
        let token_start = cursor;
        let key_start = cursor;
        while cursor < line.len()
            && !line.as_bytes()[cursor].is_ascii_whitespace()
            && !matches!(line.as_bytes()[cursor], b'=' | b'"')
        {
            cursor += 1;
        }
        if cursor == key_start || line.as_bytes().get(cursor) == Some(&b'"') {
            let error = format!("logfmt syntax error at pos {} : invalid key", cursor + 1);
            if strict {
                parse_error = Some(error);
                break;
            }
            skip_logfmt_token(line, &mut cursor);
            continue;
        }
        let key = sanitize_label(line[key_start..cursor].to_owned());
        let value = if line.as_bytes().get(cursor) == Some(&b'=') {
            cursor += 1;
            if line.as_bytes().get(cursor) == Some(&b'"') {
                match scan_logfmt_quoted(line, &mut cursor) {
                    Ok(value) => value,
                    Err(error) if strict => {
                        parse_error = Some(error);
                        break;
                    }
                    Err(_) => {
                        skip_logfmt_token(line, &mut cursor);
                        continue;
                    }
                }
            } else {
                let start = cursor;
                while cursor < line.len() && !line.as_bytes()[cursor].is_ascii_whitespace() {
                    if matches!(line.as_bytes()[cursor], b'=' | b'"') {
                        let error = format!(
                            "logfmt syntax error at pos {} : unexpected '{}'",
                            cursor + 1,
                            char::from(line.as_bytes()[cursor])
                        );
                        if strict {
                            parse_error = Some(error);
                            break;
                        }
                        skip_logfmt_token(line, &mut cursor);
                        break;
                    }
                    cursor += 1;
                }
                if cursor < line.len() && !line.as_bytes()[cursor].is_ascii_whitespace() {
                    continue;
                }
                line[start..cursor].to_owned()
            }
        } else {
            String::new()
        };
        if key.is_empty() {
            if strict {
                parse_error = Some(format!(
                    "logfmt syntax error at pos {} : invalid key",
                    token_start + 1
                ));
                break;
            }
            continue;
        }
        if keep_empty || !value.is_empty() {
            all.entry(key).or_insert(value);
        }
    }
    if expressions.is_empty() {
        Ok((all, parse_error))
    } else {
        Ok((
            expressions
                .iter()
                .map(|expression| {
                    (
                        expression.label.clone(),
                        all.get(&sanitize_label(expression.expression.clone()))
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect(),
            parse_error,
        ))
    }
}

pub(super) fn skip_logfmt_token(line: &str, cursor: &mut usize) {
    while *cursor < line.len() && !line.as_bytes()[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }
}

pub(super) fn scan_logfmt_quoted(
    line: &str,
    cursor: &mut usize,
) -> std::result::Result<String, String> {
    *cursor += 1;
    let mut output = String::new();
    let mut chars = line[*cursor..].char_indices();
    while let Some((relative, character)) = chars.next() {
        *cursor += character.len_utf8();
        match character {
            '"' => return Ok(output),
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
    for (name, value) in object {
        if name != "_entry" {
            labels.insert(sanitize_label(name.clone()), json_string(value));
        }
    }
    Ok((unpacked, labels))
}

pub(super) fn sanitize_label(name: String) -> String {
    let mut result = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if result.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        result.insert(0, '_');
    }
    result
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

pub(super) fn label_filter(row: &Row, expression: &LabelFilterExpr) -> Result<bool> {
    match expression {
        LabelFilterExpr::And(lhs, rhs) => {
            Ok(label_filter(row, &lhs.value)? && label_filter(row, &rhs.value)?)
        }
        LabelFilterExpr::Or(lhs, rhs) => {
            Ok(label_filter(row, &lhs.value)? || label_filter(row, &rhs.value)?)
        }
        LabelFilterExpr::Predicate(predicate) => {
            let actual = lookup(row, &predicate.label).unwrap_or("");
            compare_filter(actual, predicate.op, &predicate.value)
        }
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
        let matched = with_regex(RegexKind::Anchored, expected, |regex| {
            regex.is_match(actual)
        })?;
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
        FilterValue::Duration(value) => parse_duration_ns(actual)
            .ok()
            .and_then(|actual| actual.cmp(&parse_duration_ns(value).ok()?).into()),
        FilterValue::Ip(value) => {
            let matched = ip_matches(actual, value);
            return Ok(compare_bool(matched, op));
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

pub(super) fn label_format(row: &mut Row, assignments: &[FormatAssignment]) {
    for assignment in assignments {
        let value = if assignment.rename {
            let renamed = if row.labels.contains_key(&assignment.value) {
                Arc::make_mut(&mut row.labels).remove(&assignment.value)
            } else {
                None
            };
            renamed
                .or_else(|| row.metadata.remove(&assignment.value))
                .unwrap_or_default()
        } else {
            match render_template(&assignment.value, row) {
                Ok(value) => value,
                Err(error) => {
                    set_error(row, "TemplateFormatErr", &error.to_string());
                    continue;
                }
            }
        };
        Arc::make_mut(&mut row.labels).insert(assignment.label.clone(), value);
    }
}

pub(super) fn lookup<'a>(row: &'a Row, name: &str) -> Option<&'a str> {
    match name {
        "__line__" => Some(&row.line),
        _ => row
            .labels
            .get(name)
            .or_else(|| row.metadata.get(name))
            .map(String::as_str),
    }
}

pub(super) fn apply_unwrap(row: &mut Row, unwrap: &Unwrap) -> Result<bool> {
    let source = lookup(row, &unwrap.label).unwrap_or("").to_owned();
    let parsed = match unwrap.conversion {
        None => source.parse::<f64>().ok(),
        Some(Conversion::Bytes) => parse_bytes(&source).ok(),
        Some(Conversion::Duration) => parse_duration_ns(&source).ok().map(|value| value as f64),
        Some(Conversion::DurationSeconds) => parse_duration_ns(&source)
            .ok()
            .map(|value| value as f64 / 1_000_000_000.0),
    };
    match parsed {
        Some(value) => row.value = Some(value),
        None => set_error(row, "SampleExtractionErr", "unable to convert unwrap value"),
    }
    unwrap
        .post_filter
        .as_ref()
        .map_or(Ok(true), |filter| label_filter(row, &filter.value))
}
