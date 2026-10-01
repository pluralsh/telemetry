use super::*;

pub(super) fn render_template(template: &str, row: &Row) -> Result<String> {
    render_template_block(template, row)
}

/// Decodes the body of a Go interpreted string literal, which template
/// strings use: JSON's escapes plus `\a`, `\v`, `\'`, `\xHH`, octal `\ooo`
/// and `\UXXXXXXXX`.
fn go_unquote(body: &str) -> Result<String> {
    let invalid = || Error::Query(format!("invalid template string {body:?}"));
    let mut output = Vec::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            let mut buffer = [0; 4];
            output.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        let escaped = chars.next().ok_or_else(invalid)?;
        let mut digits = |count: usize, radix: u32| -> Result<u32> {
            let text: String = chars.by_ref().take(count).collect();
            if text.len() != count {
                return Err(invalid());
            }
            u32::from_str_radix(&text, radix).map_err(|_| invalid())
        };
        match escaped {
            'a' => output.push(0x07),
            'b' => output.push(0x08),
            'f' => output.push(0x0c),
            'n' => output.push(b'\n'),
            'r' => output.push(b'\r'),
            't' => output.push(b'\t'),
            'v' => output.push(0x0b),
            '\\' | '"' | '\'' => output.push(escaped as u8),
            'x' => output.push(digits(2, 16)? as u8),
            '0'..='7' => {
                let rest = digits(2, 8)?;
                let value = escaped.to_digit(8).expect("octal digit") * 64 + rest;
                output.push(u8::try_from(value).map_err(|_| invalid())?);
            }
            'u' | 'U' => {
                let value = digits(if escaped == 'u' { 4 } else { 8 }, 16)?;
                let character = char::from_u32(value).ok_or_else(invalid)?;
                let mut buffer = [0; 4];
                output.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            }
            _ => return Err(invalid()),
        }
    }
    Ok(String::from_utf8_lossy(&output).into_owned())
}

#[derive(Clone, Debug)]
pub(super) enum TemplateValue {
    String(String),
    Number(f64),
    Bool(bool),
    Time(i64),
}

impl TemplateValue {
    fn text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Number(value) => format_number(*value),
            Self::Bool(value) => value.to_string(),
            Self::Time(value) => Utc
                .timestamp_nanos(*value)
                .format("%Y-%m-%d %H:%M:%S%.f %z %Z")
                .to_string(),
        }
    }

    fn number(&self) -> Result<f64> {
        match self {
            Self::Number(value) => Ok(*value),
            Self::String(value) => value
                .parse()
                .map_err(|_| Error::Query(format!("template value {value:?} is not numeric"))),
            Self::Bool(_) | Self::Time(_) => {
                Err(Error::Query("template value is not numeric".into()))
            }
        }
    }

    fn truthy(&self) -> bool {
        match self {
            Self::String(value) => !value.is_empty(),
            Self::Number(value) => *value != 0.0,
            Self::Bool(value) => *value,
            Self::Time(_) => true,
        }
    }
}

/// Every function a template may call. Anything else is rejected before the
/// query runs, as Go's template parser does, rather than failing per line.
const TEMPLATE_FUNCTIONS: &[&str] = &[
    "Replace",
    "ToLower",
    "ToUpper",
    "Trim",
    "TrimLeft",
    "TrimPrefix",
    "TrimRight",
    "TrimSpace",
    "TrimSuffix",
    "add",
    "addf",
    "alignLeft",
    "alignRight",
    "b64dec",
    "b64enc",
    "bytes",
    "ceil",
    "contains",
    "count",
    "date",
    "default",
    "div",
    "divf",
    "duration",
    "duration_seconds",
    "float64",
    "floor",
    "hasPrefix",
    "hasSuffix",
    "indent",
    "int",
    "lower",
    "max",
    "maxf",
    "min",
    "minf",
    "mod",
    "mul",
    "mulf",
    "nindent",
    "printf",
    "regexReplaceAll",
    "regexReplaceAllLiteral",
    "repeat",
    "replace",
    "round",
    "sub",
    "subf",
    "substr",
    "title",
    "toDate",
    "toDateInZone",
    "trim",
    "trimAll",
    "trimPrefix",
    "trimSuffix",
    "trunc",
    "unixEpoch",
    "unixEpochMillis",
    "unixEpochNanos",
    "unixToTime",
    "upper",
    "urldecode",
    "urlencode",
];

const TEMPLATE_IDENTIFIERS: &[&str] = &["__line__", "__timestamp__", "now", "true", "false"];

/// Rejects templates that call a function the engine does not implement.
pub(super) fn check_template(template: &str) -> Result<()> {
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        let action_start = start + 2;
        let end = rest[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = rest[action_start..end].trim();
        rest = &rest[end + 2..];
        if action != "else" && action != "end" {
            check_template_expression(action.strip_prefix("if ").unwrap_or(action))?;
        }
    }
    Ok(())
}

fn check_template_expression(expression: &str) -> Result<()> {
    for command in split_top_level(expression, '|')? {
        let command = command.trim();
        let tokens = template_tokens(command)?;
        for token in &tokens {
            if let TemplateValue::String(token) = token
                && let Some(inner) = token.strip_prefix("\0expr:")
            {
                check_template_expression(inner)?;
            }
        }
        let starts_word = command
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_');
        let Some(TemplateValue::String(name)) = tokens.first() else {
            continue;
        };
        if starts_word
            && !TEMPLATE_FUNCTIONS.contains(&name.as_str())
            && !TEMPLATE_IDENTIFIERS.contains(&name.as_str())
        {
            return Err(Error::Query(format!(
                "template: function {name:?} not defined"
            )));
        }
    }
    Ok(())
}

pub(super) fn render_template_block(template: &str, row: &Row) -> Result<String> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let action_start = start + 2;
        let end = rest[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = rest[action_start..end].trim();
        rest = &rest[end + 2..];
        if let Some(condition) = action.strip_prefix("if ") {
            let (body, tail) = template_branch(rest)?;
            let (yes, no) = split_template_else(body)?;
            let selected = if eval_template_expression(condition, row)?.truthy() {
                yes
            } else {
                no
            };
            output.push_str(&render_template_block(selected, row)?);
            rest = tail;
        } else if action == "else" || action == "end" {
            return Err(Error::Query(format!(
                "unexpected template action {action:?}"
            )));
        } else {
            output.push_str(&eval_template_expression(action, row)?.text());
        }
    }
    output.push_str(rest);
    Ok(output)
}

pub(super) fn template_branch(input: &str) -> Result<(&str, &str)> {
    let mut depth = 1usize;
    let mut cursor = 0usize;
    while let Some(start) = input[cursor..].find("{{") {
        let start = cursor + start;
        let action_start = start + 2;
        let end = input[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = input[action_start..end].trim();
        if action.starts_with("if ") {
            depth += 1;
        } else if action == "end" {
            depth -= 1;
            if depth == 0 {
                return Ok((&input[..start], &input[end + 2..]));
            }
        }
        cursor = end + 2;
    }
    Err(Error::Query("template if has no end".into()))
}

pub(super) fn split_template_else(body: &str) -> Result<(&str, &str)> {
    let mut depth = 0usize;
    let mut cursor = 0usize;
    while let Some(start) = body[cursor..].find("{{") {
        let start = cursor + start;
        let action_start = start + 2;
        let end = body[action_start..]
            .find("}}")
            .map(|end| action_start + end)
            .ok_or_else(|| Error::Query("unterminated template action".into()))?;
        let action = body[action_start..end].trim();
        if action.starts_with("if ") {
            depth += 1;
        } else if action == "end" {
            depth = depth.saturating_sub(1);
        } else if action == "else" && depth == 0 {
            return Ok((&body[..start], &body[end + 2..]));
        }
        cursor = end + 2;
    }
    Ok((body, ""))
}

pub(super) fn eval_template_expression(expression: &str, row: &Row) -> Result<TemplateValue> {
    let commands = split_top_level(expression, '|')?;
    let mut value = None;
    for command in commands {
        let mut tokens = template_tokens(command.trim())?;
        if let Some(previous) = value.take() {
            tokens.push(previous);
        }
        value = Some(eval_template_command(&tokens, row)?);
    }
    value.ok_or_else(|| Error::Query("empty template action".into()))
}

pub(super) fn split_top_level(input: &str, separator: char) -> Result<Vec<&str>> {
    let mut result = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::Query("unbalanced template parentheses".into()))?;
            }
            value if value == separator && !quoted && depth == 0 => {
                result.push(&input[start..index]);
                start = index + value.len_utf8();
            }
            _ => {}
        }
    }
    if quoted || depth != 0 {
        return Err(Error::Query("unbalanced template expression".into()));
    }
    result.push(&input[start..]);
    Ok(result)
}

pub(super) fn template_tokens(command: &str) -> Result<Vec<TemplateValue>> {
    let bytes = command.as_bytes();
    let mut result = Vec::new();
    let mut cursor = skip_ascii_whitespace(bytes, 0);
    while cursor < command.len() {
        let (token, end) = match bytes[cursor] {
            b'"' => {
                let end = quoted_end(bytes, cursor + 1);
                if bytes[end - 1] != b'"' {
                    return Err(Error::Query("unterminated template string".into()));
                }
                (go_unquote(&command[cursor + 1..end - 1])?, end)
            }
            b'(' => {
                let end = paren_end(bytes, cursor + 1)
                    .ok_or_else(|| Error::Query("unbalanced template parentheses".into()))?;
                (format!("\0expr:{}", &command[cursor + 1..end - 1]), end)
            }
            b')' => return Err(Error::Query("unbalanced template parentheses".into())),
            _ => {
                let word = bytes[cursor..]
                    .iter()
                    .take_while(|b| !b.is_ascii_whitespace() && !matches!(b, b'(' | b')'))
                    .count();
                let end = cursor + word;
                (command[cursor..end].to_owned(), end)
            }
        };
        result.push(TemplateValue::String(token));
        cursor = skip_ascii_whitespace(bytes, end);
    }
    Ok(result)
}

/// Returns the position just past the `)` closing a group whose body starts
/// at `cursor`, or `None` if the group is unbalanced.
fn paren_end(bytes: &[u8], mut cursor: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut quoted = false;
    while depth != 0 {
        match *bytes.get(cursor)? {
            b'"' => quoted = !quoted,
            b'(' if !quoted => depth += 1,
            b')' if !quoted => depth -= 1,
            _ => {}
        }
        cursor += 1;
    }
    Some(cursor)
}

pub(super) fn eval_template_command(tokens: &[TemplateValue], row: &Row) -> Result<TemplateValue> {
    let Some(first) = tokens.first() else {
        return Err(Error::Query("empty template command".into()));
    };
    let name = first.text();
    if tokens.len() == 1 {
        return resolve_template_argument(first, row);
    }
    let arguments = tokens[1..]
        .iter()
        .map(|argument| resolve_template_argument(argument, row))
        .collect::<Result<Vec<_>>>()?;
    apply_template_function(&name, &arguments)
}

pub(super) fn resolve_template_argument(value: &TemplateValue, row: &Row) -> Result<TemplateValue> {
    let TemplateValue::String(value) = value else {
        return Ok(value.clone());
    };
    if let Some(expression) = value.strip_prefix("\0expr:") {
        return eval_template_expression(expression, row);
    }
    if let Some(name) = value.strip_prefix('.') {
        return Ok(TemplateValue::String(
            lookup(row, name).unwrap_or("").to_owned(),
        ));
    }
    match value.as_str() {
        "__line__" => Ok(TemplateValue::String(row.line.clone())),
        "__timestamp__" => Ok(TemplateValue::Time(row.timestamp_ns)),
        "now" => Ok(TemplateValue::Time(
            Utc::now().timestamp_nanos_opt().unwrap_or(0),
        )),
        "true" => Ok(TemplateValue::Bool(true)),
        "false" => Ok(TemplateValue::Bool(false)),
        _ => value
            .parse::<f64>()
            .map(TemplateValue::Number)
            .or_else(|_| Ok(TemplateValue::String(value.clone()))),
    }
}

type TemplateFunction = fn(&str, &[TemplateValue]) -> Result<Option<TemplateValue>>;

pub(super) fn apply_template_function(
    name: &str,
    arguments: &[TemplateValue],
) -> Result<TemplateValue> {
    const FUNCTIONS: [TemplateFunction; 4] = [
        string_function,
        numeric_function,
        encoding_function,
        time_function,
    ];
    for function in FUNCTIONS {
        if let Some(value) = function(name, arguments)? {
            return Ok(value);
        }
    }
    Err(Error::Query(format!(
        "unknown or unsupported template function {name:?}"
    )))
}

/// Go's `fmt.Sprintf` for the plain verbs `%s %v %d %f %q %%`; flags and
/// widths are rejected rather than misformatted.
fn printf(format: &str, arguments: &[TemplateValue]) -> Result<String> {
    let mut output = String::with_capacity(format.len());
    let mut arguments = arguments.iter();
    let mut chars = format.chars();
    while let Some(character) = chars.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        let verb = chars
            .next()
            .ok_or_else(|| Error::Query("printf format ends with %".into()))?;
        if verb == '%' {
            output.push('%');
            continue;
        }
        let argument = arguments
            .next()
            .ok_or_else(|| Error::Query(format!("printf %{verb} is missing an argument")))?;
        match verb {
            's' | 'v' => output.push_str(&argument.text()),
            'd' => output.push_str(&format!("{}", argument.number()?.trunc() as i64)),
            'f' => output.push_str(&format!("{:.6}", argument.number()?)),
            'q' => output.push_str(&serde_json::to_string(&argument.text())?),
            _ => {
                return Err(Error::Query(format!("unsupported printf verb %{verb}")));
            }
        }
    }
    Ok(output)
}

/// Go's `strings` functions take the source first; their Sprig counterparts
/// take it last. Returns `(source, operand)`.
fn source_and_operand(arguments: &[TemplateValue], source_first: bool) -> (String, String) {
    let (source, operand) = if source_first { (0, 1) } else { (1, 0) };
    (arguments[source].text(), arguments[operand].text())
}

fn string_function(name: &str, arguments: &[TemplateValue]) -> Result<Option<TemplateValue>> {
    let value = match name {
        "ToLower" | "lower" => unary_string(arguments, str::to_lowercase)?,
        "ToUpper" | "upper" => unary_string(arguments, str::to_uppercase)?,
        "title" => unary_string(arguments, title_case)?,
        "TrimSpace" | "trim" => unary_string(arguments, |value| value.trim().to_owned())?,
        "Trim" | "trimAll" | "TrimLeft" | "TrimRight" => {
            require_args(name, arguments, 2)?;
            let (source, cutset) = source_and_operand(arguments, name != "trimAll");
            let cut = |c| cutset.contains(c);
            let trimmed = match name {
                "TrimLeft" => source.trim_start_matches(cut),
                "TrimRight" => source.trim_end_matches(cut),
                _ => source.trim_matches(cut),
            };
            TemplateValue::String(trimmed.to_owned())
        }
        "TrimPrefix" | "trimPrefix" | "TrimSuffix" | "trimSuffix" => {
            require_args(name, arguments, 2)?;
            let (source, affix) = source_and_operand(arguments, name.starts_with('T'));
            let stripped = if name.ends_with("Prefix") {
                source.strip_prefix(&affix)
            } else {
                source.strip_suffix(&affix)
            };
            TemplateValue::String(stripped.unwrap_or(&source).to_owned())
        }
        "printf" if !arguments.is_empty() => {
            TemplateValue::String(printf(&arguments[0].text(), &arguments[1..])?)
        }
        "replace" => {
            require_args(name, arguments, 3)?;
            TemplateValue::String(
                arguments[2]
                    .text()
                    .replace(&arguments[0].text(), &arguments[1].text()),
            )
        }
        "Replace" => {
            require_args(name, arguments, 4)?;
            let count = arguments[3].number()? as isize;
            let source = arguments[0].text();
            TemplateValue::String(if count < 0 {
                source.replace(&arguments[1].text(), &arguments[2].text())
            } else {
                source.replacen(&arguments[1].text(), &arguments[2].text(), count as usize)
            })
        }
        "contains" | "hasPrefix" | "hasSuffix" => {
            require_args(name, arguments, 2)?;
            let (source, needle) = source_and_operand(arguments, false);
            TemplateValue::Bool(match name {
                "contains" => source.contains(&needle),
                "hasPrefix" => source.starts_with(&needle),
                _ => source.ends_with(&needle),
            })
        }
        "repeat" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].number()?.max(0.0) as usize;
            TemplateValue::String(arguments[1].text().repeat(count))
        }
        "trunc" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].number()? as isize;
            let chars = arguments[1].text().chars().collect::<Vec<_>>();
            let slice = if count < 0 {
                &chars[chars.len().saturating_sub(count.unsigned_abs())..]
            } else {
                &chars[..chars.len().min(count as usize)]
            };
            TemplateValue::String(slice.iter().collect())
        }
        "substr" => {
            require_args(name, arguments, 3)?;
            let chars = arguments[2].text().chars().collect::<Vec<_>>();
            let start = (arguments[0].number()? as usize).min(chars.len());
            let end = if arguments[1].number()? < 0.0 {
                chars.len()
            } else {
                (arguments[1].number()? as usize).min(chars.len())
            };
            TemplateValue::String(chars[start.min(end)..end].iter().collect())
        }
        "indent" | "nindent" => {
            require_args(name, arguments, 2)?;
            let padding = " ".repeat(arguments[0].number()?.max(0.0) as usize);
            let value = arguments[1].text().replace('\n', &format!("\n{padding}"));
            TemplateValue::String(if name == "nindent" {
                format!("\n{padding}{value}")
            } else {
                format!("{padding}{value}")
            })
        }
        "alignLeft" | "alignRight" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].number()?.max(0.0) as usize;
            let mut chars = arguments[1].text().chars().collect::<Vec<_>>();
            if chars.len() > count {
                chars = if name == "alignLeft" {
                    chars[..count].to_vec()
                } else {
                    chars[chars.len() - count..].to_vec()
                };
            }
            let value = chars.iter().collect::<String>();
            TemplateValue::String(if name == "alignLeft" {
                format!("{value:<count$}")
            } else {
                format!("{value:>count$}")
            })
        }
        "default" => {
            require_args(name, arguments, 2)?;
            if arguments[1].truthy() {
                arguments[1].clone()
            } else {
                arguments[0].clone()
            }
        }
        "count" => {
            require_args(name, arguments, 2)?;
            let haystack = arguments[1].text();
            let count = with_regex(RegexKind::Plain, &arguments[0].text(), |regex| {
                regex.find_iter(&haystack).count()
            })?;
            TemplateValue::Number(count as f64)
        }
        "regexReplaceAll" | "regexReplaceAllLiteral" => {
            require_args(name, arguments, 3)?;
            let (haystack, replacement) = (arguments[1].text(), arguments[2].text());
            TemplateValue::String(with_regex(
                RegexKind::Plain,
                &arguments[0].text(),
                |regex| {
                    if name == "regexReplaceAll" {
                        regex
                            .replace_all(&haystack, replacement.as_str())
                            .into_owned()
                    } else {
                        regex
                            .replace_all(&haystack, regex::NoExpand(&replacement))
                            .into_owned()
                    }
                },
            )?)
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn title_case(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn numeric_function(name: &str, arguments: &[TemplateValue]) -> Result<Option<TemplateValue>> {
    let numbers = || {
        arguments
            .iter()
            .map(TemplateValue::number)
            .collect::<Result<Vec<_>>>()
    };
    let value = match name {
        "int" | "float64" | "ceil" | "floor" => {
            require_args(name, arguments, 1)?;
            let value = arguments[0].number()?;
            match name {
                "int" => value.trunc(),
                "ceil" => value.ceil(),
                "floor" => value.floor(),
                _ => value,
            }
        }
        "add" | "sub" | "mul" | "div" | "mod" | "addf" | "subf" | "mulf" | "divf" => {
            let values = numbers()?;
            let (&first, rest) = values
                .split_first()
                .ok_or_else(|| Error::Query(format!("{name} requires arguments")))?;
            rest.iter().try_fold(first, |value, &next| {
                Ok::<_, Error>(match name {
                    "add" | "addf" => value + next,
                    "sub" | "subf" => value - next,
                    "mul" | "mulf" => value * next,
                    "div" => integer_op(name, value, next, i64::checked_div)?,
                    "mod" => integer_op(name, value, next, i64::checked_rem)?,
                    "divf" => value / next,
                    _ => unreachable!(),
                })
            })?
        }
        "min" | "minf" | "max" | "maxf" => numbers()?
            .into_iter()
            .reduce(if name.starts_with("min") {
                f64::min
            } else {
                f64::max
            })
            .ok_or_else(|| Error::Query(format!("{name} requires arguments")))?,
        "round" => {
            if arguments.is_empty() || arguments.len() > 3 {
                return Err(Error::Query("round expects one to three arguments".into()));
            }
            let precision = arguments
                .get(1)
                .map(TemplateValue::number)
                .transpose()?
                .unwrap_or(0.0) as i32;
            let factor = 10f64.powi(precision);
            (arguments[0].number()? * factor).round() / factor
        }
        "bytes" => {
            require_args(name, arguments, 1)?;
            parse_bytes(&arguments[0].text())?
        }
        "duration" | "duration_seconds" => {
            require_args(name, arguments, 1)?;
            parse_duration_ns(&arguments[0].text())? as f64 / 1_000_000_000.0
        }
        _ => return Ok(None),
    };
    Ok(Some(TemplateValue::Number(value)))
}

fn encoding_function(name: &str, arguments: &[TemplateValue]) -> Result<Option<TemplateValue>> {
    let value = match name {
        "b64enc" => {
            require_args(name, arguments, 1)?;
            base64::engine::general_purpose::STANDARD.encode(arguments[0].text())
        }
        "b64dec" => {
            require_args(name, arguments, 1)?;
            let mut input = arguments[0].text();
            if input.len() % 4 > 1 {
                input.extend(std::iter::repeat_n('=', 4 - input.len() % 4));
            }
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(input)
                .map_err(|error| Error::Query(error.to_string()))?;
            String::from_utf8_lossy(&decoded).into_owned()
        }
        "urlencode" => {
            require_args(name, arguments, 1)?;
            query_escape(&arguments[0].text())
        }
        "urldecode" => {
            require_args(name, arguments, 1)?;
            query_unescape(&arguments[0].text())?
        }
        _ => return Ok(None),
    };
    Ok(Some(TemplateValue::String(value)))
}

fn time_function(name: &str, arguments: &[TemplateValue]) -> Result<Option<TemplateValue>> {
    let value = match name {
        "unixEpoch" | "unixEpochMillis" | "unixEpochNanos" => {
            require_args(name, arguments, 1)?;
            let timestamp = template_timestamp(&arguments[0])?;
            TemplateValue::Number(
                match name {
                    "unixEpoch" => timestamp as f64 / 1_000_000_000.0,
                    "unixEpochMillis" => (timestamp / 1_000_000) as f64,
                    _ => timestamp as f64,
                }
                .trunc(),
            )
        }
        "unixToTime" => {
            require_args(name, arguments, 1)?;
            TemplateValue::Time(template_timestamp(&arguments[0])?)
        }
        "date" => {
            require_args(name, arguments, 2)?;
            let timestamp = template_timestamp(&arguments[1])?;
            TemplateValue::String(format_go_time(timestamp, &arguments[0].text()))
        }
        "toDate" | "toDateInZone" => {
            require_args(name, arguments, if name == "toDate" { 2 } else { 3 })?;
            let source = arguments.last().expect("required").text();
            TemplateValue::Time(parse_go_time(&source, &arguments[0].text())?)
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(super) fn unary_string(
    arguments: &[TemplateValue],
    function: impl FnOnce(&str) -> String,
) -> Result<TemplateValue> {
    require_args("string function", arguments, 1)?;
    Ok(TemplateValue::String(function(&arguments[0].text())))
}

pub(super) fn require_args(name: &str, arguments: &[TemplateValue], count: usize) -> Result<()> {
    if arguments.len() != count {
        Err(Error::Query(format!(
            "template function {name} expects {count} arguments, got {}",
            arguments.len()
        )))
    } else {
        Ok(())
    }
}

pub(super) fn template_timestamp(value: &TemplateValue) -> Result<i64> {
    match value {
        TemplateValue::Time(value) => Ok(*value),
        TemplateValue::String(value) => {
            let multiplier = match value.len() {
                5 => 86_400_000_000_000,
                10 => 1_000_000_000,
                13 => 1_000_000,
                16 => 1_000,
                19 => 1,
                _ => {
                    return Err(Error::Query(format!(
                        "cannot infer epoch unit for {value:?}"
                    )));
                }
            };
            value
                .parse::<i64>()
                .map(|value| value.saturating_mul(multiplier))
                .map_err(|error| Error::Query(error.to_string()))
        }
        _ => Err(Error::Query("template value is not a timestamp".into())),
    }
}

pub(super) fn format_go_time(timestamp_ns: i64, layout: &str) -> String {
    let format = layout
        .replace("2006", "%Y")
        .replace("01", "%m")
        .replace("02", "%d")
        .replace("15", "%H")
        .replace("04", "%M")
        .replace("05", "%S")
        .replace("MST", "%Z");
    Utc.timestamp_nanos(timestamp_ns)
        .format(&format)
        .to_string()
}

pub(super) fn parse_go_time(source: &str, layout: &str) -> Result<i64> {
    if layout.contains('Z') {
        return DateTime::parse_from_rfc3339(source)
            .map(|value| value.timestamp_nanos_opt().unwrap_or(0))
            .map_err(|error| Error::Query(error.to_string()));
    }
    let format = layout
        .replace("2006", "%Y")
        .replace("01", "%m")
        .replace("02", "%d")
        .replace("15", "%H")
        .replace("04", "%M")
        .replace("05", "%S");
    chrono::NaiveDateTime::parse_from_str(source, &format)
        .map(|value| value.and_utc().timestamp_nanos_opt().unwrap_or(0))
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(source, &format).map(|value| {
                value
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight")
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
            })
        })
        .map_err(|error| Error::Query(error.to_string()))
}

pub(super) fn query_escape(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                output.push(char::from(byte));
            }
            b' ' => output.push('+'),
            _ => output.push_str(&format!("%{byte:02X}")),
        }
    }
    output
}

pub(super) fn query_unescape(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = input
                    .next()
                    .ok_or_else(|| Error::Query("truncated URL escape".into()))?;
                let low = input
                    .next()
                    .ok_or_else(|| Error::Query("truncated URL escape".into()))?;
                let hex = [high, low];
                bytes.push(
                    u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or(""), 16)
                        .map_err(|error| Error::Query(error.to_string()))?,
                );
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8(bytes).map_err(|error| Error::Query(error.to_string()))
}

pub(super) fn format_number(value: f64) -> String {
    if value.abs() >= 1_000_000.0 {
        let scientific = format!("{value:.0e}");
        let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
        let exponent = exponent.parse::<i32>().expect("numeric exponent");
        format!("{mantissa}e{exponent:+03}")
    } else if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

/// Integer `div`/`mod` over truncated operands; zero divisors and overflow are query errors.
fn integer_op(
    name: &str,
    lhs: f64,
    rhs: f64,
    op: impl FnOnce(i64, i64) -> Option<i64>,
) -> Result<f64> {
    op(lhs as i64, rhs as i64)
        .map(|value| value as f64)
        .ok_or_else(|| Error::Query(format!("{name}: division by zero or overflow")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, arguments: &[&str]) -> Result<TemplateValue> {
        let arguments = arguments
            .iter()
            .map(|value| TemplateValue::String((*value).to_owned()))
            .collect::<Vec<_>>();
        apply_template_function(name, &arguments)
    }

    fn text(name: &str, arguments: &[&str]) -> String {
        call(name, arguments).unwrap().text()
    }

    fn number(name: &str, arguments: &[&str]) -> f64 {
        match call(name, arguments).unwrap() {
            TemplateValue::Number(value) => value,
            other => panic!("{name} returned {other:?}"),
        }
    }

    #[test]
    fn template_tokens_split_strings_groups_and_words() {
        let tokens = template_tokens(r#" printf "%s \"q\"" (upper .a) .b "#).unwrap();
        let texts: Vec<_> = tokens.iter().map(TemplateValue::text).collect();
        assert_eq!(texts, ["printf", "%s \"q\"", "\0expr:upper .a", ".b"]);
        assert!(template_tokens(r#"printf "open"#).is_err());
        assert!(template_tokens("(upper .a").is_err());
        assert!(template_tokens("upper )").is_err());
    }

    #[test]
    fn integer_division_by_zero_is_a_query_error() {
        assert!(call("div", &["7", "0"]).is_err());
        assert!(call("mod", &["7", "0"]).is_err());
        assert_eq!(number("divf", &["7", "0"]), f64::INFINITY);
    }

    #[test]
    fn go_string_functions_take_source_first_and_sprig_last() {
        assert_eq!(text("Trim", &["xxhixx", "x"]), "hi");
        assert_eq!(text("trimAll", &["x", "xxhixx"]), "hi");
        assert_eq!(text("TrimLeft", &["xxhixx", "x"]), "hixx");
        assert_eq!(text("TrimRight", &["xxhixx", "x"]), "xxhi");
        assert_eq!(text("TrimPrefix", &["pre-body", "pre-"]), "body");
        assert_eq!(text("trimPrefix", &["pre-", "pre-body"]), "body");
        assert_eq!(text("TrimSuffix", &["body.log", ".log"]), "body");
        assert_eq!(text("trimSuffix", &[".log", "body.log"]), "body");
        assert_eq!(text("TrimPrefix", &["body", "x"]), "body");
        assert_eq!(text("contains", &["ell", "hello"]), "true");
        assert_eq!(text("hasPrefix", &["he", "hello"]), "true");
        assert_eq!(text("hasSuffix", &["he", "hello"]), "false");
    }

    #[test]
    fn string_functions() {
        assert_eq!(text("title", &["hello  world"]), "Hello World");
        assert_eq!(text("upper", &["abc"]), "ABC");
        assert_eq!(text("replace", &["a", "b", "banana"]), "bbnbnb");
        assert_eq!(text("Replace", &["banana", "a", "o", "2"]), "bonona");
        assert_eq!(text("trunc", &["-3", "abcdef"]), "def");
        assert_eq!(text("substr", &["1", "3", "abcdef"]), "bc");
        assert_eq!(text("nindent", &["2", "a\nb"]), "\n  a\n  b");
        assert_eq!(text("alignRight", &["4", "ab"]), "  ab");
        assert_eq!(text("default", &["fallback", ""]), "fallback");
        assert_eq!(text("default", &["fallback", "set"]), "set");
        assert_eq!(text("regexReplaceAll", &["l+", "hello", "L"]), "heLo");
        assert_eq!(number("count", &["l", "hello"]), 2.0);
    }

    #[test]
    fn numeric_functions() {
        assert_eq!(number("add", &["1", "2", "3"]), 6.0);
        assert_eq!(number("div", &["7", "2"]), 3.0);
        assert_eq!(number("divf", &["7", "2"]), 3.5);
        assert_eq!(number("mod", &["7", "2"]), 1.0);
        assert_eq!(number("max", &["1", "5", "3"]), 5.0);
        assert_eq!(number("int", &["3.7"]), 3.0);
        assert_eq!(number("ceil", &["1.2"]), 2.0);
        assert_eq!(number("floor", &["1.8"]), 1.0);
        assert_eq!(number("round", &["1.256", "2"]), 1.26);
        assert_eq!(number("bytes", &["2KiB"]), 2048.0);
        assert_eq!(number("duration", &["1m30s"]), 90.0);
        assert!(call("add", &[]).is_err());
    }

    #[test]
    fn encoding_and_time_functions() {
        assert_eq!(text("b64enc", &["hi"]), "aGk=");
        assert_eq!(text("b64dec", &["aGk"]), "hi");
        assert_eq!(text("urlencode", &["a b&c"]), "a+b%26c");
        assert_eq!(text("urldecode", &["a+b%26c"]), "a b&c");
        let time = [TemplateValue::Time(1_500_000_000_123_456_789)];
        let epoch = |name| match apply_template_function(name, &time).unwrap() {
            TemplateValue::Number(value) => value,
            other => panic!("{name} returned {other:?}"),
        };
        assert_eq!(epoch("unixEpoch"), 1_500_000_000.0);
        assert_eq!(epoch("unixEpochMillis"), 1_500_000_000_123.0);
    }

    #[test]
    fn rejects_unknown_functions_and_bad_arity() {
        assert!(call("nope", &["x"]).is_err());
        assert!(call("Trim", &["x"]).is_err());
    }

    #[test]
    fn declared_functions_are_implemented() {
        let arguments = vec![TemplateValue::String("1".into()); 4];
        for name in TEMPLATE_FUNCTIONS {
            let unknown = (0..=arguments.len()).all(|count| {
                matches!(
                    apply_template_function(name, &arguments[..count]),
                    Err(Error::Query(message)) if message.starts_with("unknown or unsupported")
                )
            });
            assert!(!unknown, "{name} is declared but not implemented");
        }
    }

    #[test]
    fn templates_are_checked_before_rendering() {
        assert!(check_template("{{ nosuchfunc }}").is_err());
        assert!(check_template("{{ .a | nosuchfunc }}").is_err());
        assert!(check_template("{{ upper (nosuchfunc .a) }}").is_err());
        assert!(
            check_template(r#"{{ if .a }}{{ printf "%s" .a | upper }}{{ else }}x{{ end }}"#)
                .is_ok()
        );
        assert!(check_template("{{ __line__ }} {{ .a }} {{ 1 }}").is_ok());
    }

    #[test]
    fn printf_and_go_escapes() {
        assert_eq!(
            go_unquote(r"\x1b[0m\t\u00e9\101").unwrap(),
            "\x1b[0m\té\x41"
        );
        assert_eq!(
            printf(
                "%s=%d %q%%",
                &[
                    TemplateValue::String("a".into()),
                    TemplateValue::Number(2.7),
                    TemplateValue::String("b".into()),
                ]
            )
            .unwrap(),
            "a=2 \"b\"%"
        );
    }
}
