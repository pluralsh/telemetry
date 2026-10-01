//! Loki's template function library: Go `strings` functions, the Sprig
//! subset Loki enables, and Loki's own additions. Both template syntaxes call
//! these; integer-valued Sprig functions return `Int` and the `f` variants
//! return `Float`, as Sprig's `int64` and `float64` results do.

use std::rc::Rc;

use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};

use super::{Value, ensure_output_len};
use crate::query::{RegexKind, parse_bytes, parse_duration_ns, with_regex};
use crate::{Error, Result};

/// Every library function, as Loki registers them.
pub(super) const FUNCTIONS: &[&str] = &[
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
    "fromJson",
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
    "now",
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

type Function = fn(&str, &[Value]) -> Result<Option<Value>>;

pub(super) fn call(name: &str, arguments: &[Value]) -> Result<Value> {
    const GROUPS: [Function; 4] = [
        string_function,
        numeric_function,
        encoding_function,
        time_function,
    ];
    for group in GROUPS {
        if let Some(value) = group(name, arguments)? {
            return Ok(value);
        }
    }
    Err(Error::Query(format!(
        "unknown or unsupported template function {name:?}"
    )))
}

/// Go's `strings` functions take the source first; their Sprig counterparts
/// take it last. Returns `(source, operand)`.
fn source_and_operand(arguments: &[Value], source_first: bool) -> (String, String) {
    let (source, operand) = if source_first { (0, 1) } else { (1, 0) };
    (arguments[source].text(), arguments[operand].text())
}

fn string_function(name: &str, arguments: &[Value]) -> Result<Option<Value>> {
    let value = match name {
        "ToLower" | "lower" => unary_string(name, arguments, str::to_lowercase)?,
        "ToUpper" | "upper" => unary_string(name, arguments, str::to_uppercase)?,
        "title" => unary_string(name, arguments, title_case)?,
        "TrimSpace" | "trim" => unary_string(name, arguments, |value| value.trim().to_owned())?,
        "Trim" | "trimAll" | "TrimLeft" | "TrimRight" => {
            require_args(name, arguments, 2)?;
            let (source, cutset) = source_and_operand(arguments, name != "trimAll");
            let cut = |c| cutset.contains(c);
            let trimmed = match name {
                "TrimLeft" => source.trim_start_matches(cut),
                "TrimRight" => source.trim_end_matches(cut),
                _ => source.trim_matches(cut),
            };
            Value::String(trimmed.to_owned())
        }
        "TrimPrefix" | "trimPrefix" | "TrimSuffix" | "trimSuffix" => {
            require_args(name, arguments, 2)?;
            let (source, affix) = source_and_operand(arguments, name.starts_with('T'));
            let stripped = if name.ends_with("Prefix") {
                source.strip_prefix(&affix)
            } else {
                source.strip_suffix(&affix)
            };
            Value::String(stripped.unwrap_or(&source).to_owned())
        }
        "replace" => {
            require_args(name, arguments, 3)?;
            Value::String(
                arguments[2]
                    .text()
                    .replace(&arguments[0].text(), &arguments[1].text()),
            )
        }
        "Replace" => {
            require_args(name, arguments, 4)?;
            let count = arguments[3].int()?;
            let source = arguments[0].text();
            Value::String(if count < 0 {
                source.replace(&arguments[1].text(), &arguments[2].text())
            } else {
                source.replacen(&arguments[1].text(), &arguments[2].text(), count as usize)
            })
        }
        "contains" | "hasPrefix" | "hasSuffix" => {
            require_args(name, arguments, 2)?;
            let (source, needle) = source_and_operand(arguments, false);
            Value::Bool(match name {
                "contains" => source.contains(&needle),
                "hasPrefix" => source.starts_with(&needle),
                _ => source.ends_with(&needle),
            })
        }
        "repeat" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].int()?.max(0) as usize;
            let source = arguments[1].text();
            ensure_output_len(source.len().saturating_mul(count))?;
            Value::String(source.repeat(count))
        }
        "trunc" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].int()?;
            let chars = arguments[1].text().chars().collect::<Vec<_>>();
            let slice = if count < 0 {
                &chars[chars.len().saturating_sub(count.unsigned_abs() as usize)..]
            } else {
                &chars[..chars.len().min(count as usize)]
            };
            Value::String(slice.iter().collect())
        }
        "substr" => {
            require_args(name, arguments, 3)?;
            let chars = arguments[2].text().chars().collect::<Vec<_>>();
            let start = arguments[0].int()?.max(0) as usize;
            let end = arguments[1].int()?;
            let end = if end < 0 {
                chars.len()
            } else {
                (end as usize).min(chars.len())
            };
            Value::String(chars[start.min(end)..end].iter().collect())
        }
        "indent" | "nindent" => {
            require_args(name, arguments, 2)?;
            let width = arguments[0].int()?.max(0) as usize;
            ensure_output_len(width)?;
            let padding = " ".repeat(width);
            let value = arguments[1].text().replace('\n', &format!("\n{padding}"));
            Value::String(if name == "nindent" {
                format!("\n{padding}{value}")
            } else {
                format!("{padding}{value}")
            })
        }
        "alignLeft" | "alignRight" => {
            require_args(name, arguments, 2)?;
            let count = arguments[0].int()?.max(0) as usize;
            ensure_output_len(count)?;
            let mut chars = arguments[1].text().chars().collect::<Vec<_>>();
            if chars.len() > count {
                chars = if name == "alignLeft" {
                    chars[..count].to_vec()
                } else {
                    chars[chars.len() - count..].to_vec()
                };
            }
            let value = chars.iter().collect::<String>();
            Value::String(if name == "alignLeft" {
                format!("{value:<count$}")
            } else {
                format!("{value:>count$}")
            })
        }
        // Sprig's `default`, which a pipeline usually calls with the tested
        // value appended; without it the default applies.
        "default" => match arguments {
            [fallback] => fallback.clone(),
            [fallback, given] => {
                if given.truthy() {
                    given.clone()
                } else {
                    fallback.clone()
                }
            }
            _ => return Err(arity_error(name, 2, arguments.len())),
        },
        "count" => {
            require_args(name, arguments, 2)?;
            let haystack = arguments[1].text();
            let count = with_regex(RegexKind::Plain, &arguments[0].text(), |regex| {
                regex.find_iter(&haystack).count()
            })?;
            Value::Int(count as i64)
        }
        "regexReplaceAll" | "regexReplaceAllLiteral" => {
            require_args(name, arguments, 3)?;
            let (haystack, replacement) = (arguments[1].text(), arguments[2].text());
            Value::String(with_regex(
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
        "fromJson" => {
            require_args(name, arguments, 1)?;
            // Sprig's `fromJson` yields nil for malformed input.
            match serde_json::from_str::<serde_json::Value>(&arguments[0].text()) {
                Ok(value @ (serde_json::Value::Array(_) | serde_json::Value::Object(_))) => {
                    Value::Json(Rc::new(value))
                }
                Ok(value) => Value::from_json(&value),
                Err(_) => Value::Nil,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// Go's `strings.Title`: upper-cases each letter that follows a separator.
fn title_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut previous = ' ';
    for character in value.chars() {
        let separator = !(previous.is_alphanumeric() || previous == '_');
        if separator {
            output.extend(character.to_uppercase());
        } else {
            output.push(character);
        }
        previous = character;
    }
    output
}

fn numeric_function(name: &str, arguments: &[Value]) -> Result<Option<Value>> {
    let value = match name {
        "int" => {
            require_args(name, arguments, 1)?;
            Value::Int(arguments[0].int()?)
        }
        "float64" | "ceil" | "floor" => {
            require_args(name, arguments, 1)?;
            let value = arguments[0].float()?;
            Value::Float(match name {
                "ceil" => value.ceil(),
                "floor" => value.floor(),
                _ => value,
            })
        }
        "add" | "mul" | "sub" | "div" | "mod" | "max" | "min" => {
            let values = arguments
                .iter()
                .map(Value::int)
                .collect::<Result<Vec<_>>>()?;
            if matches!(name, "sub" | "div" | "mod") && values.len() != 2 {
                return Err(arity_error(name, 2, values.len()));
            }
            let Some((&first, rest)) = values.split_first() else {
                return Ok(Some(Value::Int(if name == "mul" { 1 } else { 0 })));
            };
            Value::Int(rest.iter().try_fold(first, |value, &next| {
                Ok::<_, Error>(match name {
                    "add" => value.wrapping_add(next),
                    "mul" => value.wrapping_mul(next),
                    "sub" => value.wrapping_sub(next),
                    "max" => value.max(next),
                    "min" => value.min(next),
                    "div" => value
                        .checked_div(next)
                        .ok_or_else(|| Error::Query("div: integer divide by zero".into()))?,
                    _ => value
                        .checked_rem(next)
                        .ok_or_else(|| Error::Query("mod: integer divide by zero".into()))?,
                })
            })?)
        }
        "addf" | "subf" | "mulf" | "divf" | "maxf" | "minf" => {
            let values = arguments
                .iter()
                .map(Value::float)
                .collect::<Result<Vec<_>>>()?;
            let Some((&first, rest)) = values.split_first() else {
                return Ok(Some(Value::Float(if name == "mulf" { 1.0 } else { 0.0 })));
            };
            Value::Float(rest.iter().fold(first, |value, &next| match name {
                "addf" => value + next,
                "subf" => value - next,
                "mulf" => value * next,
                "divf" => value / next,
                "maxf" => value.max(next),
                _ => value.min(next),
            }))
        }
        "round" => {
            if arguments.is_empty() || arguments.len() > 3 {
                return Err(Error::Query("round expects one to three arguments".into()));
            }
            let precision = arguments.get(1).map(Value::int).transpose()?.unwrap_or(0) as i32;
            let round_on = arguments
                .get(2)
                .map(Value::float)
                .transpose()?
                .unwrap_or(0.5);
            let factor = 10f64.powi(precision);
            let scaled = arguments[0].float()? * factor;
            // Sprig compares the signed fraction, so negatives round down.
            let rounded = if scaled.fract() >= round_on {
                scaled.ceil()
            } else {
                scaled.floor()
            };
            Value::Float(rounded / factor)
        }
        "bytes" => {
            require_args(name, arguments, 1)?;
            Value::Float(parse_bytes(&arguments[0].text())?)
        }
        "duration" | "duration_seconds" => {
            require_args(name, arguments, 1)?;
            Value::Float(parse_duration_ns(&arguments[0].text())? as f64 / 1_000_000_000.0)
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn encoding_function(name: &str, arguments: &[Value]) -> Result<Option<Value>> {
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
            // Loki renders a decoding failure as the error text.
            match base64::engine::general_purpose::STANDARD.decode(input) {
                Ok(decoded) => String::from_utf8_lossy(&decoded).into_owned(),
                Err(error) => error.to_string(),
            }
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
    Ok(Some(Value::String(value)))
}

fn time_function(name: &str, arguments: &[Value]) -> Result<Option<Value>> {
    let value = match name {
        "now" => {
            require_args(name, arguments, 0)?;
            Value::Time(Utc::now().timestamp_nanos_opt().unwrap_or(0))
        }
        "unixEpoch" | "unixEpochMillis" | "unixEpochNanos" => {
            require_args(name, arguments, 1)?;
            let timestamp = template_timestamp(&arguments[0])?;
            Value::String(
                match name {
                    "unixEpoch" => timestamp.div_euclid(1_000_000_000),
                    "unixEpochMillis" => timestamp.div_euclid(1_000_000),
                    _ => timestamp,
                }
                .to_string(),
            )
        }
        "unixToTime" => {
            require_args(name, arguments, 1)?;
            Value::Time(template_timestamp(&arguments[0])?)
        }
        "date" => {
            require_args(name, arguments, 2)?;
            let timestamp = match &arguments[1] {
                Value::Int(seconds) => seconds.saturating_mul(1_000_000_000),
                other => template_timestamp(other)?,
            };
            Value::String(format_go_time(timestamp, &arguments[0].text()))
        }
        "toDate" | "toDateInZone" => {
            require_args(name, arguments, if name == "toDate" { 2 } else { 3 })?;
            let source = arguments.last().expect("required").text();
            Value::Time(parse_go_time(&source, &arguments[0].text())?)
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

fn unary_string(
    name: &str,
    arguments: &[Value],
    function: impl FnOnce(&str) -> String,
) -> Result<Value> {
    require_args(name, arguments, 1)?;
    Ok(Value::String(function(&arguments[0].text())))
}

fn require_args(name: &str, arguments: &[Value], count: usize) -> Result<()> {
    if arguments.len() == count {
        Ok(())
    } else {
        Err(arity_error(name, count, arguments.len()))
    }
}

fn arity_error(name: &str, expected: usize, got: usize) -> Error {
    Error::Query(format!(
        "template function {name} expects {expected} arguments, got {got}"
    ))
}

fn template_timestamp(value: &Value) -> Result<i64> {
    match value {
        Value::Time(value) => Ok(*value),
        Value::String(value) => {
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
        other => Err(Error::Query(format!(
            "template value of type {} is not a timestamp",
            other.type_name()
        ))),
    }
}

fn format_go_time(timestamp_ns: i64, layout: &str) -> String {
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

fn parse_go_time(source: &str, layout: &str) -> Result<i64> {
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

/// Go's `url.QueryEscape`.
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

fn query_unescape(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let hex = [input.next(), input.next()];
                let [Some(high), Some(low)] = hex else {
                    return Err(Error::Query("truncated URL escape".into()));
                };
                bytes.push(
                    u8::from_str_radix(std::str::from_utf8(&[high, low]).unwrap_or(""), 16)
                        .map_err(|error| Error::Query(error.to_string()))?,
                );
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8(bytes).map_err(|error| Error::Query(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::String((*value).to_owned()))
            .collect()
    }

    fn text(name: &str, values: &[&str]) -> String {
        call(name, &arguments(values)).unwrap().text()
    }

    #[test]
    fn integer_functions_return_ints_and_f_variants_floats() {
        assert!(matches!(
            call("add", &arguments(&["1", "2", "3"])).unwrap(),
            Value::Int(6)
        ));
        assert!(matches!(
            call("div", &arguments(&["7", "2"])).unwrap(),
            Value::Int(3)
        ));
        assert!(matches!(
            call("mod", &arguments(&["7", "2"])).unwrap(),
            Value::Int(1)
        ));
        assert!(matches!(
            call("max", &arguments(&["1", "5", "3"])).unwrap(),
            Value::Int(5)
        ));
        assert!(matches!(
            call("int", &arguments(&["3.7"])).unwrap(),
            Value::Int(3)
        ));
        assert_eq!(text("divf", &["7", "2"]), "3.5");
        assert_eq!(text("addf", &["1", "2"]), "3");
        assert_eq!(text("ceil", &["1.2"]), "2");
        assert_eq!(text("floor", &["1.8"]), "1");
        assert_eq!(text("round", &["1.256", "2"]), "1.26");
        assert_eq!(text("round", &["-2.5"]), "-3");
        assert_eq!(text("bytes", &["2KiB"]), "2048");
        assert_eq!(text("bytes", &["2MB"]), "2e+06");
        assert_eq!(text("duration", &["1m30s"]), "90");
        assert_eq!(text("add", &[]), "0");
    }

    #[test]
    fn integer_division_by_zero_is_a_query_error() {
        assert!(call("div", &arguments(&["7", "0"])).is_err());
        assert!(call("mod", &arguments(&["7", "0"])).is_err());
        assert_eq!(text("divf", &["7", "0"]), "+Inf");
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
        assert_eq!(text("title", &["hello  world-wide"]), "Hello  World-Wide");
        assert_eq!(text("upper", &["abc"]), "ABC");
        assert_eq!(text("replace", &["a", "b", "banana"]), "bbnbnb");
        assert_eq!(text("Replace", &["banana", "a", "o", "2"]), "bonona");
        assert_eq!(text("trunc", &["-3", "abcdef"]), "def");
        assert_eq!(text("substr", &["1", "3", "abcdef"]), "bc");
        assert_eq!(text("nindent", &["2", "a\nb"]), "\n  a\n  b");
        assert_eq!(text("alignRight", &["4", "ab"]), "  ab");
        assert_eq!(text("default", &["fallback", ""]), "fallback");
        assert_eq!(text("default", &["fallback", "set"]), "set");
        assert_eq!(text("default", &["fallback"]), "fallback");
        assert_eq!(text("regexReplaceAll", &["l+", "hello", "L"]), "heLo");
        assert_eq!(text("count", &["l", "hello"]), "2");
        assert!(call("repeat", &arguments(&["1000000000", "xx"])).is_err());
    }

    #[test]
    fn from_json_decodes_documents_and_yields_nil_when_malformed() {
        assert!(matches!(
            call("fromJson", &arguments(&[r#"{"a":1}"#])).unwrap(),
            Value::Json(_)
        ));
        assert!(
            matches!(call("fromJson", &arguments(&["\"x\""])).unwrap(), Value::String(value) if value == "x")
        );
        assert!(matches!(
            call("fromJson", &arguments(&["{"])).unwrap(),
            Value::Nil
        ));
    }

    #[test]
    fn encoding_and_time_functions() {
        assert_eq!(text("b64enc", &["hi"]), "aGk=");
        assert_eq!(text("b64dec", &["aGk"]), "hi");
        assert_eq!(text("urlencode", &["a b&c"]), "a+b%26c");
        assert_eq!(text("urldecode", &["a+b%26c"]), "a b&c");
        let time = [Value::Time(1_500_000_000_123_456_789)];
        assert_eq!(call("unixEpoch", &time).unwrap().text(), "1500000000");
        assert_eq!(
            call("unixEpochMillis", &time).unwrap().text(),
            "1500000000123"
        );
        assert_eq!(
            call(
                "date",
                &[Value::String("2006-01-02".into()), Value::Int(86_400)]
            )
            .unwrap()
            .text(),
            "1970-01-02"
        );
    }

    #[test]
    fn rejects_unknown_functions_and_bad_arity() {
        assert!(call("nope", &arguments(&["x"])).is_err());
        assert!(call("Trim", &arguments(&["x"])).is_err());
        assert!(call("sub", &arguments(&["1"])).is_err());
    }

    #[test]
    fn declared_functions_are_implemented() {
        let values = arguments(&["1", "1", "1", "1"]);
        for name in FUNCTIONS {
            let unknown = (0..=values.len()).all(|count| {
                matches!(
                    call(name, &values[..count]),
                    Err(Error::Query(message)) if message.starts_with("unknown or unsupported")
                )
            });
            assert!(!unknown, "{name} is declared but not implemented");
        }
    }
}
