//! Go's `fmt` formatting for template values: `Sprint`, `Sprintln` and
//! `Sprintf` with flags, width and precision. As in Go, a verb that does not
//! fit its operand renders `%!verb(type=value)` rather than failing.

use super::{MAX_OUTPUT_BYTES, Value};
use crate::{Error, Result};

/// Go's `%v` for a `float64`: the shortest representation, in exponent form
/// when the decimal exponent is below -4 or at least 6.
pub(super) fn format_float(value: f64) -> String {
    format_g(value, None)
}

/// `fmt.Sprint`: spaces separate operands when neither side is a string.
pub(super) fn sprint(arguments: &[Value]) -> String {
    let mut output = String::new();
    for (index, argument) in arguments.iter().enumerate() {
        let is_string = matches!(argument, Value::String(_));
        if index > 0 && !is_string && !matches!(arguments[index - 1], Value::String(_)) {
            output.push(' ');
        }
        output.push_str(&argument.text());
    }
    output
}

/// `fmt.Sprintln`: spaces always separate operands and a newline ends them.
pub(super) fn sprintln(arguments: &[Value]) -> String {
    let mut output = arguments
        .iter()
        .map(Value::text)
        .collect::<Vec<_>>()
        .join(" ");
    output.push('\n');
    output
}

#[derive(Default)]
struct Spec {
    minus: bool,
    plus: bool,
    sharp: bool,
    space: bool,
    zero: bool,
    width: Option<usize>,
    precision: Option<usize>,
}

pub(super) fn sprintf(format: &str, arguments: &[Value]) -> Result<String> {
    let mut output = String::with_capacity(format.len());
    let mut arguments = arguments.iter();
    let mut chars = format.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        let mut spec = Spec::default();
        while let Some(&flag) = chars.peek() {
            match flag {
                '-' => spec.minus = true,
                '+' => spec.plus = true,
                '#' => spec.sharp = true,
                ' ' => spec.space = true,
                '0' => spec.zero = true,
                _ => break,
            }
            chars.next();
        }
        spec.width = parse_count(&mut chars, &mut arguments)?;
        if chars.peek() == Some(&'.') {
            chars.next();
            spec.precision = Some(parse_count(&mut chars, &mut arguments)?.unwrap_or(0));
        }
        let Some(verb) = chars.next() else {
            output.push_str("%!(NOVERB)");
            break;
        };
        if verb == '%' {
            output.push('%');
            continue;
        }
        let Some(argument) = arguments.next() else {
            output.push_str(&format!("%!{verb}(MISSING)"));
            continue;
        };
        if spec.width.unwrap_or(0) > MAX_OUTPUT_BYTES {
            return Err(Error::Query("printf width is too large".into()));
        }
        output.push_str(&format_one(verb, &spec, argument));
    }
    let extra = arguments
        .map(|argument| format!("{}={}", argument.type_name(), argument.text()))
        .collect::<Vec<_>>();
    if !extra.is_empty() {
        output.push_str(&format!("%!(EXTRA {})", extra.join(", ")));
    }
    Ok(output)
}

fn parse_count<'a>(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    arguments: &mut impl Iterator<Item = &'a Value>,
) -> Result<Option<usize>> {
    if chars.peek() == Some(&'*') {
        chars.next();
        return Ok(arguments
            .next()
            .and_then(|argument| match argument {
                Value::Int(value) => usize::try_from(*value).ok(),
                _ => None,
            })
            .or(Some(0)));
    }
    let mut digits = String::new();
    while let Some(&digit) = chars.peek().filter(|c| c.is_ascii_digit()) {
        digits.push(digit);
        chars.next();
    }
    if digits.is_empty() {
        Ok(None)
    } else {
        digits
            .parse()
            .map(Some)
            .map_err(|_| Error::Query("printf width is too large".into()))
    }
}

fn format_one(verb: char, spec: &Spec, argument: &Value) -> String {
    let bad = || format!("%!{verb}({}={})", argument.type_name(), argument.text());
    if let Value::Nil = argument
        && verb != 'v'
    {
        return format!("%!{verb}(<nil>)");
    }
    let body = match (verb, argument) {
        ('v', Value::String(value)) if spec.sharp => go_quote(value),
        ('v', Value::Int(value)) => return format_int('d', spec, *value),
        ('v', Value::Float(value)) => return format_float_verb('g', spec, *value),
        ('v', _) => argument.text(),
        ('s', Value::Int(_) | Value::Float(_) | Value::Bool(_)) => return bad(),
        ('s', _) => truncate(&argument.text(), spec.precision),
        ('q', Value::Int(value)) => go_quote_rune(*value),
        ('q', Value::String(_) | Value::Time(_) | Value::Json(_)) => {
            go_quote(&truncate(&argument.text(), spec.precision))
        }
        ('t', Value::Bool(value)) => value.to_string(),
        ('d' | 'b' | 'o' | 'O' | 'x' | 'X' | 'c' | 'U', Value::Int(value)) => {
            return format_int(verb, spec, *value);
        }
        ('x' | 'X', Value::String(value)) => {
            let hex = value
                .bytes()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            if verb == 'X' { hex.to_uppercase() } else { hex }
        }
        ('e' | 'E' | 'f' | 'F' | 'g' | 'G', Value::Float(value)) => {
            return format_float_verb(verb, spec, *value);
        }
        _ => return bad(),
    };
    pad(spec, body)
}

fn truncate(text: &str, precision: Option<usize>) -> String {
    match precision {
        Some(precision) => text.chars().take(precision).collect(),
        None => text.to_owned(),
    }
}

fn format_int(verb: char, spec: &Spec, value: i64) -> String {
    let magnitude = value.unsigned_abs();
    let digits = match verb {
        'd' => magnitude.to_string(),
        'b' => format!("{magnitude:b}"),
        'o' | 'O' => format!("{magnitude:o}"),
        'x' => format!("{magnitude:x}"),
        'X' => format!("{magnitude:X}"),
        'c' => {
            return pad(
                spec,
                u32::try_from(value)
                    .ok()
                    .and_then(char::from_u32)
                    .unwrap_or('\u{FFFD}')
                    .to_string(),
            );
        }
        _ => return pad(spec, format!("U+{magnitude:04X}")),
    };
    let digits = match spec.precision {
        Some(precision) if digits.len() < precision => {
            format!("{}{digits}", "0".repeat(precision - digits.len()))
        }
        _ => digits,
    };
    let prefix = match verb {
        'O' => "0o",
        'b' if spec.sharp => "0b",
        'o' if spec.sharp => "0",
        'x' if spec.sharp => "0x",
        'X' if spec.sharp => "0X",
        _ => "",
    };
    let sign = sign(value < 0, spec);
    pad_signed(
        spec,
        sign,
        &format!("{prefix}{digits}"),
        spec.precision.is_none(),
    )
}

fn format_float_verb(verb: char, spec: &Spec, value: f64) -> String {
    if !value.is_finite() {
        let text = if value.is_nan() {
            "NaN"
        } else if value > 0.0 {
            if spec.plus { "+Inf" } else { "Inf" }
        } else {
            "-Inf"
        };
        return pad(spec, text.into());
    }
    let magnitude = value.abs();
    let body = match verb {
        'f' | 'F' => format!("{magnitude:.*}", spec.precision.unwrap_or(6)),
        'e' | 'E' => {
            let text = go_exponent(&format!("{magnitude:.*e}", spec.precision.unwrap_or(6)));
            if verb == 'E' {
                text.to_uppercase()
            } else {
                text
            }
        }
        _ => {
            let text = format_g(magnitude, spec.precision);
            if verb == 'G' {
                text.to_uppercase()
            } else {
                text
            }
        }
    };
    let negative = value.is_sign_negative() && value != 0.0;
    pad_signed(spec, sign(negative, spec), &body, true)
}

/// Go's `%g` (and `%v`): exponent form when the decimal exponent is below -4
/// or reaches the precision, which is 6 for the shortest representation.
fn format_g(value: f64, precision: Option<usize>) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let scientific = match precision {
        Some(precision) => format!("{value:.*e}", precision.max(1) - 1),
        None => format!("{value:e}"),
    };
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
    let exponent: i32 = exponent.parse().expect("numeric exponent");
    let mantissa = if mantissa.contains('.') {
        mantissa.trim_end_matches('0').trim_end_matches('.')
    } else {
        mantissa
    };
    let digits = mantissa.chars().filter(char::is_ascii_digit).count() as i32;
    let limit = match precision {
        None => 6,
        Some(precision) => {
            let precision = precision.max(1) as i32;
            if precision > digits && digits > exponent {
                digits
            } else {
                precision
            }
        }
    };
    if exponent < -4 || exponent >= limit {
        go_exponent(&format!("{mantissa}e{exponent}"))
    } else {
        let decimals = (digits - exponent - 1).max(0) as usize;
        format!("{value:.decimals$}")
    }
}

/// Rewrites Rust's `1.5e7` exponent as Go's `1.5e+07`.
fn go_exponent(text: &str) -> String {
    let (mantissa, exponent) = text.split_once('e').expect("scientific notation");
    let exponent: i32 = exponent.parse().expect("numeric exponent");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

fn sign(negative: bool, spec: &Spec) -> &'static str {
    if negative {
        "-"
    } else if spec.plus {
        "+"
    } else if spec.space {
        " "
    } else {
        ""
    }
}

/// Pads a signed number; zero padding goes between the sign and the digits.
fn pad_signed(spec: &Spec, sign: &str, digits: &str, zero_allowed: bool) -> String {
    let width = spec.width.unwrap_or(0);
    let len = sign.chars().count() + digits.chars().count();
    if spec.zero && zero_allowed && !spec.minus && len < width {
        format!("{sign}{}{digits}", "0".repeat(width - len))
    } else {
        pad(spec, format!("{sign}{digits}"))
    }
}

fn pad(spec: &Spec, text: String) -> String {
    let width = spec.width.unwrap_or(0);
    let len = text.chars().count();
    if len >= width {
        text
    } else if spec.minus {
        format!("{text}{}", " ".repeat(width - len))
    } else {
        format!("{}{text}", " ".repeat(width - len))
    }
}

/// Go's `strconv.Quote`.
pub(super) fn go_quote(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        push_escaped(&mut output, character, '"');
    }
    output.push('"');
    output
}

fn go_quote_rune(value: i64) -> String {
    let character = u32::try_from(value)
        .ok()
        .and_then(char::from_u32)
        .unwrap_or('\u{FFFD}');
    let mut output = String::from("'");
    push_escaped(&mut output, character, '\'');
    output.push('\'');
    output
}

fn push_escaped(output: &mut String, character: char, quote: char) {
    match character {
        '\x07' => output.push_str(r"\a"),
        '\x08' => output.push_str(r"\b"),
        '\x0c' => output.push_str(r"\f"),
        '\n' => output.push_str(r"\n"),
        '\r' => output.push_str(r"\r"),
        '\t' => output.push_str(r"\t"),
        '\x0b' => output.push_str(r"\v"),
        '\\' => output.push_str(r"\\"),
        character if character == quote => {
            output.push('\\');
            output.push(character);
        }
        character if character.is_control() => {
            let code = u32::from(character);
            if code < 0x80 {
                output.push_str(&format!(r"\x{code:02x}"));
            } else if code <= 0xffff {
                output.push_str(&format!(r"\u{code:04x}"));
            } else {
                output.push_str(&format!(r"\U{code:08x}"));
            }
        }
        character => output.push(character),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> Value {
        Value::String(value.into())
    }

    fn sprintf_ok(format: &str, arguments: &[Value]) -> String {
        sprintf(format, arguments).unwrap()
    }

    #[test]
    fn floats_print_like_go() {
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(3.5), "3.5");
        assert_eq!(format_float(123_456.0), "123456");
        assert_eq!(format_float(1_234_567.0), "1.234567e+06");
        assert_eq!(format_float(0.0001), "0.0001");
        assert_eq!(format_float(0.00001), "1e-05");
        assert_eq!(format_float(-2.5e-7), "-2.5e-07");
    }

    #[test]
    fn verbs_flags_width_and_precision() {
        assert_eq!(sprintf_ok("%s=%d", &[s("a"), Value::Int(2)]), "a=2");
        assert_eq!(sprintf_ok("%-5s|%5s|", &[s("ab"), s("cd")]), "ab   |   cd|");
        assert_eq!(
            sprintf_ok(
                "%05d %+d %x %#X",
                &[
                    Value::Int(-42),
                    Value::Int(3),
                    Value::Int(255),
                    Value::Int(255)
                ]
            ),
            "-0042 +3 ff 0XFF"
        );
        assert_eq!(
            sprintf_ok(
                "%.2f %8.3f %e",
                &[
                    Value::Float(1.23456),
                    Value::Float(2.5),
                    Value::Float(1234.5)
                ]
            ),
            "1.23    2.500 1.234500e+03"
        );
        assert_eq!(
            sprintf_ok(
                "%g %.3g %G",
                &[
                    Value::Float(1e21),
                    Value::Float(1234.5678),
                    Value::Float(1e-7)
                ]
            ),
            "1e+21 1.23e+03 1E-07"
        );
        assert_eq!(
            sprintf_ok(
                "%q %v %t %c %.2s",
                &[
                    s("a\"b\n"),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Int(65),
                    s("hello")
                ]
            ),
            "\"a\\\"b\\n\" true false A he"
        );
        assert_eq!(
            sprintf_ok("%x %5.1f%%", &[s("hi"), Value::Float(99.44)]),
            "6869  99.4%"
        );
    }

    #[test]
    fn mismatches_render_go_markers() {
        assert_eq!(sprintf_ok("%d", &[Value::Float(2.7)]), "%!d(float64=2.7)");
        assert_eq!(sprintf_ok("%d", &[s("200")]), "%!d(string=200)");
        assert_eq!(sprintf_ok("%s", &[Value::Int(3)]), "%!s(int=3)");
        assert_eq!(sprintf_ok("%s %s", &[s("a")]), "a %!s(MISSING)");
        assert_eq!(
            sprintf_ok("%s", &[s("a"), Value::Int(1)]),
            "a%!(EXTRA int=1)"
        );
    }

    #[test]
    fn sprint_spacing() {
        assert_eq!(
            sprint(&[Value::Int(1), Value::Int(2), s("x"), Value::Int(3)]),
            "1 2x3"
        );
        assert_eq!(sprintln(&[s("a"), Value::Int(1)]), "a 1\n");
    }
}
