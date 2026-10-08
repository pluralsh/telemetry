//! Display and formatting utilities.

/// Format a number for display with SI suffixes and appropriate precision.
///
/// Large numbers are displayed with K/M/B suffixes, while smaller numbers
/// use appropriate decimal places based on magnitude.
///
/// # Examples
///
/// ```
/// use common::display::format_number;
///
/// assert_eq!(format_number(1234567.0), "1.23M");
/// assert_eq!(format_number(1500.0), "1.50K");
/// assert_eq!(format_number(42.0), "42");
/// assert_eq!(format_number(0.5), "0.50");
/// assert_eq!(format_number(0.005), "0.0050");
/// ```
pub fn format_number(value: f64) -> String {
    let abs = value.abs();

    // Determine appropriate precision based on magnitude
    let (formatted, suffix) = if abs >= 1_000_000_000.0 {
        (value / 1_000_000_000.0, "B")
    } else if abs >= 1_000_000.0 {
        (value / 1_000_000.0, "M")
    } else if abs >= 1_000.0 {
        (value / 1_000.0, "K")
    } else {
        (value, "")
    };

    // Format with appropriate decimal places
    if suffix.is_empty() {
        if abs == 0.0 {
            "0".to_string()
        } else if abs < 0.01 {
            format!("{:.4}", formatted)
        } else if abs < 1.0 {
            format!("{:.2}", formatted)
        } else if formatted.fract() == 0.0 {
            format!("{:.0}", formatted)
        } else {
            format!("{:.2}", formatted)
        }
    } else {
        format!("{:.2}{}", formatted, suffix)
    }
}

/// Lowercase hex encoding of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Formats a sample value the way Prometheus does: `NaN`, `+Inf`, `-Inf`, or the
/// shortest round-trip decimal (which matches Go's `FormatFloat(v, 'f', -1, 64)`).
pub fn prometheus_float(value: f64) -> String {
    FloatText::prometheus(value).as_str().to_owned()
}

/// [`prometheus_float`]'s text as a [`std::fmt::Display`], for writers that
/// need no intermediate `String`.
pub struct PrometheusFloat(pub f64);

impl std::fmt::Display for PrometheusFloat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(FloatText::prometheus(self.0).as_str())
    }
}

/// Formats a sample value the way Prometheus' JSON API does
/// (`jsonutil.MarshalFloat`): like [`prometheus_float`], except magnitudes below
/// `1e-6` or at least `1e21` use Go's exponent form (`1e-07`, `1.5e+21`).
pub fn prometheus_json_float(value: f64) -> String {
    FloatText::prometheus_json(value).as_str().to_owned()
}

/// A float's Prometheus spelling on the stack. Every formatter in this
/// module goes through it: the shortest round-trip digits come from `ryu`,
/// laid out here, so no float reaches `core::fmt`.
pub struct FloatText {
    buf: [u8; FLOAT_TEXT_CAPACITY],
    len: usize,
}

/// Fixed notation of the smallest subnormal: sign, `0.`, 323 zeros and its
/// digits.
const FLOAT_TEXT_CAPACITY: usize = 352;

impl FloatText {
    /// [`prometheus_float`]'s text.
    pub fn prometheus(value: f64) -> Self {
        Self::format(value, false)
    }

    /// [`prometheus_json_float`]'s text.
    pub fn prometheus_json(value: f64) -> Self {
        Self::format(value, true)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).expect("float text is ASCII")
    }

    fn format(value: f64, json: bool) -> Self {
        let mut text = Self {
            buf: [0; FLOAT_TEXT_CAPACITY],
            len: 0,
        };
        if value.is_nan() {
            text.push(b"NaN");
            return text;
        }
        if value.is_infinite() {
            text.push(if value > 0.0 { b"+Inf" } else { b"-Inf" });
            return text;
        }
        if value.is_sign_negative() {
            text.push(b"-");
        }
        let mut ryu = ryu::Buffer::new();
        let (digits, point) = decimal_digits(ryu.format_finite(value.abs()));
        let Some(digits) = digits else {
            text.push(b"0");
            return text;
        };
        let digits: &[u8] = &digits;
        let abs = value.abs();
        if json && !(1e-6..1e21).contains(&abs) {
            // Go's 'e' form: one leading digit and an exponent of at least
            // two digits.
            text.push(&digits[..1]);
            if digits.len() > 1 {
                text.push(b".");
                text.push(&digits[1..]);
            }
            let exponent = point - 1;
            text.push(if exponent < 0 { b"e-" } else { b"e+" });
            let magnitude = exponent.unsigned_abs();
            if magnitude < 10 {
                text.push(b"0");
            }
            text.push(itoa(magnitude, &mut [0; 3]));
        } else if point <= 0 {
            text.push(b"0.");
            text.zeros(point.unsigned_abs() as usize);
            text.push(digits);
        } else if point as usize >= digits.len() {
            text.push(digits);
            text.zeros(point as usize - digits.len());
        } else {
            text.push(&digits[..point as usize]);
            text.push(b".");
            text.push(&digits[point as usize..]);
        }
        text
    }

    fn push(&mut self, bytes: &[u8]) {
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }

    fn zeros(&mut self, count: usize) {
        self.buf[self.len..self.len + count].fill(b'0');
        self.len += count;
    }
}

/// The significant digits of a shortest round-trip decimal: at most 17.
struct Digits {
    buf: [u8; 24],
    len: usize,
}

impl std::ops::Deref for Digits {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// The significant digits of `ryu`'s spelling of a non-negative finite
/// value, without leading or trailing zeros (`None` for zero), and the
/// position of the decimal point: the value is `0.DIGITS × 10^point`.
fn decimal_digits(formatted: &str) -> (Option<Digits>, i32) {
    let (mantissa, exponent) = match formatted.split_once('e') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().expect("ryu exponent")),
        None => (formatted, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = Digits {
        buf: [0; 24],
        len: 0,
    };
    let mut point = integer.len() as i32 + exponent;
    for &byte in integer.as_bytes().iter().chain(fraction.as_bytes()) {
        if digits.len == 0 && byte == b'0' {
            point -= 1;
            continue;
        }
        digits.buf[digits.len] = byte;
        digits.len += 1;
    }
    while digits.len > 0 && digits.buf[digits.len - 1] == b'0' {
        digits.len -= 1;
    }
    if digits.len == 0 {
        return (None, 0);
    }
    (Some(digits), point)
}

fn itoa(mut value: u32, buf: &mut [u8; 3]) -> &[u8] {
    let mut start = buf.len();
    loop {
        start -= 1;
        buf[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &buf[start..];
        }
    }
}

/// Rewrites `name` into a valid Prometheus label name: every character outside
/// `[A-Za-z0-9_]` becomes `_`, and a leading digit is prefixed with `_`.
pub fn sanitize_label_name(name: &str) -> String {
    sanitized_label_name(name).into_owned()
}

/// [`sanitize_label_name`], borrowing `name` when it is already valid.
pub fn sanitized_label_name(name: &str) -> std::borrow::Cow<'_, str> {
    let leading_digit = name.as_bytes().first().is_some_and(u8::is_ascii_digit);
    let valid = |byte: &u8| byte.is_ascii_alphanumeric() || *byte == b'_';
    if !leading_digit && name.as_bytes().iter().all(valid) {
        return std::borrow::Cow::Borrowed(name);
    }
    let mut result = String::with_capacity(name.len() + usize::from(leading_digit));
    if leading_digit {
        result.push('_');
    }
    result.extend(name.chars().map(|character| {
        if character.is_ascii_alphanumeric() || character == '_' {
            character
        } else {
            '_'
        }
    }));
    std::borrow::Cow::Owned(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_format_billions() {
        assert_eq!(format_number(1_234_567_890.0), "1.23B");
        assert_eq!(format_number(5_000_000_000.0), "5.00B");
    }

    #[test]
    fn should_format_millions() {
        assert_eq!(format_number(1_234_567.0), "1.23M");
        assert_eq!(format_number(5_500_000.0), "5.50M");
    }

    #[test]
    fn should_format_thousands() {
        assert_eq!(format_number(1_234.0), "1.23K");
        assert_eq!(format_number(52_647.0), "52.65K");
    }

    #[test]
    fn should_format_whole_numbers() {
        assert_eq!(format_number(42.0), "42");
        assert_eq!(format_number(100.0), "100");
    }

    #[test]
    fn should_format_decimals() {
        assert_eq!(format_number(42.5), "42.50");
        assert_eq!(format_number(0.75), "0.75");
    }

    #[test]
    fn should_format_small_decimals() {
        assert_eq!(format_number(0.005), "0.0050");
        assert_eq!(format_number(0.0001), "0.0001");
    }

    #[test]
    fn should_handle_zero() {
        assert_eq!(format_number(0.0), "0");
    }

    #[test]
    fn should_handle_negative_numbers() {
        assert_eq!(format_number(-1_500.0), "-1.50K");
        assert_eq!(format_number(-42.5), "-42.50");
    }

    #[test]
    fn should_hex_encode_bytes() {
        assert_eq!(hex(&[0x00, 0xab, 0x0f]), "00ab0f");
        assert_eq!(hex(&[]), "");
    }

    #[test]
    fn should_format_prometheus_floats() {
        assert_eq!(prometheus_float(6.0), "6");
        assert_eq!(prometheus_float(-0.0), "-0");
        assert_eq!(prometheus_float(f64::NAN), "NaN");
        assert_eq!(prometheus_float(f64::INFINITY), "+Inf");
        assert_eq!(prometheus_float(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn should_format_prometheus_json_floats_like_marshal_float() {
        assert_eq!(prometheus_json_float(4.0), "4");
        assert_eq!(prometheus_json_float(0.000_001), "0.000001");
        assert_eq!(prometheus_json_float(1e-7), "1e-07");
        assert_eq!(prometheus_json_float(-1e-128), "-1e-128");
        assert_eq!(prometheus_json_float(1.5e21), "1.5e+21");
        assert_eq!(prometheus_json_float(1e20), "100000000000000000000");
        assert_eq!(prometheus_json_float(0.0), "0");
        assert_eq!(prometheus_json_float(f64::NAN), "NaN");
        assert_eq!(prometheus_json_float(f64::NEG_INFINITY), "-Inf");
    }

    proptest::proptest! {
        #[test]
        fn float_text_matches_core_display(bits in proptest::num::u64::ANY) {
            let value = f64::from_bits(bits);
            proptest::prop_assume!(value.is_finite());
            let text = FloatText::prometheus(value);
            proptest::prop_assert_eq!(text.as_str(), value.to_string());
            let abs = value.abs();
            let json = FloatText::prometheus_json(value);
            if abs == 0.0 || (1e-6..1e21).contains(&abs) {
                proptest::prop_assert_eq!(json.as_str(), value.to_string());
            } else {
                let lower = format!("{value:e}");
                let (mantissa, exponent) = lower.split_once('e').unwrap();
                let (sign, digits) = match exponent.strip_prefix('-') {
                    Some(digits) => ('-', digits),
                    None => ('+', exponent),
                };
                proptest::prop_assert_eq!(json.as_str(), format!("{mantissa}e{sign}{digits:0>2}"));
            }
        }
    }

    #[test]
    fn should_sanitize_label_names() {
        assert_eq!(sanitize_label_name("service.name"), "service_name");
        assert_eq!(sanitize_label_name("9lives"), "_9lives");
        assert_eq!(sanitize_label_name("already_valid_1"), "already_valid_1");
        assert_eq!(sanitize_label_name("é.x"), "__x");
        assert_eq!(sanitize_label_name("9é"), "_9_");
        assert_eq!(sanitize_label_name(""), "");
    }
}
