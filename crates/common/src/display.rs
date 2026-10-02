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
    PrometheusFloat(value).to_string()
}

/// [`prometheus_float`]'s text as a [`std::fmt::Display`], for writers that
/// need no intermediate `String`.
pub struct PrometheusFloat(pub f64);

impl std::fmt::Display for PrometheusFloat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            value if value.is_nan() => formatter.write_str("NaN"),
            f64::INFINITY => formatter.write_str("+Inf"),
            f64::NEG_INFINITY => formatter.write_str("-Inf"),
            value => std::fmt::Display::fmt(&value, formatter),
        }
    }
}

/// Formats a sample value the way Prometheus' JSON API does
/// (`jsonutil.MarshalFloat`): like [`prometheus_float`], except magnitudes below
/// `1e-6` or at least `1e21` use Go's exponent form (`1e-07`, `1.5e+21`).
pub fn prometheus_json_float(value: f64) -> String {
    let abs = value.abs();
    if !value.is_finite() || abs == 0.0 || (1e-6..1e21).contains(&abs) {
        return prometheus_float(value);
    }
    let formatted = format!("{value:e}");
    let (mantissa, exponent) = formatted
        .split_once('e')
        .expect("LowerExp output contains an exponent");
    let (sign, digits) = match exponent.strip_prefix('-') {
        Some(digits) => ('-', digits),
        None => ('+', exponent),
    };
    format!("{mantissa}e{sign}{digits:0>2}")
}

/// Rewrites `name` into a valid Prometheus label name: every character outside
/// `[A-Za-z0-9_]` becomes `_`, and a leading digit is prefixed with `_`.
pub fn sanitize_label_name(name: &str) -> String {
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

    #[test]
    fn should_sanitize_label_names() {
        assert_eq!(sanitize_label_name("service.name"), "service_name");
        assert_eq!(sanitize_label_name("9lives"), "_9lives");
    }
}
