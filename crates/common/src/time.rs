//! Wall-clock conversions and duration parsing shared across products.
//!
//! Conversions clamp pre-epoch times to zero and saturate at `i64::MAX`, so
//! callers never have to handle a clock that is out of range.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn since_epoch(time: SystemTime) -> Duration {
    time.duration_since(UNIX_EPOCH).unwrap_or_default()
}

fn saturating_i64(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub fn unix_nanos(time: SystemTime) -> i64 {
    saturating_i64(since_epoch(time).as_nanos())
}

pub fn unix_millis(time: SystemTime) -> i64 {
    saturating_i64(since_epoch(time).as_millis())
}

pub fn unix_secs(time: SystemTime) -> i64 {
    saturating_i64(u128::from(since_epoch(time).as_secs()))
}

pub fn now_ns() -> i64 {
    unix_nanos(SystemTime::now())
}

pub fn now_ms() -> i64 {
    unix_millis(SystemTime::now())
}

pub fn now_secs() -> i64 {
    unix_secs(SystemTime::now())
}

pub fn duration_ns(duration: Duration) -> i64 {
    saturating_i64(duration.as_nanos())
}

pub fn duration_ms(duration: Duration) -> i64 {
    saturating_i64(duration.as_millis())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("system clock is before the Unix epoch")]
    BeforeEpoch,
    #[error("Unix timestamp exceeds u64 milliseconds")]
    Overflow,
}

/// Current Unix time in milliseconds, failing rather than clamping when the
/// clock cannot be represented (used where a bogus time would corrupt data).
pub fn checked_now_ms() -> Result<u64, ClockError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ClockError::BeforeEpoch)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| ClockError::Overflow)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurationError {
    #[error("invalid duration {0:?}")]
    Invalid(String),
    #[error("duration {0:?} is outside the valid range")]
    OutOfRange(String),
}

/// Longer units sharing a prefix with shorter ones (`ms`/`m`) come first.
const DURATION_UNITS: [(&str, f64); 10] = [
    ("ns", 1.0),
    ("us", 1e3),
    ("µs", 1e3),
    ("ms", 1e6),
    ("s", 1e9),
    ("m", 60e9),
    ("h", 3_600e9),
    ("d", 86_400e9),
    ("w", 604_800e9),
    ("y", 31_536_000e9),
];

/// Parses a Go-style compound duration (`1h30m`, `1.5s`, `250ms`) extended
/// with `d`, `w` and `y`, as used by LogQL and TraceQL, into nanoseconds.
pub fn parse_duration_ns(source: &str) -> Result<i64, DurationError> {
    let invalid = || DurationError::Invalid(source.to_owned());
    if source.is_empty() {
        return Err(invalid());
    }
    let mut total = 0.0_f64;
    let mut rest = source;
    while !rest.is_empty() {
        let number_end = rest
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .filter(|end| *end > 0)
            .ok_or_else(invalid)?;
        let amount: f64 = rest[..number_end].parse().map_err(|_| invalid())?;
        rest = &rest[number_end..];
        let (unit, scale) = DURATION_UNITS
            .iter()
            .find(|(unit, _)| rest.starts_with(unit))
            .ok_or_else(invalid)?;
        total += amount * scale;
        rest = &rest[unit.len()..];
    }
    if !total.is_finite() || total > i64::MAX as f64 {
        return Err(DurationError::OutOfRange(source.to_owned()));
    }
    Ok(total as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compound_and_fractional_durations() {
        assert_eq!(parse_duration_ns("1h30m"), Ok(5_400_000_000_000));
        assert_eq!(parse_duration_ns("1.5s"), Ok(1_500_000_000));
        assert_eq!(parse_duration_ns("250ms"), Ok(250_000_000));
        assert_eq!(parse_duration_ns("3µs"), Ok(3_000));
        assert_eq!(parse_duration_ns("2us5ns"), Ok(2_005));
        assert_eq!(parse_duration_ns("1w1d"), Ok(691_200_000_000_000));
        assert_eq!(parse_duration_ns("1y"), Ok(31_536_000_000_000_000));
    }

    #[test]
    fn rejects_malformed_durations() {
        for source in ["", "5", "s", "5x", "1..5s", "1h 30m"] {
            assert_eq!(
                parse_duration_ns(source),
                Err(DurationError::Invalid(source.to_owned())),
                "{source:?}"
            );
        }
        assert_eq!(
            parse_duration_ns("1000y"),
            Err(DurationError::OutOfRange("1000y".to_owned()))
        );
    }

    #[test]
    fn conversions_clamp_and_saturate() {
        let before_epoch = UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(unix_nanos(before_epoch), 0);
        assert_eq!(
            unix_millis(UNIX_EPOCH + Duration::from_millis(1_234)),
            1_234
        );
        assert_eq!(unix_secs(UNIX_EPOCH + Duration::from_millis(1_999)), 1);
        assert_eq!(duration_ns(Duration::MAX), i64::MAX);
        assert_eq!(duration_ms(Duration::from_secs(2)), 2_000);
        assert!(checked_now_ms().unwrap() > 0);
    }
}
