use super::*;

pub(super) static BYTE_SIZE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b)?$").expect("static bytes regex")
});
pub(super) static ANSI_ESCAPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").expect("static ANSI regex"));

pub(super) fn parse_duration_ns(source: &str) -> Result<i64> {
    common::time::parse_duration_ns(source).map_err(|error| Error::Query(error.to_string()))
}

/// Byte sizes as go-humanize parses them for Loki: thousands separators are
/// allowed and the result is a whole number of bytes.
pub(super) fn parse_bytes(source: &str) -> Result<f64> {
    let source = source.replace(',', "");
    let captures = BYTE_SIZE
        .captures(&source)
        .ok_or_else(|| Error::Query(format!("invalid byte size {source:?}")))?;
    let value: f64 = captures[1]
        .parse()
        .map_err(|_| Error::Query(format!("invalid byte size {source:?}")))?;
    let suffix = captures.get(2).map_or("", |value| value.as_str());
    let power = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "kb" | "kib" => 1,
        "mb" | "mib" => 2,
        "gb" | "gib" => 3,
        "tb" | "tib" => 4,
        "pb" | "pib" => 5,
        "eb" | "eib" => 6,
        _ => return Err(Error::Query(format!("invalid byte size {source:?}"))),
    };
    Ok((value
        * if suffix.to_ascii_lowercase().contains('i') {
            1024f64.powi(power)
        } else {
            1000f64.powi(power)
        })
    .trunc())
}

/// Whether `text` holds an address inside the `ip()` pattern anywhere, as
/// Loki's IP filters scan (e.g. `client 10.0.0.7:80` matches `10.0.0.0/8`).
pub(super) fn contains_ip(text: &str, pattern: &str) -> bool {
    let Some(pattern) = IpPattern::parse(pattern) else {
        return false;
    };
    text.split(|character: char| !(character.is_ascii_hexdigit() || ".:".contains(character)))
        .filter(|candidate| !candidate.is_empty())
        .any(|candidate| {
            let address = candidate.parse::<IpAddr>().ok().or_else(|| {
                // `host:port`, which never parses as IPv6 when it has dots.
                let (host, _) = candidate.rsplit_once(':')?;
                host.contains('.').then(|| host.parse().ok()).flatten()
            });
            address.is_some_and(|address| pattern.contains(address))
        })
}

/// Go's `time.ParseDuration`, which Loki applies to label values: no day or
/// week units, an optional sign, and a bare `0`.
pub(super) fn parse_go_duration_ns(source: &str) -> Option<f64> {
    let (negative, unsigned) = match source.as_bytes().first()? {
        b'-' => (true, &source[1..]),
        b'+' => (false, &source[1..]),
        _ => (false, source),
    };
    if unsigned == "0" {
        return Some(0.0);
    }
    if unsigned.contains(['d', 'w', 'y']) {
        return None;
    }
    let value = common::time::parse_duration_ns(unsigned).ok()? as f64;
    Some(if negative { -value } else { value })
}
