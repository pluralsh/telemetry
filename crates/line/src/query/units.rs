use super::*;

pub(super) static BYTE_SIZE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b)?$").expect("static bytes regex")
});
pub(super) static ANSI_ESCAPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").expect("static ANSI regex"));

pub(super) fn parse_duration_ns(source: &str) -> Result<i64> {
    common::time::parse_duration_ns(source).map_err(|error| Error::Query(error.to_string()))
}

pub(super) fn parse_bytes(source: &str) -> Result<f64> {
    let captures = BYTE_SIZE
        .captures(source)
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
    Ok(value
        * if suffix.to_ascii_lowercase().contains('i') {
            1024f64.powi(power)
        } else {
            1000f64.powi(power)
        })
}

pub(super) fn ip_matches(candidate: &str, expression: &str) -> bool {
    if let Some((network, prefix)) = expression.split_once('/') {
        let (Ok(candidate), Ok(network), Ok(prefix)) = (
            candidate.parse::<IpAddr>(),
            network.parse::<IpAddr>(),
            prefix.parse::<u8>(),
        ) else {
            return false;
        };
        match (candidate, network) {
            (IpAddr::V4(candidate), IpAddr::V4(network)) if prefix <= 32 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - prefix)
                };
                u32::from(candidate) & mask == u32::from(network) & mask
            }
            (IpAddr::V6(candidate), IpAddr::V6(network)) if prefix <= 128 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - prefix)
                };
                u128::from(candidate) & mask == u128::from(network) & mask
            }
            _ => false,
        }
    } else {
        candidate.parse::<IpAddr>().ok() == expression.parse::<IpAddr>().ok()
            && candidate.parse::<IpAddr>().is_ok()
    }
}
