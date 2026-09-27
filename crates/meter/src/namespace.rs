use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Maximum encoded namespace length.
pub const MAX_NAMESPACE_LEN: usize = 255;

/// A validated tenant namespace used to isolate all storage and query state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Namespace(String);

impl Default for Namespace {
    fn default() -> Self {
        Self("default".to_string())
    }
}

impl Namespace {
    pub fn new(value: impl Into<String>) -> Result<Self, NamespaceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(NamespaceError::Empty);
        }
        if value.len() > MAX_NAMESPACE_LEN {
            return Err(NamespaceError::TooLong {
                length: value.len(),
                maximum: MAX_NAMESPACE_LEN,
            });
        }
        if value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(NamespaceError::ControlCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Namespace {
    type Err = NamespaceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NamespaceError {
    #[error("namespace cannot be empty")]
    Empty,
    #[error("namespace length {length} exceeds maximum {maximum}")]
    TooLong { length: usize, maximum: usize },
    #[error("namespace cannot contain NUL or control characters")]
    ControlCharacter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_namespaces() {
        assert_eq!(Namespace::new("acme").unwrap().as_str(), "acme");
        assert_eq!(Namespace::new(""), Err(NamespaceError::Empty));
        assert_eq!(
            Namespace::new("bad\nname"),
            Err(NamespaceError::ControlCharacter)
        );
    }
}
