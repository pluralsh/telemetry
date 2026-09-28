// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listener: SocketAddr,
    pub filesystem: pseudofs::Config,
    pub max_unary_file_size_bytes: u64,
    pub max_decoding_message_bytes: usize,
    pub max_encoding_message_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listener: "0.0.0.0:9093".parse().expect("valid default listener"),
            filesystem: pseudofs::Config::default(),
            max_unary_file_size_bytes: 8 * 1024 * 1024,
            max_decoding_message_bytes: 16 * 1024 * 1024,
            max_encoding_message_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path).map_err(ConfigError::Io)?;
        let config: Self = serde_yaml::from_str(&raw).map_err(ConfigError::Yaml)?;
        config.validate().map_err(ConfigError::Validation)?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.filesystem.validate()?;
        if self.max_decoding_message_bytes == 0 || self.max_encoding_message_bytes == 0 {
            return Err("gRPC message limits must be greater than zero".to_owned());
        }
        if self.max_unary_file_size_bytes == 0
            || self.max_unary_file_size_bytes > self.filesystem.max_file_size_bytes
        {
            return Err(
                "max_unary_file_size_bytes must be positive and not exceed max_file_size_bytes"
                    .to_owned(),
            );
        }
        if self.max_unary_file_size_bytes as usize > self.max_decoding_message_bytes
            || self.max_unary_file_size_bytes as usize > self.max_encoding_message_bytes
        {
            return Err("max_unary_file_size_bytes must not exceed gRPC message limits".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read configuration: {0}")]
    Io(#[source] std::io::Error),
    #[error("invalid YAML configuration: {0}")]
    Yaml(#[source] serde_yaml::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_checked_in_example_are_valid() {
        Config::default().validate().unwrap();
        let example: Config =
            serde_yaml::from_str(include_str!("../../../config/pseudofs.example.yaml")).unwrap();
        example.validate().unwrap();
        assert_eq!(example.listener.port(), 9093);
    }

    #[test]
    fn rejects_zero_limits() {
        let config = Config {
            max_decoding_message_bytes: 0,
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }
}
