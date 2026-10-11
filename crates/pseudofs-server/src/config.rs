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
    pub search: SearchLimits,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listener: "0.0.0.0:9093".parse().expect("valid default listener"),
            filesystem: pseudofs::Config::default(),
            max_unary_file_size_bytes: 8 * 1024 * 1024,
            max_decoding_message_bytes: 16 * 1024 * 1024,
            max_encoding_message_bytes: 16 * 1024 * 1024,
            search: SearchLimits::default(),
        }
    }
}

/// Defaults and caps for Glob and Grep requests. A request value of zero uses
/// the default; larger values are capped at the maximum.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchLimits {
    pub default_glob_results: u32,
    pub max_glob_results: u32,
    pub default_grep_matches: u32,
    pub max_grep_matches: u32,
    pub max_grep_context_lines: u32,
    pub default_grep_file_size_bytes: u64,
    pub max_grep_file_size_bytes: u64,
    /// Files one Grep request searches at once.
    pub grep_concurrency: u32,
    /// Directory entries one Glob or Grep walk may read before failing with
    /// RESOURCE_EXHAUSTED.
    pub max_scanned_entries: u64,
}

impl Default for SearchLimits {
    fn default() -> Self {
        Self {
            default_glob_results: 1000,
            max_glob_results: 10_000,
            default_grep_matches: 100,
            max_grep_matches: 1000,
            max_grep_context_lines: 100,
            default_grep_file_size_bytes: 16 * 1024 * 1024,
            max_grep_file_size_bytes: 64 * 1024 * 1024,
            grep_concurrency: 8,
            max_scanned_entries: 1_000_000,
        }
    }
}

impl SearchLimits {
    fn validate(&self) -> Result<(), String> {
        if self.grep_concurrency == 0 || self.max_scanned_entries == 0 {
            return Err(
                "search.grep_concurrency and search.max_scanned_entries must be positive"
                    .to_owned(),
            );
        }
        let pairs = [
            (
                "glob_results",
                self.default_glob_results as u64,
                self.max_glob_results as u64,
            ),
            (
                "grep_matches",
                self.default_grep_matches as u64,
                self.max_grep_matches as u64,
            ),
            (
                "grep_file_size_bytes",
                self.default_grep_file_size_bytes,
                self.max_grep_file_size_bytes,
            ),
        ];
        for (name, default, max) in pairs {
            if default == 0 || default > max {
                return Err(format!(
                    "search.default_{name} must be positive and not exceed search.max_{name}"
                ));
            }
        }
        Ok(())
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
        self.search.validate()
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
    fn fuzz_configs_are_valid() {
        for raw in [
            include_str!("../../../tests/regression/products/pseudofs/regression.yaml"),
            include_str!("../../../tests/regression/products/pseudofs/production.yaml"),
            include_str!("../../../tests/regression/products/pseudofs/local.yaml"),
        ] {
            let config: Config = serde_yaml::from_str(raw).unwrap();
            config.validate().unwrap();
        }
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
