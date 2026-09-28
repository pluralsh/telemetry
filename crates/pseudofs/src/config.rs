// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use common::storage::config::StorageConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub storage: StorageConfig,
    pub chunk_size_bytes: usize,
    pub max_file_size_bytes: u64,
    pub max_append_generations: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            chunk_size_bytes: 1024 * 1024,
            max_file_size_bytes: 1024 * 1024 * 1024,
            max_append_generations: 64,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.chunk_size_bytes == 0 {
            return Err("chunk_size_bytes must be greater than zero".to_owned());
        }
        if self.max_file_size_bytes == 0 {
            return Err("max_file_size_bytes must be greater than zero".to_owned());
        }
        if self.chunk_size_bytes as u64 > self.max_file_size_bytes {
            return Err("chunk_size_bytes must not exceed max_file_size_bytes".to_owned());
        }
        if self.max_append_generations < 2 {
            return Err("max_append_generations must be at least two".to_owned());
        }
        Ok(())
    }
}
