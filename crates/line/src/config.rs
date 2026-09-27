// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, StorageConfig};

/// Single-node Line storage and immutable-page tuning.
#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub segment_duration: Duration,
    pub retention: Option<Duration>,
    pub page: PageConfig,
}

#[derive(Clone, Debug)]
pub struct PageConfig {
    pub target_size_bytes: usize,
    pub max_rows: usize,
    pub max_age: Duration,
    pub rows_per_block: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                path: "line".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: ".data".to_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            segment_duration: Duration::from_secs(60 * 60),
            retention: None,
            page: PageConfig::default(),
        }
    }
}

impl Default for PageConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 1024 * 1024,
            max_rows: 16_384,
            max_age: Duration::from_secs(1),
            rows_per_block: 256,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.segment_duration.is_zero() {
            return Err(crate::Error::Invalid(
                "segment_duration must be positive".to_owned(),
            ));
        }
        if self.page.target_size_bytes == 0
            || self.page.max_rows == 0
            || self.page.rows_per_block == 0
        {
            return Err(crate::Error::Invalid(
                "page size, row, and block limits must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}
