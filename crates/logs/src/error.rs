// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

/// Errors returned by Logs' single-node core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid log data: {0}")]
    Invalid(String),
    #[error("write buffer is full")]
    Backpressure,
    #[error("Logs is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("corrupt Logs record: {0}")]
    Corrupt(String),
    #[error("storage error: {0}")]
    Storage(#[from] common::StorageError),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("query error: {0}")]
    Query(String),
    #[error("regular expression error: {0}")]
    Regex(#[from] regex::Error),
    #[error(transparent)]
    Shard(#[from] sharding::ShardSetError),
}

impl From<common::serde::DeserializeError> for Error {
    fn from(error: common::serde::DeserializeError) -> Self {
        Self::Corrupt(error.message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
