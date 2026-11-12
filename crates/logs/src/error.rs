// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

/// Errors returned by Logs' single-node core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid log data: {0}")]
    Invalid(String),
    /// The write budget is full, or ingest admission is shedding while L0
    /// compaction catches up.
    #[error("writer is at capacity, retry later")]
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

impl Error {
    /// A copy for callers sharing one failed query: the same kind where the
    /// source can be cloned, and one that maps to the same status otherwise.
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            Self::Invalid(message) => Self::Invalid(message.clone()),
            Self::Backpressure => Self::Backpressure,
            Self::Unavailable(message) => Self::Unavailable(message.clone()),
            Self::Corrupt(message) => Self::Corrupt(message.clone()),
            Self::Storage(error) => Self::Storage(error.clone()),
            Self::Json(error) => Self::Corrupt(error.to_string()),
            Self::Query(message) => Self::Query(message.clone()),
            Self::Regex(error) => Self::Query(error.to_string()),
            Self::Shard(error) => Self::Unavailable(error.to_string()),
        }
    }

    /// The `reason` label of `logs_query_errors_total`.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Invalid(_) | Self::Query(_) | Self::Regex(_) => "invalid",
            Self::Backpressure => "backpressure",
            Self::Unavailable(_) | Self::Shard(_) => "unavailable",
            Self::Storage(_) => "storage",
            Self::Corrupt(_) | Self::Json(_) => "corrupt",
        }
    }
}

impl From<common::serde::DeserializeError> for Error {
    fn from(error: common::serde::DeserializeError) -> Self {
        Self::Corrupt(error.message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
