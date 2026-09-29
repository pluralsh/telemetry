// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

/// Errors returned by Line's single-node core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid log data: {0}")]
    Invalid(String),
    #[error("write buffer is full")]
    Backpressure,
    #[error("Line writer is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("corrupt Line record: {0}")]
    Corrupt(String),
    #[error("storage error: {0}")]
    Storage(#[from] common::StorageError),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("compression error: {0}")]
    Compression(#[from] snap::Error),
    #[error("query error: {0}")]
    Query(String),
    #[error("regular expression error: {0}")]
    Regex(#[from] regex::Error),
}

impl From<common::serde::DeserializeError> for Error {
    fn from(error: common::serde::DeserializeError) -> Self {
        Self::Corrupt(error.message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
