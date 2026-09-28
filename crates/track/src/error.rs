// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid trace data: {0}")]
    Invalid(String),
    #[error("corrupt Track record: {0}")]
    Corrupt(String),
    #[error("storage error: {0}")]
    Storage(#[from] common::StorageError),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf error: {0}")]
    Protobuf(#[from] prost::DecodeError),
    #[error("compression error: {0}")]
    Compression(#[from] snap::Error),
    #[error(transparent)]
    TraceQl(#[from] crate::traceql::QueryError),
}

impl From<common::serde::DeserializeError> for Error {
    fn from(error: common::serde::DeserializeError) -> Self {
        Self::Corrupt(error.message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
