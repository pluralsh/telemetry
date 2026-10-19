// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

pub mod config;
mod service;

pub use service::Service;

pub async fn open(config: &config::Config) -> pseudofs::Result<(pseudofs::PseudoFs, Service)> {
    let fs = pseudofs::PseudoFs::open(config.filesystem.clone()).await?;
    Ok((
        fs.clone(),
        Service::new(fs, config.max_unary_file_size_bytes, config.search.clone()),
    ))
}
