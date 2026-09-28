// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![forbid(unsafe_code)]

pub mod config;
mod service;

pub use service::Service;

pub async fn open(config: &config::Config) -> pseudofs::Result<(pseudofs::PseudoFs, Service)> {
    let fs = pseudofs::PseudoFs::open(config.filesystem.clone()).await?;
    Ok((
        fs.clone(),
        Service::new(fs, config.max_unary_file_size_bytes),
    ))
}
