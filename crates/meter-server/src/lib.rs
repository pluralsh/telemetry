pub mod config;
pub mod openapi;

mod http;
mod internal_writer;
mod state;

pub use http::router;
pub use internal_writer::grpc_service;
pub use state::AppState;

#[cfg(test)]
use config::{Config, Durability, NamespaceConfig, ServerMode, ShardingBackend, StaticOwner};
#[cfg(test)]
use meter::{Label, Namespace, Sample, Series, ShardingOptions};
#[cfg(test)]
use proto::meter::internal::v1::{
    Durability as ProtoDurability, Label as ProtoLabel, Namespace as ProtoNamespace,
    Sample as ProtoSample, Series as ProtoSeries, WriteBatchRequest,
};
#[cfg(test)]
use sharding::{Assignment, Owner, ShardId, ShardMap, ShardRange};
#[cfg(all(test, feature = "kubernetes"))]
use sharding::{balanced_contiguous, kubernetes::membership_changed};
#[cfg(test)]
use state::{assignment_for, meter_config};
#[cfg(test)]
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
mod tests;
