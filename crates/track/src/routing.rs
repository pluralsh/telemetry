//! Track storage-shard routing: a trace hashes by namespace plus trace ID
//! and picks its routing epoch by its earliest span start.

use sharding::{ShardId, ShardMap};

use crate::{Namespace, Trace, TraceId};

/// Canonical Track routing key used for shard selection.
pub(crate) fn canonical_routing_key(namespace: &Namespace, trace_id: TraceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + namespace.as_bytes().len() + trace_id.as_bytes().len());
    key.extend_from_slice(&(namespace.as_bytes().len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    key.extend_from_slice(trace_id.as_bytes());
    key
}

/// Shard owning a trace (or partial trace) whose earliest span starts at
/// `start_ns`.
pub fn route(
    assignment: &ShardMap,
    namespace: &Namespace,
    trace_id: TraceId,
    start_ns: u64,
) -> ShardId {
    assignment.route_key(
        &canonical_routing_key(namespace, trace_id),
        i64::try_from(start_ns).unwrap_or(i64::MAX),
    )
}

pub fn route_trace(assignment: &ShardMap, namespace: &Namespace, trace: &Trace) -> ShardId {
    route(
        assignment,
        namespace,
        trace.trace_id,
        trace.timestamp_range().0,
    )
}

/// Every shard that may hold spans of `trace_id` across routing epochs.
pub fn trace_shards(
    assignment: &ShardMap,
    namespace: &Namespace,
    trace_id: TraceId,
) -> Vec<ShardId> {
    assignment.shards_for_key(&canonical_routing_key(namespace, trace_id))
}
