use crate::{Namespace, TraceId};

/// Canonical Track routing key used for shard selection.
pub(crate) fn canonical_routing_key(namespace: &Namespace, trace_id: TraceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + namespace.as_bytes().len() + trace_id.as_bytes().len());
    key.extend_from_slice(&(namespace.as_bytes().len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    key.extend_from_slice(trace_id.as_bytes());
    key
}
