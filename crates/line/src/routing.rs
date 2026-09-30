//! Line storage-shard routing: streams hash by namespace plus labels, and
//! each entry picks its routing epoch by its own timestamp.

use std::collections::BTreeMap;

use sharding::{ShardId, ShardMap};

use crate::{Labels, LogBatch, LogEntry, Namespace};

/// Canonical Line routing key used for shard selection.
pub(crate) fn canonical_routing_key(namespace: &Namespace, labels: &Labels) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&(namespace.as_bytes().len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    for label in labels.iter() {
        key.extend_from_slice(&(label.name.len() as u32).to_be_bytes());
        key.extend_from_slice(label.name.as_bytes());
        key.extend_from_slice(&(label.value.len() as u32).to_be_bytes());
        key.extend_from_slice(label.value.as_bytes());
    }
    key
}

/// Shard owning an entry of `labels` timestamped `timestamp_ns`.
pub fn route(
    assignment: &ShardMap,
    namespace: &Namespace,
    labels: &Labels,
    timestamp_ns: i64,
) -> ShardId {
    assignment.route_key(&canonical_routing_key(namespace, labels), timestamp_ns)
}

/// Splits `batches` by the shard owning each entry. A stream whose entries
/// straddle a routing epoch cutover is written to both shards.
pub fn split(
    assignment: &ShardMap,
    namespace: &Namespace,
    batches: Vec<LogBatch>,
) -> BTreeMap<ShardId, Vec<LogBatch>> {
    let mut grouped: BTreeMap<ShardId, Vec<LogBatch>> = BTreeMap::new();
    for batch in batches {
        let key = canonical_routing_key(namespace, &batch.labels);
        if assignment.epochs.len() == 1 {
            grouped
                .entry(assignment.route_key(&key, 0))
                .or_default()
                .push(batch);
            continue;
        }
        let mut by_shard: BTreeMap<ShardId, Vec<LogEntry>> = BTreeMap::new();
        for entry in batch.entries {
            by_shard
                .entry(assignment.route_key(&key, entry.timestamp_ns))
                .or_default()
                .push(entry);
        }
        for (shard, entries) in by_shard {
            grouped
                .entry(shard)
                .or_default()
                .push(LogBatch::new(batch.labels.clone(), entries));
        }
    }
    grouped
}
