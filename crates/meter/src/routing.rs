use std::collections::BTreeMap;

use sharding::{ShardId, ShardMap};

use crate::{Label, Namespace, Sample, Series};

/// Canonical Meter routing key used for shard selection.
pub(crate) fn canonical_routing_key(namespace: &Namespace, labels: &[Label]) -> Vec<u8> {
    let mut labels: Vec<&Label> = labels.iter().collect();
    if !labels.is_sorted() {
        labels.sort_unstable();
    }
    let mut key = Vec::new();
    key.extend_from_slice(namespace.as_bytes());
    for label in labels {
        key.push(0);
        key.extend_from_slice(label.name.as_bytes());
        key.push(0);
        key.extend_from_slice(label.value.as_bytes());
    }
    key
}

/// Shard owning a sample of `labels` timestamped `timestamp_ms`.
pub fn route(
    assignment: &ShardMap,
    namespace: &Namespace,
    labels: &[Label],
    timestamp_ms: i64,
) -> ShardId {
    assignment.route_key(
        &canonical_routing_key(namespace, labels),
        timestamp_ms.saturating_mul(1_000_000),
    )
}

/// Splits `series` by the shard owning each sample. A series whose samples
/// straddle a routing epoch cutover is written to both shards.
pub fn split(
    assignment: &ShardMap,
    namespace: &Namespace,
    series: Vec<Series>,
) -> BTreeMap<ShardId, Vec<Series>> {
    let mut grouped: BTreeMap<ShardId, Vec<Series>> = BTreeMap::new();
    for item in series {
        let key = canonical_routing_key(namespace, &item.labels);
        if assignment.epochs.len() == 1 {
            grouped
                .entry(assignment.route_key(&key, 0))
                .or_default()
                .push(item);
            continue;
        }
        let mut by_shard: BTreeMap<ShardId, Vec<Sample>> = BTreeMap::new();
        for sample in &item.samples {
            let shard = assignment.route_key(&key, sample.timestamp_ms.saturating_mul(1_000_000));
            by_shard.entry(shard).or_default().push(sample.clone());
        }
        if by_shard.len() <= 1 {
            let shard = by_shard.into_keys().next().unwrap_or_else(|| {
                assignment.route_key(&key, common::time::now_ms().saturating_mul(1_000_000))
            });
            grouped.entry(shard).or_default().push(item);
            continue;
        }
        for (shard, samples) in by_shard {
            grouped.entry(shard).or_default().push(Series {
                labels: item.labels.clone(),
                metric_type: item.metric_type,
                unit: item.unit.clone(),
                description: item.description.clone(),
                samples,
            });
        }
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_order_independent_and_namespace_sensitive() {
        let labels = vec![Label::new("z", "1"), Label::new("a", "2")];
        let reversed = labels.iter().cloned().rev().collect::<Vec<_>>();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();

        assert_eq!(
            canonical_routing_key(&a, &labels),
            canonical_routing_key(&a, &reversed)
        );
        assert_ne!(
            canonical_routing_key(&a, &labels),
            canonical_routing_key(&b, &labels)
        );
    }
}
