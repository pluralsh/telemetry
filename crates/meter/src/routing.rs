use crate::{Label, Namespace};

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
