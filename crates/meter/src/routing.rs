use crate::{Label, Namespace};

/// Canonical Meter routing key shared by shard selection and storage slots.
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

pub(crate) fn routing_slot(namespace: &Namespace, labels: &[Label]) -> u16 {
    sharding::RoutingSlot::from_key(&canonical_routing_key(namespace, labels)).get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_is_order_independent_and_namespace_sensitive() {
        let labels = vec![Label::new("z", "1"), Label::new("a", "2")];
        let reversed = labels.iter().cloned().rev().collect::<Vec<_>>();
        let a = Namespace::new("a").unwrap();
        let b = Namespace::new("b").unwrap();

        assert_eq!(routing_slot(&a, &labels), routing_slot(&a, &reversed));
        assert_ne!(routing_slot(&a, &labels), routing_slot(&b, &labels));
    }
}
