use crate::{Labels, Namespace};

/// Canonical Line routing key shared by shard selection and storage slots.
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

pub(crate) fn routing_slot(namespace: &Namespace, labels: &Labels) -> u16 {
    sharding::RoutingSlot::from_key(&canonical_routing_key(namespace, labels)).get()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Label;

    #[test]
    fn slot_uses_the_canonical_sharded_line_key() {
        let namespace = Namespace::new("tenant").unwrap();
        let labels = Labels::new(vec![Label::new("z", "1"), Label::new("a", "2")]).unwrap();
        assert_eq!(
            routing_slot(&namespace, &labels),
            sharding::hash_routing_key(&canonical_routing_key(&namespace, &labels))
                .routing_slot()
                .get()
        );
    }
}
