use crate::{Namespace, TraceId};

/// Canonical Track routing key shared by shard selection and storage slots.
pub(crate) fn canonical_routing_key(namespace: &Namespace, trace_id: TraceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + namespace.as_bytes().len() + trace_id.as_bytes().len());
    key.extend_from_slice(&(namespace.as_bytes().len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    key.extend_from_slice(trace_id.as_bytes());
    key
}

pub(crate) fn routing_slot(namespace: &Namespace, trace_id: TraceId) -> u16 {
    sharding::RoutingSlot::from_key(&canonical_routing_key(namespace, trace_id)).get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_uses_the_canonical_sharded_track_key() {
        let namespace = Namespace::new("tenant").unwrap();
        let trace_id = TraceId::new([3; 16]).unwrap();
        assert_eq!(
            routing_slot(&namespace, trace_id),
            sharding::hash_routing_key(&canonical_routing_key(&namespace, trace_id))
                .routing_slot()
                .get()
        );
    }
}
