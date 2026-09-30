use crate::{Labels, Namespace};

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
