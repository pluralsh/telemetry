// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

const ROUTING_DOMAIN: &[u8] = b"plural-telemetry-pseudofs-tenant-routing-v1\0";

pub(crate) fn routing_slot(tenant: &str) -> u16 {
    let mut key = Vec::with_capacity(ROUTING_DOMAIN.len() + tenant.len());
    key.extend_from_slice(ROUTING_DOMAIN);
    key.extend_from_slice(tenant.as_bytes());
    sharding::RoutingSlot::from_key(&key).get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_routing_is_stable_and_tenant_specific() {
        assert_eq!(routing_slot("acme"), routing_slot("acme"));
        assert_ne!(routing_slot("acme"), routing_slot("globex"));
    }
}
