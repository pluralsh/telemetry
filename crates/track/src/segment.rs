// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::Arc;

use slatedb::{PrefixExtractor, PrefixTarget};

use crate::codec::routing_prefix_len;

const EXTRACTOR_NAME: &str = "track-trace/v1";

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceSegmentExtractor;

impl TraceSegmentExtractor {
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for TraceSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            PrefixTarget::Point(bytes) => {
                let result = routing_prefix_len(bytes);
                assert!(
                    result.is_some(),
                    "TraceSegmentExtractor received malformed Track key: {:02x?}",
                    bytes
                );
                result
            }
            PrefixTarget::Prefix(bytes) => routing_prefix_len(bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Namespace, codec::segment_prefix};

    #[test]
    fn extracts_namespace_and_time_segment() {
        let prefix = segment_prefix(&Namespace::new("tenant").unwrap(), -1);
        let extractor = TraceSegmentExtractor;
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(prefix.clone())),
            Some(prefix.len())
        );
        assert_eq!(extractor.name(), "track-trace/v1");
    }
}
