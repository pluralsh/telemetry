//! Generated internal protocol types for the Telemetry product family.

// tonic's generated service traits return `tonic::Status` by value.
#![allow(clippy::result_large_err)]

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("telemetry_descriptor");

pub mod metrics {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("metrics.internal.v1");
        }
    }
}

pub mod logs {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("logs.internal.v1");
        }
    }
}

pub mod traces {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("traces.internal.v1");
        }
    }
}

pub mod pseudofs {
    pub mod v1 {
        tonic::include_proto!("pseudofs.v1");
    }
}

/// Plural Console's gRPC contract, used to report ingested usage.
pub mod plrl {
    tonic::include_proto!("plrl");
}
