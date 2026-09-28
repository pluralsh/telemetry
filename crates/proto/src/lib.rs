//! Generated internal protocol types for the Telemetry product family.

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("telemetry_descriptor");

pub mod meter {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("meter.internal.v1");
        }
    }
}

pub mod line {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("line.internal.v1");
        }
    }
}

pub mod track {
    pub mod internal {
        pub mod v1 {
            tonic::include_proto!("track.internal.v1");
        }
    }
}

pub mod pseudofs {
    pub mod v1 {
        tonic::include_proto!("pseudofs.v1");
    }
}
