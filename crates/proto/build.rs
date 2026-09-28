fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("telemetry_descriptor.bin");
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);
    prost_config.bytes([".pseudofs.v1"]);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos_with_config(
            prost_config,
            &[
                "proto/meter/internal/v1/writer.proto",
                "proto/line/internal/v1/writer.proto",
                "proto/track/internal/v1/writer.proto",
                "proto/pseudofs/v1/pseudofs.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/meter/internal/v1/writer.proto");
    println!("cargo:rerun-if-changed=proto/line/internal/v1/writer.proto");
    println!("cargo:rerun-if-changed=proto/track/internal/v1/writer.proto");
    println!("cargo:rerun-if-changed=proto/pseudofs/v1/pseudofs.proto");
    Ok(())
}
