fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos_with_config(
            prost_config,
            &[
                "proto/meter/internal/v1/writer.proto",
                "proto/line/internal/v1/writer.proto",
                "proto/track/internal/v1/writer.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/meter/internal/v1/writer.proto");
    println!("cargo:rerun-if-changed=proto/line/internal/v1/writer.proto");
    println!("cargo:rerun-if-changed=proto/track/internal/v1/writer.proto");
    Ok(())
}
