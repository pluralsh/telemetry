fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost_config = prost_build::Config::new();
    prost_config.protoc_executable(protoc);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos_with_config(
            prost_config,
            &["proto/jaeger/model.proto", "proto/jaeger/collector.proto"],
            &["proto/jaeger"],
        )?;
    println!("cargo:rerun-if-changed=proto/jaeger");
    Ok(())
}
