# syntax=docker/dockerfile:1.7
FROM rust:1.91-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,id=telemetry-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=telemetry-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=telemetry-target,target=/workspace/target,sharing=locked \
    cargo build --release --locked --package regression \
    && mkdir -p /out \
    && cp target/release/regression /out/regression

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /out/regression /usr/local/bin/regression
USER 65532:65532
ENTRYPOINT ["regression"]
