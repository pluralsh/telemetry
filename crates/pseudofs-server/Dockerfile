# syntax=docker/dockerfile:1.7
FROM rust:1.91-bookworm AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked --version 0.1.78
WORKDIR /workspace

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
# Optional `-C target-cpu` (e.g. x86-64-v3); the binary will not run on older CPUs.
ARG RUST_TARGET_CPU=
COPY --from=planner /workspace/recipe.json recipe.json
COPY crates/proto/proto/ crates/proto/proto/
COPY crates/logs/src/logql/grammar.lalrpop crates/logs/src/logql/grammar.lalrpop
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    RUSTFLAGS="${RUST_TARGET_CPU:+-C target-cpu=$RUST_TARGET_CPU}" cargo chef cook --release --locked --package pseudofs-server --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    RUSTFLAGS="${RUST_TARGET_CPU:+-C target-cpu=$RUST_TARGET_CPU}" cargo build --release --locked --package pseudofs-server \
    && mkdir -p /out \
    && cp target/release/pseudofs-server /out/pseudofs-server

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY config /app/config
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /out/pseudofs-server /usr/local/bin/pseudofs-server
ENV HOME=/var/lib/pseudofs
USER 10001:10001
EXPOSE 9093
ENTRYPOINT ["pseudofs-server"]
CMD ["--config", "/app/config/pseudofs.example.yaml"]
