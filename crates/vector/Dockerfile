# syntax=docker/dockerfile:1.7
# usearch's numkong kernels need a newer GCC than bookworm's for arm64 targets.
FROM rust:1.91-trixie AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked --version 0.1.78
WORKDIR /workspace

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
# Optional `-C target-cpu` (e.g. x86-64-v3); the binary will not run on older CPUs.
ARG RUST_TARGET_CPU=
COPY --from=planner /workspace/recipe.json recipe.json
COPY vendor/ vendor/
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    RUSTFLAGS="${RUST_TARGET_CPU:+-C target-cpu=$RUST_TARGET_CPU}" cargo chef cook --release --locked --package plural-vector --features http-server,kubernetes --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    RUSTFLAGS="${RUST_TARGET_CPU:+-C target-cpu=$RUST_TARGET_CPU}" cargo build --release --locked --package plural-vector --features http-server,kubernetes --bin plural-vector \
    && mkdir -p /out \
    && cp target/release/plural-vector /out/plural-vector

FROM gcr.io/distroless/cc-debian13:nonroot
WORKDIR /app
COPY config/vector.example.yaml /app/config/vector.example.yaml
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /out/plural-vector /usr/local/bin/plural-vector
ENV HOME=/var/lib/vector
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["plural-vector"]
CMD ["cluster", "--config", "/app/config/vector.example.yaml"]
