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
COPY --from=planner /workspace/recipe.json recipe.json
# These files are consumed by workspace build scripts while cargo-chef cooks dependencies.
COPY crates/proto/proto/ crates/proto/proto/
COPY crates/line/src/logql/grammar.lalrpop crates/line/src/logql/grammar.lalrpop
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    cargo chef cook --release --locked --package meter-server --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,id=telemetry-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,id=telemetry-cargo-git-${TARGETARCH},target=/usr/local/cargo/git,sharing=shared \
    cargo build --release --locked --package meter-server \
    && mkdir -p /out \
    && cp target/release/meter-server /out/meter-server

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY config /app/config
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /out/meter-server /usr/local/bin/meter-server
ENV HOME=/var/lib/meter
USER 10001:10001
EXPOSE 8080 9090
ENTRYPOINT ["meter-server"]
CMD ["--config", "/app/config/meter.yaml"]
