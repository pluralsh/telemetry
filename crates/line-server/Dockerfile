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
    cargo build --release --locked --package line-server \
    && mkdir -p /out \
    && cp target/release/line-server /out/line-server

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY config /app/config
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /out/line-server /usr/local/bin/line-server
ENV HOME=/var/lib/line
USER 10001:10001
EXPOSE 3100
ENTRYPOINT ["line-server"]
CMD ["--config", "/app/config/line.example.yaml"]
