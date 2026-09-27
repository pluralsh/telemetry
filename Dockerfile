# syntax=docker/dockerfile:1.7
FROM debian:bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://mise.run | MISE_INSTALL_PATH=/usr/local/bin/mise sh
WORKDIR /workspace
COPY mise.toml .
RUN mise trust --yes mise.toml && mise install
COPY . .
RUN mise exec -- cargo build --release --locked --package meter-server --package regression

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home meter
WORKDIR /app
COPY --from=builder /workspace/config /app/config
COPY LICENSE THIRD_PARTY_NOTICES.md /app/
COPY --from=builder /workspace/target/release/meter-server /usr/local/bin/meter-server
USER meter
EXPOSE 8080 9090
ENTRYPOINT ["meter-server"]
CMD ["--config", "/app/config/meter.yaml"]

FROM runtime AS regression
COPY --from=builder /workspace/target/release/regression /usr/local/bin/regression
ENTRYPOINT ["regression"]
CMD []
