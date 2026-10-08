# syntax=docker/dockerfile:1.7
# Build from the repository root: the site reads OpenAPI specs and benchmark
# history from documentation/ at build time.
FROM node:24-alpine AS builder
WORKDIR /workspace/site
COPY site/package.json site/package-lock.json ./
RUN --mount=type=cache,id=telemetry-docs-npm,target=/root/.npm,sharing=shared \
    npm ci --no-audit --no-fund
COPY documentation/ /workspace/documentation/
COPY site/ ./
# Semver releases show in the header; anything else renders as "dev".
ARG VERSION=
# Serve under a sub-path, e.g. /telemetry. Must start with "/" and not end with "/".
ARG DOCS_BASE_PATH=
ENV NEXT_TELEMETRY_DISABLED=1 \
    NEXT_PUBLIC_DOCS_VERSION=${VERSION} \
    DOCS_BASE_PATH=${DOCS_BASE_PATH}
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.29-alpine
ARG DOCS_BASE_PATH=
COPY site/nginx.conf /etc/nginx/conf.d/default.conf
COPY LICENSE /usr/share/nginx/
COPY --from=builder /workspace/site/out /usr/share/nginx/html${DOCS_BASE_PATH}
EXPOSE 8080
