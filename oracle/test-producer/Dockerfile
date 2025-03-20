# -----------------------------------------------------------------------------
# 1. Global Build Arguments & Metadata
# -----------------------------------------------------------------------------
ARG RUST_VERSION=1.74
ARG DEBIAN_VERSION=bookworm
ARG CHEF_IMAGE=lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-slim-${DEBIAN_VERSION}
ARG RUNTIME_BASE=gcr.io/distroless/cc-debian12:nonroot

ARG TINI_VERSION=v0.19.0
ARG TINI_SHA256=c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee

ARG BUILD_VERSION="0.1.0"
ARG VCS_REF="development"
ARG BUILD_DATE="2023-11-02"

# -----------------------------------------------------------------------------
# 2. Cargo Chef Stage - Prepare Dependency Recipe
# -----------------------------------------------------------------------------
FROM ${CHEF_IMAGE} AS chef
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# -----------------------------------------------------------------------------
# 3. Builder Stage - Compile Rust with Cached Dependencies
# -----------------------------------------------------------------------------
FROM ${CHEF_IMAGE} AS builder

ARG CARGO_BUILD_JOBS="default"
ARG CARGO_PROFILE=release
ENV RUSTFLAGS="-C opt-level=3 -C codegen-units=1"

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    g++ \
    make \
    python3 \
    libsasl2-dev \
    zlib1g-dev \
    binutils \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create schema directories
RUN mkdir -p ../clickhouse 

# Cook dependencies separately
COPY --from=chef /app/recipe.json recipe.json
RUN cargo chef cook --profile ${CARGO_PROFILE} --recipe-path recipe.json

# Copy the ACTUAL schema.proto from the project
COPY ../clickhouse/schema.proto ../clickhouse/schema.proto

# Build the application
COPY . .
RUN cargo build --profile ${CARGO_PROFILE} --locked --jobs "${CARGO_BUILD_JOBS}" \
    && strip target/${CARGO_PROFILE}/qos_kafka_producer \
    && cp target/${CARGO_PROFILE}/qos_kafka_producer /app/qos_kafka_producer

# -----------------------------------------------------------------------------
# 4. Preparation Stage (Tini + Directory Setup)
# -----------------------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS preparation

ARG TINI_VERSION
ARG TINI_SHA256

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Download Tini and verify its checksum
RUN curl -fsSL -o /tini \
      https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-amd64 \
    && echo "${TINI_SHA256}  /tini" | sha256sum --check \
    && chmod +x /tini

# Create log directory (owned by non-root UID 65532)
RUN mkdir -p /var/log/qos_producer \
    && chown 65532:65532 /var/log/qos_producer

# -----------------------------------------------------------------------------
# 5. Final Stage - Minimal Distroless Image
# -----------------------------------------------------------------------------
FROM ${RUNTIME_BASE}

COPY --from=preparation /tini /usr/local/bin/tini
COPY --from=builder /app/qos_kafka_producer /usr/local/bin/qos_kafka_producer
COPY --from=preparation /var/log/qos_producer /var/log/qos_producer

ENV TZ=UTC \
    LANG=C.UTF-8 \
    SSL_CERT_DIR=/etc/ssl/certs \
    RUST_BACKTRACE=1

WORKDIR /
STOPSIGNAL SIGTERM

# Optional healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/qos_kafka_producer", "--health-check"] || exit 1

# Distroless:nonroot automatically runs as UID 65532
USER nonroot

ENTRYPOINT ["/usr/local/bin/tini", "--", "/usr/local/bin/qos_kafka_producer"]

# -----------------------------------------------------------------------------
# 6. OCI Labels for Metadata
# -----------------------------------------------------------------------------
LABEL org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/graphops/qos-oracle" \
      org.opencontainers.image.description="The Graph QoS Oracle Kafka Producer" \
      org.opencontainers.image.vendor="GraphOps" \
      org.opencontainers.image.title="QoS Kafka Producer" \
      maintainer="GraphOps <support@graphops.xyz>" 