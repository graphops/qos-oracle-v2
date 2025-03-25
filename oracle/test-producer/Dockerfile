# -----------------------------------------------------------------------------
# 1. Global Build Arguments & Metadata
# -----------------------------------------------------------------------------
ARG RUST_VERSION=1.84
ARG DEBIAN_VERSION=bookworm
ARG CHEF_IMAGE=lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-slim-${DEBIAN_VERSION}
ARG RUNTIME_BASE=debian:${DEBIAN_VERSION}-slim

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
# Copy the test-producer code, not the entire context
COPY ./test-producer .
# Generate the Cargo.lock file first
RUN cargo update
RUN cargo chef prepare --recipe-path recipe.json

# -----------------------------------------------------------------------------
# 3. Builder Stage - Compile Rust with Cached Dependencies
# -----------------------------------------------------------------------------
FROM ${CHEF_IMAGE} AS builder

ARG CARGO_BUILD_JOBS="default"
ARG CARGO_PROFILE=release
ENV RUSTFLAGS="-C opt-level=3 -C codegen-units=1"
ENV CXX=/usr/bin/g++
ENV CC=/usr/bin/gcc

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    g++ \
    build-essential \
    cmake \
    make \
    python3 \
    libsasl2-dev \
    zlib1g-dev \
    binutils \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/g++ /usr/bin/c++

WORKDIR /app

# Create the proper directory structure
RUN mkdir -p /app/clickhouse

# Copy the actual schema.proto file from the project root
COPY ./clickhouse/schema.proto /app/clickhouse/schema.proto

# Modify build.rs to use the local path
COPY ./test-producer/build.rs /app/build.rs.original
RUN sed 's|"../clickhouse/schema.proto"|"./clickhouse/schema.proto"|g' /app/build.rs.original > /app/build.rs.new

# Cook dependencies separately
COPY --from=chef /app/recipe.json recipe.json
COPY --from=chef /app/Cargo.lock Cargo.lock
RUN CXX=/usr/bin/g++ CC=/usr/bin/gcc cargo chef cook --profile ${CARGO_PROFILE} --recipe-path recipe.json

# Build the application
COPY ./test-producer .
# Replace the build.rs with our modified version
RUN cp /app/build.rs.new /app/build.rs

# Verify the proto file exists before building
RUN ls -la /app/clickhouse/ && \
    cat /app/build.rs && \
    cargo build --profile ${CARGO_PROFILE} --jobs "${CARGO_BUILD_JOBS}" \
    && strip target/${CARGO_PROFILE}/qos_kafka_producer \
    && cp target/${CARGO_PROFILE}/qos_kafka_producer /app/qos_kafka_producer

# -----------------------------------------------------------------------------
# 4. Preparation Stage (Tini + Directory Setup)
# -----------------------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS preparation

ARG TINI_VERSION
ARG TINI_SHA256

# Install required packages and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Fetch and verify tini
RUN curl -fsSL -o /tini \
      https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-amd64 \
    && echo "${TINI_SHA256}  /tini" | sha256sum -c - \
    && chmod +x /tini

# Create logging directory
RUN mkdir -p /var/log/qos_producer \
    && chown 65532:65532 /var/log/qos_producer

# -----------------------------------------------------------------------------
# 5. Final Stage - Minimal Runtime Image
# -----------------------------------------------------------------------------
FROM ${RUNTIME_BASE}

COPY --from=preparation /tini /usr/local/bin/tini
COPY --from=preparation /var/log/qos_producer /var/log/qos_producer
COPY --from=builder /app/qos_kafka_producer /usr/local/bin/qos_kafka_producer

# Install required runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    zlib1g ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=UTC \
    LANG=C.UTF-8

WORKDIR /
STOPSIGNAL SIGTERM

# Set non-root user (same as distroless)
RUN groupadd --gid 65532 nonroot && \
    useradd --uid 65532 --gid 65532 -s /sbin/nologin nonroot

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