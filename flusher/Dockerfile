# Multi-stage build for TRACE flusher
FROM rust:1.97-alpine AS builder

RUN apk add --no-cache build-base cmake perl

WORKDIR /build/trace

# The flusher depends on the shared Iceberg publisher crate.
COPY iceberg/Cargo.toml ./iceberg/Cargo.toml
COPY iceberg/src ./iceberg/src
COPY flusher/Cargo.toml flusher/Cargo.lock ./flusher/
COPY flusher/src ./flusher/src

# Build release binary
RUN cargo build --release --manifest-path flusher/Cargo.toml

# Runtime image
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

# Create directories
RUN mkdir -p /data/logs /data/dlq

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/trace-flusher /app/trace-flusher

# Set environment defaults
ENV TRACE_LOG_DIR=/data/logs
ENV TRACE_DLQ_DIR=/data/dlq
ENV TRACE_S3_REGION=us-east-1
ENV TRACE_S3_PREFIX=trace-events

ENTRYPOINT ["/app/trace-flusher"]
