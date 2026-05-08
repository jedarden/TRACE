# Multi-stage build for TRACE flusher
FROM rust:1.83-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build release binary
RUN cargo build --release

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

ENTRYPOINT ["/app/trace-flusher"]
