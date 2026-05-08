# Multi-stage build for TRACE collector
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

# Create log directory
RUN mkdir -p /data/logs

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/trace-collector /app/trace-collector

# Expose collector port
EXPOSE 8080

# Set log directory
ENV TRACE_LOG_DIR=/data/logs

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/trace-collector"]
