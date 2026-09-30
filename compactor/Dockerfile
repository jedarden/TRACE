# Build stage
FROM rust:1.97-alpine AS builder

RUN apk add --no-cache build-base cmake perl

WORKDIR /build/trace

COPY iceberg/Cargo.toml ./iceberg/Cargo.toml
COPY iceberg/src ./iceberg/src
COPY compactor/Cargo.toml compactor/Cargo.lock ./compactor/
COPY compactor/src ./compactor/src

RUN cargo build --release --manifest-path compactor/Cargo.toml

# Runtime stage
FROM alpine:3

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /build/trace/target/release/trace-compactor /app/trace-compactor

ENV TRACE_LOG_DIR=/data/logs
ENV RUST_LOG=info

ENTRYPOINT ["/app/trace-compactor"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep -x trace-compactor || exit 1
