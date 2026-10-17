# Build stage
FROM rust:1.97-alpine AS builder

RUN apk add --no-cache build-base cmake perl

WORKDIR /build/trace

COPY iceberg/Cargo.toml ./iceberg/Cargo.toml
COPY iceberg/src ./iceberg/src
COPY syncer/Cargo.toml syncer/Cargo.lock ./syncer/
COPY syncer/src ./syncer/src

RUN cargo build --release --manifest-path syncer/Cargo.toml --target-dir /build/trace/target

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/trace/target/release/trace-syncer /app/trace-syncer

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD pgrep -x trace-syncer || exit 1

# Run the syncer
ENTRYPOINT ["/app/trace-syncer"]
CMD ["--help"]
