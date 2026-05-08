# Build stage
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# Runtime stage
FROM alpine:3

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/target/release/trace-compactor /app/trace-compactor

ENV TRACE_LOG_DIR=/data/logs
ENV RUST_LOG=info

ENTRYPOINT ["/app/trace-compactor"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep -x trace-compactor || exit 1
