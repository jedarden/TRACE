# Build stage
FROM rust:1.83-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconf openssl-dev

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

# Build the binary
RUN cargo build --release

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

# Copy the binary
COPY --from=builder /build/target/release/trace-analytics /usr/local/bin/trace-analytics

# Create directories for data and reports
RUN mkdir -p /data/analytics /data/reports

# Set permissions
RUN chmod +x /usr/local/bin/trace-analytics

# Set the entrypoint
ENTRYPOINT ["trace-analytics"]

# Default command (list available reports)
CMD ["list"]
