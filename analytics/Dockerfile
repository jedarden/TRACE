# Build stage
FROM rust:1.83-alpine AS builder

WORKDIR /build

# Install build dependencies. g++ is required: the bundled duckdb feature
# compiles a large C++ library via the cc crate.
RUN apk add --no-cache musl-dev pkgconf openssl-dev g++

# Copy Cargo files plus everything the release build embeds at compile time
# (queries.rs and web.rs include_str! these; schemas/ and docs/ are used by
# test targets only, which the image build does not compile).
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ ./src/
COPY queries/ ./queries/
COPY static/ ./static/

# Build the binary
RUN cargo build --release

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies. libstdc++ satisfies the duckdb-linked
# binary's dynamic C++ runtime dependency.
RUN apk add --no-cache ca-certificates libstdc++

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
