# Build stage
FROM rust:1.83-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy all source files at once
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY src/ src/

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/otel-log-generator /usr/local/bin/

ENTRYPOINT ["otel-log-generator"]
CMD ["otel", "--help"]
