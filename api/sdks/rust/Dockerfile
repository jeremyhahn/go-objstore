# Dockerfile for Rust SDK testing
FROM rust:1.83-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    curl \
    git \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy proto files (needed by build.rs)
COPY api/proto /proto

# Copy Cargo files and build script for dependency caching
COPY api/sdks/rust/Cargo.toml ./
COPY api/sdks/rust/Cargo.lock* ./
COPY api/sdks/rust/build.rs ./

# Create dummy source to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies
RUN cargo build --release && \
    rm -rf src target/release/deps/go_objstore* || true

# Copy actual source code
COPY api/sdks/rust/src ./src
COPY api/sdks/rust/tests ./tests
COPY api/sdks/rust/examples ./examples

# Default command runs unit tests
CMD ["cargo", "test", "--lib", "--bins"]
