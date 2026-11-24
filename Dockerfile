# syntax=docker/dockerfile:1.7
# Multi-stage build for go-objstore
# Stage 1: Build the application
FROM golang:1.27.4-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates make

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
# Private modules resolve through an optional BuildKit secret holding only the tokenised insteadOf key, read with a quoted substitution and never stored in a layer.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        export GOPRIVATE='github.com/jeremyhahn/*'; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="$(cat /run/secrets/gomodauth)"; \
        export GIT_CONFIG_VALUE_0='https://github.com/jeremyhahn/'; \
    fi; \
    go mod download

# Copy source code
COPY . .

# Build arguments for selecting backends
# Default: all backends (Makefile defaults). Override with BUILD_BACKENDS=local for minimal build
ARG BUILD_BACKENDS="all"

# Build the binaries based on BUILD_BACKENDS
# Note: Makefile defaults to all backends enabled, so no flags needed for "all"
RUN if [ "$BUILD_BACKENDS" = "local" ]; then \
        echo "Building with LOCAL backend only..." && \
        make build-cli WITH_LOCAL=1 WITH_AWS_S3=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0 && \
        make build-server WITH_LOCAL=1 WITH_AWS_S3=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0; \
    else \
        echo "Building with ALL backends (using Makefile defaults)..." && \
        make build-cli && \
        make build-server; \
    fi

# Stage 2: minimal runtime image, pinned to a minor tag that still receives patch updates.
FROM alpine:3.24

# Install runtime dependencies and upgrade the OpenSSL libraries to the base line's latest patch.
RUN apk add --no-cache ca-certificates tzdata \
    && apk upgrade --no-cache libcrypto3 libssl3

# Create non-root user
RUN adduser -D -u 1000 appuser

# Set working directory
WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/bin/objstore /app/objstore
COPY --from=builder /build/bin/objstore-server /app/objstore-server

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose common ports (REST: 8080, gRPC: 50051, QUIC: 4433, MCP: 8081)
EXPOSE 8080 50051 4433 8081

# The server defaults suit a host install, so the container names its root and binds every interface; publish only the ports you need.
CMD ["/app/objstore-server", \
     "-path", "/tmp/objstore", \
     "-grpc-addr", "0.0.0.0:50051", \
     "-rest-host", "0.0.0.0", \
     "-quic=true", "-quic-addr", "0.0.0.0:4433", \
     "-mcp=true", "-mcp-addr", "0.0.0.0:8081"]
