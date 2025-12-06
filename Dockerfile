# Multi-stage build for go-objstore
# Stage 1: Build the application
FROM golang:1.25.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates make

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build arguments for selecting backends
# Default: all backends (Makefile defaults). Override with BUILD_BACKENDS=local for minimal build
ARG BUILD_BACKENDS="all"

# Build the binaries based on BUILD_BACKENDS
# Note: Makefile defaults to all backends enabled, so no flags needed for "all"
RUN if [ "$BUILD_BACKENDS" = "local" ]; then \
        echo "Building with LOCAL backend only..." && \
        make build-cli WITH_LOCAL=1 WITH_AWS_S3=0 WITH_MINIO=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0 && \
        make build-server WITH_LOCAL=1 WITH_AWS_S3=0 WITH_MINIO=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0; \
    else \
        echo "Building with ALL backends (using Makefile defaults)..." && \
        make build-cli && \
        make build-server; \
    fi

# Stage 2: Create minimal runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

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

# Default command runs the server
CMD ["/app/objstore-server"]
