# syntax=docker/dockerfile:1.7
# Dockerfile for CLI integration tests
FROM golang:1.27.4-alpine

# Allow Go toolchain auto-upgrade
ENV GOTOOLCHAIN=auto

# Install build dependencies
RUN apk add --no-cache \
    git \
    make \
    bash \
    curl

# Set working directory
WORKDIR /app

# Copy go module files first for better caching
COPY go.mod go.sum ./

# Download dependencies; resolve the private go-enclave module through an optional BuildKit secret holding only the token key, read on tmpfs and never kept in a layer.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        export GOPRIVATE='github.com/jeremyhahn/*'; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="$(cat /run/secrets/gomodauth)"; \
        export GIT_CONFIG_VALUE_0='https://github.com/jeremyhahn/'; \
    fi; \
    go mod download

# Copy the rest of the application
COPY . .

# Build the CLI binary with all backends
RUN go build -tags "local awss3 gcpstorage azureblob glacier azurearchive" -o /usr/local/bin/objstore ./cmd/objstore

# Create test storage directory
RUN mkdir -p /test-storage

# Default environment
ENV OBJECTSTORE_BACKEND=local
ENV OBJECTSTORE_BACKEND_PATH=/test-storage

# Run tests by default with integration tag
CMD ["go", "test", "-v", "-tags", "integration,local,awss3,gcpstorage,azureblob,glacier,azurearchive", "./test/integration/cli/...", "-timeout", "10m"]
