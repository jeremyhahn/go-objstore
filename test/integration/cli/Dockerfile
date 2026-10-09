# syntax=docker/dockerfile:1.7
# Dockerfile for CLI integration tests
FROM golang:1.27.2-alpine

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

# Download dependencies
# go.mod pins the internal first-party module github.com/jeremyhahn/go-enclave,
# published to the internal Gitea rather than github.com. Resolving it needs
# GOPRIVATE plus git's insteadOf rewrite carrying a read-only token
# (conventions/dependency-resolution.md §6), supplied as an OPTIONAL BuildKit
# secret carrying ONLY the tokenised GIT_CONFIG_KEY_0 value. It is mounted on
# tmpfs for this RUN alone and read via a quoted command substitution — never
# sourced as shell, so a credential containing shell metacharacters is assigned
# literally rather than executed. It is never a build arg or ENV (recoverable
# via docker history) and never written to a layer. The public parts of the
# rewrite are literals below; only the token-bearing key is secret. The mount
# is optional, so the same Dockerfile still builds against the public proxy
# when no secret is provided.
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
RUN go build -tags "local awss3 minio gcpstorage azureblob glacier azurearchive" -o /usr/local/bin/objstore ./cmd/objstore

# Create test storage directory
RUN mkdir -p /test-storage

# Default environment
ENV OBJECTSTORE_BACKEND=local
ENV OBJECTSTORE_BACKEND_PATH=/test-storage

# Run tests by default with integration tag
CMD ["go", "test", "-v", "-tags", "integration,local,awss3,minio,gcpstorage,azureblob,glacier,azurearchive", "./test/integration/cli/...", "-timeout", "10m"]
