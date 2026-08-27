# syntax=docker/dockerfile:1.7
# Multi-stage build for go-objstore
# Stage 1: Build the application
FROM golang:1.26.7-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates make

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
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
#
# The CI `docker` job supplies this secret. The SDK integration compose harness
# also builds this image (its objstore-server service) but does NOT supply the
# secret yet, so those builds cannot resolve go-enclave — tracked in issue #36.
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
        make build-cli WITH_LOCAL=1 WITH_AWS_S3=0 WITH_MINIO=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0 && \
        make build-server WITH_LOCAL=1 WITH_AWS_S3=0 WITH_MINIO=0 WITH_GCP_STORAGE=0 WITH_AZURE_BLOB=0 WITH_GLACIER=0 WITH_AZURE_ARCHIVE=0; \
    else \
        echo "Building with ALL backends (using Makefile defaults)..." && \
        make build-cli && \
        make build-server; \
    fi

# Stage 2: Create minimal runtime image
# Pinned to a minor tag, not :latest. An unpinned base makes the image's
# security posture depend on the day it was built: the same Dockerfile
# produced an image with 15 CRITICAL/HIGH OpenSSL CVEs (libcrypto3 3.5.5-r0)
# and, days later, a clean one (3.5.7-r0). 3.24 still receives patch updates,
# so this pins the line without freezing out fixes.
FROM alpine:3.24

# Install runtime dependencies, and upgrade the OpenSSL libraries to pull the
# published patch for the base image's libcrypto3/libssl3. This realizes the
# 3.24 pin note above ("still receives patch updates … without freezing out
# fixes"): the base layer shipped 3.5.7-r0, one patch behind the fix for
# CVE-2026-14456 (OpenSSL DoS, HIGH), which is published as 3.5.8-r0.
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

# Default command runs the server
CMD ["/app/objstore-server"]
