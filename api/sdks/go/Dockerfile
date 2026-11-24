# syntax=docker/dockerfile:1.7
# Go SDK test image; the build context is the repository root, which the go.mod replace points at.
FROM golang:1.27.4-alpine

# Build tools for CGO (race detector needs a C toolchain)
RUN apk add --no-cache git make curl bash gcc musl-dev

WORKDIR /workspace

# Copy the entire repository (needed for the replace directive in go.mod)
COPY . .

WORKDIR /workspace/api/sdks/go

# Resolve the in-tree parent module via replace; private modules use the optional gomodauth secret.
RUN --mount=type=secret,id=gomodauth \
    set -eu; \
    if [ -s /run/secrets/gomodauth ]; then \
        export GOPRIVATE='github.com/jeremyhahn/*'; \
        export GIT_CONFIG_COUNT=1; \
        export GIT_CONFIG_KEY_0="$(cat /run/secrets/gomodauth)"; \
        export GIT_CONFIG_VALUE_0='https://github.com/jeremyhahn/'; \
    fi; \
    go mod download

# Default command runs unit tests (with race detector enabled via CGO)
ENV CGO_ENABLED=1
CMD ["go", "test", "-v", "-race", "-timeout=60s", "./..."]
