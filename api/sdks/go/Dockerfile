# syntax=docker/dockerfile:1.7
# Dockerfile for Go SDK testing
# Context must be the repository ROOT (the go.mod `replace` directive points the
# parent module at ../../../, so the whole repo must be in the build context).
# Image must satisfy the parent module's go directive (>= 1.27.1).
FROM golang:1.27.1-alpine

# Build tools for CGO (race detector needs a C toolchain)
RUN apk add --no-cache git make curl bash gcc musl-dev

WORKDIR /workspace

# Copy the entire repository (needed for the replace directive in go.mod)
COPY . .

WORKDIR /workspace/api/sdks/go

# Resolve dependencies against the in-tree parent module via the replace
# directive. The parent (go-objstore) requires the internal module go-enclave,
# published to Gitea rather than github.com, so resolving it needs GOPRIVATE plus
# git's insteadOf rewrite carrying a read-only token
# (conventions/dependency-resolution.md §6), supplied as an OPTIONAL BuildKit
# secret (the file carries only the tokenised GIT_CONFIG_KEY_0 value, read via a
# quoted command substitution — never sourced as shell). The mount is optional,
# so the same Dockerfile still builds against the public proxy when none is
# provided. The compose file passes it from GOMODAUTH_ENV, which the CI jobs
# stage with the stage-gomodauth composite action.
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
