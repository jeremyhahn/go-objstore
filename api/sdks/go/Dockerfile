# Dockerfile for Go SDK testing
# Context must be the repository ROOT (the go.mod `replace` directive points the
# parent module at ../../../, so the whole repo must be in the build context).
# Image must satisfy the parent module's go directive (>= 1.26.3).
FROM golang:1.26.3-alpine

# Build tools for CGO (race detector needs a C toolchain)
RUN apk add --no-cache git make curl bash gcc musl-dev

WORKDIR /workspace

# Copy the entire repository (needed for the replace directive in go.mod)
COPY . .

WORKDIR /workspace/api/sdks/go

# Resolve dependencies against the in-tree parent module via the replace directive
RUN go mod download

# Default command runs unit tests (with race detector enabled via CGO)
ENV CGO_ENABLED=1
CMD ["go", "test", "-v", "-race", "-timeout=60s", "./..."]
