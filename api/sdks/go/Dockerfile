# Dockerfile for Go SDK testing
# Context should be the repository root

FROM golang:1.24-alpine

# Install system dependencies including build tools for CGO
RUN apk add --no-cache \
    git \
    make \
    curl \
    bash \
    gcc \
    musl-dev

# Set working directory
WORKDIR /workspace

# Copy entire repository (needed for replace directive in go.mod)
COPY . .

# Set working directory to SDK
WORKDIR /workspace/api/sdks/go

# Download dependencies
RUN go mod download

# Default command runs unit tests (with race detector enabled via CGO)
ENV CGO_ENABLED=1
CMD ["go", "test", "-v", "-race", "-timeout=30s", "./..."]
