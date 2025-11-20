# Dockerfile for CLI integration tests
FROM golang:1.23-alpine

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

# Copy go module files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application
COPY . .

# Build the CLI binary with all backends
RUN go build -tags "local awss3 minio gcpstorage azureblob glacier azurearchive" -o /usr/local/bin/objstore ./cmd/objstore

# Create test storage directory
RUN mkdir -p /test-storage

# Run tests by default with all backends
CMD ["go", "test", "-v", "-tags", "local awss3 minio gcpstorage azureblob glacier azurearchive", "./test/integration/cli/...", "-timeout", "10m"]
