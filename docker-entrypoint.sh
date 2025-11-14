#!/bin/bash
set -e

# Wait for fake-gcs to be ready
echo "Waiting for services to be ready..."
for i in $(seq 1 100); do
    if curl -sS -o /dev/null http://fake-gcs:4443/; then
        echo "✓ Services are ready"
        break
    fi
    sleep 0.2
done

# Download dependencies
echo "Downloading Go dependencies..."
/usr/local/go/bin/go mod download

# Create coverage directory
mkdir -p coverage

# Run integration tests
echo "Running integration tests..."
/usr/local/go/bin/go test \
    -tags=integration \
    -coverpkg="${PKG_COVER}" \
    -coverprofile=coverage/integration.out \
    -v \
    ./pkg/... \
    ./integration
