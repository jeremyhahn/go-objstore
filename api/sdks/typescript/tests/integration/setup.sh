#!/bin/bash

set -e

echo "Setting up integration test environment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

# Check if go-objstore image exists, if not build it
if ! docker images | grep -q "go-objstore"; then
    echo "Building go-objstore Docker image..."
    cd ../../../..
    docker build -t go-objstore:latest .
    cd -
fi

# Start Docker Compose services
echo "Starting Docker Compose services..."
docker-compose -f tests/integration/docker-compose.yml up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
        echo "REST service is ready"
        break
    fi
    echo "Waiting for REST service... ($((attempt + 1))/$max_attempts)"
    sleep 2
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    echo "Error: Services failed to start within timeout"
    docker-compose -f tests/integration/docker-compose.yml logs
    exit 1
fi

echo "Integration test environment is ready"
