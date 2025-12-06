#!/bin/bash

set -e

echo "Tearing down integration test environment..."

# Stop and remove Docker Compose services
docker-compose -f tests/integration/docker-compose.yml down -v

echo "Integration test environment cleaned up"
