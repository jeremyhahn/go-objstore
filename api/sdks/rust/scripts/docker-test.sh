#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GO_OBJSTORE_ROOT="$(cd "$PROJECT_ROOT/../../.." && pwd)"

echo "==> Starting Docker integration tests for Rust SDK"
echo "Project root: $PROJECT_ROOT"
echo "Go objstore root: $GO_OBJSTORE_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create temporary storage directory
TEMP_STORAGE=$(mktemp -d)
trap "rm -rf $TEMP_STORAGE" EXIT

echo -e "${YELLOW}==> Creating temporary storage at: $TEMP_STORAGE${NC}"

# Check if go-objstore binary exists
if [ ! -f "$GO_OBJSTORE_ROOT/go-objstore" ]; then
    echo -e "${YELLOW}==> Building go-objstore binary...${NC}"
    cd "$GO_OBJSTORE_ROOT"
    make build || {
        echo -e "${RED}Failed to build go-objstore binary${NC}"
        exit 1
    }
fi

# Start go-objstore server with local storage backend
echo -e "${YELLOW}==> Starting go-objstore server...${NC}"

# Create config for local storage
cat > "$TEMP_STORAGE/config.yaml" <<EOF
server:
  rest:
    enabled: true
    port: 8080
  grpc:
    enabled: true
    port: 50051
  http3:
    enabled: false
    port: 4433

storage:
  backend: local
  local:
    path: $TEMP_STORAGE/data

logging:
  level: info
  format: json
EOF

# Create data directory
mkdir -p "$TEMP_STORAGE/data"

# Start the server in background
cd "$GO_OBJSTORE_ROOT"
./go-objstore serve --config "$TEMP_STORAGE/config.yaml" &
SERVER_PID=$!

# Ensure server is killed on exit
trap "kill $SERVER_PID 2>/dev/null || true; rm -rf $TEMP_STORAGE" EXIT

echo -e "${YELLOW}==> Waiting for server to be ready...${NC}"

# Wait for REST server to be ready
MAX_RETRIES=30
RETRY_COUNT=0
while ! curl -s http://localhost:8080/health > /dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}Server failed to start within timeout${NC}"
        kill $SERVER_PID 2>/dev/null || true
        exit 1
    fi
    echo "Waiting for server... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 1
done

echo -e "${GREEN}==> Server is ready!${NC}"

# Wait for gRPC server (give it a bit more time)
sleep 2

# Run integration tests
echo -e "${YELLOW}==> Running integration tests...${NC}"
cd "$PROJECT_ROOT"

export OBJSTORE_REST_URL="http://localhost:8080"
export OBJSTORE_GRPC_URL="http://localhost:50051"

# Run tests with --ignored flag to include integration tests
if cargo test --test integration_test -- --ignored --test-threads=1; then
    echo -e "${GREEN}==> All integration tests passed!${NC}"
    TEST_RESULT=0
else
    echo -e "${RED}==> Some integration tests failed${NC}"
    TEST_RESULT=1
fi

# Stop the server
echo -e "${YELLOW}==> Stopping server...${NC}"
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo -e "${GREEN}==> Docker integration tests completed${NC}"

exit $TEST_RESULT
