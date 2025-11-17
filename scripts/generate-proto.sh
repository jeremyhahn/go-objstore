#!/bin/bash

# Script to generate Go code from protobuf definitions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Add Go bin directory to PATH
GOBIN=$(go env GOPATH)/bin
export PATH="${GOBIN}:${PATH}"

echo -e "${GREEN}Generating protobuf code for go-objstore...${NC}"

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo -e "${RED}Error: protoc is not installed${NC}"
    echo "Please install protoc:"
    echo "  macOS: brew install protobuf"
    echo "  Linux: sudo apt-get install -y protobuf-compiler"
    exit 1
fi

# Check if protoc-gen-go is installed
if ! command -v protoc-gen-go &> /dev/null; then
    echo -e "${YELLOW}Installing protoc-gen-go...${NC}"
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

# Check if protoc-gen-go-grpc is installed
if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo -e "${YELLOW}Installing protoc-gen-go-grpc...${NC}"
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Create output directory if it doesn't exist
mkdir -p "${PROJECT_ROOT}/api/proto"

echo -e "${GREEN}Generating Go code from protobuf definitions...${NC}"

# Generate Go code
protoc \
    --proto_path="${PROJECT_ROOT}/api/proto" \
    --go_out="${PROJECT_ROOT}/api/proto" \
    --go_opt=paths=source_relative \
    --go-grpc_out="${PROJECT_ROOT}/api/proto" \
    --go-grpc_opt=paths=source_relative \
    "${PROJECT_ROOT}/api/proto/objstore.proto"

echo -e "${GREEN}Successfully generated protobuf code!${NC}"
echo -e "Generated files:"
echo -e "  - ${PROJECT_ROOT}/api/proto/objstore.pb.go"
echo -e "  - ${PROJECT_ROOT}/api/proto/objstore_grpc.pb.go"
