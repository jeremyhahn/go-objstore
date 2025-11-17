#!/bin/bash
# Test script for all object storage backends

set -e  # Exit on error

OBJSTORE_BIN="${OBJSTORE_BIN:-./bin/objstore}"
TEST_FILE="test-data.txt"
TEST_KEY="test/test-file.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🧪 Testing objstore with multiple backends"
echo "=========================================="
echo

# Create test data
echo "Hello from objstore!" > "$TEST_FILE"

# Function to test a backend
test_backend() {
    local backend=$1
    local config_file=$2

    echo -e "${YELLOW}Testing $backend backend...${NC}"

    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Config file not found: $config_file${NC}"
        echo "   Create it from ${config_file}.example"
        return 1
    fi

    # Test put
    echo "  📤 Uploading file..."
    if $OBJSTORE_BIN --config "$config_file" put "$TEST_FILE" "$TEST_KEY"; then
        echo -e "  ${GREEN}✓ Upload successful${NC}"
    else
        echo -e "  ${RED}✗ Upload failed${NC}"
        return 1
    fi

    # Test list
    echo "  📋 Listing objects..."
    if $OBJSTORE_BIN --config "$config_file" list; then
        echo -e "  ${GREEN}✓ List successful${NC}"
    else
        echo -e "  ${RED}✗ List failed${NC}"
        return 1
    fi

    # Test exists
    echo "  🔍 Checking existence..."
    if $OBJSTORE_BIN --config "$config_file" exists "$TEST_KEY"; then
        echo -e "  ${GREEN}✓ Exists check successful${NC}"
    else
        echo -e "  ${RED}✗ Exists check failed${NC}"
        return 1
    fi

    # Test get
    echo "  📥 Downloading file..."
    if $OBJSTORE_BIN --config "$config_file" get "$TEST_KEY" "downloaded-${backend}.txt"; then
        echo -e "  ${GREEN}✓ Download successful${NC}"
        rm -f "downloaded-${backend}.txt"
    else
        echo -e "  ${RED}✗ Download failed${NC}"
        return 1
    fi

    # Test delete
    echo "  🗑️  Deleting file..."
    if $OBJSTORE_BIN --config "$config_file" delete "$TEST_KEY"; then
        echo -e "  ${GREEN}✓ Delete successful${NC}"
    else
        echo -e "  ${RED}✗ Delete failed${NC}"
        return 1
    fi

    echo -e "${GREEN}✅ $backend backend: ALL TESTS PASSED${NC}"
    echo
    return 0
}

# Test results tracking
PASSED=0
FAILED=0
BACKENDS=()
RESULTS=()

# Test Local backend
if test_backend "local" ".objstore.yaml"; then
    ((PASSED++))
    BACKENDS+=("local")
    RESULTS+=("PASS")
else
    ((FAILED++))
    BACKENDS+=("local")
    RESULTS+=("FAIL")
fi

# Test S3 backend
if test_backend "s3" ".objstore-s3.yaml"; then
    ((PASSED++))
    BACKENDS+=("s3")
    RESULTS+=("PASS")
else
    ((FAILED++))
    BACKENDS+=("s3")
    RESULTS+=("FAIL")
fi

# Test GCS backend
if test_backend "gcs" ".objstore-gcs.yaml"; then
    ((PASSED++))
    BACKENDS+=("gcs")
    RESULTS+=("PASS")
else
    ((FAILED++))
    BACKENDS+=("gcs")
    RESULTS+=("FAIL")
fi

# Test Azure backend
if test_backend "azure" ".objstore-azure.yaml"; then
    ((PASSED++))
    BACKENDS+=("azure")
    RESULTS+=("PASS")
else
    ((FAILED++))
    BACKENDS+=("azure")
    RESULTS+=("FAIL")
fi

# Cleanup
rm -f "$TEST_FILE"

# Summary
echo "=========================================="
echo "📊 Test Summary"
echo "=========================================="
echo
for i in "${!BACKENDS[@]}"; do
    if [ "${RESULTS[$i]}" = "PASS" ]; then
        echo -e "${GREEN}✅ ${BACKENDS[$i]}: PASSED${NC}"
    else
        echo -e "${RED}❌ ${BACKENDS[$i]}: FAILED${NC}"
    fi
done
echo
echo -e "Total: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some tests failed${NC}"
    exit 1
fi
