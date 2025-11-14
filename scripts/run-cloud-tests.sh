#!/bin/bash
# Helper script to run cloud integration tests with proper environment setup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Cloud Integration Test Runner${NC}"
echo -e "${BLUE}============================================${NC}"
echo

# Check if running from project root
if [ ! -f "Makefile" ]; then
    echo -e "${RED}Error: This script must be run from the project root${NC}"
    exit 1
fi

# Check which cloud backends are configured
S3_CONFIGURED=0
GCS_CONFIGURED=0
AZURE_CONFIGURED=0

echo -e "${YELLOW}Checking cloud CLI authentication...${NC}"
echo

# Check AWS
if command -v aws &> /dev/null; then
    if aws sts get-caller-identity &> /dev/null; then
        echo -e "${GREEN}✓ AWS CLI authenticated${NC}"
        S3_CONFIGURED=1
        export OBJSTORE_TEST_S3=1
        if [ -z "$OBJSTORE_TEST_S3_REGION" ]; then
            export OBJSTORE_TEST_S3_REGION=us-east-1
        fi
    else
        echo -e "${YELLOW}⚠ AWS CLI not authenticated (run: aws configure)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ AWS CLI not installed${NC}"
fi

# Check GCP
if command -v gcloud &> /dev/null; then
    if gcloud auth application-default print-access-token &> /dev/null; then
        echo -e "${GREEN}✓ GCP CLI authenticated${NC}"
        GCS_CONFIGURED=1
        export OBJSTORE_TEST_GCS=1
        # Get project ID if not set
        if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
            GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project 2>/dev/null)
            if [ -n "$GOOGLE_CLOUD_PROJECT" ]; then
                export GOOGLE_CLOUD_PROJECT
            fi
        fi
    else
        echo -e "${YELLOW}⚠ GCP CLI not authenticated (run: gcloud auth application-default login)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ GCP CLI not installed${NC}"
fi

# Check Azure
if command -v az &> /dev/null; then
    if az account show &> /dev/null; then
        echo -e "${GREEN}✓ Azure CLI authenticated${NC}"
        if [ -n "$OBJSTORE_TEST_AZURE_ACCOUNT" ] && [ -n "$OBJSTORE_TEST_AZURE_KEY" ]; then
            AZURE_CONFIGURED=1
            export OBJSTORE_TEST_AZURE=1
        else
            echo -e "${YELLOW}  Note: Set OBJSTORE_TEST_AZURE_ACCOUNT and OBJSTORE_TEST_AZURE_KEY to run tests${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ Azure CLI not authenticated (run: az login)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Azure CLI not installed${NC}"
fi

echo

# Count configured backends
CONFIGURED_COUNT=$((S3_CONFIGURED + GCS_CONFIGURED + AZURE_CONFIGURED))

if [ $CONFIGURED_COUNT -eq 0 ]; then
    echo -e "${RED}Error: No cloud backends are configured!${NC}"
    echo
    echo "Please authenticate with at least one cloud provider:"
    echo "  - AWS:   aws configure"
    echo "  - GCP:   gcloud auth application-default login"
    echo "  - Azure: az login + set OBJSTORE_TEST_AZURE_ACCOUNT/KEY"
    exit 1
fi

echo -e "${GREEN}Found $CONFIGURED_COUNT configured cloud backend(s)${NC}"
echo

# Parse command line arguments
BACKEND="all"
if [ $# -gt 0 ]; then
    BACKEND=$1
fi

# Build CLI first
echo -e "${BLUE}Building CLI...${NC}"
make build-cli

echo
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Running Tests${NC}"
echo -e "${BLUE}============================================${NC}"
echo

# Run tests based on backend
case $BACKEND in
    s3|aws)
        if [ $S3_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing AWS S3...${NC}"
            make test-cloud-s3
        else
            echo -e "${RED}AWS not configured, skipping${NC}"
        fi
        ;;

    gcs|gcp|google)
        if [ $GCS_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing Google Cloud Storage...${NC}"
            make test-cloud-gcs
        else
            echo -e "${RED}GCP not configured, skipping${NC}"
        fi
        ;;

    azure)
        if [ $AZURE_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing Azure Blob Storage...${NC}"
            make test-cloud-azure
        else
            echo -e "${RED}Azure not configured, skipping${NC}"
        fi
        ;;

    cross)
        if [ $CONFIGURED_COUNT -eq 3 ]; then
            echo -e "${YELLOW}Testing cross-cloud migration...${NC}"
            make test-cloud-cross
        else
            echo -e "${RED}All 3 cloud backends must be configured for cross-cloud tests${NC}"
            exit 1
        fi
        ;;

    all)
        TESTS_RUN=0

        if [ $S3_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing AWS S3...${NC}"
            make test-cloud-s3
            ((TESTS_RUN++))
        fi

        if [ $GCS_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing Google Cloud Storage...${NC}"
            make test-cloud-gcs
            ((TESTS_RUN++))
        fi

        if [ $AZURE_CONFIGURED -eq 1 ]; then
            echo -e "${YELLOW}Testing Azure Blob Storage...${NC}"
            make test-cloud-azure
            ((TESTS_RUN++))
        fi

        if [ $TESTS_RUN -eq 0 ]; then
            echo -e "${RED}No tests were run!${NC}"
            exit 1
        fi
        ;;

    *)
        echo -e "${RED}Unknown backend: $BACKEND${NC}"
        echo
        echo "Usage: $0 [backend]"
        echo
        echo "Backends:"
        echo "  s3, aws     - Test AWS S3"
        echo "  gcs, gcp    - Test Google Cloud Storage"
        echo "  azure       - Test Azure Blob Storage"
        echo "  cross       - Test cross-cloud migration (requires all 3)"
        echo "  all         - Test all configured backends (default)"
        exit 1
        ;;
esac

echo
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}✓ Tests Complete!${NC}"
echo -e "${GREEN}============================================${NC}"
