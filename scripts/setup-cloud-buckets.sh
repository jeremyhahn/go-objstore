#!/bin/bash
# Script to create cloud buckets/containers for integration tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

BUCKET_NAME="go-objstore-integration-test"
CONTAINER_NAME="go-objstore-integration-test"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Cloud Bucket/Container Setup${NC}"
echo -e "${BLUE}============================================${NC}"
echo
echo "This script will create buckets/containers for cloud integration tests:"
echo "  - AWS S3:   $BUCKET_NAME"
echo "  - GCS:      $BUCKET_NAME"
echo "  - Azure:    $CONTAINER_NAME"
echo

# AWS S3
if command -v aws &> /dev/null; then
    echo -e "${YELLOW}Setting up AWS S3...${NC}"

    if aws sts get-caller-identity &> /dev/null; then
        REGION="${AWS_DEFAULT_REGION:-us-east-1}"

        # Check if bucket exists
        if aws s3 ls "s3://$BUCKET_NAME" 2>/dev/null; then
            echo -e "${GREEN}✓ S3 bucket already exists: $BUCKET_NAME${NC}"
        else
            # Try to create bucket
            if aws s3 mb "s3://$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
                echo -e "${GREEN}✓ Created S3 bucket: $BUCKET_NAME in $REGION${NC}"
            else
                echo -e "${RED}✗ Failed to create S3 bucket (permission denied)${NC}"
                echo -e "${YELLOW}  Please create bucket manually or ask your admin:${NC}"
                echo -e "${YELLOW}  aws s3 mb s3://$BUCKET_NAME --region $REGION${NC}"
            fi
        fi
    else
        echo -e "${YELLOW}⚠ AWS CLI not authenticated (run: aws configure)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ AWS CLI not installed${NC}"
fi

echo

# Google Cloud Storage
if command -v gcloud &> /dev/null; then
    echo -e "${YELLOW}Setting up Google Cloud Storage...${NC}"

    if gcloud auth application-default print-access-token &> /dev/null 2>&1; then
        PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

        if [ -z "$PROJECT_ID" ]; then
            echo -e "${YELLOW}⚠ No GCP project set. Set one with:${NC}"
            echo -e "${YELLOW}  gcloud config set project YOUR_PROJECT_ID${NC}"
        else
            # Check if bucket exists
            if gcloud storage ls "gs://$BUCKET_NAME" 2>/dev/null; then
                echo -e "${GREEN}✓ GCS bucket already exists: $BUCKET_NAME${NC}"
            else
                # Try to create bucket
                if gcloud storage buckets create "gs://$BUCKET_NAME" --location=US 2>/dev/null; then
                    echo -e "${GREEN}✓ Created GCS bucket: $BUCKET_NAME${NC}"
                else
                    echo -e "${RED}✗ Failed to create GCS bucket${NC}"
                    echo -e "${YELLOW}  Please create bucket manually or ask your admin:${NC}"
                    echo -e "${YELLOW}  gcloud storage buckets create gs://$BUCKET_NAME --location=US${NC}"
                fi
            fi
        fi
    else
        echo -e "${YELLOW}⚠ GCP CLI not authenticated (run: gcloud auth application-default login)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ GCP CLI not installed${NC}"
fi

echo

# Azure Blob Storage
if command -v az &> /dev/null; then
    echo -e "${YELLOW}Setting up Azure Blob Storage...${NC}"

    if az account show &> /dev/null; then
        # Check for storage account
        if [ -z "$AZURE_STORAGE_ACCOUNT" ]; then
            echo -e "${YELLOW}⚠ AZURE_STORAGE_ACCOUNT environment variable not set${NC}"
            echo -e "${YELLOW}  Please set it to your storage account name:${NC}"
            echo -e "${YELLOW}  export AZURE_STORAGE_ACCOUNT=your-account-name${NC}"
        else
            # Get account key if not set
            if [ -z "$AZURE_STORAGE_KEY" ]; then
                echo "  Getting storage account key..."
                AZURE_STORAGE_KEY=$(az storage account keys list \
                    --account-name "$AZURE_STORAGE_ACCOUNT" \
                    --query '[0].value' -o tsv 2>/dev/null)

                if [ -z "$AZURE_STORAGE_KEY" ]; then
                    echo -e "${RED}✗ Failed to get storage account key${NC}"
                    echo -e "${YELLOW}  Set AZURE_STORAGE_KEY manually or check permissions${NC}"
                fi
            fi

            if [ -n "$AZURE_STORAGE_KEY" ]; then
                # Check if container exists
                if az storage container exists \
                    --name "$CONTAINER_NAME" \
                    --account-name "$AZURE_STORAGE_ACCOUNT" \
                    --account-key "$AZURE_STORAGE_KEY" \
                    --query exists -o tsv 2>/dev/null | grep -q true; then
                    echo -e "${GREEN}✓ Azure container already exists: $CONTAINER_NAME${NC}"
                else
                    # Try to create container
                    if az storage container create \
                        --name "$CONTAINER_NAME" \
                        --account-name "$AZURE_STORAGE_ACCOUNT" \
                        --account-key "$AZURE_STORAGE_KEY" \
                        --output none 2>/dev/null; then
                        echo -e "${GREEN}✓ Created Azure container: $CONTAINER_NAME${NC}"
                    else
                        echo -e "${RED}✗ Failed to create Azure container${NC}"
                        echo -e "${YELLOW}  Please create container manually:${NC}"
                        echo -e "${YELLOW}  az storage container create --name $CONTAINER_NAME${NC}"
                    fi
                fi
            fi
        fi
    else
        echo -e "${YELLOW}⚠ Azure CLI not authenticated (run: az login)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Azure CLI not installed${NC}"
fi

echo
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Setup Complete${NC}"
echo -e "${BLUE}============================================${NC}"
echo
echo "You can now run cloud integration tests with:"
echo "  make test-cloud"
echo
echo "Or individual backends:"
echo "  make test-cloud-s3"
echo "  make test-cloud-gcs"
echo "  make test-cloud-azure"
