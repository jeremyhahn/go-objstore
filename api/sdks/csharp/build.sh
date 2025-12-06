#!/bin/bash
set -e

# Build and test script for C# SDK
# This script uses Docker to build and test the SDK without requiring .NET SDK installation

echo "======================================"
echo "Go-ObjStore C# SDK - Build & Test"
echo "======================================"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH"
    exit 1
fi

# Parse command line arguments
TARGET=${1:-test}

case $TARGET in
    build)
        echo "Building C# SDK..."
        docker build --target build -t go-objstore-csharp-sdk:build -f Dockerfile.build .
        ;;

    test)
        echo "Running unit tests..."
        docker build --target test -t go-objstore-csharp-sdk:test -f Dockerfile.build .

        # Extract test results
        docker create --name temp-container go-objstore-csharp-sdk:test
        docker cp temp-container:/src/coverage ./coverage || true
        docker rm temp-container

        echo ""
        echo "Test results saved to ./coverage/"
        ;;

    integration-test)
        echo "Running integration tests..."
        echo "Starting go-objstore server and running tests..."
        docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit
        docker-compose -f docker-compose.test.yml down -v
        ;;

    package)
        echo "Creating NuGet package..."
        docker build --target package -t go-objstore-csharp-sdk:package -f Dockerfile.build .

        # Extract package
        docker create --name temp-container go-objstore-csharp-sdk:package
        mkdir -p dist
        docker cp temp-container:/dist/. ./dist/
        docker rm temp-container

        echo ""
        echo "NuGet package created in ./dist/"
        ls -lh ./dist/
        ;;

    clean)
        echo "Cleaning build artifacts..."
        rm -rf bin obj Tests/bin Tests/obj coverage TestResults dist
        docker rmi go-objstore-csharp-sdk:build go-objstore-csharp-sdk:test go-objstore-csharp-sdk:package 2>/dev/null || true
        ;;

    *)
        echo "Usage: $0 {build|test|integration-test|package|clean}"
        echo ""
        echo "Targets:"
        echo "  build              - Build the SDK"
        echo "  test               - Run unit tests"
        echo "  integration-test   - Run integration tests (requires go-objstore Docker image)"
        echo "  package            - Create NuGet package"
        echo "  clean              - Clean build artifacts"
        exit 1
        ;;
esac

echo ""
echo "Done!"
