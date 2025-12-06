#!/bin/bash

# Test runner for Ruby SDK
# This script can be used to run tests in a Docker container if Ruby is not installed locally

set -e

RUBY_VERSION="3.2"
SDK_DIR="/home/jhahn/sources/go-objstore/api/sdks/ruby"

echo "Ruby SDK Test Runner"
echo "===================="
echo ""

# Check if we should use Docker
USE_DOCKER=false
if ! command -v ruby &> /dev/null; then
    echo "Ruby not found locally, will use Docker for testing"
    USE_DOCKER=true
elif ! command -v bundle &> /dev/null; then
    echo "Bundler not found locally, will use Docker for testing"
    USE_DOCKER=true
else
    echo "Using local Ruby installation"
    ruby --version
    bundle --version
fi

if [ "$USE_DOCKER" = true ]; then
    echo ""
    echo "Running tests in Docker container..."

    docker run --rm \
        -v "$SDK_DIR:/app" \
        -w /app \
        ruby:$RUBY_VERSION \
        bash -c "
            gem install bundler && \
            bundle install && \
            echo '' && \
            echo 'Running unit tests...' && \
            bundle exec rspec spec/unit --format documentation && \
            echo '' && \
            echo 'Generating coverage report...' && \
            COVERAGE=true bundle exec rspec spec/unit && \
            echo '' && \
            echo 'Coverage Summary:' && \
            if [ -f coverage/.last_run.json ]; then
                cat coverage/.last_run.json
            fi
        "
else
    echo ""
    echo "Installing dependencies..."
    cd "$SDK_DIR"
    bundle install

    echo ""
    echo "Running unit tests..."
    bundle exec rspec spec/unit --format documentation

    echo ""
    echo "Generating coverage report..."
    COVERAGE=true bundle exec rspec spec/unit

    echo ""
    echo "Coverage Summary:"
    if [ -f coverage/.last_run.json ]; then
        cat coverage/.last_run.json
    fi
fi

echo ""
echo "Tests completed!"
