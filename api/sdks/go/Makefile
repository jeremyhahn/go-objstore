.PHONY: all build test clean coverage integration-test lint fmt docker-build docker-test-unit docker-test-integration docker-test help

# Variables
BINARY_NAME=objstore-sdk
GO=go
GOTEST=$(GO) test
GOVET=$(GO) vet
GOFMT=gofmt
GOLINT=golangci-lint
COVERAGE_FILE=coverage.txt
COVERAGE_HTML=coverage.html

# Default target
all: build test

# Build the SDK (just verify compilation)
build:
	@echo "Building SDK..."
	$(GO) build -v ./...

# Run unit tests in Docker
test:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit go-unit-tests
	docker compose down -v

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -timeout=30s -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@echo "Generating HTML coverage report..."
	$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"
	@echo "Overall coverage:"
	@$(GO) tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print "Total: " $$3}'

# Run integration tests in Docker
integration-test:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server go-integration-tests
	docker compose down -v

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Lint code
lint:
	@echo "Linting code..."
	@if command -v $(GOLINT) >/dev/null 2>&1; then \
		$(GOLINT) run ./...; \
	else \
		echo "golangci-lint not installed, skipping..."; \
		$(GOVET) ./...; \
	fi

# Clean build artifacts and Docker resources
clean:
	@echo "Cleaning build artifacts..."
	$(GO) clean
	rm -f $(COVERAGE_FILE) $(COVERAGE_HTML)
	rm -rf integration/data
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod tidy

# Run all checks (format, lint, test)
check: fmt lint test

# Install development tools
install-tools:
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Build Docker test image
docker-build:
	@echo "Building Docker test image..."
	docker compose build go-unit-tests

# Run unit tests in Docker
docker-test-unit:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit go-unit-tests
	docker compose down -v

# Run integration tests in Docker
docker-test-integration:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server go-integration-tests
	docker compose down -v

# Run all tests in Docker
docker-test: docker-test-unit docker-test-integration

# Help target
help:
	@echo "Available targets:"
	@echo "  all                   - Build and test (default)"
	@echo "  build                 - Build the SDK"
	@echo "  test                  - Run unit tests (local)"
	@echo "  coverage              - Run tests with coverage report"
	@echo "  integration-test      - Run integration tests with Docker (local)"
	@echo "  docker-build          - Build Docker test image"
	@echo "  docker-test-unit      - Run unit tests in Docker"
	@echo "  docker-test-integration - Run integration tests in Docker"
	@echo "  docker-test           - Run all tests in Docker"
	@echo "  fmt                   - Format code"
	@echo "  lint                  - Lint code"
	@echo "  clean                 - Clean build artifacts"
	@echo "  deps                  - Download and tidy dependencies"
	@echo "  check                 - Run format, lint, and test"
	@echo "  install-tools         - Install development tools"
	@echo "  help                  - Show this help message"
