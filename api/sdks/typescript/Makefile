.PHONY: help install build clean test test-unit test-integration coverage lint format proto docs all docker-build docker-test-unit docker-test-integration docker-test

# Default target
help:
	@echo "TypeScript SDK for go-objstore"
	@echo ""
	@echo "Available targets:"
	@echo "  install            - Install dependencies"
	@echo "  build              - Compile TypeScript to JavaScript"
	@echo "  clean              - Remove build artifacts and dependencies"
	@echo "  test               - Run all tests with coverage"
	@echo "  test-unit          - Run unit tests only"
	@echo "  test-integration   - Run integration tests with Docker"
	@echo "  coverage           - Generate and display coverage report"
	@echo "  lint               - Run ESLint"
	@echo "  format             - Format code with Prettier"
	@echo "  proto              - Generate TypeScript code from protobuf"
	@echo "  docs               - Generate documentation"
	@echo "  all                - Install, build, and test"

# Install dependencies
install:
	npm install

# Build the project
build: install
	npm run build

# Clean build artifacts, dependencies, and Docker resources
clean:
	@echo "Cleaning build artifacts..."
	npm run clean
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true

# Run unit tests in Docker
test:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit typescript-unit-tests
	docker compose down -v

# Run unit tests with coverage
test-unit:
	npm run test:unit

# Run integration tests in Docker
integration-test:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server typescript-integration-tests
	docker compose down -v

# Generate coverage report
coverage:
	npm run test -- --coverage --coverageReporters=text --coverageReporters=html
	@echo ""
	@echo "Coverage report generated in coverage/index.html"

# Lint the code
lint:
	npm run lint

# Format the code
format:
	npm run format

# Generate TypeScript code from protobuf
proto:
	npm run proto:generate

# Generate documentation
docs:
	@echo "Generating TypeScript documentation..."
	@npx typedoc --out docs src/index.ts

# Build and test everything
all: clean install build test

# Development commands
.PHONY: dev watch

# Watch mode for development
watch:
	npm run build -- --watch

# Development mode
dev: install
	npm run build -- --watch

docker-build: ## Build Docker test image
	@echo "Building Docker test image..."
	docker compose build typescript-unit-tests

docker-test-unit: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit typescript-unit-tests
	docker compose down -v

docker-test-integration: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server typescript-integration-tests
	docker compose down -v

docker-test: docker-test-unit docker-test-integration ## Run all tests in Docker
