.PHONY: help install build test test-unit test-integration clean coverage lint format proto docker-build docker-test-unit docker-test-integration docker-test

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies
	@echo "Installing dependencies..."
	npm install

proto: ## Generate protobuf code
	@echo "Generating protobuf code..."
	npm run build:proto

build: clean proto ## Build the SDK
	@echo "Building SDK..."
	npm run build:src
	@echo "Build complete!"

test: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit javascript-unit-tests
	docker compose down -v

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	npm run test:unit

integration-test: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server javascript-integration-tests
	docker compose down -v

clean: ## Clean build artifacts and Docker resources
	@echo "Cleaning build artifacts..."
	npm run clean
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true
	@echo "Clean complete!"

coverage: ## Generate coverage report
	@echo "Generating coverage report..."
	npm run coverage

lint: ## Lint code
	@echo "Linting code..."
	npm run lint

lint-fix: ## Lint and fix code
	@echo "Linting and fixing code..."
	npm run lint:fix

format: ## Format code
	@echo "Formatting code..."
	npm run format

watch: ## Run tests in watch mode
	@echo "Running tests in watch mode..."
	npm run test:watch

publish-dry-run: build test ## Dry run of npm publish
	@echo "Running publish dry-run..."
	npm publish --dry-run

publish: build test ## Publish to npm (requires authentication)
	@echo "Publishing to npm..."
	npm publish

verify: lint test ## Verify code quality and tests
	@echo "All verifications passed!"

ci: install verify coverage ## CI pipeline
	@echo "CI pipeline complete!"

docker-up: ## Start Docker services for integration tests
	@echo "Starting Docker services..."
	docker-compose -f tests/integration/docker-compose.test.yml up -d

docker-down: ## Stop Docker services
	@echo "Stopping Docker services..."
	docker-compose -f tests/integration/docker-compose.test.yml down -v

docker-logs: ## Show Docker logs
	docker-compose -f tests/integration/docker-compose.test.yml logs -f

docker-build: ## Build Docker test image
	@echo "Building Docker test image..."
	docker compose build javascript-unit-tests

docker-test-unit: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit javascript-unit-tests
	docker compose down -v

docker-test-integration: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server javascript-integration-tests
	docker compose down -v

docker-test: docker-test-unit docker-test-integration ## Run all tests in Docker
