.PHONY: build test clean coverage integration-test install lint format docker-build docker-test-unit docker-test-integration docker-test help

# Variables
RUBY := ruby
BUNDLE := bundle
RSPEC := $(BUNDLE) exec rspec
RUBOCOP := $(BUNDLE) exec rubocop
DOCKER := docker
DOCKER_COMPOSE := docker compose

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'

install: ## Install dependencies
	$(BUNDLE) install

build: install ## Build the gem
	gem build objstore.gemspec

test: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit ruby-unit-tests
	docker compose down -v

coverage: install ## Run tests with coverage report
	COVERAGE=true $(RSPEC) spec/unit --format documentation
	@echo ""
	@echo "Coverage report generated in coverage/"
	@if command -v open >/dev/null 2>&1; then \
		open coverage/index.html; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open coverage/index.html; \
	fi

integration-test: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server ruby-integration-tests
	docker compose down -v

docker-up: ## Start Docker containers for integration tests
	@echo "Starting go-objstore server..."
	@$(DOCKER_COMPOSE) -f docker-compose.test.yml up -d
	@echo "Waiting for services to start..."
	@sleep 3

docker-down: ## Stop Docker containers
	@echo "Stopping Docker containers..."
	@$(DOCKER_COMPOSE) -f docker-compose.test.yml down -v

docker-logs: ## Show Docker container logs
	@$(DOCKER_COMPOSE) -f docker-compose.test.yml logs

lint: install ## Run RuboCop linter
	$(RUBOCOP) lib spec

format: install ## Auto-fix RuboCop issues
	$(RUBOCOP) -a lib spec

clean: ## Clean up generated files and Docker resources
	@echo "Cleaning build artifacts..."
	rm -rf coverage/
	rm -rf .bundle/
	rm -rf vendor/bundle/
	rm -f *.gem
	rm -f spec/examples.txt
	rm -rf tmp/
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true

clean-all: clean docker-down ## Clean everything including Docker containers

console: install ## Start interactive console
	$(BUNDLE) exec irb -r ./lib/objstore

docs: install ## Generate documentation
	$(BUNDLE) exec yard doc

test-all: test integration-test ## Run all tests

ci: install lint test coverage ## Run all CI checks

docker-build: ## Build Docker test image
	@echo "Building Docker test image..."
	docker compose build ruby-unit-tests

docker-test-unit: ## Run unit tests in Docker
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit ruby-unit-tests
	docker compose down -v

docker-test-integration: ## Run integration tests in Docker
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server ruby-integration-tests
	docker compose down -v

docker-test: docker-test-unit docker-test-integration ## Run all tests in Docker
