.PHONY: help install build test clean coverage lint format integration-test docker-test docker-build docker-test-unit docker-test-integration generate-proto

# Default target
help:
	@echo "Go-ObjStore Python SDK - Available targets:"
	@echo "  install               - Install dependencies (local)"
	@echo "  build                 - Build the package"
	@echo "  generate-proto        - Generate gRPC code from proto files"
	@echo "  test                  - Run unit tests (local)"
	@echo "  integration-test      - Run integration tests (local)"
	@echo "  coverage              - Generate coverage report (local)"
	@echo "  lint                  - Run linters (local)"
	@echo "  format                - Format code (local)"
	@echo "  docker-build          - Build Docker test image"
	@echo "  docker-test-unit      - Run unit tests in Docker"
	@echo "  docker-test-integration - Run integration tests in Docker"
	@echo "  docker-test           - Run both unit and integration tests in Docker"
	@echo "  clean                 - Remove build artifacts"

# Install dependencies
install:
	pip install -e ".[dev]"

# Build the package
build: clean
	python -m build

# Generate gRPC code from proto files
generate-proto:
	bash scripts/generate_grpc.sh

# Run unit tests in Docker
test:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit python-unit-tests
	docker compose down -v

# Run integration tests in Docker
integration-test:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server python-integration-tests
	docker compose down -v

# Build Docker test image
docker-build:
	@echo "Building Docker test image..."
	docker compose build python-unit-tests

# Run unit tests in Docker
docker-test-unit:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit python-unit-tests
	docker compose down -v

# Run integration tests in Docker
docker-test-integration:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server python-integration-tests
	docker compose down -v

# Run all tests in Docker (unit + integration)
docker-test: docker-test-unit docker-test-integration
	@echo "All Docker tests completed!"

# Generate coverage report
coverage:
	pytest tests/ -v --cov=objstore --cov-report=html --cov-report=xml --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/index.html"

# Run linters
lint:
	flake8 objstore/ tests/
	mypy objstore/
	black --check objstore/ tests/
	isort --check-only objstore/ tests/

# Format code
format:
	black objstore/ tests/
	isort objstore/ tests/

# Clean build artifacts and Docker resources
clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf coverage.xml
	rm -rf .mypy_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true
