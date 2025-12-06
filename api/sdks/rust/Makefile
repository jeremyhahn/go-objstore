.PHONY: build test clean coverage docker-coverage integration-test docker-test docker-build docker-test-unit docker-test-integration fmt clippy check all

# Default target
all: fmt clippy build test

# Build the project
build:
	@echo "Building go-objstore Rust SDK..."
	cargo build --release

# Run unit tests in Docker
test:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit rust-unit-tests
	docker compose down -v

# Run integration tests in Docker
integration-test:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server rust-integration-tests
	docker compose down -v

# Build Docker test image
docker-build:
	@echo "Building Docker test image..."
	docker compose build rust-unit-tests

# Run unit tests in Docker
docker-test-unit:
	@echo "Running unit tests in Docker..."
	docker compose up --build --abort-on-container-exit rust-unit-tests
	docker compose down -v

# Run integration tests in Docker
docker-test-integration:
	@echo "Running integration tests in Docker..."
	docker compose up --build --abort-on-container-exit objstore-server rust-integration-tests
	docker compose down -v

# Run all tests in Docker
docker-test: docker-test-unit docker-test-integration

# Generate code coverage
coverage:
	@echo "Generating code coverage report..."
	@command -v cargo-llvm-cov >/dev/null 2>&1 || { \
		echo "Installing cargo-llvm-cov..."; \
		cargo install cargo-llvm-cov; \
	}
	cargo llvm-cov --lib --bins --html --open

# Run coverage in Docker (recommended)
docker-coverage:
	@echo "Running coverage tests in Docker..."
	docker compose up --build --abort-on-container-exit rust-coverage
	docker compose down -v

# Alternative coverage with tarpaulin (local)
coverage-tarpaulin:
	@echo "Generating code coverage with tarpaulin..."
	@command -v cargo-tarpaulin >/dev/null 2>&1 || { \
		echo "Installing cargo-tarpaulin..."; \
		cargo install cargo-tarpaulin; \
	}
	cargo tarpaulin --out Html --output-dir coverage

# Clean build artifacts and Docker resources
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf coverage/
	rm -rf target/
	@echo "Cleaning Docker resources..."
	docker compose down -v 2>/dev/null || true

# Format code
fmt:
	@echo "Formatting code..."
	cargo fmt

# Check formatting
fmt-check:
	@echo "Checking code formatting..."
	cargo fmt -- --check

# Run clippy linter
clippy:
	@echo "Running clippy..."
	cargo clippy -- -D warnings

# Check without building
check:
	@echo "Checking code..."
	cargo check

# Build documentation
docs:
	@echo "Building documentation..."
	cargo doc --no-deps --open

# Run all checks (format, clippy, test)
ci: fmt-check clippy test
	@echo "CI checks passed!"

# Install development dependencies
install-dev:
	@echo "Installing development dependencies..."
	cargo install cargo-llvm-cov
	cargo install cargo-tarpaulin
	rustup component add rustfmt clippy

# Run examples
example-rest:
	cargo run --example rest_client

example-grpc:
	cargo run --example grpc_client

example-quic:
	cargo run --example quic_client

example-unified:
	cargo run --example unified_client

# Help target
help:
	@echo "Available targets:"
	@echo "  all              - Run fmt, clippy, build, and test"
	@echo "  build            - Build the project in release mode"
	@echo "  test             - Run unit tests in Docker"
	@echo "  integration-test - Run integration tests in Docker"
	@echo "  docker-test      - Run both unit and integration tests in Docker"
	@echo "  docker-coverage  - Generate code coverage report in Docker (recommended)"
	@echo "  coverage         - Generate code coverage report locally with llvm-cov"
	@echo "  clean            - Remove build artifacts"
	@echo "  fmt              - Format code"
	@echo "  fmt-check        - Check code formatting"
	@echo "  clippy           - Run clippy linter"
	@echo "  check            - Check code without building"
	@echo "  docs             - Build and open documentation"
	@echo "  ci               - Run CI checks (format, clippy, test)"
	@echo "  install-dev      - Install development dependencies"
	@echo "  example-*        - Run specific example"
