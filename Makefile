# Makefile for go-objstore
# Object store abstraction library with cloud and local storage support

# ==============================================================================
# Configuration
# ==============================================================================

# Optional storage backend features (set to 1 to enable)
# Default: All backends enabled for production builds and releases
WITH_LOCAL ?= 1
WITH_AWS_S3 ?= 1
WITH_MINIO ?= 1
WITH_GCP_STORAGE ?= 1
WITH_AZURE_BLOB ?= 1
WITH_GLACIER ?= 1
WITH_AZURE_ARCHIVE ?= 1

# Group variables (convenience flags to enable all backends for a provider)
# Setting these will override individual backend flags
WITH_AWS ?= 0
WITH_GCP ?= 0
WITH_AZURE ?= 0

# Apply group flags
ifeq ($(WITH_AWS),1)
	WITH_AWS_S3 := 1
	WITH_GLACIER := 1
endif
ifeq ($(WITH_GCP),1)
	WITH_GCP_STORAGE := 1
endif
ifeq ($(WITH_AZURE),1)
	WITH_AZURE_BLOB := 1
	WITH_AZURE_ARCHIVE := 1
endif

# Build tags based on backend flags
BUILD_TAGS :=
ifeq ($(WITH_LOCAL),1)
	BUILD_TAGS += local
endif
ifeq ($(WITH_AWS_S3),1)
	BUILD_TAGS += awss3
endif
ifeq ($(WITH_MINIO),1)
	BUILD_TAGS += minio
endif
ifeq ($(WITH_GCP_STORAGE),1)
	BUILD_TAGS += gcpstorage
endif
ifeq ($(WITH_AZURE_BLOB),1)
	BUILD_TAGS += azureblob
endif
ifeq ($(WITH_GLACIER),1)
	BUILD_TAGS += glacier
endif
ifeq ($(WITH_AZURE_ARCHIVE),1)
	BUILD_TAGS += azurearchive
endif

# Build tag flags for go commands
ifneq ($(BUILD_TAGS),)
	TAG_FLAGS := -tags "$(BUILD_TAGS)"
else
	TAG_FLAGS :=
endif

# Go parameters
GO := go
GOBUILD := $(GO) build $(TAG_FLAGS)
GOTEST := $(GO) test $(TAG_FLAGS)
GOMOD := $(GO) mod
GOCLEAN := $(GO) clean

# Docker Compose (use v2 syntax for GitHub Actions compatibility)
DOCKER_COMPOSE := docker compose

# Project structure
PROJECT_NAME := go-objstore
MODULE := go-objstore

# Build directories
BIN_DIR := bin
COVERAGE_DIR := coverage

# Test coverage packages
PKG_COVER := github.com/jeremyhahn/go-objstore/pkg/adapters,github.com/jeremyhahn/go-objstore/pkg/azure,github.com/jeremyhahn/go-objstore/pkg/azurearchive,github.com/jeremyhahn/go-objstore/pkg/local,github.com/jeremyhahn/go-objstore/pkg/s3,github.com/jeremyhahn/go-objstore/pkg/minio,github.com/jeremyhahn/go-objstore/pkg/factory,github.com/jeremyhahn/go-objstore/pkg/glacier,github.com/jeremyhahn/go-objstore/pkg/gcs,github.com/jeremyhahn/go-objstore/pkg/storagefs,github.com/jeremyhahn/go-objstore/pkg/cli,github.com/jeremyhahn/go-objstore/pkg/server/grpc,github.com/jeremyhahn/go-objstore/pkg/server/rest,github.com/jeremyhahn/go-objstore/pkg/server/quic,github.com/jeremyhahn/go-objstore/pkg/server/mcp

# Color output (ANSI escape codes)
RESET := \033[0m
BOLD := \033[1m
RED := \033[31m
GREEN := \033[32m
YELLOW := \033[33m
BLUE := \033[34m
CYAN := \033[36m

# ==============================================================================
# Default Target
# ==============================================================================

.DEFAULT_GOAL := build

# ==============================================================================
# Primary Targets
# ==============================================================================

.PHONY: deps
## deps: Install dependencies for tests
deps:
	@echo "$(CYAN)$(BOLD)→ Installing dependencies...$(RESET)"
	@$(GOMOD) tidy
	@$(GO) get -t ./...
	@echo "$(GREEN)✓ Dependencies installed$(RESET)"

.PHONY: build
## build: Build CLI, server, and shared library
build:
	@echo "$(CYAN)$(BOLD)→ Building the library packages...$(RESET)"
	@echo "$(CYAN)  Build tags: $(BUILD_TAGS)$(RESET)"
	@$(GOBUILD) ./...
	@echo "$(GREEN)✓ Library packages compiled$(RESET)"
	@$(MAKE) --no-print-directory build-cli
	@$(MAKE) --no-print-directory build-server
	@$(MAKE) --no-print-directory lib
	@echo "$(GREEN)$(BOLD)✓ All builds complete!$(RESET)"

.PHONY: build-cli
## build-cli: Build the CLI tool
build-cli:
	@echo "$(CYAN)$(BOLD)→ Building CLI tool...$(RESET)"
	@echo "$(CYAN)  Build tags: $(BUILD_TAGS)$(RESET)"
	@mkdir -p $(BIN_DIR)
	@$(GOBUILD) -o $(BIN_DIR)/objstore ./cmd/objstore
	@echo "$(GREEN)✓ CLI built: $(BIN_DIR)/objstore$(RESET)"

.PHONY: build-server
## build-server: Build all server binaries (all-in-one and individual)
build-server:
	@echo "$(CYAN)$(BOLD)→ Building server binaries...$(RESET)"
	@echo "$(CYAN)  Build tags: $(BUILD_TAGS)$(RESET)"
	@mkdir -p $(BIN_DIR)
	@$(GOBUILD) -o $(BIN_DIR)/objstore-server ./cmd/objstore-server
	@$(GOBUILD) -o $(BIN_DIR)/objstore-grpc-server ./cmd/objstore-grpc-server
	@$(GOBUILD) -o $(BIN_DIR)/objstore-rest-server ./cmd/objstore-rest-server
	@$(GOBUILD) -o $(BIN_DIR)/objstore-quic-server ./cmd/objstore-quic-server
	@$(GOBUILD) -o $(BIN_DIR)/objstore-mcp-server ./cmd/objstore-mcp-server
	@echo "$(GREEN)✓ All-in-one server built: $(BIN_DIR)/objstore-server$(RESET)"
	@echo "$(GREEN)✓ gRPC server built: $(BIN_DIR)/objstore-grpc-server$(RESET)"
	@echo "$(GREEN)✓ REST server built: $(BIN_DIR)/objstore-rest-server$(RESET)"
	@echo "$(GREEN)✓ QUIC server built: $(BIN_DIR)/objstore-quic-server$(RESET)"
	@echo "$(GREEN)✓ MCP server built: $(BIN_DIR)/objstore-mcp-server$(RESET)"

.PHONY: build-all
## build-all: Alias for build (builds library, CLI, and server)
build-all: build

.PHONY: generate-proto
## generate-proto: Generate protobuf code for gRPC
generate-proto:
	@echo "$(CYAN)$(BOLD)→ Generating protobuf code...$(RESET)"
	@bash ./scripts/generate-proto.sh
	@echo "$(GREEN)✓ Protobuf code generated$(RESET)"

.PHONY: test
## test: Run unit tests (fast, in-memory, no system modifications)
test:
	@echo "$(CYAN)$(BOLD)→ Running unit tests with coverage...$(RESET)"
	@mkdir -p $(COVERAGE_DIR)
	$(GO) test -tags="local awss3 minio gcpstorage azureblob glacier azurearchive" -coverprofile=$(COVERAGE_DIR)/unit.out -covermode=atomic ./pkg/...
	@echo ""
	@echo "$(CYAN)$(BOLD)→ Coverage Summary:$(RESET)"
	@$(GO) tool cover -func=$(COVERAGE_DIR)/unit.out | tail -1 | awk '{print "  $(GREEN)Total Coverage: " $$NF "$(RESET)"}' || true
	@echo "$(GREEN)✓ Unit tests complete$(RESET)"

.PHONY: integration-test
## integration-test: Run all integration tests (all backends)
integration-test: integration-test-local integration-test-s3 integration-test-minio integration-test-azure integration-test-gcs integration-test-factory
	@echo "$(GREEN)$(BOLD)✓ All integration tests complete!$(RESET)"

.PHONY: integration-test-local
## integration-test-local: Run local storage integration tests
integration-test-local:
	@echo "$(CYAN)$(BOLD)→ Running local storage integration tests...$(RESET)"
	@cd test/integration/local && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/local && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/local && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ Local integration tests complete$(RESET)"

.PHONY: integration-test-s3
## integration-test-s3: Run S3/MinIO integration tests
integration-test-s3:
	@echo "$(CYAN)$(BOLD)→ Running S3/MinIO integration tests...$(RESET)"
	@cd test/integration/s3 && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/s3 && $(DOCKER_COMPOSE) up -d minio
	@cd test/integration/s3 && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/s3 && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ S3 integration tests complete$(RESET)"

.PHONY: integration-test-minio
## integration-test-minio: Run MinIO integration tests
integration-test-minio:
	@echo "$(CYAN)$(BOLD)→ Running MinIO integration tests...$(RESET)"
	@cd test/integration/minio && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/minio && $(DOCKER_COMPOSE) up -d minio
	@cd test/integration/minio && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/minio && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ MinIO integration tests complete$(RESET)"

.PHONY: integration-test-azure
## integration-test-azure: Run Azure/Azurite integration tests
integration-test-azure:
	@echo "$(CYAN)$(BOLD)→ Running Azure/Azurite integration tests...$(RESET)"
	@cd test/integration/azure && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/azure && $(DOCKER_COMPOSE) up -d azurite
	@cd test/integration/azure && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/azure && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ Azure integration tests complete$(RESET)"

.PHONY: integration-test-gcs
## integration-test-gcs: Run GCS emulator integration tests
integration-test-gcs:
	@echo "$(CYAN)$(BOLD)→ Running GCS emulator integration tests...$(RESET)"
	@cd test/integration/gcs && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/gcs && $(DOCKER_COMPOSE) up -d fake-gcs
	@cd test/integration/gcs && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/gcs && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ GCS integration tests complete$(RESET)"

.PHONY: integration-test-factory
## integration-test-factory: Run factory/common integration tests
integration-test-factory:
	@echo "$(CYAN)$(BOLD)→ Running factory integration tests...$(RESET)"
	@cd test/integration/factory && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/factory && $(DOCKER_COMPOSE) run --rm test
	@cd test/integration/factory && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ Factory integration tests complete$(RESET)"

.PHONY: integration-test-encryption
## integration-test-encryption: Run encryption integration tests
integration-test-encryption:
	@echo "$(CYAN)$(BOLD)→ Running encryption integration tests...$(RESET)"
	@$(GO) test -tags=integration -v ./pkg/encryption
	@echo "$(GREEN)✓ Encryption integration tests complete$(RESET)"

.PHONY: test-cli
## test-cli: Run CLI integration tests in Docker
test-cli:
	@echo "$(CYAN)$(BOLD)→ Running CLI integration tests...$(RESET)"
	@cd test/integration/cli && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/cli && $(DOCKER_COMPOSE) up --abort-on-container-exit --exit-code-from test
	@cd test/integration/cli && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ CLI integration tests complete$(RESET)"

.PHONY: test-servers
## test-servers: Run server integration tests (gRPC, REST, QUIC, MCP) in Docker
test-servers:
	@echo "$(CYAN)$(BOLD)→ Running server integration tests...$(RESET)"
	@echo "$(CYAN)  Generating test certificates...$(RESET)"
	@cd test/integration/server && bash generate-certs.sh
	@echo "$(CYAN)  Starting server test environment...$(RESET)"
	@cd test/integration/server && $(DOCKER_COMPOSE) down -v >/dev/null 2>&1 || true
	@cd test/integration/server && $(DOCKER_COMPOSE) up --abort-on-container-exit --exit-code-from test
	@cd test/integration/server && $(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✓ Server integration tests complete$(RESET)"

.PHONY: integration-test-all
## integration-test-all: Run all integration tests including CLI and servers
integration-test-all: integration-test test-cli test-servers
	@echo "$(GREEN)$(BOLD)✓ All integration tests complete!$(RESET)"

# ==============================================================================
# Cloud Integration Tests (Real AWS S3, GCP, Azure)
# ==============================================================================

.PHONY: test-cloud-integration
## test-cloud-integration: Run all cloud backend tests (requires cloud credentials)
test-cloud-integration: test-cloud-s3 test-cloud-gcs test-cloud-azure
	@echo "$(GREEN)$(BOLD)✓ All cloud integration tests complete!$(RESET)"

.PHONY: test-cloud-s3
## test-cloud-s3: Run AWS S3 cloud integration tests
test-cloud-s3:
	@echo "$(CYAN)$(BOLD)→ Running AWS S3 cloud tests...$(RESET)"
	@echo "$(YELLOW)⚠  This test uses real AWS S3 and may incur costs$(RESET)"
	@OBJSTORE_TEST_REAL_S3=1 \
		$(GO) test -v -tags=cloud_integration,awss3 ./test/integration/s3 -run TestS3_RealCloud
	@echo "$(GREEN)✓ S3 cloud tests complete$(RESET)"

.PHONY: test-cloud-gcs
## test-cloud-gcs: Run Google Cloud Storage integration tests
test-cloud-gcs:
	@echo "$(CYAN)$(BOLD)→ Running GCS cloud tests...$(RESET)"
	@echo "$(YELLOW)⚠  This test uses real GCP and may incur costs$(RESET)"
	@OBJSTORE_TEST_REAL_GCS=1 \
		$(GO) test -v -tags=cloud_integration,gcpstorage ./test/integration/gcs -run TestGCS_RealCloud
	@echo "$(GREEN)✓ GCS cloud tests complete$(RESET)"

.PHONY: test-cloud-azure
## test-cloud-azure: Run Azure Blob Storage integration tests
test-cloud-azure:
	@echo "$(CYAN)$(BOLD)→ Running Azure cloud tests...$(RESET)"
	@echo "$(YELLOW)⚠  This test uses real Azure and may incur costs$(RESET)"
	@if [ -z "$$OBJSTORE_TEST_AZURE_ACCOUNT" ] || [ -z "$$OBJSTORE_TEST_AZURE_KEY" ]; then \
		echo "$(RED)✗ OBJSTORE_TEST_AZURE_ACCOUNT and OBJSTORE_TEST_AZURE_KEY must be set$(RESET)"; \
		exit 1; \
	fi
	@OBJSTORE_TEST_REAL_AZURE=1 \
		$(GO) test -v -tags=cloud_integration,azureblob ./test/integration/azure -run TestAzure_RealCloud
	@echo "$(GREEN)✓ Azure cloud tests complete$(RESET)"

# ==============================================================================
# Benchmarks
# ==============================================================================

.PHONY: bench
## bench: Run all benchmarks
bench: bench-local bench-s3 bench-minio bench-gcs bench-azure bench-factory bench-common bench-storagefs
	@echo "$(GREEN)$(BOLD)✓ All benchmarks complete!$(RESET)"

.PHONY: bench-local
## bench-local: Run local storage benchmarks
bench-local:
	@echo "$(CYAN)$(BOLD)→ Running local storage benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=local ./pkg/local
	@echo "$(GREEN)✓ Local benchmarks complete$(RESET)"

.PHONY: bench-s3
## bench-s3: Run S3 storage benchmarks
bench-s3:
	@echo "$(CYAN)$(BOLD)→ Running S3 storage benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=awss3 ./pkg/s3
	@echo "$(GREEN)✓ S3 benchmarks complete$(RESET)"

.PHONY: bench-minio
## bench-minio: Run MinIO storage benchmarks
bench-minio:
	@echo "$(CYAN)$(BOLD)→ Running MinIO storage benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=minio ./pkg/minio
	@echo "$(GREEN)✓ MinIO benchmarks complete$(RESET)"

.PHONY: bench-gcs
## bench-gcs: Run GCS storage benchmarks
bench-gcs:
	@echo "$(CYAN)$(BOLD)→ Running GCS storage benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=gcpstorage ./pkg/gcs
	@echo "$(GREEN)✓ GCS benchmarks complete$(RESET)"

.PHONY: bench-azure
## bench-azure: Run Azure storage benchmarks
bench-azure:
	@echo "$(CYAN)$(BOLD)→ Running Azure storage benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=azureblob ./pkg/azure
	@echo "$(GREEN)✓ Azure benchmarks complete$(RESET)"

.PHONY: bench-factory
## bench-factory: Run factory benchmarks
bench-factory:
	@echo "$(CYAN)$(BOLD)→ Running factory benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem -tags=local ./pkg/factory
	@echo "$(GREEN)✓ Factory benchmarks complete$(RESET)"

.PHONY: bench-common
## bench-common: Run common package benchmarks
bench-common:
	@echo "$(CYAN)$(BOLD)→ Running common package benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem ./pkg/common
	@echo "$(GREEN)✓ Common benchmarks complete$(RESET)"

.PHONY: bench-storagefs
## bench-storagefs: Run storagefs benchmarks
bench-storagefs:
	@echo "$(CYAN)$(BOLD)→ Running storagefs benchmarks...$(RESET)"
	@$(GO) test -bench=. -benchmem ./pkg/storagefs
	@echo "$(GREEN)✓ StorageFS benchmarks complete$(RESET)"


.PHONY: coverage-check
## coverage-check: Check per-package coverage and highlight packages under 90%
coverage-check:
	@echo "$(CYAN)$(BOLD)=== Package Coverage Report ===$(RESET)"
	@echo "$(CYAN)Using build tags: local,awss3,minio,gcpstorage,azureblob,glacier,azurearchive$(RESET)"
	@echo ""
	@for pkg in $$($(GO) list ./pkg/...); do \
		output=$$($(GO) test -tags="local,awss3,minio,gcpstorage,azureblob,glacier,azurearchive" -cover "$$pkg" 2>/dev/null); \
		if echo "$$output" | grep -q "no statements"; then \
			printf "%-70s %6s\n" "$$pkg" "  N/A"; \
		else \
			coverage=$$(echo "$$output" | grep -oP 'coverage: \K[0-9.]+' || echo "0.0"); \
			printf "%-70s %6s%%\n" "$$pkg" "$$coverage"; \
		fi; \
	done | sort -t% -k2 -n
	@echo ""
	@echo "$(CYAN)$(BOLD)=== Packages Under 90% ===$(RESET)"
	@UNDER_90=0; \
	for pkg in $$($(GO) list ./pkg/...); do \
		output=$$($(GO) test -tags="local,awss3,minio,gcpstorage,azureblob,glacier,azurearchive" -cover "$$pkg" 2>/dev/null); \
		if echo "$$output" | grep -q "no statements"; then \
			continue; \
		fi; \
		coverage=$$(echo "$$output" | grep -oP 'coverage: \K[0-9.]+' || echo "0.0"); \
		if [ "$$(echo "$$coverage < 90" | bc -l 2>/dev/null || echo "1")" = "1" ]; then \
			printf "$(RED)%-70s %6s%% ⚠️$(RESET)\n" "$$pkg" "$$coverage"; \
			UNDER_90=1; \
		fi; \
	done; \
	if [ "$$UNDER_90" = "0" ]; then \
		echo "$(GREEN)✓ All packages meet 90% coverage threshold!$(RESET)"; \
	else \
		echo ""; \
		echo "$(YELLOW)⚠ Some packages are below 90% coverage$(RESET)"; \
	fi

.PHONY: coverage-report
## coverage-report: Display coverage report
coverage-report: test
	@echo "$(CYAN)$(BOLD)→ Generating coverage report...$(RESET)"
	@if [ -f ./$(COVERAGE_DIR)/integration.out ]; then \
		echo "$(CYAN)→ Merging unit and integration coverage...$(RESET)"; \
		$(GO) run ./tools/mergecov ./$(COVERAGE_DIR)/unit.out ./$(COVERAGE_DIR)/integration.out > ./$(COVERAGE_DIR)/merged.out; \
		$(GO) tool cover -func=./$(COVERAGE_DIR)/merged.out | tail -n +1; \
	else \
		echo "$(YELLOW)⚠ Integration coverage not found, showing unit test coverage only$(RESET)"; \
		$(GO) tool cover -func=./$(COVERAGE_DIR)/unit.out | tail -n +1; \
	fi
	@echo "$(GREEN)✓ Coverage report complete$(RESET)"

.PHONY: docker-build
## docker-build: Build Docker container
docker-build:
	@echo "$(CYAN)$(BOLD)→ Building docker container...$(RESET)"
	@docker build -t $(PROJECT_NAME) .
	@echo "$(GREEN)✓ Docker image built: $(PROJECT_NAME)$(RESET)"

.PHONY: docker-run
## docker-run: Run Docker container interactively
docker-run:
	@echo "$(CYAN)$(BOLD)→ Running docker container...$(RESET)"
	@docker run -it $(PROJECT_NAME)

.PHONY: docker-test
## docker-test: Run tests in Docker container
docker-test: docker-build
	@echo "$(CYAN)$(BOLD)→ Running tests in docker container...$(RESET)"
	@docker run $(PROJECT_NAME) make test
	@echo "$(GREEN)$(BOLD)✓ Docker tests complete!$(RESET)"

.PHONY: lint
## lint: Run golangci-lint for code quality analysis
lint:
	@echo "$(CYAN)$(BOLD)→ Running golangci-lint...$(RESET)"
	@GOPATH=$$(go env GOPATH); \
	if [ -x "$$GOPATH/bin/golangci-lint" ]; then \
		$$GOPATH/bin/golangci-lint run --timeout=10m && \
		echo "$(GREEN)✓ Linting passed$(RESET)"; \
	elif command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run --timeout=10m && \
		echo "$(GREEN)✓ Linting passed$(RESET)"; \
	else \
		echo "$(RED)✗ golangci-lint not found$(RESET)"; \
		echo "$(YELLOW)  Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(RESET)"; \
		echo "$(YELLOW)  Or: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b \$$(go env GOPATH)/bin$(RESET)"; \
		exit 1; \
	fi

.PHONY: install-security-tools
## install-security-tools: Install security scanning tools (gosec, govulncheck)
install-security-tools:
	@echo "$(CYAN)$(BOLD)→ Installing security tools...$(RESET)"
	@echo "$(CYAN)  Installing gosec...$(RESET)"
	@$(GO) install github.com/securego/gosec/v2/cmd/gosec@latest
	@echo "$(GREEN)✓ gosec installed$(RESET)"
	@echo "$(CYAN)  Installing govulncheck...$(RESET)"
	@$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	@echo "$(GREEN)✓ govulncheck installed$(RESET)"
	@echo "$(GREEN)$(BOLD)✓ All security tools installed successfully$(RESET)"

.PHONY: security
## security: Run security checks (gosec + govulncheck)
security:
	@echo "$(CYAN)$(BOLD)→ Running security checks...$(RESET)"
	@echo "$(CYAN)  Running gosec...$(RESET)"
	@GOPATH=$$(go env GOPATH); \
	if [ -x "$$GOPATH/bin/gosec" ]; then \
		cd $(PWD) && $$GOPATH/bin/gosec -no-fail -fmt json -out gosec-results.json -exclude-dir=examples ./... && echo "$(GREEN)✓ gosec passed$(RESET)" || echo "$(YELLOW)⚠ gosec found issues (see gosec-results.json)$(RESET)"; \
	elif command -v gosec > /dev/null 2>&1; then \
		cd $(PWD) && gosec -no-fail -fmt json -out gosec-results.json -exclude-dir=examples ./... && echo "$(GREEN)✓ gosec passed$(RESET)" || echo "$(YELLOW)⚠ gosec found issues (see gosec-results.json)$(RESET)"; \
	else \
		echo "$(RED)✗ gosec not found$(RESET)"; \
		echo "$(YELLOW)  Install: make install-security-tools$(RESET)"; \
		echo "$(YELLOW)  Or manually: go install github.com/securego/gosec/v2/cmd/gosec@latest$(RESET)"; \
		echo "$(YELLOW)  Note: You may need to add \$$GOPATH/bin to your PATH$(RESET)"; \
		exit 1; \
	fi
	@echo "$(CYAN)  Running govulncheck...$(RESET)"
	@GOPATH=$$(go env GOPATH); \
	if [ -x "$$GOPATH/bin/govulncheck" ]; then \
		$$GOPATH/bin/govulncheck ./... && echo "$(GREEN)✓ govulncheck passed$(RESET)" || echo "$(YELLOW)⚠ govulncheck found vulnerabilities$(RESET)"; \
	elif command -v govulncheck > /dev/null 2>&1; then \
		govulncheck ./... && echo "$(GREEN)✓ govulncheck passed$(RESET)" || echo "$(YELLOW)⚠ govulncheck found vulnerabilities$(RESET)"; \
	else \
		echo "$(YELLOW)⚠ govulncheck not found (skipping)$(RESET)"; \
		echo "$(YELLOW)  Install: make install-security-tools$(RESET)"; \
		echo "$(YELLOW)  Or manually: go install golang.org/x/vuln/cmd/govulncheck@latest$(RESET)"; \
		echo "$(YELLOW)  Note: You may need to add \$$GOPATH/bin to your PATH$(RESET)"; \
	fi
	@echo "$(GREEN)✓ Security checks complete$(RESET)"

.PHONY: ci-local
## ci-local: Run all CI checks locally (test, lint, security, build) - use this before committing!
ci-local:
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running CI Checks Locally$(RESET)"
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo ""
	@echo "$(CYAN)$(BOLD)[1/4] Running unit tests...$(RESET)"
	@$(MAKE) test || (echo "$(RED)✗ Tests failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[2/4] Running linter...$(RESET)"
	@$(MAKE) lint || (echo "$(RED)✗ Linting failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[3/4] Running security checks...$(RESET)"
	@$(MAKE) security || (echo "$(RED)✗ Security checks failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[4/4] Building binaries...$(RESET)"
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) build-cli || (echo "$(RED)✗ CLI build failed$(RESET)" && exit 1)
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) build-server || (echo "$(RED)✗ Server build failed$(RESET)" && exit 1)
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) lib || (echo "$(RED)✗ Library build failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"
	@echo "$(GREEN)$(BOLD)  ✓ All CI Checks Passed!$(RESET)"
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"
	@echo ""
	@echo "$(YELLOW)$(BOLD)TIP:$(RESET) Run 'make ci-local-full' to also run integration tests"

.PHONY: ci-local-full
## ci-local-full: Run ALL CI checks locally including integration tests - comprehensive pre-push check
ci-local-full:
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running FULL CI Checks Locally$(RESET)"
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo ""
	@$(MAKE) ci-local || exit 1
	@echo ""
	@echo "$(CYAN)$(BOLD)[5/5] Running integration tests...$(RESET)"
	@$(MAKE) integration-test || (echo "$(RED)✗ Integration tests failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"
	@echo "$(GREEN)$(BOLD)  ✓ ALL Checks Passed (including integration tests)!$(RESET)"
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"

.PHONY: pre-commit
## pre-commit: Run all pre-commit checks (format, vet, lint, test)
pre-commit: clean
	@echo "$(CYAN)$(BOLD)→ Running pre-commit checks...$(RESET)\n"
	@bash ./scripts/pre-commit

.PHONY: clean
## clean: Clean up build artifacts and test outputs
clean:
	@echo "$(CYAN)$(BOLD)→ Cleaning up...$(RESET)"
	@$(GOCLEAN) -cache
	@$(GOCLEAN) -testcache
	@rm -rf $(BIN_DIR) $(COVERAGE_DIR)
	@rm -f coverage.html coverage-check.sh
	@rm -f examples/c_client/libobjstore.h
	@rm -f examples/c_client/simple_example examples/c_client/test_objstore
	@rm -f integration.log test.log quic*.log test*.log
	@rm -f .objstore.yaml
	@rm -rf test/integration/cli/storage test/integration/server/storage
	@rm -rf test-storage/ testdata/storage/ testdata/temp/
	@find /tmp -maxdepth 1 \( -name "cli-test" -o -name "objstore*" -o -name "*objstore*" \) -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.test" ! -path "./vendor/*" ! -path "./.git/*" -delete
	@find . -type f -name "*.out" ! -path "./vendor/*" ! -path "./.git/*" ! -path "./coverage/*" -delete
	@find . -type f -name "*.html" ! -path "./vendor/*" ! -path "./.git/*" ! -path "./docs/*" ! -path "./coverage/*" -delete
	@find /tmp -type f -name "*objstore*.log" -o -name "*test*.log" -o -name "*objstore*.log" 2>/dev/null | xargs -r rm -f
	@rm -f objstore-server objstore-quic-server objstore.pb.go objstore_grpc.pb.go
	@echo "$(GREEN)✓ Clean complete$(RESET)"

.PHONY: lib
## lib: Generate a shared object (.so) file for the library
lib:
	@echo "$(CYAN)$(BOLD)→ Building shared library...$(RESET)"
	@echo "$(CYAN)  Build tags: $(BUILD_TAGS)$(RESET)"
	@mkdir -p $(BIN_DIR)
	@$(GO) build $(TAG_FLAGS) -buildmode=c-shared -o $(BIN_DIR)/libobjstore.so ./cmd/objstorelib
	@mv $(BIN_DIR)/libobjstore.h examples/c_client/libobjstore.h
	@echo "$(GREEN)✓ Created $(BIN_DIR)/libobjstore.so and examples/c_client/libobjstore.h$(RESET)"

# ==============================================================================
# Help Target
# ==============================================================================

.PHONY: help
## help: Display this help message
help:
	@echo "$(BOLD)$(BLUE)go-objstore Makefile$(RESET)"
	@echo "$(CYAN)Object store abstraction library with cloud and local storage support$(RESET)"
	@echo ""
	@echo "$(BOLD)Usage:$(RESET)"
	@echo "  make $(YELLOW)<target>$(RESET)"
	@echo ""
	@echo "$(BOLD)Available Targets:$(RESET)"
	@echo "  $(YELLOW)deps$(RESET)                         Install dependencies for tests"
	@echo "  $(YELLOW)build$(RESET)                        Build CLI, server, and shared library"
	@echo "  $(YELLOW)build-cli$(RESET)                    Build the CLI tool"
	@echo "  $(YELLOW)build-server$(RESET)                 Build all server binaries (all-in-one and individual)"
	@echo "  $(YELLOW)lib$(RESET)                          Build shared object library (.so)"
	@echo "  $(YELLOW)build-all$(RESET)                    Alias for build (builds all)"
	@echo "  $(YELLOW)generate-proto$(RESET)               Generate protobuf code for gRPC"
	@echo "  $(YELLOW)test$(RESET)                         Run unit tests"
	@echo "  $(YELLOW)lint$(RESET)                         Run golangci-lint for code quality"
	@echo "  $(YELLOW)security$(RESET)                     Run security checks (gosec + govulncheck)"
	@echo "  $(YELLOW)pre-commit$(RESET)                   Run all pre-commit checks"
	@echo "  $(YELLOW)integration-test$(RESET)             Run storage backend integration tests"
	@echo "  $(YELLOW)integration-test-local$(RESET)       Run local storage integration tests"
	@echo "  $(YELLOW)integration-test-s3$(RESET)          Run S3 integration tests"
	@echo "  $(YELLOW)integration-test-minio$(RESET)       Run MinIO integration tests"
	@echo "  $(YELLOW)integration-test-azure$(RESET)       Run Azure integration tests"
	@echo "  $(YELLOW)integration-test-gcs$(RESET)         Run GCS integration tests"
	@echo "  $(YELLOW)test-cli$(RESET)                     Run CLI integration tests"
	@echo "  $(YELLOW)test-servers$(RESET)                 Run server integration tests (gRPC, REST, QUIC, MCP)"
	@echo "  $(YELLOW)integration-test-all$(RESET)         Run all integration tests"
	@echo "  $(YELLOW)test-cloud$(RESET)                   Run all cloud backend tests (real AWS/GCP/Azure)"
	@echo "  $(YELLOW)test-cloud-s3$(RESET)                Run AWS S3 cloud tests"
	@echo "  $(YELLOW)test-cloud-gcs$(RESET)               Run Google Cloud Storage tests"
	@echo "  $(YELLOW)test-cloud-azure$(RESET)             Run Azure Blob Storage tests"
	@echo "  $(YELLOW)coverage-check$(RESET)               Check per-package coverage (highlight <90%)"
	@echo "  $(YELLOW)coverage-report$(RESET)              Merge and display coverage reports"
	@echo "  $(YELLOW)docker-build$(RESET)                 Build docker container"
	@echo "  $(YELLOW)docker-run$(RESET)                   Run docker container interactively"
	@echo "  $(YELLOW)docker-test$(RESET)                  Start container and run tests"
	@echo "  $(YELLOW)clean$(RESET)                        Clean up all resources"
	@echo "  $(YELLOW)show-backends$(RESET)                Display enabled storage backends"
	@echo "  $(YELLOW)version$(RESET)                      Display current version"
	@echo "  $(YELLOW)version-bump-patch$(RESET)           Bump patch version (X.Y.Z -> X.Y.Z+1)"
	@echo "  $(YELLOW)version-bump-minor$(RESET)           Bump minor version (X.Y.Z -> X.Y+1.0)"
	@echo "  $(YELLOW)version-bump-major$(RESET)           Bump major version (X.Y.Z -> X+1.0.0)"
	@echo "  $(YELLOW)release$(RESET)                      Create git release tag based on VERSION file"
	@echo "  $(YELLOW)release-push$(RESET)                 Push release tag to origin"
	@echo "  $(YELLOW)help$(RESET)                         Display this help message"
	@echo ""
	@echo "$(BOLD)Storage Backend Variables:$(RESET)"
	@echo "  Control which storage backends are included in the build:"
	@echo "  $(GREEN)WITH_LOCAL=1/0$(RESET)        Local disk storage (default: $(WITH_LOCAL))"
	@echo "  $(GREEN)WITH_AWS_S3=1/0$(RESET)       Amazon S3 storage (default: $(WITH_AWS_S3))"
	@echo "  $(GREEN)WITH_MINIO=1/0$(RESET)        MinIO S3-compatible storage (default: $(WITH_MINIO))"
	@echo "  $(GREEN)WITH_GCP_STORAGE=1/0$(RESET)  Google Cloud Storage (default: $(WITH_GCP_STORAGE))"
	@echo "  $(GREEN)WITH_AZURE_BLOB=1/0$(RESET)   Azure Blob Storage (default: $(WITH_AZURE_BLOB))"
	@echo "  $(GREEN)WITH_GLACIER=1/0$(RESET)      AWS Glacier archival (default: $(WITH_GLACIER))"
	@echo "  $(GREEN)WITH_AZURE_ARCHIVE=1/0$(RESET) Azure Archive tier (default: $(WITH_AZURE_ARCHIVE))"
	@echo ""
	@echo "$(BOLD)Group Variables (enable all backends for a provider):$(RESET)"
	@echo "  $(GREEN)WITH_AWS=1/0$(RESET)          Enable all AWS backends (S3 + Glacier) (default: $(WITH_AWS))"
	@echo "  $(GREEN)WITH_GCP=1/0$(RESET)          Enable all GCP backends (Storage) (default: $(WITH_GCP))"
	@echo "  $(GREEN)WITH_AZURE=1/0$(RESET)        Enable all Azure backends (Blob) (default: $(WITH_AZURE))"
	@echo ""
	@echo "  $(CYAN)Examples:$(RESET)"
	@echo "    make build                                     # Default: local storage only"
	@echo "    make build WITH_AWS=1                          # Enable all AWS backends"
	@echo "    make build WITH_AWS_S3=1 WITH_GCP_STORAGE=1    # Enable S3 and GCS"
	@echo "    make build WITH_AZURE=1 WITH_GCP=1             # Enable Azure and GCP"
	@echo "    make test WITH_AWS=0 WITH_GCP=0 WITH_AZURE=0   # Test local storage only"
	@echo "    make lib WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1    # Build .so with all backends"
	@echo ""
	@echo "  $(YELLOW)Active build tags:$(RESET) $(BUILD_TAGS)"
	@echo ""
	@echo "$(BOLD)Common Workflows:$(RESET)"
	@echo "  $(GREEN)Development:$(RESET)        make clean && make build && make test"
	@echo "  $(GREEN)Pre-commit Checks:$(RESET)   make pre-commit"
	@echo "  $(GREEN)Code Quality:$(RESET)        make lint && make security"
	@echo "  $(GREEN)Quick Test:$(RESET)         make test"
	@echo "  $(GREEN)Full Test:$(RESET)          make integration-test"
	@echo "  $(GREEN)Coverage:$(RESET)           make coverage-report"
	@echo "  $(GREEN)Docker Dev:$(RESET)         make docker-run"
	@echo ""
	@echo "$(BOLD)Project Info:$(RESET)"
	@echo "  Module:          $(MODULE)"
	@echo "  Go Version:      $$(go version 2>/dev/null | awk '{print $$3}' || echo 'Not installed')"
	@echo "  Coverage Goal:   ≥90%"
	@echo ""

.PHONY: show-backends
## show-backends: Display enabled storage backends for current build configuration
show-backends:
	@echo "$(CYAN)$(BOLD)Current Storage Backend Configuration:$(RESET)"
	@echo ""
	@echo "  $(BOLD)Local Storage:$(RESET)"
	@if [ "$(WITH_LOCAL)" = "1" ]; then echo "    ✓ Local disk storage"; else echo "    ✗ Local disk storage"; fi
	@echo ""
	@echo "  $(BOLD)Cloud Storage Backends:$(RESET)"
	@if [ "$(WITH_AWS_S3)" = "1" ]; then echo "    ✓ AWS S3"; else echo "    ✗ AWS S3"; fi
	@if [ "$(WITH_MINIO)" = "1" ]; then echo "    ✓ MinIO"; else echo "    ✗ MinIO"; fi
	@if [ "$(WITH_GCP_STORAGE)" = "1" ]; then echo "    ✓ Google Cloud Storage"; else echo "    ✗ Google Cloud Storage"; fi
	@if [ "$(WITH_AZURE_BLOB)" = "1" ]; then echo "    ✓ Azure Blob Storage"; else echo "    ✗ Azure Blob Storage"; fi
	@echo ""
	@echo "  $(BOLD)Archival Backends:$(RESET)"
	@if [ "$(WITH_GLACIER)" = "1" ]; then echo "    ✓ AWS Glacier"; else echo "    ✗ AWS Glacier"; fi
	@if [ "$(WITH_AZURE_ARCHIVE)" = "1" ]; then echo "    ✓ Azure Archive"; else echo "    ✗ Azure Archive"; fi
	@echo ""
	@echo "  $(BOLD)Build Tags:$(RESET) $(BUILD_TAGS)"
	@echo ""

# ==============================================================================
# Version Management
# ==============================================================================

# Read version from VERSION file
VERSION := $(shell cat VERSION 2>/dev/null || echo "0.1.0-alpha")

.PHONY: version
## version: Display current version
version:
	@echo "$(CYAN)$(BOLD)Current Version:$(RESET) $(VERSION)"

.PHONY: version-bump-patch
## version-bump-patch: Bump patch version (X.Y.Z -> X.Y.Z+1)
version-bump-patch:
	@echo "$(CYAN)$(BOLD)→ Bumping patch version...$(RESET)"
	@CURRENT_VERSION=$$(cat VERSION | sed 's/-alpha//' | sed 's/-beta//' | sed 's/-rc.*//''); \
	MAJOR=$$(echo $$CURRENT_VERSION | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT_VERSION | cut -d. -f2); \
	PATCH=$$(echo $$CURRENT_VERSION | cut -d. -f3); \
	NEW_PATCH=$$(($$PATCH + 1)); \
	NEW_VERSION="$$MAJOR.$$MINOR.$$NEW_PATCH"; \
	echo "$$NEW_VERSION" > VERSION; \
	echo "$(GREEN)✓ Version bumped from $$CURRENT_VERSION to $$NEW_VERSION$(RESET)"
	@cat VERSION

.PHONY: version-bump-minor
## version-bump-minor: Bump minor version (X.Y.Z -> X.Y+1.0)
version-bump-minor:
	@echo "$(CYAN)$(BOLD)→ Bumping minor version...$(RESET)"
	@CURRENT_VERSION=$$(cat VERSION | sed 's/-alpha//' | sed 's/-beta//' | sed 's/-rc.*//''); \
	MAJOR=$$(echo $$CURRENT_VERSION | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT_VERSION | cut -d. -f2); \
	NEW_MINOR=$$(($$MINOR + 1)); \
	NEW_VERSION="$$MAJOR.$$NEW_MINOR.0"; \
	echo "$$NEW_VERSION" > VERSION; \
	echo "$(GREEN)✓ Version bumped from $$CURRENT_VERSION to $$NEW_VERSION$(RESET)"
	@cat VERSION

.PHONY: version-bump-major
## version-bump-major: Bump major version (X.Y.Z -> X+1.0.0)
version-bump-major:
	@echo "$(CYAN)$(BOLD)→ Bumping major version...$(RESET)"
	@CURRENT_VERSION=$$(cat VERSION | sed 's/-alpha//' | sed 's/-beta//' | sed 's/-rc.*//''); \
	MAJOR=$$(echo $$CURRENT_VERSION | cut -d. -f1); \
	NEW_MAJOR=$$(($$MAJOR + 1)); \
	NEW_VERSION="$$NEW_MAJOR.0.0"; \
	echo "$$NEW_VERSION" > VERSION; \
	echo "$(GREEN)✓ Version bumped from $$CURRENT_VERSION to $$NEW_VERSION$(RESET)"
	@cat VERSION

.PHONY: release
## release: Create a release tag based on VERSION file
release:
	@echo "$(CYAN)$(BOLD)→ Creating release v$(VERSION)...$(RESET)"
	@if [ -z "$$(git status --porcelain)" ]; then \
		git tag -a "v$(VERSION)" -m "Release v$(VERSION)"; \
		echo "$(GREEN)✓ Created release tag v$(VERSION)$(RESET)"; \
		echo "$(CYAN)  To push the tag, run: git push origin v$(VERSION)$(RESET)"; \
	else \
		echo "$(RED)✗ Working directory is not clean. Commit or stash changes first.$(RESET)"; \
		exit 1; \
	fi

.PHONY: release-push
## release-push: Push release tag to origin
release-push:
	@echo "$(CYAN)$(BOLD)→ Pushing release v$(VERSION) to origin...$(RESET)"
	@git push origin "v$(VERSION)"
	@echo "$(GREEN)✓ Release v$(VERSION) pushed to origin$(RESET)"

