# Makefile for go-objstore
# Object store abstraction library with cloud and local storage support

# ==============================================================================
# Configuration
# ==============================================================================

# Optional storage backend features (set to 1 to enable)
# Default: All backends enabled for production builds and releases
WITH_LOCAL ?= 1
WITH_AWS_S3 ?= 1
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

# GO_BUILD_TAGS is the tag set the ci-template passes to lint, build, test,
# security and integration-test (handbook devcontainer.md §6). It defaults to
# the repository's current tags (every backend); override it on the command
# line (`make build GO_BUILD_TAGS="local awss3"`). Space- or comma-separated.
GO_BUILD_TAGS ?= $(strip $(BUILD_TAGS))
export GO_BUILD_TAGS
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
COMMA := ,
GO_BUILD_TAGS_CSV := $(subst $(SPACE),$(COMMA),$(strip $(GO_BUILD_TAGS)))

# Build tag flags for go commands
ifneq ($(strip $(GO_BUILD_TAGS)),)
	TAG_FLAGS := -tags "$(GO_BUILD_TAGS)"
else
	TAG_FLAGS :=
endif

# Version injection (read VERSION file; override pkg/version.Version at link time).
# Named BUILD_VERSION, not VERSION: `make release-alpha VERSION=vX.Y.Z-alpha` and
# `make version-set VERSION=X.Y.Z` take VERSION as an explicit argument, and a
# Makefile-level VERSION would shadow it (release-alpha template v2.3.6 (b)).
BUILD_VERSION := $(shell cat VERSION 2>/dev/null || echo "0.1.0-alpha")
VERSION_PKG := github.com/jeremyhahn/go-objstore/pkg/version
VERSION_LDFLAGS := -X $(VERSION_PKG).Version=$(BUILD_VERSION)

# Go parameters
GO := go
GOBUILD := $(GO) build $(TAG_FLAGS) -ldflags "$(VERSION_LDFLAGS)"
GOTEST := $(GO) test $(TAG_FLAGS)
GOMOD := $(GO) mod
GOCLEAN := $(GO) clean

# Gate tools: scripts/require-tool.sh fails closed (non-zero, install hint) when
# a tool is absent, so no gate passes vacuously (go-objstore#37).
REQUIRE_TOOL := scripts/require-tool.sh
GOLANGCI_LINT_VERSION ?= 2.14.0

# Docker Compose (use v2 syntax for GitHub Actions compatibility)
DOCKER_COMPOSE := docker compose

# Compose project prefix for the integration targets. Concurrent CI jobs share
# one Docker host, and compose's default project name (the directory) made one
# job's `down -v --remove-orphans` remove another job's running containers.
# Unique per CI job (run id + job), stable for a local run; each target adds
# its own suffix. Sanitised to compose's charset: lowercase, digits, - and _.
# CI names the prefix itself with IT_PROJECT, which ci.yml's tear-down steps
# pass to `make clean-test-containers` as well.
COMPOSE_PROJECT ?= $(shell printf '%s' "$(or $(IT_PROJECT),objstore-$(or $(GITHUB_RUN_ID),local)-$(or $(GITHUB_JOB),$(USER),user))" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_\n-' '-')

# Project structure
PROJECT_NAME := go-objstore
MODULE := go-objstore

# Build directories
BIN_DIR := bin
COVERAGE_DIR := coverage

# Minimum total unit-test coverage enforced by `make coverage` and `make ci`.
# ci.yml passes its own COVERAGE_THRESHOLD, the same number.
COVERAGE_THRESHOLD ?= 89

# Module authentication for docker builds. github.com/jeremyhahn/go-enclave is
# private, so every image build that runs `go mod download` needs the forge
# credentials. They reach a build only as a BuildKit secret file outside the
# checkout: never a build arg, an ENV or a COPY, so no image layer carries
# them. Two formats are in use:
#   MODULE_ENV_FILE   GOPRIVATE, GIT_CONFIG_COUNT, GIT_CONFIG_KEY_0 and
#                     GIT_CONFIG_VALUE_0 as KEY=value lines: the devcontainer's
#                     secret, and the file ci.yml hands to every make call.
#   GOMODAUTH_SECRET  the bare GIT_CONFIG_KEY_0 value: the secret the root
#                     Dockerfile and the integration and SDK compose files
#                     read, through GOMODAUTH_ENV.
# `make stage-gomodauth` writes MODULE_ENV_FILE from the environment unless
# the caller names one; `make stage-gomodauth-secret` derives GOMODAUTH_SECRET
# from it. Both are mode 0600 and `make clean-gomodauth` removes them. The
# recipes read values from the environment and from files only; none is ever
# expanded into a command line, so neither `make -n` nor the log can show a
# token. No credentials stage empty files, and the builds resolve from the
# public proxy.
GOMODAUTH_DIR := $(or $(RUNNER_TEMP),$(TMPDIR),/tmp)/$(PROJECT_NAME)-gomodauth-$(shell id -u)
GOMODAUTH_FILE := $(GOMODAUTH_DIR)/gomodauth.env
GOMODAUTH_SECRET := $(GOMODAUTH_DIR)/gomodauth.secret
MODULE_ENV_FILE ?= $(GOMODAUTH_FILE)

# The devcontainer images (build/docker/devcontainer/Dockerfile): the
# toolchain image CI runs its gates in, and the integration image it scans.
# TOOLCHAIN_INPUTS is every file the tools, base and toolchain stages read;
# the toolchain tag is their content hash, so a change to any of them is a new
# image and nothing else is. IT_CI_IMAGE names the integration image by the
# checkout's content: tree, uncommitted diff and untracked files.
DEVCONTAINER_DIR := build/docker/devcontainer
TOOLCHAIN_INPUTS := $(DEVCONTAINER_DIR)/Dockerfile $(DEVCONTAINER_DIR)/Dockerfile.dockerignore \
	scripts/tool-pins.sh scripts/install-tools.sh \
	go.mod go.sum backup/go.mod backup/go.sum api/sdks/go/go.mod api/sdks/go/go.sum \
	examples/encryption/go.mod examples/encryption/go.sum
TOOLCHAIN_TAG = $(shell { cat $(TOOLCHAIN_INPUTS); printf '%s' "$(GO_IMAGE)"; } | sha256sum | cut -c1-16)
TOOLCHAIN_IMAGE = $(PROJECT_NAME)-toolchain:$(TOOLCHAIN_TAG)
# The glibc image `make lib-test` runs in (the Dockerfile's capi stage): a Go
# c-shared library cannot load on the toolchain image's musl.
CAPI_IMAGE = $(PROJECT_NAME)-capi:$(TOOLCHAIN_TAG)
IT_IMAGE ?= $(PROJECT_NAME)-it:local
# The release image's tag (root Dockerfile); CI names one per run.
RELEASE_IMAGE ?= $(PROJECT_NAME)
IT_CI_IMAGE = $(PROJECT_NAME)-ci-it:$(shell { git rev-parse 'HEAD^{tree}'; git diff HEAD; \
	git ls-files -oz --exclude-standard | xargs -0r sha256sum; } 2>/dev/null | sha256sum | cut -c1-16)
# GO_IMAGE overrides the Dockerfile's Go base image; unset, its default
# applies. GO_BUILD_PROXY_ARG hands the caller's GOPROXY to the stages that
# fetch; the shell expands $$GOPROXY, so make never echoes its value.
GO_IMAGE ?=
GO_BUILD_PROXY_ARG = $(if $(GOPROXY),--build-arg GO_BUILD_PROXY="$$GOPROXY")
DEVCONTAINER_BUILD = DOCKER_BUILDKIT=1 docker buildx build --load \
	$(if $(GO_IMAGE),--build-arg GO_IMAGE=$(GO_IMAGE)) \
	$(GO_BUILD_PROXY_ARG) \
	--secret id=gomodauth,src=$(MODULE_ENV_FILE) \
	-f $(DEVCONTAINER_DIR)/Dockerfile

# The nested modules: each has its own go.mod and replaces the root module by
# relative path. `make build` builds them; CI tests them in its nested-module
# jobs.
NESTED_MODULES := backup api/sdks/go examples/encryption

# Test coverage packages
PKG_COVER := github.com/jeremyhahn/go-objstore/pkg/adapters,github.com/jeremyhahn/go-objstore/pkg/azure,github.com/jeremyhahn/go-objstore/pkg/azurearchive,github.com/jeremyhahn/go-objstore/pkg/local,github.com/jeremyhahn/go-objstore/pkg/s3,github.com/jeremyhahn/go-objstore/pkg/factory,github.com/jeremyhahn/go-objstore/pkg/glacier,github.com/jeremyhahn/go-objstore/pkg/gcs,github.com/jeremyhahn/go-objstore/pkg/storagefs,github.com/jeremyhahn/go-objstore/pkg/cli,github.com/jeremyhahn/go-objstore/pkg/server/grpc,github.com/jeremyhahn/go-objstore/pkg/server/rest,github.com/jeremyhahn/go-objstore/pkg/server/quic,github.com/jeremyhahn/go-objstore/pkg/server/mcp

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
	@echo "$(CYAN)  Build tags: $(GO_BUILD_TAGS)$(RESET)"
	@$(GOBUILD) ./...
	@echo "$(GREEN)✓ Library packages compiled$(RESET)"
	@echo "$(CYAN)$(BOLD)→ Building the library packages in FIPS 140-3 mode (GOFIPS140=latest)...$(RESET)"
	@GOFIPS140=latest $(GOBUILD) ./...
	@echo "$(GREEN)✓ FIPS build compiled$(RESET)"
	@for m in $(NESTED_MODULES); do \
		echo "$(CYAN)$(BOLD)→ Building the nested module $$m...$(RESET)"; \
		(cd $$m && $(GO) build ./...) || exit 1; \
	done
	@echo "$(GREEN)✓ Nested modules compiled$(RESET)"
	@$(MAKE) --no-print-directory build-cli
	@$(MAKE) --no-print-directory build-server
	@$(MAKE) --no-print-directory lib
	@echo "$(GREEN)$(BOLD)✓ All builds complete!$(RESET)"

.PHONY: build-cli
## build-cli: Build the CLI tool
build-cli:
	@echo "$(CYAN)$(BOLD)→ Building CLI tool...$(RESET)"
	@echo "$(CYAN)  Build tags: $(GO_BUILD_TAGS)$(RESET)"
	@mkdir -p $(BIN_DIR)
	@$(GOBUILD) -o $(BIN_DIR)/objstore ./cmd/objstore
	@echo "$(GREEN)✓ CLI built: $(BIN_DIR)/objstore$(RESET)"

.PHONY: build-server
## build-server: Build all server binaries (all-in-one and individual)
build-server:
	@echo "$(CYAN)$(BOLD)→ Building server binaries...$(RESET)"
	@echo "$(CYAN)  Build tags: $(GO_BUILD_TAGS)$(RESET)"
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
## test: Run unit tests under the race detector, with coverage
#
# -race is not optional here (go-objstore#18). This repository's product is
# locked memory, storage pools and replication -- concurrency-sensitive code --
# and the suite ran without the race detector anywhere, locally or in CI, which
# is how a data race in the REST server's TLS start-up survived on main. It
# rides in this target rather than a separate CI job so it costs no extra
# runner slot: CI's `test` job and nightly's `fips` job both call `make test`,
# so both gained race coverage.
#
# -covermode=atomic is required with -race and was already set.
#
# The package list is ./pkg/..., ./internal/... and cmd/objstorelib, NOT
# ./cmd/... . The library shim is the only cmd package carrying tests, and its
# guards against negative lengths from C are worth gating (go-objstore#25); the
# other six cmd packages have no test files at all, so sweeping them in would
# add ~5 points of uncovered statements to the denominator while gating
# nothing. The server binaries' configuration contract (defaults, file, env,
# flags and their precedence) lives in internal/serverconfig so that it IS
# gated here (go-objstore#34). Widening the coverage gate to cover the binaries
# is a deliberate decision, not a side effect of this list.
#
# The backup agent is its own module, which ./... does not reach, so the
# target ends with backup-test: its race tests and per-package coverage gate
# run wherever `make test` does, CI included. It writes no coverage profile,
# so unit.out stays the root module's alone.
test: test-unit
	@$(MAKE) --no-print-directory backup-test

.PHONY: test-unit
## test-unit: Run the root module's unit tests (race detector) into coverage/unit.out
test-unit:
	@echo "$(CYAN)$(BOLD)→ Running unit tests with coverage (race detector)...$(RESET)"
	@mkdir -p $(COVERAGE_DIR)
	$(GO) test -race -timeout=20m $(TAG_FLAGS) -coverprofile=$(COVERAGE_DIR)/unit.out -covermode=atomic ./pkg/... ./internal/... ./cmd/objstorelib/...
	$(GO) test -race -timeout=20m $(TAG_FLAGS) ./cmd/objstore
	@echo ""
	@echo "$(CYAN)$(BOLD)→ Coverage Summary:$(RESET)"
	@$(GO) tool cover -func=$(COVERAGE_DIR)/unit.out | tail -1 | awk '{print "  $(GREEN)Total Coverage: " $$NF "$(RESET)"}' || true
	@echo "$(GREEN)✓ Unit tests complete$(RESET)"

.PHONY: coverage
## coverage: Run test-unit and fail below COVERAGE_THRESHOLD (the CI coverage gate)
# The backup module is gated by backup-test, in CI's nested-module-test job.
coverage: test-unit
	@total=$$($(GO) tool cover -func=$(COVERAGE_DIR)/unit.out | awk '/^total:/ {print $$3}' | tr -d %); \
	echo "Total coverage: $${total}% (threshold: $(COVERAGE_THRESHOLD)%)"; \
	awk -v c="$$total" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { if (c + 0 < t + 0) { print "coverage below " t "%"; exit 1 } }'

.PHONY: test-fips
## test-fips: Run coverage and backup-test in FIPS 140-3 only mode (GOFIPS140=latest GODEBUG=fips140=only)
# A test that reaches a non-approved algorithm fails here, not in a FIPS
# deployment.
test-fips: export GOFIPS140 := latest
test-fips: export GODEBUG := fips140=only
test-fips:
	@$(MAKE) --no-print-directory coverage
	@$(MAKE) --no-print-directory backup-test

.PHONY: integration-test
## integration-test: Run all integration tests (all backends + CLI + replication)
integration-test: integration-test-local integration-test-s3 integration-test-azure integration-test-gcs integration-test-factory integration-test-replication integration-test-cli
	@echo "$(GREEN)$(BOLD)✓ All integration tests complete!$(RESET)"

# The suites under test/integration, each its own compose project
# $(COMPOSE_PROJECT)-<suite>.
IT_SUITES := local s3 azure gcs factory replication cli
SDKS := typescript go python ruby rust csharp

# Every target whose docker build fetches go-enclave reads the staged bare-key
# secret through GOMODAUTH_ENV (see stage-gomodauth).
GOMODAUTH_TARGETS := $(addprefix integration-test-,$(IT_SUITES)) \
	$(addprefix test-sdk-,$(SDKS)) $(addprefix integration-test-sdk-,$(SDKS)) \
	test-sdks integration-test-sdks docker-build-release
$(GOMODAUTH_TARGETS): stage-gomodauth-secret
$(GOMODAUTH_TARGETS): export GOMODAUTH_ENV = $(GOMODAUTH_SECRET)

.PHONY: clean-test-containers
## clean-test-containers: Remove the integration suites' compose stacks and volumes (IT_PROJECT names CI's)
clean-test-containers:
	@for s in $(IT_SUITES); do \
		$(DOCKER_COMPOSE) -p "$(COMPOSE_PROJECT)-$$s" -f test/integration/$$s/docker-compose.yml down -v --remove-orphans || true; \
	done

.PHONY: integration-test-local
## integration-test-local: Run local storage integration tests
integration-test-local:
	@echo "$(CYAN)$(BOLD)→ Running local storage integration tests...$(RESET)"
# --build on every run: the source is baked into these images, so a cached
# image from an earlier commit silently tests old code. That cost real
# debugging time once already.
	@cd test/integration/local && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-local down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/local && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-local run --build --rm test
	@cd test/integration/local && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-local down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ Local integration tests complete$(RESET)"

.PHONY: integration-test-s3
## integration-test-s3: Run S3 integration tests against Floci
integration-test-s3:
	@echo "$(CYAN)$(BOLD)→ Running S3 integration tests (Floci)...$(RESET)"
	@cd test/integration/s3 && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-s3 down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/s3 && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-s3 up -d --wait floci
	@cd test/integration/s3 && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-s3 run --build --rm test
	@cd test/integration/s3 && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-s3 down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ S3 integration tests complete$(RESET)"

.PHONY: integration-test-azure
## integration-test-azure: Run Azure/Azurite integration tests
integration-test-azure:
	@echo "$(CYAN)$(BOLD)→ Running Azure/Azurite integration tests...$(RESET)"
	@cd test/integration/azure && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-azure down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/azure && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-azure up -d azurite
	@cd test/integration/azure && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-azure run --build --rm test
	@cd test/integration/azure && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-azure down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ Azure integration tests complete$(RESET)"

.PHONY: integration-test-gcs
## integration-test-gcs: Run GCS emulator integration tests
integration-test-gcs:
	@echo "$(CYAN)$(BOLD)→ Running GCS emulator integration tests...$(RESET)"
	@cd test/integration/gcs && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-gcs down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/gcs && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-gcs up -d fake-gcs
	@cd test/integration/gcs && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-gcs run --build --rm test
	@cd test/integration/gcs && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-gcs down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ GCS integration tests complete$(RESET)"

.PHONY: integration-test-factory
## integration-test-factory: Run factory/common integration tests
integration-test-factory:
	@echo "$(CYAN)$(BOLD)→ Running factory integration tests...$(RESET)"
	@cd test/integration/factory && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-factory down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/factory && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-factory run --build --rm test
	@cd test/integration/factory && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-factory down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ Factory integration tests complete$(RESET)"

.PHONY: integration-test-replication
## integration-test-replication: Run replication integration tests
integration-test-replication:
	@echo "$(CYAN)$(BOLD)→ Running replication integration tests...$(RESET)"
	@cd test/integration/replication && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-replication down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/replication && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-replication run --build --rm test
	@cd test/integration/replication && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-replication down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ Replication integration tests complete$(RESET)"

.PHONY: integration-test-cli
## integration-test-cli: Run CLI integration tests in Docker
integration-test-cli:
	@echo "$(CYAN)$(BOLD)→ Running CLI integration tests...$(RESET)"
	@cd test/integration/cli && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-cli down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/cli && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-cli up --build --abort-on-container-exit --exit-code-from test
	@cd test/integration/cli && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-cli down -v --remove-orphans --timeout 30
	@echo "$(GREEN)✓ CLI integration tests complete$(RESET)"

.PHONY: test-cli
## test-cli: Alias for integration-test-cli
test-cli: integration-test-cli

.PHONY: test-servers
## test-servers: Run server integration tests (gRPC, REST, QUIC, MCP) in Docker
test-servers:
	@echo "$(CYAN)$(BOLD)→ Running server integration tests...$(RESET)"
	@echo "$(CYAN)  Generating test certificates...$(RESET)"
	@cd test/integration/server && bash generate-certs.sh
	@echo "$(CYAN)  Starting server test environment...$(RESET)"
	@cd test/integration/server && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-server down -v --remove-orphans --timeout 30 >/dev/null 2>&1 || true
	@cd test/integration/server && $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-server up --build --abort-on-container-exit --exit-code-from test; status=$$?; $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT)-server down -v --remove-orphans --timeout 30; exit $$status
	@echo "$(GREEN)✓ Server integration tests complete$(RESET)"

.PHONY: integration-test-all
## integration-test-all: Run all integration tests including servers
integration-test-all: integration-test test-servers
	@echo "$(GREEN)$(BOLD)✓ All integration tests complete (including servers)!$(RESET)"

# ==============================================================================
# SDK Tests
# ==============================================================================

.PHONY: test-sdks
## test-sdks: Run ALL SDK unit tests (TypeScript, Go, Python, Ruby, Rust, C#)
test-sdks:
	@echo "$(CYAN)$(BOLD)→ Running all SDK unit tests...$(RESET)"
	@cd api/sdks && $(MAKE) test

.PHONY: integration-test-sdks
## integration-test-sdks: Run ALL SDK integration tests (TypeScript, Go, Python, Ruby, Rust, C#)
integration-test-sdks:
	@echo "$(CYAN)$(BOLD)→ Running all SDK integration tests...$(RESET)"
	@cd api/sdks && $(MAKE) integration-test

.PHONY: conformance-test
## conformance-test: Run the cross-protocol conformance suite (all 5 transports against one server)
conformance-test:
	@echo "$(CYAN)$(BOLD)→ Running cross-protocol conformance suite...$(RESET)"
	@cd api/sdks/go && go test -tags conformance -run TestConformance -count=1 -timeout 300s -v .
	@echo "$(GREEN)✓ Conformance suite complete$(RESET)"

.PHONY: sdk-smoke
## sdk-smoke: Run SDK e2e smoke tests against a locally launched server (MCP + Unix transports)
sdk-smoke:
	@echo "$(CYAN)$(BOLD)→ Running SDK e2e smoke tests...$(RESET)"
	@bash -c 'set -e; \
		eval "$$(./scripts/start-test-server.sh)"; \
		trap "kill $$(cat /tmp/objstore-smoke.pid) 2>/dev/null || true" EXIT; \
		echo "→ Go SDK smoke"; \
		(cd api/sdks/go && go test -tags e2esmoke -run TestE2ESmoke -count=1 -v .); \
		if command -v python3 >/dev/null && python3 -c "import pytest" 2>/dev/null; then \
			echo "→ Python SDK smoke"; (cd api/sdks/python && python3 -m pytest tests/e2e -q); \
		else echo "⊘ Python smoke skipped (pytest unavailable)"; fi; \
		if command -v ruby >/dev/null && (cd api/sdks/ruby && bundle exec rspec --version >/dev/null 2>&1); then \
			echo "→ Ruby SDK smoke"; (cd api/sdks/ruby && bundle exec rspec spec/e2e); \
		else echo "⊘ Ruby smoke skipped (rspec unavailable)"; fi; \
		if command -v npx >/dev/null && [ -d api/sdks/typescript/node_modules ]; then \
			echo "→ TypeScript SDK smoke"; (cd api/sdks/typescript && npx jest tests/e2e); \
		else printf "$(YELLOW)⊘ TypeScript smoke skipped (requires node/npx and node_modules)$(RESET)\n"; fi; \
		if command -v dotnet >/dev/null && dotnet --list-sdks 2>/dev/null | grep -q "^9\."; then \
			echo "→ C# SDK smoke"; (cd api/sdks/csharp && dotnet test Tests/Tests.csproj --filter "Category=E2E"); \
		else printf "$(YELLOW)⊘ C# smoke skipped (requires .NET 9 SDK)$(RESET)\n"; fi'
	@echo "$(GREEN)✓ SDK smoke tests complete$(RESET)"

.PHONY: test-sdk-typescript
## test-sdk-typescript: Run TypeScript SDK unit tests
test-sdk-typescript:
	@cd api/sdks/typescript && $(MAKE) test

.PHONY: test-sdk-go
## test-sdk-go: Run Go SDK unit tests
test-sdk-go:
	@cd api/sdks/go && $(MAKE) test

# The backup agent is its own module (backup/go.mod), so `make lint` and
# `make test` at the root do not reach it. These two targets gate it: vet,
# gofmt and golangci-lint with the root configuration, then the race-enabled
# unit tests with COVERAGE_THRESHOLD applied to each library package
# (backup/pkg/... and backup/internal/...; the command is tested, not gated,
# as at the root).
BACKUP_DIR := backup

.PHONY: backup-lint
## backup-lint: Vet, gofmt and golangci-lint the backup agent module
backup-lint:
	@echo "$(CYAN)$(BOLD)→ Linting the backup agent module...$(RESET)"
	@cd $(BACKUP_DIR) && $(GO) vet ./...
	@UNFORMATTED=$$(gofmt -l $(BACKUP_DIR)); if [ -n "$$UNFORMATTED" ]; then \
		echo "$(RED)✗ gofmt needed:$(RESET) $$UNFORMATTED"; exit 1; fi
	@BIN=$$($(REQUIRE_TOOL) golangci-lint $(GOLANGCI_LINT_VERSION)) || { echo "$(RED)✗ golangci-lint $(GOLANGCI_LINT_VERSION) required$(RESET)"; exit 1; }; \
	cd $(BACKUP_DIR) && "$$BIN" run --timeout=10m --config ../.golangci.yml ./... && \
	echo "$(GREEN)✓ Backup agent lint passed$(RESET)"

.PHONY: backup-test
## backup-test: Run the backup agent module's unit tests (race detector, per-package coverage gate)
backup-test:
	@echo "$(CYAN)$(BOLD)→ Running backup agent unit tests (race detector)...$(RESET)"
	@mkdir -p $(COVERAGE_DIR)
	@cd $(BACKUP_DIR) && $(GO) test -race -timeout=20m -cover ./... > ../$(COVERAGE_DIR)/backup-test.log 2>&1; \
	rc=$$?; cat ../$(COVERAGE_DIR)/backup-test.log; exit $$rc
	@awk -v min=$(COVERAGE_THRESHOLD) ' \
		$$1 == "ok" && $$2 ~ /\/backup\/(pkg|internal)\// { \
			for (i = 1; i <= NF; i++) if ($$i == "coverage:") { c = $$(i + 1); sub(/%/, "", c); \
				if (c + 0 < min) { printf "✗ %s coverage %s%% is below %s%%\n", $$2, c, min; bad = 1 } } } \
		END { exit bad }' $(COVERAGE_DIR)/backup-test.log
	@echo "$(GREEN)✓ Backup agent unit tests complete$(RESET)"

.PHONY: test-sdk-python
## test-sdk-python: Run Python SDK unit tests
test-sdk-python:
	@cd api/sdks/python && $(MAKE) test

.PHONY: test-sdk-ruby
## test-sdk-ruby: Run Ruby SDK unit tests
test-sdk-ruby:
	@cd api/sdks/ruby && $(MAKE) test

.PHONY: test-sdk-rust
## test-sdk-rust: Run Rust SDK unit tests
test-sdk-rust:
	@cd api/sdks/rust && $(MAKE) test

.PHONY: test-sdk-csharp
## test-sdk-csharp: Run C# SDK unit tests
test-sdk-csharp:
	@cd api/sdks/csharp && $(MAKE) test

.PHONY: integration-test-sdk-typescript
## integration-test-sdk-typescript: Run TypeScript SDK integration tests
integration-test-sdk-typescript:
	@cd api/sdks/typescript && $(MAKE) integration-test

.PHONY: integration-test-sdk-go
## integration-test-sdk-go: Run Go SDK integration tests
integration-test-sdk-go:
	@cd api/sdks/go && $(MAKE) integration-test

.PHONY: integration-test-sdk-python
## integration-test-sdk-python: Run Python SDK integration tests
integration-test-sdk-python:
	@cd api/sdks/python && $(MAKE) integration-test

.PHONY: integration-test-sdk-ruby
## integration-test-sdk-ruby: Run Ruby SDK integration tests
integration-test-sdk-ruby:
	@cd api/sdks/ruby && $(MAKE) integration-test

.PHONY: integration-test-sdk-rust
## integration-test-sdk-rust: Run Rust SDK integration tests
integration-test-sdk-rust:
	@cd api/sdks/rust && $(MAKE) integration-test

.PHONY: integration-test-sdk-csharp
## integration-test-sdk-csharp: Run C# SDK integration tests
integration-test-sdk-csharp:
	@cd api/sdks/csharp && $(MAKE) integration-test

.PHONY: test-sdks-all
## test-sdks-all: Run ALL SDK unit + integration tests, fail-fast (aborts on first failure)
test-sdks-all: test-sdk-typescript integration-test-sdk-typescript test-sdk-go integration-test-sdk-go test-sdk-python integration-test-sdk-python test-sdk-ruby integration-test-sdk-ruby test-sdk-rust integration-test-sdk-rust test-sdk-csharp integration-test-sdk-csharp
	@echo "$(GREEN)$(BOLD)✓ All SDK unit + integration tests passed$(RESET)"

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
bench: bench-local bench-s3 bench-gcs bench-azure bench-factory bench-common bench-storagefs
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
	@echo "$(CYAN)Using build tags: $(GO_BUILD_TAGS_CSV)$(RESET)"
	@echo ""
	@for pkg in $$($(GO) list ./pkg/...); do \
		output=$$($(GO) test -tags="$(GO_BUILD_TAGS_CSV)" -cover "$$pkg" 2>/dev/null); \
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
		output=$$($(GO) test -tags="$(GO_BUILD_TAGS_CSV)" -cover "$$pkg" 2>/dev/null); \
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

.PHONY: stage-gomodauth
## stage-gomodauth: Stage the module credentials for docker builds, mode 0600, outside the checkout
stage-gomodauth:
	@umask 077; mkdir -p "$(GOMODAUTH_DIR)"; \
	[ "$$(stat -c %u "$(GOMODAUTH_DIR)")" = "$$(id -u)" ] || { echo "$(RED)✗ $(GOMODAUTH_DIR) is not owned by $$(id -un)$(RESET)"; exit 1; }; \
	if [ "$(MODULE_ENV_FILE)" = "$(GOMODAUTH_FILE)" ]; then \
		: > "$(GOMODAUTH_FILE)"; \
		for v in GOPRIVATE GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0; do \
			eval "val=\$${$$v:-}"; \
			[ -z "$$val" ] || printf '%s=%s\n' "$$v" "$$val" >> "$(GOMODAUTH_FILE)"; \
		done; \
	fi; \
	[ -f "$(MODULE_ENV_FILE)" ] || { echo "$(RED)✗ MODULE_ENV_FILE $(MODULE_ENV_FILE) does not exist$(RESET)"; exit 1; }

.PHONY: stage-gomodauth-secret
## stage-gomodauth-secret: Stage the bare-key secret the root Dockerfile and the compose files read
stage-gomodauth-secret: stage-gomodauth
	@umask 077; sed -n 's/^\(export \)\{0,1\}GIT_CONFIG_KEY_0=//p' "$(MODULE_ENV_FILE)" | \
		sed "s/^['\"]//; s/['\"]$$//" | tr -d '\n' > "$(GOMODAUTH_SECRET)"; \
	if [ -s "$(GOMODAUTH_SECRET)" ]; then echo "staged the module credential"; \
	else echo "no module credential staged; builds resolve from the public proxy"; fi

.PHONY: clean-gomodauth
## clean-gomodauth: Remove the staged module credentials
clean-gomodauth:
	@rm -rf "$(GOMODAUTH_DIR)"

.PHONY: docker-build
## docker-build: Build the devcontainer's toolchain and integration images
docker-build: docker-build-toolchain docker-build-capi docker-build-it

# A present image is reused: its tag is its content. Two jobs that build the
# same tag at once race to it, and the loser succeeds if the winner's image is
# there.
.PHONY: docker-build-toolchain
## docker-build-toolchain: Build the toolchain image CI runs its gates in (tag: make toolchain-image)
docker-build-toolchain: stage-gomodauth
	@if docker image inspect $(TOOLCHAIN_IMAGE) >/dev/null 2>&1; then \
		echo "$(TOOLCHAIN_IMAGE) is present"; \
	else \
		$(DEVCONTAINER_BUILD) --target toolchain -t $(TOOLCHAIN_IMAGE) . || \
			docker image inspect $(TOOLCHAIN_IMAGE) >/dev/null 2>&1; \
	fi

.PHONY: docker-build-capi
## docker-build-capi: Build the glibc image CI runs make lib-test in (tag: make capi-image)
docker-build-capi: stage-gomodauth
	@if docker image inspect $(CAPI_IMAGE) >/dev/null 2>&1; then \
		echo "$(CAPI_IMAGE) is present"; \
	else \
		$(DEVCONTAINER_BUILD) --target capi -t $(CAPI_IMAGE) . || \
			docker image inspect $(CAPI_IMAGE) >/dev/null 2>&1; \
	fi

.PHONY: docker-build-it
## docker-build-it: Build the integration image as IT_IMAGE (CI: make it-image)
docker-build-it: stage-gomodauth
	@if [ "$(IT_IMAGE)" = "$(IT_CI_IMAGE)" ] && docker image inspect $(IT_IMAGE) >/dev/null 2>&1; then \
		echo "$(IT_IMAGE) is present"; \
	else \
		$(DEVCONTAINER_BUILD) --target integration -t $(IT_IMAGE) . || \
			{ [ "$(IT_IMAGE)" = "$(IT_CI_IMAGE)" ] && docker image inspect $(IT_IMAGE) >/dev/null 2>&1; }; \
	fi

.PHONY: toolchain-image
## toolchain-image: Print the toolchain image's content-addressed tag
toolchain-image:
	@echo $(TOOLCHAIN_IMAGE)

.PHONY: capi-image
## capi-image: Print the C API image's content-addressed tag
capi-image:
	@echo $(CAPI_IMAGE)

.PHONY: it-image
## it-image: Print the integration image's content-addressed tag
it-image:
	@echo $(IT_CI_IMAGE)

.PHONY: deps-check
## deps-check: Fail on a module requirement behind its latest release (scripts/deps-check.sh)
deps-check:
	@./scripts/deps-check.sh

.PHONY: lint-gates
## lint-gates: The tree-wide gates: VERSION consistency and go mod verify for every module
lint-gates: version-check
	@for m in . $(NESTED_MODULES); do \
		echo "$(CYAN)→ go mod verify in $$m$(RESET)"; \
		(cd $$m && $(GO) mod verify) || exit 1; \
	done
	@echo "$(GREEN)✓ Lint gates passed$(RESET)"

.PHONY: docker-build-release
## docker-build-release: Build the release image (root Dockerfile) as RELEASE_IMAGE
docker-build-release:
	@echo "$(CYAN)$(BOLD)→ Building the release image...$(RESET)"
	@DOCKER_BUILDKIT=1 docker buildx build --load \
		--secret id=gomodauth,src=$(GOMODAUTH_SECRET) \
		-t $(RELEASE_IMAGE) -f Dockerfile .
	@echo "$(GREEN)✓ Docker image built: $(RELEASE_IMAGE)$(RESET)"

.PHONY: docker-run
## docker-run: Run Docker container interactively
docker-run:
	@echo "$(CYAN)$(BOLD)→ Running docker container...$(RESET)"
	@docker run -it $(RELEASE_IMAGE)

.PHONY: docker-test
## docker-test: Run tests in Docker container
docker-test: docker-build-release
	@echo "$(CYAN)$(BOLD)→ Running tests in docker container...$(RESET)"
	@docker run $(RELEASE_IMAGE) make test
	@echo "$(GREEN)$(BOLD)✓ Docker tests complete!$(RESET)"

.PHONY: lint
## lint: Run golangci-lint for code quality analysis
lint:
	@echo "$(CYAN)$(BOLD)→ Running golangci-lint $(GOLANGCI_LINT_VERSION)...$(RESET)"
	@BIN=$$($(REQUIRE_TOOL) golangci-lint $(GOLANGCI_LINT_VERSION)) || { echo "$(RED)✗ golangci-lint $(GOLANGCI_LINT_VERSION) required$(RESET)"; exit 1; }; \
	echo "$(CYAN)  Using $$BIN$(RESET)"; \
	"$$BIN" run --timeout=10m --build-tags "$(GO_BUILD_TAGS_CSV)" && \
	echo "$(GREEN)✓ Linting passed$(RESET)"
	@echo "$(CYAN)$(BOLD)→ Running actionlint...$(RESET)"
	@BIN=$$($(REQUIRE_TOOL) actionlint) || { echo "$(RED)✗ actionlint required$(RESET)"; exit 1; }; \
	"$$BIN" .github/workflows/*.yml && \
	echo "$(GREEN)✓ actionlint passed$(RESET)"

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

# The build tags that gate the cloud backends into the build. gosec and
# govulncheck both need them or they analyse 129 of the 140 files and the
# S3/Glacier/Azure implementations go unscanned.
# An explicit GO_BUILD_TAGS overrides the default.
SECURITY_TAGS = $(if $(filter file,$(origin GO_BUILD_TAGS)),local$(COMMA)awss3$(COMMA)gcpstorage$(COMMA)azureblob$(COMMA)glacier,$(GO_BUILD_TAGS_CSV))

.PHONY: security
## security: Full devops.md §8 suite (gosec, govulncheck, go mod verify, trivy fs)
# Container images are NOT scanned here -- `make trivy-image` builds and scans
# the release image, which needs a Docker daemon and several minutes. CI scans
# the same image in its image job, on the full tier. The gosec half is
# scripts/gosec-scan.sh, which explains its two scans.
security:
	@echo "$(CYAN)$(BOLD)→ Running security checks...$(RESET)"
	@echo "$(CYAN)  Running gosec...$(RESET)"
	@PATH="$$PATH:$$(go env GOPATH)/bin" ./scripts/gosec-scan.sh "$(SECURITY_TAGS)" && echo "$(GREEN)✓ gosec passed$(RESET)"
	@echo "$(CYAN)  Running govulncheck in every module...$(RESET)"
	@BIN=$$($(REQUIRE_TOOL) govulncheck) || { echo "$(RED)✗ govulncheck required$(RESET)"; exit 1; }; \
	export GOFLAGS="-tags=$(SECURITY_TAGS)"; \
	for m in . $(NESTED_MODULES); do \
		echo "$(CYAN)  govulncheck in $$m$(RESET)"; \
		(cd $$m && "$$BIN" ./...) || { echo "$(RED)✗ govulncheck found vulnerabilities in $$m$(RESET)"; exit 1; }; \
	done; \
	echo "$(GREEN)✓ govulncheck passed$(RESET)"
	@echo "$(CYAN)  Verifying module integrity...$(RESET)"
	@for m in . $(NESTED_MODULES); do (cd $$m && $(GO) mod verify) || { echo "$(RED)✗ go mod verify failed in $$m$(RESET)"; exit 1; }; done
	@echo "$(GREEN)✓ go mod verify passed$(RESET)"
	# test/ and examples/ do not ship. coverage/ and bin/ are build output.
	# **/vendor is gitignored language-SDK tooling that is not in a CI checkout.
	# api/sdks/* IS scanned: those SDKs ship, so their lockfiles are in scope.
	@echo "$(CYAN)  Running trivy filesystem scan...$(RESET)"
	@BIN=$$($(REQUIRE_TOOL) trivy) || { echo "$(RED)✗ trivy required for the filesystem scan$(RESET)"; exit 1; }; \
	"$$BIN" fs --scanners vuln,secret --severity CRITICAL,HIGH --exit-code 1 \
		--skip-dirs test --skip-dirs examples --skip-dirs coverage --skip-dirs bin \
		--skip-dirs '**/vendor' . \
		&& echo "$(GREEN)✓ trivy fs passed$(RESET)" \
		|| { echo "$(RED)✗ trivy found CRITICAL/HIGH findings$(RESET)"; exit 1; }
	@echo "$(GREEN)✓ Security checks complete$(RESET)"
	@echo "$(YELLOW)Note: container images are scanned separately - run 'make trivy-image'$(RESET)"

.PHONY: trivy-image
## trivy-image: Build the release image and scan it with Trivy (CRITICAL/HIGH gate)
# Mirrors the gate in the ci.yml docker job so the same failure reproduces
# locally (devops.md §4). Builds the image, so it needs Docker and is not part
# of `make security`.
trivy-image: docker-build-release
	@echo "$(CYAN)$(BOLD)→ Scanning container image with Trivy...$(RESET)"
	@BIN=$$($(REQUIRE_TOOL) trivy) || { echo "$(RED)✗ trivy required for the image scan$(RESET)"; exit 1; }; \
	"$$BIN" image --severity CRITICAL,HIGH --exit-code 1 --no-progress $(RELEASE_IMAGE) \
		&& echo "$(GREEN)✓ trivy image passed$(RESET)" \
		|| { echo "$(RED)✗ trivy found CRITICAL/HIGH findings in $(RELEASE_IMAGE)$(RESET)"; exit 1; }

.PHONY: ci
## ci: Run the full CI pipeline locally (mirrors .github/workflows/ci.yml, Go-core only; SDK suites run via test-sdks/test-sdks-all)
ci:
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running CI Pipeline (mirrors GitHub CI)$(RESET)"
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo ""
	@echo "$(CYAN)$(BOLD)[1/8] Verifying module dependencies...$(RESET)"
	@$(GO) mod verify || (echo "$(RED)✗ Module verification failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[2/8] Verifying version consistency...$(RESET)"
	@$(MAKE) version-check || (echo "$(RED)✗ Version check failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[3/8] Running unit tests with coverage...$(RESET)"
	@$(MAKE) test || (echo "$(RED)✗ Tests failed$(RESET)" && exit 1)
	@COVERAGE=$$($(GO) tool cover -func=$(COVERAGE_DIR)/unit.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "  Coverage: $${COVERAGE}% (threshold: $(COVERAGE_THRESHOLD)%)"; \
	if awk "BEGIN{exit !($${COVERAGE} < $(COVERAGE_THRESHOLD))}"; then \
		echo "$(RED)✗ Coverage $${COVERAGE}% is below threshold $(COVERAGE_THRESHOLD)%$(RESET)"; \
		exit 1; \
	fi
	@echo ""
	@echo "$(CYAN)$(BOLD)[4/8] Running cross-protocol conformance suite...$(RESET)"
	@$(MAKE) conformance-test || (echo "$(RED)✗ Conformance tests failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[5/8] Running linter...$(RESET)"
	@$(MAKE) lint || (echo "$(RED)✗ Linting failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[6/8] Running security checks...$(RESET)"
	@$(MAKE) security || (echo "$(RED)✗ Security checks failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[7/8] Building binaries...$(RESET)"
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) build-cli || (echo "$(RED)✗ CLI build failed$(RESET)" && exit 1)
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) build-server || (echo "$(RED)✗ Server build failed$(RESET)" && exit 1)
	@WITH_LOCAL=1 WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1 $(MAKE) lib || (echo "$(RED)✗ Library build failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(CYAN)$(BOLD)[8/8] Running integration tests...$(RESET)"
	@$(MAKE) integration-test || (echo "$(RED)✗ Integration tests failed$(RESET)" && exit 1)
	@echo ""
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"
	@echo "$(GREEN)$(BOLD)  ✓ CI Pipeline Passed!$(RESET)"
	@echo "$(GREEN)$(BOLD)========================================$(RESET)"
	@echo ""
	@echo "$(YELLOW)Note: container image scanning is not part of this pipeline - run 'make trivy-image'$(RESET)"
	@echo "$(YELLOW)      (CI gates the same image in the docker job; Grype is a GitHub-only second opinion).$(RESET)"

.PHONY: pre-commit
## pre-commit: Run all pre-commit checks (format, vet, lint, test)
pre-commit: clean
	@echo "$(CYAN)$(BOLD)→ Running pre-commit checks...$(RESET)\n"
	@bash ./scripts/pre-commit

.PHONY: clean
## clean: Clean up build artifacts and test outputs
clean:
	@echo "$(CYAN)$(BOLD)→ Cleaning up...$(RESET)"
	@$(GOCLEAN) -cache 2>/dev/null || true
	@$(GOCLEAN) -testcache 2>/dev/null || true
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
	@# Clean SDK build artifacts (may require sudo if created by Docker)
	@rm -rf api/sdks/typescript/node_modules api/sdks/typescript/coverage api/sdks/typescript/dist 2>/dev/null || true
	@rm -rf api/sdks/python/.venv api/sdks/python/venv api/sdks/python/.pytest_cache api/sdks/python/.mypy_cache 2>/dev/null || true
	@rm -rf api/sdks/python/*.egg-info api/sdks/python/dist api/sdks/python/build api/sdks/python/htmlcov 2>/dev/null || true
	@find api/sdks/python -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf api/sdks/ruby/coverage api/sdks/ruby/.bundle api/sdks/ruby/vendor 2>/dev/null || true
	@rm -rf api/sdks/rust/target 2>/dev/null || true
	@rm -rf api/sdks/csharp/bin api/sdks/csharp/obj api/sdks/csharp/Tests/bin api/sdks/csharp/Tests/obj 2>/dev/null || true
	@rm -rf api/sdks/go/bin 2>/dev/null || true
	@echo "$(GREEN)✓ Clean complete$(RESET)"

.PHONY: lib-test
## lib-test: Build the shared library and run the C API test suite against it
#
# The ONLY coverage that reaches the //export wrappers. `go test` refuses cgo in
# test files ("use of cgo in test ... not supported"), so no Go test can call
# ObjstorePut and friends with C arguments — a guard could be deleted from any
# wrapper and every Go test would still pass (go-objstore#25).
#
# Depends on `lib` so the .so and its generated header are current; pass the
# same WITH_* flags you would pass to `lib`.
lib-test: lib
	@echo "$(CYAN)$(BOLD)→ Running C API test suite...$(RESET)"
	@$(MAKE) -C examples/c_client run
	@echo "$(GREEN)✓ C API test suite passed$(RESET)"

.PHONY: lib
## lib: Generate a shared object (.so) file for the library
lib:
	@echo "$(CYAN)$(BOLD)→ Building shared library...$(RESET)"
	@echo "$(CYAN)  Build tags: $(GO_BUILD_TAGS)$(RESET)"
	@mkdir -p $(BIN_DIR)
	@$(GO) build $(TAG_FLAGS) -ldflags "$(VERSION_LDFLAGS)" -buildmode=c-shared -o $(BIN_DIR)/libobjstore.so ./cmd/objstorelib
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
	@echo "  $(YELLOW)security$(RESET)                     Full §8 suite: gosec, govulncheck, go mod verify, trivy fs"
	@echo "  $(YELLOW)trivy-image$(RESET)                  Build the release image and scan it with Trivy (needs Docker)"
	@echo "  $(YELLOW)pre-commit$(RESET)                   Run all pre-commit checks"
	@echo "  $(YELLOW)integration-test$(RESET)             Run all integration tests (backends + CLI)"
	@echo "  $(YELLOW)integration-test-local$(RESET)       Run local storage integration tests"
	@echo "  $(YELLOW)integration-test-s3$(RESET)          Run S3 integration tests"
	@echo "  $(YELLOW)integration-test-azure$(RESET)       Run Azure integration tests"
	@echo "  $(YELLOW)integration-test-gcs$(RESET)         Run GCS integration tests"
	@echo "  $(YELLOW)integration-test-replication$(RESET) Run replication integration tests"
	@echo "  $(YELLOW)integration-test-cli$(RESET)         Run CLI integration tests"
	@echo "  $(YELLOW)test-servers$(RESET)                 Run server integration tests (gRPC, REST, QUIC, MCP)"
	@echo "  $(YELLOW)integration-test-all$(RESET)         Run all integration tests including servers"
	@echo "  $(YELLOW)test-sdks$(RESET)                    Run ALL SDK unit tests (TS, Go, Python, Ruby, Rust, C#)"
	@echo "  $(YELLOW)integration-test-sdks$(RESET)        Run ALL SDK integration tests (TS, Go, Python, Ruby, Rust, C#)"
	@echo "  $(YELLOW)test-sdks-all$(RESET)                Run ALL SDK unit + integration tests, fail-fast"
	@echo "  $(YELLOW)ci$(RESET)                           Run full CI pipeline locally (Go-core only; SDKs via test-sdks-all)"
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
	@echo "  $(YELLOW)version-sync$(RESET)                 Propagate root VERSION to all SDK manifests"
	@echo "  $(YELLOW)version-check$(RESET)                Verify all SDK manifests match root VERSION"
	@echo "  $(YELLOW)version-set$(RESET)                  Set root VERSION (VERSION=X.Y.Z) and sync SDKs"
	@echo "  $(YELLOW)version-bump-patch$(RESET)           Bump patch version (X.Y.Z -> X.Y.Z+1)"
	@echo "  $(YELLOW)version-bump-minor$(RESET)           Bump minor version (X.Y.Z -> X.Y+1.0)"
	@echo "  $(YELLOW)version-bump-major$(RESET)           Bump major version (X.Y.Z -> X+1.0.0)"
	@echo "  $(YELLOW)release$(RESET)                      Create git release tag based on VERSION file"
	@echo "  $(YELLOW)release-push$(RESET)                 Push release tag to origin"
	@echo "  $(YELLOW)release-alpha$(RESET)                Cut a signed -alpha release (VERSION=vX.Y.Z-alpha)"
	@echo "  $(YELLOW)changelog-assemble$(RESET)           Fold changelog.d/ fragments into CHANGELOG.md (VERSION=vX.Y.Z-alpha; run by release-alpha)"
	@echo "  $(YELLOW)test-release-alpha$(RESET)           Run the release path regression harnesses (template + repo wiring)"
	@echo "  $(YELLOW)help$(RESET)                         Display this help message"
	@echo ""
	@echo "$(BOLD)Storage Backend Variables:$(RESET)"
	@echo "  Control which storage backends are included in the build:"
	@echo "  $(GREEN)WITH_LOCAL=1/0$(RESET)        Local disk storage (default: $(WITH_LOCAL))"
	@echo "  $(GREEN)WITH_AWS_S3=1/0$(RESET)       Amazon S3 storage (default: $(WITH_AWS_S3))"
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

# BUILD_VERSION is defined near the top of this file (used for ldflags injection).

.PHONY: version
## version: Display current version
version:
	@echo "$(CYAN)$(BOLD)Current Version:$(RESET) $(BUILD_VERSION)"

.PHONY: version-sync
## version-sync: Propagate the root VERSION to every SDK manifest
version-sync:
	@echo "$(CYAN)$(BOLD)→ Syncing VERSION ($(BUILD_VERSION)) to all SDKs...$(RESET)"
	@bash ./scripts/sync-version.sh
	@echo "$(GREEN)✓ SDK versions synced$(RESET)"

.PHONY: version-check
## version-check: Verify every SDK manifest matches the root VERSION
version-check:
	@echo "$(CYAN)$(BOLD)→ Verifying SDK versions match VERSION ($(BUILD_VERSION))...$(RESET)"
	@bash ./scripts/check-version.sh
	@echo "$(GREEN)✓ All SDK versions match$(RESET)"

# version-set takes the new version via VERSION=X.Y.Z. The argument is only
# honored when it was provided on the command line; otherwise the target
# errors instead of silently re-writing the file with a stale value.
.PHONY: version-set
## version-set: Set root VERSION to X.Y.Z and sync all SDKs (usage: make version-set VERSION=X.Y.Z)
version-set:
	@if [ "$(origin VERSION)" != "command line" ]; then \
		echo "$(RED)✗ VERSION must be passed on the command line (usage: make version-set VERSION=X.Y.Z)$(RESET)"; \
		exit 1; \
	fi
	@if [ -z "$(strip $(VERSION))" ]; then \
		echo "$(RED)✗ VERSION must not be empty (usage: make version-set VERSION=X.Y.Z)$(RESET)"; \
		exit 1; \
	fi
	@echo "$(CYAN)$(BOLD)→ Setting version to $(VERSION)...$(RESET)"
	@printf '%s\n' "$(VERSION)" > VERSION
	@echo "$(GREEN)✓ Root VERSION set to $(VERSION)$(RESET)"
	@$(MAKE) --no-print-directory version-sync

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
	@echo "$(CYAN)$(BOLD)→ Creating release v$(BUILD_VERSION)...$(RESET)"
	@if [ -z "$$(git status --porcelain)" ]; then \
		git tag -a "v$(BUILD_VERSION)" -m "Release v$(BUILD_VERSION)"; \
		echo "$(GREEN)✓ Created release tag v$(BUILD_VERSION)$(RESET)"; \
		echo "$(CYAN)  To push the tag, run: git push origin v$(BUILD_VERSION)$(RESET)"; \
	else \
		echo "$(RED)✗ Working directory is not clean. Commit or stash changes first.$(RESET)"; \
		exit 1; \
	fi

.PHONY: release-push
## release-push: Push release tag to origin
release-push:
	@echo "$(CYAN)$(BOLD)→ Pushing release v$(BUILD_VERSION) to origin...$(RESET)"
	@git push origin "v$(BUILD_VERSION)"
	@echo "$(GREEN)✓ Release v$(BUILD_VERSION) pushed to origin$(RESET)"


# Builds never resolve through a workspace: modules come from Gitea tags
# (dependency-resolution.md §2). Immunizes against stray go.work files.
export GOWORK=off

# ---- dependency-resolution.md targets (2026-08-13 cutover) ----
# The nested modules tagged in lockstep with the root are LOCKSTEP_MODULES in
# mk/release.mk (release-versioning.md P2). deps-latest runs before compiling in a
# consumer repo. deps-latest picks the highest tag INCLUDING -alpha
# prereleases — bare 'go get @latest' ignores prereleases and would downgrade
# to legacy v0.0.0.

.PHONY: deps-latest

# ---- release-alpha: see mk/release.mk (issue #32) ----
# The signed -alpha release path: CHANGELOG fragment assembly, then the company
# release-alpha template (vendored at mk/template/, local additions listed in
# mk/template/release-common.mk), tagging the root, backup and api/sdks/go
# modules in lockstep.
#
#   make release-alpha VERSION=vX.Y.Z-alpha
include mk/release.mk

deps-latest:
	@GOWORK=off sh -c ' \
	SELF=$$(go list -m); \
	for m in $$(go list -m -f "{{if not .Indirect}}{{.Path}}{{end}}" all 2>/dev/null | grep "^github.com/jeremyhahn/" | grep -vx "$$SELF"); do \
	  case "$$(go list -m -f "{{if .Replace}}{{.Replace.Path}}{{end}}" "$$m" 2>/dev/null)" in .*|/*) continue;; esac; \
	  V=$$(go list -m -versions "$$m" 2>/dev/null | tr " " "\n" | tail -1); \
	  if [ -n "$$V" ] && [ "$$V" != "$$m" ]; then go get "$$m@$$V" || exit 1; fi; \
	done; go mod tidy'
