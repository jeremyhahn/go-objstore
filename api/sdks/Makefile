.PHONY: help test integration-test test-all clean
.PHONY: test-typescript test-go test-python test-ruby test-rust test-csharp
.PHONY: integration-test-typescript integration-test-go integration-test-python integration-test-ruby integration-test-rust integration-test-csharp

# Color output
RESET := \033[0m
BOLD := \033[1m
RED := \033[31m
GREEN := \033[32m
YELLOW := \033[33m
CYAN := \033[36m

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "$(BOLD)$(CYAN)go-objstore SDK Test Suite$(RESET)"
	@echo ""
	@echo "$(BOLD)Aggregate Targets:$(RESET)"
	@echo "  $(YELLOW)test$(RESET)                      Run ALL SDK unit tests"
	@echo "  $(YELLOW)integration-test$(RESET)          Run ALL SDK integration tests"
	@echo "  $(YELLOW)test-all$(RESET)                  Run ALL unit + integration tests"
	@echo ""
	@echo "$(BOLD)Individual SDK Unit Tests:$(RESET)"
	@echo "  $(YELLOW)test-typescript$(RESET)           Run TypeScript SDK unit tests"
	@echo "  $(YELLOW)test-go$(RESET)                   Run Go SDK unit tests"
	@echo "  $(YELLOW)test-python$(RESET)               Run Python SDK unit tests"
	@echo "  $(YELLOW)test-ruby$(RESET)                 Run Ruby SDK unit tests"
	@echo "  $(YELLOW)test-rust$(RESET)                 Run Rust SDK unit tests"
	@echo "  $(YELLOW)test-csharp$(RESET)               Run C# SDK unit tests"
	@echo ""
	@echo "$(BOLD)Individual SDK Integration Tests:$(RESET)"
	@echo "  $(YELLOW)integration-test-typescript$(RESET)  Run TypeScript SDK integration tests"
	@echo "  $(YELLOW)integration-test-go$(RESET)          Run Go SDK integration tests"
	@echo "  $(YELLOW)integration-test-python$(RESET)      Run Python SDK integration tests"
	@echo "  $(YELLOW)integration-test-ruby$(RESET)        Run Ruby SDK integration tests"
	@echo "  $(YELLOW)integration-test-rust$(RESET)        Run Rust SDK integration tests"
	@echo "  $(YELLOW)integration-test-csharp$(RESET)      Run C# SDK integration tests"
	@echo ""
	@echo "$(BOLD)Utilities:$(RESET)"
	@echo "  $(YELLOW)clean$(RESET)                     Clean all SDK build artifacts"

# ==============================================================================
# Aggregate Targets
# ==============================================================================

test: test-typescript test-go test-python test-ruby test-rust test-csharp ## Run ALL SDK unit tests
	@echo "$(GREEN)$(BOLD)✓ ALL SDK unit tests passed$(RESET)"

integration-test: integration-test-typescript integration-test-go integration-test-python integration-test-ruby integration-test-rust integration-test-csharp ## Run ALL SDK integration tests
	@echo "$(GREEN)$(BOLD)✓ ALL SDK integration tests passed$(RESET)"

test-all: test integration-test ## Run ALL unit + integration tests
	@echo "$(GREEN)$(BOLD)✓ All SDK tests complete!$(RESET)"

# ==============================================================================
# Individual SDK Unit Tests
# ==============================================================================

test-typescript: ## Run TypeScript SDK unit tests
	@cd typescript && $(MAKE) test

test-go: ## Run Go SDK unit tests
	@cd go && $(MAKE) test

test-python: ## Run Python SDK unit tests
	@cd python && $(MAKE) test

test-ruby: ## Run Ruby SDK unit tests
	@cd ruby && $(MAKE) test

test-rust: ## Run Rust SDK unit tests
	@cd rust && $(MAKE) test

test-csharp: ## Run C# SDK unit tests
	@cd csharp && $(MAKE) test

# ==============================================================================
# Individual SDK Integration Tests
# ==============================================================================

integration-test-typescript: ## Run TypeScript SDK integration tests
	@cd typescript && $(MAKE) integration-test

integration-test-go: ## Run Go SDK integration tests
	@cd go && $(MAKE) integration-test

integration-test-python: ## Run Python SDK integration tests
	@cd python && $(MAKE) integration-test

integration-test-ruby: ## Run Ruby SDK integration tests
	@cd ruby && $(MAKE) integration-test

integration-test-rust: ## Run Rust SDK integration tests
	@cd rust && $(MAKE) integration-test

integration-test-csharp: ## Run C# SDK integration tests
	@cd csharp && $(MAKE) integration-test

# ==============================================================================
# Utilities
# ==============================================================================

clean: ## Clean all SDK build artifacts
	@echo "$(CYAN)$(BOLD)→ Cleaning all SDK build artifacts...$(RESET)"
	@cd typescript && $(MAKE) clean 2>/dev/null || true
	@cd go && $(MAKE) clean 2>/dev/null || true
	@cd python && $(MAKE) clean 2>/dev/null || true
	@cd ruby && $(MAKE) clean 2>/dev/null || true
	@cd rust && $(MAKE) clean 2>/dev/null || true
	@cd csharp && $(MAKE) clean 2>/dev/null || true
	@echo "$(GREEN)✓ All SDK artifacts cleaned$(RESET)"
