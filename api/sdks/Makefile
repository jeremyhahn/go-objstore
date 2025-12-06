.PHONY: help test integration-test test-all clean
.PHONY: test-typescript test-go test-python test-ruby test-rust test-csharp test-javascript
.PHONY: integration-test-typescript integration-test-go integration-test-python integration-test-ruby integration-test-rust integration-test-csharp integration-test-javascript

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
	@echo "  $(YELLOW)test-javascript$(RESET)           Run JavaScript SDK unit tests"
	@echo ""
	@echo "$(BOLD)Individual SDK Integration Tests:$(RESET)"
	@echo "  $(YELLOW)integration-test-typescript$(RESET)  Run TypeScript SDK integration tests"
	@echo "  $(YELLOW)integration-test-go$(RESET)          Run Go SDK integration tests"
	@echo "  $(YELLOW)integration-test-python$(RESET)      Run Python SDK integration tests"
	@echo "  $(YELLOW)integration-test-ruby$(RESET)        Run Ruby SDK integration tests"
	@echo "  $(YELLOW)integration-test-rust$(RESET)        Run Rust SDK integration tests"
	@echo "  $(YELLOW)integration-test-csharp$(RESET)      Run C# SDK integration tests"
	@echo "  $(YELLOW)integration-test-javascript$(RESET)  Run JavaScript SDK integration tests"
	@echo ""
	@echo "$(BOLD)Utilities:$(RESET)"
	@echo "  $(YELLOW)clean$(RESET)                     Clean all SDK build artifacts"

# ==============================================================================
# Aggregate Targets
# ==============================================================================

test: ## Run ALL SDK unit tests
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running ALL SDK Unit Tests$(RESET)"
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo ""
	@FAILED=""; \
	echo "$(CYAN)[1/7] TypeScript SDK...$(RESET)"; \
	$(MAKE) -s test-typescript && echo "$(GREEN)✓ TypeScript tests passed$(RESET)" || { echo "$(RED)✗ TypeScript tests failed$(RESET)"; FAILED="$$FAILED typescript"; }; \
	echo ""; \
	echo "$(CYAN)[2/7] Go SDK...$(RESET)"; \
	$(MAKE) -s test-go && echo "$(GREEN)✓ Go tests passed$(RESET)" || { echo "$(RED)✗ Go tests failed$(RESET)"; FAILED="$$FAILED go"; }; \
	echo ""; \
	echo "$(CYAN)[3/7] Python SDK...$(RESET)"; \
	$(MAKE) -s test-python && echo "$(GREEN)✓ Python tests passed$(RESET)" || { echo "$(RED)✗ Python tests failed$(RESET)"; FAILED="$$FAILED python"; }; \
	echo ""; \
	echo "$(CYAN)[4/7] Ruby SDK...$(RESET)"; \
	$(MAKE) -s test-ruby && echo "$(GREEN)✓ Ruby tests passed$(RESET)" || { echo "$(RED)✗ Ruby tests failed$(RESET)"; FAILED="$$FAILED ruby"; }; \
	echo ""; \
	echo "$(CYAN)[5/7] Rust SDK...$(RESET)"; \
	$(MAKE) -s test-rust && echo "$(GREEN)✓ Rust tests passed$(RESET)" || { echo "$(RED)✗ Rust tests failed$(RESET)"; FAILED="$$FAILED rust"; }; \
	echo ""; \
	echo "$(CYAN)[6/7] C# SDK...$(RESET)"; \
	$(MAKE) -s test-csharp && echo "$(GREEN)✓ C# tests passed$(RESET)" || { echo "$(RED)✗ C# tests failed$(RESET)"; FAILED="$$FAILED csharp"; }; \
	echo ""; \
	echo "$(CYAN)[7/7] JavaScript SDK...$(RESET)"; \
	$(MAKE) -s test-javascript && echo "$(GREEN)✓ JavaScript tests passed$(RESET)" || { echo "$(RED)✗ JavaScript tests failed$(RESET)"; FAILED="$$FAILED javascript"; }; \
	echo ""; \
	echo "$(CYAN)$(BOLD)========================================$(RESET)"; \
	if [ -z "$$FAILED" ]; then \
		echo "$(GREEN)$(BOLD)  ✓ ALL SDK Unit Tests Passed!$(RESET)"; \
	else \
		echo "$(RED)$(BOLD)  ✗ Failed SDKs:$$FAILED$(RESET)"; \
		exit 1; \
	fi; \
	echo "$(CYAN)$(BOLD)========================================$(RESET)"

integration-test: ## Run ALL SDK integration tests
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo "$(CYAN)$(BOLD)  Running ALL SDK Integration Tests$(RESET)"
	@echo "$(CYAN)$(BOLD)========================================$(RESET)"
	@echo ""
	@FAILED=""; \
	echo "$(CYAN)[1/7] TypeScript SDK...$(RESET)"; \
	$(MAKE) -s integration-test-typescript && echo "$(GREEN)✓ TypeScript integration tests passed$(RESET)" || { echo "$(RED)✗ TypeScript integration tests failed$(RESET)"; FAILED="$$FAILED typescript"; }; \
	echo ""; \
	echo "$(CYAN)[2/7] Go SDK...$(RESET)"; \
	$(MAKE) -s integration-test-go && echo "$(GREEN)✓ Go integration tests passed$(RESET)" || { echo "$(RED)✗ Go integration tests failed$(RESET)"; FAILED="$$FAILED go"; }; \
	echo ""; \
	echo "$(CYAN)[3/7] Python SDK...$(RESET)"; \
	$(MAKE) -s integration-test-python && echo "$(GREEN)✓ Python integration tests passed$(RESET)" || { echo "$(RED)✗ Python integration tests failed$(RESET)"; FAILED="$$FAILED python"; }; \
	echo ""; \
	echo "$(CYAN)[4/7] Ruby SDK...$(RESET)"; \
	$(MAKE) -s integration-test-ruby && echo "$(GREEN)✓ Ruby integration tests passed$(RESET)" || { echo "$(RED)✗ Ruby integration tests failed$(RESET)"; FAILED="$$FAILED ruby"; }; \
	echo ""; \
	echo "$(CYAN)[5/7] Rust SDK...$(RESET)"; \
	$(MAKE) -s integration-test-rust && echo "$(GREEN)✓ Rust integration tests passed$(RESET)" || { echo "$(RED)✗ Rust integration tests failed$(RESET)"; FAILED="$$FAILED rust"; }; \
	echo ""; \
	echo "$(CYAN)[6/7] C# SDK...$(RESET)"; \
	$(MAKE) -s integration-test-csharp && echo "$(GREEN)✓ C# integration tests passed$(RESET)" || { echo "$(RED)✗ C# integration tests failed$(RESET)"; FAILED="$$FAILED csharp"; }; \
	echo ""; \
	echo "$(CYAN)[7/7] JavaScript SDK...$(RESET)"; \
	$(MAKE) -s integration-test-javascript && echo "$(GREEN)✓ JavaScript integration tests passed$(RESET)" || { echo "$(RED)✗ JavaScript integration tests failed$(RESET)"; FAILED="$$FAILED javascript"; }; \
	echo ""; \
	echo "$(CYAN)$(BOLD)========================================$(RESET)"; \
	if [ -z "$$FAILED" ]; then \
		echo "$(GREEN)$(BOLD)  ✓ ALL SDK Integration Tests Passed!$(RESET)"; \
	else \
		echo "$(RED)$(BOLD)  ✗ Failed SDKs:$$FAILED$(RESET)"; \
		exit 1; \
	fi; \
	echo "$(CYAN)$(BOLD)========================================$(RESET)"

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

test-javascript: ## Run JavaScript SDK unit tests
	@cd javascript && $(MAKE) test

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

integration-test-javascript: ## Run JavaScript SDK integration tests
	@cd javascript && $(MAKE) integration-test

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
	@cd javascript && $(MAKE) clean 2>/dev/null || true
	@echo "$(GREEN)✓ All SDK artifacts cleaned$(RESET)"
