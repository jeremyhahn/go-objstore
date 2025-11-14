# Makefile for go-objstore C API test client
#
# This Makefile builds the C test program that demonstrates usage of
# the go-objstore shared library.

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -Werror -std=c99 -pedantic
LDFLAGS = ../../bin/libobjstore.so -lpthread -ldl

# Paths
OBJECTSTORE_LIB = ../../bin/libobjstore.so
OBJECTSTORE_HEADER = libobjstore.h

# Source and targets
TEST_SRC = test_objstore.c
TEST_TARGET = test_objstore
EXAMPLE_SRC = simple_example.c
EXAMPLE_TARGET = simple_example

# Default target
.PHONY: all
all: check-lib $(TEST_TARGET) $(EXAMPLE_TARGET)

# Check if the objstore library exists
.PHONY: check-lib
check-lib:
	@if [ ! -f "$(OBJECTSTORE_LIB)" ]; then \
		echo "Error: libobjstore.so not found. Run 'make lib' in the project root."; \
		exit 1; \
	fi
	@if [ ! -f "$(OBJECTSTORE_HEADER)" ]; then \
		echo "Error: libobjstore.h not found. Run 'make lib' in the project root."; \
		exit 1; \
	fi

# Build the test program
$(TEST_TARGET): $(TEST_SRC)
	@echo "Building $(TEST_TARGET)..."
	$(CC) $(CFLAGS) -o $(TEST_TARGET) $(TEST_SRC) $(LDFLAGS)
	@echo "Build successful!"

# Build the simple example
$(EXAMPLE_TARGET): $(EXAMPLE_SRC)
	@echo "Building $(EXAMPLE_TARGET)..."
	$(CC) $(CFLAGS) -o $(EXAMPLE_TARGET) $(EXAMPLE_SRC) $(LDFLAGS)
	@echo "Build successful!"

# Run the test program
.PHONY: run
run: $(TEST_TARGET)
	@echo "Running $(TEST_TARGET)..."
	@echo ""
	LD_LIBRARY_PATH=../../bin:$$LD_LIBRARY_PATH ./$(TEST_TARGET)

# Run the simple example
.PHONY: example
example: $(EXAMPLE_TARGET)
	@echo "Running $(EXAMPLE_TARGET)..."
	@echo ""
	LD_LIBRARY_PATH=../../bin:$$LD_LIBRARY_PATH ./$(EXAMPLE_TARGET)

# Run with valgrind for memory leak detection
.PHONY: valgrind
valgrind: $(TEST_TARGET)
	@echo "Running $(TEST_TARGET) with valgrind..."
	@echo ""
	LD_LIBRARY_PATH=../../bin:$$LD_LIBRARY_PATH valgrind \
		--leak-check=full \
		--show-leak-kinds=all \
		--track-origins=yes \
		--verbose \
		./$(TEST_TARGET)

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -f $(TEST_TARGET) $(EXAMPLE_TARGET)
	rm -rf /tmp/objstore_test /tmp/simple_objstore
	@echo "Clean complete!"

# Help target
.PHONY: help
help:
	@echo "go-objstore C API Test Client"
	@echo ""
	@echo "Available targets:"
	@echo "  all       - Build all programs (default)"
	@echo "  run       - Build and run the test suite"
	@echo "  example   - Build and run the simple example"
	@echo "  valgrind  - Run the test suite with valgrind"
	@echo "  clean     - Remove build artifacts"
	@echo "  help      - Show this help message"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - The libobjstore.so library must be built first"
	@echo "  - Run 'make lib' in the project root directory"
	@echo ""
	@echo "Example usage:"
	@echo "  cd /path/to/go-objstore"
	@echo "  make lib"
	@echo "  cd examples/c_client"
	@echo "  make example    # Run simple demo"
	@echo "  make run        # Run full test suite"
