# Installation Guide

## Prerequisites

### 1. Install Rust

```bash
# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Source the environment
source $HOME/.cargo/env

# Verify installation
cargo --version
rustc --version
```

### 2. Install Protocol Buffers Compiler

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler
```

#### macOS
```bash
brew install protobuf
```

#### Verify installation
```bash
protoc --version
```

### 3. Install Development Tools (Optional)

```bash
# Code coverage tools
cargo install cargo-llvm-cov
cargo install cargo-tarpaulin

# Additional components
rustup component add rustfmt clippy
```

## Building the SDK

### First Build

The first build will:
1. Download and compile dependencies
2. Generate protobuf code from `.proto` files
3. Compile the SDK

```bash
cd /home/jhahn/sources/go-objstore/api/sdks/rust

# Build in debug mode
cargo build

# Build in release mode (optimized)
cargo build --release
```

### Build Output

- Debug binaries: `target/debug/`
- Release binaries: `target/release/`
- Generated protobuf code: `src/proto/`

## Running Tests

### Unit Tests

```bash
# Run all unit tests
cargo test --lib

# Run with output
cargo test --lib -- --nocapture

# Run specific test
cargo test test_error_display
```

### Integration Tests

Integration tests require a running go-objstore server.

#### Option 1: Using Docker Script (Recommended)

```bash
# Ensure go-objstore is built
cd /home/jhahn/sources/go-objstore
make build

# Run Docker integration tests
cd /home/jhahn/sources/go-objstore/api/sdks/rust
./scripts/docker-test.sh
```

#### Option 2: Manual Server

```bash
# Terminal 1: Start server
cd /home/jhahn/sources/go-objstore
./go-objstore serve

# Terminal 2: Run tests
cd /home/jhahn/sources/go-objstore/api/sdks/rust
cargo test --test integration_test -- --ignored
```

### Code Coverage

```bash
# Using llvm-cov (recommended)
cargo install cargo-llvm-cov
cargo llvm-cov --lib --bins --html --open

# Using tarpaulin
cargo install cargo-tarpaulin
cargo tarpaulin --out Html --output-dir coverage
open coverage/index.html
```

## Running Examples

```bash
# Ensure server is running first
cd /home/jhahn/sources/go-objstore
./go-objstore serve

# In another terminal
cd /home/jhahn/sources/go-objstore/api/sdks/rust

# Run REST example
cargo run --example rest_client

# Run gRPC example
cargo run --example grpc_client

# Run QUIC example (requires HTTP3 enabled)
cargo run --example quic_client

# Run unified example
cargo run --example unified_client
```

## Using the Makefile

The Makefile provides convenient shortcuts:

```bash
# Build
make build

# Run tests
make test

# Run integration tests
make integration-test

# Run Docker integration tests
make docker-test

# Generate coverage
make coverage

# Format code
make fmt

# Run clippy
make clippy

# Run all checks
make ci

# Clean
make clean

# Help
make help
```

## Troubleshooting

### protoc not found

**Error:** `protoc failed: program not found`

**Solution:**
```bash
# Ubuntu/Debian
sudo apt-get install -y protobuf-compiler

# macOS
brew install protobuf
```

### Build fails with linking errors

**Error:** Various linking errors

**Solution:**
```bash
# Clean and rebuild
cargo clean
cargo build
```

### Tests fail to connect to server

**Error:** Connection refused or timeout

**Solution:**
1. Ensure go-objstore server is running
2. Check server is listening on correct ports:
   - REST: 8080
   - gRPC: 50051
   - QUIC: 4433

```bash
# Check if ports are listening
netstat -tlnp | grep -E '8080|50051|4433'

# Or use lsof
lsof -i :8080
lsof -i :50051
lsof -i :4433
```

### QUIC tests fail

**Error:** QUIC connection errors

**Solution:**
QUIC/HTTP3 requires special server configuration. The server must:
1. Have HTTP3 enabled in config
2. Have valid TLS certificates
3. Listen on UDP port (not TCP)

This is expected if HTTP3 is not configured on the server.

## IDE Setup

### Visual Studio Code

Install extensions:
- `rust-analyzer`: Rust language server
- `Better TOML`: TOML syntax highlighting
- `CodeLLDB`: Debugging support

### IntelliJ IDEA / CLion

Install the Rust plugin from JetBrains Marketplace.

### Vim/Neovim

Install `rust.vim` or use `coc-rust-analyzer` with CoC.

## Next Steps

1. Read the [README.md](README.md) for usage examples
2. Explore the [examples/](examples/) directory
3. Check out the API documentation:
   ```bash
   cargo doc --open
   ```
4. Run the test suite to verify everything works
5. Start building your application!

## Getting Help

- Check existing tests for usage examples
- Read inline documentation with `cargo doc`
- Review example files in `examples/`
- Check the main go-objstore documentation
