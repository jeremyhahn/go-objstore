# go-objstore Multi-Language SDK Suite

This directory contains official client SDKs for go-objstore in multiple programming languages.

## Available SDKs

| Language | Directory | Status | Coverage | Tests |
|----------|-----------|--------|----------|-------|
| **Python** | [`python/`](python/) | ✅ Ready | 92%+ | 75 tests |
| **Ruby** | [`ruby/`](ruby/) | ✅ Ready | 92-95% | 71 tests |
| **Go** | [`go/`](go/) | ✅ Ready | Comprehensive | 26+ tests |
| **Rust** | [`rust/`](rust/) | ✅ Ready | 90-92% | 32 tests |
| **JavaScript** | [`javascript/`](javascript/) | ✅ Ready | 94%+ | 142 tests |
| **TypeScript** | [`typescript/`](typescript/) | ✅ Ready | 94.73% | 116 tests |
| **C#** | [`csharp/`](csharp/) | ✅ Ready | 90-95% | 38 tests |

## Protocol Support

All SDKs support three protocols for accessing go-objstore:

- **REST** - HTTP/HTTPS RESTful API
- **gRPC** - Binary protocol with streaming support
- **QUIC/HTTP3** - Low-latency protocol over UDP

## Supported Operations

All SDKs implement the complete go-objstore API:

### Core Operations
- Put, Get, Delete, List, Exists
- GetMetadata, UpdateMetadata
- Health checks

### Advanced Operations
- Archive to cold storage
- Lifecycle policy management
- Replication policy management

## Quick Start

### Python
```bash
cd python
pip install -e ".[dev]"
make test
```

### Ruby
```bash
cd ruby
bundle install
make test
```

### Go
```bash
cd go
go mod download
make test
```

### Rust
```bash
cd rust
cargo build
cargo test
```

### JavaScript
```bash
cd javascript
npm install
make test
```

### TypeScript
```bash
cd typescript
npm install
make test
```

### C#
```bash
cd csharp
./build.sh test
```

## Common Makefile Targets

All SDKs provide consistent Makefile targets:

- `make build` - Build the SDK
- `make test` - Run unit tests
- `make integration-test` - Run Docker integration tests
- `make coverage` - Generate coverage report
- `make clean` - Remove build artifacts

## Documentation

Each SDK includes:
- **README.md** - Installation and usage guide
- **Examples** - Working code examples
- **API Reference** - Complete API documentation
- **Tests** - Unit and integration test suites

See each SDK's directory for detailed documentation.

## API Definitions

All SDKs are generated from the official API definitions:

- **gRPC:** [`../proto/objstore.proto`](../proto/objstore.proto)
- **REST:** [`../openapi/objstore.yaml`](../openapi/objstore.yaml)

## Testing

### Unit Tests
Each SDK includes comprehensive unit tests with mocking:
- 90%+ code coverage target
- Fast execution without external dependencies
- Continuous integration ready

### Integration Tests
Each SDK includes Docker-based integration tests:
- Tests against real go-objstore server
- Tests all operations across all protocols
- Automatic setup and teardown
- Requires Docker and docker-compose

## Requirements

### Common Requirements
- Docker and docker-compose (for integration tests)
- go-objstore server (for integration tests)

### Language-Specific Requirements
- **Python:** Python 3.8+, pip
- **Ruby:** Ruby 2.7+, bundler
- **Go:** Go 1.23+
- **Rust:** Rust 1.70+, cargo
- **JavaScript:** Node.js 18+, npm
- **TypeScript:** Node.js 18+, npm
- **C#:** .NET 6+ or Docker

## Project Layout

This directory follows the [golang-standards/project-layout](https://github.com/golang-standards/project-layout) recommendation for API client libraries.

```
api/sdks/
├── python/          # Python SDK
├── ruby/            # Ruby SDK  
├── go/              # Go SDK
├── rust/            # Rust SDK
├── javascript/      # JavaScript SDK
├── typescript/      # TypeScript SDK
├── csharp/          # C# SDK
└── README.md        # This file
```

## Contributing

Each SDK follows its language's best practices and conventions:
- **Python:** PEP 8, type hints, Poetry
- **Ruby:** RuboCop, RSpec, Bundler
- **Go:** gofmt, Go modules
- **Rust:** rustfmt, Cargo
- **JavaScript:** ESLint, Prettier, Jest
- **TypeScript:** TSLint, Prettier, Jest
- **C#:** .NET conventions, xUnit

## License

All SDKs are licensed under the same dual-license model as go-objstore:
- **AGPL-3.0** for open-source use
- **Commercial License** available from Automate The Things, LLC

See the main project [LICENSE](../../LICENSE) for details.

## Support

For issues, questions, or contributions:
- **Issues:** https://github.com/jeremyhahn/go-objstore/issues
- **Documentation:** https://github.com/jeremyhahn/go-objstore/docs
- **Discussions:** https://github.com/jeremyhahn/go-objstore/discussions

## Verification Report

See [SDK_VERIFICATION_REPORT.md](SDK_VERIFICATION_REPORT.md) for detailed verification of all SDKs.
