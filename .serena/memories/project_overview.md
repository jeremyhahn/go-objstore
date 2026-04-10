# go-objstore

Unified object storage and file system abstraction library for Go.

## Purpose
Multi-backend storage abstraction with facade pattern, replication, lifecycle policies, and multiple server interfaces. Provides CLI, servers (gRPC, REST, QUIC/HTTP3, MCP), C shared library, and SDKs in 7 languages.

## Supported Backends
- Local disk, AWS S3, MinIO, Google Cloud Storage, Azure Blob
- Archival: AWS Glacier, Azure Archive
- All backends selectable via build tags

## Tech Stack
- Language: Go 1.25.6
- Module: `github.com/jeremyhahn/go-objstore`
- Build: Makefile with build tags (local, awss3, minio, gcpstorage, azureblob, glacier, azurearchive)
- Servers: gRPC (protobuf), REST (gin), QUIC/HTTP3 (quic-go), MCP (jsonrpc2)
- CI: GitHub Actions + Docker Compose
- Linting: golangci-lint, gosec, govulncheck
- Coverage goal: >= 90%
- Dual license: AGPL-3.0 / Commercial

## Architecture
- `pkg/` - Core library packages (per-backend adapters, factory, storagefs, common, encryption, cli, servers)
- `cmd/objstore` - CLI tool
- `cmd/objstore-server` - All-in-one server
- `cmd/objstore-{grpc,rest,quic,mcp}-server` - Individual server binaries
- `cmd/objstorelib` - C shared library entry point
- `api/` - Proto definitions + SDKs (TypeScript, Go, Python, Ruby, Rust, C#, JavaScript)
- `test/integration/` - Docker Compose-based integration tests per backend
