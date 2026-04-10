# Codebase Structure

```
go-objstore/
├── pkg/                    # Core library
│   ├── common/             # Shared types and interfaces
│   ├── factory/            # Backend factory (runtime selection)
│   ├── local/              # Local disk backend
│   ├── s3/                 # AWS S3 backend
│   ├── minio/              # MinIO backend
│   ├── gcs/                # Google Cloud Storage backend
│   ├── azure/              # Azure Blob backend
│   ├── azurearchive/       # Azure Archive backend
│   ├── glacier/            # AWS Glacier backend
│   ├── storagefs/          # Filesystem abstraction
│   ├── adapters/           # Pluggable adapters (logging, auth)
│   ├── encryption/         # Encryption support
│   ├── cli/                # CLI implementation
│   └── server/{grpc,rest,quic,mcp}/ # Server implementations
├── cmd/
│   ├── objstore/           # CLI entry point
│   ├── objstore-server/    # All-in-one server
│   ├── objstore-{grpc,rest,quic,mcp}-server/ # Individual servers
│   └── objstorelib/        # C shared library
├── api/
│   ├── proto/              # Protobuf definitions
│   └── sdks/               # SDKs (7 languages)
├── test/integration/       # Docker Compose integration tests
│   ├── local/ s3/ minio/ azure/ gcs/ factory/ replication/ cli/ server/
├── bin/                    # Build output
├── coverage/               # Coverage output
├── scripts/                # Helper scripts
├── tools/                  # Internal tools (mergecov)
└── Makefile
```
