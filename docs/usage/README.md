# Usage Guide

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Practical guides for using go-objstore components in your applications.

## Feature documents

[Architecture](architecture.md) · [Engineering](engineering.md) · [CLI](cli.md) · [API](api.md) · [User guide](user.md)

## Getting Started

New to go-objstore? Start here:

[Getting Started Guide](getting-started.md)

## Core Functionality

### Storage Backends
Learn how to use different storage backends for your application.

[Using Storage Backends](storage-backends.md)

### StorageFS
Use object storage with familiar filesystem operations.

[Using StorageFS](../storagefs/README.md)

### Encryption
Enable at-rest encryption for stored data.

[Using Encryption](../configuration/encryption.md)

### Lifecycle Policies
Automate data retention and archival.

[Using Lifecycle Policies](../lifecycle/README.md)

## Servers

### gRPC Server
Run a gRPC server to expose storage over RPC.

[Configuring the gRPC Server](../configuration/grpc-server.md)

### REST API Server
Run a REST API server for HTTP/JSON access.

[Configuring the REST Server](../configuration/rest-server.md)

### QUIC Server
Run a QUIC/HTTP3 server for improved performance.

[Configuring the QUIC Server](../configuration/quic-server.md)

### MCP Server
Run an MCP server for AI model integrations.

[Configuring the MCP Server](../configuration/mcp-server.md)

## Tools

### CLI Tool
Use the command-line interface for storage operations.

[Using the CLI](cli.md)

### C API
Embed go-objstore in C/C++ applications.

[Using the C API](../c_client/README.md)

## Deployment

Production deployment considerations and best practices.

[Deployment Guide](deployment.md)

## Examples

Detailed working examples are available in the [`examples/`](../../examples/) directory of the repository. Examples demonstrate common patterns and use cases with full source code.

## Support

For questions and issues:
- Check the relevant usage guide above
- Review the configuration documentation
- See the architecture documentation for design details
- Check the examples directory for working code
- Open an issue on GitHub
