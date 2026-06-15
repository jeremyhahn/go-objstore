# Configuration Overview

This section covers configuration for all go-objstore components. Configuration is provided through command-line flags (servers), an optional YAML file plus flags (CLI), or programmatic Go APIs (library use).

## Configuration Methods

### Command-Line Flags
The server binaries (`objstore-server`, `objstore-grpc-server`, `objstore-rest-server`, `objstore-quic-server`, `objstore-mcp-server`) are configured entirely with command-line flags. They do not load configuration files.

### CLI Configuration File
The `objstore` CLI optionally loads a YAML config file (`.objstore.yaml`) and supports `OBJECTSTORE_*` environment variable overrides. See [CLI Configuration](cli.md).

### Programmatic Configuration
Applications embedding go-objstore configure components directly through Go APIs (`ServerConfig` structs and functional options in `pkg/server/*`). This provides the full set of options — TLS, auth adapters, rate limiting, timeouts — most of which are not exposed as binary flags.

## Configuration Hierarchy

For the CLI, configuration follows a precedence order:
1. Command-line flags (highest precedence)
2. Environment variables (`OBJECTSTORE_*`)
3. Configuration file
4. Built-in defaults (lowest precedence)

## Component Configuration

### Storage Backends
Configure backend type, credentials, and connection parameters.

[Storage Backend Configuration](storage-backends.md)

### Servers
Configure protocol-specific servers including ports, TLS, and authentication.

[gRPC Server Configuration](grpc-server.md)
[REST Server Configuration](rest-server.md)
[QUIC Server Configuration](quic-server.md)
[MCP Server Configuration](mcp-server.md)

### Encryption
Configure encryption keys, key backends, and encryption algorithms.

[Encryption Configuration](encryption.md)

### Lifecycle Policies
Configure automatic data retention and archival policies.

[Lifecycle Configuration](lifecycle.md)

### CLI Tool
Configure CLI defaults, output formats, and backend connections.

[CLI Configuration](cli.md)

## Common Configuration Patterns

### Credentials
Credentials can be provided through:
- Configuration file
- Environment variables
- Cloud provider credential chains
- Credential files in standard locations

### TLS Configuration
TLS certificates configured with:
- Server certificate and private key
- Client CA for mTLS
- Cipher suites and TLS versions
- Certificate paths or embedded PEM

### Logging
Logging configuration includes:
- Log level (debug, info, warn, error)
- Output format (JSON, text)
- Output destination (stdout, stderr, file)
- Custom logger adapters

### Connection Pooling
Backend connection pooling configured with:
- Maximum concurrent connections
- Connection timeout
- Idle connection timeout
- Connection retry policy

## Configuration Validation

Configuration is validated at startup:
- Flag values are type-checked by the flag parser
- Cross-field validation (e.g., QUIC requires both cert and key, or `-selfsigned`)
- Backend-specific validation when the backend initializes

Servers exit with a descriptive error message on invalid configuration.

## Configuration Examples

Working examples are available in the repository under `examples/` (basic usage, facade usage, encryption, lifecycle policies, gRPC client, and more).

## Secrets Management

### Sensitive Values
Sensitive configuration values like passwords and API keys should not be committed to version control. Prefer environment variables and cloud credential chains over flags or config files for secrets.

### External Secret Stores
Production deployments should use external secret management:
- AWS Secrets Manager
- Google Cloud Secret Manager
- Azure Key Vault
- HashiCorp Vault

### Encryption at Rest
Configuration files containing secrets should be encrypted at rest using system encryption or secret management tools.

## Dynamic Configuration

### Configuration Reloading
Servers read their configuration once at startup; changing flags requires a restart. Lifecycle and replication policies, however, are managed at runtime through the API and persist across restarts.

### Service Discovery
Server addresses and backend endpoints can integrate with service discovery:
- Consul
- etcd
- Kubernetes service discovery
- DNS-based discovery

## Deployment Considerations

### Container Deployments
Configuration in containers:
- Mount configuration as volume
- Use environment variables for secrets
- Override specific values through env vars
- Use init containers for secret injection

### Kubernetes
Kubernetes-specific patterns:
- ConfigMaps for non-sensitive configuration
- Secrets for sensitive values
- Environment variables from ConfigMaps and Secrets
- Volume mounts for large configuration

### Cloud Environments
Cloud platform integration:
- IAM roles for authentication
- Parameter stores for configuration
- Service-specific configuration (S3 bucket policies, etc.)
- Regional configuration for multi-region deployments

## Troubleshooting Configuration

### Testing Configuration
Test configuration before relying on a deployment:
- Use `objstore health` to verify backend connectivity
- Verify TLS certificates separately (e.g., with openssl)
- Check credential validity with the cloud provider's CLI

### Configuration Debugging
Server startup logs report:
- The selected backend and storage path
- Each enabled transport and its listen address
- Replication policy file location
- Backend initialization errors
