# Configuration Overview

This section covers configuration for all go-objstore components. Configuration can be provided through YAML files, environment variables, or programmatic APIs.

## Configuration Methods

### Configuration Files
YAML files provide structured configuration for all components. The CLI and servers load configuration from specified file paths.

### Environment Variables
Environment variables override file configuration. Useful for containerized deployments and CI/CD pipelines.

### Programmatic Configuration
Applications can configure components directly through Go APIs. Provides maximum flexibility for dynamic configuration.

## Configuration Hierarchy

Configuration follows a precedence order:
1. Programmatic API calls (highest precedence)
2. Environment variables
3. Configuration files
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

Configuration is validated on load:
- Required fields checked
- Types validated
- Cross-field validation (e.g., TLS requires cert and key)
- Backend-specific validation

Validation errors reported with specific field paths and helpful messages.

## Configuration Examples

Complete configuration examples available in repository:
- Single backend configurations
- Multi-backend configurations
- Server configurations with TLS
- Encryption with different key backends
- Complex lifecycle policies

## Secrets Management

### Sensitive Values
Sensitive configuration values like passwords and API keys should not be committed to version control.

### Environment Variable Substitution
Configuration files can reference environment variables:
```yaml
password: ${DB_PASSWORD}
```

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
Some components support configuration reload without restart:
- TLS certificate rotation
- Logging level changes
- Backend connection pool sizing

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

### Validation Errors
Configuration validation errors include:
- Field path indicating problematic configuration
- Error description
- Suggested fixes where applicable

### Testing Configuration
Test configuration without starting services:
- Use validation commands in CLI
- Test backend connectivity separately
- Verify TLS certificates
- Check credential validity

### Configuration Debugging
Enable debug logging to see:
- Configuration loading process
- Effective configuration after merging
- Backend initialization steps
- Connection establishment
