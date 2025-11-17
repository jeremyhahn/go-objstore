# gRPC Server Configuration

Configuration reference for the gRPC server.

## Basic Configuration

```yaml
grpc:
  enabled: true
  address: "0.0.0.0:9090"
  max_concurrent_streams: 100
  max_receive_message_size: 4194304  # 4MB
  max_send_message_size: 4194304     # 4MB
```

## Parameters

### Network
- `address` - Listen address and port (default: "0.0.0.0:9090")
- `max_concurrent_streams` - Maximum concurrent gRPC streams (default: 100)

### Message Limits
- `max_receive_message_size` - Maximum incoming message size in bytes (default: 4MB)
- `max_send_message_size` - Maximum outgoing message size in bytes (default: 4MB)

### Timeouts
- `connection_timeout` - Connection establishment timeout (default: 10s)
- `keepalive_time` - Keepalive ping interval (default: 2h)
- `keepalive_timeout` - Keepalive ping timeout (default: 20s)

## TLS Configuration

### Server TLS
```yaml
grpc:
  tls:
    enabled: true
    cert_file: /path/to/server.crt
    key_file: /path/to/server.key
```

### Mutual TLS (mTLS)
```yaml
grpc:
  tls:
    enabled: true
    cert_file: /path/to/server.crt
    key_file: /path/to/server.key
    client_ca_file: /path/to/client-ca.crt
    client_auth_type: require_and_verify  # require_and_verify, verify_if_given, no_client_cert
```

## Authentication

### No Authentication
```yaml
grpc:
  auth:
    enabled: false
```

### Bearer Token
```yaml
grpc:
  auth:
    enabled: true
    type: bearer
    bearer_token: "secret-token-here"
```

### Custom Authentication
Configure through adapter interface. Adapter can implement any authentication scheme.

## Interceptors

### Logging
```yaml
grpc:
  logging:
    enabled: true
    log_payload: false  # Log request/response bodies
    log_metadata: true  # Log gRPC metadata
```

### Metrics
```yaml
grpc:
  metrics:
    enabled: true
    endpoint: "/metrics"
```

### Rate Limiting
```yaml
grpc:
  rate_limit:
    enabled: true
    requests_per_second: 100
    burst: 200
```

## Health Checks

```yaml
grpc:
  health_check:
    enabled: true
    interval: 30s
```

Health check service exposed at grpc.health.v1.Health.

## Reflection

```yaml
grpc:
  reflection:
    enabled: true  # Enable gRPC Server Reflection
```

Enables service discovery and tools like grpcurl.

## Complete Example

```yaml
grpc:
  enabled: true
  address: "0.0.0.0:9090"
  max_concurrent_streams: 100
  max_receive_message_size: 10485760  # 10MB
  max_send_message_size: 10485760     # 10MB
  connection_timeout: 10s
  keepalive_time: 2h
  keepalive_timeout: 20s
  
  tls:
    enabled: true
    cert_file: /etc/objstore/tls/server.crt
    key_file: /etc/objstore/tls/server.key
    client_ca_file: /etc/objstore/tls/ca.crt
    client_auth_type: require_and_verify
  
  auth:
    enabled: true
    type: bearer
    bearer_token: ${GRPC_AUTH_TOKEN}
  
  logging:
    enabled: true
    log_payload: false
    log_metadata: true
  
  metrics:
    enabled: true
  
  rate_limit:
    enabled: true
    requests_per_second: 1000
    burst: 2000
  
  health_check:
    enabled: true
    interval: 30s
  
  reflection:
    enabled: true
```

## Environment Variable Overrides

- `GRPC_ADDRESS` - Override listen address
- `GRPC_TLS_ENABLED` - Enable/disable TLS
- `GRPC_TLS_CERT` - Server certificate path
- `GRPC_TLS_KEY` - Server key path
- `GRPC_AUTH_TOKEN` - Bearer token
