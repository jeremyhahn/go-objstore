# REST API Server Configuration

Configuration reference for the REST API server.

## Basic Configuration

```yaml
rest:
  enabled: true
  address: "0.0.0.0:8080"
  read_timeout: 30s
  write_timeout: 30s
  idle_timeout: 120s
```

## Parameters

### Network
- `address` - Listen address and port (default: "0.0.0.0:8080")
- `base_path` - API base path (default: "/api/v1")

### Timeouts
- `read_timeout` - Maximum duration for reading request (default: 30s)
- `write_timeout` - Maximum duration for writing response (default: 30s)
- `idle_timeout` - Maximum idle connection time (default: 120s)
- `shutdown_timeout` - Graceful shutdown timeout (default: 30s)

### Limits
- `max_body_size` - Maximum request body size in bytes (default: 10MB)
- `max_header_size` - Maximum header size in bytes (default: 1MB)

## TLS Configuration

```yaml
rest:
  tls:
    enabled: true
    cert_file: /path/to/server.crt
    key_file: /path/to/server.key
    min_version: "1.2"  # 1.0, 1.1, 1.2, 1.3
    cipher_suites:
      - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
      - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
```

## Authentication

```yaml
rest:
  auth:
    enabled: true
    type: bearer  # bearer, api_key, basic
    bearer_token: ${API_TOKEN}
```

### API Key Authentication
```yaml
rest:
  auth:
    enabled: true
    type: api_key
    api_key_header: X-API-Key
    api_keys:
      - ${API_KEY_1}
      - ${API_KEY_2}
```

## CORS Configuration

```yaml
rest:
  cors:
    enabled: true
    allowed_origins:
      - "https://example.com"
      - "https://app.example.com"
    allowed_methods:
      - GET
      - POST
      - PUT
      - DELETE
    allowed_headers:
      - Content-Type
      - Authorization
    exposed_headers:
      - X-Request-ID
    max_age: 3600
    allow_credentials: true
```

## Compression

```yaml
rest:
  compression:
    enabled: true
    level: 5  # 1-9, higher = more compression
    min_size: 1024  # Minimum response size to compress (bytes)
    types:
      - application/json
      - text/plain
```

## Logging

```yaml
rest:
  logging:
    enabled: true
    format: json  # json, text
    log_requests: true
    log_responses: false
    log_headers: false
```

## Rate Limiting

```yaml
rest:
  rate_limit:
    enabled: true
    requests_per_minute: 100
    burst: 50
    key_function: ip  # ip, header, custom
```

## Health and Metrics

```yaml
rest:
  health_check:
    enabled: true
    path: /health
    
  metrics:
    enabled: true
    path: /metrics
```

## Complete Example

```yaml
rest:
  enabled: true
  address: "0.0.0.0:8080"
  base_path: "/api/v1"
  read_timeout: 30s
  write_timeout: 30s
  idle_timeout: 120s
  max_body_size: 10485760  # 10MB
  
  tls:
    enabled: true
    cert_file: /etc/objstore/tls/server.crt
    key_file: /etc/objstore/tls/server.key
    min_version: "1.2"
  
  auth:
    enabled: true
    type: bearer
    bearer_token: ${API_TOKEN}
  
  cors:
    enabled: true
    allowed_origins:
      - "*"
    allowed_methods:
      - GET
      - POST
      - PUT
      - DELETE
      - HEAD
    allowed_headers:
      - "*"
    max_age: 3600
  
  compression:
    enabled: true
    level: 6
    min_size: 1024
  
  logging:
    enabled: true
    format: json
    log_requests: true
  
  rate_limit:
    enabled: true
    requests_per_minute: 1000
    burst: 100
  
  health_check:
    enabled: true
    path: /health
  
  metrics:
    enabled: true
    path: /metrics
```

## API Endpoints

### Objects
- `GET /objects` - List objects
- `GET /objects/{key}` - Get object
- `PUT /objects/{key}` - Put object
- `DELETE /objects/{key}` - Delete object
- `HEAD /objects/{key}` - Check existence
- `GET /objects/{key}/metadata` - Get metadata

### Query Parameters
- `prefix` - Filter by prefix
- `delimiter` - Hierarchical delimiter
- `limit` - Maximum results
- `marker` - Pagination marker

## Environment Variable Overrides

- `REST_ADDRESS` - Listen address
- `REST_TLS_ENABLED` - Enable TLS
- `REST_TLS_CERT` - Certificate path
- `REST_TLS_KEY` - Key path
- `API_TOKEN` - Authentication token
