# Server Middleware

This package provides comprehensive middleware systems for both REST (Gin) and gRPC servers in the go-objstore project.

## Features

### 1. Rate Limiting (`ratelimit.go`)

Rate limiting middleware to protect your APIs from abuse and ensure fair resource usage.

**Features:**
- Configurable requests per second and burst limits
- Per-IP or global rate limiting
- Proper HTTP 429 / gRPC ResourceExhausted error responses
- Uses `golang.org/x/time/rate` for token bucket algorithm

**Usage (REST):**
```go
import "github.com/jeremyhahn/go-objstore/pkg/server/middleware"

config := &middleware.RateLimitConfig{
    RequestsPerSecond: 100,
    Burst:             200,
    PerIP:             true, // Enable per-IP rate limiting
}

server, _ := rest.NewServer(storage, &rest.ServerConfig{
    EnableRateLimit: true,
    RateLimitConfig: config,
})
```

**Usage (gRPC):**
```go
import "github.com/jeremyhahn/go-objstore/pkg/server/middleware"

config := &middleware.RateLimitConfig{
    RequestsPerSecond: 100,
    Burst:             200,
    PerIP:             false, // Global rate limit for gRPC
}

server, _ := grpc.NewServer(storage,
    grpc.WithRateLimit(true, config),
)
```

### 2. Security Headers (`security.go`)

Adds comprehensive security headers to HTTP responses to protect against common web vulnerabilities.

**Security Headers:**
- `X-Content-Type-Options: nosniff` - Prevents MIME type sniffing
- `X-Frame-Options: DENY` - Prevents clickjacking attacks
- `X-XSS-Protection: 1; mode=block` - Enables browser XSS protection
- `Content-Security-Policy: default-src 'self'` - Controls resource loading
- `Strict-Transport-Security` - Forces HTTPS (only when TLS is enabled)
- `Referrer-Policy` - Controls referrer information
- `Permissions-Policy` - Controls browser features

**Usage:**
```go
import "github.com/jeremyhahn/go-objstore/pkg/server/middleware"

config := &middleware.SecurityHeadersConfig{
    EnableHSTS:            true,  // Enable only when using TLS
    HSTSMaxAge:            31536000,
    HSTSIncludeSubdomains: true,
    ContentSecurityPolicy: "default-src 'self'; script-src 'self' 'unsafe-inline'",
    XFrameOptions:         "SAMEORIGIN",
}

server, _ := rest.NewServer(storage, &rest.ServerConfig{
    EnableSecurityHeaders: true,
    SecurityHeadersConfig: config,
})
```

### 3. Request ID Tracking (`requestid.go`)

Generates unique request IDs for request tracing and correlation across services.

**Features:**
- Automatic generation of unique request IDs
- X-Request-ID header passthrough support
- Context propagation for logging and tracing
- Works with both REST and gRPC

**Usage (REST):**
```go
server, _ := rest.NewServer(storage, &rest.ServerConfig{
    EnableRequestID: true, // Enabled by default
})

// In your handler, retrieve the request ID:
requestID := middleware.GetRequestIDFromGinContext(c)
```

**Usage (gRPC):**
```go
server, _ := grpc.NewServer(storage,
    grpc.WithRequestID(true), // Enabled by default
)

// In your handler, retrieve the request ID:
requestID := middleware.GetRequestIDFromContext(ctx)
```

## Middleware Execution Order

### REST (Gin)
1. Recovery (always enabled)
2. Error Handling (always enabled)
3. **Request ID** (tracks all requests)
4. **Rate Limiting** (protects against abuse)
5. **Security Headers** (adds security headers)
6. CORS (if enabled)
7. Authentication (always enabled)
8. Logging (if enabled)
9. Request Size Limit (if configured)

### gRPC
1. Recovery (always enabled)
2. **Request ID** (tracks all requests)
3. **Rate Limiting** (protects against abuse)
4. Authentication (always enabled)
5. Logging (if enabled)
6. Metrics (if enabled)
7. Custom interceptors

## Configuration Examples

### Complete REST Server Configuration
```go
server, err := rest.NewServer(storage, &rest.ServerConfig{
    Host:                  "0.0.0.0",
    Port:                  8080,
    EnableCORS:            true,
    EnableLogging:         true,
    EnableRequestID:       true,
    EnableRateLimit:       true,
    RateLimitConfig: &middleware.RateLimitConfig{
        RequestsPerSecond: 100,
        Burst:             200,
        PerIP:             true,
    },
    EnableSecurityHeaders: true,
    SecurityHeadersConfig: &middleware.SecurityHeadersConfig{
        EnableHSTS:            true,
        HSTSMaxAge:            31536000,
        ContentSecurityPolicy: "default-src 'self'",
        XFrameOptions:         "DENY",
    },
    MaxRequestSize: 100 * 1024 * 1024, // 100MB
    Logger:         adapters.NewDefaultLogger(),
    Authenticator:  adapters.NewNoOpAuthenticator(),
})
```

### Complete gRPC Server Configuration
```go
server, err := grpc.NewServer(storage,
    grpc.WithAddress(":50051"),
    grpc.WithRequestID(true),
    grpc.WithRateLimit(true, &middleware.RateLimitConfig{
        RequestsPerSecond: 100,
        Burst:             200,
    }),
    grpc.WithLogging(true),
    grpc.WithMetrics(true),
)
```

## Testing

All middleware includes comprehensive unit tests with high coverage:

```bash
go test ./pkg/server/middleware/... -v -cover
```

Current test coverage: **96.2%**

## Default Configurations

### Rate Limiting Defaults
- Requests per second: 100
- Burst: 200
- Per-IP: false (global rate limit)

### Security Headers Defaults
- HSTS: Disabled (enable only with TLS)
- HSTS Max Age: 31536000 (1 year)
- CSP: "default-src 'self'"
- X-Frame-Options: "DENY"
- X-Content-Type-Options: "nosniff"
- X-XSS-Protection: "1; mode=block"
- Referrer-Policy: "strict-origin-when-cross-origin"

### Request ID
- Enabled by default
- Uses cryptographically secure random generation
- 32-character hexadecimal ID format

## Performance Considerations

- Rate limiting uses in-memory token buckets (efficient and fast)
- Per-IP rate limiting creates limiters on-demand with concurrent access protection
- Request ID generation uses crypto/rand for security
- Security headers are static and add minimal overhead
- All middleware is designed for high-performance production use

## Thread Safety

All middleware implementations are thread-safe:
- Rate limiting uses sync.RWMutex for concurrent access to per-IP limiters
- Request ID generation uses crypto/rand which is thread-safe
- Security headers are stateless

## Error Handling

### Rate Limiting
- REST: Returns HTTP 429 (Too Many Requests) with Retry-After header
- gRPC: Returns ResourceExhausted status code

### Request ID
- Gracefully handles missing request IDs
- Supports passthrough of existing X-Request-ID headers
- Works with both incoming and outgoing contexts
