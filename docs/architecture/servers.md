# Server Architecture

go-objstore provides multiple server implementations that expose the storage layer through different protocols. All servers share common patterns for authentication, TLS, and configuration.

## Server Types

### gRPC Server
Protocol Buffers-based RPC server for high-performance communication. Supports bidirectional streaming for efficient data transfer. Uses HTTP/2 for multiplexing and flow control.

Key characteristics:
- Strongly typed Protocol Buffer schemas
- Efficient binary serialization
- Streaming support for large objects
- Built-in load balancing and service discovery
- Excellent cross-language support

### REST API Server
HTTP/JSON API server for broad compatibility. Uses the Gin web framework for routing and middleware. Follows REST conventions with standard HTTP methods.

Key characteristics:
- Simple HTTP/JSON interface
- Widely supported by HTTP clients
- Easy debugging with standard tools
- OpenAPI specification available
- Suitable for web browsers and curl

### QUIC Server
HTTP/3 server over QUIC protocol. Provides improved performance over traditional TCP-based HTTP, especially on unreliable networks.

Key characteristics:
- Reduced connection establishment time
- Better handling of packet loss
- Connection migration across network changes
- Multiplexing without head-of-line blocking
- Encrypted by default

### MCP Server
Model Context Protocol server for AI model integrations. Exposes storage operations in a format optimized for language models and AI assistants.

Key characteristics:
- Designed for LLM interactions
- JSON-RPC 2.0 over stdio or HTTP
- Resource and tool abstraction
- Integration with AI development tools

## Common Architecture Patterns

### Handler Layer
All servers implement a thin handler layer that:
- Maps protocol-specific requests to storage interface calls
- Handles serialization and deserialization
- Manages streaming for large objects
- Returns protocol-appropriate responses and errors

### Middleware Stack
Servers use middleware for cross-cutting concerns:
- Authentication and authorization
- Request logging and tracing
- Rate limiting and throttling
- Compression
- CORS headers (REST only)
- Metrics collection

### TLS Configuration
All servers support TLS and mutual TLS (mTLS):
- Server certificates for encrypted connections
- Client certificate validation for mTLS
- Configurable cipher suites and TLS versions
- Certificate reloading without restart

### Authentication
Pluggable authentication through adapter interface:
- Bearer token authentication
- API key authentication
- mTLS certificate-based authentication
- Custom authentication implementations

### Error Handling
Consistent error handling across protocols:
- Typed errors mapped to protocol-specific codes
- Detailed error messages for debugging
- Stack traces in development mode
- Client-safe error messages in production

## Facade Integration

All servers use the `objstore` facade for storage operations instead of accessing backends directly. This provides:
- Centralized input validation and security checks
- Multi-backend routing via backend name configuration
- Consistent error handling across all server types

### Server Initialization Pattern

```go
// Initialize the facade with backends
objstore.Initialize(&objstore.FacadeConfig{
    BackendConfigs: map[string]objstore.BackendConfig{
        "default": {Type: "local", Settings: map[string]string{"path": "/data"}},
    },
    DefaultBackend: "default",
})

// Create server with backend name (empty string uses default)
server, _ := grpcserver.NewServer(
    grpcserver.WithAddress(":50051"),
    grpcserver.WithBackend(""), // Uses default backend
)
server.Start()
```

All server handlers use `objstore.*` functions internally:
```go
// Inside handler implementation
func (h *Handler) Put(key string, data io.Reader) error {
    return objstore.PutWithContext(ctx, h.keyRef(key), data)
}
```

## Server Lifecycle

### Initialization
1. Initialize objstore facade with backend configuration
2. Parse server-specific configuration
3. Configure TLS if enabled
4. Set up authentication adapter
5. Initialize protocol-specific server with backend name
6. Register handlers
7. Start listening on configured port

### Graceful Shutdown
1. Stop accepting new connections
2. Drain in-flight requests with timeout
3. Close storage backend connections
4. Clean up resources

### Health Checks
All servers expose health check endpoints:
- Liveness probe - server is running
- Readiness probe - server is ready to accept traffic
- Backend connectivity checks

## Protocol-Specific Details

### gRPC Service Definition
The gRPC service defines methods for all storage operations. Streaming RPCs handle large object transfers efficiently. The Protocol Buffer schema ensures type safety across client and server.

### REST API Endpoints
Standard REST endpoints for storage operations:
- `PUT /objects/{key}` - Upload object
- `GET /objects/{key}` - Download object
- `DELETE /objects/{key}` - Delete object
- `GET /objects` - List objects
- `HEAD /objects/{key}` - Check existence
- `GET /objects/{key}/metadata` - Get metadata only

### QUIC Transport
QUIC provides reliable, ordered delivery with independent streams. Each HTTP/3 request uses a separate QUIC stream, preventing head-of-line blocking. The server handles QUIC connection state and stream management.

### MCP Protocol
MCP servers expose storage as resources and tools. Resources represent stored objects, while tools perform operations. The protocol uses JSON-RPC 2.0 for request/response handling.

## Performance Considerations

### Connection Pooling
Backend storage clients use connection pooling to reduce overhead. Pool sizes are configurable based on expected load.

### Request Buffering
Servers buffer requests and responses appropriately:
- Small objects may be fully buffered
- Large objects use streaming to avoid memory pressure
- Configurable buffer sizes per protocol

### Concurrent Request Handling
All servers handle multiple concurrent requests:
- gRPC uses goroutines per RPC call
- REST uses goroutines per HTTP request
- QUIC uses goroutines per stream
- MCP processes requests sequentially

### Resource Limits
Servers enforce limits to prevent resource exhaustion:
- Maximum request body size
- Maximum concurrent connections
- Request timeout durations
- Rate limiting per client

## Deployment Models

### Single Protocol
Deploy one server type for specific use cases:
- gRPC for microservices
- REST for web applications
- QUIC for mobile clients
- MCP for AI integrations

### Multi-Protocol
Run multiple servers on different ports from the same process. Shares storage backend and configuration. Useful for supporting diverse clients.

### Gateway Pattern
Use a gateway or proxy in front of servers for additional features like load balancing, API management, and monitoring.
