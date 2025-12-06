# Facade Migration Guide

This guide explains how to migrate all go-objstore servers, CLI, and client code to use the new facade pattern.

## Overview

The facade pattern provides a centralized, secure API for object storage operations across multiple backends. It prevents leaky abstractions and ensures consistent validation across all entry points (gRPC, REST, QUIC, MCP, CLI).

## Benefits

1. **Centralized Validation**: All input validation happens at the facade layer, preventing injection attacks
2. **Multi-Backend Support**: Seamlessly work with multiple storage backends using `backend:key` syntax
3. **Security**: Path traversal, null bytes, and control characters are blocked at the facade layer
4. **Consistency**: All interfaces use the same API, reducing code duplication
5. **Simplified Error Handling**: Consistent error messages across all entry points

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Applications                         │
│   (gRPC, REST, QUIC, MCP, CLI, Direct Usage)           │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│              objstore.Facade (Singleton)                │
│  • Input validation (pkg/validation)                    │
│  • Backend routing (backend:key parsing)                │
│  • Error sanitization                                   │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                Storage Backends                         │
│    (Local, S3, MinIO, GCS, Azure, etc.)                │
└─────────────────────────────────────────────────────────┘
```

## Key Changes

### Before (Direct Storage Access)

```go
// Old pattern - creates leaky abstraction
storage, err := factory.NewStorage("local", settings)
if err != nil {
    return err
}

err = storage.Put(key, data)
```

### After (Facade Pattern - Simplified API)

```go
// Application startup - initialize facade ONCE with simplified API
// The facade creates backends internally via the factory
err := objstore.Initialize(&objstore.FacadeConfig{
    BackendConfigs: map[string]objstore.BackendConfig{
        "local": {Type: "local", Settings: map[string]string{"path": "/data"}},
        "s3":    {Type: "s3", Settings: map[string]string{"bucket": "mybucket"}},
    },
    DefaultBackend: "local",
})

// Throughout application - use facade functions
err = objstore.Put(key, data)                          // Uses default backend
err = objstore.PutWithContext(ctx, "s3:key", data)     // Uses specific backend
```

### Legacy Pattern (Still Supported)

For advanced use cases or when you need to configure storage backends manually:

```go
// Create backends manually using factory
local, _ := factory.NewStorage("local", map[string]string{"path": "/data"})
s3, _ := factory.NewStorage("s3", map[string]string{"bucket": "mybucket"})

// Initialize facade with pre-configured storage instances
err := objstore.Initialize(&objstore.FacadeConfig{
    Backends: map[string]common.Storage{
        "local": local,
        "s3":    s3,
    },
    DefaultBackend: "local",
})
```

## Migration Steps

### 1. Server Initialization Pattern

All servers should:
1. Initialize the facade at application startup (before creating servers)
2. Create servers with a backend name (empty string for default)
3. Use facade functions in all handlers

**Example: Application Main**

```go
// cmd/myserver/main.go

func main() {
    // Initialize facade with simplified API
    err := objstore.Initialize(&objstore.FacadeConfig{
        BackendConfigs: map[string]objstore.BackendConfig{
            "default": {
                Type:     "local",
                Settings: map[string]string{"path": "/data/storage"},
            },
        },
        DefaultBackend: "default",
    })
    if err != nil {
        log.Fatalf("Failed to initialize objstore: %v", err)
    }

    // Create server - uses backend name, not storage instance
    server, err := grpcserver.NewServer(
        grpcserver.WithAddress(":50051"),
        grpcserver.WithBackend(""), // Empty string = default backend
    )
    if err != nil {
        log.Fatalf("Failed to create server: %v", err)
    }

    server.Start()
}
```

**Server Struct Pattern:**

```go
// pkg/server/grpc/server.go

type Server struct {
    api.UnimplementedObjectStoreServer
    backend string  // Backend name (empty = default)
}

// keyRef constructs a key reference with backend prefix if needed
func (s *Server) keyRef(key string) string {
    if s.backend == "" {
        return key
    }
    return s.backend + ":" + key
}
```

### 2. Handler Migration Pattern

**Before:**
```go
func (s *Server) PutObject(ctx context.Context, req *api.PutObjectRequest) (*api.PutObjectResponse, error) {
    // Manual validation
    if req.Key == "" {
        return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
    }

    // Direct storage access
    err := s.storage.PutWithContext(ctx, req.Key, bytes.NewReader(req.Data))
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &api.PutObjectResponse{Success: true}, nil
}
```

**After:**
```go
func (s *Server) PutObject(ctx context.Context, req *api.PutObjectRequest) (*api.PutObjectResponse, error) {
    // Validation is automatic in facade
    // Support backend:key syntax (e.g., "s3:myfile.txt")
    err := objstore.PutWithContext(ctx, req.Key, bytes.NewReader(req.Data))
    if err != nil {
        // Facade returns sanitized errors
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &api.PutObjectResponse{Success: true}, nil
}
```

### 3. REST Server Migration

**Before:**
```go
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")

    // Manual validation
    if key == "" {
        writeError(w, http.StatusBadRequest, "key required")
        return
    }

    err := s.storage.PutWithContext(r.Context(), key, r.Body)
    if err != nil {
        writeError(w, http.StatusInternalServerError, err.Error())
        return
    }

    writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}
```

**After:**
```go
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")

    // Facade handles validation and backend routing
    err := objstore.PutWithContext(r.Context(), key, r.Body)
    if err != nil {
        // Sanitized error from facade
        writeError(w, http.StatusInternalServerError, err.Error())
        return
    }

    writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}
```

### 4. CLI Migration

**Before:**
```go
func putCommand(cfg *Config) error {
    storage, err := factory.NewStorage(cfg.Backend, cfg.BackendSettings)
    if err != nil {
        return err
    }

    return storage.Put(cfg.Key, data)
}
```

**After:**
```go
func initializeStorageFromConfig(cfg *Config) error {
    // Create backends from config
    backends := make(map[string]common.Storage)

    for name, settings := range cfg.Backends {
        storage, err := factory.NewStorage(name, settings)
        if err != nil {
            return fmt.Errorf("failed to create backend %s: %w", name, err)
        }
        backends[name] = storage
    }

    // Initialize facade once
    return objstore.Initialize(&objstore.FacadeConfig{
        Backends:       backends,
        DefaultBackend: cfg.DefaultBackend,
    })
}

func putCommand(cfg *Config) error {
    // Facade already initialized
    // Support "backend:key" syntax from CLI
    keyRef := cfg.Key
    if cfg.Backend != "" {
        keyRef = fmt.Sprintf("%s:%s", cfg.Backend, cfg.Key)
    }

    return objstore.Put(keyRef, data)
}
```

### 5. MCP Server Migration

**Before:**
```go
func (s *Server) handlePutTool(args map[string]interface{}) (interface{}, error) {
    key, _ := args["key"].(string)
    data, _ := args["data"].(string)

    err := s.storage.Put(key, strings.NewReader(data))
    if err != nil {
        return nil, err
    }

    return map[string]bool{"success": true}, nil
}
```

**After:**
```go
func (s *Server) handlePutTool(args map[string]interface{}) (interface{}, error) {
    key, _ := args["key"].(string)
    data, _ := args["data"].(string)

    // Facade handles validation and backend routing
    err := objstore.Put(key, strings.NewReader(data))
    if err != nil {
        return nil, err
    }

    return map[string]bool{"success": true}, nil
}
```

## Backend Reference Syntax

The facade supports referencing specific backends using `backend:key` syntax:

```go
// Use default backend
objstore.Put("myfile.txt", data)

// Use specific backend
objstore.PutWithContext(ctx, "s3:myfile.txt", data)
objstore.PutWithContext(ctx, "local:myfile.txt", data)

// Works in all operations
objstore.GetWithContext(ctx, "gcs:backups/2024/file.txt")
objstore.DeleteWithContext(ctx, "azure:temp/cache.dat")
objstore.ListWithContext(ctx, "s3:logs/")
```

## Validation Features

The facade automatically validates:

1. **Path Traversal**: Blocks `..`, `../`, `/.., etc.
2. **Absolute Paths**: Blocks `/etc/passwd`, `C:\Windows`, etc.
3. **Null Bytes**: Blocks `file\x00.txt`
4. **Control Characters**: Blocks `\n`, `\r`, `\t`, etc.
5. **Backend Names**: Only allows lowercase alphanumeric and hyphens
6. **Key Length**: Maximum 1024 characters
7. **Metadata**: Validates custom metadata keys and values

## Error Handling

The facade provides sanitized error messages:

```go
err := objstore.Put("../etc/passwd", data)
// Error: "invalid key: key contains path traversal attempt"

err := objstore.PutWithContext(ctx, "INVALID:key", data)
// Error: "invalid key reference: invalid backend in key reference: backend name contains invalid characters (allowed: a-z, 0-9, -)"
```

## Testing

### Test Helper Pattern

Create a test helper function to initialize the facade consistently:

```go
// test_helpers_test.go
func initTestFacade(t *testing.T, storage common.Storage) {
    t.Helper()
    objstore.Reset()
    err := objstore.Initialize(&objstore.FacadeConfig{
        Backends:       map[string]common.Storage{"default": storage},
        DefaultBackend: "default",
    })
    if err != nil {
        t.Fatalf("Failed to initialize facade: %v", err)
    }
}

func createTestServer(t *testing.T, storage common.Storage) *Server {
    t.Helper()
    initTestFacade(t, storage)
    server, err := NewServer(&ServerConfig{
        Backend: "", // Use default backend
    })
    if err != nil {
        t.Fatalf("Failed to create server: %v", err)
    }
    return server
}
```

### Unit Tests

```go
func TestHandler(t *testing.T) {
    storage := local.NewMemoryStorage()
    server := createTestServer(t, storage)
    defer objstore.Reset()

    // Test your handler
    err := server.HandlePut(ctx, "test.txt", data)
    require.NoError(t, err)
}
```

### Integration Tests

Integration tests with real backends:

```go
func TestServerIntegration(t *testing.T) {
    local, err := factory.NewStorage("local", map[string]string{
        "path": t.TempDir(),
    })
    require.NoError(t, err)

    server := createTestServer(t, local)
    defer objstore.Reset()

    // Run integration tests
}
```

## Migration Checklist

### For Each Server (gRPC, REST, QUIC, MCP)

- [ ] Remove direct storage fields from server structs
- [ ] Initialize facade in server constructor/initialization
- [ ] Update all handlers to use facade functions
- [ ] Remove manual validation code (facade handles it)
- [ ] Update error handling to use facade errors
- [ ] Add support for `backend:key` syntax in handlers
- [ ] Update tests to initialize facade
- [ ] Test with multiple backends

### For CLI

- [ ] Add facade initialization in main/setup
- [ ] Update all commands to use facade functions
- [ ] Add support for `--backend` flag with `backend:key` syntax
- [ ] Remove manual storage creation in commands
- [ ] Update tests

### For Examples

- [ ] Update all examples to show facade pattern
- [ ] Demonstrate multi-backend usage
- [ ] Show validation features
- [ ] Update documentation

## Best Practices

1. **Initialize Once**: Call `objstore.Initialize()` once at application startup
2. **Reset in Tests**: Call `objstore.Reset()` before each test
3. **Use Context**: Prefer `*WithContext` variants for proper cancellation
4. **Backend Naming**: Use lowercase names with hyphens (e.g., `my-s3-backend`)
5. **Error Handling**: Trust facade error messages; they're sanitized for security
6. **Validation**: Don't add additional validation; facade handles it
7. **Testing**: Test both default backend and specific backend:key syntax

## Example: Complete Server Update

See `/examples/facade-usage/main.go` for a complete working example.

## Support

For questions or issues during migration:
- Review the facade implementation: `pkg/objstore/facade.go`
- Check validation rules: `pkg/validation/validation.go`
- See integration tests: `pkg/objstore/facade_integration_test.go`
- Review example: `examples/facade-usage/main.go`
