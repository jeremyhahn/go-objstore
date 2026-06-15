# MCP Server Configuration

Configuration reference for the Model Context Protocol server.

The MCP server is configured with command-line flags. There is no configuration
file. Two binaries serve MCP:

- `objstore-mcp-server` - standalone MCP server
- `objstore-server` - combined server (gRPC + REST + QUIC + MCP)

## Standalone Server Flags (objstore-mcp-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `http` | Server mode: `stdio` or `http` |
| `--addr` | `:8081` | HTTP server address (only for http mode) |
| `--backend` | `local` | Storage backend (`local`, `s3`, `gcs`, `azure`) |
| `--path` | `/tmp/objstore` | Storage path for the local backend |

```bash
# HTTP mode (default), listening on :8081
objstore-mcp-server --backend local --path /var/lib/objstore

# Stdio mode (for MCP clients that spawn the server as a subprocess)
objstore-mcp-server --mode stdio --backend local --path /var/lib/objstore
```

## Combined Server Flags (objstore-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--mcp` | `true` | Enable the MCP server |
| `--mcp-mode` | `http` | MCP mode: `stdio` or `http` |
| `--mcp-addr` | `:8081` | MCP HTTP server address |

## Transport Modes

### Stdio
JSON-RPC 2.0 over stdin/stdout. Logs go to stderr so stdout stays clean for the
protocol stream. Use this mode when an MCP client (such as Claude Desktop)
launches the server as a subprocess.

### HTTP
JSON-RPC 2.0 over HTTP POST. Endpoints:

- `POST /` - JSON-RPC 2.0 requests
- `GET /health` - Health check (returns `OK`)

The default request body limit in HTTP mode is 100MB.

## Tools

The server exposes these MCP tools:

- `objstore_put` - Store an object (data must be base64-encoded)
- `objstore_get` - Retrieve an object (data returned base64-encoded)
- `objstore_delete` - Delete an object
- `objstore_list` - List objects
- `objstore_exists` - Check object existence
- `objstore_get_metadata` - Get object metadata
- `objstore_update_metadata` - Update object metadata
- `objstore_health` - Backend health check
- `objstore_archive` - Archive an object
- `objstore_add_policy` - Add lifecycle policy
- `objstore_remove_policy` - Remove lifecycle policy
- `objstore_get_policies` - List lifecycle policies
- `objstore_apply_policies` - Apply lifecycle policies

Object data always travels base64-encoded inside the JSON-RPC payload so the
transport remains binary-safe. Raw (non-base64) data in `objstore_put` is
rejected.

## Protocol Details

MCP uses JSON-RPC 2.0 over the configured transport.

### Request Format
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "objstore_get",
    "arguments": {
      "key": "path/to/object"
    }
  }
}
```

### Response Format
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"key\":\"path/to/object\",\"data\":\"aGVsbG8=\"}"
      }
    ]
  }
}
```

## Authentication and Advanced Settings (Programmatic)

The binaries do not expose authentication flags. When embedding the server,
configure adapters through `mcpserver.ServerConfig` (see `pkg/server/mcp`):

- `Authenticator` / `Authorizer` - pluggable auth adapters (HTTP mode; always
  enforced for HTTP, opt-in for stdio via `EnforceStdioAuthz`)
- `TLSConfig` - TLS/mTLS for HTTP mode
- `MaxBodySize` - HTTP request body limit (default 100MB)
- `EnableRateLimit` / `RateLimitConfig` - rate limiting
- `EnableAudit` / `AuditLogger` - audit logging
- `ResourcePrefix` - prefix for MCP resource listings
- `Backend` - named backend to use (empty = default backend)
