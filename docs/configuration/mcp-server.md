# MCP Server Configuration

Configuration reference for the Model Context Protocol server.

## Basic Configuration

```yaml
mcp:
  enabled: true
  transport: stdio  # stdio, http
  log_level: info
```

## Transport Options

### Stdio Transport
```yaml
mcp:
  transport: stdio
  stdin: /dev/stdin
  stdout: /dev/stdout
  stderr: /dev/stderr
```

### HTTP Transport
```yaml
mcp:
  transport: http
  address: "0.0.0.0:3000"
  base_path: "/mcp"
  read_timeout: 30s
  write_timeout: 30s
```

## Server Information

```yaml
mcp:
  server_info:
    name: objstore-mcp
    version: "1.0.0"
    description: "Object storage server with MCP protocol"
```

## Resources Configuration

```yaml
mcp:
  resources:
    enabled: true
    list_prefix: ""  # Default prefix for listing
    max_results: 1000
```

## Tools Configuration

```yaml
mcp:
  tools:
    enabled: true
    allowed_tools:
      - get_object
      - put_object
      - delete_object
      - list_objects
```

## Prompts Configuration

```yaml
mcp:
  prompts:
    enabled: true
    custom_prompts:
      - name: "list-objects"
        description: "List all objects in storage"
      - name: "get-object-content"
        description: "Get content of a specific object"
```

## Authentication

```yaml
mcp:
  auth:
    enabled: true
    type: token
    token: ${MCP_AUTH_TOKEN}
```

## Logging

```yaml
mcp:
  logging:
    enabled: true
    format: json
    level: info
    log_requests: true
    log_responses: false
```

## Complete Example

```yaml
mcp:
  enabled: true
  transport: http
  address: "0.0.0.0:3000"
  base_path: "/mcp"
  
  server_info:
    name: objstore-mcp
    version: "1.0.0"
  
  resources:
    enabled: true
    list_prefix: ""
    max_results: 1000
  
  tools:
    enabled: true
    allowed_tools:
      - get_object
      - put_object
      - delete_object
      - list_objects
  
  prompts:
    enabled: true
  
  auth:
    enabled: true
    type: token
    token: ${MCP_AUTH_TOKEN}
  
  logging:
    enabled: true
    format: json
    level: info
```

## Protocol Details

MCP uses JSON-RPC 2.0 over the configured transport.

### Request Format
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "get_object",
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
        "text": "object content"
      }
    ]
  }
}
```

## Environment Variable Overrides

- `MCP_TRANSPORT` - Transport type
- `MCP_ADDRESS` - Listen address (HTTP transport)
- `MCP_AUTH_TOKEN` - Authentication token
