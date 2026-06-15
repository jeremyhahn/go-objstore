# REST API Server Configuration

Configuration reference for the REST API server.

The REST server is configured with command-line flags. There is no configuration
file. Two binaries serve the REST API:

- `objstore-rest-server` - standalone REST server
- `objstore-server` - combined server (gRPC + REST + QUIC + MCP)

Advanced settings (timeouts, TLS, auth adapters, rate limiting) are available
programmatically through `restserver.ServerConfig` when embedding the server in
your own application (see `pkg/server/rest`).

## Standalone Server Flags (objstore-rest-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `localhost` | REST server host |
| `--port` | `8080` | REST server port |
| `--backend` | `local` | Storage backend (`local`, `s3`, `gcs`, `azure`) |
| `--path` | `/tmp/objstore` | Storage path for the local backend |
| `--metrics-public` | `false` | Expose `/metrics` without authorization |

```bash
objstore-rest-server --host 0.0.0.0 --port 8080 --backend local --path /var/lib/objstore
```

## Combined Server Flags (objstore-server)

The combined server enables REST by default and uses these REST-related flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--rest` | `true` | Enable the REST server |
| `--rest-port` | `8080` | REST server port (binds `0.0.0.0`) |
| `--metrics-public` | `false` | Expose `/metrics` without authorization |
| `--rate-limit` | `false` | Enable rate limiting on all transports |
| `--rate-limit-rps` | `100` | Rate limit requests per second |
| `--rate-limit-burst` | `200` | Rate limit burst size |
| `--rate-limit-per-client` | `false` | Rate limit per client instead of globally |
| `--audit` | `true` | Enable audit logging on all transports |

```bash
objstore-server --rest-port 8080 --backend local --path /var/lib/objstore
```

## Built-in Defaults

The server runs with these defaults (`restserver.DefaultServerConfig()`):

- `read_timeout`: 60s
- `write_timeout`: 60s
- `idle_timeout`: 120s
- `max_request_size`: 100MB (104857600 bytes)
- CORS: enabled (all origins, no credentials)
- Security headers: enabled
- Request ID middleware: enabled
- Audit logging: enabled (JSON format)
- Rate limiting: disabled
- TLS: disabled
- `/metrics`: requires authorization (set `--metrics-public` to exempt it)

## TLS, Authentication, and Rate Limiting (Programmatic)

The standalone binaries do not expose TLS or authentication flags for REST.
When embedding the server, configure these through `restserver.ServerConfig`:

```go
config := restserver.DefaultServerConfig()
config.Host = "0.0.0.0"
config.Port = 8443
config.TLSConfig = &adapters.TLSConfig{
    CertFile: "/etc/objstore/tls/server.crt",
    KeyFile:  "/etc/objstore/tls/server.key",
}
config.Authenticator = myAuthenticator // adapters.Authenticator
config.Authorizer = myAuthorizer       // adapters.Authorizer
config.EnableRateLimit = true

server, err := restserver.NewServer(storage, config)
```

## API Endpoints

All object routes are available under `/api/v1` and, for backwards
compatibility, at the root path.

### Objects
- `GET /api/v1/objects` - List objects
- `GET /api/v1/objects/{key}` - Get object
- `PUT /api/v1/objects/{key}` - Put object
- `DELETE /api/v1/objects/{key}` - Delete object (returns `204 No Content`)
- `HEAD /api/v1/objects/{key}` - Check existence
- `HEAD /api/v1/exists/{key}` - Check existence
- `GET /api/v1/metadata/{key}` - Get metadata
- `PUT /api/v1/metadata/{key}` - Update metadata

### Lifecycle and Archive
- `POST /api/v1/archive` - Archive an object
- `GET /api/v1/policies` - List lifecycle policies
- `POST /api/v1/policies` - Add lifecycle policy
- `DELETE /api/v1/policies/{id}` - Remove lifecycle policy
- `POST /api/v1/policies/apply` - Apply lifecycle policies

### Replication
- `POST /api/v1/replication/policies` - Add replication policy
- `GET /api/v1/replication/policies` - List replication policies
- `GET /api/v1/replication/policies/{id}` - Get replication policy
- `DELETE /api/v1/replication/policies/{id}` - Remove replication policy
- `POST /api/v1/replication/trigger` - Trigger replication
- `GET /api/v1/replication/status/{id}` - Get replication status

### Operational
- `GET /health` - Health check (no auth required)
- `GET /metrics` - Prometheus metrics (requires authorization unless `--metrics-public`)
- `GET /swagger/*` - Swagger UI

### Query Parameters (list)
- `prefix` - Filter by prefix

## Container Example

```bash
docker run -p 8080:8080 objstore \
  objstore-rest-server --host 0.0.0.0 --port 8080 --backend local --path /data
```
