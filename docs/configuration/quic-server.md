# QUIC Server Configuration

Configuration reference for the QUIC/HTTP3 server.

The QUIC server is configured with command-line flags. There is no configuration
file. Two binaries serve QUIC/HTTP3:

- `objstore-quic-server` - standalone QUIC server
- `objstore-server` - combined server (gRPC + REST + QUIC + MCP)

## Standalone Server Flags (objstore-quic-server)

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:4433` | UDP address to listen on |
| `-backend` | `local` | Storage backend type (`local`, `s3`, `gcs`, `azure`) |
| `-path` | `./data` | Storage path (for local backend) |
| `-tlscert` | (none) | Path to TLS certificate file |
| `-tlskey` | (none) | Path to TLS private key file |
| `-selfsigned` | `false` | Use a self-signed certificate (testing only) |
| `-maxbodysize` | `104857600` (100MB) | Maximum request body size in bytes |
| `-readtimeout` | `30s` | Read timeout |
| `-writetimeout` | `30s` | Write timeout |
| `-idletimeout` | `60s` | Idle timeout |
| `-maxstreams` | `100` | Maximum bidirectional streams per connection |

```bash
objstore-quic-server \
  -addr :4433 \
  -backend local \
  -path /var/lib/objstore \
  -tlscert /etc/objstore/tls/server.crt \
  -tlskey /etc/objstore/tls/server.key
```

## Combined Server Flags (objstore-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--quic` | `true` | Enable the QUIC/HTTP3 server |
| `--quic-addr` | `:4433` | QUIC server address |
| `--quic-tls-cert` | (none) | QUIC TLS certificate file |
| `--quic-tls-key` | (none) | QUIC TLS key file |
| `--quic-self-signed` | `false` | Use self-signed cert for QUIC (testing only) |

If no TLS configuration is provided, the combined server logs a warning and
leaves QUIC disabled.

## TLS

QUIC requires TLS 1.3; the server will not start without a certificate.
Provide `-tlscert`/`-tlskey`, or pass `-selfsigned` to generate an in-memory
self-signed certificate for testing. Never use `-selfsigned` in production.

## Protocol Defaults

The server runs with these QUIC defaults (`quicserver.DefaultOptions()`):

- `max_idle_timeout`: 60s
- `keep_alive_period`: 30s
- `max_incoming_streams` / `max_incoming_uni_streams`: 100
- `initial_stream_receive_window`: 512KB
- `max_stream_receive_window`: 6MB
- `initial_connection_receive_window`: 1MB
- `max_connection_receive_window`: 15MB
- Path MTU discovery: enabled
- 0-RTT: disabled (for security)
- Datagrams: disabled

These are tunable programmatically through `quicserver.Options` when embedding
the server (see `pkg/server/quic`).

## UDP Configuration

QUIC uses UDP. Ensure the UDP port is open in the firewall:

```bash
# Linux
sudo firewall-cmd --add-port=4433/udp

# iptables
sudo iptables -A INPUT -p udp --dport 4433 -j ACCEPT
```
