# QUIC Server Configuration

Configuration reference for the QUIC/HTTP3 server.

## Basic Configuration

```yaml
quic:
  enabled: true
  address: "0.0.0.0:4433"
  max_streams: 100
  max_idle_timeout: 30s
  max_receive_buffer_size: 10485760  # 10MB
```

## Parameters

### Network
- `address` - Listen address and port (default: "0.0.0.0:4433")
- `max_streams` - Maximum concurrent bidirectional streams (default: 100)

### QUIC Protocol
- `max_idle_timeout` - Connection idle timeout (default: 30s)
- `max_receive_buffer_size` - Maximum receive buffer size (default: 10MB)
- `max_receive_stream_flow_control_window` - Stream flow control window
- `max_receive_connection_flow_control_window` - Connection flow control window
- `keep_alive_period` - Keep-alive interval (default: 10s)

### HTTP/3
- `max_header_bytes` - Maximum header size (default: 16KB)
- `max_uni_streams` - Maximum unidirectional streams (default: 100)

## TLS Configuration

QUIC requires TLS 1.3. Configuration is mandatory.

```yaml
quic:
  tls:
    cert_file: /path/to/server.crt
    key_file: /path/to/server.key
    # Client auth optional
    client_ca_file: /path/to/client-ca.crt
    client_auth: require  # require, request, none
```

## Authentication

```yaml
quic:
  auth:
    enabled: true
    type: bearer
    bearer_token: ${QUIC_AUTH_TOKEN}
```

## Connection Migration

```yaml
quic:
  connection_migration:
    enabled: true  # Allow connections to migrate across network changes
```

## Performance Tuning

```yaml
quic:
  performance:
    disable_path_mtu_discovery: false
    initial_stream_receive_window: 512000  # 512KB
    max_stream_receive_window: 6291456     # 6MB
    initial_connection_receive_window: 1048576  # 1MB
    max_connection_receive_window: 15728640    # 15MB
```

## Complete Example

```yaml
quic:
  enabled: true
  address: "0.0.0.0:4433"
  max_streams: 100
  max_idle_timeout: 30s
  max_receive_buffer_size: 10485760
  keep_alive_period: 10s
  
  tls:
    cert_file: /etc/objstore/tls/server.crt
    key_file: /etc/objstore/tls/server.key
  
  auth:
    enabled: true
    type: bearer
    bearer_token: ${QUIC_AUTH_TOKEN}
  
  connection_migration:
    enabled: true
  
  performance:
    initial_stream_receive_window: 512000
    max_stream_receive_window: 6291456
    initial_connection_receive_window: 1048576
    max_connection_receive_window: 15728640
```

## UDP Configuration

QUIC uses UDP. Ensure UDP port is open in firewall:

```bash
# Linux
sudo firewall-cmd --add-port=4433/udp

# iptables
sudo iptables -A INPUT -p udp --dport 4433 -j ACCEPT
```

## Environment Variable Overrides

- `QUIC_ADDRESS` - Listen address
- `QUIC_TLS_CERT` - Certificate path
- `QUIC_TLS_KEY` - Key path
- `QUIC_AUTH_TOKEN` - Authentication token
