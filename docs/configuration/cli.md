# CLI Configuration

Configuration reference for the objstore command-line interface.

## Configuration File

The CLI loads an optional YAML config file named `.objstore.yaml`, searched in:
1. `$HOME/.objstore.yaml`
2. `./.objstore.yaml` (current directory)

Override with the `--config` flag:
```bash
objstore --config /path/to/config.yaml [command]
```

Config file keys match the global flag names:

```yaml
backend: s3
backend-bucket: my-bucket
backend-region: us-east-1
output-format: json
```

## Global Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | (none) | Config file (default is `$HOME/.objstore.yaml`) |
| `--server` | (none) | Server URL for remote operations (e.g., `http://localhost:8080`) |
| `--server-protocol` | `rest` | Server protocol: `rest`, `grpc`, or `quic` |
| `--backend` | `local` | Storage backend (`local`, `s3`, `minio`, `gcs`, `azure`) |
| `--backend-path` | `./storage` | Path for local backend |
| `--backend-bucket` | (none) | Bucket name for cloud backends |
| `--backend-region` | (none) | Region for cloud backends |
| `--backend-key` | (none) | Access key for cloud backends |
| `--backend-secret` | (none) | Secret key for cloud backends |
| `--backend-url` | (none) | Custom endpoint URL for cloud backends |
| `--output-format`, `-o` | `text` | Output format (`text`, `json`, `table`) |

## Backend Configuration

Configure the default backend for CLI commands:

```yaml
backend: local
backend-path: /var/lib/objstore
```

```yaml
backend: s3
backend-bucket: prod-bucket
backend-region: us-east-1
```

## Remote Server Mode

The CLI can operate against a running objstore server instead of a local
backend:

```bash
objstore --server http://localhost:8080 --server-protocol rest list
objstore --server localhost:50051 --server-protocol grpc get my/key out.txt
```

## Credentials

### Environment Variables
Cloud provider credential chains are honored as usual:
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

### Configuration File
```yaml
backend: s3
backend-bucket: my-bucket
backend-region: us-east-1
# Credentials from the AWS credential chain
# Never hardcode credentials in config
```

## Environment Variable Overrides

Configuration keys can be overridden with environment variables using the
`OBJECTSTORE_` prefix and the uppercased key name:

- `OBJECTSTORE_BACKEND` - Backend type
- `OBJECTSTORE_SERVER` - Server URL

Keys containing dashes (e.g. `backend-path`) are easiest to set via the config
file or flags.

## Configuration Precedence

From highest to lowest precedence:
1. Command-line flags
2. Environment variables (`OBJECTSTORE_*`)
3. Configuration file
4. Built-in defaults

## Lifecycle Persistence

For the `local` backend, the CLI uses a persistent lifecycle manager so
policies survive across CLI invocations. Policies are stored in
`.lifecycle-policies.json` in the storage path.
