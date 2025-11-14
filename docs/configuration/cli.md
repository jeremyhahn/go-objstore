# CLI Configuration

Configuration reference for the objstore command-line interface.

## Configuration File

Default locations (searched in order):
1. `./objstore.yaml`
2. `~/.objstore.yaml`
3. `/etc/objstore/config.yaml`

Override with `--config` flag:
```bash
objstore --config /path/to/config.yaml [command]
```

## Basic Configuration

```yaml
backend: s3
config:
  region: us-east-1
  bucket: my-bucket

# CLI-specific settings
cli:
  output_format: text  # text, json, yaml
  color: auto          # auto, always, never
  quiet: false
  verbose: false
```

## Backend Configuration

Configure default backend for CLI commands:

```yaml
backend: local
config:
  path: /var/lib/objstore
```

Multiple backends in profile:

```yaml
profiles:
  dev:
    backend: local
    config:
      path: /tmp/objstore-dev
  
  prod:
    backend: s3
    config:
      region: us-east-1
      bucket: prod-bucket
  
  staging:
    backend: s3
    config:
      region: us-west-2
      bucket: staging-bucket
```

Use profile:
```bash
objstore --profile prod [command]
```

## Output Configuration

```yaml
cli:
  output_format: json
  color: always
  
  # Format-specific options
  json:
    pretty: true
    indent: 2
  
  text:
    show_headers: true
    column_separator: " | "
```

## Logging Configuration

```yaml
cli:
  logging:
    level: info  # debug, info, warn, error
    format: text  # text, json
    file: /var/log/objstore.log  # Optional log file
```

## Command Defaults

Set default options for specific commands:

```yaml
cli:
  commands:
    put:
      buffer_size: 65536
      parallel: 4
    
    get:
      output_dir: ./downloads
    
    list:
      max_results: 100
      delimiter: "/"
```

## Credentials

### Environment Variables
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export OBJSTORE_BACKEND=s3
export OBJSTORE_REGION=us-east-1
export OBJSTORE_BUCKET=my-bucket
```

### Configuration File
```yaml
backend: s3
config:
  region: us-east-1
  bucket: my-bucket
  # Credentials from AWS credential chain
  # Never hardcode credentials in config
```

## Complete Example

```yaml
# Default backend
backend: s3
config:
  region: us-east-1
  bucket: production-data

# CLI settings
cli:
  output_format: json
  color: auto
  quiet: false
  verbose: false
  
  json:
    pretty: true
    indent: 2
  
  logging:
    level: info
    format: text
  
  commands:
    put:
      buffer_size: 131072  # 128KB
      parallel: 8
    
    list:
      max_results: 1000
      delimiter: "/"

# Multiple profiles
profiles:
  dev:
    backend: local
    config:
      path: /tmp/objstore-dev
  
  prod:
    backend: s3
    config:
      region: us-east-1
      bucket: prod-bucket
  
  backup:
    backend: glacier
    config:
      region: us-west-2
      vault: backup-vault
```

## Environment Variables

Override configuration with environment variables:

- `OBJSTORE_BACKEND` - Backend type
- `OBJSTORE_CONFIG` - Configuration file path
- `OBJSTORE_PROFILE` - Profile name
- `OBJSTORE_OUTPUT` - Output format
- `OBJSTORE_LOG_LEVEL` - Logging level
- Backend-specific variables (AWS_*, GOOGLE_*, AZURE_*)

## Command-Line Flags

Flags override both configuration file and environment variables:

```bash
objstore --backend s3 \
         --config /etc/objstore/config.yaml \
         --profile prod \
         --output json \
         --verbose \
         list
```

## Configuration Precedence

From highest to lowest precedence:
1. Command-line flags
2. Environment variables
3. Configuration file
4. Profile settings
5. Built-in defaults
```

## Environment Variable Overrides

- `OBJSTORE_CONFIG` - Configuration file path
- `OBJSTORE_BACKEND` - Default backend
- `OBJSTORE_PROFILE` - Active profile
- `OBJSTORE_OUTPUT` - Output format
- `OBJSTORE_COLOR` - Color output
- `OBJSTORE_LOG_LEVEL` - Log level
