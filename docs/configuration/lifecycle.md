# Lifecycle Policy Configuration

Configuration reference for automatic data retention and archival policies.

## Basic Configuration

```yaml
lifecycle:
  enabled: true
  evaluation_interval: 24h
  policies:
    - id: cleanup-logs
      prefix: logs/
      action: delete
      retention: 720h  # 30 days
```

## Policy Parameters

### Required Fields
- `id` - Unique policy identifier
- `prefix` - Object key prefix to match
- `action` - Action to perform (delete or archive)
- `retention` - How long to keep objects (duration string)

### Optional Fields
- `enabled` - Enable this policy (default: true)
- `destination` - Archive backend for archive action
- `filters` - Additional filtering criteria

## Policy Actions

### Delete Action
```yaml
lifecycle:
  policies:
    - id: delete-temp-files
      prefix: temp/
      action: delete
      retention: 24h
```

Objects older than retention period are permanently deleted.

### Archive Action
```yaml
lifecycle:
  policies:
    - id: archive-old-data
      prefix: data/
      action: archive
      retention: 2160h  # 90 days
      destination: glacier
```

Objects older than retention period moved to archive backend.

## Duration Formats

Retention periods use Go duration strings:
- `24h` - 24 hours
- `168h` - 7 days (1 week)
- `720h` - 30 days
- `2160h` - 90 days
- `8760h` - 365 days (1 year)

## Multiple Policies

```yaml
lifecycle:
  enabled: true
  evaluation_interval: 24h
  
  policies:
    # Delete temporary uploads after 1 day
    - id: cleanup-uploads
      prefix: uploads/temp/
      action: delete
      retention: 24h
    
    # Delete logs after 30 days
    - id: cleanup-logs
      prefix: logs/
      action: delete
      retention: 720h
    
    # Archive old data after 90 days
    - id: archive-data
      prefix: data/
      action: archive
      retention: 2160h
      destination: glacier
    
    # Delete archived data after 7 years
    - id: cleanup-archives
      prefix: archives/
      action: delete
      retention: 61320h  # 7 years
```

## Evaluation Configuration

```yaml
lifecycle:
  enabled: true
  evaluation_interval: 24h  # How often to evaluate policies
  evaluation_timeout: 1h    # Maximum time for evaluation
  batch_size: 1000          # Objects per batch
  workers: 4                # Concurrent workers
```

## Archive Destinations

Define archive backends:

```yaml
storage:
  backends:
    glacier:
      type: glacier
      config:
        region: us-east-1
        vault: long-term-storage

lifecycle:
  policies:
    - id: archive-old
      prefix: historical/
      action: archive
      retention: 2160h
      destination: glacier  # References backend name
```

## Filtering

### Size-Based Filtering
```yaml
lifecycle:
  policies:
    - id: delete-small-temp
      prefix: temp/
      action: delete
      retention: 24h
      filters:
        min_size: 0
        max_size: 1048576  # 1MB
```

### Name Pattern Filtering
```yaml
lifecycle:
  policies:
    - id: delete-logs
      prefix: logs/
      action: delete
      retention: 720h
      filters:
        name_pattern: "*.log"
```

## Backend-Specific Behavior

### Backends with Native Lifecycle Support
Some backends implement lifecycle natively:
- Amazon S3 lifecycle rules
- Google Cloud Storage lifecycle management
- Azure Blob Storage lifecycle policies

When available, native implementation is preferred.

### Backends without Native Support
Application-level policy evaluation for:
- Local filesystem
- MinIO (unless configured)
- Custom backends

## Monitoring and Metrics

```yaml
lifecycle:
  metrics:
    enabled: true
    export_interval: 60s
```

Metrics exported:
- Objects evaluated per policy
- Objects deleted
- Objects archived
- Errors encountered
- Evaluation duration

## Error Handling

```yaml
lifecycle:
  error_handling:
    retry_attempts: 3
    retry_delay: 5s
    continue_on_error: true  # Don't stop on individual object errors
```

## Complete Example

```yaml
lifecycle:
  enabled: true
  evaluation_interval: 24h
  evaluation_timeout: 1h
  batch_size: 1000
  workers: 4
  
  policies:
    # Temporary uploads
    - id: cleanup-temp-uploads
      prefix: uploads/temp/
      action: delete
      retention: 24h
      enabled: true
    
    # Application logs
    - id: cleanup-app-logs
      prefix: logs/app/
      action: delete
      retention: 720h  # 30 days
      filters:
        name_pattern: "*.log"
    
    # Access logs
    - id: cleanup-access-logs
      prefix: logs/access/
      action: delete
      retention: 2160h  # 90 days
    
    # Old application data
    - id: archive-old-data
      prefix: data/
      action: archive
      retention: 4320h  # 180 days
      destination: glacier
      filters:
        min_size: 1048576  # Only archive files > 1MB
    
    # Very old archives
    - id: delete-ancient-archives
      prefix: archives/
      action: delete
      retention: 61320h  # 7 years
  
  metrics:
    enabled: true
    export_interval: 60s
  
  error_handling:
    retry_attempts: 3
    retry_delay: 5s
    continue_on_error: true
```

## Testing Policies

Test policies without executing:

```bash
# Dry run mode
objstore lifecycle evaluate --dry-run

# Test specific policy
objstore lifecycle evaluate --policy=cleanup-logs --dry-run
```

## Manual Execution

Trigger policy evaluation manually:

```bash
# Evaluate all policies
objstore lifecycle evaluate

# Evaluate specific policy
objstore lifecycle evaluate --policy=cleanup-logs
```

## Environment Variable Overrides

- `LIFECYCLE_ENABLED` - Enable lifecycle management
- `LIFECYCLE_INTERVAL` - Evaluation interval
- `LIFECYCLE_WORKERS` - Number of workers
