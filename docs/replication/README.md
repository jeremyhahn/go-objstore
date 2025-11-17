# Replication and Sync Documentation

Comprehensive guide to replicating and synchronizing data between storage backends in go-objstore.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Replication Modes](#replication-modes)
4. [Encryption Layers](#encryption-layers)
5. [Configuration](#configuration)
6. [API Usage](#api-usage)
7. [Best Practices](#best-practices)
8. [Technical Details](#technical-details)

---

## Overview

The go-objstore replication system provides automated synchronization between storage backends with comprehensive encryption support. Key features include:

- **Multi-backend support**: Replicate between any combination of storage backends (local, S3, GCS, Azure, etc.)
- **Flexible replication modes**: Choose between transparent (re-encryption) or opaque (direct copy) modes
- **Three-layer encryption**: Backend at-rest, source DEK, and destination DEK encryption
- **Change detection**: Efficient metadata-based change detection using ETag and LastModified
- **Background sync**: Automated periodic synchronization with configurable intervals
- **Incremental sync**: JSONL-based change log for efficient incremental updates
- **Real-time sync**: File system watcher for immediate change propagation
- **Full API coverage**: Manage replication via gRPC, REST, QUIC, or CLI

### Use Cases

- **Disaster recovery**: Replicate production data to backup storage
- **Multi-cloud backup**: Copy data across cloud providers for redundancy
- **Data migration**: Move data from one backend to another
- **Geographic distribution**: Sync data to multiple regions
- **Development/staging**: Mirror production data to test environments
- **Compliance**: Maintain encrypted copies in different jurisdictions

---

## Quick Start

### Basic Replication Setup

Create a simple replication policy that copies local files to S3:

```bash
# Using CLI
objstore replication add \
  --id=local-to-s3 \
  --source-backend=local \
  --source-setting=path=/data/production \
  --dest-backend=s3 \
  --dest-setting=bucket=backup-bucket \
  --dest-setting=region=us-east-1 \
  --interval=5m \
  --mode=transparent

# List policies
objstore replication list

# Trigger manual sync
objstore replication sync --id=local-to-s3

# Sync all policies
objstore replication sync --all
```

### Programmatic Usage

```go
package main

import (
    "context"
    "time"

    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/replication"
    "github.com/jeremyhahn/go-objstore/pkg/adapters"
    "github.com/jeremyhahn/go-objstore/pkg/audit"
)

func main() {
    // Create replication manager
    logger := adapters.NewStdoutLogger()
    auditLog := audit.NewJSONAuditLogger("audit.log")

    manager, err := replication.NewPersistentReplicationManager(
        &replication.OSFileSystem{},
        "policies.json",
        5*time.Minute,
        logger,
        auditLog,
    )
    if err != nil {
        panic(err)
    }

    // Define replication policy
    policy := common.ReplicationPolicy{
        ID:                  "local-to-s3",
        SourceBackend:       "local",
        SourceSettings:      map[string]string{"path": "/data/production"},
        DestinationBackend:  "s3",
        DestinationSettings: map[string]string{
            "bucket": "backup-bucket",
            "region": "us-east-1",
        },
        CheckInterval:   5 * time.Minute,
        Enabled:         true,
        ReplicationMode: common.ReplicationModeTransparent,
    }

    // Add policy
    err = manager.AddPolicy(policy)
    if err != nil {
        panic(err)
    }

    // Start background sync
    ctx := context.Background()
    go manager.Run(ctx)

    // Trigger manual sync
    result, err := manager.SyncPolicy(ctx, "local-to-s3")
    if err != nil {
        panic(err)
    }

    fmt.Printf("Synced %d objects in %s\n", result.Synced, result.Duration)
}
```

---

## Replication Modes

### Transparent Mode

In transparent mode, data is decrypted from the source and re-encrypted for the destination. This allows for different encryption keys at each end.

**Use when:**
- Source and destination use different encryption keys
- You need to process or transform data during replication
- Migrating between backends with different encryption requirements

**Example:**
```go
policy := common.ReplicationPolicy{
    ID:              "encrypted-migration",
    SourceBackend:   "local",
    DestinationBackend: "s3",
    ReplicationMode: common.ReplicationModeTransparent,
    Encryption: &common.EncryptionPolicy{
        Source: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "source-key-id",
        },
        Destination: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "dest-key-id",
        },
    },
}
```

### Opaque Mode

In opaque mode, encrypted blobs are copied directly without decryption/re-encryption. Backend at-rest encryption still applies, but client-side encryption is bypassed.

**Use when:**
- Source and destination use the same encryption keys
- Maximum performance is required
- Creating encrypted backups
- Archiving encrypted data

**Example:**
```go
policy := common.ReplicationPolicy{
    ID:              "encrypted-backup",
    SourceBackend:   "s3-primary",
    DestinationBackend: "s3-backup",
    ReplicationMode: common.ReplicationModeOpaque,
    Encryption: &common.EncryptionPolicy{
        Backend: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "backend-key",
        },
    },
}
```

---

## Encryption Layers

The replication system supports three independent encryption layers:

### Layer 1: Backend At-Rest Encryption

Storage-layer encryption provided by the backend itself.

- **Cloud backends**: S3 SSE/KMS, GCS CMEK, Azure SSE (configured via backend settings)
- **Local backend**: Configurable via `encryption.backend` in replication policy
- **Default**: No encryption (noop)

**Example (S3 with KMS):**
```go
DestinationSettings: map[string]string{
    "bucket":     "encrypted-bucket",
    "region":     "us-east-1",
    "kms_key_id": "arn:aws:kms:us-east-1:123456789:key/my-key",
}
```

### Layer 2: Source DEK (Data Encryption Key)

Application-layer encryption applied before reading from source.

- Configured via `encryption.source` in replication policy
- Decrypts data after download from source storage
- Only used in transparent mode
- Default: No encryption (noop)

### Layer 3: Destination DEK

Application-layer encryption applied before writing to destination.

- Configured via `encryption.destination` in replication policy
- Encrypts data before upload to destination storage
- Only used in transparent mode
- Default: No encryption (noop)

### Encryption Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Client-Side Destination DEK (Application)         │
│ - Configurable via encryption.destination                   │
│ - Encrypts before upload to destination                     │
│ - Default: noop                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 2: Client-Side Source DEK (Application)              │
│ - Configurable via encryption.source                        │
│ - Decrypts after download from source                       │
│ - Default: noop                                              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Backend At-Rest Encryption (Storage)              │
│ - Cloud: S3 SSE/KMS, GCS CMEK, Azure SSE                   │
│ - Local: Configurable via encryption.backend                │
│ - Default: noop                                              │
└─────────────────────────────────────────────────────────────┘
```

### Complete Encryption Example

```go
policy := common.ReplicationPolicy{
    ID:              "triple-encrypted-backup",
    SourceBackend:   "local",
    SourceSettings:  map[string]string{"path": "/secure/data"},
    DestinationBackend: "s3",
    DestinationSettings: map[string]string{
        "bucket":     "encrypted-backup",
        "region":     "us-east-1",
        "kms_key_id": "arn:aws:kms:us-east-1:123:key/s3-key",
    },
    ReplicationMode: common.ReplicationModeTransparent,
    Encryption: &common.EncryptionPolicy{
        Backend: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "local-disk-key",
        },
        Source: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "app-source-dek",
        },
        Destination: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "app-dest-dek",
        },
    },
}

// Set encryption factories
backendFactory := createCustomFactory("local-disk-key")
sourceFactory := createCustomFactory("app-source-dek")
destFactory := createCustomFactory("app-dest-dek")

manager.SetBackendEncrypterFactory("triple-encrypted-backup", backendFactory)
manager.SetSourceEncrypterFactory("triple-encrypted-backup", sourceFactory)
manager.SetDestinationEncrypterFactory("triple-encrypted-backup", destFactory)
```

---

## Configuration

### Policy Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique identifier for the policy |
| `source_backend` | string | Yes | Source backend type (local, s3, gcs, azure, minio) |
| `source_settings` | map[string]string | Yes | Backend-specific configuration |
| `source_prefix` | string | No | Filter objects by key prefix |
| `destination_backend` | string | Yes | Destination backend type |
| `destination_settings` | map[string]string | Yes | Backend-specific configuration |
| `check_interval` | duration | Yes | How often to check for changes |
| `enabled` | bool | Yes | Whether policy is active |
| `replication_mode` | string | Yes | "transparent" or "opaque" |
| `encryption` | object | No | Encryption configuration |

### Backend Settings Examples

**Local Backend:**
```go
Settings: map[string]string{
    "path": "/data/storage",
}
```

**S3 Backend:**
```go
Settings: map[string]string{
    "bucket":    "my-bucket",
    "region":    "us-east-1",
    "endpoint":  "https://s3.amazonaws.com",  // Optional
    "kms_key_id": "arn:aws:kms:...",          // Optional (SSE-KMS)
}
```

**MinIO Backend:**
```go
Settings: map[string]string{
    "bucket":    "my-bucket",
    "endpoint":  "http://localhost:9000",
    "accessKey": "minioadmin",
    "secretKey": "minioadmin",
    "region":    "us-east-1",
}
```

**GCS Backend:**
```go
Settings: map[string]string{
    "bucket": "my-gcs-bucket",
    "project": "my-project-id",  // Optional
}
```

**Azure Blob Backend:**
```go
Settings: map[string]string{
    "accountName":   "mystorageaccount",
    "accountKey":    "base64encodedkey==",
    "containerName": "mycontainer",
}
```

### YAML Configuration File

```yaml
replication:
  - id: local-to-s3-backup
    source:
      backend: local
      settings:
        path: /data/production
      prefix: important/
    destination:
      backend: s3
      settings:
        bucket: production-backup
        region: us-west-2
        kms_key_id: arn:aws:kms:us-west-2:123456789:key/backup-key
    replication_mode: transparent
    check_interval: 5m
    enabled: true
    encryption:
      backend:
        enabled: true
        provider: custom
        default_key: local-encryption-key
      source:
        enabled: true
        provider: custom
        default_key: app-source-key
      destination:
        enabled: true
        provider: custom
        default_key: app-dest-key

  - id: s3-to-gcs-mirror
    source:
      backend: s3
      settings:
        bucket: primary-bucket
        region: us-east-1
    destination:
      backend: gcs
      settings:
        bucket: mirror-bucket
    replication_mode: transparent
    check_interval: 10m
    enabled: true
```

---

## API Usage

### gRPC API

```go
import (
    "context"

    "google.golang.org/grpc"
    objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
)

func main() {
    // Connect to server
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    client := objstorepb.NewObjectStoreClient(conn)
    ctx := context.Background()

    // Create policy
    policy := &objstorepb.ReplicationPolicy{
        Id:                  "my-policy",
        SourceBackend:       "local",
        SourceSettings:      map[string]string{"path": "/data"},
        DestinationBackend:  "s3",
        DestinationSettings: map[string]string{"bucket": "backup"},
        CheckIntervalSeconds: 300,
        Enabled:             true,
        ReplicationMode:     objstorepb.ReplicationMode_TRANSPARENT,
    }

    // Add policy
    addResp, err := client.AddReplicationPolicy(ctx, &objstorepb.AddReplicationPolicyRequest{
        Policy: policy,
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(addResp.Message)

    // List policies
    listResp, err := client.GetReplicationPolicies(ctx, &objstorepb.GetReplicationPoliciesRequest{})
    if err != nil {
        panic(err)
    }
    for _, p := range listResp.Policies {
        fmt.Printf("Policy: %s (%s -> %s)\n", p.Id, p.SourceBackend, p.DestinationBackend)
    }

    // Trigger sync
    syncResp, err := client.TriggerReplication(ctx, &objstorepb.TriggerReplicationRequest{
        PolicyId: "my-policy",
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Synced %d objects\n", syncResp.Result.Synced)

    // Remove policy
    _, err = client.RemoveReplicationPolicy(ctx, &objstorepb.RemoveReplicationPolicyRequest{
        Id: "my-policy",
    })
    if err != nil {
        panic(err)
    }
}
```

### REST API

```bash
# Add replication policy
curl -X POST http://localhost:8080/replication/policies \
  -H "Content-Type: application/json" \
  -d '{
    "id": "my-policy",
    "source_backend": "local",
    "source_settings": {"path": "/data"},
    "destination_backend": "s3",
    "destination_settings": {"bucket": "backup", "region": "us-east-1"},
    "check_interval": 300000000000,
    "enabled": true,
    "replication_mode": "transparent"
  }'

# List policies
curl http://localhost:8080/replication/policies

# Get specific policy
curl http://localhost:8080/replication/policies/my-policy

# Trigger sync for specific policy
curl -X POST http://localhost:8080/replication/trigger?policy_id=my-policy

# Trigger sync for all policies
curl -X POST http://localhost:8080/replication/trigger

# Remove policy
curl -X DELETE http://localhost:8080/replication/policies/my-policy
```

### CLI Commands

```bash
# Add replication policy
objstore replication add \
  --id=backup-policy \
  --source-backend=local \
  --source-setting=path=/data/production \
  --dest-backend=s3 \
  --dest-setting=bucket=production-backup \
  --dest-setting=region=us-east-1 \
  --prefix=critical/ \
  --interval=5m \
  --mode=transparent \
  --source-dek=source-encryption-key \
  --dest-dek=dest-encryption-key

# List all policies
objstore replication list

# Get specific policy
objstore replication get --id=backup-policy

# Sync specific policy
objstore replication sync --id=backup-policy

# Sync all enabled policies
objstore replication sync --all

# Remove policy
objstore replication remove --id=backup-policy
```

---

## Best Practices

### Performance Optimization

1. **Use Opaque Mode for Same-Key Scenarios**
   ```go
   // When source and destination use same encryption
   ReplicationMode: common.ReplicationModeOpaque
   ```

2. **Set Appropriate Check Intervals**
   ```go
   // Frequent changes: shorter interval
   CheckInterval: 1 * time.Minute

   // Infrequent changes: longer interval
   CheckInterval: 30 * time.Minute
   ```

3. **Use Prefix Filtering**
   ```go
   // Only replicate specific prefixes
   SourcePrefix: "important-data/"
   ```

4. **Enable Incremental Sync (Phase 2)**
   - Use change log for large datasets
   - Reduces metadata scanning overhead

### Security Best Practices

1. **Always Use Encryption for Sensitive Data**
   ```go
   Encryption: &common.EncryptionPolicy{
       Source: &common.EncryptionConfig{Enabled: true},
       Destination: &common.EncryptionConfig{Enabled: true},
   }
   ```

2. **Rotate Encryption Keys Regularly**
   ```go
   // Update encryption keys periodically
   manager.SetSourceEncrypterFactory(policyID, newFactory)
   ```

3. **Use Different Keys for Different Environments**
   ```go
   // Production
   SourceKey: "prod-encryption-key"

   // Staging
   DestinationKey: "staging-encryption-key"
   ```

4. **Enable Backend Encryption for Cloud Storage**
   ```go
   DestinationSettings: map[string]string{
       "kms_key_id": "arn:aws:kms:...",  // S3 SSE-KMS
   }
   ```

### Reliability Best Practices

1. **Monitor Sync Results**
   ```go
   result, err := manager.SyncPolicy(ctx, policyID)
   if result.Failed > 0 {
       // Alert on failures
       alertOps(result.Errors)
   }
   ```

2. **Use Appropriate Timeouts**
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
   defer cancel()
   ```

3. **Test Disaster Recovery**
   ```bash
   # Regularly verify backup integrity
   objstore replication sync --id=dr-policy
   objstore get --backend=backup test-restore-file
   ```

4. **Enable Audit Logging**
   ```go
   auditLog := audit.NewJSONAuditLogger("replication-audit.log")
   ```

### Cost Optimization

1. **Minimize Cross-Region Transfers**
   - Use same-region replication when possible
   - Consider data transfer costs

2. **Use Lifecycle Policies**
   ```go
   // Archive old backups to cheaper storage
   lifecycle.AddPolicy(common.LifecyclePolicy{
       Prefix:      "backups/",
       Action:      "archive",
       Retention:   90 * 24 * time.Hour,
       Destination: glacierBackend,
   })
   ```

3. **Schedule Sync During Off-Peak Hours**
   ```go
   // Lower bandwidth costs during off-peak
   CheckInterval: 6 * time.Hour  // Sync 4x daily
   ```

---

## Technical Details

For detailed technical information about the replication implementation, architecture, and development roadmap, see:

- [File System Watcher Guide](watchers.md) - Real-time change detection

### Key Components

- **ReplicationManager**: Manages policies and orchestrates sync operations
- **ChangeDetector**: Detects changes using metadata comparison
- **Syncer**: Executes object synchronization with encryption support
- **ChangeLog**: Tracks changes for incremental sync (Phase 2)
- **EncrypterFactory**: Provides encryption/decryption capabilities

### Supported Backends

All storage backends support replication:
- Local filesystem
- Amazon S3
- Google Cloud Storage (GCS)
- Azure Blob Storage
- MinIO (S3-compatible)

---

## Examples

### Example 1: Simple Local to S3 Backup

```go
policy := common.ReplicationPolicy{
    ID:                  "daily-backup",
    SourceBackend:       "local",
    SourceSettings:      map[string]string{"path": "/var/app/data"},
    DestinationBackend:  "s3",
    DestinationSettings: map[string]string{
        "bucket": "app-backups",
        "region": "us-east-1",
    },
    CheckInterval:   24 * time.Hour,
    Enabled:         true,
    ReplicationMode: common.ReplicationModeTransparent,
}
```

### Example 2: Multi-Cloud with Encryption

```go
policy := common.ReplicationPolicy{
    ID:             "s3-to-gcs-encrypted",
    SourceBackend:  "s3",
    SourceSettings: map[string]string{
        "bucket": "aws-primary",
        "region": "us-east-1",
    },
    DestinationBackend: "gcs",
    DestinationSettings: map[string]string{
        "bucket": "gcp-backup",
    },
    CheckInterval:   1 * time.Hour,
    Enabled:         true,
    ReplicationMode: common.ReplicationModeTransparent,
    Encryption: &common.EncryptionPolicy{
        Source: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "aws-app-key",
        },
        Destination: &common.EncryptionConfig{
            Enabled:    true,
            Provider:   "custom",
            DefaultKey: "gcp-app-key",
        },
    },
}
```

### Example 3: Disaster Recovery with Prefix Filter

```go
policy := common.ReplicationPolicy{
    ID:                  "dr-critical-data",
    SourceBackend:       "s3",
    SourceSettings:      map[string]string{
        "bucket": "production-data",
        "region": "us-east-1",
    },
    SourcePrefix:        "critical/",  // Only replicate critical data
    DestinationBackend:  "s3",
    DestinationSettings: map[string]string{
        "bucket": "dr-backup",
        "region": "us-west-2",
    },
    CheckInterval:   5 * time.Minute,  // Frequent sync for DR
    Enabled:         true,
    ReplicationMode: common.ReplicationModeOpaque,  // Same keys, use opaque
}
```

---

## Troubleshooting

### Common Issues

**Issue: Sync fails with "policy not found"**
```bash
# Verify policy exists
objstore replication list

# Check policy ID spelling
objstore replication get --id=your-policy-id
```

**Issue: "Encryption factory not set"**
```go
// Ensure factories are set after adding policy
manager.AddPolicy(policy)
manager.SetSourceEncrypterFactory(policyID, sourceFactory)
manager.SetDestinationEncrypterFactory(policyID, destFactory)
```

**Issue: High sync latency**
```go
// Use opaque mode if possible
ReplicationMode: common.ReplicationModeOpaque

// Or reduce check interval
CheckInterval: 30 * time.Second
```

**Issue: Objects not syncing**
```bash
# Check if policy is enabled
objstore replication list

# Manually trigger sync to see errors
objstore replication sync --id=policy-id
```

### Debug Mode

Enable detailed logging:
```go
logger := adapters.NewStdoutLogger()
logger.SetLevel(adapters.LogLevelDebug)
```

### Monitoring Sync Results

```go
result, err := manager.SyncPolicy(ctx, policyID)
if err != nil {
    log.Printf("Sync error: %v", err)
}

log.Printf("Sync statistics:")
log.Printf("  Synced: %d", result.Synced)
log.Printf("  Failed: %d", result.Failed)
log.Printf("  Deleted: %d", result.Deleted)
log.Printf("  Bytes: %d", result.BytesTotal)
log.Printf("  Duration: %s", result.Duration)

for _, errMsg := range result.Errors {
    log.Printf("  Error: %s", errMsg)
}
```

---

## Support

For additional help:
- [Main Documentation](../../README.md)
- [Testing Guide](../testing.md)
- [Architecture Overview](../architecture/README.md)
- [GitHub Issues](https://github.com/jeremyhahn/go-objstore/issues)
