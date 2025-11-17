# Lifecycle Policies

Lifecycle policies define rules for automatic deletion or archival of objects based on age or other criteria.

## Overview

All storage backends support lifecycle policies that can:
- **Delete** objects after a certain age
- **Archive** objects to long-term storage
- **Manage retention** periods automatically

## Policy Structure

A lifecycle policy consists of:

```go
type LifecyclePolicy struct {
    ID          string        // Unique policy identifier
    Prefix      string        // Key prefix to match
    Action      string        // "delete" or "archive"
    Destination Storage       // Archive destination (if action is "archive")
    Retention   time.Duration // Age after which action is taken
}
```

## Creating Policies

### Delete Policy

Automatically delete objects older than 30 days:

```go
policy := common.LifecyclePolicy{
    ID:     "delete-old-logs",
    Prefix: "logs/",
    Action: "delete",
    Retention: 30 * 24 * time.Hour,
}

err := storage.AddPolicy(policy)
```

### Archive Policy

Move objects to Glacier after 90 days:

```go
// Create archive destination
glacier, _ := factory.NewArchiver("glacier", map[string]string{
    "region": "us-east-1",
    "vault":  "long-term-archive",
})

// Create policy
policy := common.LifecyclePolicy{
    ID:          "archive-old-data",
    Prefix:      "data/",
    Action:      "archive",
    Destination: glacier,
    Retention:      90 * 24 * time.Hour,
}

err := storage.AddPolicy(policy)
```

## Backend-Specific Behavior

### Local Storage

Local storage supports **two types of lifecycle managers**:

#### Persistent Manager (CLI Default)

The persistent manager saves policies to disk and is **automatically used by the CLI**:
- **Policies survive process restarts**
- Saved as JSON in `.lifecycle-policies.json` in the storage directory
- Automatically used when using the `objstore` CLI tool
- Best for production deployments

**CLI Usage** (uses persistent manager automatically):
```bash
# Add a policy - saved to storage/.lifecycle-policies.json
objstore policy add cleanup tmp/ 7 delete

# List policies - reads from disk
objstore policy list

# Apply policies manually (for cron jobs)
objstore policy apply

# Policies persist across CLI invocations
```

**Cron Job Example:**
```bash
# Run policy enforcement daily at 2 AM
0 2 * * * /usr/local/bin/objstore policy apply
```

**Programmatic Usage**:
```go
local, _ := factory.NewStorage("local", map[string]string{
    "path":                 "/var/data",
    "lifecycleManagerType": "persistent",           // Use persistent manager
    "lifecyclePolicyFile":  ".lifecycle-policies.json", // Optional, this is the default
})

// Add delete policy - will be saved to disk
policy := common.LifecyclePolicy{
    ID:     "cleanup",
    Prefix: "tmp/",
    Action: "delete",
    Retention: 7 * 24 * time.Hour, // 7 days
}
local.AddPolicy(policy)

// After process restart, the policy will be automatically reloaded
```

#### In-Memory Manager

The in-memory manager is available for programmatic use when you don't need persistence:
- Tracks policies in memory
- **Policies are lost on process restart**
- Automatically applies policies on a schedule
- Best for testing and development

```go
local, _ := factory.NewStorage("local", map[string]string{
    "path": "/var/data",
    "lifecycleManagerType": "memory",  // Explicitly use in-memory manager
})

// Add delete policy - stored only in memory
policy := common.LifecyclePolicy{
    ID:     "cleanup",
    Prefix: "tmp/",
    Action: "delete",
    Retention: 7 * 24 * time.Hour, // 7 days
}
local.AddPolicy(policy)
```

**Important Note**: Archive policies with `Destination` cannot be fully persisted, as the `Archiver` interface cannot be serialized. After a restart, you must re-register archive destinations before archive policies will work.

### Amazon S3

S3 policies are **stored in S3 configuration** using native S3 lifecycle rules:
- Policies persist across restarts
- Managed by S3 service
- May take up to 24 hours to apply

```go
s3, _ := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
})

policy := common.LifecyclePolicy{
    ID:     "s3-cleanup",
    Prefix: "old-data/",
    Action: "delete",
    Retention: 60 * 24 * time.Hour,
}
s3.AddPolicy(policy)
```

### Google Cloud Storage

GCS policies use **native GCS lifecycle management**:
- Policies stored in GCS bucket configuration
- Applied automatically by GCS
- May take up to 24 hours

```go
gcs, _ := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
})

policy := common.LifecyclePolicy{
    ID:     "gcs-archive",
    Prefix: "archive/",
    Action: "delete",
    Retention: 365 * 24 * time.Hour,
}
gcs.AddPolicy(policy)
```

### Azure Blob Storage

Azure policies use **blob lifecycle management**:
- Policies configured in Azure Blob service
- Supports tiered storage (Hot, Cool, Archive)
- Applied automatically

```go
azure, _ := factory.NewStorage("azure", map[string]string{
    "accountName":   "myaccount",
    "accountKey":    "key==",
    "containerName": "data",
})

policy := common.LifecyclePolicy{
    ID:     "azure-tiering",
    Prefix: "historical/",
    Action: "archive",
    Retention: 180 * 24 * time.Hour,
}
azure.AddPolicy(policy)
```

## Managing Policies

### Add Policy

```go
err := storage.AddPolicy(policy)
if err != nil {
    if errors.Is(err, common.ErrInvalidPolicy) {
        // Handle invalid policy
    }
}
```

### Remove Policy

```go
err := storage.RemovePolicy("policy-id")
```

### List Policies

```go
policies, err := storage.GetPolicies()
for _, p := range policies {
    fmt.Printf("Policy: %s, Prefix: %s, Action: %s\n",
        p.ID, p.Prefix, p.Action)
}
```

## Archive Destinations

Policies with `action: "archive"` require a destination:

### Local Storage

Archive to a different local directory, mount point, or network filesystem:

```go
// Archive to a different local directory (e.g., NFS mount)
localArchiver, _ := factory.NewArchiver("local", map[string]string{
    "path": "/mnt/nfs/backups",
})

policy := common.LifecyclePolicy{
    ID:          "to-nfs-backup",
    Prefix:      "logs/",
    Action:      "archive",
    Destination: localArchiver,
    Retention:      90 * 24 * time.Hour, // 90 days
}
```

**Use cases for local archiver:**
- Archive from local storage to NFS network drive
- Move files to cheaper/slower storage on a different mount
- Archive to external USB drives or network-attached storage
- Transfer files to backup servers over mounted filesystems

### AWS Glacier

```go
glacier, _ := factory.NewArchiver("glacier", map[string]string{
    "region": "us-east-1",
    "vault":  "archive-vault",
})

policy := common.LifecyclePolicy{
    ID:          "to-glacier",
    Prefix:      "archive/",
    Action:      "archive",
    Destination: glacier,
    Retention:      365 * 24 * time.Hour,
}
```

### Azure Archive Tier

```go
azureArchive, _ := factory.NewArchiver("azure-archive", map[string]string{
    "accountName":   "myaccount",
    "accountKey":    "key==",
    "containerName": "archive",
})

policy := common.LifecyclePolicy{
    ID:          "to-azure-archive",
    Prefix:      "cold/",
    Action:      "archive",
    Destination: azureArchive,
    Retention:      730 * 24 * time.Hour, // 2 years
}
```

## Policy Validation

Policies are validated when added:

```go
// Invalid: missing ID
policy := common.LifecyclePolicy{
    Prefix: "data/",
    Action: "delete",
    Retention: 30 * 24 * time.Hour,
}
err := storage.AddPolicy(policy) // Returns ErrInvalidPolicy

// Invalid: unsupported action
policy := common.LifecyclePolicy{
    ID:     "bad",
    Action: "compress", // Not supported
}
err := storage.AddPolicy(policy) // Returns ErrInvalidPolicy

// Invalid: archive without destination
policy := common.LifecyclePolicy{
    ID:     "bad-archive",
    Action: "archive",
    // Missing Destination
}
err := storage.AddPolicy(policy) // Returns ErrInvalidPolicy
```

## Best Practices

1. **Use specific prefixes**: Narrow scopes reduce unintended deletions
2. **Test policies**: Verify on test data before production
3. **Monitor execution**: Track what gets deleted/archived
4. **Document policies**: Keep records of what policies do
5. **Use appropriate MaxAge**: Balance cost vs. data retention needs
6. **Archive before delete**: Consider archiving valuable data first

## Examples

### Tiered Storage Strategy

```go
// Hot data: keep for 30 days
s3, _ := factory.NewStorage("s3", ...)

// Archive to Glacier after 30 days
glacier, _ := factory.NewArchiver("glacier", ...)
archivePolicy := common.LifecyclePolicy{
    ID:          "to-glacier",
    Prefix:      "data/",
    Action:      "archive",
    Destination: glacier,
    Retention:      30 * 24 * time.Hour,
}
s3.AddPolicy(archivePolicy)

// Delete from S3 after archiving (35 days total)
deletePolicy := common.LifecyclePolicy{
    ID:     "cleanup-archived",
    Prefix: "data/",
    Action: "delete",
    Retention: 35 * 24 * time.Hour,
}
s3.AddPolicy(deletePolicy)
```

### Log Rotation

```go
local, _ := factory.NewStorage("local", ...)

// Delete logs older than 7 days
policy := common.LifecyclePolicy{
    ID:     "log-rotation",
    Prefix: "logs/",
    Action: "delete",
    Retention: 7 * 24 * time.Hour,
}
local.AddPolicy(policy)
```

### Compliance Retention

```go
// Keep for 7 years for compliance
s3, _ := factory.NewStorage("s3", ...)

policy := common.LifecyclePolicy{
    ID:     "compliance-retention",
    Prefix: "compliance/",
    Action: "delete",
    Retention: 7 * 365 * 24 * time.Hour,
}
s3.AddPolicy(policy)
```

## Advanced: Custom Lifecycle Storage

For advanced use cases, you can create a persistent lifecycle manager with a custom filesystem implementation. This allows you to store lifecycle policies anywhere that adheres to the `common.FileSystem` interface.

### Using StorageFS

The most flexible approach is to use the storagefs abstraction, which works with any storage backend:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/storagefs"
)

// Create storage backend for lifecycle policies (can be S3, GCS, Azure, etc.)
policyStorage, _ := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-lifecycle-policies",
})

// Wrap with storagefs
fs := storagefs.New(policyStorage)

// Create filesystem adapter for lifecycle manager
fsAdapter := common.NewFileSystemAdapter(fs)

// Create persistent lifecycle manager
lifecycleManager, _ := common.NewPersistentLifecycleManager(fsAdapter, "policies.json")

// Now you can use this manager with any storage backend
// This allows policies to be stored in S3 while data is stored locally, for example
```

### Custom FileSystem Implementation

You can also implement your own `common.FileSystem` interface:

```go
type MyCustomFileSystem struct {
    // Your implementation
}

func (fs *MyCustomFileSystem) OpenFile(name string, flag int, perm os.FileMode) (common.LifecycleFile, error) {
    // Your custom implementation
}

func (fs *MyCustomFileSystem) Remove(name string) error {
    // Your custom implementation
}

// Create lifecycle manager with custom filesystem
customFS := &MyCustomFileSystem{}
lifecycleManager, _ := common.NewPersistentLifecycleManager(customFS, "policies.json")
```

This allows you to:
- Store policies in a database
- Encrypt policy files
- Use network filesystems
- Implement custom versioning
- Add audit logging

## Implementation Notes

- **CLI (local backend)**: Automatically uses persistent lifecycle manager, policies saved to `.lifecycle-policies.json`
- **Local backend (programmatic)**: Defaults to in-memory for backwards compatibility, use `lifecycleManagerType: "persistent"` for disk persistence
- **Cloud backends**: Policies persisted in cloud provider configuration
- **Archive operations**: Atomic - object copied then original deleted
- **Error handling**: Failed policy actions logged but don't stop execution
- **Custom implementations**: Users can provide their own FileSystem implementation for persistent lifecycle storage

## See Also

- [Storage Backends](../backends/README.md)
- [StorageFS Documentation](../storagefs/README.md)
