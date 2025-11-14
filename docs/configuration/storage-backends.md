# Storage Backend Configuration

Configuration reference for all supported storage backends.

## Local Filesystem

**Backend Type**: `local`

### Required Parameters
- `path` - Directory path for object storage

### Optional Parameters
- `create_if_missing` - Create directory if it doesn't exist (default: true)
- `permissions` - Directory permissions in octal (default: 0755)

### Credentials
No credentials required. Uses filesystem permissions for access control.

### Example Configuration
```yaml
backend: local
config:
  path: /var/lib/objstore/data
  create_if_missing: true
  permissions: 0750
```

## Amazon S3

**Backend Type**: `s3`

### Required Parameters
- `region` - AWS region (e.g., us-east-1)
- `bucket` - S3 bucket name

### Optional Parameters
- `endpoint` - Custom endpoint URL (for S3-compatible services)
- `disable_ssl` - Disable SSL/TLS (default: false)
- `force_path_style` - Use path-style addressing (default: false)
- `max_retries` - Maximum retry attempts (default: 3)
- `timeout` - Request timeout in seconds (default: 30)

### Credentials
Uses AWS SDK credential chain in order:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. EC2 instance profile (IAM role)
4. ECS task role
5. Web identity token

### Example Configuration
```yaml
backend: s3
config:
  region: us-east-1
  bucket: my-application-data
  max_retries: 5
  timeout: 60
```

### S3-Compatible Services
For MinIO or other S3-compatible services:
```yaml
backend: s3
config:
  region: us-east-1
  bucket: data
  endpoint: https://minio.example.com:9000
  force_path_style: true
```

## Google Cloud Storage

**Backend Type**: `gcs`

### Required Parameters
- `bucket` - GCS bucket name

### Optional Parameters
- `project_id` - GCP project ID (uses default if not specified)
- `timeout` - Request timeout in seconds (default: 30)
- `retry_max_attempts` - Maximum retry attempts (default: 3)

### Credentials
Uses Google Application Default Credentials in order:
1. Environment variable `GOOGLE_APPLICATION_CREDENTIALS` pointing to service account key
2. Default service account on GCE/GKE
3. gcloud CLI credentials

### Example Configuration
```yaml
backend: gcs
config:
  bucket: my-gcs-bucket
  project_id: my-project-123
  timeout: 60
```

## Azure Blob Storage

**Backend Type**: `azure`

### Required Parameters
- `accountName` - Azure storage account name
- `containerName` - Blob container name

### Credential Parameters (one required)
- `accountKey` - Storage account access key
- `sasToken` - Shared access signature token
- `connectionString` - Complete connection string

### Optional Parameters
- `endpoint` - Custom endpoint (for Azurite or custom domains)
- `timeout` - Request timeout in seconds (default: 30)
- `max_retries` - Maximum retry attempts (default: 3)

### Credentials
Multiple authentication methods:
1. Account key (shared key)
2. SAS token (scoped access)
3. Connection string (includes key)
4. Azure AD (managed identity, requires additional setup)

### Example Configuration
```yaml
backend: azure
config:
  accountName: mystorageaccount
  accountKey: base64_encoded_key==
  containerName: application-data
  max_retries: 5
```

## MinIO

**Backend Type**: `minio` or use `s3` with custom endpoint

### Required Parameters
- `endpoint` - MinIO server endpoint
- `accessKey` - MinIO access key
- `secretKey` - MinIO secret key
- `bucket` - Bucket name
- `useSSL` - Use SSL/TLS (true/false)

### Optional Parameters
- `region` - Region (default: us-east-1)

### Example Configuration
```yaml
backend: minio
config:
  endpoint: localhost:9000
  accessKey: minioadmin
  secretKey: minioadmin
  bucket: test-bucket
  useSSL: false
```

## AWS Glacier

**Backend Type**: `glacier`

Archive-only backend for long-term storage.

### Required Parameters
- `region` - AWS region
- `vault` - Glacier vault name

### Optional Parameters
- `timeout` - Request timeout in seconds (default: 300)

### Credentials
Uses AWS SDK credential chain (same as S3).

### Example Configuration
```yaml
backend: glacier
config:
  region: us-east-1
  vault: long-term-archives
  timeout: 600
```

### Important Notes
- Write-only from application perspective
- Retrieval requires restore request (hours to days)
- Minimum storage duration charges apply
- Use as lifecycle policy destination only

## Azure Archive

**Backend Type**: `azure-archive`

Archive-only backend using Azure Archive tier.

### Required Parameters
- `accountName` - Azure storage account name
- `accountKey` - Storage account access key
- `containerName` - Container name

### Optional Parameters
- `timeout` - Request timeout in seconds (default: 300)

### Example Configuration
```yaml
backend: azure-archive
config:
  accountName: archiveaccount
  accountKey: base64_encoded_key==
  containerName: archives
```

### Important Notes
- Objects written to Archive tier
- Retrieval requires rehydration (hours)
- Minimum 180-day storage duration
- Use as lifecycle policy destination only

## Backend Selection Guide

### Development
Use `local` backend for:
- Fast iteration
- No cloud costs
- Simple setup
- Testing

### Production - Hot Data
Use `s3`, `gcs`, or `azure` for:
- Frequently accessed data
- Low-latency requirements
- Standard availability SLAs
- Feature-rich APIs

### Production - Archival
Use `glacier` or `azure-archive` for:
- Long-term retention
- Infrequently accessed data
- Cost optimization
- Compliance requirements

### On-Premises
Use `minio` or `local` for:
- Data sovereignty requirements
- Existing infrastructure
- Air-gapped environments
- Cost control

## Multi-Backend Configuration

Applications can use multiple backends simultaneously:

```yaml
backends:
  primary:
    type: s3
    config:
      region: us-east-1
      bucket: hot-data

  archive:
    type: glacier
    config:
      region: us-west-2
      vault: cold-storage

  local_cache:
    type: local
    config:
      path: /var/cache/objstore
```

## Credential Management

### Environment Variables
Never hardcode credentials. Use environment variables:

```yaml
backend: s3
config:
  region: ${AWS_REGION}
  bucket: ${S3_BUCKET}
  # Credentials from AWS SDK credential chain
```

### Cloud-Native IAM
Prefer cloud provider IAM roles:
- AWS: EC2/ECS/EKS IAM roles
- GCP: GCE/GKE service accounts
- Azure: Managed identities

### Secrets Managers
Use secret management services:
- AWS Secrets Manager
- GCP Secret Manager
- Azure Key Vault
- HashiCorp Vault

## Connection Pooling

All backends support connection pooling. Configure through:

```yaml
backend: s3
config:
  # ... other config ...
  connection_pool:
    max_idle_conns: 100
    max_idle_conns_per_host: 10
    idle_conn_timeout: 90
```

## Timeout Configuration

Configure timeouts for reliability:

```yaml
backend: s3
config:
  # ... other config ...
  timeout: 30  # Request timeout in seconds
  max_retries: 3  # Retry attempts
  retry_wait_min: 1  # Minimum retry backoff (seconds)
  retry_wait_max: 30  # Maximum retry backoff (seconds)
```

## Validation

Backend configuration is validated on initialization:
- Required parameters checked
- Credential validity tested
- Connectivity verified (optional)
- Bucket/container existence confirmed (optional)

Failed validation returns descriptive errors with resolution steps.
