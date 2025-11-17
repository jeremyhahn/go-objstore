# Using the CLI

Practical guide for the objstore command-line tool.

## Installation

Build the CLI:

```bash
go build -o objstore cmd/objstore/main.go
```

Or install:

```bash
go install ./cmd/objstore
```

## Basic Commands

### Put Objects
Upload objects to storage:

```bash
objstore put <key> <file>
```

From stdin:

```bash
echo "content" | objstore put object.txt -
```

### Get Objects
Download objects:

```bash
objstore get <key>
```

Save to file:

```bash
objstore get <key> -o output.txt
```

### Delete Objects
Remove objects:

```bash
objstore delete <key>
```

### List Objects
List all objects:

```bash
objstore list
```

With prefix filter:

```bash
objstore list logs/
```

### Archive Objects
Archive an object to different storage:

```bash
# Archive to local directory (e.g., NFS mount)
objstore archive logs/old.log local --destination-path /mnt/backup

# Archive to AWS Glacier
objstore archive old-data.zip glacier

# Archive to Azure Archive
objstore archive backups/2023.tar azurearchive
```

## Common Workflows

### Backup Files
```bash
# Backup directory
for file in /data/*; do
  objstore put "backup/$(basename $file)" "$file"
done
```

### Restore Files
```bash
# Restore all backups
objstore list --prefix backup/ | while read key; do
  objstore get "$key" -o "/restore/$key"
done
```

### Sync Directory
```bash
# Upload changes
find /data -type f -newer last-sync | while read file; do
  objstore put "data/$file" "$file"
done
```

### Pipeline Integration
```bash
# Process and upload
cat data.txt | process-data | objstore put processed/data.txt -
```

## Configuration

### View Configuration
Check current settings:

```bash
objstore config
objstore config -o json
```

### Override Backend
```bash
objstore --backend s3 --backend-region us-west-2 --backend-bucket mybucket list
```

### Output Formats
```bash
# JSON output
objstore list -o json

# Table format
objstore list -o table

# Text format (default)
objstore list -o text
```

## Advanced Features

### Metadata
Set metadata on upload:

```bash
objstore put file.txt data.txt --content-type text/plain --custom author=user,version=1.0
```

Get metadata only:

```bash
objstore get file.txt --metadata
objstore get file.txt --metadata -o json
```

### Lifecycle Policies
Manage automatic deletion or archiving:

```bash
# Delete logs after 30 days
objstore policy add cleanup-old-logs logs/ 30 delete

# Archive reports after 1 year
objstore policy add archive-reports reports/ 365 archive

# List all policies
objstore policy list

# Remove a policy
objstore policy remove cleanup-old-logs
```

## Scripting

### Error Handling
```bash
if ! objstore get file.txt -o output.txt; then
  echo "Failed to download file"
  exit 1
fi
```

### Check Existence
```bash
if objstore exists file.txt; then
  echo "File exists"
fi
```

### Batch Operations
```bash
# Delete all with prefix
objstore list --prefix old/ | xargs -I {} objstore delete {}
```

## Troubleshooting

### Check Health
Verify backend connectivity:

```bash
objstore health
objstore health -o json
objstore --backend s3 health  # Test S3 connection
```

### Check Configuration
Review current settings:

```bash
objstore config
objstore config -o json
```

### Test Connection
Try listing objects to verify access:

```bash
objstore list
```

## Help and Examples

All commands show examples when you need help:

```bash
# Show help for any command
objstore put --help
objstore policy add --help

# Examples are also shown when you make an error
objstore policy add  # Shows examples automatically
```
