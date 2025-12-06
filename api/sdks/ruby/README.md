# ObjectStore Ruby SDK

A comprehensive Ruby SDK for go-objstore supporting REST, gRPC, and QUIC/HTTP3 protocols.

## Features

- Support for multiple protocols: REST, gRPC, and QUIC/HTTP3
- Unified client interface with runtime protocol switching
- **Streaming support for large files** - Memory-efficient uploads and downloads
- Complete API coverage for all go-objstore operations
- Comprehensive error handling with input validation
- **Full YARD documentation** - Complete API reference with examples
- Full test coverage (95%+)
- Docker-based integration tests
- Type-safe models for requests and responses

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'objstore'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install objstore
```

## Quick Start

### Basic Usage

```ruby
require 'objstore'

# Create a client (defaults to REST protocol)
client = ObjectStore::Client.new

# Upload an object
response = client.put("my-file.txt", "Hello, World!")
puts "Upload successful: #{response.success?}"
puts "ETag: #{response.etag}"

# Download an object
response = client.get("my-file.txt")
puts "Content: #{response.data}"
puts "Size: #{response.metadata.size} bytes"

# Check if object exists
if client.exists?("my-file.txt")
  puts "Object exists!"
end

# List objects
list = client.list(prefix: "my-")
list.objects.each do |obj|
  puts "  #{obj.key} - #{obj.metadata.size} bytes"
end

# Delete an object
response = client.delete("my-file.txt")
puts "Deleted: #{response.success?}"
```

### Protocol Selection

```ruby
# REST (default)
client = ObjectStore::Client.new(protocol: :rest, port: 8080)

# gRPC
client = ObjectStore::Client.new(protocol: :grpc, port: 50051)

# QUIC/HTTP3 (defaults to port 8443 with SSL)
client = ObjectStore::Client.new(protocol: :quic)

# Switch protocols at runtime
client.switch_protocol(:grpc)
```

### Advanced Configuration

```ruby
client = ObjectStore::Client.new(
  protocol: :rest,
  host: "objstore.example.com",
  port: 8080,
  use_ssl: true,
  timeout: 60
)
```

## API Reference

### Streaming Operations (New!)

For large files, use streaming operations to minimize memory usage:

#### Streaming Upload

```ruby
# Upload from file
File.open("large_video.mp4", "rb") do |file|
  response = client.put_stream(
    "videos/large_video.mp4",
    file,
    metadata: ObjectStore::Models::Metadata.new(content_type: "video/mp4")
  )
  puts "Uploaded with ETag: #{response.etag}"
end

# Upload from StringIO
require 'stringio'
io = StringIO.new("Large text content...")
client.put_stream("documents/large.txt", io)

# Custom chunk size for reading
File.open("huge_file.bin", "rb") do |file|
  client.put_stream("huge_file.bin", file, chunk_size: 65536)  # 64 KB chunks
end
```

#### Streaming Download

```ruby
# Download to file
File.open("output.mp4", "wb") do |file|
  metadata = client.get_stream("videos/large_video.mp4") do |chunk|
    file.write(chunk)
  end
  puts "Downloaded #{metadata.size} bytes"
end

# Process chunks without saving to disk
total_size = 0
client.get_stream("videos/large_video.mp4") do |chunk|
  total_size += chunk.bytesize
  # Process chunk (e.g., compute hash, analyze content, etc.)
end
puts "Total size: #{total_size} bytes"

# Progress tracking
downloaded = 0
expected_size = client.get_metadata("videos/large_video.mp4").metadata.size

client.get_stream("videos/large_video.mp4") do |chunk|
  downloaded += chunk.bytesize
  progress = (downloaded.to_f / expected_size * 100).to_i
  puts "Progress: #{progress}%"
end
```

### Object Operations

#### Put Object

```ruby
# Simple upload
client.put("documents/report.pdf", pdf_data)

# Upload with metadata
metadata = ObjectStore::Models::Metadata.new(
  content_type: "application/pdf",
  custom: {
    "author" => "John Doe",
    "department" => "Engineering",
    "version" => "1.0"
  }
)

response = client.put("documents/report.pdf", pdf_data, metadata)
```

#### Get Object

```ruby
response = client.get("documents/report.pdf")
File.write("downloaded.pdf", response.data)

puts "Content-Type: #{response.metadata.content_type}"
puts "Size: #{response.metadata.size} bytes"
puts "ETag: #{response.metadata.etag}"
```

#### Delete Object

```ruby
response = client.delete("documents/report.pdf")
puts "Deleted: #{response.success?}"
```

#### List Objects

```ruby
# List all objects
list = client.list

# List with prefix
list = client.list(prefix: "documents/")

# List with delimiter (directory-like structure)
list = client.list(prefix: "documents/", delimiter: "/")

# Pagination
list = client.list(max_results: 100, continue_from: next_token)

# Process results
list.objects.each do |obj|
  puts "#{obj.key}: #{obj.metadata.size} bytes"
end

# Check for more results
if list.truncated
  next_page = client.list(continue_from: list.next_token)
end
```

#### Check Existence

```ruby
if client.exists?("documents/report.pdf")
  puts "File exists"
else
  puts "File not found"
end
```

### Metadata Operations

#### Get Metadata

```ruby
response = client.get_metadata("documents/report.pdf")

if response.success?
  metadata = response.metadata
  puts "Content-Type: #{metadata.content_type}"
  puts "Size: #{metadata.size}"
  puts "Custom metadata:"
  metadata.custom.each do |key, value|
    puts "  #{key}: #{value}"
  end
end
```

#### Update Metadata

```ruby
metadata = ObjectStore::Models::Metadata.new(
  content_type: "application/pdf",
  custom: {
    "author" => "Jane Smith",
    "version" => "2.0"
  }
)

response = client.update_metadata("documents/report.pdf", metadata)
puts "Updated: #{response.success?}"
```

### Health Check

```ruby
response = client.health

if response.healthy?
  puts "Server is healthy"
  puts "Status: #{response.status}"
else
  puts "Server is not healthy"
end

# Check specific service
response = client.health(service: "storage")
```

### Archive Operations

```ruby
response = client.archive(
  "old-documents/archive.zip",
  destination_type: "glacier",
  destination_settings: {
    "region" => "us-east-1",
    "vault" => "my-vault"
  }
)

puts "Archived: #{response.success?}"
```

### Lifecycle Policies

#### Add Lifecycle Policy

```ruby
policy = ObjectStore::Models::LifecyclePolicy.new(
  id: "delete-old-logs",
  prefix: "logs/",
  retention_seconds: 30 * 24 * 60 * 60, # 30 days
  action: "delete"
)

response = client.add_policy(policy)
puts "Policy added: #{response[:success]}"
```

#### Archive Policy

```ruby
policy = ObjectStore::Models::LifecyclePolicy.new(
  id: "archive-old-data",
  prefix: "data/",
  retention_seconds: 90 * 24 * 60 * 60, # 90 days
  action: "archive",
  destination_type: "glacier",
  destination_settings: {
    "region" => "us-west-2"
  }
)

client.add_policy(policy)
```

#### Get Policies

```ruby
response = client.get_policies

response[:policies].each do |policy|
  puts "Policy: #{policy.id}"
  puts "  Prefix: #{policy.prefix}"
  puts "  Retention: #{policy.retention_seconds} seconds"
  puts "  Action: #{policy.action}"
end

# Get policies for specific prefix
response = client.get_policies(prefix: "logs/")
```

#### Remove Policy

```ruby
response = client.remove_policy("delete-old-logs")
puts "Removed: #{response[:success]}"
```

#### Apply Policies

```ruby
response = client.apply_policies

puts "Applied #{response[:policies_count]} policies"
puts "Processed #{response[:objects_processed]} objects"
```

### Replication Policies

#### Add Replication Policy

```ruby
policy = ObjectStore::Models::ReplicationPolicy.new(
  id: "s3-to-gcs-replication",
  source_backend: "s3",
  source_settings: {
    "bucket" => "my-s3-bucket",
    "region" => "us-east-1"
  },
  source_prefix: "data/",
  destination_backend: "gcs",
  destination_settings: {
    "bucket" => "my-gcs-bucket",
    "project" => "my-project"
  },
  check_interval_seconds: 3600, # Check every hour
  enabled: true,
  replication_mode: "TRANSPARENT"
)

response = client.add_replication_policy(policy)
puts "Replication policy added: #{response[:success]}"
```

#### Get Replication Policies

```ruby
response = client.get_replication_policies

response[:policies].each do |policy|
  puts "Policy: #{policy.id}"
  puts "  Source: #{policy.source_backend}"
  puts "  Destination: #{policy.destination_backend}"
  puts "  Enabled: #{policy.enabled}"
end
```

#### Get Specific Replication Policy

```ruby
response = client.get_replication_policy("s3-to-gcs-replication")
policy = response[:policy]

puts "Policy: #{policy.id}"
puts "Check interval: #{policy.check_interval_seconds}s"
```

#### Trigger Replication

```ruby
# Trigger all policies
response = client.trigger_replication

# Trigger specific policy
response = client.trigger_replication(policy_id: "s3-to-gcs-replication")

# Parallel replication with custom workers
response = client.trigger_replication(
  policy_id: "s3-to-gcs-replication",
  parallel: true,
  worker_count: 8
)

# Check results
result = response[:result]
puts "Synced: #{result[:synced]}"
puts "Deleted: #{result[:deleted]}"
puts "Failed: #{result[:failed]}"
puts "Bytes: #{result[:bytes_total]}"
puts "Duration: #{result[:duration_ms]}ms"
```

#### Get Replication Status

```ruby
response = client.get_replication_status("s3-to-gcs-replication")
status = response[:status]

puts "Total objects synced: #{status.total_objects_synced}"
puts "Total objects deleted: #{status.total_objects_deleted}"
puts "Total bytes synced: #{status.total_bytes_synced}"
puts "Total errors: #{status.total_errors}"
puts "Last sync: #{status.last_sync_time}"
puts "Average duration: #{status.average_sync_duration_ms}ms"
puts "Sync count: #{status.sync_count}"
```

#### Remove Replication Policy

```ruby
response = client.remove_replication_policy("s3-to-gcs-replication")
puts "Removed: #{response[:success]}"
```

## Error Handling

The SDK provides comprehensive error handling with input validation and descriptive error messages.

### Exception Types

- `ObjectStore::Error` - Base exception class
- `ObjectStore::NotFoundError` - Resource not found (404)
- `ObjectStore::ValidationError` - Server-side validation error (400)
- `ObjectStore::TimeoutError` - Request timeout
- `ObjectStore::ServerError` - Server error (5xx)
- `ObjectStore::ConnectionError` - Network connection error
- `ArgumentError` - Client-side input validation error (Ruby standard)

### Basic Error Handling

```ruby
require 'objstore'

client = ObjectStore::Client.new

begin
  client.get("non-existent-file.txt")
rescue ObjectStore::NotFoundError => e
  puts "File not found: #{e.message}"
rescue ObjectStore::ValidationError => e
  puts "Validation error: #{e.message}"
rescue ObjectStore::TimeoutError => e
  puts "Request timed out: #{e.message}"
rescue ObjectStore::ServerError => e
  puts "Server error: #{e.message}"
rescue ObjectStore::ConnectionError => e
  puts "Connection error: #{e.message}"
rescue ObjectStore::Error => e
  puts "General error: #{e.message}"
end
```

### Input Validation (New!)

The SDK now validates inputs before making API calls:

```ruby
# Invalid key - raises ArgumentError
begin
  client.get("")  # Empty key
rescue ArgumentError => e
  puts e.message  # "Key must be a non-empty string"
end

begin
  client.get(nil)  # Nil key
rescue ArgumentError => e
  puts e.message  # "Key must be a non-empty string"
end

# Invalid data - raises ArgumentError
begin
  client.put("file.txt", nil)
rescue ArgumentError => e
  puts e.message  # "Data cannot be nil"
end

# Invalid IO - raises ArgumentError
begin
  client.put_stream("file.txt", "not an IO")
rescue ArgumentError => e
  puts e.message  # "IO object must respond to :read"
end

# Missing block - raises ArgumentError
begin
  client.get_stream("file.txt")  # No block provided
rescue ArgumentError => e
  puts e.message  # "Block required for streaming"
end

# Invalid policy ID - raises ArgumentError
begin
  client.remove_policy("")
rescue ArgumentError => e
  puts e.message  # "Policy ID must be a non-empty string"
end
```

### Best Practices

```ruby
# Validate user input before calling SDK methods
def safe_get_object(client, key)
  return nil if key.nil? || key.strip.empty?

  begin
    client.get(key)
  rescue ObjectStore::NotFoundError
    nil
  rescue ObjectStore::Error => e
    logger.error("Failed to get object: #{e.message}")
    nil
  end
end

# Use specific error handling for different scenarios
def upload_with_retry(client, key, data, max_retries: 3)
  retries = 0

  begin
    client.put(key, data)
  rescue ObjectStore::TimeoutError, ObjectStore::ConnectionError => e
    retries += 1
    if retries < max_retries
      sleep(2 ** retries)  # Exponential backoff
      retry
    else
      raise
    end
  rescue ObjectStore::ValidationError => e
    # Don't retry validation errors
    logger.error("Validation failed: #{e.message}")
    raise
  end
end
```

## Development

### Setup

```bash
# Install dependencies
make install

# Or manually
bundle install
```

### Running Tests

```bash
# Run unit tests
make test

# Run tests with coverage
make coverage

# Run integration tests (requires Docker)
make integration-test

# Run all tests
make test-all

# Run linter
make lint

# Auto-fix linting issues
make format
```

### Integration Tests with Docker

The integration tests use Docker Compose to start go-objstore servers with all three protocols:

```bash
# Start Docker containers
make docker-up

# Run integration tests
make integration-test

# Stop containers
make docker-down

# View logs
make docker-logs
```

### Building the Gem

```bash
make build
```

### Interactive Console

```bash
make console
```

## Documentation

### YARD Documentation (New!)

The SDK now includes comprehensive YARD documentation for all public methods:

```bash
# Generate documentation
yard doc

# Start documentation server
yard server

# Then open http://localhost:8808 in your browser
```

Documentation includes:
- Method signatures with parameter types
- Return value descriptions
- Possible exceptions
- Usage examples
- Cross-references between related methods

### API Reference

See the inline YARD documentation in the source code for detailed API reference.

## Test Coverage

The SDK maintains 95%+ code coverage across all components:

- Models: 100%
- REST Client: 95%+
- gRPC Client: 95%+
- QUIC Client: 95%+
- Unified Client: 100%
- Streaming: 95%+
- Validation: 100%

Run `make coverage` to generate a detailed coverage report.

## Requirements

- Ruby >= 2.7.0
- For integration tests: Docker and Docker Compose

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Ensure tests pass (`make test-all`)
5. Ensure linting passes (`make lint`)
6. Commit your changes (`git commit -am 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

AGPL-3.0 - see LICENSE file for details

## Support

For issues and questions:
- GitHub Issues: https://github.com/jeremyhahn/go-objstore/issues
- Documentation: https://github.com/jeremyhahn/go-objstore

## Changelog

### 0.1.0 (2025-11-23)

- Initial release
- Support for REST, gRPC, and QUIC/HTTP3 protocols
- Complete API coverage
- Comprehensive test suite
- Docker-based integration tests
