# Changelog

All notable changes to the Ruby SDK will be documented in this file.

## [Unreleased]

### Added

#### Streaming Support
- **`put_stream(key, io, metadata: nil, chunk_size: 8192)`** - Upload objects from IO streams
  - Accepts any IO object (File, StringIO, or custom objects with `#read` method)
  - Configurable chunk size for reading
  - Memory-efficient for large files
  - Available in all client implementations (REST, gRPC, QUIC)

- **`get_stream(key, chunk_size: 8192) { |chunk| }`** - Download objects in chunks
  - Yields chunks via block for processing
  - Returns metadata after streaming
  - Memory-efficient for large downloads
  - Supports progress tracking
  - Available in all client implementations (REST, gRPC, QUIC)

#### YARD Documentation
- Comprehensive YARD documentation for all public methods
- Added `@param` tags with types for all parameters
- Added `@return` tags describing return values
- Added `@raise` tags documenting possible exceptions
- Added `@example` usage examples for key methods
- Class-level documentation with usage examples
- Documentation for Models and error classes

#### Input Validation
- **Key validation** - All operations requiring a key now validate:
  - Key must not be nil
  - Key must not be empty string
  - Key must not be whitespace-only
  - Raises `ArgumentError` with descriptive message

- **Data validation** - Upload operations validate:
  - Data must not be nil
  - Raises `ArgumentError` with descriptive message

- **IO validation** - Streaming operations validate:
  - IO object must respond to `:read` method
  - Raises `ArgumentError` if not a valid IO object

- **Policy ID validation** - Policy operations validate:
  - Policy ID must not be nil
  - Policy ID must not be empty string
  - Policy ID must not be whitespace-only
  - Raises `ArgumentError` with descriptive message

- **Block validation** - Streaming downloads require:
  - Block must be provided to `get_stream`
  - Raises `ArgumentError` if block missing

### Changed

#### QUIC Client Port Default
- **BREAKING**: Changed default QUIC port from `8080` to `8443`
  - Aligns with standard QUIC/HTTP3 port convention
  - More secure by default (assumes TLS)
  - Override by passing `port:` parameter if needed

#### QUIC Client SSL Default
- Changed default `use_ssl` from `false` to `true` for QUIC client
  - QUIC typically runs over TLS by default
  - More secure default configuration

### Fixed
- Improved error messages for validation failures
- Better handling of edge cases in streaming operations
- Consistent metadata handling across all protocols

## Migration Guide

### For Streaming Support

Before:
```ruby
# Reading entire file into memory
data = File.read("large_file.bin")
client.put("large_file.bin", data)

# Downloading entire file into memory
response = client.get("large_file.bin")
File.write("output.bin", response.data)
```

After:
```ruby
# Streaming upload - memory efficient
File.open("large_file.bin", "rb") do |file|
  client.put_stream("large_file.bin", file)
end

# Streaming download - memory efficient
File.open("output.bin", "wb") do |file|
  client.get_stream("large_file.bin") { |chunk| file.write(chunk) }
end
```

### For QUIC Port Change

Before:
```ruby
# Used port 8080 by default
client = ObjectStore::Client.new(protocol: :quic)
```

After:
```ruby
# Now uses port 8443 by default
client = ObjectStore::Client.new(protocol: :quic)

# To use the old port:
client = ObjectStore::Client.new(protocol: :quic, port: 8080)
```

### For Input Validation

No code changes required, but be aware:

```ruby
# These will now raise ArgumentError
client.get("")        # Empty key
client.get(nil)       # Nil key
client.put("key", nil)  # Nil data
client.put_stream("key", "not an IO")  # Invalid IO

# Use proper validation in your code
begin
  client.get(user_provided_key)
rescue ArgumentError => e
  puts "Invalid input: #{e.message}"
end
```

## Testing

New test suites added:

- `spec/unit/streaming_spec.rb` - Tests for streaming functionality
- `spec/unit/validation_spec.rb` - Tests for input validation

Run tests:
```bash
bundle exec rspec spec/unit/streaming_spec.rb
bundle exec rspec spec/unit/validation_spec.rb
```

## Examples

New example added:
- `examples/streaming_example.rb` - Comprehensive streaming usage examples

Run example:
```bash
ruby examples/streaming_example.rb
```

## Documentation

Generate YARD documentation:
```bash
yard doc
```

View documentation:
```bash
yard server
```

Then open http://localhost:8808 in your browser.
