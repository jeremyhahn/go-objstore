# Integration Tests

This directory contains integration tests for the Ruby SDK that test against a running go-objstore server.

## Test Files

### comprehensive_spec.rb

A comprehensive table-driven integration test suite that tests all 19 operations across all 3 protocols (REST, gRPC, QUIC).

**Operations Tested:**
1. `put` - Upload objects
2. `get` - Retrieve objects
3. `delete` - Delete objects
4. `exists?` - Check object existence
5. `list` - List objects with prefix filtering
6. `get_metadata` - Get object metadata
7. `update_metadata` - Update object metadata
8. `archive` - Archive objects (skipped for local backend)
9. `add_policy` - Add lifecycle policies
10. `remove_policy` - Remove lifecycle policies
11. `get_policies` - List all lifecycle policies
12. `apply_policies` - Apply lifecycle policies
13. `add_replication_policy` - Add replication policies (skipped for local backend)
14. `remove_replication_policy` - Remove replication policies (skipped for local backend)
15. `get_replication_policies` - List replication policies (skipped for local backend)
16. `get_replication_policy` - Get specific replication policy (skipped for local backend)
17. `trigger_replication` - Trigger replication sync (skipped for local backend)
18. `get_replication_status` - Get replication status (skipped for local backend)
19. `health` - Health check endpoint

**Protocols Tested:**
- REST (HTTP/1.1) - Fully supported
- gRPC - Partially implemented (requires protobuf stubs)
- QUIC (HTTP/3) - Not supported in Ruby (requires UDP-based HTTP/3)

**Test Features:**
- Table-driven test design using RSpec shared_examples
- Automatic setup and teardown for each operation
- Protocol-specific skipping for unsupported features
- Backend-specific skipping (e.g., archive, replication for local backend)
- Cross-protocol consistency tests
- Response structure validation
- Comprehensive error handling tests
- Automatic cleanup of test data

### integration_spec.rb

Basic integration tests covering core functionality. This is the original integration test file.

## Running the Tests

### Prerequisites

1. Start the go-objstore server(s):
   ```bash
   # From the project root
   make run-rest    # REST server on port 8080
   make run-grpc    # gRPC server on port 50051
   make run-quic    # QUIC server on port 4433
   ```

2. Ensure Ruby dependencies are installed:
   ```bash
   cd api/sdks/ruby
   bundle install
   ```

### Run All Integration Tests

```bash
cd api/sdks/ruby
bundle exec rspec spec/integration/
```

### Run Only Comprehensive Tests

```bash
bundle exec rspec spec/integration/comprehensive_spec.rb
```

### Run Specific Protocol Tests

```bash
# Test only REST protocol
bundle exec rspec spec/integration/comprehensive_spec.rb -e "with REST protocol"

# Test only gRPC protocol
bundle exec rspec spec/integration/comprehensive_spec.rb -e "with GRPC protocol"

# Test only QUIC protocol (will skip most tests)
bundle exec rspec spec/integration/comprehensive_spec.rb -e "with QUIC protocol"
```

### Run Specific Operation Tests

```bash
# Test only the put operation across all protocols
bundle exec rspec spec/integration/comprehensive_spec.rb -e "put operation"

# Test only health checks
bundle exec rspec spec/integration/comprehensive_spec.rb -e "health operation"

# Test only replication operations
bundle exec rspec spec/integration/comprehensive_spec.rb -e "replication"
```

### Run with Verbose Output

```bash
bundle exec rspec spec/integration/comprehensive_spec.rb --format documentation
```

## Environment Variables

Configure the test environment using these variables:

- `OBJSTORE_HOST` - Server hostname (default: localhost)
- `OBJSTORE_REST_PORT` - REST server port (default: 8080)
- `OBJSTORE_GRPC_PORT` - gRPC server port (default: 50051)
- `OBJSTORE_QUIC_PORT` - QUIC server port (default: 4433)
- `OBJSTORE_BACKEND` - Backend type: local, s3, gcs, etc. (default: local)

Example:
```bash
OBJSTORE_HOST=testserver.local \
OBJSTORE_REST_PORT=9090 \
OBJSTORE_BACKEND=s3 \
bundle exec rspec spec/integration/comprehensive_spec.rb
```

## Test Output

The comprehensive test suite will:
- Show which tests are skipped and why
- Display detailed error messages for failures
- Provide response structure validation
- Clean up test data automatically
- Report cross-protocol consistency results

### Expected Skip Reasons

- **QUIC tests**: "Ruby does not support HTTP/3 - QUIC tests skipped"
- **gRPC tests**: "gRPC requires protobuf stubs - partial implementation"
- **Archive operations**: "Archive operations not supported by local backend"
- **Replication operations**: "Replication not supported by local backend"

## Test Data Cleanup

All tests include automatic cleanup:
- Objects created during tests are deleted after test completion
- Lifecycle policies are removed after testing
- Replication policies are removed after testing
- Failed tests may leave orphaned data (check test-* and comprehensive-test-* keys)

To manually clean up test data:
```bash
# List all test keys
curl http://localhost:8080/objects?prefix=comprehensive-test-

# Delete individual test keys if needed
curl -X DELETE http://localhost:8080/objects/{key}
```

## Contributing

When adding new operations to the SDK:

1. Add a new test case to the `OPERATION_TEST_CASES` array
2. Define the operation name, setup, test logic, and cleanup requirements
3. Specify any protocol or backend skip conditions
4. Ensure proper response structure validation
5. Run the comprehensive test suite to verify all protocols

## Troubleshooting

### Connection Refused Errors

If you see connection errors:
```
Faraday::ConnectionFailed: Failed to open TCP connection to localhost:8080
```

Solution: Ensure the server is running on the expected port.

### Timeout Errors

If tests timeout:
- Check server logs for errors
- Increase timeout in client initialization
- Verify network connectivity

### Skipped Tests

All skipped tests will show the reason:
- Check if the protocol is supported
- Verify the backend supports the operation
- Review environment variables

### Test Data Not Cleaning Up

If test data remains after tests:
- Check test output for cleanup errors
- Manually delete remaining test keys
- Review the `cleanup_test_key` helper method

## Coverage

The comprehensive test suite provides:
- 100% operation coverage (all 19 operations)
- 100% protocol coverage (REST, gRPC, QUIC with appropriate skips)
- Response structure validation for all response types
- Error handling validation for common error cases
- Cross-protocol consistency validation
- Proper isolation between tests with unique keys
