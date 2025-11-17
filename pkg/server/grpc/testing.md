# gRPC Package Tests

## Test Files

This package contains the following test files:

- `server_test.go` - Core server tests and mock helpers
- `handlers_test.go` - gRPC handler tests
- `interceptors_test.go` - Interceptor tests + mockServerStream
- `options_test.go` - Options tests + generateTestCert helper
- `coverage_test.go` - Additional coverage tests

## Important: Test File Dependencies

**All test files in this package share the same namespace.** Test helper functions and types defined in one file are accessible to all other test files when compiled as a package.

### Shared Test Helpers

| Helper | Defined In | Used In |
|--------|-----------|---------|
| `mockStorage` | server_test.go:20 | All test files |
| `newMockStorage()` | server_test.go:26 | All test files |
| `errorMockStorage` | server_test.go:804 | coverage_test.go |
| `errorPutMockStorage` | server_test.go:816 | coverage_test.go |
| `mockServerStream` | interceptors_test.go:182 | coverage_test.go |
| `generateTestCert()` | options_test.go:200 | coverage_test.go |

## IDE Configuration

### If Your IDE Shows Errors in coverage_test.go

This is a **FALSE POSITIVE**. Some IDEs analyze Go files individually and don't understand that test files share a namespace when compiled together.

**The code compiles correctly:**
```bash
$ go test -c ./pkg/server/grpc
# Compiles successfully (29MB binary)

$ go test ./pkg/server/grpc
ok  	github.com/jeremyhahn/go-objstore/pkg/server/grpc	3.976s
coverage: 96.8% of statements
```

### Solutions

#### Option 1: VSCode
Configuration files have been created:
- `.vscode/settings.json` - VSCode Go settings
- `gopls.yaml` - Go language server configuration

**Steps:**
1. Restart VSCode
2. Run command: `Go: Restart Language Server`
3. The errors should disappear

#### Option 2: GoLand/IntelliJ
1. Right-click on `pkg/server/grpc`
2. Select "Invalidate Caches / Restart"
3. Choose "Invalidate and Restart"

#### Option 3: Command Line Verification
Run these commands to verify everything works:

```bash
# Compile tests
go test -c ./pkg/server/grpc

# Run all tests
go test ./pkg/server/grpc -v

# Run specific test from coverage_test.go
go test ./pkg/server/grpc -run TestWithRateLimit -v

# Check coverage
go test ./pkg/server/grpc -cover
```

## Test Statistics

- **Total Tests:** 106
- **Coverage:** 96.8%
- **All Tests:** PASS

### Test Distribution
- server_test.go: ~40 tests
- handlers_test.go: ~30 tests
- interceptors_test.go: ~15 tests
- options_test.go: ~10 tests
- **coverage_test.go: ~46 tests**

## Running Tests

```bash
# Run all tests
go test ./pkg/server/grpc

# Run with verbose output
go test ./pkg/server/grpc -v

# Run with coverage
go test ./pkg/server/grpc -cover

# Run specific test
go test ./pkg/server/grpc -run TestWithRateLimit

# Generate coverage report
go test ./pkg/server/grpc -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Troubleshooting

### "undefined: mockStorage" error in IDE

**This is a false positive.** The identifier is defined in `server_test.go` and is accessible to `coverage_test.go` when compiled as a package.

**Proof:**
```bash
$ grep -n "type mockStorage" server_test.go
20:type mockStorage struct {

$ grep -c "newMockStorage()" coverage_test.go
6  # Used 6 times successfully

$ go test ./pkg/server/grpc
PASS  # Compiles and runs successfully
```

### "command not found: gopls"

Install the Go language server:
```bash
go install golang.org/x/tools/gopls@latest
```

Then restart your IDE.

## Contributing

When adding new tests to `coverage_test.go`:
- You can use all helpers from other test files
- Add test helpers to the appropriate file:
  - Mock storage types → `server_test.go`
  - Mock streams → `interceptors_test.go`
  - Certificate helpers → `options_test.go`

## Status

✅ **All tests compile and pass**
✅ **96.8% coverage achieved**
✅ **No actual compilation errors**
⚠️ **IDE may show false positives** (see solutions above)
