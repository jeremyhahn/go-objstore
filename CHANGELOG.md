# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking / wire-visible changes

- MCP transport: object data is now base64-encoded in BOTH directions
  (objstore_put decodes, objstore_get encodes). Out-of-tree MCP clients
  sending raw text must base64-encode; non-base64 put data is rejected with
  "data must be base64-encoded". This makes the MCP transport binary-safe and
  consistent with the Unix transport. All six bundled SDKs were updated in
  the same change.
- REST/QUIC: DELETE /objects/{key} now returns 204 No Content (was 200 +
  JSON body on REST), matching the OpenAPI contract. All bundled SDKs accept
  both 204 and the legacy 200.
- REST: /metrics now requires authorization by default. Set the new
  MetricsPublic server config (or --metrics-public flag) to restore
  unauthenticated Prometheus scraping.
- JSON-RPC transports (MCP, Unix): authorization denials now return code
  -32001 (forbidden) on both transports (MCP previously returned -32600);
  not-found errors now return -32004 instead of the blanket -32603 internal
  error. New implementation-defined codes: -32002 unauthenticated,
  -32005 already exists, -32029 rate limited.
- Go SDK: UnixClient.TriggerReplication now sends the policy id as "id"
  (matching the server protocol). Previously the misnamed "policy_id" field
  was silently ignored and EVERY policy was synced.
- Unix transport: add_policy accepts an exact retention_seconds parameter
  (taking precedence over after_days) and get_policies responses include
  retention_seconds alongside after_days, so sub-day retention works over
  Unix like every other transport. All six SDK unix clients send
  retention_seconds and no longer reject sub-day values.
- QUIC server: backend timeouts now return 504 via the shared error
  taxonomy (was 408) and client cancellations 499, matching REST/gRPC for
  the same failure class.
- MCP default HTTP address: the library fallback is now :8081 (was :8080,
  which collided with the REST default). SDK defaults and doc examples
  (Python 8090, Ruby 8083, C# 8082, Rust 8080) are all aligned to 8081.
- gRPC server: unary and stream rate-limit interceptors now share one
  limiter (one bucket) instead of two independent ones, and per-IP rate
  limiting keys gRPC requests by peer address instead of a global bucket.

### Added

- SDK error-type parity: every SDK now maps the full canonical table —
  HTTP 400/401/403/404/409/429, JSON-RPC -32602/-32002/-32001/-32004/
  -32005/-32029, and the equivalent gRPC codes — to typed errors on every
  transport. New types: Go sentinels ErrInvalidArgument, ErrUnauthenticated,
  ErrPermissionDenied, ErrAlreadyExists, ErrRateLimited (rate-limited errors
  also satisfy errors.Is(err, ErrTemporaryFailure) so retry semantics are
  preserved); TypeScript AlreadyExistsError and RateLimitError; Python
  AuthorizationError, AlreadyExistsError, RateLimitError; Ruby
  AuthenticationError, AlreadyExistsError, RateLimitError; Rust
  Unauthenticated, AlreadyExists, RateLimited, InvalidArgument variants;
  C# ValidationException, AuthenticationException, AuthorizationException,
  AlreadyExistsException, RateLimitException. The C# REST/QUIC clients no
  longer surface raw HttpRequestException for non-2xx responses, and the
  Ruby REST/QUIC/MCP HTTP paths map 401/403/409/429 to typed errors.
- Azure backend: GetMetadata now returns real blob properties (size,
  content type, timestamps, custom metadata) and UpdateMetadata is
  implemented via SetMetadata/SetHTTPHeaders — both previously faked
  success with empty data. Missing blobs report not-found through the
  shared taxonomy.
- GCS backend: UpdateMetadata implemented via ObjectAttrsToUpdate
  (previously a silent no-op) and RemovePolicy actually removes the
  matching bucket lifecycle rule (previously returned success without
  doing anything).
- Glacier backend: Put streams archives with the Glacier multipart upload
  API (16 MiB parts, SHA-256 tree hash) instead of buffering the entire
  archive in memory; failed multipart uploads are aborted server-side.
- Pool: LeastUtilizationStrategy is implemented (cumulative size-weighted
  load tracking via the caller's StateStore, deterministic tie-breaking) —
  it previously always returned ErrStrategyNotImplemented.
- CLI: `policy add ... archive` validates archiver configuration up front
  with actionable errors (ErrGlacierArchiverUnavailable,
  ErrArchiveVaultRequired) and supports dedicated archive-vault-name /
  archive-region config keys; local-mode replication commands return a
  clear use-server-mode error.
- Shared cross-transport error taxonomy (pkg/common.Classify +
  pkg/server/errors): REST, gRPC, QUIC, MCP, and Unix now map the same error
  to the same class (404/NotFound/-32004, 403/PermissionDenied/-32001, …).
- Shared JSON-RPC 2.0 envelope/codes/parsing package (pkg/server/jsonrpc)
  used by both the MCP and Unix transports.
- Middleware parity: rate limiting, audit logging, and request-ID tracking
  are now available on QUIC, MCP (HTTP + stdio), and Unix transports, wired
  via new config fields and objstore-server flags (--rate-limit,
  --rate-limit-rps, --rate-limit-burst, --rate-limit-per-client, --audit).
- Cross-protocol conformance suite (make conformance-test): launches one
  server with all five transports and asserts byte-for-byte round trips,
  visibility, list, policy, and error-shape parity across every transport
  pair. Runs in CI.
- SDK unit tests now run in CI for all six languages (sdk-tests matrix job);
  make version-check gates version consistency in CI.
- E2E smoke tests for the Python, Ruby, TypeScript, and C# SDKs
  (make sdk-smoke, scripts/start-test-server.sh).
- C# SDK: MaxBufferSize option bounds in-memory buffering for MCP
  PutStreamAsync (default 64 MiB); new options-aware McpClient DI
  constructor wires Token/TenantID/headers.
- Ruby SDK: ObjectStore::AuthorizationError for JSON-RPC -32001 denials.
- Rust SDK: Error::Forbidden variant for JSON-RPC -32001 denials.

### Security

- Python SDK: the QUIC client's `verify_ssl` now defaults to `True`
  (certificate verification on by default). Pass `verify_ssl=False` only for
  testing against self-signed certificates. **Breaking** for callers relying
  on the old insecure default.
- Ruby SDK: the QUIC client no longer hard-codes `OpenSSL::SSL::VERIFY_NONE`;
  certificates are verified by default. Pass `verify_ssl: false` for testing
  against self-signed certificates. **Breaking** as above.
- MCP server: `resources/list` and `resources/read` errors are now sanitized
  through the shared error taxonomy instead of echoing raw error text
  (prevented potential path/internal-detail disclosure).
- REST: `/health` (always) and `/metrics` (when `MetricsPublic` is set)
  bypass authentication as well as authorization, so health checks and
  Prometheus scrapers work behind strict authenticators. The Swagger UI and
  spec still require authentication (they are exempt only from fine-grained
  authorization).
- REST RBAC: GET /objects (list) is now authorized as the list action,
  matching gRPC/QUIC/MCP/Unix. It was previously authorized as read, so a
  read-only principal could list and a list-only principal was denied.
- gRPC mTLS fallback now passes the full TLS connection state (preserving
  VerifiedChains) to AuthenticateMTLS, and MTLSAuthenticator verifies the
  peer chain against RequiredRoots when configured (the field was previously
  stored but never used, so any presented certificate minted a principal
  under permissive ClientAuth modes).
- Unix socket server refuses to start (and never deletes the file) when
  SocketPath points at an existing non-socket file; stale sockets from a
  previous run are still cleaned up.
- Unix socket server: `MaxConnections` now bounds accepted goroutines and
  file descriptors (the semaphore is acquired before spawning the handler);
  previously every connection got a goroutine and the limit only bounded
  concurrent processing.

### Fixed

- TypeScript SDK QUIC client: `get`/`exists`/`getMetadata` now send the
  configured auth headers (previously bypassed, causing 401s on token-secured
  deployments while other operations succeeded); `exists` no longer reports
  transport failures (DNS, refused) as "object missing"; HTTP 404 responses
  now raise errors instead of being treated as success for delete,
  updateMetadata, and policy operations.
- Unix transport SDK policy listing: the server returns policies as a bare
  JSON array, but the TypeScript and Python clients silently returned empty
  lists and the Ruby client raised TypeError. All clients now parse the bare
  array (and accept a wrapped {"policies": [...]} shape defensively).
- SDK unix clients now hold one persistent connection with JSON-RPC
  response-ID validation and automatic reconnect after errors or the
  server's 30s idle close. Previously TypeScript/Python/Ruby/Rust/C# dialed
  a fresh socket per request and the Go client never re-dialed (one idle
  period bricked the client) and could consume a stale response from a
  timed-out call as the next call's reply.
- SDK JSON-RPC error mapping is now code-based (-32004 not found, -32002
  unauthenticated, -32001 forbidden, -32602 invalid params, …) instead of
  message-substring matching; the Go unix/MCP/gRPC clients now map not-found
  to ErrObjectNotFound so errors.Is works on every transport, and the
  TypeScript REST/MCP/Unix clients raise the typed errors from errors.ts
  (404 → ObjectNotFoundError etc.) instead of generic Error/ConnectionError.
- C# SDK unix client AddReplicationPolicyAsync now transmits destination
  settings and the check-interval schedule (previously dropped, producing
  non-functional replication policies); PutStreamAsync enforces the same
  MaxBufferSize bound as the MCP client.
- Audit middleware reuses the request ID set by RequestIDMiddleware (a
  typed-vs-string context-key mismatch made the lookup always miss, so audit
  logs carried a second generated ID and the X-Request-ID response header
  was overwritten, breaking audit/access-log correlation).
- objstore-server graceful shutdown no longer races transport startup:
  servers are constructed before their goroutines launch and shutdown waits
  for all transports to stop before removing the unix socket.
- REST and MCP servers: Start() panicked with a nil-pointer dereference when
  given a TLS-disabled adapters.TLSConfig (Build() returns nil); they now
  serve plaintext, matching a nil TLSConfig.
- QUIC GET/HEAD/metadata handlers no longer collapse every backend error to
  404 "object not found"; errors route through the shared taxonomy
  (503 unavailable, 504 deadline, 403 permission), and the REST metadata
  handler does the same.
- MCP objstore_put/objstore_get tool descriptions now state that data is
  base64-encoded (the schema previously advertised "string or base64",
  which silently corrupted raw strings that happened to be valid base64).
- common.Classify no longer falls back to exact error-string matching;
  producers wrap the canonical sentinels (errors.Is is the single
  mechanism), and common.ValidationError unwraps to ErrInvalidArgument so
  backend-level key validation classifies as 400 instead of 500.
- Rust SDK REST put_stream streams the request body via chunked transfer
  encoding instead of buffering the entire object in memory; the Ruby MCP
  client reuses one Net::HTTP connection instead of a TCP+TLS handshake per
  request; the Go gRPC client builds its auth metadata once per client
  instead of per call.
- Rust SDK unix client: `add_replication_policy` now transmits
  `replication_mode` (previously omitted, so the server defaulted the mode).
- Python SDK unix client: invalid base64 in a get response now raises
  `ServerError` instead of silently returning empty bytes.
- Ruby SDK unix client: malformed JSON from the server now raises
  `ProtocolError` instead of `ConnectionError`.
- JSON-RPC transports (MCP/unix): backend outages now map to a distinct
  `-32003` (unavailable) code, and cancellation/timeout produce
  distinguishable messages, matching REST (503/499/504) and gRPC
  (Unavailable/Canceled/DeadlineExceeded) semantics.
- Lifecycle/replication "policy already exists" errors now classify through
  the shared taxonomy (409 on REST/QUIC regardless of error wrapping); the
  per-handler string comparisons were removed.
- The OpenAPI spec now documents the /metrics endpoint and its
  MetricsPublic-gated authentication behavior.
- REST HEAD /exists/{key} now conforms to the OpenAPI contract: 200 when the
  object exists, 404 when absent, no body. It previously always returned
  200 + a JSON body that HEAD clients cannot read, so the CLI reported every
  object as existing.
- QUIC server gained the HEAD /exists/{key} route for REST parity (the
  legacy GET /objects/{key}?exists= variant is preserved).
- CLI QUIC client now speaks genuine HTTP/3 over UDP via quic-go (it
  previously used a TCP HTTP client and could never reach the QUIC server);
  custom CAs are honored via SSL_CERT_FILE, and Close() releases the
  transport.
- CLI REST client GetPolicies now parses the server's wrapped
  {"policies": [...], "count": n} response (it previously expected a bare
  array and always failed).
- Validation errors (empty key, path traversal, invalid characters) now wrap
  common.ErrInvalidArgument so every transport reports them as
  400/InvalidArgument/-32602 instead of internal errors.
- Local backend UpdateMetadata wraps missing objects in ErrKeyNotFound, and
  Classify recognizes raw fs.ErrNotExist, so metadata updates on missing
  keys surface as NotFound on every transport instead of Internal.
- Server integration suite: restored the missing Dockerfile.test (the suite
  could not run at all), build the CLI from the bind-mounted source inside
  the test container, and `make test-servers` now passes --build so server
  images are never stale. The suite's CLI tests were aligned with the CLI's
  documented exists exit-code contract, and stale expectations pinning the
  old error codes (Internal for not-found, 200 for DELETE) were updated.
- C# SDK test Dockerfile now copies the Internal/ directory; Ruby SDK rescue
  chains no longer swallow the typed AuthorizationError/ProtocolError.
- MCP/Python SDK: binary data was corrupted by UTF-8 replacement encoding on
  put and never base64-decoded on get.
- Go SDK: UnixClient.GetPolicies swallowed RPC errors and returned an empty
  result; SetDeadline failures are now surfaced.
- Go SDK MCP/C# SDK MCP/Ruby SDK MCP clients: removed the silent plain-text
  fallback when get data is not valid base64 (protocol violations now error).
- SDK error mapping: JSON-RPC -32001 was misinterpreted as "not found" by
  the C#, Python, Ruby, and Rust clients; it is the server's forbidden code.
- Rate limiter: the per-IP eviction goroutine could never be stopped (leak
  per limiter); lastSeen is now updated lock-free on the hot path.
- Replication changelog: a failed reopen after rotation/rewrite left a
  closed file handle; subsequent writes now lazily recover.
- Unix server: the first read on a connection was not covered by the read
  deadline, letting connect-and-stall clients pin a goroutine.
- QUIC server: panic recovery no longer attempts to write a 500 after
  response headers were sent; the stream is aborted instead.
- MCP resources/read now returns binary resources as base64 "blob" entries
  per the MCP spec instead of corrupting them in a "text" field.
- REST and MCP HTTP servers clamp TLS to a 1.2 minimum regardless of adapter
  configuration.
- C# SDK: HttpRequestMessage/HttpResponseMessage disposal leaks across the
  REST, QUIC, and MCP clients; GetStreamAsync now returns a stream that owns
  (and disposes) its response; PutStreamAsync no longer disposes the
  caller's input stream.
- Ruby SDK Unix client: replaced non-portable SO_RCVTIMEO timeval packing
  with IO.select-based read/write timeouts.

### Changed

- Root VERSION synced to 0.2.0 to match all SDK manifests.
- cmd/* server binaries log via log/slog (structured) instead of log.Printf.
- Dead code removed: never-returned error sentinels in the QUIC/Unix server
  packages, the unused CreateSelfSignedCert stub and its sentinel in
  pkg/adapters, an unused validation regex, and orphaned test mocks across
  eight packages; the lifecycle-policies example now actually runs its
  glacier/multi-tier/lifecycle-manager examples.
- Test infra hardening: the server integration compose no longer publishes
  host ports (tests run in-network; host port collisions on dev machines
  can no longer break the suite) and `make test-servers` cleans up
  containers even on failure; `make sdk-smoke` skips legs whose language
  toolchain is missing (e.g. .NET 9) instead of failing; local
  `make security` govulncheck is blocking to match GitHub CI; `make ci`
  gained version-check and conformance-test; GitHub CI gained the
  replication and CLI integration suites; the CLI archive/policy
  integration tests run for real against the local backend instead of
  being unconditionally skipped; rate-limit middleware tests exercise the
  modern API with one compact deprecated-shim compat test.
- docs/configuration rewritten around the servers' real flags and defaults
  (the old docs described a YAML config system no binary loads); QUIC port
  examples corrected to 4433; six backend doc.go files now carry the
  correct AGPL-3.0/Commercial header; the TypeScript QUIC client's
  HTTP/1.1-only limitation is documented prominently.
- Go SDK internals deduplicated (shared auth-header, JSON-RPC envelope, and
  HTTP streaming helpers); TypeScript JSON-RPC envelope types unified in
  src/clients/jsonrpc.ts; Rust stream buffering unified in collect_stream.
- SDK internals deduplicated further: Python shares auth-header/HTTP-error
  helpers (objstore/_http.py) and a JSON-RPC error mapper (_jsonrpc.py)
  across the REST/QUIC/MCP/Unix clients; Ruby MCP/Unix clients share a
  JsonRpcHelpers mixin; Rust MCP/Unix clients share a crate-private jsonrpc
  module; C# shares AuthHeaders/ParseCustomMap/JsonRpcErrorMapper in
  Internal/. The C# NuGet description now lists all five protocols.
- Repo hygiene: integration-test certificates are no longer committed
  (generated by make test-servers at run time and gitignored); root-built
  server binaries are gitignored.

## [0.1.5-alpha] - 2026-05-31

### Changed

- Go toolchain upgraded from 1.21 to 1.26.4
- All SDK packages updated to version 0.2.0 for API parity across languages
- TypeScript SDK: JavaScript SDK consolidated into TypeScript package (@go-objstore/client)
  - Ships compiled JavaScript (ESM + CJS) usable from plain JavaScript projects
- Encryption example: go-xkms dependency removed, now uses stdlib AES-256-GCM implementation

## [0.1.4-alpha] - 2025-12-06

### Added

- Memory Storage Backend: New in-memory storage implementation (pkg/memory)
  - Thread-safe concurrent access with RWMutex
  - Full Storage interface implementation
  - Lifecycle manager with delete and archive policy support
  - Ideal for testing, development, and cache scenarios
  - 93% test coverage

- Unix Server Handler Tests: Comprehensive test coverage for all JSON-RPC handlers
  - Tests for archive, lifecycle policy, and replication handlers
  - Mock replication manager for testing replication handler success paths
  - Coverage increased from 46.5% to 87.1%

- Memory Lifecycle Tests: Tests for lifecycle manager Process() function
  - Delete and archive action coverage
  - Edge case testing for policy processing

### Changed

- CI/CD Pipeline: Updated golangci-lint from v1.x to v2.7.1
  - Migrated golangci-lint-action from v6 to v7
  - Updated .golangci.yml to v2 configuration format
  - Added memory package to errcheck exclusions

- Test Coverage: Improved overall coverage from 88.9% to 90.7%
  - Unix server: 46.5% → 87.1%
  - Memory package: 84.2% → 93.1%
  - Exceeds 89% CI threshold

- Code Quality: Applied gofmt formatting and removed redundant blank lines
  - Struct field alignment fixes across protocol definitions
  - Consistent whitespace in license headers
  - Updated local storage tests to use common.ErrKeyNotFound

### Fixed

- Local Storage Tests: Fixed error assertions to use errors.Is() with common.ErrKeyNotFound instead of os.IsNotExist()
- Golangci-lint v2 Compatibility: Resolved configuration schema issues with v2 format migration

## [0.1.3-alpha] - 2025-12-06

### Added

- Multi-Language SDK Support: Added official client SDKs for 6 programming languages
  - TypeScript SDK with full type safety and async/await support
  - Go SDK with idiomatic Go patterns and context support
  - Python SDK with async support and type hints
  - Ruby SDK with Rails integration patterns
  - Rust SDK with async/await and strong typing
  - C# SDK with .NET 8 support and async patterns
  - JavaScript SDK with CommonJS and ESM support
  - All SDKs include comprehensive unit and integration tests
  - Docker-based integration testing for all SDKs

- Unix Socket Server: New high-performance local IPC server (pkg/server/unix)
  - Unix domain socket support for local applications
  - Low-latency communication without network overhead
  - Full feature parity with REST/gRPC/QUIC servers
  - Comprehensive test coverage

- Replication Integration Tests: Dedicated Docker-based replication test suite
  - Moved replication tests to test/integration/replication/
  - Tests for no encryption, backend encryption, and three-layer encryption
  - Prefix filtering and empty source edge case tests

- CLI Client Unix Socket Support: Added Unix socket transport to CLI client (pkg/cli/client/unix.go)

### Fixed

- Three-Layer Encryption Metadata Conflict: Fixed critical bug where backend at-rest encryption and client-side DEK encryption used the same metadata field
  - Changed local backend to use `at_rest_encryption_key_id` and `at_rest_encryption_algorithm` metadata fields
  - Prevents metadata overwrites when both encryption layers are active
  - Fixes data truncation issues in three-layer encryption replication

- CLI Integration Tests Docker Build: Fixed .dockerignore excluding test files from Docker builds
  - Integration tests now properly copied into Docker containers
  - All CLI integration tests passing

### Changed

- Server Handler Refactoring: Consolidated and simplified all server handlers
  - gRPC, REST, QUIC, and MCP servers now share common test helpers
  - Improved error handling consistency across all protocols
  - Enhanced lifecycle management and replication handlers
  - Removed redundant error types (consolidated into common errors)

- Facade Pattern Enhancements: Extended facade with additional features
  - Improved multi-backend routing and error handling
  - Enhanced test coverage for facade operations
  - Better integration with server handlers

- Documentation Updates: Updated architecture and usage documentation
  - Updated server documentation with Unix socket server
  - Enhanced storage layer documentation
  - Improved getting started guide

## [0.1.2-alpha] - 2025-11-22

### Added

- Facade Pattern: Implemented comprehensive facade pattern for centralized object storage API
  - pkg/validation: Input validation preventing path traversal, injection attacks, and malformed input (98.2% test coverage)
  - pkg/objstore: Singleton facade with multi-backend support and backend:key routing syntax (57.7% test coverage)
  - Multi-backend support: Work with multiple storage backends simultaneously using backend:key syntax
  - Automatic validation: All inputs validated at facade layer to prevent security vulnerabilities
  - Security hardening: Blocks path traversal (.., ../), absolute paths, null bytes, control characters
  - Centralized error handling: Sanitized error messages prevent information disclosure
  - Thread-safe singleton pattern with Initialize/Reset for testing
  - Backend routing: Support for backend:key syntax (e.g., "s3:myfile.txt", "local:cache.dat")
  - Comprehensive documentation: Migration guide (docs/facade-migration.md) and working examples
  - Example code: Complete facade usage example in examples/facade-usage/main.go

### Changed

- README: Updated with facade pattern documentation and examples, showing both new facade pattern and legacy direct storage access
- Architecture: Introduced facade layer as recommended API while maintaining backward compatibility with direct storage access

### Security

- Input validation now enforces strict rules across all entry points
- Path traversal protection: Rejects .., ../, /.., /../ patterns
- Absolute path blocking: Prevents /etc/passwd, C:\Windows style attacks
- Null byte filtering: Blocks \x00 injection attempts
- Control character validation: Rejects \n, \r, \t in keys
- Backend name validation: Only lowercase alphanumeric and hyphens allowed
- Length limits: 1024 chars for keys, 64 for backend names
- Log injection prevention via SanitizeForLog function

## [0.1.1-alpha] - 2025-11-20

### Added

- FIPS 140-3 Builds: Added FIPS-compliant binary builds using Go 1.24's GOFIPS140 mode
  - FIPS builds available for Linux (amd64, arm64)
  - Binaries named with -fips suffix (e.g., objstore-fips)
  - NIST FIPS 140-3 cryptographic compliance for regulated environments
  - Release artifacts include both standard and FIPS variants

### Changed

- Release Workflow: Updated to only trigger on tags pushed from the main branch
  - CI continues to run on both main and develop branches
  - Release builds restricted to production branch for stability
- Go Version: Updated release builds to use Go 1.24 for FIPS 140-3 support

## [0.1.1-alpha] - 2025-11-20

### Fixed

- Dependency Compatibility: Downgraded github.com/quic-go/qpack from v0.6.0 to v0.5.1 to maintain compatibility with quic-go v0.56.0
- Build System: Removed restrictive //go:build local build tags from all files in pkg/local package, allowing the package to be included in normal builds without requiring explicit build tags
- Test Coverage: Updated Makefile test target to include all build tags (local, awss3, minio, gcpstorage, azureblob, glacier, azurearchive), ensuring comprehensive test coverage across all backend implementations
- Compilation Errors: Resolved compilation errors in pkg/replication/syncer.go and test files in pkg/server/quic caused by missing pkg/local package
- Linting Issues: Reduced linting errors from 290 to 64 (78% reduction)
  - Fixed all err113 errors by using errors.Is() instead of direct error comparisons
  - Fixed all errcheck errors by adding proper error handling for Close() calls
  - Fixed gocritic exitAfterDefer warnings by replacing os.Exit() with log.Fatal()
  - Fixed staticcheck SA9003 empty branch warnings
  - Fixed staticcheck SA1012 nil context warnings by using context.TODO()
  - Fixed staticcheck S1009 unnecessary nil checks before len()
  - Fixed staticcheck ST1011 variable naming issues
  - Added nolint directives for AWS SDK v1 deprecation warnings (migration to v2 planned)
- Code Quality: Extracted 35+ magic string constants across 25+ files to improve maintainability
  - Centralized repeated string literals (action types, content types, file names, etc.)
  - Eliminated typo risks from duplicated strings
  - Improved code readability with named constants

### Added

- Test Coverage: Added tests for GetReplicationStatusCommand and FormatReplicationStatus functions to improve CLI package coverage
- Package Coverage: Enabled testing for previously excluded packages:
  - pkg/minio: 94.4% coverage
  - pkg/azurearchive: 93.1% coverage
- Error Handling: Enhanced error handling across all packages
  - Proper error wrapping and checking throughout codebase
  - Consistent use of errors.Is() for error comparison
  - Deferred cleanup handlers with error checking

### Changed

- Test Coverage: Improved overall test coverage from 89.8% to 90.5%, exceeding the 90% coverage target
- CLI Package: Increased coverage from 85.1% to 91.5%
- Code Quality: Enhanced maintainability through systematic refactoring
  - Replaced magic strings with named constants across all packages
  - Improved error handling patterns
  - Better resource cleanup and context handling
- Dependencies: Updated golangci-lint configuration to enforce stricter code quality standards

## [0.1.0-alpha] - 2025-11-14

### Added

- **Core Storage Interface**: Unified Storage interface supporting multiple backend implementations
- **Multiple Storage Backends**:
  - Local filesystem storage for development and testing
  - Amazon S3 backend for AWS object storage
  - MinIO backend for self-hosted S3-compatible storage
  - Google Cloud Storage (GCS) backend for Google Cloud
  - Azure Blob Storage backend for Microsoft Azure
  - AWS Glacier for long-term cold storage archival
  - Azure Archive for Azure long-term archival

- **Advanced Features**:
  - Context support for all operations (cancellation and timeouts)
  - Metadata support with custom key-value pairs
  - Lifecycle policies for automatic deletion and archival
  - Pagination support for listing large object collections
  - Directory operations and filesystem abstraction via StorageFS
  - Pluggable adapters for custom logging and authentication

- **Replication System**:
  - Replication policies with configurable source and destination backends
  - Transparent replication mode (decrypt-copy-encrypt)
  - Opaque replication mode (copy encrypted blobs as-is)
  - Three-layer encryption support (backend at-rest, source DEK, destination DEK)
  - Change detection via metadata (ETag and LastModified)
  - Incremental sync with JSONL-based change log
  - Parallel worker support for high-performance replication
  - File system watcher for real-time change detection
  - Background sync with ticker-based scheduling

- **Server Implementations**:
  - gRPC server for service-to-service communication
  - REST API server with HTTP endpoints
  - QUIC/HTTP3 server for next-generation protocol support
  - MCP (Model Context Protocol) server for AI integration
  - Complete API parity across all server protocols (100%)

- **CLI Tool** (`objstore`):
  - Object storage and retrieval commands
  - Support for all backends via configuration
  - Configuration via YAML files, environment variables, or command-line flags
  - Data piping between backends for migration and backup
  - Flexible stdin/stdout support for shell integration
  - Local and remote operation support (--server flag)
  - Complete gRPC, REST, and QUIC client implementations
  - Replication policy management commands

- **C API**:
  - Shared library (`libobjstore`) for embedding in C/C++ applications
  - C language bindings for all core functionality
  - Compiled as dynamic library for flexibility and multiple language bindings

- **Protocol Buffers**:
  - gRPC protocol definitions for service communication
  - Efficient binary serialization and schema evolution support

- **Documentation**:
  - Comprehensive README with quick start guide
  - API reference documentation
  - Backend configuration guides
  - StorageFS filesystem abstraction guide
  - Lifecycle policies documentation
  - Replication user guide (856 lines)
  - C API reference
  - Testing guide with coverage metrics
  - Getting started tutorials

- **Testing**:
  - Comprehensive unit tests with 92% code coverage
  - Integration tests for all backends using Docker
  - Integration tests for all server implementations
  - Test fixtures and helpers for consistent testing
  - CI/CD workflow support

- **Build System**:
  - Makefile with convenient build and test targets
  - Modular build configuration with optional features
  - Docker and docker-compose for development and testing
  - Support for building CLI, server, and library binaries

- **Examples**:
  - Runnable Go examples for all major features
  - C client examples with C API usage
  - StorageFS filesystem examples

### Security

- TLS/mTLS support for secure gRPC communication
- Secure credential handling for cloud backends
- Context-aware timeout and cancellation for resource cleanup

[0.1.4-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.4-alpha
[0.1.3-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.3-alpha
[0.1.2-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.2-alpha
[0.1.1-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.1-alpha
[0.1.0-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.0-alpha
