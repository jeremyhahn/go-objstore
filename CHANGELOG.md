# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The local provider's `List`, `ListWithContext` and `ListWithOptions` no longer
  fail with a "no such file or directory" error when a concurrent `Put` renames
  its `.tmp-*` file (or an object is deleted) while the listing is walking the
  directory. The walk now uses `filepath.WalkDir` and treats an entry that
  vanished as absent; in-flight `.tmp-*` files are never returned as keys
  (issue #68).

### Changed

- The local provider's `ListWithOptions` returns an empty result, not an error,
  when the storage directory does not exist, matching `List` and
  `ListWithContext`, which already did (issue #68).

## [0.4.0-alpha] - 2026-10-08

### Added

- The server binaries (`objstore-server` and the four `objstore-*-server`
  binaries) resolve every setting through one viper-based configuration layer:
  an optional YAML file (`-config` or `OBJSTORE_CONFIG`), `OBJSTORE_*`
  environment variables and flags, with precedence flags > environment > file >
  defaults (issue #34, owner ruling 2026-08-26). `objstore-server` gains
  `-rest-host` / `OBJSTORE_REST_HOST` / `rest.host`: its REST listener was
  previously pinned to `0.0.0.0`. See `docs/configuration/servers.md` and
  `configs/objstore-server.yaml.example`.
- `objstore-server` gains `-grpc-tls-cert` / `-grpc-tls-key` (and the
  `grpc.tls_cert` / `grpc.tls_key` keys, `OBJSTORE_GRPC_TLS_*`), serving gRPC
  over TLS as `objstore-grpc-server` already could. A TLS certificate without
  its key (or the reverse) is rejected for gRPC and QUIC.
- The standalone `objstore-grpc-server`, `objstore-rest-server`,
  `objstore-quic-server` and `objstore-mcp-server` apply `rate_limit.*` and
  `audit.enabled` (flags `-rate-limit*` and `-audit`, `OBJSTORE_RATE_LIMIT_*`,
  `OBJSTORE_AUDIT_ENABLED`) exactly as `objstore-server` does.
- `mcpserver.DefaultServerConfig()` and `mcpserver.DefaultHTTPAddress` carry
  the MCP HTTP listen default, which `NewServer` applies to an empty
  `HTTPAddress`; the serving path no longer holds its own fallback.

### Fixed

- C API (`libobjstore`): `ObjstoreNewStorage` returns -1 with
  `ErrHandlesExhausted` once 2^31-1 handles have been issued, instead of
  truncating the next handle to a 32-bit `C.int` that could alias an earlier
  storage (#59). Found by gosec G115 once CI scanned the cgo package at all.
- The REST server built its listen address with `"%s:%d"`, so an IPv6 host
  such as `::1` produced the unparseable `::1:8080`. It now uses
  `net.JoinHostPort` (`[::1]:8080`).
- `rest.NewServer` returns typed errors, `*rest.TrustedProxiesError`
  (configured proxies, cause) and `*rest.HandlerInitError` (cause), instead of
  `fmt.Errorf` strings; both unwrap to the underlying error.

### Changed

- go-enclave v0.3.0-alpha to v0.8.0-alpha (issue #66) in the root,
  `api/sdks/go` and `examples/encryption` modules, with `go` 1.27.1 and
  `golang.org/x/sys` v0.48.0. `NewMemoryEnclaveFromString` and `OpenString` are
  removed upstream: `cli.Config` brings string credentials in as
  `NewMemoryEnclave([]byte(s))` and reads them through `WithBytes`.
  `Destroy` now returns an error, so `cli.Config.Destroy`,
  `cli.Config.SetBackendKeyEnclave`, `cli.Config.SetBackendSecretEnclave` and
  `adapters.TLSConfig.Destroy` return `error` (a failure while replacing an
  enclave in the `TLSConfig` builder chain is reported by its `Destroy`), and
  `cmd/objstore` reports a failed credential release on exit.
  go-enclave v0.8.0-alpha requires Go 1.27.1, so every CI, nightly and release
  `GO_VERSION`, the `golang:` image bases (root, Go SDK and integration
  Dockerfiles), the Rust SDK workflow's `setup-go` pin and the documented
  minimum Go version move from 1.27.0 to 1.27.1.
  The credential staging for docker builds that run `go mod download` is now
  one composite action (`.github/actions/stage-gomodauth`) used by the
  integration-test, SDK-test and Docker-scan jobs, so the `sdk-tests` job
  supplies the `gomodauth` secret too. `api/sdks/go/go.mod` no longer lists
  go-enclave: the SDK imports nothing from it, so `go mod tidy` does not want it.
  The S3 and MinIO integration tests (and the root `docker-compose.yml`) build
  MinIO from source at `RELEASE.2025-10-15T17-29-55Z` through the one
  `test/integration/minio-server/Dockerfile`, because the published MinIO
  images now refuse anonymous pulls; `integration-test-s3` and
  `integration-test-minio` bring the service up with `--build`.
- Go toolchain 1.26.7 to 1.27.0 (portfolio standard, owner ruling
  2026-09-01; issue #19): the `go` directive in every module (root,
  `api/sdks/go`, `examples/encryption`), the CI/nightly/release `GO_VERSION`
  pins, the Rust SDK workflow's `setup-go` pin (was 1.21), and every
  `golang:` Dockerfile base. The Debian-based integration images now pin
  `golang:1.27.0-bookworm` instead of an unqualified tag whose Debian
  release floated; the alpine bases stay alpine (devops.md §9a, alpine
  first). `go mod tidy` is a no-op in all three modules.
  The CI linter moves from golangci-lint v2.12.2 to v2.13.2, built with
  `go install` under the job's Go: v2.12.2 cannot lint a `go 1.27.0` module.
- Audit logging now defaults to on for the standalone QUIC and MCP servers
  (`audit.enabled`, default `true`), matching `objstore-server`; set
  `-audit=false` to turn it off. When MCP runs in stdio mode, audit records
  are written to standard error so they no longer mix into the JSON-RPC
  stream on standard output (this also affected `objstore-server
  -mcp-mode stdio`).

- The server binaries now refuse to start on an invalid configuration
  (unknown config-file key, malformed value, address that is not `host:port`,
  invalid MCP mode, stray positional argument), reporting every problem and
  exiting with status 2. `objstore-server` previously logged an invalid
  `-mcp-mode` and ran without MCP.

- The locked-memory primitive moved out of this repository. `pkg/enclave` is
  removed and its contents now ship as the Layer 0 module
  `github.com/jeremyhahn/go-enclave` (v0.3.0-alpha), so libraries that do not
  sit above go-objstore can consume it (issue #29, owner-ruled 2026-08-23;
  adopted here in #30, re-pinned to v0.3.0-alpha per #31). The exported
  surface is unchanged — same `package enclave`, same `MemoryEnclave`,
  `LockedMemoryError`, `ErrLockedMemoryUnavailable` and `WithBytes` — so call
  sites are identical and only the import path moves. v0.3.0-alpha backs the
  enclave with `memcall` directly, so `github.com/awnumar/memguard` leaves
  this repository's dependency graph entirely. **Consumers that imported
  `github.com/jeremyhahn/go-objstore/pkg/enclave` must switch to
  `github.com/jeremyhahn/go-enclave`.**

- `make release-alpha` is the company release-alpha template v2.3.9, vendored
  verbatim at `mk/release-alpha.mk`, replacing the v1 recipe (issue #32). The
  version is now an explicit argument (`make release-alpha VERSION=vX.Y.Z-alpha`)
  rather than a patch-only auto-increment; every git write signs through
  `agent-git.sh` as `agent-go-objstore`; the tag is annotated; only the release
  commit and that one tag are pushed, to `origin`; and the recipe fails closed
  on a wrong branch, a dirty tree, an existing tag, an unreachable remote or a
  missing CHANGELOG section. The Makefile's build version variable is renamed
  `BUILD_VERSION` so it no longer shadows the `VERSION=` argument. The
  repository's release wiring lives in `mk/release.mk`.
- CHANGELOG entries are now `changelog.d/<issue>-<slug>.md` fragments
  (release-versioning.md §5). `make release-alpha` first runs
  `changelog-assemble`, which folds the fragments and any `## [Unreleased]`
  content into the version's section, grouped by subsection, deletes the
  consumed fragments and commits that, signed, before the tag is created.

### Build

- Docker builds now fetch the internal first-party dependency go-enclave from
  Gitea via an optional BuildKit secret (`--mount=type=secret,id=gomodauth`)
  carrying the tokenised git `insteadOf` rewrite
  (conventions/dependency-resolution.md §6). This is required because go-enclave
  is go-objstore's first internal dependency: a plain `go mod download` inside a
  container resolves it against public github.com, where it does not exist. The
  root `Dockerfile`, every `test/integration/*` image and compose file, the
  server compose and the SDK compose files are covered; the CI `integration-test`
  and `docker` jobs stage the credential under `RUNNER_TEMP` (outside the build
  context) and pass it through. The secret file holds **only** the tokenised
  `GIT_CONFIG_KEY_0` value as raw text (never `export NAME=value` shell); each
  Dockerfile assigns it with a quoted command substitution
  (`GIT_CONFIG_KEY_0="$(cat …)"`), so a credential containing shell
  metacharacters is assigned literally rather than executed. To run integration
  tests locally, point `GOMODAUTH_ENV` at a file **outside this repository**
  (the compose build context is the repo root; `.dockerignore` guards
  `gomodauth*` at any depth as a backstop) containing that single line,
  e.g. `url.http://x:<token>@<gitea-host>/jeremyhahn/.insteadOf`; unset,
  `GOMODAUTH_ENV` defaults to an empty secret and resolves from the public proxy.

- `make test-release-alpha` runs the template's own regression harness and
  `mk/testdata/release-path-tests.sh`, which drives the repository's release
  path against a throwaway remote: explicit-version refusal, wrapper arguments,
  an annotated single tag, a failed commit leaving no tag and no push, the
  assembled CHANGELOG in the tagged tree, and no command injection through any
  release input.

## [0.3.0-alpha] - 2026-08-19

### Added

- `pkg/enclave`: `MemoryEnclave`, a region of locked, zeroized memory for
  holding keys, credentials and other sensitive material for their whole
  lifetime. Backed by memguard's `LockedBuffer` — `mlock`'d pages, guard
  pages, canary, wiped on `Destroy`, core dumps disabled. It performs no
  cryptography and never uses memguard's `Enclave`/`Seal` path, so it is clean
  under `GODEBUG=fips140=only`, which CI enforces. Allocation failure under
  `RLIMIT_MEMLOCK` returns `ErrLockedMemoryUnavailable` instead of aborting the
  process. See `docs/architecture/protected-memory.md`.
- `adapters.TLSConfig.WithServerKeyEnclave`, `ServerKeyEnclave` and `Destroy`:
  supply the server private key in locked memory rather than a plain `[]byte`.
- `cli.GetConfigProtected`, `Config.SetBackendKeyEnclave`,
  `SetBackendSecretEnclave`, `SetBackendKeyFromString`,
  `SetBackendSecretFromString`, `BackendKeyEnclave`, `BackendSecretEnclave`
  and `Destroy`: hold the backend credentials in locked memory rather than in
  plain strings.
- `enclave.LockedMemoryError`: the typed error returned when locked memory
  cannot be allocated. It carries memguard's diagnostic cause and reports
  itself as `ErrLockedMemoryUnavailable` through `errors.Is`, so callers keep
  matching the sentinel. Replaces a `fmt.Errorf` wrap, which the portfolio
  error convention bans in library code.
- `cli.Config.StorageSettings`: the error-returning replacement for
  `GetStorageSettings`. It reports `enclave.ErrEnclaveDestroyed` when a
  credential was placed in an enclave that has since been destroyed, instead
  of silently substituting the deprecated plain field.

### Changed

- `TLSConfig.Build` reads a key file into a `MemoryEnclave` instead of calling
  `tls.LoadX509KeyPair`, so the PEM-encoded key is never left in unprotected
  heap memory. Behavior and errors are unchanged.
- The `objstore` CLI loads its configuration with `GetConfigProtected` and
  zeroizes the credentials on exit. Credentials are materialized as plain
  strings only at the backend settings-map boundary.
- `quic.SaveCertificateToPEM` zeroizes the DER encoding of the private key
  after writing it.
- `TLSConfig.WithServerCertFiles`, `WithServerCertPEM` and
  `WithServerKeyEnclave` each select exactly one server-key source and clear
  the others, destroying a superseded enclave and zeroizing a superseded
  `ServerKeyPEM`. `WithServerCertPEM` copies the key it is given, so the
  config wipes only memory it allocated and never the caller's array. `Build` ranks the enclave first, so reconfiguring a config
  that already held one previously left it attached and kept serving the
  superseded key — a certificate rotation that looked applied and was not.
- `TLSConfig.WithServerKeyEnclave` now zeroizes and clears `ServerKeyPEM` when
  it installs the enclave. Selecting the protected path previously left a
  complete private key in ordinary heap memory for the config's lifetime,
  merely unread — which made incremental migration off the deprecated API
  ineffective. The wipe reaches the caller's backing array if it still holds
  one.
- Enclave allocation and borrows are coordinated process-wide. memguard's
  allocation-failure path purges and **unmaps** every live buffer in the
  process while holding only its own locks, so a per-enclave mutex could not
  keep a borrow safe against an unrelated goroutine exhausting locked memory —
  the borrowed pages went away underneath the reader and the process faulted.
  Allocation now takes a package-wide lock exclusively and every borrow takes
  it shared. `Destroy` takes it exclusively too — memguard's `Buffer.Destroy`
  also panics, and therefore purges, when releasing pages fails — and `Size`
  and `IsAlive` take it shared, because they read memguard state a purge
  rewrites. A subprocess fault-injection test holds a borrow across a foreign
  allocation failure; without the coordination it reproduces as a SIGSEGV.
- `MemoryEnclave` holds its read lock across the whole of `WithBytes`,
  `OpenString` and `EqualTo`, so `Destroy` blocks until in-flight borrows have
  returned rather than unmapping pages a reader is still holding. `Open`, which
  hands back a slice that outlives the lock, is documented as unsafe alongside
  a concurrent `Destroy`; every consumer in this repository uses the scoped
  forms.
- `NewMemoryEnclave` wipes the caller's slice on every return path. memguard
  wipes its source only after a successful allocation, so the secret previously
  survived in the caller's memory when locked memory was exhausted — the exact
  path `ErrLockedMemoryUnavailable` exists for.
- The credential path fails CLOSED. Once an enclave has been selected it is the
  only source: `Config.StorageSettings` and `TLSConfig.Build` return
  `ErrEnclaveDestroyed` rather than reading the deprecated plain field, which
  would otherwise authenticate with stale unprotected material after memguard
  purged the enclave. The plain fields are still read normally when no enclave
  was ever set.

### Deprecated

Nothing is removed and no exported signature changed; the plain-memory paths
below keep working and are removed in the next major version. Where both are
set, the enclave wins.

- `adapters.TLSConfig.ServerKeyPEM` and `TLSConfig.WithServerCertPEM` — use
  `WithServerKeyEnclave`.
- `cli.Config.BackendKey` and `cli.Config.BackendSecret` — use
  `GetConfigProtected` or the `SetBackend*` setters.
- `cli.Config.GetStorageSettings` — its signature cannot report a destroyed
  enclave. It now omits such a credential rather than substituting the plain
  field, so the backend fails to authenticate; use `StorageSettings` to see
  why.

### Dependencies

- Added `github.com/awnumar/memguard` v0.23.0 (and its `memcall` dependency).

## [0.2.0] - 2026-08-10

Version reconciliation. No functional change: the code in this release is
the same tree that carried the 0.1.5-alpha stamp, plus package
documentation.

The workstation and dev server had diverged. The server carried test
coverage work, a Go 1.26.4 bump and SDK parity; the workstation carried a
release commit whose version stamping reverted VERSION and the eleven SDK
manifests from 0.2.0 back to 0.1.5-alpha. The trees were reconciled in
favour of the workstation, which was verified to contain all of the
server's work, and the version is now restored to 0.2.0 -- the minor bump
above the last published release, 0.1.5-alpha. No 0.2.0 tag had ever been
cut, so nothing is superseded.

### Added

- Package-level doc.go for every package.

## [0.1.5-alpha] - 2026-06-15

Cross-transport consistency, SDK hardening, and security fixes across the
REST/gRPC/QUIC/MCP/Unix servers and all six SDKs (Go, TypeScript, Python,
Ruby, Rust, C#).

### Breaking / wire-visible

- MCP transport is binary-safe: object data is base64-encoded in both
  directions; non-base64 `objstore_put` data is rejected.
- REST/QUIC `DELETE` returns 204 No Content (SDKs still accept the legacy 200).
- JSON-RPC (MCP, Unix) uses canonical codes: -32001 forbidden,
  -32002 unauthenticated, -32003 unavailable, -32004 not found,
  -32005 already exists, -32029 rate limited.
- REST `/metrics` requires authorization by default (`--metrics-public` opts out).
- Unix `add_policy` accepts exact `retention_seconds`; MCP default address is `:8081`.

### Added

- Shared cross-transport error taxonomy and JSON-RPC envelope package, with
  full typed-error parity across all six SDKs (HTTP, JSON-RPC, and gRPC codes
  map to typed errors on every transport).
- Cross-protocol conformance suite plus per-SDK unit and e2e-smoke tests in CI.
- Middleware parity (rate limiting, audit logging, request-ID tracking) on the
  QUIC, MCP, and Unix transports.
- Real implementations for previously stubbed/no-op paths: Azure
  GetMetadata/UpdateMetadata, GCS UpdateMetadata/RemovePolicy, Glacier
  multipart upload, and the pool least-utilization strategy.

### Security

- gRPC mTLS now verifies the peer chain against RequiredRoots and preserves
  VerifiedChains (previously any presented certificate minted a principal).
- Python and Ruby QUIC clients verify TLS certificates by default.
- Swagger requires authentication; REST list is authorized as the list action;
  MCP error text is sanitized; the Unix server bounds connections and refuses
  to delete a non-socket path.

### Fixed

- SDK unix clients use one persistent, response-ID-validated connection with
  automatic reconnect (was a fresh socket per request; the Go client could
  return a stale reply).
- Unix policy listing, TypeScript QUIC auth headers, code-based JSON-RPC error
  mapping, audit request-ID correlation, graceful-shutdown races, TLS-disabled
  startup panics, and QUIC error-taxonomy mapping.
- Genuine HTTP/3 CLI QUIC client, plus numerous SDK streaming, connection-reuse,
  and resource-leak fixes.

### Changed

- Unit-test coverage raised to 98.6% across all packages.
- Internal deduplication of auth/JSON-RPC/HTTP helpers in every SDK; dead code
  removed; `docs/configuration` rewritten around the real server flags.
- Go toolchain 1.21 to 1.26.4; the JavaScript SDK is consolidated into the
  TypeScript package; the encryption example uses stdlib AES-256-GCM (the
  go-xkms dependency was removed).

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
