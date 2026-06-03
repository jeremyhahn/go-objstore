# Suggested Commands

## Build
- `make build` - Build library, CLI, servers, and shared library
- `make build-cli` - Build CLI only (`bin/objstore`)
- `make build-server` - Build all server binaries
- `make lib` - Build C shared library (`bin/libobjstore.so`)
- `make generate-proto` - Generate protobuf/gRPC code

## Testing
- `make test` - Unit tests with coverage (fast, in-memory)
- `make integration-test` - All integration tests (Docker Compose)
- `make integration-test-local` - Local storage integration tests
- `make integration-test-s3` - S3/MinIO integration tests
- `make integration-test-azure` - Azure/Azurite integration tests
- `make integration-test-gcs` - GCS emulator integration tests
- `make integration-test-cli` - CLI integration tests
- `make test-servers` - Server integration tests (gRPC, REST, QUIC, MCP)
- `make integration-test-all` - All integration tests including servers
- `make test-sdks` - All SDK unit tests
- `make test-sdk-<lang>` - Per-language SDK tests (typescript, go, python, ruby, rust, csharp)

## Code Quality
- `make lint` - golangci-lint
- `make security` - gosec + govulncheck
- `make pre-commit` - All pre-commit checks
- `make ci-local` - Full local CI (test + lint + security + build)
- `make ci-local-full` - CI + integration tests

## Coverage
- `make coverage-check` - Per-package coverage report
- `make coverage-report` - Merged coverage report

## Backend Selection
- `make build WITH_AWS=1 WITH_GCP=1 WITH_AZURE=1` - All backends
- `make build WITH_AWS_S3=0 WITH_AZURE_BLOB=0` - Exclude specific backends

## Versioning
- `make version` - Show current version
- `make version-bump-{patch,minor,major}` - Bump version
- `make release` - Create git tag
- `make release-push` - Push tag to origin

## Clean
- `make clean` - Clean all artifacts
