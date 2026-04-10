# Style and Conventions

- Standard Go conventions (gofmt enforced)
- Facade pattern for centralized storage API
- Build tags for conditional backend compilation
- Each backend in its own `pkg/` sub-package
- Factory pattern for runtime backend selection
- Docker Compose for integration tests (emulators: MinIO, Azurite, fake-gcs)
- golangci-lint with custom `.golangci.yml` config
- gosec with `.gosec.yaml` config
- Pre-commit script in `scripts/pre-commit`
- Coverage goal: >= 90% per package
- Dual license: AGPL-3.0 (open source) / Commercial
