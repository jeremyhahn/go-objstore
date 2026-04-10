# Task Completion Checklist

1. `make test` - Run unit tests
2. `make lint` - Run golangci-lint
3. `make security` - Run security checks
4. `make build` - Verify build succeeds
5. If integration-relevant: `make integration-test` or specific backend test
6. If proto changes: `make generate-proto`
7. Or just: `make ci-local` for the full pipeline
