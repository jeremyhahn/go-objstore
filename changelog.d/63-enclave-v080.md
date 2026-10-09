### Changed

- Adopted `github.com/jeremyhahn/go-enclave` v0.8.0-alpha (was v0.3.0-alpha),
  issue #63. The module graph has no memguard or memcall in the root module,
  `api/sdks/go` or `examples/encryption`. go-enclave requires Go 1.27.1, so the
  `go` directive, the CI `GO_VERSION`, the Dockerfile base images and the
  documented minimum Go version move from 1.27.0 to 1.27.1.
- **BREAKING:** `cli.Config.Destroy` and `adapters.TLSConfig.Destroy` now return
  an `error`, the enclave release report that go-enclave v0.8.0-alpha's
  `MemoryEnclave.Destroy` returns. `cli.Config.SetBackendKeyEnclave` and
  `SetBackendSecretEnclave` return the same report for the enclave they
  replaced. A `defer cfg.Destroy()` still compiles; the material is wiped
  whatever the error says.
- The credential read path copies a secret out through `WithBytes` instead of the
  removed `MemoryEnclave.OpenString`, and `SetBackendKeyFromString` /
  `SetBackendSecretFromString` build their enclaves with
  `NewMemoryEnclave([]byte(s))` instead of the removed
  `NewMemoryEnclaveFromString`.
- `docs/architecture/protected-memory.md` describes the v0.8.0-alpha
  contract: no slice-returning accessor, frozen enclaves, `EqualTo` returning
  `(bool, error)`, and `Destroy` returning an error.
