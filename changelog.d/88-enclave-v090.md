### Changed

- **BREAKING:** Adopted `github.com/jeremyhahn/go-enclave` v0.9.0-alpha (was
  v0.8.0-alpha), issue #88, which makes the memory enclave's type internal.
  `adapters.TLSConfig.WithServerKeyEnclave` and `ServerKeyEnclave`, and
  `cli.Config.SetBackendKeyEnclave`, `SetBackendSecretEnclave`,
  `BackendKeyEnclave` and `BackendSecretEnclave` take and return
  `enclave.ByteAccess`, was `*enclave.MemoryEnclave`; build one with
  `enclave.NewMemory`. go-enclave's requirement raises `golang.org/x/sys` to
  v0.49.0. `docs/architecture/protected-memory.md` uses the new names.
