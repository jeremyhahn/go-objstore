### Changed

- Every third-party module the root, `backup`, `api/sdks/go` and
  `examples/encryption` go.mod files require, direct and indirect, is at its
  latest release, so `make deps-check` passes on push again. Among them:
  `golang.org/x/crypto` v0.58.0, `golang.org/x/net` v0.61.0,
  `google.golang.org/grpc` v1.84.0 and `google.golang.org/api` v0.309.0.
  The in-repo `go-objstore` requires of the three nested modules name
  v0.8.0-alpha and keep their local `replace`.
- `github.com/jeremyhahn/go-enclave` moves to v0.10.0-alpha in the root,
  `backup` and `examples/encryption` modules. That release changes no Go API.
  `backup`'s require of the in-repo `api/sdks/go` module is held at its zero
  pseudo-version, which the local `replace` overrides.
