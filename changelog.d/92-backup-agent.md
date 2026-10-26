### Added

- The backup agent's module, `backup/` (`github.com/jeremyhahn/go-objstore/backup`,
  its own go.mod), issue #92. It consumes the Go client SDK module
  (`api/sdks/go`) and go-enclave, and nothing else first-party; it never
  imports go-xkms. A stub `objstore-backup` command builds and prints usage;
  it has no backup commands yet.
- The key seam, `backup/pkg/keyseam`: `Provider` with
  `WrapKey(ctx, dataKey enclave.ByteAccess) ([]byte, error)` and
  `UnwrapKey(ctx, wrapped []byte) (enclave.ByteAccess, error)`, which a go-xkms
  client implements outside this repository. Errors are `*keyseam.Error`,
  matching `ErrUnwrap`, `ErrMalformedWrappedKey`, `ErrKeyDestroyed` or
  `ErrInvalidKey`, and never carry key material. `DataKey` holds the
  unwrapped repository data key in go-enclave locked memory until `Close`
  destroys it; only the wrapped form is persisted.
- The standalone provider, `keyseam.PassphraseProvider`: the KEK is derived
  with PBKDF2-HMAC-SHA-256 (`crypto/pbkdf2`) from a passphrase in locked
  memory, over a random 16-byte salt stored in the wrapped key's
  authenticated header, with at least 600000 iterations, and held in locked
  memory; the data key is wrapped with AES-256-GCM.
- The chunk and seal core: a FastCDC chunker (`backup/pkg/chunker`, standard
  library only, min, average and max sizes configurable, deterministic);
  chunk IDs as HMAC-SHA-256 under a subkey of the data key; sealed objects
  (`backup/pkg/seal`) as AES-256-GCM with a random 96-bit nonce under
  per-purpose HKDF-SHA-256 subkeys (chunk ID, pack, index, snapshot), the
  additional data binding the object's type and ID. The sealed format
  records a compression identifier; only `none` is implemented.
- `make backup-lint` and `make backup-test` gate the module, which the root
  `make lint` and `make test` do not reach.
