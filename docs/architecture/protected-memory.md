# Protected Memory

Keys, passphrases, tokens and credentials live in protected storage for their
whole lifetime in the process and are zeroized on release — never in a plain
`[]byte` or `string`.

The primitive is `pkg/enclave.MemoryEnclave`: a region of locked, zeroized
memory backed by a memguard `LockedBuffer`. The bytes sit on `mlock`'d pages
(never paged to swap), fenced by inaccessible guard pages, checked by a canary,
and wiped when the enclave is destroyed. memguard disables core dumps
process-wide when the package is imported.

```go
e, err := enclave.NewMemoryEnclave(keyPEM) // keyPEM is wiped
if err != nil {
    return err
}
defer e.Destroy()

err = e.WithBytes(func(key []byte) error {
    cert, err := tls.X509KeyPair(certPEM, key)
    ...
})
```

## Lifetime and concurrency

`Destroy` wipes and **unmaps** the pages. A borrow that outlives it is not a
stale read, it is a read of unmapped memory — the process faults, and no error
is returned to catch. Swapping the buffer handle under an atomic would keep
`IsAlive` honest and still leave the borrower on freed memory, so the
discipline is a lock over the whole access, not a flag:

- `WithBytes`, `OpenString`, `EqualTo`, `Size`, `IsAlive` and `String` hold the
  enclave's read lock across the entire access.
- `Destroy` takes the write lock, so it **blocks** until every in-flight borrow
  has returned. It never wipes memory a reader is still holding.
- A package-wide `purgeMu` serializes every **purge-capable** operation against
  every borrow. Purge-capable means allocation *and* `Destroy`: memguard's
  `Buffer.Destroy` calls `core.Panic` if releasing the pages fails
  (core/buffer.go:180-183), and Panic purges. Both take `purgeMu` exclusively.
  memguard's allocation-failure path calls `core.Purge` before it panics, and
  Purge destroys every live buffer in the process — ending in `memcall.Free`,
  so the pages are unmapped — while holding only memguard's own locks. Without
  this, a goroutine parked in `WithBytes` on one enclave could have its pages
  unmapped by an unrelated goroutine exhausting locked memory on another, which
  faults the process rather than returning an error. Allocation takes `purgeMu`
  exclusively; borrows take it shared, so they still run concurrently with each
  other. A subprocess fault-injection test parks a borrow across a foreign
  allocation failure; without the coordination it reproduces as a SIGSEGV.
- Consequently `fn` passed to `WithBytes` must not construct, destroy, or
  borrow ANY enclave — not merely the one it is borrowing. Go's `RWMutex`
  blocks new readers once a writer is queued, so a nested borrow of a second
  enclave deadlocks against an allocation that began after the outer borrow.
- `Open` stands outside all of this by construction: the slice it returns is
  covered by no lock, so a purge can unmap it with no `Destroy` anywhere in
  the picture. That is the third reason its documentation calls it unsafe.
- `fn` passed to `WithBytes` must not call back into the same enclave: the read
  lock is held for the duration of the call.

`Open` is the one exception, and is documented as such. It returns a slice
aliasing the locked pages that is no longer covered by the lock, so it is safe
only where the caller can establish that no `Destroy` runs concurrently. Prefer
`WithBytes` — every consumer in this repository uses the scoped forms.

## Selecting the protected path removes the plain copy

Choosing an enclave does not merely change which field is authoritative — it
zeroizes the one it supersedes. `TLSConfig.WithServerKeyEnclave` wipes and
clears `ServerKeyPEM`, and the `cli.Config` setters clear `BackendKey` /
`BackendSecret`, so a caller migrating incrementally off the deprecated API
does not leave a complete private key or credential resident in ordinary heap
memory for the object's lifetime. `Build` ignoring the old copy is not the same
as the old copy being gone.

`TLSConfig` carries three mutually exclusive server-key sources — enclave, PEM
and files — and `Build` ranks the enclave first, so each setter selects exactly
one and clears the rest. Reconfiguring a config that already holds an enclave
destroys it; without that, a later `WithServerCertFiles` or `WithServerCertPEM`
would leave the old enclave attached and `Build` would keep serving the
superseded key, which is a certificate rotation that appears to have happened
and has not.

`WithServerCertPEM` copies the key it is handed, so the config owns its plain
copy outright: replacing or destroying a source wipes only memory this
repository allocated, never the caller's array, and a replacement that aliases
the value being replaced cannot be zeroized on its way in. A caller holding its
own copy is responsible for wiping it.

## Fail-closed on the credential path

Once an enclave has been selected for a credential or key, it is the **only**
source for it. A destroyed enclave reports `ErrEnclaveDestroyed`; it never
falls back to the deprecated plain field beside it.

This matters because an enclave can be destroyed without this code asking:
memguard purges every live locked buffer when the process runs out of lockable
memory (see *Failure behavior*). If a stale value were still sitting in
`BackendKey` or `ServerKeyPEM` at that moment, a fallback would silently
authenticate with unprotected material the caller believed it had replaced.

- `cli.Config.StorageSettings` returns the error. `GetStorageSettings` cannot —
  its signature predates this — so it is deprecated and **omits** the
  credential instead, and the backend fails to authenticate rather than using
  the stale one.
- `adapters.TLSConfig.Build` returns `ErrEnclaveDestroyed` rather than serving
  TLS from `ServerKeyPEM`.

The deprecated plain fields are still read normally when **no** enclave was
ever set; the rule closes a fallback, not the compatibility path.

## Naming

The type is `MemoryEnclave`, and derived names come from it
(`NewMemoryEnclave`, `Open`, `Destroy`, `WithServerKeyEnclave`,
`BackendKeyEnclave`). The name describes the mechanism — a region of locked
memory.

`Secret` is deliberately **not** used: in this codebase it already means a
configuration value and a secret *reference*, so a reader cannot tell which is
meant. `SecureString`, `SecureBytes` and `ProtectedBuffer` are likewise not
used. One vocabulary across the portfolio is the point — it is what makes a
later audit a `grep`.

## FIPS

A `MemoryEnclave` performs no cryptography. Only the memguard `LockedBuffer`
path is used, never `Enclave`/`Seal`, whose sealing uses NaCl secretbox — not a
FIPS 140-approved algorithm. The repository's CI runs the whole unit suite
under `GODEBUG=fips140=only`, including this package.

## Failure behavior

`mlock` is bounded by `RLIMIT_MEMLOCK`. memguard aborts the process when it
cannot lock its pages; a foundational library must not do that to its host, so
`NewMemoryEnclave` converts the abort into a `*LockedMemoryError`, which
carries memguard's own diagnostic — the failing address and the kernel errno,
which is what separates a memlock ceiling from any other allocation failure.
Callers match the `ErrLockedMemoryUnavailable` sentinel, which `errors.Is`
reports for every `LockedMemoryError`; the struct is diagnostic, not contract. Note
that memguard purges every live locked buffer before that abort, so enclaves
elsewhere in the process report `ErrEnclaveDestroyed` afterwards. Deployments
that hold many enclaves should raise `RLIMIT_MEMLOCK` accordingly.

The ownership contract holds on that path too: `NewMemoryEnclave` wipes the
caller's slice before **every** return, failures included. memguard moves and
wipes its source only after a successful allocation, so without an explicit
wipe the secret would survive in the caller's memory on precisely the
exhaustion path this handling exists for. A subprocess fault-injection test
lowers `RLIMIT_MEMLOCK` to one page and asserts both the typed error and the
wiped source.

## Migration status

Protected entry points are **additive**. The plain-memory entry points remain,
deprecated, until the coordinated portfolio-wide breaking change; consumers are
not broken by this release.

| Site | Deprecated (plain) | Protected replacement |
|---|---|---|
| `adapters.TLSConfig` server key | `ServerKeyPEM`, `WithServerCertPEM` | `WithServerKeyEnclave`, `ServerKeyEnclave`, `Destroy` |
| `cli.Config` backend credentials | `BackendKey`, `BackendSecret`, credentials from `GetConfig` | `GetConfigProtected`, `SetBackendKeyEnclave`, `SetBackendSecretEnclave`, `Destroy` |

Where both are set, the enclave wins. This repository's own callers use the
protected path: the `objstore` CLI loads its configuration with
`GetConfigProtected`, and `TLSConfig.Build` reads a key file into an enclave
rather than through `tls.LoadX509KeyPair`.

**What "protected" claims, and what it does not.** These entry points protect
*custody within this repository*: between the boundaries listed below, a
credential lives only in locked, zeroized memory and is never held as a plain
string in a struct for the process lifetime, which is exactly what the
deprecated fields did. They are not end-to-end protection. `StorageSettings`
materializes each credential as a Go string again at `Configure`, and the
backend and its vendor SDK retain copies of that string for their own
lifetime; `Config.Destroy` wipes the enclaves and cannot reach those copies.
Read the boundary list as part of the guarantee, not as a footnote to it.

## Residual plain-memory boundaries

These are boundaries with APIs this repository does not own. They are recorded
here rather than silently left:

- **Backend `Configure(map[string]string)`** — `pkg/s3`, `pkg/minio`,
  `pkg/azure`, `pkg/azurearchive` receive credentials as plain map values and
  hand them to vendor SDK credential types (`credentials.NewStaticCredentials`,
  `azblob.NewSharedKeyCredential`) that keep plain copies. `cli.Config`
  materializes a credential only at this call, never for the process lifetime.
  Closing this requires changing the `Storage` interface, which is a breaking
  Layer 0/1 change reserved for the coordinated wave — and the vendor SDKs
  would still hold plain copies. Tracked as issue #16.
- **Flag and configuration parsing** — cobra and viper hand out `string`
  values, which cannot be wiped. One unprotected copy therefore exists between
  the parser and the enclave.
- **`crypto/tls` and `crypto/x509`** — `tls.X509KeyPair` parses the enclaved
  PEM into a `crypto.PrivateKey` held in the returned `tls.Certificate`, and
  the RSA or ECDSA private parameters then live in ordinary, pageable Go heap
  memory for the lifetime of the `tls.Config`. Destroying the enclave removes
  the PEM encoding, not the parsed key. **This is a custody stop, not an
  oversight**: Go's TLS stack signs with an in-heap `crypto.Signer`, so no
  Layer 0 library can close it without either a protected `crypto.Signer`
  backed by a KMS/HSM or a custom TLS stack — both architecture decisions
  above this repository. What the enclave buys is that the key spends its life
  at rest in locked memory and is exposed to the parser for the duration of
  one call, instead of sitting in a plain `[]byte` field for the process
  lifetime.
