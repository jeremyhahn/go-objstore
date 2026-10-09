# Protected Memory

Keys, passphrases, tokens and credentials live in protected storage for their
whole lifetime in the process and are zeroized on release — never in a plain
`[]byte` or `string`.

The primitive is `MemoryEnclave` from
[`github.com/jeremyhahn/go-enclave`](http://198.51.100.91:3001/jeremyhahn/go-enclave):
a region of locked, zeroized memory that go-enclave maps itself. The bytes sit
on `mlock`'d pages (never paged to swap), fenced by inaccessible guard pages,
preceded by a random canary, and wiped when the enclave is destroyed. Core dumps
are kept out at two levels: `RLIMIT_CORE` is lowered process-wide on import (best
effort, reported by `CoreDumpsDisabled` rather than assumed) and every enclave's
pages are additionally marked `MADV_DONTDUMP` (and, on Linux, `MADV_DONTFORK`,
so a forked child has no mapping there). This repository builds against
go-enclave v0.8.0-alpha, which maps the pages itself with `golang.org/x/sys`;
there is no memguard or memcall in the module graph.

The primitive lived in this repository as `pkg/enclave` until 2026-08-25. It was
extracted to its own Layer 0 module (issue #29, owner-ruled 2026-08-23) so that
libraries which do not sit above go-objstore can consume it; go-objstore is now
an ordinary consumer. **go-enclave is the normative source for the mechanism.**
What this document specifies is how *this repository* uses it — the sections
below on borrow discipline are the contract go-objstore's own call sites honour,
and go-enclave's own documentation governs if the two ever disagree.

```go
import "github.com/jeremyhahn/go-enclave"

e, err := enclave.NewMemoryEnclave(keyPEM) // keyPEM is wiped
if err != nil {
    return err
}
defer func() { _ = e.Destroy() }() // wipes first; a refused release is retried by WipeAll

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

- `WithBytes`, `EqualTo`, `Size`, `IsAlive` and `String` hold the
  enclave's read lock across the entire access. Strings are read through
  `WithBytes`; `OpenString` and `Open` no longer exist, and string material
  comes in as `NewMemoryEnclave([]byte(s))`.
- `Destroy` takes the write lock, so it **blocks** until every in-flight borrow
  has returned. It never wipes memory a reader is still holding. It returns an
  error (never panics): `ErrEnclaveCorrupted` when the guards were overwritten,
  after the material is wiped. `Config.Destroy` and `TLSConfig.Destroy` return
  that error.
- There is no process-wide coordination and no shared purge lock. Because
  go-enclave maps its own memory, nothing in the process can unmap an enclave it
  does not own: an allocation that hits the `RLIMIT_MEMLOCK` ceiling fails only
  itself, returning `ErrLockedMemoryUnavailable`, and every other live enclave
  keeps its pages.
- `fn` passed to `WithBytes` must not call back into the **same** enclave: the
  read lock is held for the duration of the call, so a nested `Destroy`
  deadlocks and a nested read deadlocks whenever a `Destroy` is already waiting
  for the write lock. It **may** construct, destroy or borrow **other** enclaves
  — there is no shared purge lock to forbid it.
- `MemoryEnclave` has no accessor that returns a slice outliving the lock: the
  old `Open`, `OpenString` and `NewMemoryEnclaveFromString` were removed in
  go-enclave v0.8.0-alpha for exactly that reason. Every read is a `WithBytes`
  borrow, and string material enters through `NewMemoryEnclave([]byte(s))`.
- An enclave built by `NewMemoryEnclave` is frozen (read-only): a write through
  the `WithBytes` view faults. `EqualTo` returns `(bool, error)` — a destroyed
  enclave is `ErrEnclaveDestroyed`, never a bare `false`.
- `Destroy` returns an error: `nil`, `ErrReleasedWithoutWipe`,
  `ErrLockedMemoryUnavailable` (a refused release; the next `Destroy` or
  `WipeAll` retries) or `ErrEnclaveCorrupted`. The material is wiped in every
  case. `cli.Config.Destroy` and `adapters.TLSConfig.Destroy` return it, and
  `cli.Config.SetBackendKeyEnclave` / `SetBackendSecretEnclave` return the
  report of the enclave they replaced. The fluent `TLSConfig.With*` setters have
  no error to return, so they discard it; the next `WipeAll` still reports it.

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

This matters because an enclave can be destroyed independently of the code
reading it — an explicit `Destroy` during credential rotation, or a `Config`
teardown — after which the plain field beside it may still hold a stale value.
If a fallback read that field, it would silently authenticate with unprotected
material the caller believed it had replaced.

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
(`NewMemoryEnclave`, `WithBytes`, `Destroy`, `WithServerKeyEnclave`,
`BackendKeyEnclave`). The name describes the mechanism — a region of locked
memory.

`Secret` is deliberately **not** used: in this codebase it already means a
configuration value and a secret *reference*, so a reader cannot tell which is
meant. `SecureString`, `SecureBytes` and `ProtectedBuffer` are likewise not
used. One vocabulary across the portfolio is the point — it is what makes a
later audit a `grep`.

## FIPS

A `MemoryEnclave` performs no cryptography. It maps, locks and wipes memory; the
only cryptographic primitive anywhere near it is `crypto/rand` filling the guard
canary, a FIPS-approved DRBG. go-enclave has no memguard in its graph, so the
non-approved NaCl secretbox that memguard's unused `Enclave`/`Seal` paths
carried is not in this repository's build graph. go-enclave's own CI covers the
primitive under `GODEBUG=fips140=only`; this repository's FIPS job runs its whole
unit suite — every consumer of the enclave included — under the same setting.

## Failure behavior

`mlock` is bounded by `RLIMIT_MEMLOCK`. When the kernel refuses to map or lock
the pages — most often at that ceiling — `NewMemoryEnclave` releases whatever it
had already taken and returns a `*LockedMemoryError` carrying the underlying
`mmap`/`mlock`/`mprotect` cause and the kernel errno, which is what separates a
memlock ceiling from any other allocation failure. Callers match the
`ErrLockedMemoryUnavailable` sentinel, which `errors.Is` reports for every
`LockedMemoryError`; the struct is diagnostic, not contract. The failure touches
only the allocation that hit the ceiling: because go-enclave owns its mapping and
nothing can purge it, enclaves elsewhere in the process are unaffected.
Deployments that hold many enclaves should raise `RLIMIT_MEMLOCK`
accordingly.

The ownership contract holds on that path too: `NewMemoryEnclave` wipes the
caller's slice before **every** return, failures included — so even on the
mapping-failure path, where nothing was copied out, the secret does not survive
in the caller's memory on precisely the resource-exhaustion case this handling
exists for. A subprocess fault-injection test in go-enclave lowers
`RLIMIT_MEMLOCK` to one page and asserts both the typed error and the wiped
source.

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

- **Backend `Configure(map[string]string)`** — `pkg/s3`,
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
