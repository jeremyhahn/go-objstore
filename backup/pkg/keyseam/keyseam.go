// Package keyseam is the backup agent's key seam: the interface through which a
// key-encryption key (KEK) provider wraps and unwraps a repository's data key,
// the data key's locked-memory lifecycle, and the standalone provider that
// derives its KEK from a passphrase.
//
// A repository is encrypted under one random 32-byte data key. Only its wrapped
// form is ever persisted; the unwrapped key lives in go-enclave locked memory
// from the moment it is generated or unwrapped until DataKey.Close destroys it.
// A provider backed by a key-management service implements Provider outside
// this module, which never depends on one.
package keyseam

import (
	"context"

	enclave "github.com/jeremyhahn/go-enclave"
)

// DataKeySize is the length in bytes of a repository data key.
const DataKeySize = 32

// Provider wraps and unwraps a repository data key under a key-encryption key
// the provider holds.
//
// WrapKey reads the data key through its handle and returns the wrapped form,
// the only form of the key that may be persisted. UnwrapKey returns a new
// locked-memory handle holding the data key; the caller owns it and destroys
// it. A provider never retains the data key or a handle to it.
//
// Implementations are safe for concurrent use, honor ctx, and return errors
// that carry no key material:
//
//   - a wrapped key that does not authenticate under the provider's KEK (a
//     wrong KEK, a tampered wrapped key) matches ErrUnwrap;
//   - a wrapped key the provider cannot parse matches ErrMalformedWrappedKey;
//   - a destroyed data key handle, or a provider whose own key material is
//     destroyed, matches ErrKeyDestroyed;
//   - a data key handle of the wrong size matches ErrInvalidKey;
//   - a cancelled or expired ctx matches ctx.Err().
type Provider interface {
	WrapKey(ctx context.Context, dataKey enclave.ByteAccess) ([]byte, error)
	UnwrapKey(ctx context.Context, wrapped []byte) (enclave.ByteAccess, error)
}
