// Package keyseam wraps and unwraps a repository's data key through a KEK Provider and keeps the unwrapped key in go-enclave locked memory.
package keyseam

import (
	"context"

	enclave "github.com/jeremyhahn/go-enclave"
)

// DataKeySize is the length in bytes of a repository data key.
const DataKeySize = 32

// Provider wraps and unwraps a data key under its KEK, never retains it, is safe for concurrent use and returns ErrUnwrap, ErrMalformedWrappedKey, ErrKeyDestroyed, ErrInvalidKey or ctx.Err().
type Provider interface {
	WrapKey(ctx context.Context, dataKey enclave.ByteAccess) ([]byte, error)
	UnwrapKey(ctx context.Context, wrapped []byte) (enclave.ByteAccess, error)
}
