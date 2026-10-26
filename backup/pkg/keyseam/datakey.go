package keyseam

import (
	"context"
	"crypto/rand"
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"
)

const (
	opGenerate = "generate data key"
	opOpen     = "open data key"
	opClose    = "close data key"
)

// DataKey is a repository's unwrapped data key, held in go-enclave locked
// memory. The repository that opened it closes it, which destroys the key.
type DataKey struct {
	h enclave.ByteAccess
}

// NewDataKey generates a random data key in locked memory and wraps it with p.
// It returns the key and its wrapped form, which the caller persists. On error
// no key survives.
func NewDataKey(ctx context.Context, p Provider) (*DataKey, []byte, error) {
	if p == nil {
		return nil, nil, newError(opGenerate, ErrNilProvider, nil)
	}
	h, err := generate()
	if err != nil {
		return nil, nil, newError(opGenerate, ErrInvalidKey, err)
	}
	wrapped, err := p.WrapKey(ctx, h)
	if err != nil {
		return nil, nil, errors.Join(err, destroy(opGenerate, h))
	}
	return &DataKey{h: h}, wrapped, nil
}

// OpenDataKey unwraps wrapped with p into locked memory. Errors are p's, or
// ErrInvalidKey when p returns a key of the wrong size.
func OpenDataKey(ctx context.Context, p Provider, wrapped []byte) (*DataKey, error) {
	if p == nil {
		return nil, newError(opOpen, ErrNilProvider, nil)
	}
	h, err := p.UnwrapKey(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, newError(opOpen, ErrInvalidKey, nil)
	}
	if h.Size() != DataKeySize {
		return nil, errors.Join(newError(opOpen, ErrInvalidKey, nil), destroy(opOpen, h))
	}
	return &DataKey{h: h}, nil
}

// Handle returns the locked-memory handle holding the key. It is borrowed: it
// stays valid until Close, and the caller must not destroy it.
func (k *DataKey) Handle() enclave.ByteAccess {
	return k.h
}

// Wrap wraps the key again with p, for example under a new passphrase.
func (k *DataKey) Wrap(ctx context.Context, p Provider) ([]byte, error) {
	if p == nil {
		return nil, newError("wrap data key", ErrNilProvider, nil)
	}
	return p.WrapKey(ctx, k.h)
}

// Close destroys the key. It is idempotent; after it every use of the key or
// its handle fails with an error matching ErrKeyDestroyed.
func (k *DataKey) Close() error {
	return destroy(opClose, k.h)
}

// generate returns DataKeySize random bytes produced directly in locked memory,
// frozen read-only.
func generate() (enclave.ByteAccess, error) {
	h, err := enclave.NewMemorySized(DataKeySize)
	if err != nil {
		return nil, err
	}
	err = h.WithBytes(func(key []byte) error {
		_, rerr := rand.Read(key)
		return rerr
	})
	if err == nil {
		var f enclave.Freezer
		if enclave.As(h, &f) {
			err = f.Freeze()
		}
	}
	if err != nil {
		return nil, errors.Join(err, h.Destroy())
	}
	return h, nil
}

// destroy destroys h, returning nil or an *Error.
func destroy(op string, h enclave.Handle) error {
	if err := h.Destroy(); err != nil {
		return &Error{Op: op, Err: ErrKeyDestroyed, Cause: err}
	}
	return nil
}
