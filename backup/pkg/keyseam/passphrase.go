package keyseam

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/backup/internal/gcm"
)

const (
	// MinIterations is the fewest PBKDF2 iterations the passphrase provider
	// derives with or accepts in a wrapped key.
	MinIterations = 600_000

	// MaxIterations is the most PBKDF2 iterations a wrapped key may demand, so
	// a forged wrapped key cannot make unwrapping run for hours.
	MaxIterations = 100_000_000

	// SaltSize is the length in bytes of the random PBKDF2 salt.
	SaltSize = 16

	opWrap   = "wrap"
	opUnwrap = "unwrap"

	passphraseMagic   = "OBKP"
	passphraseVersion = 1
	// passphraseHeaderSize is magic, version, iterations and salt.
	passphraseHeaderSize = len(passphraseMagic) + 1 + 4 + SaltSize
	passphraseWrapSize   = passphraseHeaderSize + DataKeySize + gcm.Overhead
)

// PassphraseProvider derives the KEK with PBKDF2-HMAC-SHA-256 over a random salt and wraps with AES-256-GCM: "OBKP" | version (1) | iterations (4, BE) | salt (16) | nonce (12) | sealed key (32) | tag (16), header as AAD.
type PassphraseProvider struct {
	passphrase enclave.ByteAccess
	iterations int
}

var _ Provider = (*PassphraseProvider)(nil)

// NewPassphraseProvider takes ownership of passphrase (destroyed on Close); iterations 0 means MinIterations.
func NewPassphraseProvider(passphrase enclave.ByteAccess, iterations int) (*PassphraseProvider, error) {
	const op = "new passphrase provider"
	if iterations == 0 {
		iterations = MinIterations
	}
	if passphrase == nil {
		return nil, newError(op, ErrInvalidKey, nil)
	}
	if !passphrase.IsAlive() {
		return nil, newError(op, ErrKeyDestroyed, nil)
	}
	if iterations < MinIterations || iterations > MaxIterations {
		return nil, newError(op, ErrIterations, nil)
	}
	return &PassphraseProvider{passphrase: passphrase, iterations: iterations}, nil
}

// Close destroys the passphrase. After it, WrapKey and UnwrapKey fail with
// ErrKeyDestroyed.
func (p *PassphraseProvider) Close() error {
	return destroy("close passphrase provider", p.passphrase)
}

// WrapKey wraps dataKey under a KEK derived from the passphrase and a new salt.
func (p *PassphraseProvider) WrapKey(ctx context.Context, dataKey enclave.ByteAccess) (wrapped []byte, err error) {
	if err = ctx.Err(); err != nil {
		return nil, &Error{Op: opWrap, Err: err}
	}
	switch {
	case dataKey == nil:
		return nil, newError(opWrap, ErrInvalidKey, nil)
	case !dataKey.IsAlive():
		return nil, newError(opWrap, ErrKeyDestroyed, nil)
	case dataKey.Size() != DataKeySize:
		return nil, newError(opWrap, ErrInvalidKey, nil)
	}

	header := make([]byte, passphraseHeaderSize)
	copy(header, passphraseMagic)
	header[len(passphraseMagic)] = passphraseVersion
	binary.BigEndian.PutUint32(header[len(passphraseMagic)+1:], uint32(p.iterations)) //nolint:gosec // bounded by MaxIterations
	salt := header[passphraseHeaderSize-SaltSize:]
	if _, err = rand.Read(salt); err != nil {
		return nil, newError(opWrap, ErrInvalidKey, err)
	}

	kek, err := p.deriveKEK(ctx, opWrap, salt, p.iterations)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, destroy(opWrap, kek)) }()

	aead, err := gcm.New(kek)
	if err != nil {
		return nil, newError(opWrap, ErrInvalidKey, err)
	}
	out := make([]byte, passphraseHeaderSize, passphraseWrapSize)
	copy(out, header)
	err = dataKey.WithBytes(func(key []byte) error {
		out = aead.Seal(out, nil, key, header) //nolint:gosec // G407: the AEAD draws a random nonce itself
		return nil
	})
	if err != nil {
		return nil, newError(opWrap, ErrInvalidKey, err)
	}
	return out, nil
}

// UnwrapKey parses wrapped, derives its KEK from the passphrase and the stored
// salt and iteration count, and opens the data key into locked memory.
func (p *PassphraseProvider) UnwrapKey(ctx context.Context, wrapped []byte) (key enclave.ByteAccess, err error) {
	if err = ctx.Err(); err != nil {
		return nil, &Error{Op: opUnwrap, Err: err}
	}
	if len(wrapped) != passphraseWrapSize ||
		!bytes.Equal(wrapped[:len(passphraseMagic)], []byte(passphraseMagic)) ||
		wrapped[len(passphraseMagic)] != passphraseVersion {
		return nil, newError(opUnwrap, ErrMalformedWrappedKey, nil)
	}
	iterations := int(binary.BigEndian.Uint32(wrapped[len(passphraseMagic)+1:]))
	if iterations < MinIterations || iterations > MaxIterations {
		return nil, newError(opUnwrap, ErrMalformedWrappedKey, nil)
	}
	header := wrapped[:passphraseHeaderSize]
	salt := header[passphraseHeaderSize-SaltSize:]

	kek, err := p.deriveKEK(ctx, opUnwrap, salt, iterations)
	if err != nil {
		return nil, err
	}
	defer func() {
		if derr := destroy(opUnwrap, kek); derr != nil {
			// Return no key with an error: the caller would not destroy it.
			if key != nil {
				derr = errors.Join(derr, key.Destroy())
				key = nil
			}
			err = errors.Join(err, derr)
		}
	}()

	aead, err := gcm.New(kek)
	if err != nil {
		return nil, newError(opUnwrap, ErrInvalidKey, err)
	}
	plain, err := aead.Open(nil, nil, wrapped[passphraseHeaderSize:], header)
	if err != nil {
		// The AEAD's own error says only that authentication failed.
		return nil, newError(opUnwrap, ErrUnwrap, nil)
	}
	h, err := enclave.NewMemory(plain) // wipes plain on every path
	if err != nil {
		return nil, newError(opUnwrap, ErrInvalidKey, err)
	}
	return h, nil
}

// deriveKEK derives the key-encryption key for salt into locked memory.
func (p *PassphraseProvider) deriveKEK(ctx context.Context, op string, salt []byte, iterations int) (enclave.ByteAccess, error) {
	var kek []byte
	err := p.passphrase.WithBytes(func(pw []byte) error {
		var derr error
		kek, derr = pbkdf2.Key(sha256.New, string(pw), salt, iterations, gcm.KeySize)
		return derr
	})
	if err != nil {
		enclave.Wipe(kek)
		return nil, newError(op, ErrInvalidKey, err)
	}
	h, err := enclave.NewMemory(kek) // wipes kek on every path
	if err != nil {
		return nil, newError(op, ErrInvalidKey, err)
	}
	// PBKDF2 cannot be interrupted; honor a ctx that ended while it ran.
	if cerr := ctx.Err(); cerr != nil {
		return nil, errors.Join(&Error{Op: op, Err: cerr}, destroy(op, h))
	}
	return h, nil
}
