// Package gcm builds AES-256-GCM with random 96-bit nonces from a key held in go-enclave locked memory; the key schedule stays on the heap until collected.
package gcm

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"
)

// KeySize is the AES-256 key length in bytes.
const KeySize = 32

// Overhead is how much longer a sealed message is than its plaintext: the
// 12-byte nonce and the 16-byte tag.
const Overhead = 12 + 16

// ErrKeySize reports a key handle that does not hold exactly KeySize bytes.
var ErrKeySize = errors.New("gcm: key is not 32 bytes")

// New returns an AES-256-GCM AEAD under key; it fails with ErrKeySize or go-enclave's read error.
func New(key enclave.ByteAccess) (cipher.AEAD, error) {
	if key == nil {
		return nil, ErrKeySize
	}
	if !key.IsAlive() {
		return nil, enclave.ErrEnclaveDestroyed
	}
	if key.Size() != KeySize {
		return nil, ErrKeySize
	}
	var aead cipher.AEAD
	err := key.WithBytes(func(k []byte) error {
		block, err := aes.NewCipher(k)
		if err == nil {
			aead, err = cipher.NewGCMWithRandomNonce(block)
		}
		return err
	})
	return aead, err
}
