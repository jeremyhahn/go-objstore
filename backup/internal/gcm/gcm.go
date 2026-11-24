// Package gcm builds AES-256-GCM from a key held in go-enclave locked memory.
//
// The AEAD draws a fresh random 96-bit nonce for every Seal and carries it at
// the front of the ciphertext (cipher.NewGCMWithRandomNonce), the construction
// FIPS 140-3 approves for caller-unmanaged nonces. The key is read from locked
// memory only to build the cipher; the AES key schedule lives on the heap
// until it is collected, the residual go-enclave documents for software AES.
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

// New returns an AES-256-GCM AEAD with random nonces under the key in key.
// It fails with ErrKeySize for a handle of the wrong size, and with go-enclave's
// error (enclave.ErrEnclaveDestroyed for a destroyed handle) when the key
// cannot be read.
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
