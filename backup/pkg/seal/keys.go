package seal

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/backup/internal/gcm"
)

// DataKeySize is the length in bytes of the repository data key subkeys are
// derived from.
const DataKeySize = 32

// The HKDF info labels, one per purpose. They are part of the repository
// format: changing one makes every existing repository unreadable.
const (
	labelChunkID  = "go-objstore backup v1 chunk-id"
	labelPack     = "go-objstore backup v1 pack"
	labelIndex    = "go-objstore backup v1 index"
	labelSnapshot = "go-objstore backup v1 snapshot"
)

// IDSize is the length in bytes of a chunk ID.
const IDSize = sha256.Size

// ID is a chunk ID: HMAC-SHA-256 of the chunk under the chunk-ID subkey.
type ID [IDSize]byte

// String returns the ID in lowercase hex.
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// Keys holds a repository's subkeys in locked memory. It is safe for
// concurrent use until Close.
type Keys struct {
	chunkID  enclave.ByteAccess
	pack     enclave.ByteAccess
	index    enclave.ByteAccess
	snapshot enclave.ByteAccess
}

// DeriveKeys derives the per-purpose subkeys from dataKey, which it only reads:
// the caller still owns and destroys dataKey. It fails with ErrInvalidKey for
// a nil key or one that is not DataKeySize bytes, and ErrKeysDestroyed for a
// destroyed one.
func DeriveKeys(dataKey enclave.ByteAccess) (*Keys, error) {
	const op = "derive keys"
	switch {
	case dataKey == nil:
		return nil, newError(op, ErrInvalidKey, nil)
	case !dataKey.IsAlive():
		return nil, newError(op, ErrKeysDestroyed, nil)
	case dataKey.Size() != DataKeySize:
		return nil, newError(op, ErrInvalidKey, nil)
	}
	k := &Keys{}
	for _, d := range []struct {
		dst   *enclave.ByteAccess
		label string
	}{
		{&k.chunkID, labelChunkID},
		{&k.pack, labelPack},
		{&k.index, labelIndex},
		{&k.snapshot, labelSnapshot},
	} {
		sub, err := derive(dataKey, d.label)
		if err != nil {
			return nil, errors.Join(newError(op, ErrInvalidKey, err), k.Close())
		}
		*d.dst = sub
	}
	return k, nil
}

// derive returns the HKDF-SHA-256 subkey of dataKey for label, in locked memory.
func derive(dataKey enclave.ByteAccess, label string) (enclave.ByteAccess, error) {
	var sub []byte
	err := dataKey.WithBytes(func(dk []byte) error {
		var herr error
		sub, herr = hkdf.Key(sha256.New, dk, nil, label, gcm.KeySize)
		return herr
	})
	if err != nil {
		enclave.Wipe(sub)
		return nil, err
	}
	return enclave.NewMemory(sub) // wipes sub on every path
}

// Close destroys every subkey. It is idempotent; after it every operation fails
// with ErrKeysDestroyed.
func (k *Keys) Close() error {
	var errs []error
	for _, h := range []enclave.ByteAccess{k.chunkID, k.pack, k.index, k.snapshot} {
		if h == nil {
			continue
		}
		if err := h.Destroy(); err != nil {
			errs = append(errs, newError("close", ErrKeysDestroyed, err))
		}
	}
	return errors.Join(errs...)
}

// ChunkID returns the chunk ID of data.
func (k *Keys) ChunkID(data []byte) (ID, error) {
	var id ID
	err := k.chunkID.WithBytes(func(key []byte) error {
		m := hmac.New(sha256.New, key)
		m.Write(data)
		m.Sum(id[:0])
		return nil
	})
	if err != nil {
		return ID{}, newError("chunk id", ErrInvalidKey, err)
	}
	return id, nil
}

// keyFor returns the subkey that seals objects of type t.
func (k *Keys) keyFor(op string, t Type) (enclave.ByteAccess, error) {
	var h enclave.ByteAccess
	switch t {
	case TypePack:
		h = k.pack
	case TypeIndex:
		h = k.index
	case TypeSnapshot:
		h = k.snapshot
	default:
		return nil, newError(op, ErrUnknownType, nil)
	}
	if !h.IsAlive() {
		return nil, newError(op, ErrKeysDestroyed, nil)
	}
	return h, nil
}
