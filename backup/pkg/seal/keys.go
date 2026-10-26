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
	labelObjectID = "go-objstore backup v1 object-id"
	labelPack     = "go-objstore backup v1 pack"
	labelIndex    = "go-objstore backup v1 index"
	labelSnapshot = "go-objstore backup v1 snapshot"
	labelConfig   = "go-objstore backup v1 config"
)

// IDSize is the length in bytes of a chunk or object ID.
const IDSize = sha256.Size

// ID is a chunk ID, HMAC-SHA-256 of the chunk under the chunk-ID subkey, or
// an object ID, HMAC-SHA-256 of the object's content under the object-ID
// subkey.
type ID [IDSize]byte

// String returns the ID in lowercase hex.
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// MarshalText returns the ID in lowercase hex.
func (id ID) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

// UnmarshalText parses an ID from hex, failing with ErrMalformedID.
func (id *ID) UnmarshalText(text []byte) error {
	parsed, err := ParseID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// ParseID parses an ID from its lowercase hex form, failing with
// ErrMalformedID.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 2*IDSize {
		return ID{}, newError("parse id", ErrMalformedID, nil)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil || id.String() != s {
		return ID{}, newError("parse id", ErrMalformedID, nil)
	}
	return id, nil
}

// Keys holds a repository's subkeys in locked memory. It is safe for
// concurrent use until Close.
type Keys struct {
	chunkID  enclave.ByteAccess
	objectID enclave.ByteAccess
	pack     enclave.ByteAccess
	index    enclave.ByteAccess
	snapshot enclave.ByteAccess
	config   enclave.ByteAccess
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
		{&k.objectID, labelObjectID},
		{&k.pack, labelPack},
		{&k.index, labelIndex},
		{&k.snapshot, labelSnapshot},
		{&k.config, labelConfig},
	} {
		sub, err := derive(dataKey, d.label)
		if err != nil {
			return nil, errors.Join(newError(op, ErrInvalidKey, err), k.Close())
		}
		*d.dst = sub
	}
	return k, nil
}

// derive returns the subkey for label in locked memory.
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

// Close destroys every subkey. Later calls return nil; every other method
// fails with ErrKeysDestroyed.
func (k *Keys) Close() error {
	var errs []error
	for _, h := range []enclave.ByteAccess{k.chunkID, k.objectID, k.pack, k.index, k.snapshot, k.config} {
		if h == nil {
			continue
		}
		if err := h.Destroy(); err != nil {
			errs = append(errs, newError("close", ErrKeysDestroyed, err))
		}
	}
	return errors.Join(errs...)
}

// ChunkID returns the chunk's ID: HMAC-SHA-256 of data under the chunk-ID
// subkey.
func (k *Keys) ChunkID(data []byte) (ID, error) {
	return mac("chunk id", k.chunkID, nil, data)
}

// ObjectID returns a content-derived name for an object of type t:
// HMAC-SHA-256 of the type and content under the object-ID subkey. Equal
// content gives the same ID, and the ID reveals nothing about the content.
func (k *Keys) ObjectID(t Type, content []byte) (ID, error) {
	return mac("object id", k.objectID, []byte{byte(t)}, content)
}

// mac returns HMAC-SHA-256 of prefix and data under key.
func mac(op string, key enclave.ByteAccess, prefix, data []byte) (ID, error) {
	var id ID
	err := key.WithBytes(func(b []byte) error {
		m := hmac.New(sha256.New, b)
		m.Write(prefix)
		m.Write(data)
		m.Sum(id[:0])
		return nil
	})
	if err != nil {
		return ID{}, newError(op, ErrInvalidKey, err)
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
	case TypeConfig:
		h = k.config
	default:
		return nil, newError(op, ErrUnknownType, nil)
	}
	if !h.IsAlive() {
		return nil, newError(op, ErrKeysDestroyed, nil)
	}
	return h, nil
}
