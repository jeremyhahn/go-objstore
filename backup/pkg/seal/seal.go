// Package seal is the backup repository's object encryption: per-purpose
// subkeys derived from the data key, keyed chunk IDs, and sealed objects.
//
// Subkeys are derived from the repository data key with HKDF-SHA-256 (RFC 5869,
// NIST SP 800-56C), one distinct info label per purpose: chunk IDs, object
// IDs, packs, indexes, snapshots and the repository config. Every subkey is
// held in go-enclave locked memory until Keys.Close destroys it.
//
// A chunk ID is HMAC-SHA-256 of the chunk under the chunk-ID subkey, so equal
// chunks deduplicate without exposing a plaintext hash. An object ID, the
// name of a pack, index or snapshot, is HMAC-SHA-256 of the object's type and
// content under the object-ID subkey.
//
// A sealed object is AES-256-GCM (NIST SP 800-38D) under its type's subkey,
// with a random 96-bit nonce:
//
//	"OBKS" | version (1) | type (1) | compression (1) | nonce (12) | ciphertext | tag (16)
//
// The additional data is the 7-byte header followed by the object's ID
// (a 4-byte big-endian length, then the ID), so an object sealed as one type
// or ID does not open as another, and its compression cannot be altered.
package seal

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/jeremyhahn/go-objstore/backup/internal/gcm"
)

// Type is the kind of a sealed object. Each type seals under its own subkey.
type Type uint8

const (
	// TypePack is an immutable pack of chunks.
	TypePack Type = 1
	// TypeIndex is an index mapping chunk IDs to packs.
	TypeIndex Type = 2
	// TypeSnapshot is a snapshot manifest.
	TypeSnapshot Type = 3
	// TypeConfig is the repository config.
	TypeConfig Type = 4
)

// String returns the type's name.
func (t Type) String() string {
	switch t {
	case TypePack:
		return "pack"
	case TypeIndex:
		return "index"
	case TypeSnapshot:
		return "snapshot"
	case TypeConfig:
		return "config"
	default:
		return "type(" + strconv.Itoa(int(t)) + ")"
	}
}

// Compression identifies how an object's plaintext was compressed before it
// was sealed. It is recorded in the sealed header.
type Compression uint8

// CompressionNone stores the plaintext as is. It is the only algorithm this
// version implements.
const CompressionNone Compression = 0

// String returns the algorithm's name.
func (c Compression) String() string {
	if c == CompressionNone {
		return "none"
	}
	return "compression(" + strconv.Itoa(int(c)) + ")"
}

const (
	magic         = "OBKS"
	formatVersion = 1
	headerSize    = len(magic) + 3
	offType       = len(magic) + 1
	offCompress   = len(magic) + 2

	// Overhead is how many bytes a sealed object adds to its compressed
	// plaintext: the header, the nonce and the tag.
	Overhead = headerSize + gcm.Overhead
)

// Seal compresses plaintext with c and seals it as an object of type t named
// id. It fails with ErrUnknownType, ErrUnsupportedCompression, or
// ErrKeysDestroyed after Close.
func (k *Keys) Seal(t Type, id []byte, c Compression, plaintext []byte) ([]byte, error) {
	const op = "seal"
	key, err := k.keyFor(op, t)
	if err != nil {
		return nil, err
	}
	payload, err := compress(op, c, plaintext)
	if err != nil {
		return nil, err
	}
	aead, err := gcm.New(key)
	if err != nil {
		return nil, newError(op, ErrInvalidKey, err)
	}
	out := make([]byte, headerSize, Overhead+len(payload))
	copy(out, magic)
	out[len(magic)] = formatVersion
	out[offType] = byte(t)
	out[offCompress] = byte(c)
	ad := additionalData(out[:headerSize], id)
	return aead.Seal(out, nil, payload, ad), nil //nolint:gosec // G407: the AEAD draws a random nonce itself
}

// Open authenticates and decrypts sealed as an object of type t named id, and
// returns its decompressed plaintext. Anything that does not authenticate (a
// wrong type, ID or key, or any altered byte) fails with ErrOpen; input too
// short or in an unknown format fails with ErrMalformed.
func (k *Keys) Open(t Type, id, sealed []byte) ([]byte, error) {
	const op = "open"
	key, err := k.keyFor(op, t)
	if err != nil {
		return nil, err
	}
	if len(sealed) < Overhead ||
		!bytes.Equal(sealed[:len(magic)], []byte(magic)) ||
		sealed[len(magic)] != formatVersion {
		return nil, newError(op, ErrMalformed, nil)
	}
	if Type(sealed[offType]) != t {
		return nil, newError(op, ErrOpen, nil)
	}
	aead, err := gcm.New(key)
	if err != nil {
		return nil, newError(op, ErrInvalidKey, err)
	}
	header := sealed[:headerSize]
	payload, err := aead.Open(nil, nil, sealed[headerSize:], additionalData(header, id))
	if err != nil {
		// The AEAD's own error says only that authentication failed.
		return nil, newError(op, ErrOpen, nil)
	}
	return decompress(op, Compression(header[offCompress]), payload)
}

// additionalData binds the header and the object's ID.
func additionalData(header, id []byte) []byte {
	ad := make([]byte, 0, len(header)+4+len(id))
	ad = append(ad, header...)
	ad = binary.BigEndian.AppendUint32(ad, uint32(len(id))) //nolint:gosec // an ID is far below 4 GiB
	return append(ad, id...)
}

func compress(op string, c Compression, plaintext []byte) ([]byte, error) {
	if c != CompressionNone {
		return nil, newError(op, ErrUnsupportedCompression, nil)
	}
	return plaintext, nil
}

func decompress(op string, c Compression, payload []byte) ([]byte, error) {
	if c != CompressionNone {
		return nil, newError(op, ErrUnsupportedCompression, nil)
	}
	return payload, nil
}
