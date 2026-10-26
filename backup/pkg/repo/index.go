package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// An index object maps chunk IDs to where they are stored. Each backup run
// that stores new chunks writes one, and loading the index merges them all.
// Its plaintext is
//
//	version (1) | count (4) | count × (chunk ID (32) | pack ID (32) | offset (8) | length (4))
//
// with the entries sorted by chunk ID and no ID twice. It is sealed with the
// index subkey, and its ID is the object ID of the plaintext.

const (
	indexVersion   = 1
	indexEntrySize = 2*seal.IDSize + 8 + 4
	indexHeader    = 1 + 4
)

// Index maps chunk IDs to their entries.
type Index struct {
	m map[seal.ID]Entry
}

// Lookup returns the entry for the chunk, if the index has it.
func (ix *Index) Lookup(id seal.ID) (Entry, bool) {
	e, ok := ix.m[id]
	return e, ok
}

// Len returns the number of chunks the index holds.
func (ix *Index) Len() int {
	return len(ix.m)
}

// LoadIndex reads every index object and merges them. When two objects list
// the same chunk, the first in name order wins; both locations hold it.
func (r *Repository) LoadIndex(ctx context.Context) (*Index, error) {
	const op = "read index"
	ids, err := r.listIDs(ctx, op, KindIndex)
	if err != nil {
		return nil, err
	}
	ix := &Index{m: map[seal.ID]Entry{}}
	for _, id := range ids {
		plain, err := r.getSealed(ctx, op, KindIndex, id)
		if err != nil {
			return nil, err
		}
		entries, ok := decodeIndex(plain)
		if !ok {
			return nil, newError(op, r.layout.Name(KindIndex, id), ErrCorrupt, nil)
		}
		for _, e := range entries {
			if _, dup := ix.m[e.Chunk]; !dup {
				ix.m[e.Chunk] = e
			}
		}
	}
	return ix, nil
}

// WriteIndex writes an index object listing entries and returns its ID. A
// chunk listed twice keeps its first entry. Writing an empty index fails
// with ErrInvalidOptions.
func (r *Repository) WriteIndex(ctx context.Context, entries []Entry) (seal.ID, error) {
	const op = "write index"
	if len(entries) == 0 {
		return seal.ID{}, newError(op, "", ErrInvalidOptions, nil)
	}
	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, func(a, b Entry) int { return bytes.Compare(a.Chunk[:], b.Chunk[:]) })
	sorted = slices.CompactFunc(sorted, func(a, b Entry) bool { return a.Chunk == b.Chunk })
	return r.putSealed(ctx, op, KindIndex, encodeIndex(sorted))
}

func encodeIndex(entries []Entry) []byte {
	b := make([]byte, 0, indexHeader+len(entries)*indexEntrySize)
	b = append(b, indexVersion)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entries))) //nolint:gosec // an index lists far fewer than 4G chunks
	for _, e := range entries {
		b = append(b, e.Chunk[:]...)
		b = append(b, e.Pack[:]...)
		b = binary.BigEndian.AppendUint64(b, e.Offset)
		b = binary.BigEndian.AppendUint32(b, e.Length)
	}
	return b
}

// decodeIndex parses an index object's plaintext and checks that its
// entries are sorted with no chunk twice.
func decodeIndex(b []byte) ([]Entry, bool) {
	if len(b) < indexHeader || b[0] != indexVersion {
		return nil, false
	}
	n := uint64(binary.BigEndian.Uint32(b[1:]))
	if n == 0 || uint64(len(b)) != indexHeader+n*indexEntrySize {
		return nil, false
	}
	entries := make([]Entry, 0, n)
	for rest := b[indexHeader:]; len(rest) > 0; rest = rest[indexEntrySize:] {
		var e Entry
		copy(e.Chunk[:], rest)
		copy(e.Pack[:], rest[seal.IDSize:])
		e.Offset = binary.BigEndian.Uint64(rest[2*seal.IDSize:])
		e.Length = binary.BigEndian.Uint32(rest[2*seal.IDSize+8:])
		if k := len(entries); k > 0 && bytes.Compare(entries[k-1].Chunk[:], e.Chunk[:]) >= 0 {
			return nil, false
		}
		entries = append(entries, e)
	}
	return entries, true
}
