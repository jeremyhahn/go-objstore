package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// A pack is a run of sealed chunks followed by a sealed trailer that lists
// them, and the trailer's length:
//
//	blob 0 | blob 1 | ... | sealed trailer | trailer length (4, big-endian)
//
// Each blob is one chunk sealed as a pack object whose additional data binds
// the chunk ID. The trailer is sealed as a pack object whose additional data
// binds the pack ID, so a pack renamed to another's name does not open. The
// trailer's plaintext is
//
//	version (1) | count (4) | count × (chunk ID (32) | offset (8) | length (4))
//
// with the blobs in order and contiguous from offset 0. The pack ID is the
// object ID of the chunk IDs in order, so the same chunks always make the
// same pack, byte for byte apart from the nonces.

const (
	trailerVersion   = 1
	trailerEntrySize = seal.IDSize + 8 + 4
	trailerHeader    = 1 + 4
)

// Additional-data prefixes keep a chunk blob and a trailer from opening as
// each other.
const (
	adChunk   = 'c'
	adTrailer = 't'
)

func blobAD(chunk seal.ID) []byte {
	return append([]byte{adChunk}, chunk[:]...)
}

func trailerAD(pack seal.ID) []byte {
	return append([]byte{adTrailer}, pack[:]...)
}

// Entry locates a chunk: the pack holding it, and the offset and length of
// its sealed blob in the pack.
type Entry struct {
	Chunk  seal.ID
	Pack   seal.ID
	Offset uint64
	Length uint32
}

// Packer groups chunks into packs of about the repository's pack size and
// writes each pack once. It is not safe for concurrent use.
type Packer struct {
	r       *Repository
	buf     []byte
	pending []Entry
	held    map[seal.ID]bool
	entries []Entry
	written int
	reused  int
}

// NewPacker returns a Packer that writes packs to r.
func (r *Repository) NewPacker() *Packer {
	return &Packer{r: r, held: map[seal.ID]bool{}}
}

// Has reports whether the packer already holds the chunk, written or pending.
func (p *Packer) Has(id seal.ID) bool {
	return p.held[id]
}

// Add seals the chunk into the current pack, writing the pack first if the
// chunk would take it past the pack size. A chunk the packer already holds
// is ignored. data is not retained.
func (p *Packer) Add(ctx context.Context, id seal.ID, data []byte) error {
	if p.held[id] {
		return nil
	}
	blob, err := p.r.keys.Seal(seal.TypePack, blobAD(id), seal.CompressionNone, data)
	if err != nil {
		return fromSeal("pack chunk", "", err)
	}
	if len(p.pending) > 0 && len(p.buf)+len(blob) > p.r.cfg.PackSize {
		if err := p.Flush(ctx); err != nil {
			return err
		}
	}
	p.pending = append(p.pending, Entry{
		Chunk:  id,
		Offset: uint64(len(p.buf)),
		Length: uint32(len(blob)), //nolint:gosec // a chunk is at most chunker.MaxMax bytes
	})
	p.buf = append(p.buf, blob...)
	p.held[id] = true
	return nil
}

// Flush writes the current pack, if it holds any chunks. If a pack with the
// same ID already exists, as after an interrupted run, Flush reads it and
// uses it when its trailer lists the same chunks at the same places.
func (p *Packer) Flush(ctx context.Context) error {
	const op = "write pack"
	if len(p.pending) == 0 {
		return nil
	}
	ids := make([]byte, 0, len(p.pending)*seal.IDSize)
	for _, e := range p.pending {
		ids = append(ids, e.Chunk[:]...)
	}
	packID, err := p.r.keys.ObjectID(seal.TypePack, ids)
	if err != nil {
		return fromSeal(op, "", err)
	}
	for i := range p.pending {
		p.pending[i].Pack = packID
	}
	trailer, err := p.r.keys.Seal(seal.TypePack, trailerAD(packID), seal.CompressionNone, encodeTrailer(p.pending))
	if err != nil {
		return fromSeal(op, "", err)
	}
	data := append(p.buf, trailer...)
	data = binary.BigEndian.AppendUint32(data, uint32(len(trailer))) //nolint:gosec // a trailer is far below 4 GiB
	name := p.r.layout.Name(KindPack, packID)
	switch err := p.r.st.Put(ctx, name, data); {
	case err == nil:
		p.written++
	case errors.Is(err, store.ErrExists):
		existing, rerr := p.r.LoadPack(ctx, packID)
		if rerr != nil {
			return rerr
		}
		if !slices.Equal(existing.Entries, p.pending) {
			return newError(op, name, ErrCorrupt, nil)
		}
		p.reused++
	default:
		return fromStore(op, name, err)
	}
	p.entries = append(p.entries, p.pending...)
	p.pending = nil
	p.buf = nil
	return nil
}

// Entries returns the entries of every pack the packer has written or
// reused, in order.
func (p *Packer) Entries() []Entry {
	return slices.Clone(p.entries)
}

// Written returns how many packs the packer wrote, and how many it found
// already in the store and reused.
func (p *Packer) Written() (written, reused int) {
	return p.written, p.reused
}

// encodeTrailer returns the trailer's plaintext for entries.
func encodeTrailer(entries []Entry) []byte {
	b := make([]byte, 0, trailerHeader+len(entries)*trailerEntrySize)
	b = append(b, trailerVersion)
	b = binary.BigEndian.AppendUint32(b, uint32(len(entries))) //nolint:gosec // bounded by the pack size
	for _, e := range entries {
		b = append(b, e.Chunk[:]...)
		b = binary.BigEndian.AppendUint64(b, e.Offset)
		b = binary.BigEndian.AppendUint32(b, e.Length)
	}
	return b
}

// decodeTrailer parses a trailer's plaintext and checks that its blobs are
// contiguous from 0 to end.
func decodeTrailer(b []byte, pack seal.ID, end uint64) ([]Entry, bool) {
	if len(b) < trailerHeader || b[0] != trailerVersion {
		return nil, false
	}
	n := uint64(binary.BigEndian.Uint32(b[1:]))
	if uint64(len(b)) != trailerHeader+n*trailerEntrySize {
		return nil, false
	}
	entries := make([]Entry, 0, n)
	var off uint64
	for rest := b[trailerHeader:]; len(rest) > 0; rest = rest[trailerEntrySize:] {
		e := Entry{Pack: pack}
		copy(e.Chunk[:], rest)
		e.Offset = binary.BigEndian.Uint64(rest[seal.IDSize:])
		e.Length = binary.BigEndian.Uint32(rest[seal.IDSize+8:])
		if e.Offset != off || e.Length < uint32(seal.Overhead) {
			return nil, false
		}
		off += uint64(e.Length)
		entries = append(entries, e)
	}
	return entries, off == end && n > 0
}

// Pack is a pack read from the store, its trailer authenticated.
type Pack struct {
	// ID is the pack's ID.
	ID seal.ID
	// Entries lists the pack's chunks in order.
	Entries []Entry
	r       *Repository
	data    []byte
}

// LoadPack reads the pack and authenticates its trailer. A pack that is
// missing, tampered with, or stored under another pack's name fails with
// ErrCorrupt.
func (r *Repository) LoadPack(ctx context.Context, id seal.ID) (*Pack, error) {
	const op = "read pack"
	name := r.layout.Name(KindPack, id)
	data, err := r.st.Get(ctx, name)
	if err != nil {
		return nil, fromStore(op, name, err)
	}
	size := uint64(len(data)) //nolint:gosec // a length is never negative
	if size < 4 {
		return nil, newError(op, name, ErrCorrupt, nil)
	}
	tlen := uint64(binary.BigEndian.Uint32(data[size-4:]))
	if tlen > size-4 {
		return nil, newError(op, name, ErrCorrupt, nil)
	}
	end := size - 4 - tlen
	plain, err := r.keys.Open(seal.TypePack, trailerAD(id), data[end:size-4])
	if err != nil {
		return nil, fromSeal(op, name, err)
	}
	entries, ok := decodeTrailer(plain, id, end)
	if !ok {
		return nil, newError(op, name, ErrCorrupt, nil)
	}
	return &Pack{ID: id, Entries: entries, r: r, data: data[:end]}, nil
}

// Chunk opens the chunk at e, which must name this pack, and checks that its
// content has e's chunk ID. It fails with ErrCorrupt otherwise.
func (p *Pack) Chunk(e Entry) ([]byte, error) {
	const op = "read chunk"
	name := p.r.layout.Name(KindPack, p.ID)
	if e.Pack != p.ID || e.Offset > uint64(len(p.data)) || uint64(e.Length) > uint64(len(p.data))-e.Offset {
		return nil, newError(op, name, ErrCorrupt, nil)
	}
	plain, err := p.r.keys.Open(seal.TypePack, blobAD(e.Chunk), p.data[e.Offset:e.Offset+uint64(e.Length)])
	if err != nil {
		return nil, fromSeal(op, name, err)
	}
	got, err := p.r.ChunkID(plain)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(got[:], e.Chunk[:]) {
		return nil, newError(op, name, ErrCorrupt, nil)
	}
	return plain, nil
}

// ReadChunk loads e's pack and returns the chunk.
func (r *Repository) ReadChunk(ctx context.Context, e Entry) ([]byte, error) {
	p, err := r.LoadPack(ctx, e.Pack)
	if err != nil {
		return nil, err
	}
	return p.Chunk(e)
}
