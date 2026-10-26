package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// chunks returns n distinct chunks of size bytes and their IDs.
func chunks(t *testing.T, r *Repository, n, size int) ([][]byte, []seal.ID) {
	t.Helper()
	var data [][]byte
	var ids []seal.ID
	for i := range n {
		c := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, size/2)
		id, err := r.ChunkID(c)
		if err != nil {
			t.Fatalf("ChunkID: %v", err)
		}
		data, ids = append(data, c), append(ids, id)
	}
	return data, ids
}

// pack writes the chunks with a fresh packer and returns its entries.
func pack(t *testing.T, r *Repository, data [][]byte, ids []seal.ID) (*Packer, []Entry) {
	t.Helper()
	ctx := context.Background()
	p := r.NewPacker()
	for i := range data {
		if err := p.Add(ctx, ids[i], data[i]); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return p, p.Entries()
}

func TestPacker(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	data, ids := chunks(t, r, 12, 1000)
	p := r.NewPacker()
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}
	for i := range data {
		if err := p.Add(ctx, ids[i], data[i]); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if !p.Has(ids[i]) {
			t.Fatal("Has is false after Add")
		}
	}
	_ = p.Add(ctx, ids[0], data[0]) // held already: ignored
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	entries := p.Entries()
	if written, reused := p.Written(); written < 3 || reused != 0 || len(entries) != len(ids) {
		t.Fatalf("written %d reused %d entries %d", written, reused, len(entries))
	}
	packs, _ := st.List(ctx, r.Layout().Dir(KindPack))
	for _, name := range packs {
		obj, _ := st.Get(ctx, name)
		if len(obj) > MinPackSize+1100 {
			t.Fatalf("pack %s is %d bytes, past the target", name, len(obj))
		}
	}
	for i, e := range entries {
		got, err := r.ReadChunk(ctx, e)
		if err != nil || !bytes.Equal(got, data[i]) || e.Chunk != ids[i] {
			t.Fatalf("ReadChunk(%d) = %d bytes, %v", i, len(got), err)
		}
	}

	// The same chunks again make the same packs, which are found and reused.
	again, againEntries := pack(t, r, data, ids)
	if written, reused := again.Written(); written != 0 || reused != len(packs) {
		t.Fatalf("second packer wrote %d, reused %d", written, reused)
	}
	if !slices.Equal(againEntries, entries) {
		t.Fatal("reused packs give other entries")
	}
}

func TestPackTampering(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	data, ids := chunks(t, r, 4, 800)
	_, a := pack(t, r, data[:2], ids[:2])
	_, b := pack(t, r, data[2:], ids[2:])
	nameA := r.Layout().Name(KindPack, a[0].Pack)
	nameB := r.Layout().Name(KindPack, b[0].Pack)
	orig, _ := st.Get(ctx, nameA)
	origB, _ := st.Get(ctx, nameB)
	restore := func() { st.Replace(nameA, orig) }

	flip := func(i int) []byte {
		c := slices.Clone(orig)
		c[i] ^= 1
		return c
	}
	// A flipped bit in a chunk blob opens the trailer but not the chunk.
	st.Replace(nameA, flip(10))
	_, err := r.ReadChunk(ctx, a[0])
	assertRepoError(t, err, ErrCorrupt, seal.ErrOpen)

	for name, obj := range map[string][]byte{
		"trailer":   flip(len(orig) - 10),
		"length":    flip(len(orig) - 1),
		"short":     orig[:3],
		"huge len":  binary.BigEndian.AppendUint32(nil, 1<<31),
		"renamed B": origB,
	} {
		t.Run(name, func(t *testing.T) {
			st.Replace(nameA, obj)
			_, err := r.LoadPack(ctx, a[0].Pack)
			assertRepoError(t, err, ErrCorrupt)
		})
	}
	st.Delete(nameA)
	_, err = r.LoadPack(ctx, a[0].Pack)
	assertRepoError(t, err, ErrCorrupt, store.ErrNotFound)
	restore()

	pk, err := r.LoadPack(ctx, a[0].Pack)
	if err != nil {
		t.Fatalf("LoadPack: %v", err)
	}
	wrongPack, outside, wrongChunk := a[0], a[0], a[0]
	wrongPack.Pack = b[0].Pack
	outside.Offset = 1 << 40
	wrongChunk.Chunk = ids[1]
	for _, e := range []Entry{wrongPack, outside, wrongChunk} {
		_, err := pk.Chunk(e)
		assertRepoError(t, err, ErrCorrupt)
	}

	// A blob that opens under one chunk ID but holds another chunk.
	blob, _ := r.keys.Seal(seal.TypePack, blobAD(ids[0]), seal.CompressionNone, data[1])
	forged := &Pack{ID: a[0].Pack, r: r, data: blob}
	_, err = forged.Chunk(Entry{Chunk: ids[0], Pack: a[0].Pack, Length: uint32(len(blob))})
	assertRepoError(t, err, ErrCorrupt)

	// A trailer that authenticates but lists other chunks than the packer's
	// is not reused.
	trailer, _ := r.keys.Seal(seal.TypePack, trailerAD(a[0].Pack), seal.CompressionNone, encodeTrailer(a[:1]))
	short := slices.Concat(orig[:a[0].Length], trailer, binary.BigEndian.AppendUint32(nil, uint32(len(trailer))))
	st.Replace(nameA, short)
	p := r.NewPacker()
	_ = p.Add(ctx, ids[0], data[0])
	_ = p.Add(ctx, ids[1], data[1])
	assertRepoError(t, p.Flush(ctx), ErrCorrupt)

	// A trailer whose plaintext does not parse.
	bad, _ := r.keys.Seal(seal.TypePack, trailerAD(a[0].Pack), seal.CompressionNone, []byte{9})
	st.Replace(nameA, slices.Concat(bad, binary.BigEndian.AppendUint32(nil, uint32(len(bad)))))
	_, err = r.LoadPack(ctx, a[0].Pack)
	assertRepoError(t, err, ErrCorrupt)
}

func TestDecodeTrailer(t *testing.T) {
	e := Entry{Offset: 0, Length: 100}
	good := encodeTrailer([]Entry{e})
	if got, ok := decodeTrailer(good, seal.ID{1}, 100); !ok || len(got) != 1 || got[0].Pack != (seal.ID{1}) {
		t.Fatalf("decodeTrailer(good) = %v, %v", got, ok)
	}
	gap := e
	gap.Offset = 5
	tiny := e
	tiny.Length = 1
	for name, tc := range map[string]struct {
		b   []byte
		end uint64
	}{
		"empty":   {nil, 0},
		"version": {append([]byte{2}, good[1:]...), 100},
		"count":   {good[:len(good)-1], 100},
		"none":    {encodeTrailer(nil), 0},
		"gap":     {encodeTrailer([]Entry{gap}), 105},
		"tiny":    {encodeTrailer([]Entry{tiny}), 1},
		"end":     {good, 99},
	} {
		if _, ok := decodeTrailer(tc.b, seal.ID{}, tc.end); ok {
			t.Fatalf("%s: decodeTrailer accepted %x", name, tc.b)
		}
	}
}

func TestPackerFailures(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	data, ids := chunks(t, r, 1, 500)
	fr := *r
	fr.st = &faulty{Store: st, fail: failOn("put", "/packs/")}
	p := fr.NewPacker()
	_ = p.Add(ctx, ids[0], data[0])
	assertRepoError(t, p.Flush(ctx), ErrStorage, errDown)

	// An existing pack that cannot be read back is not reused.
	_, _ = pack(t, r, data, ids)
	fr.st = &faulty{Store: st, fail: failOn("get", "/packs/")}
	p = fr.NewPacker()
	_ = p.Add(ctx, ids[0], data[0])
	assertRepoError(t, p.Flush(ctx), ErrStorage, errDown)

	p = r.NewPacker()
	_ = p.Add(ctx, ids[0], data[0])
	_ = r.Close()
	assertRepoError(t, p.Flush(ctx), ErrKeys)
	assertRepoError(t, p.Add(ctx, seal.ID{9}, data[0]), ErrKeys)
	_, err := r.WriteIndex(ctx, []Entry{{}})
	assertRepoError(t, err, ErrKeys)
}

func TestIndex(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	data, ids := chunks(t, r, 6, 300)
	_, first := pack(t, r, data[:4], ids[:4])
	_, second := pack(t, r, data[2:], ids[2:])

	ix, err := r.LoadIndex(ctx)
	if err != nil || ix.Len() != 0 {
		t.Fatalf("empty LoadIndex = %v, %v", ix, err)
	}
	idA, err := r.WriteIndex(ctx, append(first, first[0]))
	if err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	idB, err := r.WriteIndex(ctx, second)
	if err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	// Writing the same index again is accepted and names the same object.
	if again, err := r.WriteIndex(ctx, first); err != nil || again != idA {
		t.Fatalf("rewrite = %v, %v", again, err)
	}
	_, err = r.WriteIndex(ctx, nil)
	assertRepoError(t, err, ErrInvalidOptions)

	ix, err = r.LoadIndex(ctx)
	if err != nil || ix.Len() != 6 {
		t.Fatalf("LoadIndex = %d entries, %v", ix.Len(), err)
	}
	for i, id := range ids {
		e, ok := ix.Lookup(id)
		if !ok {
			t.Fatalf("chunk %d missing", i)
		}
		if got, err := r.ReadChunk(ctx, e); err != nil || !bytes.Equal(got, data[i]) {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}
	if _, ok := ix.Lookup(seal.ID{7}); ok {
		t.Fatal("Lookup found an unknown chunk")
	}

	nameA, nameB := r.Layout().Name(KindIndex, idA), r.Layout().Name(KindIndex, idB)
	origA, _ := st.Get(ctx, nameA)
	origB, _ := st.Get(ctx, nameB)
	tampered := slices.Clone(origA)
	tampered[len(tampered)/2] ^= 1
	for name, obj := range map[string][]byte{"tampered": tampered, "renamed": origB} {
		t.Run(name, func(t *testing.T) {
			st.Replace(nameA, obj)
			_, err := r.LoadIndex(ctx)
			assertRepoError(t, err, ErrCorrupt, seal.ErrOpen)
		})
	}
	// An object under the index's ID that holds another index is not
	// accepted as the same index.
	_, err = r.WriteIndex(ctx, first)
	assertRepoError(t, err, ErrCorrupt)
	st.Replace(nameA, origA)

	// An index object that authenticates but does not parse.
	if _, err := r.putSealed(ctx, "test", KindIndex, []byte{1, 0}); err != nil {
		t.Fatalf("putSealed: %v", err)
	}
	_, err = r.LoadIndex(ctx)
	assertRepoError(t, err, ErrCorrupt)

	st.Replace(r.Layout().Dir(KindIndex)+"stray", nil)
	_, err = r.LoadIndex(ctx)
	assertRepoError(t, err, ErrMalformedName)

	fr := *r
	fr.st = &faulty{Store: st, fail: failOn("list", "/index/")}
	_, err = fr.LoadIndex(ctx)
	assertRepoError(t, err, ErrStorage)
	fr.st = &faulty{Store: st, fail: failOn("put", "/index/")}
	_, err = fr.WriteIndex(ctx, second[:1])
	assertRepoError(t, err, ErrStorage)
}

func TestDecodeIndex(t *testing.T) {
	a, b := Entry{Chunk: seal.ID{1}}, Entry{Chunk: seal.ID{2}}
	good := encodeIndex([]Entry{a, b})
	if got, ok := decodeIndex(good); !ok || len(got) != 2 {
		t.Fatalf("decodeIndex(good) = %v, %v", got, ok)
	}
	for name, in := range map[string][]byte{
		"empty":    nil,
		"version":  append([]byte{2}, good[1:]...),
		"length":   good[:len(good)-1],
		"none":     encodeIndex(nil),
		"unsorted": encodeIndex([]Entry{b, a}),
		"twice":    encodeIndex([]Entry{a, a}),
	} {
		if _, ok := decodeIndex(in); ok {
			t.Fatalf("%s: decodeIndex accepted %x", name, in)
		}
	}
}

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	when := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	s := &Snapshot{
		Time: when, Host: "host-a", Root: "/srv/data",
		Nodes: []Node{
			{Path: ".", Type: NodeDir, Mode: 0o755 | 1<<31, ModTime: when},
			{Path: "f", Type: NodeFile, Mode: 0o644, Size: 3, ModTime: when, SHA256: "ab", Chunks: []seal.ID{{1}}},
			{Path: "l", Type: NodeSymlink, ModTime: when, Target: "f"},
		},
		Skipped: 1,
	}
	idA, err := r.WriteSnapshot(ctx, s)
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	s2 := *s
	s2.Host = "host-b"
	idB, _ := r.WriteSnapshot(ctx, &s2)
	got, err := r.ReadSnapshot(ctx, idA)
	if err != nil || got.Host != "host-a" || got.Version != snapshotVersion || len(got.Nodes) != 3 ||
		got.Nodes[1].Chunks[0] != (seal.ID{1}) || !got.Time.Equal(when) || got.Skipped != 1 {
		t.Fatalf("ReadSnapshot = %+v, %v", got, err)
	}
	list, err := r.ListSnapshots(ctx)
	if err != nil || len(list) != 2 || !slices.Contains(list, idA) || !slices.Contains(list, idB) {
		t.Fatalf("ListSnapshots = %v, %v", list, err)
	}

	nameA := r.Layout().Name(KindSnapshot, idA)
	origA, _ := st.Get(ctx, nameA)
	origB, _ := st.Get(ctx, r.Layout().Name(KindSnapshot, idB))
	tampered := slices.Clone(origA)
	tampered[len(tampered)-1] ^= 1
	for name, obj := range map[string][]byte{"tampered": tampered, "renamed": origB} {
		st.Replace(nameA, obj)
		_, err := r.ReadSnapshot(ctx, idA)
		assertRepoError(t, err, ErrCorrupt, seal.ErrOpen)
		_ = name
	}

	for name, plain := range map[string][]byte{"not json": []byte("x"), "version": []byte(`{"version":9}`)} {
		id, err := r.putSealed(ctx, "test", KindSnapshot, plain)
		if err != nil {
			t.Fatalf("%s: putSealed: %v", name, err)
		}
		_, err = r.ReadSnapshot(ctx, id)
		assertRepoError(t, err, ErrCorrupt)
	}
	_, err = r.ReadSnapshot(ctx, seal.ID{3})
	assertRepoError(t, err, ErrCorrupt, store.ErrNotFound)

	// A manifest that cannot be encoded.
	_, err = r.WriteSnapshot(ctx, &Snapshot{Time: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)})
	assertRepoError(t, err, ErrCorrupt)
}
