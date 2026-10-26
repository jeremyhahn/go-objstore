package repo

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

func TestMark(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	if got, err := r.Condemned(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Condemned on no marks = %v, %v", got, err)
	}
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a, b, c := seal.ID{1}, seal.ID{2}, seal.ID{3}
	idA, err := r.WriteMark(ctx, &Mark{Time: at, Packs: []Marked{{Pack: a, Since: at}, {Pack: b, Since: at}}})
	if err != nil {
		t.Fatalf("WriteMark: %v", err)
	}
	if _, err := r.WriteMark(ctx, &Mark{Time: at.Add(time.Hour), Packs: []Marked{{Pack: c, Since: at}}}); err != nil {
		t.Fatalf("WriteMark: %v", err)
	}
	ids, err := r.ListMarks(ctx)
	if err != nil || len(ids) != 2 {
		t.Fatalf("ListMarks = %v, %v", ids, err)
	}
	m, err := r.ReadMark(ctx, idA)
	if err != nil || !m.Time.Equal(at) || len(m.Packs) != 2 || m.Packs[1].Pack != b || !m.Packs[1].Since.Equal(at) {
		t.Fatalf("ReadMark = %+v, %v", m, err)
	}
	got, err := r.Condemned(ctx)
	if err != nil || len(got) != 3 || !got[a] || !got[b] || !got[c] {
		t.Fatalf("Condemned = %v, %v", got, err)
	}

	// A mark that does not decode, or names another version, is corrupt.
	for _, plain := range [][]byte{[]byte("x"), []byte(`{"version":9}`)} {
		id, err := r.putSealed(ctx, "test", KindMark, plain)
		if err != nil {
			t.Fatalf("putSealed: %v", err)
		}
		_, err = r.ReadMark(ctx, id)
		assertRepoError(t, err, ErrCorrupt)
		_, err = r.Condemned(ctx)
		assertRepoError(t, err, ErrCorrupt)
		st.Delete(r.Layout().Name(KindMark, id))
	}
	if err := st.Put(ctx, r.Layout().Dir(KindMark)+"bad", nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = r.Condemned(ctx)
	assertRepoError(t, err, ErrMalformedName)
}

func TestListPacks(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	data, ids := chunks(t, r, 12, 1000)
	pack(t, r, data, ids)
	names, _ := st.List(ctx, r.Layout().Dir(KindPack))
	packs, err := r.ListPacks(ctx)
	if err != nil || len(packs) != len(names) || len(packs) < 2 {
		t.Fatalf("ListPacks = %d, %v; %d stored", len(packs), err, len(names))
	}
	for i, id := range packs {
		if r.Layout().Name(KindPack, id) != names[i] {
			t.Fatalf("pack %d is %s, stored %s", i, id, names[i])
		}
	}
}

func TestPackerAvoid(t *testing.T) {
	ctx := context.Background()
	r, _ := newRepo(t)
	data, ids := chunks(t, r, 12, 1000)
	_, first := pack(t, r, data, ids)
	avoid := map[seal.ID]bool{}
	for _, e := range first {
		avoid[e.Pack] = true
	}

	// The same chunks are written again under other names, never reusing an
	// avoided pack.
	p := r.NewPacker()
	p.Avoid(avoid)
	for i := range data {
		if err := p.Add(ctx, ids[i], data[i]); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if written, reused := p.Written(); written != len(avoid) || reused != 0 {
		t.Fatalf("written %d reused %d, want %d new packs", written, reused, len(avoid))
	}
	for i, e := range p.Entries() {
		if avoid[e.Pack] {
			t.Fatalf("entry %d is in an avoided pack", i)
		}
		got, err := r.ReadChunk(ctx, e)
		if err != nil || !slices.Equal(got, data[i]) {
			t.Fatalf("ReadChunk(%d): %v", i, err)
		}
	}
}

// TestStoreHidesRemove checks that a repository opened on a store that can
// remove objects does not hand that capability on.
func TestStoreHidesRemove(t *testing.T) {
	r, st := newRepo(t)
	var _ store.PruneStore = st
	if _, ok := r.st.(store.PruneStore); ok {
		t.Fatal("the repository's store can remove objects")
	}
	if _, ok := r.st.(interface {
		Remove(context.Context, string) error
	}); ok {
		t.Fatal("the repository's store has a Remove method")
	}
	if _, ok := r.st.(*store.Memory); ok {
		t.Fatal("the repository holds the store it was opened on")
	}
}
