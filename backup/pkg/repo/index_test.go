package repo

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

func TestListAndReadIndexes(t *testing.T) {
	ctx := context.Background()
	r, _ := newRepo(t)
	ids, err := r.ListIndexes(ctx)
	if err != nil || len(ids) != 0 {
		t.Fatalf("ListIndexes = %v, %v", ids, err)
	}
	e := Entry{Chunk: seal.ID{1}, Pack: seal.ID{2}, Length: uint32(seal.Overhead)}
	id, err := r.WriteIndex(ctx, []Entry{e})
	if err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	ids, err = r.ListIndexes(ctx)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("ListIndexes = %v, %v", ids, err)
	}
	got, err := r.ReadIndex(ctx, id)
	if err != nil || len(got) != 1 || got[0] != e {
		t.Fatalf("ReadIndex = %v, %v", got, err)
	}
	_, err = r.ReadIndex(ctx, seal.ID{9})
	assertRepoError(t, err, ErrCorrupt)
}
