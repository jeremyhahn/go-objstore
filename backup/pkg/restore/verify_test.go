package restore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/backup/pkg/backup"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// hasProblem reports whether rep holds a fault matching sentinel on object.
func hasProblem(rep *Report, sentinel error, object string) bool {
	if rep == nil {
		return false
	}
	for _, p := range rep.Problems {
		if errors.Is(p, sentinel) && p.Object == object && p.Path == "" {
			return true
		}
	}
	return false
}

func TestVerifyClean(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	again, err := backup.Run(ctx, fx.r, fx.src, backup.Options{Host: "h"})
	if err != nil {
		t.Fatalf("backup.Run: %v", err)
	}
	packs, _ := fx.mem.List(ctx, prefix+"/packs/")
	rep, err := Verify(ctx, fx.r, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rep.Snapshots != 2 || rep.Indexes != 1 || rep.Packs != len(packs) || rep.Chunks < 64 ||
		rep.Files != 8 || rep.Bytes != 2*(64<<10+16) || len(rep.Problems) != 0 {
		t.Fatalf("report %+v", rep)
	}
	rep, err = Verify(ctx, fx.r, VerifyOptions{Snapshots: []seal.ID{again.Snapshot}})
	if err != nil || rep.Snapshots != 1 || rep.Files != 4 {
		t.Fatalf("Verify one = %+v, %v", rep, err)
	}
	missing := seal.ID{1}
	rep, err = Verify(ctx, fx.r, VerifyOptions{Snapshots: []seal.ID{missing}})
	if !errors.Is(err, ErrMissing) || !hasProblem(rep, ErrMissing, fx.r.Layout().Name(repo.KindSnapshot, missing)) {
		t.Fatalf("Verify missing = %v", err)
	}
}

func TestVerifyIndexMismatch(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	ids, err := fx.r.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	entries, err := fx.r.ReadIndex(ctx, ids[0])
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	wrong := entries[0]
	wrong.Length++
	id, err := fx.r.WriteIndex(ctx, []repo.Entry{wrong})
	if err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	rep, err := Verify(ctx, fx.r, VerifyOptions{})
	if !errors.Is(err, errMismatch) || !hasProblem(rep, ErrCorrupt, fx.r.Layout().Name(repo.KindIndex, id)) {
		t.Fatalf("Verify = %v", err)
	}
}

func TestVerifyErrors(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := Verify(canceled, fx.r, VerifyOptions{})
	assertError(t, err, ErrCanceled)

	packs, _ := fx.mem.List(ctx, prefix+"/packs/")
	var gets int
	for name, fail := range map[string]func(string, string) error{
		"list indexes":   failOn("list", "/index/"),
		"list packs":     failOn("list", "/packs/"),
		"read index":     failOn("get", "/index/"),
		"read pack":      failOn("get", "/packs/"),
		"list snapshots": failOn("list", "/snapshots/"),
		"read file": func(op, name string) error {
			if op == "get" && strings.Contains(name, "/packs/") {
				if gets++; gets > len(packs) {
					return errDown
				}
			}
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			fx.st.fail = fail
			defer func() { fx.st.fail = nil }()
			rep, err := Verify(ctx, fx.r, VerifyOptions{})
			assertError(t, err, ErrRepository, errDown)
			if rep == nil || len(rep.Problems) != 0 {
				t.Fatalf("report %+v", rep)
			}
		})
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fx.st.fail = func(op, name string) error {
		if op == "get" && strings.Contains(name, "/packs/") {
			cancel()
		}
		return nil
	}
	_, err = Verify(cctx, fx.r, VerifyOptions{})
	assertError(t, err, ErrCanceled)
	fx.st.fail = nil

	// A stray object among the indexes stops Verify as a fault.
	fx.mem.Replace(prefix+"/index/stray", []byte("x"))
	rep, err := Verify(ctx, fx.r, VerifyOptions{})
	assertError(t, err, ErrCorrupt, repo.ErrMalformedName)
	if len(rep.Problems) != 1 {
		t.Fatalf("problems %v", rep.Problems)
	}
}

func TestVerifyOrphan(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	// A pack whose index was never written is named by no index.
	data := []byte("a chunk no index maps")
	id, err := fx.r.ChunkID(data)
	if err != nil {
		t.Fatalf("ChunkID: %v", err)
	}
	p := fx.r.NewPacker()
	if err := p.Add(ctx, id, data); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	orphan := fx.r.Layout().Name(repo.KindPack, p.Entries()[0].Pack)
	rep, err := Verify(ctx, fx.r, VerifyOptions{})
	assertError(t, err, ErrOrphan)
	if len(rep.Problems) != 1 || !errors.Is(rep.Problems[0], ErrOrphan) || rep.Problems[0].Object != orphan {
		t.Fatalf("problems %v, want the orphan %s", rep.Problems, orphan)
	}

	// With an index unread, an orphan cannot be told apart and is not
	// reported.
	ids, _ := fx.r.ListIndexes(ctx)
	fx.mem.Replace(fx.r.Layout().Name(repo.KindIndex, ids[0]), []byte("x"))
	rep, _ = Verify(ctx, fx.r, VerifyOptions{})
	for _, e := range rep.Problems {
		if errors.Is(e, ErrOrphan) {
			t.Fatalf("orphan reported with an index unread: %v", e)
		}
	}
}
