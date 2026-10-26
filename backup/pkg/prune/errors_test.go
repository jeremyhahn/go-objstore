package prune

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

func failOn(op, part string) func(string, string) error {
	return func(o, name string) error {
		if o == op && strings.Contains(name, part) {
			return errDown
		}
		return nil
	}
}

func TestOpenErrors(t *testing.T) {
	ctx := context.Background()
	_, err := Open(ctx, nil, prefix, passphrase(t))
	assertPruneError(t, err, ErrInvalidOptions)
	_, err = Open(ctx, store.NewMemory(), prefix, passphrase(t))
	assertPruneError(t, err, ErrRepository, repo.ErrNoRepository)
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	fx.three(t)
	opts := Options{Now: func() time.Time { return mark }}
	pol := Policy{Last: 2}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := fx.pr.Run(canceled, pol, opts)
	assertPruneError(t, err, ErrCanceled)
	_, err = fx.pr.Run(ctx, Policy{Last: -1}, opts)
	assertPruneError(t, err, ErrInvalidOptions)
	_, err = fx.pr.Run(ctx, pol, Options{Grace: -time.Second})
	assertPruneError(t, err, ErrInvalidOptions)

	before := contents(fx.mem)
	for name, fail := range map[string]func(string, string) error{
		"list marks":     failOn("list", "/marks/"),
		"list snapshots": failOn("list", "/snapshots/"),
		"list indexes":   failOn("list", "/index/"),
		"list packs":     failOn("list", "/packs/"),
		"read snapshot":  failOn("get", "/snapshots/"),
		"read index":     failOn("get", "/index/"),
		"write index":    failOn("put", "/index/"),
	} {
		t.Run(name, func(t *testing.T) {
			fx.st.fail = fail
			defer func() { fx.st.fail = nil }()
			_, err := fx.pr.Run(ctx, pol, opts)
			e := assertPruneError(t, err, ErrRepository, errDown)
			if e.Op != name {
				t.Fatalf("op %q, want %q", e.Op, name)
			}
			sameContents(t, before, contents(fx.mem))
		})
	}

	// A failed mark write stops the run before anything is removed.
	fx.st.fail = failOn("put", "/marks/")
	_, err = fx.pr.Run(ctx, pol, opts)
	assertPruneError(t, err, ErrRepository, errDown)
	fx.st.fail = nil
	if fx.st.removes != 0 {
		t.Fatalf("%d removals", fx.st.removes)
	}

	// Cancellation part way is reported as such.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fx.st.fail = func(op, name string) error {
		if op == "remove" {
			cancel()
		}
		return nil
	}
	_, err = fx.pr.Run(cctx, pol, opts)
	assertPruneError(t, err, ErrCanceled)
	fx.st.fail = nil
}

func TestCorruptRepository(t *testing.T) {
	ctx := context.Background()
	opts := Options{Now: func() time.Time { return mark }}
	pol := Policy{Last: 2}
	for name, tc := range map[string]struct {
		damage func(fx *fixture)
		want   error
	}{
		"snapshot": {func(fx *fixture) {
			ids, _ := fx.r.ListSnapshots(ctx)
			fx.mem.Replace(fx.r.Layout().Name(repo.KindSnapshot, ids[0]), []byte("x"))
		}, ErrCorrupt},
		"mark": {func(fx *fixture) {
			fx.mem.Replace(fx.r.Layout().Dir(repo.KindMark)+"stray", []byte("x"))
		}, ErrCorrupt},
		"sealed mark": {func(fx *fixture) {
			ids, _ := fx.r.ListMarks(ctx)
			fx.mem.Replace(fx.r.Layout().Name(repo.KindMark, ids[0]), []byte("x"))
		}, ErrCorrupt},
		"no index": {func(fx *fixture) {
			ids, _ := fx.r.ListIndexes(ctx)
			for _, id := range ids {
				fx.mem.Delete(fx.r.Layout().Name(repo.KindIndex, id))
			}
		}, ErrUnindexed},
	} {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			fx.three(t)
			fx.run(t, Policy{Last: 3}, t0.Add(150*time.Minute))
			tc.damage(fx)
			before := contents(fx.mem)
			_, err := fx.pr.Run(ctx, pol, opts)
			assertPruneError(t, err, tc.want)
			sameContents(t, before, contents(fx.mem))
		})
	}
}

// TestOrphanPack removes a pack that no index names, such as one an
// interrupted backup left, once it has been marked for the grace period.
func TestOrphanPack(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	fx.backup(t, t0, map[string][]byte{"b": fileB})
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
	orphan := p.Entries()[0]
	if rep := fx.run(t, Policy{Last: 1}, mark); rep.Marked != 1 || len(rep.Removed) != 0 {
		t.Fatalf("mark %+v", rep)
	}
	rep := fx.run(t, Policy{Last: 1}, sweep)
	if len(rep.Removed) != 1 || rep.Removed[0] != orphan.Pack || rep.RemovedBytes != int64(orphan.Length) {
		t.Fatalf("sweep %+v", rep)
	}
	fx.verify(t, false)
}

func TestErrorMessage(t *testing.T) {
	e := newError("remove pack", "r/packs/ab/abc", ErrRepository, errDown)
	if got := e.Error(); got != "prune remove pack r/packs/ab/abc: prune: repository failure: backend down" {
		t.Fatalf("Error() = %q", got)
	}
	if got := newError("apply policy", "", ErrKeepsNothing, nil).Error(); got != "prune apply policy: prune: policy keeps no snapshot" {
		t.Fatalf("Error() = %q", got)
	}
	if !errors.Is(fromRepo("x", context.DeadlineExceeded), ErrCanceled) {
		t.Fatal("deadline is not ErrCanceled")
	}
	if kindName(repo.KindMark) != "mark" || kindName(repo.KindPack) != "pack" {
		t.Fatal("kind names")
	}
}
