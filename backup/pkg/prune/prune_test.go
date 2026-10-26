package prune

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	enclave "github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/backup/pkg/backup"
	"github.com/jeremyhahn/go-objstore/backup/pkg/chunker"
	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/restore"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const prefix = "backups/site"

var (
	smallChunks = chunker.Params{Min: 64, Avg: 256, Max: 1024}
	errDown     = errors.New("backend down")
	// t0 is when the first test snapshot is taken; sweep is a run late
	// enough to remove what the run at mark marked.
	t0    = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	mark  = t0.Add(3 * time.Hour)
	sweep = mark.Add(DefaultGrace)
)

func passphrase(t *testing.T) keyseam.Provider {
	t.Helper()
	h, err := enclave.NewMemory([]byte("correct horse"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	p, err := keyseam.NewPassphraseProvider(h, 0)
	if err != nil {
		t.Fatalf("NewPassphraseProvider: %v", err)
	}
	return p
}

// pruning is the privileged store prune runs with. It fails the failAt-th
// Remove, and any call fail rejects.
type pruning struct {
	*store.Memory
	removes int
	failAt  int
	fail    func(op, name string) error
}

func (s *pruning) check(op, name string) error {
	if s.fail != nil {
		return s.fail(op, name)
	}
	return nil
}

func (s *pruning) Get(ctx context.Context, name string) ([]byte, error) {
	if err := s.check("get", name); err != nil {
		return nil, err
	}
	return s.Memory.Get(ctx, name)
}

func (s *pruning) List(ctx context.Context, p string) ([]string, error) {
	if err := s.check("list", p); err != nil {
		return nil, err
	}
	return s.Memory.List(ctx, p)
}

func (s *pruning) Put(ctx context.Context, name string, data []byte) error {
	if err := s.check("put", name); err != nil {
		return err
	}
	return s.Memory.Put(ctx, name, data)
}

func (s *pruning) Remove(ctx context.Context, name string) error {
	s.removes++
	if s.removes == s.failAt {
		return errDown
	}
	if err := s.check("remove", name); err != nil {
		return err
	}
	return s.Memory.Remove(ctx, name)
}

// hooked is the backup's write-once store. before runs once, ahead of the
// first Put of a name containing on.
type hooked struct {
	store.Store
	on     string
	before func()
}

func (h *hooked) Put(ctx context.Context, name string, data []byte) error {
	if h.before != nil && strings.Contains(name, h.on) {
		f := h.before
		h.before = nil
		f()
	}
	return h.Store.Put(ctx, name, data)
}

type fixture struct {
	mem  *store.Memory
	st   *pruning
	hook *hooked
	// r is the repository backups write through; it cannot remove.
	r  *repo.Repository
	pr *Pruner
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	fx := &fixture{mem: store.NewMemory()}
	fx.st = &pruning{Memory: fx.mem}
	fx.hook = &hooked{Store: fx.mem}
	var err error
	fx.r, err = repo.Init(ctx, fx.hook, prefix, passphrase(t), repo.Options{Chunker: smallChunks, PackSize: repo.MinPackSize})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fx.r.Close() })
	fx.pr, err = Open(ctx, fx.st, prefix, passphrase(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = fx.pr.Close() })
	return fx
}

func content(seed byte, n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

// backup stores files as a snapshot taken at at.
func (fx *fixture) backup(t *testing.T, at time.Time, files map[string][]byte) seal.ID {
	t.Helper()
	root := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	res, err := backup.Run(context.Background(), fx.r, root, backup.Options{Host: "h", Now: func() time.Time { return at }})
	if err != nil {
		t.Fatalf("backup.Run: %v", err)
	}
	return res.Snapshot
}

func (fx *fixture) run(t *testing.T, pol Policy, at time.Time) *Report {
	t.Helper()
	rep, err := fx.pr.Run(context.Background(), pol, Options{Now: func() time.Time { return at }})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep
}

var (
	fileA = content(1, 8<<10)
	fileB = content(2, 8<<10)
	fileC = content(3, 8<<10)
)

// three stores three snapshots: s1 holds a, s2 holds b, s3 holds b and c.
// Keeping the last two forgets s1, and with it every chunk of a.
func (fx *fixture) three(t *testing.T) (s1, s2, s3 seal.ID) {
	t.Helper()
	s1 = fx.backup(t, t0, map[string][]byte{"a": fileA})
	s2 = fx.backup(t, t0.Add(time.Hour), map[string][]byte{"b": fileB})
	s3 = fx.backup(t, t0.Add(2*time.Hour), map[string][]byte{"b": fileB, "c": fileC})
	return s1, s2, s3
}

// chunksOf returns the chunks of every file in snapshot id.
func (fx *fixture) chunksOf(t *testing.T, id seal.ID) map[seal.ID]bool {
	t.Helper()
	s, err := fx.r.ReadSnapshot(context.Background(), id)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	out := map[seal.ID]bool{}
	for _, n := range s.Nodes {
		for _, c := range n.Chunks {
			out[c] = true
		}
	}
	return out
}

// packsOf returns the packs the index maps chunks to.
func (fx *fixture) packsOf(t *testing.T, chunks map[seal.ID]bool) map[seal.ID]bool {
	t.Helper()
	ix, err := fx.r.LoadIndex(context.Background())
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	out := map[seal.ID]bool{}
	for c := range chunks {
		if e, ok := ix.Lookup(c); ok {
			out[e.Pack] = true
		}
	}
	return out
}

// verify checks the whole repository, or only snapshots when given, and
// fails on any problem but a pack no index names when orphans are allowed.
func (fx *fixture) verify(t *testing.T, orphans bool, snapshots ...seal.ID) {
	t.Helper()
	rep, err := restore.Verify(context.Background(), fx.r, restore.VerifyOptions{Snapshots: snapshots})
	if rep == nil {
		t.Fatalf("Verify: %v", err)
	}
	for _, p := range rep.Problems {
		if !orphans || !errors.Is(p, restore.ErrOrphan) {
			t.Fatalf("Verify: %v", p)
		}
	}
	if !orphans && err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// restored restores snapshot id and checks it holds files.
func (fx *fixture) restored(t *testing.T, id seal.ID, files map[string][]byte) {
	t.Helper()
	dir := t.TempDir()
	if _, err := restore.Restore(context.Background(), fx.r, id, dir, restore.Options{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("restored %s: %d bytes, %v", name, len(got), err)
		}
	}
}

func contents(m *store.Memory) map[string]string {
	out := map[string]string{}
	m.Each(func(name string, data []byte) { out[name] = string(data) })
	return out
}

func sameContents(t *testing.T, before, after map[string]string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("%d objects, %d before", len(after), len(before))
	}
	for name, data := range before {
		if after[name] != data {
			t.Fatalf("%s changed", name)
		}
	}
}

func assertPruneError(t *testing.T, err error, want ...error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error %v is not a *prune.Error", err)
	}
	for _, w := range want {
		if !errors.Is(err, w) {
			t.Fatalf("error %v does not match %v", err, w)
		}
	}
	if e.Error() == "" {
		t.Fatal("empty message")
	}
	return e
}

func TestPrune(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	s1, s2, s3 := fx.three(t)
	gone := fx.chunksOf(t, s1)
	for c := range fx.chunksOf(t, s3) {
		delete(gone, c)
	}
	goneP := fx.packsOf(t, gone)
	pol := Policy{Last: 2}

	// The first run forgets s1 and marks its packs; it removes no pack.
	rep := fx.run(t, pol, mark)
	if len(rep.Kept) != 2 || len(rep.Forgotten) != 1 || rep.Forgotten[0] != s1 ||
		len(rep.Removed) != 0 || rep.Marked != len(goneP) || len(rep.Superseded) != 3 || (rep.Index == seal.ID{}) {
		t.Fatalf("first run %+v", rep)
	}
	if ok, _ := fx.mem.Exists(ctx, fx.r.Layout().Name(repo.KindSnapshot, s1)); ok {
		t.Fatal("the forgotten snapshot is still stored")
	}
	fx.verify(t, false)

	// Within the grace period the marked packs stay.
	rep = fx.run(t, pol, sweep.Add(-time.Minute))
	if len(rep.Removed) != 0 || rep.Marked != len(goneP) {
		t.Fatalf("run within grace %+v", rep)
	}

	// After it they go, with every chunk only s1 used.
	rep = fx.run(t, pol, sweep)
	if len(rep.Removed) != len(goneP) || rep.Marked != 0 || rep.RemovedBytes < int64(len(fileA)) || len(rep.Superseded) != 1 {
		t.Fatalf("sweep %+v", rep)
	}
	for _, id := range rep.Removed {
		if !goneP[id] {
			t.Fatalf("removed pack %s held a kept chunk", id)
		}
		if ok, _ := fx.mem.Exists(ctx, fx.r.Layout().Name(repo.KindPack, id)); ok {
			t.Fatalf("removed pack %s is still stored", id)
		}
	}
	ix, err := fx.r.LoadIndex(ctx)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	for c := range gone {
		if _, ok := ix.Lookup(c); ok {
			t.Fatalf("forgotten chunk %s is still indexed", c)
		}
	}
	if ids, _ := fx.r.ListMarks(ctx); len(ids) != 1 {
		t.Fatalf("%d marks left", len(ids))
	}
	fx.verify(t, false)
	fx.restored(t, s2, map[string][]byte{"b": fileB})
	fx.restored(t, s3, map[string][]byte{"b": fileB, "c": fileC})

	// Nothing is left to do.
	rep = fx.run(t, pol, sweep.Add(time.Hour))
	if len(rep.Forgotten) != 0 || len(rep.Removed) != 0 || len(rep.Superseded) != 0 || rep.Marked != 0 {
		t.Fatalf("idle run %+v", rep)
	}
}

func TestPartlyUsedPacks(t *testing.T) {
	fx := newFixture(t)
	// a and b share a pack; b alone is kept.
	small := content(4, 3<<10)
	fx.backup(t, t0, map[string][]byte{"a": small, "b": fileB})
	s2 := fx.backup(t, t0.Add(time.Hour), map[string][]byte{"b": fileB})
	rep := fx.run(t, Policy{Last: 1}, mark)
	if rep.Partial < 1 || rep.UnusedBytes == 0 || rep.UnusedBytes > int64(len(small))+1<<10 {
		t.Fatalf("report %+v", rep)
	}
	rep = fx.run(t, Policy{Last: 1}, sweep)
	if rep.Partial < 1 || rep.UnusedBytes == 0 {
		t.Fatalf("sweep %+v", rep)
	}
	fx.verify(t, false)
	fx.restored(t, s2, map[string][]byte{"b": fileB})
}

// TestBackupDuringPrune runs a backup that dedups against packs a prune run
// marks while the backup is under way.
func TestBackupDuringPrune(t *testing.T) {
	fx := newFixture(t)
	fx.backup(t, t0, map[string][]byte{"a": fileA})
	fx.backup(t, t0.Add(time.Hour), map[string][]byte{"b": fileB})
	var marked *Report
	fx.hook.on = "/snapshots/"
	fx.hook.before = func() { marked = fx.run(t, Policy{Last: 1}, mark) }
	s3 := fx.backup(t, t0.Add(2*time.Hour), map[string][]byte{"a": fileA})
	if marked == nil || marked.Marked == 0 {
		t.Fatalf("mark run %+v", marked)
	}
	rep := fx.run(t, Policy{Last: 2}, sweep)
	if len(rep.Removed) != 0 {
		t.Fatalf("sweep removed %d packs the running backup used", len(rep.Removed))
	}
	fx.verify(t, false)
	fx.restored(t, s3, map[string][]byte{"a": fileA})
}

// TestBackupBetweenMarkAndSweep runs a backup of the forgotten content after
// the mark: it stores it again, under other pack names, so the sweep loses
// nothing.
func TestBackupBetweenMarkAndSweep(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	s1 := fx.backup(t, t0, map[string][]byte{"a": fileA})
	fx.backup(t, t0.Add(time.Hour), map[string][]byte{"b": fileB})
	oldP := fx.packsOf(t, fx.chunksOf(t, s1))
	fx.run(t, Policy{Last: 1}, mark)
	res, err := backup.Run(ctx, fx.r, writeTree(t, map[string][]byte{"a": fileA}),
		backup.Options{Host: "h", Now: func() time.Time { return mark.Add(time.Hour) }})
	if err != nil {
		t.Fatalf("backup.Run: %v", err)
	}
	if res.Stats.ChunksDeduped != 0 || res.Stats.PacksReused != 0 || res.Stats.PacksWritten != len(oldP) {
		t.Fatalf("backup after mark %+v", res.Stats)
	}
	// The sweep keeps the old packs, which the kept snapshot's chunks are in,
	// but indexes the new copies; later runs mark and remove the old ones.
	rep := fx.run(t, Policy{Last: 2}, sweep)
	if len(rep.Removed) != 0 || rep.Marked != 0 {
		t.Fatalf("sweep %+v", rep)
	}
	// Until then Verify reports the old packs as in no index.
	fx.verify(t, true)
	if rep = fx.run(t, Policy{Last: 2}, sweep.Add(time.Hour)); rep.Marked != len(oldP) {
		t.Fatalf("run after sweep %+v", rep)
	}
	rep = fx.run(t, Policy{Last: 2}, sweep.Add(time.Hour+DefaultGrace))
	if len(rep.Removed) != len(oldP) {
		t.Fatalf("second sweep %+v", rep)
	}
	for _, id := range rep.Removed {
		if !oldP[id] {
			t.Fatalf("sweep removed %s, not a marked pack", id)
		}
	}
	fx.verify(t, false)
	fx.restored(t, res.Snapshot, map[string][]byte{"a": fileA})
}

func writeTree(t *testing.T, files map[string][]byte) string {
	t.Helper()
	root := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	return root
}

// TestInterrupted fails each removal in turn, checks every kept snapshot
// still restores, and checks a rerun completes the prune.
func TestInterrupted(t *testing.T) {
	ctx := context.Background()
	pol := Policy{Last: 2}
	for n := 1; ; n++ {
		fx := newFixture(t)
		_, s2, s3 := fx.three(t)
		fx.st.failAt = n
		failed := false
		for _, at := range []time.Time{mark, sweep} {
			opts := Options{Now: func() time.Time { return at }}
			if _, err := fx.pr.Run(ctx, pol, opts); err != nil {
				failed = true
				e := assertPruneError(t, err, ErrRepository, errDown)
				if !strings.HasPrefix(e.Op, "remove ") || e.Object == "" {
					t.Fatalf("removal %d: %+v", n, e)
				}
				fx.verify(t, true, s2, s3)
				fx.restored(t, s3, map[string][]byte{"b": fileB, "c": fileC})
				if _, err := fx.pr.Run(ctx, pol, opts); err != nil {
					t.Fatalf("rerun after removal %d failed: %v", n, err)
				}
			}
		}
		fx.verify(t, false)
		fx.restored(t, s2, map[string][]byte{"b": fileB})
		if !failed {
			if n < 5 {
				t.Fatalf("only %d removals", n-1)
			}
			return
		}
	}
}

func TestDryRun(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	s1, _, _ := fx.three(t)
	pol := Policy{Last: 2}
	before := contents(fx.mem)
	rep, err := fx.pr.Run(ctx, pol, Options{DryRun: true, Now: func() time.Time { return mark }})
	if err != nil || !rep.DryRun || len(rep.Forgotten) != 1 || rep.Forgotten[0] != s1 || rep.Marked == 0 ||
		len(rep.Superseded) != 3 || !(rep.Index == seal.ID{}) {
		t.Fatalf("dry run %+v, %v", rep, err)
	}
	sameContents(t, before, contents(fx.mem))

	fx.run(t, pol, mark)
	removes := fx.st.removes
	before = contents(fx.mem)
	rep, err = fx.pr.Run(ctx, pol, Options{DryRun: true, Now: func() time.Time { return sweep }})
	if err != nil || len(rep.Removed) == 0 || rep.RemovedBytes < int64(len(fileA)) || len(rep.Superseded) != 1 {
		t.Fatalf("dry sweep %+v, %v", rep, err)
	}
	sameContents(t, before, contents(fx.mem))
	if fx.st.removes != removes {
		t.Fatalf("the dry run removed %d objects", fx.st.removes-removes)
	}
	real := fx.run(t, pol, sweep)
	if len(real.Removed) != len(rep.Removed) || real.RemovedBytes != rep.RemovedBytes {
		t.Fatalf("sweep %+v, dry run said %+v", real, rep)
	}
}

func TestKeepsNothing(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	opts := Options{Now: func() time.Time { return mark }}
	// An empty repository keeps nothing whatever the policy.
	_, err := fx.pr.Run(ctx, Policy{Last: 1}, opts)
	assertPruneError(t, err, ErrKeepsNothing)
	fx.three(t)
	before := contents(fx.mem)
	for _, pol := range []Policy{{}, {Within: time.Minute}} {
		_, err := fx.pr.Run(ctx, pol, opts)
		assertPruneError(t, err, ErrKeepsNothing)
	}
	sameContents(t, before, contents(fx.mem))
	if fx.st.removes != 0 {
		t.Fatalf("%d removals", fx.st.removes)
	}
}
