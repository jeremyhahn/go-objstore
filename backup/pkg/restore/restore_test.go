package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
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
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const prefix = "backups/site"

// smallChunks and the smallest pack size spread a test tree over many
// chunks and packs.
var smallChunks = chunker.Params{Min: 64, Avg: 256, Max: 1024}

var emptyDigest = hex.EncodeToString(sha256.New().Sum(nil))

// faulty is a store whose calls fail when fail returns an error.
type faulty struct {
	store.Store
	fail func(op, name string) error
}

func (f *faulty) check(op, name string) error {
	if f.fail == nil {
		return nil
	}
	return f.fail(op, name)
}

func (f *faulty) Put(ctx context.Context, name string, data []byte) error {
	if err := f.check("put", name); err != nil {
		return err
	}
	return f.Store.Put(ctx, name, data)
}

func (f *faulty) Get(ctx context.Context, name string) ([]byte, error) {
	if err := f.check("get", name); err != nil {
		return nil, err
	}
	return f.Store.Get(ctx, name)
}

func (f *faulty) List(ctx context.Context, p string) ([]string, error) {
	if err := f.check("list", p); err != nil {
		return nil, err
	}
	return f.Store.List(ctx, p)
}

var errDown = errors.New("store down")

// failOn fails the given operation on names containing part.
func failOn(op, part string) func(string, string) error {
	return func(o, name string) error {
		if o == op && strings.Contains(name, part) {
			return errDown
		}
		return nil
	}
}

type fixture struct {
	r   *repo.Repository
	mem *store.Memory
	st  *faulty
	src string
	id  seal.ID
}

// newFixture backs up a test tree into a fresh memory repository.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	h, err := enclave.NewMemory([]byte("correct horse"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	p, err := keyseam.NewPassphraseProvider(h, 0)
	if err != nil {
		t.Fatalf("NewPassphraseProvider: %v", err)
	}
	fx := &fixture{mem: store.NewMemory()}
	fx.st = &faulty{Store: fx.mem}
	fx.r, err = repo.Init(ctx, fx.st, prefix, p, repo.Options{Chunker: smallChunks, PackSize: repo.MinPackSize})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = fx.r.Close() })
	fx.src = tree(t)
	res, err := backup.Run(ctx, fx.r, fx.src, backup.Options{Host: "h"})
	if err != nil {
		t.Fatalf("backup.Run: %v", err)
	}
	fx.id = res.Snapshot
	return fx
}

// tree builds nested directories, an empty file, a file of many chunks,
// symlinks, distinct modes and fixed modification times.
func tree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	large := make([]byte, 64<<10)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(large)
	dirs := []struct {
		path string
		perm fs.FileMode
	}{{"a", 0o750}, {"a/b", 0o700}, {"empty-dir", 0o755}}
	files := []struct {
		path string
		perm fs.FileMode
		data []byte
	}{
		{"top.txt", 0o644, []byte("hello\n")},
		{"a/empty", 0o600, nil},
		{"a/exec.sh", 0o755, []byte("#!/bin/sh\n")},
		{"a/b/large.bin", 0o640, large},
	}
	for _, d := range dirs {
		if err := os.Mkdir(filepath.Join(root, d.path), 0o700); err != nil {
			t.Fatalf("Mkdir: %v", err)
		}
	}
	for _, f := range files {
		p := filepath.Join(root, f.path)
		if err := os.WriteFile(p, f.data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := os.Chmod(p, f.perm); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
	}
	for name, target := range map[string]string{"a/link": "b/large.bin", "a/b/up": "../../top.txt"} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
	}
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	stamp := func(p string, i int) {
		at := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(filepath.Join(root, p), at, at); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
	}
	for i, f := range files {
		stamp(f.path, i)
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(filepath.Join(root, dirs[i].path), dirs[i].perm); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		stamp(dirs[i].path, 10+i)
	}
	stamp(".", 20)
	return root
}

type entry struct {
	typ   string
	perm  fs.FileMode
	mtime time.Time
	data  string
}

// listTree describes every entry under root.
func listTree(t *testing.T, root string) map[string]entry {
	t.Helper()
	m := map[string]entry{}
	err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		e := entry{perm: info.Mode().Perm(), mtime: info.ModTime().UTC()}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			e = entry{typ: "link", data: target}
		case info.IsDir():
			e.typ = "dir"
		default:
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			e.typ, e.data = "file", string(b)
		}
		m[filepath.ToSlash(rel)] = e
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	return m
}

func sameTree(t *testing.T, want, got string) {
	t.Helper()
	w, g := listTree(t, want), listTree(t, got)
	if len(w) != len(g) {
		t.Fatalf("restored %d entries, want %d", len(g), len(w))
	}
	for p, we := range w {
		ge, ok := g[p]
		if !ok || ge.typ != we.typ || ge.perm != we.perm || !ge.mtime.Equal(we.mtime) || ge.data != we.data {
			t.Fatalf("entry %q: got %s %v %v, want %s %v %v", p, ge.typ, ge.perm, ge.mtime, we.typ, we.perm, we.mtime)
		}
	}
}

// empty fails unless dir holds nothing.
func empty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%s holds %d entries, want none", dir, len(entries))
	}
}

func assertError(t *testing.T, err error, want ...error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Op == "" || e.Error() == "" {
		t.Fatalf("error %v is not a *restore.Error", err)
	}
	for _, w := range want {
		if !errors.Is(err, w) {
			t.Fatalf("error %v does not match %v", err, w)
		}
	}
	return e
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	var gets int
	fx.st.fail = func(op, name string) error {
		if op == "get" && strings.Contains(name, "/packs/") {
			gets++
		}
		return nil
	}
	dst := filepath.Join(t.TempDir(), "new", "dst")
	st, err := Restore(ctx, fx.r, fx.id, dst, Options{})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	sameTree(t, fx.src, dst)
	if st.Files != 4 || st.Dirs != 4 || st.Symlinks != 2 || st.Bytes != 64<<10+16 {
		t.Fatalf("stats %+v", st)
	}
	packs, _ := fx.mem.List(ctx, prefix+"/packs/")
	if len(packs) < 5 || gets != len(packs) {
		t.Fatalf("fetched %d packs for %d", gets, len(packs))
	}
}

func TestOverwrite(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	dst := t.TempDir()
	if _, err := Restore(ctx, fx.r, fx.id, dst, Options{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	top := filepath.Join(dst, "top.txt")
	if err := os.WriteFile(top, []byte("changed"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := Restore(ctx, fx.r, fx.id, dst, Options{})
	if e := assertError(t, err, ErrExists); e.Path == "" {
		t.Fatalf("error %v names no path", err)
	}
	if b, _ := os.ReadFile(top); string(b) != "changed" {
		t.Fatalf("refused restore changed %q", b)
	}
	if _, err := Restore(ctx, fx.r, fx.id, dst, Options{Overwrite: true}); err != nil {
		t.Fatalf("Restore with overwrite: %v", err)
	}
	sameTree(t, fx.src, dst)

	// A directory is never replaced, and a file never stands for one.
	for name, mk := range map[string]func(string) error{
		"top.txt": func(p string) error { return os.Mkdir(p, 0o700) },
		"a":       func(p string) error { return os.WriteFile(p, nil, 0o600) },
	} {
		dst := t.TempDir()
		if err := mk(filepath.Join(dst, name)); err != nil {
			t.Fatalf("make %s: %v", name, err)
		}
		_, err := Restore(ctx, fx.r, fx.id, dst, Options{Overwrite: true})
		assertError(t, err, ErrExists)
	}
}

func TestTamper(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	if _, err := Verify(ctx, fx.r, VerifyOptions{}); err != nil {
		t.Fatalf("Verify of a clean repository: %v", err)
	}
	var names []string
	fx.mem.Each(func(name string, _ []byte) {
		for _, kind := range []string{"/packs/", "/index/", "/snapshots/"} {
			if strings.Contains(name, kind) {
				names = append(names, name)
			}
		}
	})
	if len(names) < 7 {
		t.Fatalf("only %d objects", len(names))
	}
	for _, name := range names {
		orig, err := fx.mem.Get(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		for _, at := range []int{0, len(orig) / 2, len(orig) - 1} {
			flipped := bytes.Clone(orig)
			flipped[at] ^= 0x10
			fx.mem.Replace(name, flipped)

			dst := t.TempDir()
			_, err := Restore(ctx, fx.r, fx.id, dst, Options{})
			if e := assertError(t, err, ErrCorrupt); e.Object != name {
				t.Fatalf("flip at %d of %s: error names %q", at, name, e.Object)
			}
			empty(t, dst)

			rep, err := Verify(ctx, fx.r, VerifyOptions{})
			if err == nil || !hasProblem(rep, ErrCorrupt, name) {
				t.Fatalf("flip at %d of %s: verify %v", at, name, err)
			}
		}
		fx.mem.Replace(name, orig)
	}
}

func TestMissing(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	packs, _ := fx.mem.List(ctx, prefix+"/packs/")
	fx.mem.Delete(packs[0])
	_, err := Restore(ctx, fx.r, fx.id, t.TempDir(), Options{})
	if e := assertError(t, err, ErrMissing, store.ErrNotFound); e.Object != packs[0] {
		t.Fatalf("error names %q, want %q", e.Object, packs[0])
	}
	rep, err := Verify(ctx, fx.r, VerifyOptions{})
	if err == nil || !hasProblem(rep, ErrMissing, packs[0]) {
		t.Fatalf("verify %v", err)
	}

	_, err = Restore(ctx, fx.r, seal.ID{1}, t.TempDir(), Options{})
	assertError(t, err, ErrMissing)

	indexes, _ := fx.mem.List(ctx, prefix+"/index/")
	for _, name := range indexes {
		fx.mem.Delete(name)
	}
	_, err = Restore(ctx, fx.r, fx.id, t.TempDir(), Options{})
	if e := assertError(t, err, ErrUnindexed); e.Chunk == "" {
		t.Fatalf("error %v names no chunk", err)
	}
}

// craft writes a manifest holding nodes.
func craft(t *testing.T, r *repo.Repository, nodes ...repo.Node) seal.ID {
	t.Helper()
	id, err := r.WriteSnapshot(context.Background(), &repo.Snapshot{Time: time.Now(), Nodes: nodes})
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	return id
}

func dirNode(p string) repo.Node {
	return repo.Node{Path: p, Type: repo.NodeDir, Mode: fs.ModeDir | 0o755}
}

func fileNode(p string) repo.Node {
	return repo.Node{Path: p, Type: repo.NodeFile, Mode: 0o644, SHA256: emptyDigest}
}

func linkNode(p, target string) repo.Node {
	return repo.Node{Path: p, Type: repo.NodeSymlink, Mode: fs.ModeSymlink | 0o777, Target: target}
}

func TestUnsafeManifest(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	for name, tc := range map[string]struct {
		nodes []repo.Node
		found string // an existing symlink in the target, to ../outside
	}{
		"absolute":       {nodes: []repo.Node{fileNode("/tmp/x")}},
		"dot dot":        {nodes: []repo.Node{fileNode("../outside/x")}},
		"inner dot dot":  {nodes: []repo.Node{dirNode("a"), fileNode("a/../../outside/x")}},
		"link out":       {nodes: []repo.Node{linkNode("l", "../outside")}},
		"link absolute":  {nodes: []repo.Node{linkNode("l", "/etc")}},
		"link chain":     {nodes: []repo.Node{dirNode("d"), linkNode("d/s", ".."), linkNode("x", "d/s/../outside")}},
		"through link":   {nodes: []repo.Node{dirNode("sub"), linkNode("l", "sub"), fileNode("l/f")}},
		"found dir":      {nodes: []repo.Node{dirNode("evil"), fileNode("evil/f")}, found: "evil"},
		"found in path":  {nodes: []repo.Node{linkNode("x", "evil/f")}, found: "evil"},
		"found replaced": {nodes: []repo.Node{dirNode("evil")}, found: "evil"},
	} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			outside, dst := filepath.Join(base, "outside"), filepath.Join(base, "dst")
			for _, d := range []string{outside, dst} {
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatalf("Mkdir: %v", err)
				}
			}
			if tc.found != "" {
				if err := os.Symlink("../outside", filepath.Join(dst, tc.found)); err != nil {
					t.Fatalf("Symlink: %v", err)
				}
			}
			id := craft(t, fx.r, tc.nodes...)
			_, err := Restore(ctx, fx.r, id, dst, Options{Overwrite: true})
			assertError(t, err, ErrUnsafeEntry)
			empty(t, outside)
			if got := len(listTree(t, dst)); tc.found == "" && got != 1 || tc.found != "" && got != 2 {
				t.Fatalf("target holds %d entries", got)
			}
			// Verify reads no target, so only the manifest's own faults show.
			rep, err := Verify(ctx, fx.r, VerifyOptions{Snapshots: []seal.ID{id}})
			if tc.found == "" && (err == nil || !hasProblem(rep, ErrUnsafeEntry, fx.r.Layout().Name(repo.KindSnapshot, id))) ||
				tc.found != "" && err != nil {
				t.Fatalf("verify %v", err)
			}
		})
	}
}

func TestBadFileData(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	snap, err := fx.r.ReadSnapshot(ctx, fx.id)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	var large repo.Node
	for _, n := range snap.Nodes {
		if n.Path == "a/b/large.bin" {
			large = n
		}
	}
	wrongDigest := large
	wrongDigest.Path = "large.bin"
	wrongDigest.SHA256 = strings.Repeat("0", 64)
	wrongSize := fileNode("short")
	wrongSize.Size = 5
	unknown := fileNode("unknown")
	unknown.Chunks = []seal.ID{{7}}
	for name, tc := range map[string]struct {
		node  repo.Node
		want  error
		cause error
	}{
		"digest":    {wrongDigest, ErrCorrupt, errDigest},
		"size":      {wrongSize, ErrCorrupt, errSize},
		"unindexed": {unknown, ErrUnindexed, nil},
	} {
		t.Run(name, func(t *testing.T) {
			id := craft(t, fx.r, fileNode("ok"), tc.node)
			dst := t.TempDir()
			_, err := Restore(ctx, fx.r, id, dst, Options{})
			e := assertError(t, err, tc.want)
			if tc.cause != nil && !errors.Is(err, tc.cause) || e.Path != tc.node.Path {
				t.Fatalf("error %v", err)
			}
			empty(t, dst)
			rep, err := Verify(ctx, fx.r, VerifyOptions{Snapshots: []seal.ID{id}})
			if err == nil || !hasProblem(rep, tc.want, fx.r.Layout().Name(repo.KindSnapshot, id)) {
				t.Fatalf("verify %v", err)
			}
		})
	}
}

func TestRestoreErrors(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := Restore(canceled, fx.r, fx.id, t.TempDir(), Options{})
	assertError(t, err, ErrCanceled)

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err = Restore(ctx, fx.r, fx.id, file, Options{})
	assertError(t, err, ErrInvalidTarget)
	_, err = Restore(ctx, fx.r, fx.id, filepath.Join(file, "sub"), Options{})
	assertError(t, err, ErrFilesystem)

	for name, fail := range map[string]func(string, string) error{
		"index": failOn("list", "/index/"),
		"pack":  failOn("get", "/packs/"),
	} {
		t.Run(name, func(t *testing.T) {
			fx.st.fail = fail
			defer func() { fx.st.fail = nil }()
			dst := t.TempDir()
			_, err := Restore(ctx, fx.r, fx.id, dst, Options{})
			assertError(t, err, ErrRepository, errDown)
			empty(t, dst)
		})
	}

	// Canceled while fetching packs.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fx.st.fail = func(op, name string) error {
		if op == "get" && strings.Contains(name, "/packs/") {
			cancel()
		}
		return nil
	}
	dst := t.TempDir()
	_, err = Restore(cctx, fx.r, fx.id, dst, Options{})
	assertError(t, err, ErrCanceled)
	empty(t, dst)
}

func TestReadOnlyTarget(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions do not bind the superuser")
	}
	fx := newFixture(t)
	dst := t.TempDir()
	if err := os.Chmod(dst, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dst, 0o700) }()
	_, err := Restore(context.Background(), fx.r, fx.id, dst, Options{})
	assertError(t, err, ErrFilesystem, fs.ErrPermission)
}
