package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	enclave "github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/backup/pkg/chunker"
	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const (
	prefix     = "vault"
	host       = "host-quokka-17"
	contentTag = "PLAINTEXT-MARKER-of-a-file"
	largeName  = "ledger-zebra.bin"
	smallName  = "notes-walrus.txt"
	dirName    = "quarterly-reports"
	subName    = "empty-pelican"
	linkName   = "link-heron"
	pipeName   = "pipe-ibis"
	largeSize  = 64 << 10
)

var smallChunks = chunker.Params{Min: 64, Avg: 256, Max: 1024}

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

// tree builds the test tree and returns its root and the large file's
// content.
func tree(t *testing.T) (root string, large []byte, special bool) {
	t.Helper()
	root = t.TempDir()
	large = make([]byte, largeSize)
	rng := rand.NewChaCha8([32]byte{1})
	_, _ = rng.Read(large)
	copy(large[largeSize/4:], contentTag)
	write := func(rel string, data []byte) {
		if err := os.WriteFile(filepath.Join(root, rel), data, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, dirName, subName), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	write(smallName, []byte("a short note: "+contentTag))
	write(filepath.Join(dirName, largeName), large)
	if err := os.Symlink(smallName, filepath.Join(root, linkName)); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	special = mkSpecial(t, filepath.Join(root, pipeName))
	return root, large, special
}

// newRepo initializes a repository in st.
func newRepo(t *testing.T, st store.Store) *repo.Repository {
	t.Helper()
	r, err := repo.Init(context.Background(), st, prefix, passphrase(t),
		repo.Options{Chunker: smallChunks, PackSize: repo.MinPackSize})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func at(minute int) Options {
	return Options{Host: host, Now: func() time.Time {
		return time.Date(2026, 10, 26, 9, minute, 0, 0, time.UTC)
	}}
}

func assertBackupError(t *testing.T, err error, want ...error) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Op == "" || e.Error() == "" {
		t.Fatalf("error %v is not a *backup.Error", err)
	}
	for _, w := range want {
		if !errors.Is(err, w) {
			t.Fatalf("error %v does not match %v", err, w)
		}
	}
}

// restoreFile reads a file node's content back from its chunks.
func restoreFile(t *testing.T, r *repo.Repository, n repo.Node) []byte {
	t.Helper()
	ctx := context.Background()
	ix, err := r.LoadIndex(ctx)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}
	var out []byte
	for _, id := range n.Chunks {
		e, ok := ix.Lookup(id)
		if !ok {
			t.Fatalf("chunk %s of %s is not indexed", id, n.Path)
		}
		c, err := r.ReadChunk(ctx, e)
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		out = append(out, c...)
	}
	return out
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	r := newRepo(t, st)
	root, large, special := tree(t)

	res, err := Run(ctx, r, root, at(0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := res.Stats
	wantSkipped := 0
	if special {
		wantSkipped = 1
	}
	if s.Files != 2 || s.Dirs != 3 || s.Symlinks != 1 || s.Skipped != wantSkipped ||
		s.Bytes != int64(largeSize+len("a short note: "+contentTag)) || s.ChunksNew == 0 ||
		s.PacksWritten < 2 || res.Index == (seal.ID{}) {
		t.Fatalf("stats %+v", res)
	}

	snap, err := r.ReadSnapshot(ctx, res.Snapshot)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snap.Host != host || !snap.Time.Equal(at(0).Now()) || snap.Root != root || snap.Skipped != wantSkipped {
		t.Fatalf("manifest %+v", snap)
	}
	nodes := map[string]repo.Node{}
	for _, n := range snap.Nodes {
		nodes[n.Path] = n
	}
	if len(nodes) != 6 || nodes["."].Type != repo.NodeDir || nodes[dirName+"/"+subName].Type != repo.NodeDir ||
		nodes[linkName].Type != repo.NodeSymlink || nodes[linkName].Target != smallName {
		t.Fatalf("nodes %+v", snap.Nodes)
	}
	big := nodes[dirName+"/"+largeName]
	sum := sha256.Sum256(large)
	if big.Type != repo.NodeFile || big.Size != largeSize || big.SHA256 != hex.EncodeToString(sum[:]) ||
		big.Mode != 0o600 || big.ModTime.IsZero() {
		t.Fatalf("large file node %+v", big)
	}
	if got := restoreFile(t, r, big); !bytes.Equal(got, large) {
		t.Fatal("large file does not reassemble from its chunks")
	}

	// An unchanged tree stores nothing new but its manifest.
	packs := countPacks(t, st)
	again, err := Run(ctx, r, root, at(1))
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if again.Stats.PacksWritten != 0 || again.Stats.ChunksNew != 0 || again.Index != (seal.ID{}) ||
		countPacks(t, st) != packs || again.Snapshot == res.Snapshot {
		t.Fatalf("unchanged run %+v", again)
	}

	// A one-byte edit in the large file stores only the chunks around it.
	large[largeSize/2] ^= 0xff
	if err := os.WriteFile(filepath.Join(root, dirName, largeName), large, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	edit, err := Run(ctx, r, root, at(2))
	if err != nil {
		t.Fatalf("edit Run: %v", err)
	}
	total := len(big.Chunks)
	if edit.Stats.ChunksNew < 1 || edit.Stats.ChunksNew > 3 || edit.Stats.BytesNew > 3*int64(smallChunks.Max) ||
		edit.Stats.PacksWritten != 1 || edit.Stats.ChunksDeduped < total-3 {
		t.Fatalf("edit stored %+v of %d chunks", edit.Stats, total)
	}
	snap, _ = r.ReadSnapshot(ctx, edit.Snapshot)
	for _, n := range snap.Nodes {
		if n.Path == dirName+"/"+largeName && !bytes.Equal(restoreFile(t, r, n), large) {
			t.Fatal("edited file does not reassemble")
		}
	}
	if ids, _ := r.ListSnapshots(ctx); len(ids) != 3 {
		t.Fatalf("%d snapshots, want 3", len(ids))
	}

	// No name, path, host or content appears in any stored byte or name.
	needles := []string{largeName, smallName, dirName, subName, linkName, pipeName, contentTag, host, root,
		"2026-10-26"}
	window := string(large[1000:1064])
	st.Each(func(name string, data []byte) {
		for _, n := range append(needles, window) {
			if strings.Contains(name, n) || bytes.Contains(data, []byte(n)) {
				t.Fatalf("object %s holds plaintext %q", name, n)
			}
		}
	})
}

func countPacks(t *testing.T, st store.Store) int {
	t.Helper()
	names, err := st.List(context.Background(), prefix+"/packs/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return len(names)
}

// faulty is a store whose calls fail when fail returns an error.
type faulty struct {
	store.Store
	fail func(op, name string) error
}

func (f *faulty) Put(ctx context.Context, name string, data []byte) error {
	if err := f.fail("put", name); err != nil {
		return err
	}
	return f.Store.Put(ctx, name, data)
}

func (f *faulty) List(ctx context.Context, p string) ([]string, error) {
	if err := f.fail("list", p); err != nil {
		return nil, err
	}
	return f.Store.List(ctx, p)
}

var errDown = errors.New("store down")

func TestInterrupted(t *testing.T) {
	ctx := context.Background()
	root, _, _ := tree(t)
	for name, fail := range map[string]func(*int) func(string, string) error{
		// The second pack fails: the first is left, with no index.
		"pack": func(n *int) func(string, string) error {
			return func(op, name string) error {
				if op == "put" && strings.Contains(name, "/packs/") {
					if *n++; *n == 2 {
						return errDown
					}
				}
				return nil
			}
		},
		"index":    failing("put", "/index/"),
		"snapshot": failing("put", "/snapshots/"),
	} {
		t.Run(name, func(t *testing.T) {
			st := store.NewMemory()
			_ = newRepo(t, st)
			var n int
			f := &faulty{Store: st, fail: fail(&n)}
			r, err := repo.Open(ctx, f, prefix, passphrase(t))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer r.Close()
			_, err = Run(ctx, r, root, at(0))
			assertBackupError(t, err, ErrRepository, repo.ErrStorage, errDown)
			if ids, _ := r.ListSnapshots(ctx); len(ids) != 0 {
				t.Fatalf("an interrupted run left %d manifests", len(ids))
			}

			// The next run completes, reusing what the first one stored.
			f.fail = func(string, string) error { return nil }
			res, err := Run(ctx, r, root, at(1))
			if err != nil {
				t.Fatalf("rerun: %v", err)
			}
			if name == "pack" && res.Stats.PacksReused != 1 {
				t.Fatalf("rerun reused %d packs, want 1", res.Stats.PacksReused)
			}
			if ids, _ := r.ListSnapshots(ctx); len(ids) != 1 {
				t.Fatalf("%d manifests after the rerun", len(ids))
			}
		})
	}
}

func failing(op, part string) func(*int) func(string, string) error {
	return func(*int) func(string, string) error {
		return func(o, name string) error {
			if o == op && strings.Contains(name, part) {
				return errDown
			}
			return nil
		}
	}
}

// countdown is a context that is canceled once Err has been asked left
// times.
type countdown struct {
	context.Context
	left int
}

func (c *countdown) Err() error {
	if c.left--; c.left < 0 {
		return context.Canceled
	}
	return nil
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	r := newRepo(t, st)
	root, _, _ := tree(t)

	_, err := Run(ctx, r, filepath.Join(root, smallName), Options{})
	assertBackupError(t, err, ErrInvalidRoot)
	_, err = Run(ctx, r, filepath.Join(root, "missing"), Options{})
	assertBackupError(t, err, ErrFilesystem, os.ErrNotExist)

	done, cancel := context.WithCancel(ctx)
	cancel()
	_, err = Run(done, r, root, Options{})
	assertBackupError(t, err, ErrCanceled, context.Canceled)
	// Canceled part way through the large file.
	_, err = Run(&countdown{Context: ctx, left: 40}, r, root, Options{})
	assertBackupError(t, err, context.Canceled)

	// The host defaults to the system's.
	res, err := Run(ctx, r, filepath.Join(root, dirName, subName), Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap, _ := r.ReadSnapshot(ctx, res.Snapshot)
	if want, _ := os.Hostname(); snap.Host != want || snap.Time.IsZero() {
		t.Fatalf("default host %q time %v", snap.Host, snap.Time)
	}

	f := &faulty{Store: st, fail: failing("list", "/index/")(nil)}
	fr, err := repo.Open(ctx, f, prefix, passphrase(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer fr.Close()
	_, err = Run(ctx, fr, root, Options{})
	assertBackupError(t, err, ErrRepository, repo.ErrStorage)
	f.fail = failing("put", "/packs/")(nil)
	_, err = Run(ctx, fr, root, Options{})
	assertBackupError(t, err, ErrRepository, repo.ErrStorage)

	if os.Geteuid() != 0 {
		locked := filepath.Join(root, dirName, largeName)
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		_, err = Run(ctx, r, root, Options{})
		assertBackupError(t, err, ErrFilesystem, os.ErrPermission)
		_ = os.Chmod(locked, 0o600)
		dir := filepath.Join(root, dirName)
		if err := os.Chmod(dir, 0); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		_, err = Run(ctx, r, root, Options{})
		assertBackupError(t, err, ErrFilesystem, os.ErrPermission)
		_ = os.Chmod(dir, 0o750)
	}

	_ = r.Close()
	_, err = Run(ctx, r, root, Options{})
	assertBackupError(t, err, ErrRepository, repo.ErrKeys)
}
