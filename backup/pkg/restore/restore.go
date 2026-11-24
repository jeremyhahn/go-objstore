// Package restore rebuilds a snapshot's tree from a repository and verifies a
// repository without writing files. Repository data is treated as untrusted:
// every sealed object is opened and every chunk ID and file digest checked,
// and nothing is written outside the target directory.
package restore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// Options are the settings of one restore.
type Options struct {
	// Overwrite replaces existing files and symlinks at the manifest's paths.
	// Without it an existing entry there is refused. A directory is never
	// replaced, and an existing symlink is never passed through.
	Overwrite bool
}

// Stats counts what a restore wrote.
type Stats struct {
	// Files, Dirs and Symlinks count the entries restored.
	Files, Dirs, Symlinks int
	// Bytes is the total size of the files restored.
	Bytes int64
}

// Restore rebuilds snapshot id under the directory target, creating target
// if it is missing. Files get their recorded permission bits and
// modification times, and directories theirs once their entries are in
// place; symlinks keep the times the system gives them.
//
// Each pack the snapshot uses is fetched once. File content is written to
// temporary names in the target and moved to its final name only once every
// file has passed its size and digest checks, so a failed check leaves no
// file under a manifest name.
func Restore(ctx context.Context, r *repo.Repository, id seal.ID, target string, opts Options) (*Stats, error) {
	if err := ctx.Err(); err != nil {
		return nil, newError("start", "", "", ErrCanceled, err)
	}
	snap, err := r.ReadSnapshot(ctx, id)
	if err != nil {
		return nil, fromRepo("read snapshot", err)
	}
	root, err := openTarget(target)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	pl, err := newPlan(snap, func(name string) (fs.FileMode, bool) {
		info, err := root.Lstat(filepath.FromSlash(name))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return 0, false
		case err != nil:
			// An entry that cannot be examined is treated as a symlink.
			return fs.ModeSymlink, true
		}
		return info.Mode(), true
	})
	if err != nil {
		return nil, err
	}
	w := &writer{
		ctx:   ctx,
		r:     r,
		root:  root,
		opts:  opts,
		plan:  pl,
		snap:  r.Layout().Name(repo.KindSnapshot, id),
		temps: make([]string, len(pl.files)),
	}
	if err := w.run(); err != nil {
		if cerr := w.cleanup(); cerr != nil {
			return nil, errors.Join(err, cerr)
		}
		return nil, err
	}
	return &w.stats, nil
}

func openTarget(target string) (*os.Root, error) {
	const op = "open target"
	if err := os.MkdirAll(target, 0o700); err != nil {
		if info, serr := os.Stat(target); serr == nil && !info.IsDir() {
			return nil, newError(op, "", target, ErrInvalidTarget, err)
		}
		return nil, newError(op, "", target, ErrFilesystem, err)
	}
	root, err := os.OpenRoot(target)
	if err != nil {
		return nil, newError(op, "", target, ErrFilesystem, err)
	}
	return root, nil
}

// placement is where one chunk of a file goes.
type placement struct {
	file  int
	off   int64
	entry repo.Entry
}

// writer is one restore in progress.
type writer struct {
	ctx     context.Context
	r       *repo.Repository
	root    *os.Root
	opts    Options
	plan    *plan
	snap    string
	stats   Stats
	temps   []string
	created []string
}

func (w *writer) run() error {
	for _, n := range w.plan.dirs {
		if err := w.vacant(n); err != nil {
			return err
		}
	}
	for _, n := range w.plan.files {
		if err := w.vacant(n); err != nil {
			return err
		}
	}
	for _, n := range w.plan.links {
		if err := w.vacant(n); err != nil {
			return err
		}
	}
	ix, err := w.r.LoadIndex(w.ctx)
	if err != nil {
		return fromRepo("load index", err)
	}
	byPack, order, err := w.place(ix)
	if err != nil {
		return err
	}
	if err := w.makeDirs(); err != nil {
		return err
	}
	if err := w.makeTemps(); err != nil {
		return err
	}
	for _, id := range order {
		if err := w.fill(id, byPack[id]); err != nil {
			return err
		}
	}
	for i, n := range w.plan.files {
		if err := w.check(n, w.temps[i]); err != nil {
			return err
		}
	}
	return w.commit()
}

// vacant checks the entry already at n's path, if any.
func (w *writer) vacant(n *repo.Node) error {
	const op = "check target"
	info, err := w.root.Lstat(filepath.FromSlash(n.Path))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return newError(op, "", n.Path, ErrFilesystem, err)
	case n.Type == repo.NodeDir && info.IsDir():
		return nil
	case n.Type == repo.NodeDir && info.Mode()&fs.ModeSymlink != 0:
		return newError(op, "", n.Path, ErrUnsafeEntry, errThrough)
	case n.Type != repo.NodeDir && !info.IsDir() && w.opts.Overwrite:
		return nil
	}
	return newError(op, "", n.Path, ErrExists, nil)
}

// place locates every chunk of every file and groups the chunks by pack, in
// the order the packs are first needed.
func (w *writer) place(ix *repo.Index) (map[seal.ID][]placement, []seal.ID, error) {
	byPack := map[seal.ID][]placement{}
	var order []seal.ID
	for i, n := range w.plan.files {
		var off int64
		for _, c := range n.Chunks {
			e, ok := ix.Lookup(c)
			if !ok {
				err := newError("locate chunk", w.snap, n.Path, ErrUnindexed, nil)
				err.Chunk = c.String()
				return nil, nil, err
			}
			if _, seen := byPack[e.Pack]; !seen {
				order = append(order, e.Pack)
			}
			byPack[e.Pack] = append(byPack[e.Pack], placement{file: i, off: off, entry: e})
			off += plainSize(e)
		}
		if off != n.Size {
			return nil, nil, newError("locate chunk", w.snap, n.Path, ErrCorrupt, errSize)
		}
	}
	return byPack, order, nil
}

// plainSize is the size of an entry's chunk once opened. Chunks are sealed
// without compression, so it is fixed by the sealed length.
func plainSize(e repo.Entry) int64 {
	return int64(e.Length) - int64(seal.Overhead)
}

func (w *writer) makeDirs() error {
	const op = "make directory"
	for _, n := range w.plan.dirs {
		name := filepath.FromSlash(n.Path)
		err := w.root.Mkdir(name, 0o700)
		switch {
		case err == nil:
			w.created = append(w.created, name)
		case errors.Is(err, fs.ErrExist):
			if err := w.vacant(n); err != nil {
				return err
			}
		default:
			return newError(op, "", n.Path, ErrFilesystem, err)
		}
	}
	return nil
}

// tempName returns a fresh name in the directory of the slash path p.
func tempName(p string) string {
	return filepath.FromSlash(path.Join(path.Dir(p), ".restore-"+rand.Text()))
}

func (w *writer) makeTemps() error {
	for i, n := range w.plan.files {
		name := tempName(n.Path)
		f, err := w.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return newError("create file", "", n.Path, ErrFilesystem, err)
		}
		w.temps[i] = name
		if err := f.Close(); err != nil {
			return newError("create file", "", n.Path, ErrFilesystem, err)
		}
	}
	return nil
}

// fill fetches one pack and writes each of its chunks where the files need
// them.
func (w *writer) fill(id seal.ID, places []placement) error {
	if err := w.ctx.Err(); err != nil {
		return newError("read pack", "", "", ErrCanceled, err)
	}
	p, err := w.r.LoadPack(w.ctx, id)
	if err != nil {
		return fromRepo("read pack", err)
	}
	opened := map[seal.ID][]byte{}
	var f *os.File
	cur := -1
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	for _, pc := range places {
		data, ok := opened[pc.entry.Chunk]
		if !ok {
			if data, err = p.Chunk(pc.entry); err != nil {
				e := fromRepo("read chunk", err)
				e.Chunk = pc.entry.Chunk.String()
				return e
			}
			if int64(len(data)) != plainSize(pc.entry) {
				e := newError("read chunk", w.r.Layout().Name(repo.KindPack, id), "", ErrCorrupt, errLength)
				e.Chunk = pc.entry.Chunk.String()
				return e
			}
			opened[pc.entry.Chunk] = data
		}
		n := w.plan.files[pc.file]
		if pc.file != cur {
			if f != nil {
				if err := f.Close(); err != nil {
					f = nil
					return newError("write file", "", n.Path, ErrFilesystem, err)
				}
			}
			if f, err = w.root.OpenFile(w.temps[pc.file], os.O_WRONLY, 0); err != nil {
				return newError("write file", "", n.Path, ErrFilesystem, err)
			}
			cur = pc.file
		}
		if _, err := f.WriteAt(data, pc.off); err != nil {
			return newError("write file", "", n.Path, ErrFilesystem, err)
		}
	}
	if f == nil {
		return nil
	}
	err = f.Close()
	f = nil
	if err != nil {
		return newError("write file", "", w.plan.files[cur].Path, ErrFilesystem, err)
	}
	return nil
}

// check reads a restored file back and compares it with its manifest entry.
func (w *writer) check(n *repo.Node, temp string) error {
	const op = "check file"
	f, err := w.root.Open(temp)
	if err != nil {
		return newError(op, "", n.Path, ErrFilesystem, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return newError(op, "", n.Path, ErrFilesystem, err)
	}
	if size != n.Size {
		return newError(op, w.snap, n.Path, ErrCorrupt, errSize)
	}
	if hex.EncodeToString(h.Sum(nil)) != n.SHA256 {
		return newError(op, w.snap, n.Path, ErrCorrupt, errDigest)
	}
	return nil
}

// commit moves every checked file to its final name, makes the symlinks, and
// sets the directories' modes and times, children before their parents.
func (w *writer) commit() error {
	for i, n := range w.plan.files {
		if err := w.vacant(n); err != nil {
			return err
		}
		name := filepath.FromSlash(n.Path)
		if err := w.root.Rename(w.temps[i], name); err != nil {
			return newError("move file", "", n.Path, ErrFilesystem, err)
		}
		w.temps[i] = ""
		if err := w.setAttrs(n); err != nil {
			return err
		}
		w.stats.Files++
		w.stats.Bytes += n.Size
	}
	for _, n := range w.plan.links {
		if err := w.link(n); err != nil {
			return err
		}
		w.stats.Symlinks++
	}
	dirs := slices.Clone(w.plan.dirs)
	slices.Reverse(dirs)
	if w.plan.root != nil {
		dirs = append(dirs, w.plan.root)
	}
	for _, n := range dirs {
		if err := w.setAttrs(n); err != nil {
			return err
		}
		w.stats.Dirs++
	}
	return nil
}

// link makes a symlink under a temporary name and moves it into place.
func (w *writer) link(n *repo.Node) error {
	const op = "make symlink"
	if err := w.vacant(n); err != nil {
		return err
	}
	temp := tempName(n.Path)
	if err := w.root.Symlink(filepath.FromSlash(n.Target), temp); err != nil {
		return newError(op, "", n.Path, ErrFilesystem, err)
	}
	if err := w.root.Rename(temp, filepath.FromSlash(n.Path)); err != nil {
		return newError(op, "", n.Path, ErrFilesystem, errors.Join(err, w.root.Remove(temp)))
	}
	return nil
}

// setAttrs applies a node's permission bits and modification time. Other
// mode bits, such as setuid, are not restored.
func (w *writer) setAttrs(n *repo.Node) error {
	const op = "set attributes"
	name := filepath.FromSlash(n.Path)
	if err := w.root.Chmod(name, n.Mode.Perm()); err != nil {
		return newError(op, "", n.Path, ErrFilesystem, err)
	}
	if err := w.root.Chtimes(name, n.ModTime, n.ModTime); err != nil {
		return newError(op, "", n.Path, ErrFilesystem, err)
	}
	return nil
}

// cleanup removes the temporary files left by a failed restore and the
// directories it made, and returns the first removal that failed.
func (w *writer) cleanup() error {
	var first error
	remove := func(name string) {
		if err := w.root.Remove(name); err != nil && first == nil {
			first = newError("clean up", "", "", ErrFilesystem, err)
		}
	}
	for _, t := range w.temps {
		if t != "" {
			remove(t)
		}
	}
	for i := len(w.created) - 1; i >= 0; i-- {
		remove(w.created[i])
	}
	return first
}
