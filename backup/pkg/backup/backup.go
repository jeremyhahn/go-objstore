// Package backup is the backup write path: it walks a directory tree,
// splits each file into chunks, stores the chunks the repository does not
// already hold in packs, and records the run in an index object and a
// snapshot manifest.
//
// Regular files, directories and symlinks are backed up; symlinks are
// recorded, not followed. Anything else, such as sockets, pipes and devices,
// is skipped and counted. The manifest is written last, so a run that fails
// part way leaves packs and perhaps an index object, but no snapshot; the
// next run reuses what it left.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/chunker"
	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// Options adjust a run.
type Options struct {
	// Host is recorded in the manifest. Empty means the system's host name.
	Host string
	// Now returns the run's time. Nil means time.Now.
	Now func() time.Time
}

// Stats count what a run did.
type Stats struct {
	// Files, Dirs and Symlinks count the nodes backed up.
	Files, Dirs, Symlinks int
	// Skipped counts the entries not backed up because of their type.
	Skipped int
	// Bytes is the total size of the files backed up.
	Bytes int64
	// ChunksNew counts the chunks stored by this run, and BytesNew their
	// size before sealing.
	ChunksNew int
	BytesNew  int64
	// ChunksDeduped counts the chunks already stored, by an earlier run or
	// earlier in this one.
	ChunksDeduped int
	// PacksWritten counts the packs this run wrote, and PacksReused the
	// packs it found already stored by an interrupted run.
	PacksWritten, PacksReused int
}

// Result is a finished run.
type Result struct {
	// Snapshot is the manifest's ID.
	Snapshot seal.ID
	// Index is the ID of the index object the run wrote. It is the zero ID
	// when the run stored no new chunks and wrote no index object.
	Index seal.ID
	// Stats count what the run did.
	Stats Stats
}

// run is the state of one backup run.
type run struct {
	// condemned holds the packs a prune run marked: the run neither reuses
	// nor rewrites them.
	condemned map[seal.ID]bool
	r         *repo.Repository
	index     *repo.Index
	packer    *repo.Packer
	snap      repo.Snapshot
	stats     Stats
}

// Run backs up the tree under root into r and returns the manifest's ID.
// It fails with ErrInvalidRoot if root is not a directory.
func Run(ctx context.Context, r *repo.Repository, root string, opts Options) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, newError("start", root, ErrCanceled, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, newError("resolve root", root, ErrFilesystem, err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, newError("resolve root", abs, ErrFilesystem, err)
	}
	if !info.IsDir() {
		return nil, newError("resolve root", abs, ErrInvalidRoot, nil)
	}
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	host := opts.Host
	if host == "" {
		// An unknown host is recorded as empty.
		if h, err := os.Hostname(); err == nil {
			host = h
		}
	}
	condemned, err := r.Condemned(ctx)
	if err != nil {
		return nil, newError("load marks", "", ErrRepository, err)
	}
	index, err := r.LoadIndex(ctx)
	if err != nil {
		return nil, newError("load index", "", ErrRepository, err)
	}
	b := &run{
		r:         r,
		index:     index,
		packer:    r.NewPacker(),
		condemned: condemned,
		snap:      repo.Snapshot{Time: now().UTC(), Host: host, Root: abs},
	}
	b.packer.Avoid(condemned)
	if err := filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return newError("walk", path, ErrFilesystem, err)
		}
		if err := ctx.Err(); err != nil {
			return newError("walk", path, ErrCanceled, err)
		}
		return b.visit(ctx, abs, path, d)
	}); err != nil {
		return nil, err
	}
	return b.finish(ctx)
}

// visit backs up one entry of the tree.
func (b *run) visit(ctx context.Context, root, path string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return newError("stat", path, ErrFilesystem, err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return newError("stat", path, ErrFilesystem, err)
	}
	node := repo.Node{
		Path:    filepath.ToSlash(rel),
		Mode:    info.Mode(),
		ModTime: info.ModTime().UTC(),
	}
	switch {
	case info.Mode().IsDir():
		node.Type = repo.NodeDir
		b.stats.Dirs++
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return newError("read link", path, ErrFilesystem, err)
		}
		node.Type = repo.NodeSymlink
		node.Target = target
		b.stats.Symlinks++
	case info.Mode().IsRegular():
		node.Type = repo.NodeFile
		if err := b.file(ctx, path, &node); err != nil {
			return err
		}
		b.stats.Files++
		b.stats.Bytes += node.Size
	default:
		b.stats.Skipped++
		return nil
	}
	b.snap.Nodes = append(b.snap.Nodes, node)
	return nil
}

// file chunks the file at path, stores its new chunks, and fills in the
// node's size, digest and chunk list.
func (b *run) file(ctx context.Context, path string, node *repo.Node) (err error) {
	f, err := os.Open(path) //nolint:gosec // backing up the caller's tree is this package's purpose
	if err != nil {
		return newError("read file", path, ErrFilesystem, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = newError("read file", path, ErrFilesystem, cerr)
		}
	}()
	c, err := chunker.New(f, b.r.Config().Chunker)
	if err != nil {
		return newError("read file", path, ErrRepository, err)
	}
	digest := sha256.New()
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return newError("read file", path, ErrFilesystem, err)
		}
		if err := ctx.Err(); err != nil {
			return newError("read file", path, ErrCanceled, err)
		}
		digest.Write(ch.Data)
		node.Size += int64(len(ch.Data))
		id, err := b.r.ChunkID(ch.Data)
		if err != nil {
			return newError("read file", path, ErrRepository, err)
		}
		node.Chunks = append(node.Chunks, id)
		if e, ok := b.index.Lookup(id); (ok && !b.condemned[e.Pack]) || b.packer.Has(id) {
			b.stats.ChunksDeduped++
			continue
		}
		if err := b.packer.Add(ctx, id, ch.Data); err != nil {
			return newError("store chunk", path, ErrRepository, err)
		}
		b.stats.ChunksNew++
		b.stats.BytesNew += int64(len(ch.Data))
	}
	node.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return nil
}

// finish writes the last pack, the index object and, last, the manifest.
func (b *run) finish(ctx context.Context) (*Result, error) {
	if err := b.packer.Flush(ctx); err != nil {
		return nil, newError("write pack", "", ErrRepository, err)
	}
	res := &Result{}
	b.stats.PacksWritten, b.stats.PacksReused = b.packer.Written()
	if entries := b.packer.Entries(); len(entries) > 0 {
		id, err := b.r.WriteIndex(ctx, entries)
		if err != nil {
			return nil, newError("write index", "", ErrRepository, err)
		}
		res.Index = id
	}
	b.snap.Skipped = b.stats.Skipped
	id, err := b.r.WriteSnapshot(ctx, &b.snap)
	if err != nil {
		return nil, newError("write snapshot", "", ErrRepository, err)
	}
	res.Snapshot = id
	res.Stats = b.stats
	return res, nil
}
