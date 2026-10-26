package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// packCacheSize is how many opened packs Verify keeps while it checks files.
const packCacheSize = 4

// VerifyOptions select what Verify checks.
type VerifyOptions struct {
	// Snapshots are the manifests to check; nil checks every manifest.
	Snapshots []seal.ID
}

// Report is what Verify checked and found.
type Report struct {
	// Snapshots, Indexes and Packs count the objects that opened.
	Snapshots, Indexes, Packs int
	// Chunks counts the chunks the packs hold.
	Chunks int
	// Files and Bytes count the manifests' files and their content.
	Files int
	Bytes int64
	// Problems lists every fault found, in the order found: a corrupt
	// object, a missing pack, a chunk no index maps, a pack no index names.
	Problems []*Error
}

// Verify checks a repository without writing files. It opens every index,
// every pack the indexes name and every chunk in them, checking each chunk's
// ID; then it opens the selected manifests and checks every file's chunks
// and digest. Faults in the data are collected in the report, and Verify
// then returns an error joining them; a failure to reach the repository, or
// a canceled context, stops it at once.
func Verify(ctx context.Context, r *repo.Repository, opts VerifyOptions) (*Report, error) {
	v := &verifier{
		ctx:    ctx,
		r:      r,
		rep:    &Report{},
		chunks: map[seal.ID]repo.Entry{},
		cache:  map[seal.ID]*repo.Pack{},

		badPack:  map[seal.ID]bool{},
		badChunk: map[seal.ID]bool{},
	}
	if err := v.run(opts); err != nil {
		return v.rep, err
	}
	if len(v.rep.Problems) > 0 {
		errs := make([]error, len(v.rep.Problems))
		for i, p := range v.rep.Problems {
			errs[i] = p
		}
		return v.rep, errors.Join(errs...)
	}
	return v.rep, nil
}

// indexed is an index entry and the index that holds it.
type indexed struct {
	entry repo.Entry
	index seal.ID
}

type verifier struct {
	ctx    context.Context
	r      *repo.Repository
	rep    *Report
	chunks map[seal.ID]repo.Entry
	cache  map[seal.ID]*repo.Pack
	order  []seal.ID
	// badPack and badChunk hold what has already been reported.
	badPack  map[seal.ID]bool
	badChunk map[seal.ID]bool
	// unread is set when an index could not be read, so a pack it names
	// cannot be told from an orphan.
	unread bool
}

// orphans reports every stored pack that no index names. A pack written by a
// backup that has not yet stored its index is reported too.
func (v *verifier) orphans(byPack map[seal.ID][]indexed) error {
	if v.unread {
		return nil
	}
	ids, err := v.r.ListPacks(v.ctx)
	if err != nil {
		return v.stop(fromRepo("list packs", err))
	}
	for _, id := range ids {
		if _, ok := byPack[id]; !ok {
			v.rep.Problems = append(v.rep.Problems, newError("verify pack", v.r.Layout().Name(repo.KindPack, id), "", ErrOrphan, nil))
		}
	}
	return nil
}

// note records a fault in the data, or returns a failure that stops Verify.
func (v *verifier) note(e *Error) error {
	if errors.Is(e.Err, ErrRepository) || errors.Is(e.Err, ErrCanceled) {
		return e
	}
	e.Path = ""
	v.rep.Problems = append(v.rep.Problems, e)
	return nil
}

func (v *verifier) run(opts VerifyOptions) error {
	if err := v.ctx.Err(); err != nil {
		return newError("start", "", "", ErrCanceled, err)
	}
	byPack, err := v.indexes()
	if err != nil {
		return err
	}
	packs := make([]seal.ID, 0, len(byPack))
	for id := range byPack {
		packs = append(packs, id)
	}
	slices.SortFunc(packs, func(a, b seal.ID) int { return bytes.Compare(a[:], b[:]) })
	if err := v.orphans(byPack); err != nil {
		return err
	}
	for _, id := range packs {
		if err := v.pack(id, byPack[id]); err != nil {
			return err
		}
	}
	ids := opts.Snapshots
	if ids == nil {
		if ids, err = v.r.ListSnapshots(v.ctx); err != nil {
			return v.stop(fromRepo("list snapshots", err))
		}
	}
	for _, id := range ids {
		if err := v.snapshot(id); err != nil {
			return err
		}
	}
	return nil
}

// stop records a fault that ends Verify early and returns it.
func (v *verifier) stop(e *Error) error {
	if err := v.note(e); err != nil {
		return err
	}
	return e
}

func (v *verifier) indexes() (map[seal.ID][]indexed, error) {
	ids, err := v.r.ListIndexes(v.ctx)
	if err != nil {
		return nil, v.stop(fromRepo("list indexes", err))
	}
	byPack := map[seal.ID][]indexed{}
	for _, id := range ids {
		entries, err := v.r.ReadIndex(v.ctx, id)
		if err != nil {
			if err := v.note(fromRepo("verify index", err)); err != nil {
				return nil, err
			}
			v.unread = true
			continue
		}
		v.rep.Indexes++
		for _, e := range entries {
			if _, dup := v.chunks[e.Chunk]; !dup {
				v.chunks[e.Chunk] = e
			}
			byPack[e.Pack] = append(byPack[e.Pack], indexed{entry: e, index: id})
		}
	}
	return byPack, nil
}

// pack opens one pack and every chunk in it, and checks that the indexes
// agree with its trailer.
func (v *verifier) pack(id seal.ID, refs []indexed) error {
	if err := v.ctx.Err(); err != nil {
		return newError("verify pack", "", "", ErrCanceled, err)
	}
	p, err := v.r.LoadPack(v.ctx, id)
	if err != nil {
		v.badPack[id] = true
		return v.note(fromRepo("verify pack", err))
	}
	v.rep.Packs++
	held := make(map[seal.ID]repo.Entry, len(p.Entries))
	for _, e := range p.Entries {
		held[e.Chunk] = e
	}
	for _, ref := range refs {
		if held[ref.entry.Chunk] != ref.entry {
			e := newError("verify index", v.r.Layout().Name(repo.KindIndex, ref.index), "", ErrCorrupt, errMismatch)
			e.Chunk = ref.entry.Chunk.String()
			if err := v.note(e); err != nil {
				return err
			}
		}
	}
	for _, e := range p.Entries {
		v.rep.Chunks++
		if _, err := p.Chunk(e); err != nil {
			v.badChunk[e.Chunk] = true
			fe := fromRepo("verify chunk", err)
			fe.Chunk = e.Chunk.String()
			if err := v.note(fe); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *verifier) snapshot(id seal.ID) error {
	if err := v.ctx.Err(); err != nil {
		return newError("verify snapshot", "", "", ErrCanceled, err)
	}
	s, err := v.r.ReadSnapshot(v.ctx, id)
	if err != nil {
		return v.note(fromRepo("verify snapshot", err))
	}
	v.rep.Snapshots++
	name := v.r.Layout().Name(repo.KindSnapshot, id)
	if _, err := newPlan(s, nil); err != nil {
		var e *Error
		errors.As(err, &e)
		e.Op, e.Object = "verify snapshot", name
		if err := v.note(e); err != nil {
			return err
		}
	}
	for i := range s.Nodes {
		if n := &s.Nodes[i]; n.Type == repo.NodeFile {
			if err := v.file(name, n); err != nil {
				return err
			}
		}
	}
	return nil
}

// file checks one file's chunks and, when every chunk is readable, its size
// and digest.
func (v *verifier) file(snap string, n *repo.Node) error {
	const op = "verify file"
	v.rep.Files++
	h := sha256.New()
	var size int64
	complete := true
	for _, c := range n.Chunks {
		e, ok := v.chunks[c]
		if !ok {
			ue := newError(op, snap, "", ErrUnindexed, nil)
			ue.Chunk = c.String()
			if err := v.note(ue); err != nil {
				return err
			}
			complete = false
			continue
		}
		if v.badPack[e.Pack] || v.badChunk[c] {
			// Already reported against the pack or the chunk.
			complete = false
			continue
		}
		data, err := v.chunk(e)
		if err != nil {
			fe := fromRepo(op, err)
			fe.Chunk = c.String()
			if err := v.note(fe); err != nil {
				return err
			}
			complete = false
			continue
		}
		h.Write(data)
		size += int64(len(data))
	}
	v.rep.Bytes += size
	if !complete {
		return nil
	}
	switch {
	case size != n.Size:
		return v.note(newError(op, snap, "", ErrCorrupt, errSize))
	case hex.EncodeToString(h.Sum(nil)) != n.SHA256:
		return v.note(newError(op, snap, "", ErrCorrupt, errDigest))
	}
	return nil
}

// chunk opens one chunk, keeping the last few packs it opened.
func (v *verifier) chunk(e repo.Entry) ([]byte, error) {
	p, ok := v.cache[e.Pack]
	if !ok {
		var err error
		if p, err = v.r.LoadPack(v.ctx, e.Pack); err != nil {
			return nil, err
		}
		if len(v.order) == packCacheSize {
			delete(v.cache, v.order[0])
			v.order = v.order[1:]
		}
		v.cache[e.Pack] = p
		v.order = append(v.order, e.Pack)
	}
	return p.Chunk(e)
}
