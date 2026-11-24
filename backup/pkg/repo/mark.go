package repo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

const markVersion = 1

// Marked is a pack a prune run found unreferenced, with the time a run first
// found it so.
type Marked struct {
	Pack  seal.ID   `json:"pack"`
	Since time.Time `json:"since"`
}

// Mark records the packs a prune run found unreferenced; backups never reuse or write a marked pack.
type Mark struct {
	Version int       `json:"version"`
	Time    time.Time `json:"time"`
	Packs   []Marked  `json:"packs"`
}

// WriteMark stores m and returns its ID.
func (r *Repository) WriteMark(ctx context.Context, m *Mark) (seal.ID, error) {
	const op = "write mark"
	m.Version = markVersion
	plain, err := json.Marshal(m)
	if err != nil {
		return seal.ID{}, newError(op, "", ErrCorrupt, err)
	}
	return r.putSealed(ctx, op, KindMark, plain)
}

// ReadMark loads and opens the mark id.
func (r *Repository) ReadMark(ctx context.Context, id seal.ID) (*Mark, error) {
	const op = "read mark"
	plain, err := r.getSealed(ctx, op, KindMark, id)
	if err != nil {
		return nil, err
	}
	var m Mark
	if err := json.Unmarshal(plain, &m); err != nil {
		return nil, newError(op, r.layout.Name(KindMark, id), ErrCorrupt, err)
	}
	if m.Version != markVersion {
		return nil, newError(op, r.layout.Name(KindMark, id), ErrCorrupt, nil)
	}
	return &m, nil
}

// ListMarks returns the IDs of every mark, in name order.
func (r *Repository) ListMarks(ctx context.Context) ([]seal.ID, error) {
	return r.listIDs(ctx, "list marks", KindMark)
}

// Condemned returns every pack that any mark names.
func (r *Repository) Condemned(ctx context.Context) (map[seal.ID]bool, error) {
	ids, err := r.ListMarks(ctx)
	if err != nil {
		return nil, err
	}
	packs := make(map[seal.ID]bool)
	for _, id := range ids {
		m, err := r.ReadMark(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, p := range m.Packs {
			packs[p.Pack] = true
		}
	}
	return packs, nil
}

// ListPacks returns the IDs of every pack, in name order.
func (r *Repository) ListPacks(ctx context.Context) ([]seal.ID, error) {
	return r.listIDs(ctx, "list packs", KindPack)
}
