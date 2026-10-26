package repo

import (
	"bytes"
	"context"
	"errors"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// kindType maps an object kind to the seal type, and so the subkey, that
// seals it.
func kindType(k Kind) seal.Type {
	switch k {
	case KindIndex:
		return seal.TypeIndex
	case KindMark:
		return seal.TypeMark
	default:
		return seal.TypeSnapshot
	}
}

// putSealed seals plain as an object of kind k, index or snapshot, under its
// content-derived ID and writes it once. An object already there under that
// ID is accepted if it opens to the same plaintext, as when a run repeats
// one that was interrupted.
func (r *Repository) putSealed(ctx context.Context, op string, k Kind, plain []byte) (seal.ID, error) {
	t := kindType(k)
	id, err := r.keys.ObjectID(t, plain)
	if err != nil {
		return seal.ID{}, fromSeal(op, "", err)
	}
	sealed, err := r.keys.Seal(t, id[:], seal.CompressionNone, plain)
	if err != nil {
		return seal.ID{}, fromSeal(op, "", err)
	}
	name := r.layout.Name(k, id)
	err = r.st.Put(ctx, name, sealed)
	if errors.Is(err, store.ErrExists) {
		existing, gerr := r.getSealed(ctx, op, k, id)
		if gerr != nil {
			return seal.ID{}, gerr
		}
		if !bytes.Equal(existing, plain) {
			return seal.ID{}, newError(op, name, ErrCorrupt, nil)
		}
		return id, nil
	}
	if err != nil {
		return seal.ID{}, fromStore(op, name, err)
	}
	return id, nil
}

// getSealed reads and opens the object of kind k with ID id. An object that
// is missing, tampered with, or stored under another object's name fails
// with ErrCorrupt.
func (r *Repository) getSealed(ctx context.Context, op string, k Kind, id seal.ID) ([]byte, error) {
	name := r.layout.Name(k, id)
	sealed, err := r.st.Get(ctx, name)
	if err != nil {
		return nil, fromStore(op, name, err)
	}
	plain, err := r.keys.Open(kindType(k), id[:], sealed)
	if err != nil {
		return nil, fromSeal(op, name, err)
	}
	return plain, nil
}

// listIDs returns the IDs of every object of kind k, in name order. A name
// under the kind's directory that is not a valid object name fails with
// ErrMalformedName.
func (r *Repository) listIDs(ctx context.Context, op string, k Kind) ([]seal.ID, error) {
	names, err := r.st.List(ctx, r.layout.Dir(k))
	if err != nil {
		return nil, fromStore(op, r.layout.Dir(k), err)
	}
	ids := make([]seal.ID, 0, len(names))
	for _, name := range names {
		id, err := r.layout.Parse(k, name)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
