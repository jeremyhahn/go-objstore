package repo

import (
	"strings"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// Kind is a kind of repository object other than the config.
type Kind uint8

const (
	// KindKey is a wrapped data key.
	KindKey Kind = iota + 1
	// KindPack is a pack of sealed chunks.
	KindPack
	// KindIndex is an index object.
	KindIndex
	// KindSnapshot is a snapshot manifest.
	KindSnapshot
)

// dir returns the kind's directory under the repository prefix.
func (k Kind) dir() string {
	switch k {
	case KindKey:
		return "keys/"
	case KindPack:
		return "packs/"
	case KindIndex:
		return "index/"
	case KindSnapshot:
		return "snapshots/"
	default:
		return "unknown/"
	}
}

// Layout names a repository's objects under its prefix:
//
//	<prefix>config                  the sealed repository config
//	<prefix>keys/<id>               a wrapped data key; id is SHA-256 of it
//	<prefix>packs/<id[:2]>/<id>     a pack
//	<prefix>index/<id>              an index object
//	<prefix>snapshots/<id>          a snapshot manifest
//
// Every id is 64 lowercase hex digits. A pack, index or snapshot id is the
// keyed object ID of its content (seal.Keys.ObjectID), so names reveal
// nothing about the files backed up, not even when two repositories hold
// the same files.
type Layout struct {
	prefix string
}

// NewLayout returns the layout under prefix. An empty prefix is the store's
// root; otherwise the prefix is a directory and gains a trailing slash.
func NewLayout(prefix string) Layout {
	prefix = strings.TrimRight(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return Layout{prefix: prefix}
}

// Prefix returns the repository prefix, empty or ending in a slash.
func (l Layout) Prefix() string {
	return l.prefix
}

// Config returns the config's name.
func (l Layout) Config() string {
	return l.prefix + "config"
}

// Dir returns the prefix every object of kind k is named under.
func (l Layout) Dir(k Kind) string {
	return l.prefix + k.dir()
}

// Name returns the name of the object of kind k with ID id.
func (l Layout) Name(k Kind, id seal.ID) string {
	s := id.String()
	if k == KindPack {
		return l.Dir(k) + s[:2] + "/" + s
	}
	return l.Dir(k) + s
}

// Parse returns the ID in name, an object of kind k, or fails with
// ErrMalformedName.
func (l Layout) Parse(k Kind, name string) (seal.ID, error) {
	rest, ok := strings.CutPrefix(name, l.Dir(k))
	if !ok {
		return seal.ID{}, newError("parse name", name, ErrMalformedName, nil)
	}
	if k == KindPack {
		fan, tail, ok := strings.Cut(rest, "/")
		if !ok || len(fan) != 2 || !strings.HasPrefix(tail, fan) {
			return seal.ID{}, newError("parse name", name, ErrMalformedName, nil)
		}
		rest = tail
	}
	id, err := seal.ParseID(rest)
	if err != nil {
		return seal.ID{}, newError("parse name", name, ErrMalformedName, err)
	}
	return id, nil
}
