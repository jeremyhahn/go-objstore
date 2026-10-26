package repo

import (
	"context"
	"encoding/json"
	"io/fs"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
)

// snapshotVersion is the manifest format version.
const snapshotVersion = 1

// NodeType is the type of a node in a snapshot's tree.
type NodeType string

// Node types.
const (
	NodeFile    NodeType = "file"
	NodeDir     NodeType = "dir"
	NodeSymlink NodeType = "symlink"
)

// Node is one entry in a snapshot's tree.
type Node struct {
	// Path is the node's path relative to the snapshot root, with forward
	// slashes; the root itself is ".".
	Path string `json:"path"`
	// Type is the node's type.
	Type NodeType `json:"type"`
	// Mode is the node's permission and mode bits.
	Mode fs.FileMode `json:"mode"`
	// Size is a file's size in bytes.
	Size int64 `json:"size,omitempty"`
	// ModTime is the node's modification time.
	ModTime time.Time `json:"mtime"`
	// Target is a symlink's target.
	Target string `json:"target,omitempty"`
	// SHA256 is the hex SHA-256 digest of a file's content.
	SHA256 string `json:"sha256,omitempty"`
	// Chunks are a file's chunk IDs in order.
	Chunks []seal.ID `json:"chunks,omitempty"`
}

// Snapshot is the manifest of one backup run. It is sealed with the snapshot
// subkey, so its time, host and paths are stored only in ciphertext.
type Snapshot struct {
	// Version is the manifest format version; WriteSnapshot sets it.
	Version int `json:"version"`
	// Time is when the run started.
	Time time.Time `json:"time"`
	// Host is the host the run backed up.
	Host string `json:"host"`
	// Root is the directory the run backed up.
	Root string `json:"root"`
	// Nodes is the tree, parents before their children.
	Nodes []Node `json:"nodes"`
	// Skipped counts the entries the run did not back up, such as sockets
	// and devices.
	Skipped int `json:"skipped"`
}

// WriteSnapshot writes the manifest and returns its ID.
func (r *Repository) WriteSnapshot(ctx context.Context, s *Snapshot) (seal.ID, error) {
	const op = "write snapshot"
	s.Version = snapshotVersion
	plain, err := json.Marshal(s)
	if err != nil {
		return seal.ID{}, newError(op, "", ErrCorrupt, err)
	}
	return r.putSealed(ctx, op, KindSnapshot, plain)
}

// ReadSnapshot reads and opens the manifest with ID id.
func (r *Repository) ReadSnapshot(ctx context.Context, id seal.ID) (*Snapshot, error) {
	const op = "read snapshot"
	plain, err := r.getSealed(ctx, op, KindSnapshot, id)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err := json.Unmarshal(plain, &s); err != nil {
		return nil, newError(op, r.layout.Name(KindSnapshot, id), ErrCorrupt, err)
	}
	if s.Version != snapshotVersion {
		return nil, newError(op, r.layout.Name(KindSnapshot, id), ErrCorrupt, nil)
	}
	return &s, nil
}

// ListSnapshots returns the IDs of every manifest, in name order.
func (r *Repository) ListSnapshots(ctx context.Context) ([]seal.ID, error) {
	return r.listIDs(ctx, "list snapshots", KindSnapshot)
}
