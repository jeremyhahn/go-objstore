// Package store is the backup repository's storage seam: the few operations a
// repository needs on the immutable objects it holds.
//
// Objects are put once. The backup identity never deletes or overwrites, so a
// repository can live in a WORM bucket: a Put of a name that already exists
// fails with ErrExists and leaves the existing object as it was. Removing
// objects is a separate capability, PruneStore, that only prune holds.
package store

import (
	"context"
	"errors"
)

// Store holds a repository's objects. Implementations are safe for
// concurrent use.
type Store interface {
	// Put writes data under name, once. It fails with ErrExists if name
	// already exists.
	Put(ctx context.Context, name string, data []byte) error
	// Get returns the object's bytes, or fails with ErrNotFound.
	Get(ctx context.Context, name string) ([]byte, error)
	// List returns the names that begin with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// Exists reports whether name exists.
	Exists(ctx context.Context, name string) (bool, error)
}

// PruneStore is a Store that can also remove objects. It is the privileged
// capability prune runs with; the backup and restore paths take a Store, and
// a repository never exposes removal from the store it is opened on.
type PruneStore interface {
	Store
	// Remove deletes name. Removing a name that does not exist fails with
	// ErrNotFound, or succeeds on a backend that cannot tell.
	Remove(ctx context.Context, name string) error
}

var (
	// ErrExists means a Put named an object that already exists.
	ErrExists = errors.New("store: object already exists")

	// ErrNotFound means a Get named an object that does not exist.
	ErrNotFound = errors.New("store: object not found")

	// ErrInvalidName means an object name is empty.
	ErrInvalidName = errors.New("store: invalid object name")

	// ErrBackend means the backing store failed; Cause says how.
	ErrBackend = errors.New("store: backend failure")
)

// Error is a failed store operation. It matches its sentinel and its cause
// with errors.Is.
type Error struct {
	// Op is the operation: "put", "get", "list", "exists" or "remove".
	Op string
	// Name is the object name or list prefix.
	Name string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error describes the operation, the name and the failure.
func (e *Error) Error() string {
	s := "store " + e.Op + " " + e.Name + ": " + e.Err.Error()
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap returns the sentinel and the cause.
func (e *Error) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.Cause}
}

func newError(op, name string, sentinel, cause error) *Error {
	return &Error{Op: op, Name: name, Err: sentinel, Cause: cause}
}
