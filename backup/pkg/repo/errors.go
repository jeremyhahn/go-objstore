package repo

import (
	"errors"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

var (
	// ErrRepositoryExists means Init found a repository already there.
	ErrRepositoryExists = errors.New("repo: a repository already exists")

	// ErrNoRepository means Open found no repository config.
	ErrNoRepository = errors.New("repo: no repository")

	// ErrNoKey means no key object opened the repository with the provider.
	// When a key failed to unwrap, the error also matches the key seam's own
	// typed error, such as keyseam.ErrUnwrap for a wrong passphrase.
	ErrNoKey = errors.New("repo: no key opens the repository")

	// ErrCorrupt means an object authenticated but its content is not valid,
	// or an object is not where the repository says it is.
	ErrCorrupt = errors.New("repo: corrupt repository object")

	// ErrMalformedName means an object name does not follow the layout.
	ErrMalformedName = errors.New("repo: malformed object name")

	// ErrInvalidOptions means Init was given an option out of bounds.
	ErrInvalidOptions = errors.New("repo: invalid options")

	// ErrStorage means the store failed; the cause is the store's error.
	ErrStorage = errors.New("repo: storage failure")

	// ErrKeys means a key operation failed, for example after Close; the
	// cause is the seal or key seam error.
	ErrKeys = errors.New("repo: key failure")
	// ErrKeyRemoved means a key object unwraps the data key but its slot
	// was removed from the repository's key list.
	ErrKeyRemoved = errors.New("repo: key slot removed")
	// ErrUnknownKey means no key slot has the given ID.
	ErrUnknownKey = errors.New("repo: no such key slot")
	// ErrLastKey means a change would leave the repository with no key slot.
	ErrLastKey = errors.New("repo: cannot remove the last key slot")
	// ErrKeyConflict means another client changed the key slots at the same
	// time; open the repository again and retry.
	ErrKeyConflict = errors.New("repo: concurrent key change")
)

// Error is a failed repository operation. It matches its sentinel and its
// cause, which is often a store, seal or key seam error, with errors.Is and
// errors.As.
type Error struct {
	// Op is the operation, such as "open" or "read pack".
	Op string
	// Name is the object involved, or empty.
	Name string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error describes the operation, the object and the failure.
func (e *Error) Error() string {
	s := "repo " + e.Op
	if e.Name != "" {
		s += " " + e.Name
	}
	s += ": " + e.Err.Error()
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

// fromStore wraps a store failure. ErrNotFound for an object the repository
// refers to is corruption: the object is missing.
func fromStore(op, name string, err error) *Error {
	if errors.Is(err, store.ErrNotFound) {
		return newError(op, name, ErrCorrupt, err)
	}
	return newError(op, name, ErrStorage, err)
}

// fromSeal wraps a seal failure. An object that fails to authenticate or is
// malformed is corrupt; anything else is a key failure.
func fromSeal(op, name string, err error) *Error {
	if errors.Is(err, seal.ErrOpen) || errors.Is(err, seal.ErrMalformed) ||
		errors.Is(err, seal.ErrUnsupportedCompression) {
		return newError(op, name, ErrCorrupt, err)
	}
	return newError(op, name, ErrKeys, err)
}
