package prune

import (
	"context"
	"errors"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
)

// Sentinels a prune failure matches with errors.Is. None carries key
// material, plaintext or a credential.
var (
	// ErrInvalidOptions reports a policy or option out of range.
	ErrInvalidOptions = errors.New("prune: invalid options")
	// ErrKeepsNothing reports a policy that would keep no snapshot. Prune
	// removes nothing then.
	ErrKeepsNothing = errors.New("prune: policy keeps no snapshot")
	// ErrCorrupt reports a repository object that does not open or decode.
	// Prune removes nothing it has not already removed.
	ErrCorrupt = errors.New("prune: repository data is corrupt")
	// ErrUnindexed reports a chunk a kept snapshot names that no index maps.
	ErrUnindexed = errors.New("prune: chunk is in no index")
	// ErrRepository reports a storage or key failure.
	ErrRepository = errors.New("prune: repository failure")
	// ErrCanceled reports a canceled or expired context.
	ErrCanceled = errors.New("prune: canceled")
)

// Error describes a failed prune step.
type Error struct {
	// Op is the step that failed, such as "read snapshot" or "remove pack".
	Op string
	// Object is the repository object involved, or empty.
	Object string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	s := "prune " + e.Op
	if e.Object != "" {
		s += " " + e.Object
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

func newError(op, object string, sentinel, cause error) *Error {
	return &Error{Op: op, Object: object, Err: sentinel, Cause: cause}
}

// fromRepo maps a repository or store failure to a prune Error.
func fromRepo(op string, err error) *Error {
	e := newError(op, "", ErrRepository, err)
	var re *repo.Error
	if errors.As(err, &re) {
		e.Object = re.Name
	}
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		e.Err = ErrCanceled
	case errors.Is(err, repo.ErrCorrupt) || errors.Is(err, repo.ErrMalformedName):
		e.Err = ErrCorrupt
	}
	return e
}
