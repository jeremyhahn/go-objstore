package backup

import "errors"

var (
	// ErrInvalidRoot means the root to back up is not a directory.
	ErrInvalidRoot = errors.New("backup: root is not a directory")

	// ErrFilesystem means reading the tree failed; the cause is the
	// filesystem error.
	ErrFilesystem = errors.New("backup: filesystem failure")

	// ErrRepository means a repository operation failed; the cause is the
	// repository's *repo.Error.
	ErrRepository = errors.New("backup: repository failure")

	// ErrCanceled means the context ended the run; the cause is the
	// context's error.
	ErrCanceled = errors.New("backup: canceled")
)

// Error is a failed backup run. It matches its sentinel and its cause with
// errors.Is and errors.As.
type Error struct {
	// Op is the step that failed, such as "read file".
	Op string
	// Path is the local path involved, or empty. It is never stored in the
	// repository.
	Path string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error describes the step, the path and the failure.
func (e *Error) Error() string {
	s := "backup " + e.Op
	if e.Path != "" {
		s += " " + e.Path
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

func newError(op, path string, sentinel, cause error) *Error {
	return &Error{Op: op, Path: path, Err: sentinel, Cause: cause}
}
