package restore

import (
	"context"
	"errors"
	"strconv"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

var (
	// ErrInvalidTarget reports a restore target that is not a directory.
	ErrInvalidTarget = errors.New("restore: target is not a directory")
	// ErrUnsafeEntry reports a manifest entry that cannot be placed safely
	// inside the target: an absolute or non-local path, a path through a
	// symlink, a symlink that leads outside the target, or an entry the
	// manifest does not describe completely.
	ErrUnsafeEntry = errors.New("restore: entry cannot be placed safely")
	// ErrExists reports an existing entry at a path the restore would
	// replace without the overwrite option, or one it never replaces.
	ErrExists = errors.New("restore: entry already exists")
	// ErrCorrupt reports repository data that fails a check: a sealed object
	// that does not open or decode, a chunk whose ID does not match its
	// content, or a file whose size or digest does not match its manifest.
	ErrCorrupt = errors.New("restore: repository data is corrupt")
	// ErrMissing reports a repository object that is named but absent.
	ErrMissing = errors.New("restore: repository object is missing")
	// ErrUnindexed reports a chunk a manifest names that no index maps.
	ErrUnindexed = errors.New("restore: chunk is in no index")
	// ErrOrphan reports a pack that no index names.
	ErrOrphan = errors.New("restore: pack is in no index")
	// ErrFilesystem reports a failure writing under the target.
	ErrFilesystem = errors.New("restore: filesystem failure")
	// ErrRepository reports a repository failure other than corrupt or
	// missing data, such as an unreachable store.
	ErrRepository = errors.New("restore: repository failure")
	// ErrCanceled reports a restore or verify stopped by its context.
	ErrCanceled = errors.New("restore: canceled")
)

// Error is the error restore and verify return. It names repository objects
// and chunks by ID only; it never carries key material or file content.
type Error struct {
	// Op is the step that failed, such as "read pack" or "verify index".
	Op string
	// Object is the repository object involved, or empty.
	Object string
	// Chunk is the hex ID of the chunk involved, or empty.
	Chunk string
	// Path is the entry's path under the target, or empty. Verify never
	// sets it.
	Path string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	s := "restore " + e.Op
	if e.Object != "" {
		s += " " + e.Object
	}
	if e.Chunk != "" {
		s += " chunk " + e.Chunk
	}
	if e.Path != "" {
		s += " " + strconv.Quote(e.Path)
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

func newError(op, object, path string, sentinel, cause error) *Error {
	return &Error{Op: op, Object: object, Path: path, Err: sentinel, Cause: cause}
}

// fromRepo classifies a repository error, keeping the object it names.
func fromRepo(op string, err error) *Error {
	e := newError(op, "", "", ErrRepository, err)
	var re *repo.Error
	if errors.As(err, &re) {
		e.Object = re.Name
	}
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		e.Err = ErrCanceled
	case errors.Is(err, store.ErrNotFound):
		e.Err = ErrMissing
	case errors.Is(err, repo.ErrCorrupt) || errors.Is(err, repo.ErrMalformedName):
		e.Err = ErrCorrupt
	}
	return e
}

var (
	errNotLocal  = errors.New("path is absolute or not local")
	errDuplicate = errors.New("path appears twice")
	errParent    = errors.New("parent is not a directory in the manifest")
	errRoot      = errors.New("root entry is not a single directory")
	errType      = errors.New("unknown entry type")
	errFile      = errors.New("file size or digest is malformed")
	errTarget    = errors.New("symlink target is absolute or malformed")
	errEscape    = errors.New("symlink leads outside the target")
	errThrough   = errors.New("an existing symlink is in the way")
	errSize      = errors.New("file size does not match its chunks")
	errDigest    = errors.New("file digest does not match")
	errLength    = errors.New("chunk length does not match its entry")
	errMismatch  = errors.New("index entry does not match the pack")
)
