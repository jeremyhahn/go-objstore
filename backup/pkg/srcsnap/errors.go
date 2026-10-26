package srcsnap

import (
	"errors"
	"fmt"
)

var (
	// ErrConfig reports an invalid configuration: an unknown method or
	// policy, or a bad method option.
	ErrConfig = errors.New("srcsnap: invalid configuration")
	// ErrUnsupported reports a volume the method cannot snapshot, such as
	// a file system that auto has no method for.
	ErrUnsupported = errors.New("srcsnap: volume cannot be snapshotted")
	// ErrTool reports a snapshot tool that is not installed.
	ErrTool = errors.New("srcsnap: snapshot tool not found")
	// ErrCommand reports a snapshot tool that failed or printed output
	// that could not be understood.
	ErrCommand = errors.New("srcsnap: snapshot command failed")
	// ErrName reports a snapshot name, or a name a tool printed, outside
	// the pattern the package accepts.
	ErrName = errors.New("srcsnap: invalid snapshot name")
	// ErrBusy reports that another run holds the snapshot directory.
	ErrBusy = errors.New("srcsnap: another run holds the snapshot directory")
	// ErrFilesystem reports a local file system failure, such as an unsafe
	// snapshot directory or a mount point that cannot be made.
	ErrFilesystem = errors.New("srcsnap: filesystem failure")
	// ErrCanceled reports a context canceled before or during a step.
	ErrCanceled = errors.New("srcsnap: canceled")
)

// Error is the error the package returns. It names the step, the method,
// the volume and the snapshot; no tool output in it carries a secret, as
// the tools are given none.
type Error struct {
	// Op is the step that failed, such as "create" or "mount".
	Op string
	// Method is the snapshot method, or empty.
	Method Method
	// Volume is the volume, dataset or device involved, or empty.
	Volume string
	// Snapshot is the snapshot name involved, or empty.
	Snapshot string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	s := "source snapshot " + e.Op
	if e.Method != "" {
		s += " " + string(e.Method)
	}
	if e.Volume != "" {
		s += " volume " + e.Volume
	}
	if e.Snapshot != "" {
		s += " snapshot " + e.Snapshot
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

// The steps more than one method names.
const (
	opCreate = "create"
	opSweep  = "sweep"
)

// detailError is a cause the package states itself, under a sentinel.
type detailError string

func (e detailError) Error() string { return string(e) }

// detail formats a detailError.
func detail(format string, a ...any) error { return detailError(fmt.Sprintf(format, a...)) }
