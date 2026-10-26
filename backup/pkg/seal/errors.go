package seal

import (
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"
)

var (
	// ErrOpen reports a sealed object that does not authenticate: a wrong key,
	// type or ID, or any altered byte.
	ErrOpen = errors.New("seal: the object does not authenticate")

	// ErrMalformed reports input too short to be a sealed object, or in an
	// unknown format or version.
	ErrMalformed = errors.New("seal: the sealed object is malformed")

	// ErrUnknownType reports an object type this version does not know.
	ErrUnknownType = errors.New("seal: unknown object type")

	// ErrUnsupportedCompression reports a compression algorithm this version
	// does not implement.
	ErrUnsupportedCompression = errors.New("seal: unsupported compression")

	// ErrKeysDestroyed reports use of a key after it was destroyed.
	ErrKeysDestroyed = errors.New("seal: the keys have been destroyed")

	// ErrInvalidKey reports a missing data key, one of the wrong size, or a
	// subkey that could not be derived or read.
	ErrInvalidKey = errors.New("seal: invalid key")
)

// Error is the error every operation in this package returns. Err is one of the
// package's sentinels; Cause, when set, is the underlying error. Neither ever
// holds key material or plaintext.
type Error struct {
	// Op names the operation that failed, such as "seal" or "open".
	Op string
	// Err is the sentinel the failure matches.
	Err error
	// Cause is the underlying error, or nil.
	Cause error
}

// Error returns the operation, the sentinel's text and the cause's text.
func (e *Error) Error() string {
	s := e.Op + ": " + e.Err.Error()
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap returns Err and, when set, Cause, so errors.Is matches either.
func (e *Error) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.Cause}
}

// newError builds an *Error, classing a destroyed enclave as ErrKeysDestroyed.
func newError(op string, sentinel, cause error) *Error {
	if cause != nil && errors.Is(cause, enclave.ErrEnclaveDestroyed) {
		sentinel = ErrKeysDestroyed
	}
	return &Error{Op: op, Err: sentinel, Cause: cause}
}
