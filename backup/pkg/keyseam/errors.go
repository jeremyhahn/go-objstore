package keyseam

import (
	"errors"

	enclave "github.com/jeremyhahn/go-enclave"
)

var (
	// ErrUnwrap reports a wrapped key that does not authenticate under the
	// provider's key-encryption key: a wrong passphrase or KEK, or a wrapped
	// key that was altered.
	ErrUnwrap = errors.New("keyseam: the wrapped key does not open under this key-encryption key")

	// ErrMalformedWrappedKey reports a wrapped key the provider cannot parse:
	// the wrong length, format or version, or parameters outside the
	// provider's bounds.
	ErrMalformedWrappedKey = errors.New("keyseam: the wrapped key is malformed")

	// ErrKeyDestroyed reports a key handle, data key or provider whose key
	// material has been destroyed.
	ErrKeyDestroyed = errors.New("keyseam: the key has been destroyed")

	// ErrInvalidKey reports a missing key handle or one of the wrong size.
	ErrInvalidKey = errors.New("keyseam: invalid key")

	// ErrNilProvider reports an operation given no Provider.
	ErrNilProvider = errors.New("keyseam: no key provider")

	// ErrIterations reports a PBKDF2 iteration count below MinIterations or
	// above MaxIterations.
	ErrIterations = errors.New("keyseam: PBKDF2 iteration count out of bounds")
	// ErrUnknownProvider means no provider is registered under the name.
	ErrUnknownProvider = errors.New("keyseam: unknown key provider")
	// ErrProviderRegistered means a provider name is already taken.
	ErrProviderRegistered = errors.New("keyseam: key provider already registered")
)

// Error is the error every operation in this package returns. Err is one of the
// package's sentinels or a context error; Cause, when set, is the underlying
// error (from go-enclave, the standard library or a Provider). Neither ever
// holds key material.
type Error struct {
	// Op names the operation that failed, such as "wrap" or "unwrap".
	Op string
	// Err is the sentinel or context error the failure matches.
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

// newError builds an *Error, classing a destroyed enclave as ErrKeyDestroyed.
func newError(op string, sentinel, cause error) *Error {
	if cause != nil && errors.Is(cause, enclave.ErrEnclaveDestroyed) {
		sentinel = ErrKeyDestroyed
	}
	return &Error{Op: op, Err: sentinel, Cause: cause}
}
