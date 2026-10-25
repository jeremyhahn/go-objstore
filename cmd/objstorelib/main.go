// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package main

// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"unsafe"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Errors for C API
var (
	// ErrInvalidHandle is returned when an invalid storage handle is provided
	ErrInvalidHandle = errors.New("invalid storage handle")

	// ErrBufferTooSmall is returned when the provided buffer is too small
	ErrBufferTooSmall = errors.New("buffer too small")

	// ErrInvalidLength is returned when a caller passes a negative length or count
	ErrInvalidLength = errors.New("invalid length")

	// ErrHandlesExhausted is returned when every handle a C int can carry has
	// been issued
	ErrHandlesExhausted = errors.New("storage handle space exhausted")
)

// NewInvalidHandleError creates a new invalid handle error
func NewInvalidHandleError(handle int) error {
	return fmt.Errorf("%w: %d", ErrInvalidHandle, handle)
}

// NewBufferTooSmallError creates a new buffer too small error
func NewBufferTooSmallError(need, have int) error {
	return fmt.Errorf("%w: need %d bytes, have %d", ErrBufferTooSmall, need, have)
}

// InvalidLengthError reports a negative length or count supplied across the C
// boundary. It names the parameter at fault, because a caller holding only -1
// and a message cannot otherwise tell which argument it got wrong.
//
// A typed error rather than fmt.Errorf, per the repository's error conventions
// and the LockedMemoryError precedent in go-enclave: the fields stay
// programmatically reachable instead of being flattened into a string. (The
// two constructors above predate that rule and are left alone here — converting
// them is not this change's business.)
type InvalidLengthError struct {
	// Param is the exported function's parameter name, e.g. "dataLen".
	Param string
	// Value is the offending value as received.
	Value int
}

func (e *InvalidLengthError) Error() string {
	return fmt.Sprintf("%s: %s must not be negative, got %d", ErrInvalidLength, e.Param, e.Value)
}

// Is matches the ErrInvalidLength sentinel so callers branch on the category
// with errors.Is and never on this concrete type.
func (e *InvalidLengthError) Is(target error) bool {
	return target == ErrInvalidLength
}

// checkLength validates a caller-supplied length or count arriving over the C
// boundary, recording the failure for ObjstoreGetLastError. It reports whether
// the value is usable, so call sites read as a guard.
//
// Every length here crosses as a SIGNED C.int, so a negative value is
// representable and arrives in Go as a negative int. Unchecked it is not merely
// wrong but fatal: C.GoBytes panics on a negative length, which kills the host
// process that loaded this shared library rather than returning an error code
// it can act on. That is the whole point of the guard — this API's contract is
// -1 plus a retrievable message, and a panic honors neither.
func checkLength(param string, n int) bool {
	if n < 0 {
		setLastError(&InvalidLengthError{Param: param, Value: n})
		return false
	}
	return true
}

// Storage handle registry to manage Go objects from C
var (
	storageRegistry = make(map[int]any)
	storageCounter  int32
	storageMutex    sync.Mutex
	lastError       string
	lastErrorMutex  sync.Mutex
)

// registerStorage stores a Go storage object and returns a handle ID.
//
// Handles cross the C boundary as a 32-bit C.int and are never reused, so the
// counter is an int32 that refuses to pass math.MaxInt32. As a plain int it
// would truncate onto an earlier handle and route the caller's operations to
// another storage.
func registerStorage(storage any) (int32, error) {
	storageMutex.Lock()
	defer storageMutex.Unlock()
	if storageCounter == math.MaxInt32 {
		return 0, ErrHandlesExhausted
	}
	storageCounter++
	storageRegistry[int(storageCounter)] = storage
	return storageCounter, nil
}

// getStorage retrieves a storage object by handle ID
func getStorage(handle int) (any, error) {
	storageMutex.Lock()
	defer storageMutex.Unlock()
	storage, ok := storageRegistry[handle]
	if !ok {
		return nil, NewInvalidHandleError(handle)
	}
	return storage, nil
}

// unregisterStorage removes a storage object from the registry
func unregisterStorage(handle int) {
	storageMutex.Lock()
	defer storageMutex.Unlock()
	delete(storageRegistry, handle)
}

// setLastError stores an error message for retrieval by C code
func setLastError(err error) {
	lastErrorMutex.Lock()
	defer lastErrorMutex.Unlock()
	if err != nil {
		lastError = err.Error()
	} else {
		lastError = ""
	}
}

//export ObjstoreVersion
func ObjstoreVersion() *C.char {
	return C.CString("go-objstore v0.1.0")
}

//export ObjstoreGetLastError
func ObjstoreGetLastError() *C.char {
	lastErrorMutex.Lock()
	defer lastErrorMutex.Unlock()
	if lastError == "" {
		return nil
	}
	return C.CString(lastError)
}

//export ObjstoreNewStorage
func ObjstoreNewStorage(backendType *C.char, settingsKeys **C.char, settingsValues **C.char, settingsCount C.int) C.int {
	// A negative count previously fell through the `settingsCount > 0` guard
	// below and silently produced an empty settings map, so a caller that
	// miscomputed its count got a backend configured with nothing instead of an
	// error. Report it.
	if !checkLength("settingsCount", int(settingsCount)) {
		return -1
	}

	// Convert C strings to Go
	goBackendType := C.GoString(backendType)

	// Build settings map
	settings := make(map[string]string)
	if settingsCount > 0 {
		// #nosec G103 -- FFI boundary. settingsKeys/settingsValues are caller-owned
		// arrays of C strings whose length only the caller knows; settingsCount is
		// the C ABI's sole means of communicating it. The count is checked positive
		// above, and both slices are read (never written) and never outlive this
		// call. A count larger than the real allocation is a caller-contract
		// violation that no callee-side check can detect.
		keysSlice := unsafe.Slice(settingsKeys, settingsCount)
		// #nosec G103 -- same array-length contract as keysSlice above; the two are
		// parallel arrays indexed together under the same settingsCount.
		valuesSlice := unsafe.Slice(settingsValues, settingsCount)
		for i := 0; i < int(settingsCount); i++ {
			key := C.GoString(keysSlice[i])
			value := C.GoString(valuesSlice[i])
			settings[key] = value
		}
	}

	// Create storage backend
	storage, err := factory.NewStorage(goBackendType, settings, common.Credentials{})
	if err != nil {
		setLastError(err)
		return -1
	}

	handle, err := registerStorage(storage)
	if err != nil {
		if closer, ok := storage.(io.Closer); ok {
			_ = closer.Close()
		}
		setLastError(err)
		return -1
	}
	setLastError(nil)
	return C.int(handle)
}

//export ObjstorePut
func ObjstorePut(handle C.int, key *C.char, data *C.char, dataLen C.int) C.int {
	// Before C.GoBytes below, which panics on a negative length and takes the
	// host process with it.
	if !checkLength("dataLen", int(dataLen)) {
		return -1
	}

	storage, err := getStorage(int(handle))
	if err != nil {
		setLastError(err)
		return -1
	}

	goKey := C.GoString(key)
	// #nosec G103 -- FFI boundary. GoBytes COPIES dataLen bytes from caller memory
	// into a Go-owned slice, so nothing here aliases the caller's buffer past the
	// call. dataLen is known non-negative (checkLength at the top of the
	// function), which matters because gobytes PANICS on a negative length rather
	// than erroring. What remains is a value larger than the real allocation — a
	// caller-contract violation the callee cannot detect, as the C ABI requires
	// the caller to assert its own buffer's length.
	goData := C.GoBytes(unsafe.Pointer(data), dataLen)

	reader := bytes.NewReader(goData)
	err = storage.(interface {
		Put(string, io.Reader) error
	}).Put(goKey, reader)

	if err != nil {
		setLastError(err)
		return -1
	}

	setLastError(nil)
	return 0
}

//export ObjstoreGet
func ObjstoreGet(handle C.int, key *C.char, buffer *C.char, bufferSize C.int) C.int {
	// A negative bufferSize was already rejected here, but as "buffer too small:
	// need 0, have -1" — an accurate refusal with a message that sends the caller
	// looking for a sizing bug rather than a sign bug.
	if !checkLength("bufferSize", int(bufferSize)) {
		return -1
	}

	storage, err := getStorage(int(handle))
	if err != nil {
		setLastError(err)
		return -1
	}

	goKey := C.GoString(key)

	reader, err := storage.(interface {
		Get(string) (io.ReadCloser, error)
	}).Get(goKey)
	if err != nil {
		setLastError(err)
		return -1
	}
	defer func() { _ = reader.Close() }()

	// Read data into Go buffer first
	data, err := io.ReadAll(reader)
	if err != nil {
		setLastError(err)
		return -1
	}

	// Check if buffer is large enough
	if len(data) > int(bufferSize) {
		setLastError(NewBufferTooSmallError(len(data), int(bufferSize)))
		return -1
	}

	// Copy to C buffer
	if len(data) > 0 {
		// #nosec G103 -- FFI boundary, and the one write into caller memory here.
		// It is bounded on this side by two checks: bufferSize is known
		// non-negative (checkLength at the top of the function), and the
		// len(data) > bufferSize check above returns ErrBufferTooSmall, so the
		// copy can never exceed the length the caller declared. The residual risk
		// is a caller declaring a bufferSize larger than it allocated, which is a
		// caller-contract violation no callee-side check can detect.
		goBuffer := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), bufferSize)
		copy(goBuffer, data)
	}

	setLastError(nil)
	// Already implied by len(data) <= bufferSize above. Stated against the
	// int32 bounds so gosec G115 can prove the conversion; it cannot read
	// #nosec in a cgo file.
	n := len(data)
	if n < math.MinInt32 || n > math.MaxInt32 {
		setLastError(NewBufferTooSmallError(n, int(bufferSize)))
		return -1
	}
	return C.int(n)
}

//export ObjstoreDelete
func ObjstoreDelete(handle C.int, key *C.char) C.int {
	storage, err := getStorage(int(handle))
	if err != nil {
		setLastError(err)
		return -1
	}

	goKey := C.GoString(key)

	err = storage.(interface {
		Delete(string) error
	}).Delete(goKey)

	if err != nil {
		setLastError(err)
		return -1
	}

	setLastError(nil)
	return 0
}

//export ObjstoreClose
func ObjstoreClose(handle C.int) {
	unregisterStorage(int(handle))
	setLastError(nil)
}

//export ObjstoreFreeString
func ObjstoreFreeString(str *C.char) {
	// #nosec G103 -- FFI boundary, and the deallocation half of this library's
	// ownership contract: every *C.char it hands out comes from C.CString (a
	// malloc), so the caller must return it here or leak. Passing a foreign or
	// already-freed pointer is undefined, and C offers no way to verify one —
	// which is why this function exists rather than asking callers to free().
	C.free(unsafe.Pointer(str))
}

func main() {}
