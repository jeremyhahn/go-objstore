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
	"sync"
	"unsafe"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Errors for C API
var (
	// ErrInvalidHandle is returned when an invalid storage handle is provided
	ErrInvalidHandle = errors.New("invalid storage handle")

	// ErrBufferTooSmall is returned when the provided buffer is too small
	ErrBufferTooSmall = errors.New("buffer too small")
)

// NewInvalidHandleError creates a new invalid handle error
func NewInvalidHandleError(handle int) error {
	return fmt.Errorf("%w: %d", ErrInvalidHandle, handle)
}

// NewBufferTooSmallError creates a new buffer too small error
func NewBufferTooSmallError(need, have int) error {
	return fmt.Errorf("%w: need %d bytes, have %d", ErrBufferTooSmall, need, have)
}

// Storage handle registry to manage Go objects from C
var (
	storageRegistry = make(map[int]any)
	storageCounter  = 0
	storageMutex    sync.Mutex
	lastError       string
	lastErrorMutex  sync.Mutex
)

// registerStorage stores a Go storage object and returns a handle ID
func registerStorage(storage any) int {
	storageMutex.Lock()
	defer storageMutex.Unlock()
	storageCounter++
	storageRegistry[storageCounter] = storage
	return storageCounter
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
	storage, err := factory.NewStorage(goBackendType, settings)
	if err != nil {
		setLastError(err)
		return -1
	}

	handle := registerStorage(storage)
	setLastError(nil)
	return C.int(handle)
}

//export ObjstorePut
func ObjstorePut(handle C.int, key *C.char, data *C.char, dataLen C.int) C.int {
	storage, err := getStorage(int(handle))
	if err != nil {
		setLastError(err)
		return -1
	}

	goKey := C.GoString(key)
	// #nosec G103 -- FFI boundary. GoBytes COPIES dataLen bytes from caller memory
	// into a Go-owned slice, so nothing here aliases the caller's buffer past the
	// call. dataLen is caller-asserted, as the C ABI requires; a negative value
	// panics inside gobytes rather than reading out of bounds, and a value larger
	// than the real allocation is a caller-contract violation the callee cannot
	// detect. Rejecting a negative length explicitly is tracked separately.
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
		// It is bounded on this side: the len(data) > bufferSize check above
		// returns ErrBufferTooSmall before reaching this line, so the copy can
		// never exceed the length the caller declared. (A negative bufferSize also
		// cannot reach here — it fails that same check for any non-empty data, and
		// empty data does not enter this branch.) The residual risk is a caller
		// declaring a bufferSize larger than it allocated, which is a
		// caller-contract violation no callee-side check can detect.
		goBuffer := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), bufferSize)
		copy(goBuffer, data)
	}

	setLastError(nil)
	return C.int(len(data))
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
