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

	"github.com/jeremyhahn/go-enclave"

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

// checkLength rejects a negative C length (C.GoBytes would panic the host process) and records the error for ObjstoreGetLastError.
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

// registerStorage stores a storage object and returns a never-reused int32 handle ID.
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
	// A negative count is an error, not an empty settings map.
	if !checkLength("settingsCount", int(settingsCount)) {
		return -1
	}
	return C.int(buildStorage(C.GoString(backendType), readSettings(settingsKeys, settingsValues, settingsCount), nil, nil))
}

// readSettings copies the parallel C arrays into a Go map.
func readSettings(settingsKeys **C.char, settingsValues **C.char, settingsCount C.int) map[string]string {
	settings := make(map[string]string)
	if settingsCount > 0 {
		// #nosec G103 -- FFI boundary: caller-owned C string arrays of the checked count, read only within this call.
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
	return settings
}

// ObjstoreNewStorageWithCredentials is ObjstoreNewStorage for a backend that
// needs an explicit credential. key and secret are byte buffers with explicit
// lengths (not NUL-terminated strings): the S3 access key and secret key, or
// the Azure account key in secret. Pass NULL and 0 for one or both to use the
// SDK default chain. A credential still cannot go in the settings arrays.
//
// The library copies each buffer into protected memory, builds the backend,
// destroys the protected copy before returning, and zeroizes its own temporary
// copy. It keeps nothing of the credential after the call. The caller still
// owns key and secret and must wipe them (explicit_bzero, SecureZeroMemory)
// as soon as the call returns.
//
//export ObjstoreNewStorageWithCredentials
func ObjstoreNewStorageWithCredentials(backendType *C.char, settingsKeys **C.char, settingsValues **C.char, settingsCount C.int, key *C.char, keyLen C.int, secret *C.char, secretLen C.int) C.int {
	if !checkLength("settingsCount", int(settingsCount)) || !checkLength("keyLen", int(keyLen)) || !checkLength("secretLen", int(secretLen)) {
		return -1
	}
	if (key == nil && keyLen > 0) || (secret == nil && secretLen > 0) {
		setLastError(&InvalidLengthError{Param: "credential buffer", Value: -1})
		return -1
	}
	// C.GoBytes copies the caller's buffer; both Go copies are zeroized below,
	// whether or not the backend was built.
	var keyBytes, secretBytes []byte
	if keyLen > 0 {
		keyBytes = C.GoBytes(unsafe.Pointer(key), keyLen) // #nosec G103 -- length-checked caller buffer, copied once
		defer clear(keyBytes)
	}
	if secretLen > 0 {
		secretBytes = C.GoBytes(unsafe.Pointer(secret), secretLen) // #nosec G103 -- length-checked caller buffer, copied once
		defer clear(secretBytes)
	}
	return C.int(buildStorage(C.GoString(backendType), readSettings(settingsKeys, settingsValues, settingsCount), keyBytes, secretBytes))
}

// buildStorage creates the backend with key and secret in protected-memory handles, destroys them and returns the handle, or -1 with the last error set.
func buildStorage(backendType string, settings map[string]string, key, secret []byte) int32 {
	creds, release, err := protectCredentials(key, secret)
	if err != nil {
		setLastError(err)
		return -1
	}

	storage, err := factory.NewStorage(backendType, settings, creds)
	if err = errors.Join(err, release()); err != nil {
		if closer, ok := storage.(io.Closer); ok {
			_ = closer.Close()
		}
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
	return handle
}

// protectCredentials copies key and secret into protected memory. An empty
// slice leaves that handle nil. release destroys the handles and is never nil.
func protectCredentials(key, secret []byte) (common.Credentials, func() error, error) {
	var creds common.Credentials
	release := func() error { return errors.Join(destroyHandle(creds.Key), destroyHandle(creds.Secret)) }
	var err error
	if creds.Key, err = protect(key); err != nil {
		return common.Credentials{}, func() error { return nil }, err
	}
	if creds.Secret, err = protect(secret); err != nil {
		_ = release()
		return common.Credentials{}, func() error { return nil }, err
	}
	return creds, release, nil
}

func protect(material []byte) (enclave.ByteAccess, error) {
	if len(material) == 0 {
		return nil, nil
	}
	return enclave.NewMemory(material)
}

func destroyHandle(h enclave.ByteAccess) error {
	if h == nil {
		return nil
	}
	return h.Destroy()
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
	// #nosec G103 -- FFI boundary: GoBytes copies a checked non-negative dataLen from caller memory.
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
		// #nosec G103 -- FFI boundary: the copy is bounded by the checked, caller-declared bufferSize.
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
	// #nosec G103 -- FFI boundary: frees a *C.char this library allocated with C.CString.
	C.free(unsafe.Pointer(str))
}

func main() {}
