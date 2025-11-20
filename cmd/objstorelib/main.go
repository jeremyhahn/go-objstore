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
		keysSlice := unsafe.Slice(settingsKeys, settingsCount)
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
	C.free(unsafe.Pointer(str))
}

func main() {}
