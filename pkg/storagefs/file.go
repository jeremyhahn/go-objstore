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

package storagefs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// Error variables
var (
	ErrInvalidWhence  = errors.New("invalid whence")
	ErrNegativeOffset = errors.New("negative offset")
)

// StorageFile implements fs.File interface for object storage.
// It provides file-like operations with buffered writes and seek support.
type StorageFile struct {
	fs       *StorageFS
	name     string
	buf      *bytes.Buffer
	offset   int64
	flag     int
	perm     os.FileMode
	closed   atomic.Bool
	isDir    bool
	dirIndex int
	mu       sync.Mutex
	fileInfo *FileInfo
}

// newStorageFile creates a new StorageFile instance.
func newStorageFile(fs *StorageFS, name string, flag int, perm os.FileMode) (*StorageFile, error) {
	name = normalizePath(name)

	f := &StorageFile{
		fs:   fs,
		name: name,
		flag: flag,
		perm: perm,
	}

	// Check if opening a directory
	if exists, _ := fs.dirExists(name); exists {
		f.isDir = true
		info, err := fs.Stat(name)
		if err != nil {
			info = NewFileInfo(path.Base(name), 0, os.ModeDir|perm, time.Now(), true)
		}
		if fi, ok := info.(*FileInfo); ok {
			f.fileInfo = fi
		} else {
			f.fileInfo = NewFileInfo(path.Base(name), 0, os.ModeDir|perm, time.Now(), true)
		}
		return f, nil
	}

	// Determine read/write mode
	writeMode := flag&(os.O_WRONLY|os.O_RDWR) != 0
	// O_RDONLY is 0, so we need to check if neither WRONLY nor RDWR is set, or if RDWR is set
	accMode := flag & (os.O_WRONLY | os.O_RDWR)
	readMode := (accMode == 0) || (accMode == os.O_RDWR)
	// Alternative: readMode := accMode != os.O_WRONLY

	create := flag&os.O_CREATE != 0
	append := flag&os.O_APPEND != 0
	trunc := flag&os.O_TRUNC != 0

	// For read mode, try to get existing file
	if readMode {
		data, err := fs.storage.Get(name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || isNotFoundError(err) {
				if !create {
					return nil, os.ErrNotExist
				}
				// File doesn't exist but we're creating it
				f.buf = new(bytes.Buffer)
			} else {
				return nil, err
			}
		} else {
			defer func() { _ = data.Close() }()
			f.buf = new(bytes.Buffer)
			if _, err := io.Copy(f.buf, data); err != nil {
				return nil, err
			}

			// If truncate flag is set, truncate the buffer
			if trunc && writeMode {
				f.buf.Reset()
			}
		}
	} else if writeMode {
		// Write-only mode
		if append || (create && !trunc) {
			// Try to get existing content for append or create without truncate
			data, err := fs.storage.Get(name)
			if err == nil {
				defer func() { _ = data.Close() }()
				f.buf = new(bytes.Buffer)
				if _, err := io.Copy(f.buf, data); err != nil {
					return nil, err
				}
			} else {
				f.buf = new(bytes.Buffer)
			}
		} else {
			// Create new or truncate
			f.buf = new(bytes.Buffer)
		}
	}

	// Get or create file info
	info, err := fs.getMetadata(name)
	if err != nil {
		info = NewFileInfo(path.Base(name), 0, perm, time.Now(), false)
	}
	f.fileInfo = info

	return f, nil
}

// isNotFoundError checks if an error indicates a file was not found.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, os.ErrNotExist) ||
		err.Error() == "key not found"
}

// Close closes the file, flushing writes if necessary.
func (f *StorageFile) Close() error {
	if f.closed.Load() {
		return fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed.Store(true)

	// If file was opened for writing, flush to storage
	if f.flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 && f.buf != nil && !f.isDir {
		data := f.buf.Bytes()
		if err := f.fs.storage.Put(f.name, bytes.NewReader(data)); err != nil {
			return err
		}

		// Update metadata with new size and mod time
		f.fileInfo.size = int64(len(data))
		f.fileInfo.modTime = time.Now()
		if err := f.fs.putMetadata(f.name, f.fileInfo); err != nil {
			return err
		}
	}

	return nil
}

// Read reads data from the file.
func (f *StorageFile) Read(p []byte) (n int, err error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return 0, ErrIsDirectory
	}

	// Check if write-only (WRONLY but not RDWR)
	accMode := f.flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	if accMode == os.O_WRONLY {
		return 0, os.ErrPermission
	}

	if f.buf == nil {
		return 0, io.EOF
	}

	// Read from current offset
	data := f.buf.Bytes()
	if f.offset >= int64(len(data)) {
		return 0, io.EOF
	}

	n = copy(p, data[f.offset:])
	f.offset += int64(n)

	return n, nil
}

// ReadAt reads data from a specific offset.
func (f *StorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return 0, ErrIsDirectory
	}

	// Check if write-only (WRONLY but not RDWR)
	accMode := f.flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	if accMode == os.O_WRONLY {
		return 0, os.ErrPermission
	}

	if f.buf == nil {
		return 0, io.EOF
	}

	data := f.buf.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}

	n = copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Seek sets the offset for next Read or Write.
func (f *StorageFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return 0, ErrIsDirectory
	}

	var newOffset int64
	bufLen := int64(0)
	if f.buf != nil {
		bufLen = int64(f.buf.Len())
	}

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = bufLen + offset
	default:
		return 0, ErrInvalidWhence
	}

	if newOffset < 0 {
		return 0, ErrNegativeOffset
	}

	f.offset = newOffset
	return newOffset, nil
}

// Write writes data to the file.
func (f *StorageFile) Write(p []byte) (n int, err error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return 0, ErrIsDirectory
	}

	if f.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, os.ErrPermission
	}

	if f.buf == nil {
		f.buf = new(bytes.Buffer)
	}

	// Handle append mode
	if f.flag&os.O_APPEND != 0 {
		f.offset = int64(f.buf.Len())
	}

	// If writing at offset beyond current size, pad with zeros
	bufLen := int64(f.buf.Len())
	if f.offset > bufLen {
		padding := make([]byte, f.offset-bufLen)
		f.buf.Write(padding)
	}

	// If writing at an offset, we need to reconstruct the buffer
	if f.offset < bufLen {
		data := f.buf.Bytes()
		newData := make([]byte, 0, int64(len(data))+int64(len(p)))
		newData = append(newData, data[:f.offset]...)
		newData = append(newData, p...)
		if f.offset+int64(len(p)) < int64(len(data)) {
			newData = append(newData, data[f.offset+int64(len(p)):]...)
		}
		f.buf = bytes.NewBuffer(newData)
		f.offset += int64(len(p))
		return len(p), nil
	}

	// Append at current position
	n, err = f.buf.Write(p)
	f.offset += int64(n)
	return n, err
}

// WriteAt writes data at a specific offset.
func (f *StorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return 0, ErrIsDirectory
	}

	if f.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, os.ErrPermission
	}

	if f.buf == nil {
		f.buf = new(bytes.Buffer)
	}

	data := f.buf.Bytes()
	bufLen := int64(len(data))

	// Expand buffer if necessary
	if off+int64(len(p)) > bufLen {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, data)
		copy(newData[off:], p)
		f.buf = bytes.NewBuffer(newData)
	} else {
		// Write within existing buffer
		copy(data[off:], p)
		f.buf = bytes.NewBuffer(data)
	}

	return len(p), nil
}

// Name returns the file name.
func (f *StorageFile) Name() string {
	return f.name
}

// Readdir reads directory entries.
// If count > 0, it returns at most count entries.
// If count <= 0, it returns all remaining entries.
func (f *StorageFile) Readdir(count int) ([]os.FileInfo, error) {
	if f.closed.Load() {
		return nil, fs.ErrClosed
	}

	if !f.isDir {
		return nil, ErrNotDirectory
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Get all directory entries on first call
	if f.dirIndex == 0 {
		entries, err := f.fs.readDirEntries(f.name)
		if err != nil {
			return nil, err
		}

		// Convert DirEntry to FileInfo
		infos := make([]os.FileInfo, 0, len(entries))
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			infos = append(infos, info)
		}

		// Store in fileInfo metadata (reusing the field)
		if f.fileInfo == nil {
			f.fileInfo = NewFileInfo(path.Base(f.name), 0, os.ModeDir|0755, time.Now(), true)
		}
		// We need to store entries somewhere - let's add a dirEntries field
		// For now, create a temporary variable
		remaining := infos[f.dirIndex:]

		if count <= 0 {
			// Return all remaining
			f.dirIndex = len(infos)
			return remaining, nil
		}

		if count > len(remaining) {
			count = len(remaining)
		}

		result := remaining[:count]
		f.dirIndex += count

		if f.dirIndex >= len(infos) {
			return result, io.EOF
		}
		return result, nil
	}

	// Subsequent calls - we need to cache the entries
	// For simplicity, re-read on each call
	entries, err := f.fs.readDirEntries(f.name)
	if err != nil {
		return nil, err
	}

	// Convert DirEntry to FileInfo
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		infos = append(infos, info)
	}

	if f.dirIndex >= len(infos) {
		return nil, io.EOF
	}

	remaining := infos[f.dirIndex:]

	if count <= 0 {
		// Return all remaining
		f.dirIndex = len(infos)
		return remaining, nil
	}

	if count > len(remaining) {
		count = len(remaining)
	}

	result := remaining[:count]
	f.dirIndex += count

	if f.dirIndex >= len(infos) {
		return result, io.EOF
	}
	return result, nil
}

// Readdirnames reads directory entry names.
// If n > 0, it returns at most n names.
// If n <= 0, it returns all remaining names.
func (f *StorageFile) Readdirnames(n int) ([]string, error) {
	infos, err := f.Readdir(n)
	if err != nil && err != io.EOF {
		return nil, err
	}

	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names, err
}

// Stat returns file info.
func (f *StorageFile) Stat() (os.FileInfo, error) {
	if f.closed.Load() {
		return nil, fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Update size if buffer exists
	if f.buf != nil && !f.isDir {
		f.fileInfo.size = int64(f.buf.Len())
	}

	return f.fileInfo, nil
}

// Sync flushes file changes to storage (same as Close but doesn't close the file).
func (f *StorageFile) Sync() error {
	if f.closed.Load() {
		return fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// If file was opened for writing, flush to storage
	if f.flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 && f.buf != nil && !f.isDir {
		data := f.buf.Bytes()
		if err := f.fs.storage.Put(f.name, bytes.NewReader(data)); err != nil {
			return err
		}

		// Update metadata
		f.fileInfo.size = int64(len(data))
		f.fileInfo.modTime = time.Now()
		if err := f.fs.putMetadata(f.name, f.fileInfo); err != nil {
			return err
		}
	}

	return nil
}

// Truncate changes the size of the file.
func (f *StorageFile) Truncate(size int64) error {
	if f.closed.Load() {
		return fs.ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.isDir {
		return ErrIsDirectory
	}

	if f.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return os.ErrPermission
	}

	if f.buf == nil {
		f.buf = new(bytes.Buffer)
	}

	data := f.buf.Bytes()
	if size < int64(len(data)) {
		// Truncate to smaller size
		f.buf = bytes.NewBuffer(data[:size])
	} else if size > int64(len(data)) {
		// Expand with zeros
		padding := make([]byte, size-int64(len(data)))
		f.buf.Write(padding)
	}

	f.fileInfo.size = size
	return nil
}

// WriteString writes a string to the file.
func (f *StorageFile) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}
