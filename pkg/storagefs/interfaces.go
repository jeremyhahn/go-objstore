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
	"io"
	"os"
	"time"
)

// Fs is a filesystem interface that provides methods for file operations.
// This interface is similar to os package and provides a filesystem abstraction
// over object storage backends.
type Fs interface {
	// Create creates a file for writing
	Create(name string) (File, error)

	// Mkdir creates a directory with the given permissions
	Mkdir(name string, perm os.FileMode) error

	// MkdirAll creates a directory and all parent directories
	MkdirAll(path string, perm os.FileMode) error

	// Open opens a file for reading
	Open(name string) (File, error)

	// OpenFile opens a file with specified flags and permissions
	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	// Remove removes a file
	Remove(name string) error

	// RemoveAll removes a path and all children
	RemoveAll(path string) error

	// Rename renames (moves) a file
	Rename(oldname, newname string) error

	// Stat returns FileInfo for the given path
	Stat(name string) (os.FileInfo, error)

	// Chmod changes file mode bits
	Chmod(name string, mode os.FileMode) error

	// Chown changes file ownership
	Chown(name string, uid, gid int) error

	// Chtimes changes access and modification times
	Chtimes(name string, atime, mtime time.Time) error

	// Name returns the name of the filesystem
	Name() string
}

// File is a file interface that provides methods for file operations.
// This interface is similar to os.File and provides file access over
// object storage backends.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker

	// Name returns the name of the file
	Name() string

	// Stat returns FileInfo for this file
	Stat() (os.FileInfo, error)

	// Sync flushes file changes to storage
	Sync() error

	// Truncate changes the size of the file
	Truncate(size int64) error

	// WriteString writes a string to the file
	WriteString(s string) (ret int, err error)

	// Readdir reads directory entries
	Readdir(count int) ([]os.FileInfo, error)

	// Readdirnames reads directory entry names
	Readdirnames(n int) ([]string, error)
}
