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
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const (
	// metadataPrefix is the prefix used for storing file metadata
	metadataPrefix = ".meta/"
	// dirMarker is the file name used to mark directories
	dirMarker = ".dir"
)

var (
	// ErrIsDirectory indicates the path is a directory
	ErrIsDirectory = errors.New("is a directory")
	// ErrNotDirectory indicates the path is not a directory
	ErrNotDirectory = errors.New("not a directory")
)

// fileMetadata represents metadata stored for each file/directory
// This is used internally and for the File implementation
type fileMetadata struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	Mode    os.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
	IsDir   bool        `json:"isDir"`
}

// StorageFS wraps a common.Storage interface to provide filesystem semantics.
// It implements the Fs interface for file operations over object storage.
type StorageFS struct {
	storage common.Storage
}

// New creates a new StorageFS instance wrapping the given storage backend.
func New(storage common.Storage) *StorageFS {
	return &StorageFS{
		storage: storage,
	}
}

// Name returns the name of the filesystem.
func (fs *StorageFS) Name() string {
	return "StorageFS"
}

// Create creates a file in the filesystem, returning the file and an error, if any.
// For now, this returns a placeholder until the File implementation is complete.
func (fs *StorageFS) Create(name string) (File, error) {
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// Mkdir creates a directory in the filesystem.
func (fs *StorageFS) Mkdir(name string, perm os.FileMode) error {
	name = normalizePath(name)

	// Check if directory already exists
	if exists, _ := fs.dirExists(name); exists {
		return os.ErrExist
	}

	// Create directory marker
	markerKey := path.Join(name, dirMarker)
	if err := fs.storage.Put(markerKey, bytes.NewReader([]byte{})); err != nil {
		return err
	}

	// Create and store metadata
	meta := fileMetadata{
		Name:    path.Base(name),
		Size:    0,
		Mode:    os.ModeDir | perm,
		ModTime: time.Now(),
		IsDir:   true,
	}
	if err := fs.putMetadataInternal(name, meta); err != nil {
		// Try to clean up the marker
		_ = fs.storage.Delete(markerKey)
		return err
	}

	return nil
}

// MkdirAll creates a directory path and all parents that do not exist yet.
func (fs *StorageFS) MkdirAll(name string, perm os.FileMode) error {
	name = normalizePath(name)

	// Split path into components
	parts := splitPath(name)
	currentPath := ""

	for _, part := range parts {
		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = path.Join(currentPath, part)
		}
		if exists, _ := fs.dirExists(currentPath); exists {
			continue
		}

		// Create the directory
		if err := fs.Mkdir(currentPath, perm); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}

	return nil
}

// Open opens a file, returning the file and an error, if any.
// For now, this returns a placeholder until the File implementation is complete.
func (fs *StorageFS) Open(name string) (File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile opens a file with the specified flag and perm.
func (fs *StorageFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return newStorageFile(fs, name, flag, perm)
}

// Remove removes a file or directory from the filesystem.
func (fs *StorageFS) Remove(name string) error {
	name = normalizePath(name)

	// Check if it's a directory
	if exists, _ := fs.dirExists(name); exists {
		// Remove directory marker
		markerKey := path.Join(name, dirMarker)
		if err := fs.storage.Delete(markerKey); err != nil {
			return err
		}

		// Remove metadata
		if err := fs.deleteMetadata(name); err != nil {
			return err
		}

		return nil
	}

	// Check if it's a file
	if exists, _ := fs.fileExists(name); exists {
		// Remove file data
		if err := fs.storage.Delete(name); err != nil {
			return err
		}

		// Remove metadata
		if err := fs.deleteMetadata(name); err != nil {
			return err
		}

		return nil
	}

	return os.ErrNotExist
}

// RemoveAll removes a path and any children it contains.
func (fs *StorageFS) RemoveAll(name string) error {
	name = normalizePath(name)

	// List all keys with this prefix
	keys := fs.listKeys(name)

	// Delete all keys
	for _, key := range keys {
		if err := fs.storage.Delete(key); err != nil {
			// Continue on error to delete as much as possible
			continue
		}
	}

	// Delete all metadata with this prefix
	metaKeys := fs.listKeys(metadataPrefix + name)
	for _, key := range metaKeys {
		_ = fs.storage.Delete(key)
	}

	return nil
}

// Rename renames (moves) oldpath to newpath.
func (fs *StorageFS) Rename(oldpath, newpath string) error {
	oldpath = normalizePath(oldpath)
	newpath = normalizePath(newpath)

	// Check if source exists
	isDir := false
	if exists, _ := fs.dirExists(oldpath); exists {
		isDir = true
	} else if exists, _ := fs.fileExists(oldpath); exists {
		isDir = false
	} else {
		return os.ErrNotExist
	}

	// Get metadata
	meta, err := fs.getMetadataInternal(oldpath)
	if err != nil {
		return err
	}

	// Update the name in metadata
	meta.Name = path.Base(newpath)

	if isDir {
		// For directories, move the marker
		oldMarker := path.Join(oldpath, dirMarker)
		newMarker := path.Join(newpath, dirMarker)

		// Read old marker
		data, err := fs.storage.Get(oldMarker)
		if err != nil {
			return err
		}
		_ = data.Close() // #nosec G104 -- Closing read-only resource, error not actionable

		// Write new marker
		if err := fs.storage.Put(newMarker, bytes.NewReader([]byte{})); err != nil {
			return err
		}

		// Delete old marker
		if err := fs.storage.Delete(oldMarker); err != nil {
			_ = fs.storage.Delete(newMarker) // Cleanup
			return err
		}
	} else {
		// For files, move the data
		data, err := fs.storage.Get(oldpath)
		if err != nil {
			return err
		}
		defer data.Close()

		// Write to new location
		if err := fs.storage.Put(newpath, data); err != nil {
			return err
		}

		// Delete old location
		if err := fs.storage.Delete(oldpath); err != nil {
			_ = fs.storage.Delete(newpath) // Cleanup
			return err
		}
	}

	// Move metadata
	if err := fs.putMetadataInternal(newpath, meta); err != nil {
		return err
	}
	if err := fs.deleteMetadata(oldpath); err != nil {
		return err
	}

	return nil
}

// Stat returns a FileInfo describing the named file.
func (fs *StorageFS) Stat(name string) (os.FileInfo, error) {
	name = normalizePath(name)

	// Try to get metadata
	meta, err := fs.getMetadataInternal(name)
	if err != nil {
		// If metadata doesn't exist, check if the resource exists
		if exists, _ := fs.dirExists(name); exists {
			// Directory exists but no metadata - create default
			return NewFileInfo(path.Base(name), 0, os.ModeDir|0755, time.Now(), true), nil
		}
		if exists, _ := fs.fileExists(name); exists {
			// File exists but no metadata - create default
			return NewFileInfo(path.Base(name), 0, 0644, time.Now(), false), nil
		}
		return nil, os.ErrNotExist
	}

	return &FileInfo{
		name:    meta.Name,
		size:    meta.Size,
		mode:    meta.Mode,
		modTime: meta.ModTime,
		isDir:   meta.IsDir,
	}, nil
}

// Chmod changes the mode of the named file to mode.
func (fs *StorageFS) Chmod(name string, mode os.FileMode) error {
	name = normalizePath(name)

	// Get current metadata
	meta, err := fs.getMetadataInternal(name)
	if err != nil {
		return err
	}

	// Update mode
	meta.Mode = mode
	if meta.IsDir {
		meta.Mode |= os.ModeDir
	}

	// Save metadata
	return fs.putMetadataInternal(name, meta)
}

// Chown changes the owner and group of the named file.
// Not supported by StorageFS.
func (fs *StorageFS) Chown(name string, uid, gid int) error {
	return os.ErrInvalid
}

// Chtimes changes the access and modification times of the named file.
func (fs *StorageFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	name = normalizePath(name)

	// Get current metadata
	meta, err := fs.getMetadataInternal(name)
	if err != nil {
		return err
	}

	// Update modification time (we don't track access time separately)
	meta.ModTime = mtime

	// Save metadata
	return fs.putMetadataInternal(name, meta)
}

// Helper functions

// normalizePath cleans and normalizes a path for consistent storage.
func normalizePath(p string) string {
	if p == "" || p == "/" {
		return "."
	}

	// Replace backslashes with forward slashes
	p = strings.ReplaceAll(p, "\\", "/")

	// Clean the path
	p = path.Clean(p)

	// Remove leading slash
	p = strings.TrimPrefix(p, "/")

	// If we end up with empty string or ".", keep "."
	if p == "" {
		return "."
	}

	return p
}

// isDir checks if a path should be treated as a directory based on its name.
func isDir(p string) bool {
	if p == "" || p == "." || p == "/" {
		return true
	}
	return strings.HasSuffix(p, "/")
}

// splitPath splits a path into its components.
func splitPath(p string) []string {
	if p == "" || p == "." {
		return []string{}
	}

	parts := strings.Split(p, "/")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" && part != "." {
			result = append(result, part)
		}
	}
	return result
}

// putMetadata stores file metadata (using FileInfo type)
func (fs *StorageFS) putMetadata(name string, info *FileInfo) error {
	meta := fileMetadata{
		Name:    info.name,
		Size:    info.size,
		Mode:    info.mode,
		ModTime: info.modTime,
		IsDir:   info.isDir,
	}
	return fs.putMetadataInternal(name, meta)
}

// putMetadataInternal stores file metadata in the storage backend.
func (fs *StorageFS) putMetadataInternal(name string, meta fileMetadata) error {
	metaKey := metadataPrefix + name

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	if err := fs.storage.Put(metaKey, bytes.NewReader(data)); err != nil {
		return err
	}

	return nil
}

// getMetadata retrieves file metadata (returning FileInfo type)
func (fs *StorageFS) getMetadata(name string) (*FileInfo, error) {
	meta, err := fs.getMetadataInternal(name)
	if err != nil {
		return nil, err
	}

	return &FileInfo{
		name:    meta.Name,
		size:    meta.Size,
		mode:    meta.Mode,
		modTime: meta.ModTime,
		isDir:   meta.IsDir,
	}, nil
}

// getMetadataInternal retrieves file metadata from the storage backend.
func (fs *StorageFS) getMetadataInternal(name string) (fileMetadata, error) {
	metaKey := metadataPrefix + name

	data, err := fs.storage.Get(metaKey)
	if err != nil {
		return fileMetadata{}, err
	}
	defer data.Close()

	buf, err := io.ReadAll(data)
	if err != nil {
		return fileMetadata{}, err
	}

	var meta fileMetadata
	if err := json.Unmarshal(buf, &meta); err != nil {
		return fileMetadata{}, err
	}

	return meta, nil
}

// deleteMetadata removes file metadata from the storage backend.
func (fs *StorageFS) deleteMetadata(name string) error {
	metaKey := metadataPrefix + name
	if err := fs.storage.Delete(metaKey); err != nil {
		return err
	}
	return nil
}

// listKeys returns all keys with the given prefix.
// This is a helper that assumes the storage backend can provide this functionality.
func (fs *StorageFS) listKeys(prefix string) []string {
	// Since the Storage interface doesn't have a List method yet,
	// we'll need to use a type assertion to access it on the mock.
	// In real implementations, we'd need to add this to the Storage interface
	// or use a different approach.

	type lister interface {
		listKeys(prefix string) []string
	}

	if l, ok := fs.storage.(lister); ok {
		return l.listKeys(prefix)
	}

	// Return empty list if not supported
	return []string{}
}

// readDirEntries reads directory entries for the given directory path.
// It uses the storage backend's List functionality to find all items under the directory.
func (sfs *StorageFS) readDirEntries(name string) ([]fs.DirEntry, error) {
	name = normalizePath(name)

	// Build the prefix to search for
	// For a directory "a/b", we want to list items with prefix "a/b/"
	prefix := name
	if prefix != "." && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "./" {
		prefix = ""
	}

	// Use ListWithOptions with delimiter to get only direct children
	opts := &common.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	}

	result, err := sfs.storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	// Pre-allocate with reasonable capacity to reduce allocations
	entries := make([]fs.DirEntry, 0, 50)
	seen := make(map[string]bool, 50)

	// Add subdirectories from CommonPrefixes
	for _, commonPrefix := range result.CommonPrefixes {
		// Remove trailing slash and get the directory name
		dirPath := strings.TrimSuffix(commonPrefix, "/")
		if dirPath == "" {
			continue
		}

		// Get just the base name for this directory
		baseName := path.Base(dirPath)
		if seen[baseName] {
			continue
		}
		seen[baseName] = true

		// Try to get metadata for this directory
		meta, err := sfs.getMetadataInternal(dirPath)
		var info os.FileInfo
		if err != nil {
			// Create default directory info if metadata doesn't exist
			info = NewFileInfo(baseName, 0, os.ModeDir|0755, time.Now(), true)
		} else {
			info = &FileInfo{
				name:    baseName,
				size:    meta.Size,
				mode:    meta.Mode,
				modTime: meta.ModTime,
				isDir:   true,
			}
		}

		entries = append(entries, &dirEntry{info: info})
	}

	// Add files from Objects
	for _, obj := range result.Objects {
		// Skip metadata files and directory markers
		if strings.HasPrefix(obj.Key, metadataPrefix) || strings.HasSuffix(obj.Key, dirMarker) {
			continue
		}

		// Skip if not a direct child (shouldn't happen with delimiter, but be safe)
		relPath := strings.TrimPrefix(obj.Key, prefix)
		if strings.Contains(relPath, "/") {
			continue
		}

		baseName := path.Base(obj.Key)
		if baseName == "" || seen[baseName] {
			continue
		}
		seen[baseName] = true

		// Try to get metadata for this file
		meta, err := sfs.getMetadataInternal(obj.Key)
		var info os.FileInfo
		if err != nil || meta.Size == 0 && obj.Metadata != nil {
			// Use object metadata if file metadata isn't available
			size := int64(0)
			modTime := time.Now()
			if obj.Metadata != nil {
				size = obj.Metadata.Size
				modTime = obj.Metadata.LastModified
			}
			info = NewFileInfo(baseName, size, 0644, modTime, false)
		} else {
			info = &FileInfo{
				name:    baseName,
				size:    meta.Size,
				mode:    meta.Mode,
				modTime: meta.ModTime,
				isDir:   false,
			}
		}

		entries = append(entries, &dirEntry{info: info})
	}

	return entries, nil
}

// dirEntry implements fs.DirEntry
type dirEntry struct {
	info os.FileInfo
}

func (d *dirEntry) Name() string {
	return d.info.Name()
}

func (d *dirEntry) IsDir() bool {
	return d.info.IsDir()
}

func (d *dirEntry) Type() fs.FileMode {
	return d.info.Mode().Type()
}

func (d *dirEntry) Info() (os.FileInfo, error) {
	return d.info, nil
}

// dirExists checks if a directory exists in the storage backend.
func (fs *StorageFS) dirExists(name string) (bool, error) {
	markerKey := path.Join(name, dirMarker)

	// Try to get the directory marker
	data, err := fs.storage.Get(markerKey)
	if err != nil {
		return false, err
	}
	_ = data.Close() // #nosec G104 -- Closing read-only resource, error not actionable

	return true, nil
}

// fileExists checks if a file exists in the storage backend.
func (fs *StorageFS) fileExists(name string) (bool, error) {
	// Try to get the file
	data, err := fs.storage.Get(name)
	if err != nil {
		return false, err
	}
	_ = data.Close() // #nosec G104 -- Closing read-only resource, error not actionable

	return true, nil
}

// fileInfo is a simple implementation of os.FileInfo for internal use
type fileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() any           { return nil }

// Compile-time check that StorageFS implements Fs interface
var _ Fs = (*StorageFS)(nil)
