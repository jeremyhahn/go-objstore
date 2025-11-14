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
	"encoding/json"
	"os"
	"time"
)

// FileInfo implements os.FileInfo interface for StorageFS.
// It stores metadata about files and directories in the storage backend.
type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

// Name returns the base name of the file.
func (fi *FileInfo) Name() string {
	return fi.name
}

// Size returns the length in bytes for regular files.
// For directories, it returns 0.
func (fi *FileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode bits.
func (fi *FileInfo) Mode() os.FileMode {
	return fi.mode
}

// ModTime returns the modification time.
func (fi *FileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir reports whether the file is a directory.
func (fi *FileInfo) IsDir() bool {
	return fi.isDir
}

// Sys returns the underlying data source (always nil for StorageFS).
func (fi *FileInfo) Sys() any {
	return nil
}

// jsonFileInfo is used for JSON serialization/deserialization.
type jsonFileInfo struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	Mode    os.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
	IsDir   bool        `json:"isDir"`
}

// MarshalJSON implements json.Marshaler for FileInfo.
func (fi *FileInfo) MarshalJSON() ([]byte, error) {
	jfi := jsonFileInfo{
		Name:    fi.name,
		Size:    fi.size,
		Mode:    fi.mode,
		ModTime: fi.modTime,
		IsDir:   fi.isDir,
	}
	return json.Marshal(jfi)
}

// UnmarshalJSON implements json.Unmarshaler for FileInfo.
func (fi *FileInfo) UnmarshalJSON(data []byte) error {
	var jfi jsonFileInfo
	if err := json.Unmarshal(data, &jfi); err != nil {
		return err
	}
	fi.name = jfi.Name
	fi.size = jfi.Size
	fi.mode = jfi.Mode
	fi.modTime = jfi.ModTime
	fi.isDir = jfi.IsDir
	return nil
}

// NewFileInfo creates a new FileInfo instance.
func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, isDir bool) *FileInfo {
	return &FileInfo{
		name:    name,
		size:    size,
		mode:    mode,
		modTime: modTime,
		isDir:   isDir,
	}
}
