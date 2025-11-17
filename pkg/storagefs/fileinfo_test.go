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
	"testing"
	"time"
)

func TestFileInfo_Implementation(t *testing.T) {
	now := time.Now()
	fi := &FileInfo{
		name:    "test.txt",
		size:    1024,
		mode:    0644,
		modTime: now,
		isDir:   false,
	}

	if fi.Name() != "test.txt" {
		t.Errorf("Name() = %q, want %q", fi.Name(), "test.txt")
	}
	if fi.Size() != 1024 {
		t.Errorf("Size() = %d, want %d", fi.Size(), 1024)
	}
	if fi.Mode() != 0644 {
		t.Errorf("Mode() = %v, want %v", fi.Mode(), os.FileMode(0644))
	}
	if !fi.ModTime().Equal(now) {
		t.Errorf("ModTime() = %v, want %v", fi.ModTime(), now)
	}
	if fi.IsDir() {
		t.Error("IsDir() = true, want false")
	}
	if fi.Sys() != nil {
		t.Errorf("Sys() = %v, want nil", fi.Sys())
	}
}

func TestFileInfo_DirectoryInfo(t *testing.T) {
	fi := &FileInfo{
		name:  "mydir",
		isDir: true,
		mode:  os.ModeDir | 0755,
	}

	if !fi.IsDir() {
		t.Error("IsDir() = false, want true")
	}
	if fi.Size() != 0 {
		t.Errorf("Size() = %d, want 0 for directory", fi.Size())
	}
	if !fi.Mode().IsDir() {
		t.Error("Mode().IsDir() = false, want true")
	}
}

func TestFileInfo_JSONSerialization(t *testing.T) {
	now := time.Now().Truncate(time.Second) // Truncate for JSON comparison
	original := &FileInfo{
		name:    "data.json",
		size:    2048,
		mode:    0640,
		modTime: now,
		isDir:   false,
	}

	// Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Unmarshal back
	var restored FileInfo
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	// Verify all fields
	if restored.Name() != original.Name() {
		t.Errorf("Name after unmarshal = %q, want %q", restored.Name(), original.Name())
	}
	if restored.Size() != original.Size() {
		t.Errorf("Size after unmarshal = %d, want %d", restored.Size(), original.Size())
	}
	if restored.Mode() != original.Mode() {
		t.Errorf("Mode after unmarshal = %v, want %v", restored.Mode(), original.Mode())
	}
	if !restored.ModTime().Equal(original.ModTime()) {
		t.Errorf("ModTime after unmarshal = %v, want %v", restored.ModTime(), original.ModTime())
	}
	if restored.IsDir() != original.IsDir() {
		t.Errorf("IsDir after unmarshal = %v, want %v", restored.IsDir(), original.IsDir())
	}
}

func TestFileInfo_JSONRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		info *FileInfo
	}{
		{
			name: "regular file",
			info: &FileInfo{
				name:    "file.txt",
				size:    100,
				mode:    0644,
				modTime: time.Now().Truncate(time.Second),
				isDir:   false,
			},
		},
		{
			name: "directory",
			info: &FileInfo{
				name:    "mydir",
				size:    0,
				mode:    os.ModeDir | 0755,
				modTime: time.Now().Truncate(time.Second),
				isDir:   true,
			},
		},
		{
			name: "executable file",
			info: &FileInfo{
				name:    "script.sh",
				size:    512,
				mode:    0755,
				modTime: time.Now().Truncate(time.Second),
				isDir:   false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.info)
			if err != nil {
				t.Fatalf("Marshal error = %v", err)
			}

			var restored FileInfo
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatalf("Unmarshal error = %v", err)
			}

			if restored.Name() != tt.info.Name() {
				t.Errorf("Name = %q, want %q", restored.Name(), tt.info.Name())
			}
			if restored.Size() != tt.info.Size() {
				t.Errorf("Size = %d, want %d", restored.Size(), tt.info.Size())
			}
			if restored.IsDir() != tt.info.IsDir() {
				t.Errorf("IsDir = %v, want %v", restored.IsDir(), tt.info.IsDir())
			}
		})
	}
}
