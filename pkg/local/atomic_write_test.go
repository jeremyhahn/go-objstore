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

package local_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// errReaderAtomic is an io.Reader that yields some bytes and then fails, used to
// simulate a streaming error part-way through a write.
type errReaderAtomic struct {
	data []byte
	pos  int
	err  error
}

func (r *errReaderAtomic) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// noTempFilesLeft asserts that no atomic-write temporaries remain anywhere under dir.
func noTempFilesLeft(t *testing.T, dir string) {
	t.Helper()
	var leftovers []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			leftovers = append(leftovers, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if len(leftovers) > 0 {
		t.Fatalf("expected no .tmp-* files, found: %v", leftovers)
	}
}

// TestLocal_PutAtomic_FullContentNoTempLeftover verifies a successful Put produces
// the complete object content and leaves no atomic-write temporary files behind.
func TestLocal_PutAtomic_FullContentNoTempLeftover(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	if err := storage.Configure(map[string]string{"path": tempDir}); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}

	want := bytes.Repeat([]byte("atomic-write-payload-"), 1024)
	if err := storage.PutWithContext(context.Background(), "objects/data.bin", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rc, err := storage.GetWithContext(context.Background(), "objects/data.bin")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}

	noTempFilesLeft(t, tempDir)

	// The published object must carry the same final mode os.Create used (0644).
	info, err := os.Stat(filepath.Join(tempDir, "objects/data.bin"))
	if err != nil {
		t.Fatalf("stat object: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0644 {
		t.Fatalf("object mode = %o, want 0644", perm)
	}

	// The metadata sidecar must be written with restrictive 0600 perms.
	mInfo, err := os.Stat(filepath.Join(tempDir, "objects/data.bin.metadata.json"))
	if err != nil {
		t.Fatalf("stat metadata: %v", err)
	}
	if perm := mInfo.Mode().Perm(); perm != 0600 {
		t.Fatalf("metadata mode = %o, want 0600", perm)
	}
}

// TestLocal_PutAtomic_WriteErrorLeavesPreviousIntact verifies that when a write
// fails mid-stream the previously stored object is left untouched and no partial
// target or temporary file is left behind.
func TestLocal_PutAtomic_WriteErrorLeavesPreviousIntact(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	if err := storage.Configure(map[string]string{"path": tempDir}); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}

	original := []byte("original-good-content")
	if err := storage.PutWithContext(context.Background(), "objects/keep.bin", bytes.NewReader(original)); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	// Now attempt an overwrite that fails after streaming a few bytes.
	boom := errors.New("simulated read failure")
	bad := &errReaderAtomic{data: []byte("partial-new-data"), err: boom}
	err := storage.PutWithContext(context.Background(), "objects/keep.bin", bad)
	if err == nil {
		t.Fatalf("expected Put to fail on read error, got nil")
	}

	// The original object must be intact (rename never happened).
	rc, err := storage.GetWithContext(context.Background(), "objects/keep.bin")
	if err != nil {
		t.Fatalf("Get after failed overwrite failed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("original content was corrupted: got %q, want %q", got, original)
	}

	// No temp file should remain after the failed write.
	noTempFilesLeft(t, tempDir)
}
