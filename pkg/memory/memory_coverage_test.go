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

// Additional tests covering branches not reached by memory_test.go.
package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// errReader is a minimal io.Reader that always returns an error.
type errReader struct{ err error }

func (e *errReader) Read(_ []byte) (int, error) { return 0, e.err }

// TestPutWithMetadataReaderError covers the io.ReadAll error branch in PutWithMetadata.
func TestPutWithMetadataReaderError(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	sentinel := errors.New("read error")
	err := m.PutWithMetadata(context.Background(), "valid-key", &errReader{err: sentinel}, nil)
	if err == nil {
		t.Fatal("PutWithMetadata() expected error from failing reader, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("PutWithMetadata() error = %v, want sentinel read error", err)
	}
}

// TestPutWithMetadataNilMetadata covers the metadata == nil branch in PutWithMetadata.
func TestPutWithMetadataNilMetadata(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	err := m.PutWithMetadata(context.Background(), "nil-meta-key", bytes.NewReader([]byte("data")), nil)
	if err != nil {
		t.Fatalf("PutWithMetadata() with nil metadata returned error: %v", err)
	}
	meta, err := m.GetMetadata(context.Background(), "nil-meta-key")
	if err != nil {
		t.Fatalf("GetMetadata() returned error: %v", err)
	}
	if meta.Size != 4 {
		t.Fatalf("GetMetadata() size = %d, want 4", meta.Size)
	}
}

// TestGetMetadataInvalidKey covers the validateKey error branch in GetMetadata.
func TestGetMetadataInvalidKey(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	_, err := m.GetMetadata(context.Background(), "../etc/passwd")
	if err == nil {
		t.Fatal("GetMetadata() expected error for invalid key, got nil")
	}
}

// TestGetMetadataCustomMapCopy covers the obj.metadata.Custom != nil copy branch in GetMetadata.
func TestGetMetadataCustomMapCopy(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	meta := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"foo": "bar"},
	}
	if err := m.PutWithMetadata(context.Background(), "custom-key", bytes.NewReader([]byte("hello")), meta); err != nil {
		t.Fatalf("PutWithMetadata() returned error: %v", err)
	}
	got, err := m.GetMetadata(context.Background(), "custom-key")
	if err != nil {
		t.Fatalf("GetMetadata() returned error: %v", err)
	}
	if got.Custom["foo"] != "bar" {
		t.Fatalf("GetMetadata() custom map = %v, want foo=bar", got.Custom)
	}
	// Mutating the returned copy must not affect stored metadata.
	got.Custom["foo"] = "mutated"
	got2, _ := m.GetMetadata(context.Background(), "custom-key")
	if got2.Custom["foo"] != "bar" {
		t.Fatalf("GetMetadata() custom map isolation broken: got %q", got2.Custom["foo"])
	}
}

// TestUpdateMetadataInvalidKey covers the validateKey error branch in UpdateMetadata.
func TestUpdateMetadataInvalidKey(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	err := m.UpdateMetadata(context.Background(), "../etc/passwd", &common.Metadata{})
	if err == nil {
		t.Fatal("UpdateMetadata() expected error for invalid key, got nil")
	}
}

// TestUpdateMetadataNilMetadata covers the metadata == nil branch in UpdateMetadata.
func TestUpdateMetadataNilMetadata(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	if err := m.PutWithMetadata(context.Background(), "upd-nil-key", bytes.NewReader([]byte("abc")), nil); err != nil {
		t.Fatalf("PutWithMetadata() returned error: %v", err)
	}
	// Passing nil metadata must not panic; it should create a fresh Metadata value.
	if err := m.UpdateMetadata(context.Background(), "upd-nil-key", nil); err != nil {
		t.Fatalf("UpdateMetadata() with nil metadata returned error: %v", err)
	}
	got, err := m.GetMetadata(context.Background(), "upd-nil-key")
	if err != nil {
		t.Fatalf("GetMetadata() returned error: %v", err)
	}
	if got.Size != 3 {
		t.Fatalf("UpdateMetadata() size = %d, want 3", got.Size)
	}
}

// TestExistsInvalidKey covers the validateKey error branch in Exists.
func TestExistsInvalidKey(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	_, err := m.Exists(context.Background(), "../etc/passwd")
	if err == nil {
		t.Fatal("Exists() expected error for invalid key, got nil")
	}
}

// TestListWithContextInvalidPrefix covers the validateKey error branch in ListWithContext.
func TestListWithContextInvalidPrefix(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	_, err := m.ListWithContext(context.Background(), "../etc/")
	if err == nil {
		t.Fatal("ListWithContext() expected error for invalid prefix, got nil")
	}
}

// TestListWithOptionsInvalidPrefix covers the validateKey error branch in ListWithOptions.
func TestListWithOptionsInvalidPrefix(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	_, err := m.ListWithOptions(context.Background(), &common.ListOptions{Prefix: "../etc/"})
	if err == nil {
		t.Fatal("ListWithOptions() expected error for invalid prefix, got nil")
	}
}

// TestListWithOptionsCustomMetadataCopy covers the obj.metadata.Custom != nil copy branch
// inside ListWithOptions when objects have custom metadata.
func TestListWithOptionsCustomMetadataCopy(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	meta := &common.Metadata{Custom: map[string]string{"env": "test"}}
	if err := m.PutWithMetadata(context.Background(), "items/one", bytes.NewReader([]byte("x")), meta); err != nil {
		t.Fatalf("PutWithMetadata() returned error: %v", err)
	}
	result, err := m.ListWithOptions(context.Background(), &common.ListOptions{Prefix: "items/"})
	if err != nil {
		t.Fatalf("ListWithOptions() returned error: %v", err)
	}
	if len(result.Objects) != 1 {
		t.Fatalf("ListWithOptions() returned %d objects, want 1", len(result.Objects))
	}
	if result.Objects[0].Metadata.Custom["env"] != "test" {
		t.Fatalf("ListWithOptions() custom metadata = %v, want env=test", result.Objects[0].Metadata.Custom)
	}
}

// TestListWithOptionsContinueFromNotFound covers the ContinueFrom token that does not match
// any object key — startIdx stays 0 and all objects are returned from the beginning.
func TestListWithOptionsContinueFromNotFound(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := m.PutWithMetadata(context.Background(), key, bytes.NewReader([]byte("d")), nil); err != nil {
			t.Fatalf("PutWithMetadata(%q) error: %v", key, err)
		}
	}
	result, err := m.ListWithOptions(context.Background(), &common.ListOptions{
		ContinueFrom: "nonexistent",
		MaxResults:   10,
	})
	if err != nil {
		t.Fatalf("ListWithOptions() returned error: %v", err)
	}
	// ContinueFrom that matches nothing should return all items from index 0.
	if len(result.Objects) != 3 {
		t.Fatalf("ListWithOptions() returned %d objects with unmatched ContinueFrom, want 3", len(result.Objects))
	}
}

// TestArchiveInvalidKey covers the validateKey error branch in Archive.
func TestArchiveInvalidKey(t *testing.T) {
	m := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	dest := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	err := m.Archive("../etc/passwd", dest)
	if err == nil {
		t.Fatal("Archive() expected error for invalid key, got nil")
	}
}

// TestLifecycleManagerRun exercises the Run loop by setting a very short interval,
// adding a delete policy, putting a stale object, running the loop in a goroutine,
// and confirming the object is deleted within a bounded timeout.
func TestLifecycleManagerRun(t *testing.T) {
	mem := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	// Put an object and backdate it so it triggers the delete policy.
	if err := mem.PutWithMetadata(context.Background(), "run/old.txt", bytes.NewReader([]byte("stale")), nil); err != nil {
		t.Fatalf("PutWithMetadata() error: %v", err)
	}
	mem.mu.Lock()
	if obj, ok := mem.objects["run/old.txt"]; ok {
		obj.metadata.LastModified = time.Now().Add(-24 * time.Hour)
	}
	mem.mu.Unlock()

	lm := NewLifecycleManager()
	lm.interval = 10 * time.Millisecond // override so the loop fires quickly

	err := lm.AddPolicy(common.LifecyclePolicy{
		ID:        "run-delete",
		Prefix:    "run/",
		Action:    "delete",
		Retention: time.Hour,
	})
	if err != nil {
		t.Fatalf("AddPolicy() error: %v", err)
	}

	// Run the loop in the background; use a channel to detect the first deletion.
	done := make(chan struct{})
	go func() {
		// Run enters an infinite for loop; we let it cycle until the object
		// disappears, then stop caring about it (the goroutine will leak for
		// the remainder of the test binary lifetime, which is acceptable for
		// a unit test — it sleeps 10 ms between cycles and holds no resources).
		lm.Run(mem)
	}()

	// Poll until the object is gone or the deadline expires.
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	defer close(done)

	for {
		select {
		case <-deadline:
			t.Fatal("Run() did not delete the stale object within 2s")
		case <-tick.C:
			exists, _ := mem.Exists(context.Background(), "run/old.txt")
			if !exists {
				return // Run loop fired and deleted the object — test passes
			}
		}
	}
}

// TestLifecycleManagerRunArchive exercises the Run loop archive branch.
func TestLifecycleManagerRunArchive(t *testing.T) {
	src := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	dst := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	if err := src.PutWithMetadata(context.Background(), "runarch/old.txt", bytes.NewReader([]byte("content")), nil); err != nil {
		t.Fatalf("PutWithMetadata() error: %v", err)
	}
	src.mu.Lock()
	if obj, ok := src.objects["runarch/old.txt"]; ok {
		obj.metadata.LastModified = time.Now().Add(-24 * time.Hour)
	}
	src.mu.Unlock()

	lm := NewLifecycleManager()
	lm.interval = 10 * time.Millisecond

	if err := lm.AddPolicy(common.LifecyclePolicy{
		ID:          "run-archive",
		Prefix:      "runarch/",
		Action:      "archive",
		Retention:   time.Hour,
		Destination: dst,
	}); err != nil {
		t.Fatalf("AddPolicy() error: %v", err)
	}

	go lm.Run(src)

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("Run() did not archive the stale object within 2s")
		case <-tick.C:
			exists, _ := dst.Exists(context.Background(), "runarch/old.txt")
			if exists {
				// Verify data integrity.
				r, err := dst.Get("runarch/old.txt")
				if err != nil {
					t.Fatalf("Get() from archive destination error: %v", err)
				}
				data, _ := io.ReadAll(r)
				r.Close()
				if !bytes.Equal(data, []byte("content")) {
					t.Fatalf("archived data = %q, want %q", data, "content")
				}
				return
			}
		}
	}
}
