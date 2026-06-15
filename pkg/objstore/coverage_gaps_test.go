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

package objstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// mockReplicationSetter implements both ReplicationCapable (GetReplicationManager)
// and ReplicationManagerSetter (SetReplicationManager) so EnableReplication can
// wire it up.
type mockReplicationSetter struct {
	*mockStorage
	rm common.ReplicationManager
}

func newMockReplicationSetter(name string) *mockReplicationSetter {
	return &mockReplicationSetter{
		mockStorage: newMockStorage(name),
	}
}

func (m *mockReplicationSetter) GetReplicationManager() (common.ReplicationManager, error) {
	if m.rm == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return m.rm, nil
}

func (m *mockReplicationSetter) SetReplicationManager(rm common.ReplicationManager) {
	m.rm = rm
}

// TestDefaultBackend_EmptyDefaultBackend exercises the ErrNoDefaultBackend branch
// in DefaultBackend(), which requires the facade to be initialized with a valid
// backend but the defaultBackend field to be empty string.
func TestDefaultBackend_EmptyDefaultBackend(t *testing.T) {
	Reset()
	// Force the facade into an initialized-but-no-default state by directly
	// constructing the facade after initialization with an auto-selected default
	// (which sets a non-empty default), then manipulating the internal field.
	// The only legal way without touching non-test source is to set defaultBackend
	// to "" via the exported Reset + re-init sequence is insufficient, so we
	// manipulate through the package-private façade struct directly — this is
	// an internal test (package objstore).
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	// Zero out the default backend name so the ErrNoDefaultBackend path fires.
	facade.mu.Lock()
	facade.defaultBackend = ""
	facade.mu.Unlock()

	_, err = DefaultBackend()
	if err != ErrNoDefaultBackend {
		t.Errorf("expected ErrNoDefaultBackend, got %v", err)
	}
}

// TestDefaultBackend_BackendNameMissing exercises the !ok branch in DefaultBackend()
// where the default backend name is set but that name is not in the backends map.
func TestDefaultBackend_BackendNameMissing(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	// Point defaultBackend at a name that doesn't exist in the map.
	facade.mu.Lock()
	facade.defaultBackend = "ghost"
	facade.mu.Unlock()

	_, err = DefaultBackend()
	if err == nil {
		t.Fatal("expected error for missing backend, got nil")
	}
}

// TestPutWithContext_GetStorageError exercises the getStorageForKey error path
// inside PutWithContext when the named backend does not exist.
func TestPutWithContext_GetStorageError(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()
	// "nosuchbackend:key" passes key-reference validation but the backend
	// "nosuchbackend" does not exist, so getStorageForKey returns an error.
	err = PutWithContext(ctx, "nosuchbackend:key.txt", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error for non-existent backend in PutWithContext, got nil")
	}
}

// TestPutWithMetadata_GetStorageError exercises the getStorageForKey error path
// inside PutWithMetadata.
func TestPutWithMetadata_GetStorageError(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()
	err = PutWithMetadata(ctx, "nosuchbackend:key.txt", strings.NewReader("data"), &common.Metadata{
		ContentType: "text/plain",
	})
	if err == nil {
		t.Fatal("expected error for non-existent backend in PutWithMetadata, got nil")
	}
}

// TestGetWithContext_GetStorageError exercises the getStorageForKey error path
// inside GetWithContext.
func TestGetWithContext_GetStorageError(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()
	_, err = GetWithContext(ctx, "nosuchbackend:key.txt")
	if err == nil {
		t.Fatal("expected error for non-existent backend in GetWithContext, got nil")
	}
}

// TestDeleteWithContext_GetStorageError exercises the getStorageForKey error path
// inside DeleteWithContext.
func TestDeleteWithContext_GetStorageError(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()
	err = DeleteWithContext(ctx, "nosuchbackend:key.txt")
	if err == nil {
		t.Fatal("expected error for non-existent backend in DeleteWithContext, got nil")
	}
}

// TestListWithContext_NamedBackend exercises the else branch in ListWithContext
// where a named backend is specified in the key reference.
func TestListWithContext_NamedBackend(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["docs/readme.md"] = []byte("read me")
	mock.objects["docs/guide.md"] = []byte("guide")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()

	// "local:docs/" hits the else branch (backend != "")
	keys, err := ListWithContext(ctx, "local:docs/")
	if err != nil {
		t.Fatalf("ListWithContext() with named backend error = %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

// TestListWithContext_NamedBackend_Error exercises the error return when the
// named backend in the key reference does not exist.
func TestListWithContext_NamedBackend_Error(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	ctx := context.Background()
	_, err = ListWithContext(ctx, "nosuchbackend:docs/")
	if err == nil {
		t.Fatal("expected error for non-existent backend in ListWithContext, got nil")
	}
}

// TestListWithOptions_DefaultBackendError exercises the error return in
// ListWithOptions when the facade is not initialized (default backend path).
func TestListWithOptions_DefaultBackendError(t *testing.T) {
	Reset()

	ctx := context.Background()
	opts := &common.ListOptions{Prefix: ""}
	_, err := ListWithOptions(ctx, "", opts)
	if err == nil {
		t.Fatal("expected error when facade not initialized, got nil")
	}
}

// TestEnableReplication exercises the full EnableReplication function through
// multiple branches: nil config defaults, non-empty backend name, zero interval,
// empty policy file, nil logger, nil auditLog, and the RunInBackground flag.
func TestEnableReplication(t *testing.T) {
	tmpDir := t.TempDir()

	local, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	Reset()
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": local,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	// local backend implements SetReplicationManager, so EnableReplication
	// should wire up a PersistentReplicationManager using all defaults.
	err = EnableReplication("local", nil)
	if err != nil {
		t.Errorf("EnableReplication() with nil config error = %v", err)
	}
}

// TestEnableReplication_WithConfig exercises EnableReplication with an explicit
// config, including a non-default policy file, interval, and RunInBackground.
func TestEnableReplication_WithConfig(t *testing.T) {
	tmpDir := t.TempDir()

	local, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	Reset()
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": local,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	cfg := &ReplicationConfig{
		PolicyFilePath:  tmpDir + "/repl-policies.json",
		Interval:        10 * time.Millisecond,
		RunInBackground: true,
	}
	err = EnableReplication("local", cfg)
	if err != nil {
		t.Errorf("EnableReplication() with explicit config error = %v", err)
	}
	// Give the background goroutine a moment so it starts and the test can
	// exit cleanly; the goroutine is non-blocking so this is safe.
	time.Sleep(20 * time.Millisecond)
}

// TestEnableReplication_DefaultBackend exercises the backendName == "" branch in
// EnableReplication, which falls through to DefaultBackend().
func TestEnableReplication_DefaultBackend(t *testing.T) {
	tmpDir := t.TempDir()

	local, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	Reset()
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": local,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	err = EnableReplication("", nil)
	if err != nil {
		t.Errorf("EnableReplication() with empty backendName error = %v", err)
	}
}

// TestEnableReplication_InvalidBackendName exercises the validation error branch
// for an invalid backend name.
func TestEnableReplication_InvalidBackendName(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	err = EnableReplication("INVALID_BACKEND", nil)
	if err == nil {
		t.Fatal("expected error for invalid backend name, got nil")
	}
}

// TestEnableReplication_BackendNotFound exercises the Backend() error path
// when the named backend is not registered in the facade.
func TestEnableReplication_BackendNotFound(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	err = EnableReplication("nosuchbackend", nil)
	if err == nil {
		t.Fatal("expected error for missing backend, got nil")
	}
}

// TestEnableReplication_NotSupported exercises the "backend does not support
// setting replication manager" branch using a plain mockStorage that does not
// implement ReplicationManagerSetter.
func TestEnableReplication_NotSupported(t *testing.T) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"plain": newMockStorage("plain"),
		},
		DefaultBackend: "plain",
	})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	defer Reset()

	err = EnableReplication("plain", nil)
	if err == nil {
		t.Fatal("expected error when backend does not support ReplicationManagerSetter, got nil")
	}
}

// TestEnableReplication_NotInitialized exercises the error path when the facade
// has not been initialized.
func TestEnableReplication_NotInitialized(t *testing.T) {
	Reset()

	err := EnableReplication("", nil)
	if err == nil {
		t.Fatal("expected ErrNotInitialized from EnableReplication when not initialized, got nil")
	}
}

// TestGetStorageForKey_NotInitialized exercises the early-return in
// getStorageForKey when the facade is not initialized.
func TestGetStorageForKey_NotInitialized(t *testing.T) {
	Reset()

	ctx := context.Background()
	// Any *WithContext function delegates to getStorageForKey; use PutWithContext.
	err := PutWithContext(ctx, "somekey.txt", strings.NewReader("data"))
	if err != ErrNotInitialized {
		t.Errorf("expected ErrNotInitialized from getStorageForKey, got %v", err)
	}
}
