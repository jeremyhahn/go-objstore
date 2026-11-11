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

package mcp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// realReplicationStorage serves a real PersistentReplicationManager, so the
// tool sees the manager's own errors.
type realReplicationStorage struct {
	*MockStorageWithReplication
	mgr common.ReplicationManager
}

func (s *realReplicationStorage) GetReplicationManager() (common.ReplicationManager, error) {
	return s.mgr, nil
}

// TestExecuteAddReplicationPolicy_LocalBackendRefusedByDefault checks a local
// source or destination is a tool error, an invalid argument, with a default
// manager.
func TestExecuteAddReplicationPolicy_LocalBackendRefusedByDefault(t *testing.T) {
	for _, tc := range []struct{ name, src, dst string }{
		{"local source", "local", "memory"},
		{"local destination", "memory", "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, err := replication.NewPersistentReplicationManager(nil,
				filepath.Join(t.TempDir(), "policies.json"), 0, nil, nil)
			if err != nil {
				t.Fatalf("new manager: %v", err)
			}
			executor := createTestToolExecutor(t,
				&realReplicationStorage{MockStorageWithReplication: NewMockStorageWithReplication(), mgr: mgr})

			_, err = executor.Execute(context.Background(), "objstore_add_replication_policy", map[string]any{
				"id":                   "p1",
				"source_backend":       tc.src,
				"source_settings":      map[string]any{"path": "/etc"},
				"destination_backend":  tc.dst,
				"destination_settings": map[string]any{"path": "/etc"},
				"check_interval":       float64(300),
			})
			if !errors.Is(err, common.ErrInvalidArgument) || !errors.Is(err, replication.ErrClientLocalBackend) {
				t.Fatalf("err = %v, want a tool error matching ErrClientLocalBackend and ErrInvalidArgument", err)
			}
			if _, err := mgr.GetPolicy("p1"); err == nil {
				t.Error("refused policy was stored")
			}
		})
	}
}
