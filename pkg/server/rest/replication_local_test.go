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

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// realReplicationStorage serves a real PersistentReplicationManager, so the
// handler sees the manager's own errors.
type realReplicationStorage struct {
	*MockStorageWithReplication
	mgr common.ReplicationManager
}

func (s *realReplicationStorage) GetReplicationManager() (common.ReplicationManager, error) {
	return s.mgr, nil
}

// TestAddReplicationPolicy_LocalBackendRefusedByDefault checks a local
// source or destination is answered 400 by a default manager.
func TestAddReplicationPolicy_LocalBackendRefusedByDefault(t *testing.T) {
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
			storage := &realReplicationStorage{MockStorageWithReplication: NewMockStorageWithReplication(), mgr: mgr}
			router, _ := setupTestRouter(t, storage)

			body, _ := json.Marshal(AddReplicationPolicyRequest{
				ID:                   "p1",
				SourceBackend:        tc.src,
				SourceSettings:       map[string]string{"path": "/etc"},
				DestinationBackend:   tc.dst,
				DestinationSettings:  map[string]string{"path": "/etc"},
				CheckIntervalSeconds: 300,
			})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", w.Code, w.Body.String())
			}
			if _, err := mgr.GetPolicy("p1"); err == nil {
				t.Error("refused policy was stored")
			}
		})
	}
}
