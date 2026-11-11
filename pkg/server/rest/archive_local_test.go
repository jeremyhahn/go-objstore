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
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// allowLocalArchive is the wire option of a server that allows client-supplied
// local archive destinations; tests that archive to local on purpose use it.
var allowLocalArchive = factory.WireOptions{AllowLocalDestinations: true}

func postJSON(t *testing.T, router *gin.Engine, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestArchiveRefusesClientLocalDestinationByDefault(t *testing.T) {
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	srv, err := NewServer(storage, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	router := gin.New()
	router.POST("/archive", srv.Handler().Archive)
	router.POST("/policies", srv.Handler().AddPolicy)
	dest := filepath.Join(t.TempDir(), "outside")

	w := postJSON(t, router, "/archive", ArchiveRequest{
		Key:                 "obj.txt",
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dest},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("archive: status %d (%s), want 400", w.Code, w.Body.String())
	}

	w = postJSON(t, router, "/policies", AddPolicyRequest{
		ID:                  "p1",
		Action:              "archive",
		RetentionSeconds:    60,
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dest},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("add policy: status %d (%s), want 400", w.Code, w.Body.String())
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("refused calls must not create %s (stat: %v)", dest, statErr)
	}
}

func TestNewServerPassesAllowClientLocalArchiveDestinations(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.AllowClientLocalArchiveDestinations = true
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	srv, err := NewServer(storage, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if !srv.Handler().wireOpts.AllowLocalDestinations {
		t.Fatal("the setting must reach the handler")
	}
}
