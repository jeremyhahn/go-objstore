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

package quic

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// allowLocalArchive is the wire option of a server that allows client-supplied
// local archive destinations; tests that archive to local on purpose use it.
var allowLocalArchive = factory.WireOptions{AllowLocalDestinations: true}

func serveJSON(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestArchiveRefusesClientLocalDestinationByDefault(t *testing.T) {
	handler, storage := setupTestHandler(t)
	if err := storage.Put("obj.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	dest := filepath.Join(t.TempDir(), "outside")

	w := serveJSON(t, handler, "/archive", map[string]any{
		"key":                  "obj.txt",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": dest},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("archive: status %d (%s), want 400", w.Code, w.Body.String())
	}

	w = serveJSON(t, handler, "/policies", map[string]any{
		"id":                   "p1",
		"action":               "archive",
		"retention_seconds":    60,
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": dest},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("add policy: status %d (%s), want 400", w.Code, w.Body.String())
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("refused calls must not create %s (stat: %v)", dest, statErr)
	}
}

func TestOptionsAllowClientLocalArchiveDestinations(t *testing.T) {
	if DefaultOptions().AllowClientLocalArchiveDestinations {
		t.Fatal("the default must refuse")
	}
	if !DefaultOptions().WithAllowClientLocalArchiveDestinations(true).AllowClientLocalArchiveDestinations {
		t.Fatal("the builder must set the option")
	}
}
