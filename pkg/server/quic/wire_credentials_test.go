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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func postArchive(t *testing.T, settings map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	handler, storage := setupTestHandler(t)
	if err := storage.PutWithMetadata(context.Background(), "test-key", strings.NewReader("data"), &common.Metadata{ContentType: "text/plain"}); err != nil {
		t.Fatalf("store test data: %v", err)
	}
	body, _ := json.Marshal(map[string]any{
		"key":                  "test-key",
		"destination_type":     wireprobe.Backend,
		"destination_settings": settings,
	})
	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func TestArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	w := postArchive(t, map[string]string{"accessKey": "AK-quic", "secretKey": "SK-quic", "region": "eu-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	probe.AssertCredential(t, "AK-quic", "SK-quic", map[string]string{"region": "eu-1"})
	if strings.Contains(w.Body.String(), "SK-quic") {
		t.Error("response echoes the credential")
	}
}

func TestArchiveRefusesTokenSettings(t *testing.T) {
	probe := wireprobe.Register(t)
	w := postArchive(t, map[string]string{"sessionToken": "tok-quic"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if probe.Calls() != 0 || strings.Contains(w.Body.String(), "tok-quic") {
		t.Errorf("configured %d times, or the response echoes the token", probe.Calls())
	}
}
