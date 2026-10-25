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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func postArchive(t *testing.T, settings map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	storage := newMockLifecycleStorage()
	storage.objects["test-key"] = &mockObject{data: []byte("data"), metadata: &common.Metadata{Size: 4}}
	handler := newTestHandler(t, storage)
	router := gin.New()
	router.POST("/archive", handler.Archive)

	body, err := json.Marshal(ArchiveRequest{Key: "test-key", DestinationType: wireprobe.Backend, DestinationSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	w := postArchive(t, map[string]string{"accessKey": "AK-rest", "secretKey": "SK-rest", "region": "eu-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	probe.AssertCredential(t, "AK-rest", "SK-rest", map[string]string{"region": "eu-1"})
	if strings.Contains(w.Body.String(), "SK-rest") || strings.Contains(w.Body.String(), "AK-rest") {
		t.Error("response echoes the credential")
	}
}

func TestArchiveRefusesTokenSettings(t *testing.T) {
	probe := wireprobe.Register(t)
	w := postArchive(t, map[string]string{"sessionToken": "tok-rest"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	if probe.Calls() != 0 || strings.Contains(w.Body.String(), "tok-rest") {
		t.Errorf("configured %d times, or the response echoes the token", probe.Calls())
	}
}

func TestAddPolicyArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	handler := newTestHandler(t, newMockLifecycleStorage())
	router := gin.New()
	router.POST("/policies", handler.AddPolicy)

	body, _ := json.Marshal(AddPolicyRequest{
		ID: "p1", Action: "archive", RetentionSeconds: 60, DestinationType: wireprobe.Backend,
		DestinationSettings: map[string]string{"accessKey": "AK-rest", "secretKey": "SK-rest", "region": "eu-1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	probe.AssertCredential(t, "AK-rest", "SK-rest", map[string]string{"region": "eu-1"})
}
