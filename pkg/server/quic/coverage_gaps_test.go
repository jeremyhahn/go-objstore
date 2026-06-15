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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	replicationPkg "github.com/jeremyhahn/go-objstore/pkg/replication"
	"google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// Helpers shared across tests in this file
// ---------------------------------------------------------------------------

// gapHandler creates a Handler using a local-storage backend.
func gapHandler(t *testing.T) *Handler {
	t.Helper()
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// gapHandlerWithStorage creates a Handler using the provided storage backend.
func gapHandlerWithStorage(t *testing.T, storage common.Storage) *Handler {
	t.Helper()
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// ---------------------------------------------------------------------------
// NewHandler — not-initialized path (handlers.go:81-83)
// ---------------------------------------------------------------------------

func TestNewHandlerNotInitialized(t *testing.T) {
	objstore.Reset()
	_, err := NewHandler("", 1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err == nil {
		t.Fatal("expected error when objstore not initialized, got nil")
	}
}

// ---------------------------------------------------------------------------
// keyRef — backend-prefixed path (handlers.go:104)
// ---------------------------------------------------------------------------

func TestKeyRefWithBackendPrefix(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	h, err := NewHandler("mybackend", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.keyRef("k1"); got != "mybackend:k1" {
		t.Errorf("keyRef with backend = %q, want %q", got, "mybackend:k1")
	}
}

// ---------------------------------------------------------------------------
// setCORSHeaders — allowlisted origin branch (handlers.go:123-129)
// originAllowed — all branches (handlers.go:138-147)
// ---------------------------------------------------------------------------

func TestOriginAllowedFunction(t *testing.T) {
	cases := []struct {
		origin string
		list   []string
		want   bool
	}{
		{"", []string{"https://a.com"}, false},
		{"https://a.com", []string{"https://a.com"}, true},
		{"https://b.com", []string{"https://a.com"}, false},
		{"https://a.com", []string{"https://x.com", "https://a.com"}, true},
	}
	for _, c := range cases {
		if got := originAllowed(c.origin, c.list); got != c.want {
			t.Errorf("originAllowed(%q, %v) = %v, want %v", c.origin, c.list, got, c.want)
		}
	}
}

func TestCORSAllowlistedOrigin(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil,
		[]string{"https://allowed.example.com"})
	if err != nil {
		t.Fatal(err)
	}

	// OPTIONS triggers setCORSHeaders before the OPTIONS early return.
	req := httptest.NewRequest(http.MethodOptions, "/health", nil)
	req.Header.Set("Origin", "https://allowed.example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://allowed.example.com" {
		t.Errorf("ACAO = %q, want %q", got, "https://allowed.example.com")
	}
	if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("ACAC = %q, want %q", got, "true")
	}
}

func TestCORSNonAllowlistedOriginGetsNoHeader(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil,
		[]string{"https://allowed.example.com"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/health", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO should be empty for non-allowlisted origin, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// ServeHTTP — authentication failure path (handlers.go:196-204)
// ---------------------------------------------------------------------------

type gapFailAuthenticator struct{}

func (gapFailAuthenticator) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return nil, fmt.Errorf("invalid credentials")
}

func (gapFailAuthenticator) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return nil, fmt.Errorf("invalid credentials")
}

func (gapFailAuthenticator) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return nil, fmt.Errorf("invalid credentials")
}

func TestServeHTTPAuthFailure(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), gapFailAuthenticator{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/some-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for auth failure, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// ServeHTTP — route coverage for branches not hit by other tests
// (handlers.go:236-260: /exists/, /replication/trigger,
//  /replication/policies, /replication/policies/<id>, /replication/status/<id>)
// ---------------------------------------------------------------------------

func TestServeHTTPRouteExistsHead(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodHead, "/exists/somekey", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	// key absent → 404
	if w.Code != http.StatusNotFound {
		t.Errorf("HEAD /exists/somekey = %d, want 404", w.Code)
	}
}

func TestServeHTTPRouteArchiveBadType(t *testing.T) {
	h := gapHandler(t)
	body := `{"key":"k","destination_type":"__bad__"}`
	req := httptest.NewRequest(http.MethodPost, "/archive", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("POST /archive bad type = %d, want 400", w.Code)
	}
}

func TestServeHTTPRoutePoliciesApplyEmpty(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("POST /policies/apply (no policies) = %d, want 200", w.Code)
	}
}

func TestServeHTTPRoutePoliciesByIDNotFound(t *testing.T) {
	// mockErrorStorage returns ErrPolicyNotFound for RemovePolicy on missing IDs.
	mock := newMockErrorStorage()
	h := gapHandlerWithStorage(t, mock)
	req := httptest.NewRequest(http.MethodDelete, "/policies/nonexistent-id", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("DELETE /policies/nonexistent-id = %d, want 404", w.Code)
	}
}

func TestServeHTTPRouteReplicationTriggerNoSupport(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("POST /replication/trigger (no support) = %d, want 500", w.Code)
	}
}

func TestServeHTTPRouteReplicationPoliciesGetNoSupport(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("GET /replication/policies = %d, want 500", w.Code)
	}
}

func TestServeHTTPRouteReplicationPoliciesMethodNotAllowed(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/replication/policies", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /replication/policies = %d, want 405", w.Code)
	}
}

func TestServeHTTPRouteReplicationPoliciesByIDGetNoSupport(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/replication/policies/pol-1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("GET /replication/policies/pol-1 = %d, want 500", w.Code)
	}
}

func TestServeHTTPRouteReplicationPoliciesByIDMethodNotAllowed(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/replication/policies/pol-1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /replication/policies/pol-1 = %d, want 405", w.Code)
	}
}

func TestServeHTTPRouteReplicationStatusNoSupport(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/replication/status/pol-1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("GET /replication/status/pol-1 = %d, want 500", w.Code)
	}
}

func TestServeHTTPRouteDefault404(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/unknown/path", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("GET /unknown/path = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleHealth — json encode error path (handlers.go:322-324)
// The production handler calls json.NewEncoder(w).Encode(...) and logs on
// error. We test via a writer that fails after WriteHeader so Encode returns
// an error; the handler must not panic.
// ---------------------------------------------------------------------------

// gapBrokenWriter returns an error on every Write call after WriteHeader.
type gapBrokenWriter struct {
	httptest.ResponseRecorder
	wroteHeader bool
}

func (g *gapBrokenWriter) WriteHeader(code int) {
	g.wroteHeader = true
	g.ResponseRecorder.WriteHeader(code)
}

func (g *gapBrokenWriter) Write(_ []byte) (int, error) {
	if g.wroteHeader {
		return 0, fmt.Errorf("simulated write error")
	}
	return 0, nil
}

func TestHandleHealthJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	bw := &gapBrokenWriter{ResponseRecorder: *httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.handleHealth(bw, req)
	// Must not panic; error is logged internally.
}

// ---------------------------------------------------------------------------
// handlePut — json encode error path (handlers.go:405-408)
// ---------------------------------------------------------------------------

// gapWriteOnceWriter always errors on Write so json.Encoder.Encode returns an error.
// This exercises the "log error but response already started" paths.
type gapWriteOnceWriter struct {
	header http.Header
	code   int
}

func (g *gapWriteOnceWriter) Header() http.Header {
	if g.header == nil {
		g.header = make(http.Header)
	}
	return g.header
}

func (g *gapWriteOnceWriter) WriteHeader(code int) { g.code = code }

func (g *gapWriteOnceWriter) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("simulated write error")
}

func TestHandlePutJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader([]byte("data")))
	h.handlePut(bw, req, "test-key")
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleGet — info == nil path (handlers.go:422-424) via storage that
// returns nil, nil from GetMetadata
// ---------------------------------------------------------------------------

// gapNilMetaStorage is a minimal storage that always returns nil metadata.
type gapNilMetaStorage struct{ common.Storage }

func (g *gapNilMetaStorage) Configure(_ map[string]string) error { return nil }
func (g *gapNilMetaStorage) Put(key string, data io.Reader) error {
	_, _ = io.ReadAll(data)
	return nil
}
func (g *gapNilMetaStorage) PutWithContext(_ context.Context, key string, data io.Reader) error {
	return g.Put(key, data)
}
func (g *gapNilMetaStorage) PutWithMetadata(_ context.Context, _ string, data io.Reader, _ *common.Metadata) error {
	_, _ = io.ReadAll(data)
	return nil
}
func (g *gapNilMetaStorage) Get(_ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}
func (g *gapNilMetaStorage) GetWithContext(_ context.Context, key string) (io.ReadCloser, error) {
	return g.Get(key)
}
func (g *gapNilMetaStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return nil, nil // nil info, nil error
}
func (g *gapNilMetaStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return nil
}
func (g *gapNilMetaStorage) Delete(_ string) error                               { return nil }
func (g *gapNilMetaStorage) DeleteWithContext(_ context.Context, _ string) error { return nil }
func (g *gapNilMetaStorage) Exists(_ context.Context, _ string) (bool, error)    { return false, nil }
func (g *gapNilMetaStorage) List(_ string) ([]string, error)                     { return nil, nil }
func (g *gapNilMetaStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (g *gapNilMetaStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{}, nil
}
func (g *gapNilMetaStorage) Archive(_ string, _ common.Archiver) error      { return nil }
func (g *gapNilMetaStorage) GetPolicies() ([]common.LifecyclePolicy, error) { return nil, nil }
func (g *gapNilMetaStorage) AddPolicy(_ common.LifecyclePolicy) error       { return nil }
func (g *gapNilMetaStorage) RemovePolicy(_ string) error                    { return nil }

func TestHandleGetNilMetadataInfo(t *testing.T) {
	h := gapHandlerWithStorage(t, &gapNilMetaStorage{})
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("GET with nil metadata info = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleGet — get-object error after successful metadata (handlers.go:429-432)
// ---------------------------------------------------------------------------

type gapGetErrStorage struct {
	gapNilMetaStorage
	meta     *common.Metadata
	getError error
}

func (g *gapGetErrStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return g.meta, nil
}

func (g *gapGetErrStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, g.getError
}

func (g *gapGetErrStorage) Get(_ string) (io.ReadCloser, error) {
	return nil, g.getError
}

func TestHandleGetObjectReadError(t *testing.T) {
	st := &gapGetErrStorage{
		meta:     &common.Metadata{ContentType: "text/plain", Size: 4},
		getError: fmt.Errorf("read failed: %w", common.ErrUnavailable),
	}
	h := gapHandlerWithStorage(t, st)
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("GET with get error = %d, want 503", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleHead — info == nil path (handlers.go:490-493)
// ---------------------------------------------------------------------------

func TestHandleHeadNilMetadataInfo(t *testing.T) {
	h := gapHandlerWithStorage(t, &gapNilMetaStorage{})
	req := httptest.NewRequest(http.MethodHead, "/objects/some-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("HEAD with nil metadata info = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleExistsHead — all branches (handlers.go:533-555)
// ---------------------------------------------------------------------------

func TestHandleExistsHeadWrongMethod(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/exists/mykey", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /exists/ = %d, want 405", w.Code)
	}
}

func TestHandleExistsHeadEmptyKey(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodHead, "/exists/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("HEAD /exists/ (empty key) = %d, want 400", w.Code)
	}
}

func TestHandleExistsHeadBackendError(t *testing.T) {
	mock := newMockErrorStorage()
	mock.existsError = fmt.Errorf("backend down: %w", common.ErrUnavailable)
	h := gapHandlerWithStorage(t, mock)
	req := httptest.NewRequest(http.MethodHead, "/exists/mykey", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("HEAD /exists/ backend error = %d, want 503", w.Code)
	}
}

func TestHandleExistsHeadKeyNotFound(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodHead, "/exists/missing-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("HEAD /exists/missing-key = %d, want 404", w.Code)
	}
}

func TestHandleExistsHeadKeyFound(t *testing.T) {
	handler, storage := setupTestHandler(t)
	if err := storage.Put("present-key", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodHead, "/exists/present-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("HEAD /exists/present-key = %d, want 200", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleGetMetadata — all uncovered branches (handlers.go:562-604)
// ---------------------------------------------------------------------------

func TestHandleGetMetadataWrongMethod(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/metadata/mykey", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /metadata/ = %d, want 405", w.Code)
	}
}

func TestHandleGetMetadataEmptyKey(t *testing.T) {
	h := gapHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/metadata/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("GET /metadata/ (empty key) = %d, want 400", w.Code)
	}
}

func TestHandleGetMetadataNilInfo(t *testing.T) {
	h := gapHandlerWithStorage(t, &gapNilMetaStorage{})
	req := httptest.NewRequest(http.MethodGet, "/metadata/some-key", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("GET /metadata/ nil info = %d, want 404", w.Code)
	}
}

func TestHandleGetMetadataWithLastModifiedAndCustom(t *testing.T) {
	handler, storage := setupTestHandler(t)
	meta := &common.Metadata{
		ContentType:  "application/json",
		ETag:         "etag-abc",
		Size:         42,
		LastModified: time.Now(),
		Custom:       map[string]string{"owner": "alice"},
	}
	if err := storage.PutWithMetadata(context.Background(), "rich-key", bytes.NewReader([]byte("data")), meta); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metadata/rich-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /metadata/rich-key = %d, want 200", w.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["modified"] == nil || resp["modified"] == "" {
		t.Error("expected modified field to be populated")
	}
	if resp["metadata"] == nil {
		t.Error("expected metadata field to be populated")
	}
}

func TestHandleGetMetadataJSONEncodeError(t *testing.T) {
	handler, storage := setupTestHandler(t)
	meta := &common.Metadata{ContentType: "text/plain", Size: 4}
	if err := storage.PutWithMetadata(context.Background(), "enc-key", bytes.NewReader([]byte("data")), meta); err != nil {
		t.Fatal(err)
	}
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/metadata/enc-key", nil)
	handler.handleGetMetadata(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleList — json encode error path (handlers.go:656-659)
// ---------------------------------------------------------------------------

func TestHandleListJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/objects", nil)
	h.handleList(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleExists — error path (handlers.go:678-680)
// ---------------------------------------------------------------------------

func TestHandleExistsBackendError(t *testing.T) {
	mock := newMockErrorStorage()
	mock.existsError = fmt.Errorf("backend down: %w", common.ErrUnavailable)
	h := gapHandlerWithStorage(t, mock)
	req := httptest.NewRequest(http.MethodGet, "/objects/mykey?exists=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("GET ?exists= with backend error = %d, want 503", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleUpdateMetadata — json encode error path (handlers.go:719-721)
// ---------------------------------------------------------------------------

func TestHandleUpdateMetadataJSONEncodeError(t *testing.T) {
	handler, storage := setupTestHandler(t)
	if err := storage.Put("upd-key", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPatch, "/objects/upd-key",
		strings.NewReader(`{"content_type":"text/plain"}`))
	req.Header.Set("Content-Type", "application/json")
	handler.handleUpdateMetadata(bw, req, "upd-key")
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleGetPolicies — json encode error path (handlers.go:833-835)
// ---------------------------------------------------------------------------

func TestHandleGetPoliciesJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	h.handleGetPolicies(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleAddPolicy — archive action without destination_type (handlers.go:888-891)
// and json encode error path (handlers.go:914-916)
// ---------------------------------------------------------------------------

func TestHandleAddPolicyArchiveWithoutDestType(t *testing.T) {
	h := gapHandler(t)
	body := `{"id":"p1","action":"archive","retention_seconds":60}`
	req := httptest.NewRequest(http.MethodPost, "/policies", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("POST /policies archive without dest_type = %d, want 400", w.Code)
	}
}

func TestHandleAddPolicyJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	body := `{"id":"p-enc","action":"delete","retention_seconds":60}`
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/policies", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.handleAddPolicy(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handlePolicyByID — json encode error path (handlers.go:952-954)
// ---------------------------------------------------------------------------

func TestHandlePolicyByIDJSONEncodeError(t *testing.T) {
	h := gapHandler(t)

	// Add a policy so the DELETE can succeed and reach the encode step.
	addBody := `{"id":"pol-enc2","action":"delete","retention_seconds":60}`
	addReq := httptest.NewRequest(http.MethodPost, "/policies", strings.NewReader(addBody))
	addReq.Header.Set("Content-Type", "application/json")
	addW := httptest.NewRecorder()
	h.ServeHTTP(addW, addReq)
	if addW.Code != http.StatusCreated {
		t.Fatalf("setup: add policy = %d, body: %s", addW.Code, addW.Body.String())
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodDelete, "/policies/pol-enc2", nil)
	h.handlePolicyByID(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleApplyPolicies — list error branch (handlers.go:981-983)
// ---------------------------------------------------------------------------

// gapListErrStorage returns an error from ListWithOptions but has policies.
type gapListErrStorage struct {
	gapNilMetaStorage
	policies []common.LifecyclePolicy
	listErr  error
}

func (g *gapListErrStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return g.policies, nil
}

func (g *gapListErrStorage) AddPolicy(p common.LifecyclePolicy) error {
	g.policies = append(g.policies, p)
	return nil
}

func (g *gapListErrStorage) RemovePolicy(_ string) error { return nil }

func (g *gapListErrStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return nil, g.listErr
}

func TestHandleApplyPoliciesListError(t *testing.T) {
	storage := &gapListErrStorage{
		policies: []common.LifecyclePolicy{{ID: "p1", Action: "delete", Retention: time.Hour}},
		listErr:  fmt.Errorf("list failed: %w", common.ErrUnavailable),
	}
	h := gapHandlerWithStorage(t, storage)
	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("POST /policies/apply with list error = %d, want 503", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleApplyPolicies — json encode error path (handlers.go:1049-1051)
// ---------------------------------------------------------------------------

func TestHandleApplyPoliciesJSONEncodeError(t *testing.T) {
	h := gapHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	h.handleApplyPolicies(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// deriveActionResource — /metadata/<key> path (handlers.go:1065)
// ---------------------------------------------------------------------------

func TestDeriveActionResourceMetadataPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metadata/some/key", nil)
	action, resource := deriveActionResource(req)
	if action != adapters.ActionRead {
		t.Errorf("action = %q, want %q", action, adapters.ActionRead)
	}
	if resource != "some/key" {
		t.Errorf("resource = %q, want %q", resource, "some/key")
	}
}

// ---------------------------------------------------------------------------
// New — EnableAudit + EnableRateLimit middleware paths (server.go:66-75)
// ---------------------------------------------------------------------------

func TestNewServerWithAuditAndRateLimit(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithTLSConfig(tlsConfig).
		WithAudit(nil).
		WithRateLimit(nil)

	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New with audit+ratelimit = %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

// New — objstore not initialized → NewHandler error (server.go:57-59)
func TestNewServerNotInitialized(t *testing.T) {
	objstore.Reset()
	tlsConfig, _ := GenerateSelfSignedCert()
	opts := DefaultOptions().
		WithAddr(":0").
		WithTLSConfig(tlsConfig)

	_, err := New(opts)
	if err == nil {
		t.Fatal("expected error when objstore not initialized, got nil")
	}
}

// ---------------------------------------------------------------------------
// Start — ResolveUDPAddr error path (server.go:108-110)
// ---------------------------------------------------------------------------

func TestStartResolveUDPAddrError(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	// Port > 65535 is invalid; net.ResolveUDPAddr will return an error.
	opts := DefaultOptions().
		WithAddr("[::]:99999").
		WithTLSConfig(tlsConfig)

	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if startErr := srv.Start(); startErr == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		t.Error("expected Start to fail for invalid UDP address, got nil")
	}
}

// ---------------------------------------------------------------------------
// Stop — rateLimiter.Stop() branch (server.go:151-153)
// ---------------------------------------------------------------------------

func TestStopWithRateLimiter(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithTLSConfig(tlsConfig).
		WithRateLimit(nil)

	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		t.Logf("Stop returned error (acceptable): %v", err)
	}
}

// Stop — Shutdown timeout → Close fallback (server.go:156-163).
func TestStopShutdownContextExpired(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithTLSConfig(tlsConfig)

	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Already-cancelled context forces Shutdown to fail immediately,
	// exercising the Close() fallback branch.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = srv.Stop(ctx)
}

// ---------------------------------------------------------------------------
// Options.Validate — AdapterTLSConfig path (options.go:148)
// ---------------------------------------------------------------------------

func TestOptionsValidateAdapterTLSConfig(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)

	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	if err := GenerateAndSaveSelfSignedCert(certFile, keyFile); err != nil {
		t.Fatal(err)
	}

	adapterConfig := adapters.NewTLSConfig().WithServerCertFiles(certFile, keyFile)
	opts := DefaultOptions().
		WithAddr(":0").
		WithAdapterTLS(adapterConfig)

	if err := opts.Validate(); err != nil {
		t.Errorf("Validate with AdapterTLSConfig = %v, want nil", err)
	}
	if opts.TLSConfig == nil {
		t.Error("expected TLSConfig to be populated from AdapterTLSConfig")
	}
}

// ---------------------------------------------------------------------------
// SaveCertificateToPEM — cert file create error (tls.go:154-156),
//   non-RSA private key (tls.go:165-168), key file create error (tls.go:159-161)
// ---------------------------------------------------------------------------

func TestSaveCertPEMCertCreateError(t *testing.T) {
	err := SaveCertificateToPEM(
		"/nonexistent/dir/cert.pem",
		"/nonexistent/dir/key.pem",
		&tls.Certificate{
			Certificate: [][]byte{{0x30}}, // placeholder DER
			PrivateKey:  nil,
		})
	if err == nil {
		t.Error("expected error when cert file dir is invalid, got nil")
	}
}

func TestSaveCertPEMNonRSAKey(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	cfg, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	// Replace RSA private key with ECDSA key so the type assertion fails.
	cert := cfg.Certificates[0]
	cert.PrivateKey = ecKey

	tmpDir := t.TempDir()
	saveErr := SaveCertificateToPEM(
		filepath.Join(tmpDir, "cert.pem"),
		filepath.Join(tmpDir, "key.pem"),
		&cert)
	if saveErr == nil {
		t.Error("expected error for non-RSA private key, got nil")
	}
}

func TestSaveCertPEMKeyCreateError(t *testing.T) {
	cfg, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	cert := cfg.Certificates[0]
	tmpDir := t.TempDir()

	// certFile is valid but keyFile path is invalid.
	saveErr := SaveCertificateToPEM(
		filepath.Join(tmpDir, "cert.pem"),
		"/nonexistent/dir/key.pem",
		&cert)
	if saveErr == nil {
		t.Error("expected error when key file dir is invalid, got nil")
	}
}

// ---------------------------------------------------------------------------
// GenerateAndSaveSelfSignedCert — error propagation (tls.go:184-186)
// ---------------------------------------------------------------------------

func TestGenerateAndSaveSelfSignedCertBadPath(t *testing.T) {
	err := GenerateAndSaveSelfSignedCert("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for invalid file paths, got nil")
	}
}

// ---------------------------------------------------------------------------
// ServeHTTP — panic recovery paths (handlers.go:157-173)
// These lines are inside the defer func() in ServeHTTP. To hit them we need
// a real panic inside ServeHTTP routing. We achieve this by using a storage
// whose methods panic, which propagates up through the handler functions.
// ---------------------------------------------------------------------------

// gapPanicStorage panics when Get is called.
type gapPanicStorage struct {
	gapNilMetaStorage
}

func (g *gapPanicStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	panic("storage panic for coverage")
}

func TestServeHTTPPanicRecoveryBeforeHeaderWritten(t *testing.T) {
	// A panic before any headers are written must produce a 500 response.
	st := &gapPanicStorage{}
	initTestFacade(t, st)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/somekey", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("panic before header: code = %d, want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleGet — io.Copy error after successful WriteHeader (handlers.go:457-461)
// A panic in io.Copy will be recovered by ServeHTTP; alternatively we can
// trigger the error via a reader that returns an error after some data.
// ---------------------------------------------------------------------------

// gapErrorReader returns an error on the second Read call.
type gapErrorReader struct {
	data  []byte
	reads int
}

func (g *gapErrorReader) Read(p []byte) (int, error) {
	g.reads++
	if g.reads > 1 {
		return 0, fmt.Errorf("simulated read error")
	}
	n := copy(p, g.data)
	return n, nil
}

func (g *gapErrorReader) Close() error { return nil }

// gapGetErrorAfterWriteStorage returns valid metadata but an io that errors
// mid-read (after the first chunk).
type gapGetErrorAfterWriteStorage struct {
	gapNilMetaStorage
}

func (g *gapGetErrorAfterWriteStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{ContentType: "text/plain", Size: 100}, nil
}

func (g *gapGetErrorAfterWriteStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return &gapErrorReader{data: []byte("partial")}, nil
}

func (g *gapGetErrorAfterWriteStorage) Get(_ string) (io.ReadCloser, error) {
	return &gapErrorReader{data: []byte("partial")}, nil
}

func TestHandleGetIOCopyError(t *testing.T) {
	st := &gapGetErrorAfterWriteStorage{}
	initTestFacade(t, st)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	// io.Copy error after header is sent; handler logs and returns.
	h.ServeHTTP(w, req)
	// The response may be 200 (header sent before error) or 500 (if panic path).
	// We only care that the handler doesn't panic.
}

// ---------------------------------------------------------------------------
// handleExists — json encode error path (handlers.go:678-680)
// ---------------------------------------------------------------------------

// gapExistsOKStorage reports all keys as existing.
type gapExistsOKStorage struct {
	gapNilMetaStorage
}

func (g *gapExistsOKStorage) Exists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func TestHandleExistsJSONEncodeError(t *testing.T) {
	st := &gapExistsOKStorage{}
	initTestFacade(t, st)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/objects/somekey?exists=1", nil)
	h.handleExists(bw, req, "somekey")
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// handleArchive — json encode error path (handlers.go:776-778)
// This requires a valid archiver AND a successful Archive call.
// Use mockErrorStorage.Archive (which succeeds) with a broken writer.
// The archive route requires an archiver from the factory — glacier works.
// ---------------------------------------------------------------------------

func TestHandleArchiveJSONEncodeError(t *testing.T) {
	storage := newMockErrorStorage()
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Store an object to archive
	storage.objects["arch-key"] = []byte("data")
	storage.metadata["arch-key"] = &common.Metadata{Size: 4}

	// Use glacier archiver type which is registered at build time.
	body := `{"key":"arch-key","destination_type":"glacier","destination_settings":{"vaultName":"v","region":"us-east-1"}}`
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/archive", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.handleArchive(bw, req)
	// No panic; encode error is logged (if archive succeeds) or 400 (if glacier not available).
}

// ---------------------------------------------------------------------------
// handleAddPolicy — invalid archiver type for archive action (handlers.go:894-897)
// ---------------------------------------------------------------------------

func TestHandleAddPolicyInvalidArchiverType(t *testing.T) {
	h := gapHandler(t)
	body := `{"id":"p2","action":"archive","retention_seconds":60,"destination_type":"__unknown_archiver__"}`
	req := httptest.NewRequest(http.MethodPost, "/policies", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("POST /policies with unknown archiver = %d, want 400", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleApplyPolicies — json encode error path (handlers.go:1049-1051)
// needs policies to be non-empty AND all objects processed, reaching the
// encode step with a broken writer.
// ---------------------------------------------------------------------------

// gapApplyStorage has policies but no matching objects (nothing to process).
type gapApplyStorage struct {
	gapNilMetaStorage
	policies []common.LifecyclePolicy
}

func (g *gapApplyStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return g.policies, nil
}

func (g *gapApplyStorage) AddPolicy(p common.LifecyclePolicy) error {
	g.policies = append(g.policies, p)
	return nil
}

func (g *gapApplyStorage) RemovePolicy(_ string) error { return nil }

func (g *gapApplyStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{Objects: []*common.ObjectInfo{}}, nil
}

func TestHandleApplyPoliciesEncodeError(t *testing.T) {
	storage := &gapApplyStorage{
		policies: []common.LifecyclePolicy{{ID: "p1", Action: "delete", Retention: time.Hour}},
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	h.handleApplyPolicies(bw, req)
	// No panic; encode error is logged.
}

// ---------------------------------------------------------------------------
// Replication handler encode error paths — need MockStorageWithReplication.
// These tests are compiled only with the "local" build tag because
// MockStorageWithReplication is defined there.
// ---------------------------------------------------------------------------

func gapRepHandler(t *testing.T) *Handler {
	t.Helper()
	storage := NewMockStorageWithReplication()
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestHandleGetReplicationPoliciesEncodeError(t *testing.T) {
	h := gapRepHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	h.handleGetReplicationPolicies(bw, req)
}

func TestHandleAddReplicationPolicyEncodeError(t *testing.T) {
	h := gapRepHandler(t)
	body := `{"id":"rep1","source_backend":"local","destination_backend":"s3","check_interval":60,"enabled":true}`
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/replication/policies", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.handleAddReplicationPolicy(bw, req)
}

func TestHandleGetReplicationPolicyEncodeError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	// Pre-add a policy so the GET can find it.
	storage.repMgr.policies["rep-p1"] = common.ReplicationPolicy{
		ID:                 "rep-p1",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      60 * time.Second,
		Enabled:            true,
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/replication/policies/rep-p1", nil)
	h.handleGetReplicationPolicy(bw, req)
}

func TestHandleDeleteReplicationPolicyEncodeError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.policies["rep-p2"] = common.ReplicationPolicy{
		ID:                 "rep-p2",
		SourceBackend:      "local",
		DestinationBackend: "s3",
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodDelete, "/replication/policies/rep-p2", nil)
	h.handleDeleteReplicationPolicy(bw, req)
}

// ---------------------------------------------------------------------------
// handleTriggerReplication — parallel+all and parallel+policyID branches
// (handlers_replication.go:343-348)
// ---------------------------------------------------------------------------

func TestHandleTriggerReplicationParallelAll(t *testing.T) {
	h := gapRepHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/replication/trigger?parallel=true", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("POST /replication/trigger?parallel=true = %d, want 200", w.Code)
	}
}

func TestHandleTriggerReplicationParallelByPolicyID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.policies["sync-pol"] = common.ReplicationPolicy{
		ID:                 "sync-pol",
		SourceBackend:      "local",
		DestinationBackend: "s3",
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/replication/trigger?parallel=true&policy_id=sync-pol&worker_count=2", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("POST /replication/trigger?parallel=true&policy_id=sync-pol = %d, want 200", w.Code)
	}
}

func TestHandleTriggerReplicationEncodeError(t *testing.T) {
	h := gapRepHandler(t)
	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
	h.handleTriggerReplication(bw, req)
}

// ---------------------------------------------------------------------------
// handleGetReplicationStatus — manager doesn't implement status interface
// (handlers_replication.go:414-417)
// ---------------------------------------------------------------------------

// gapRepMgrNoStatus implements ReplicationManager but NOT GetReplicationStatus.
type gapRepMgrNoStatus struct{}

func (gapRepMgrNoStatus) AddPolicy(_ common.ReplicationPolicy) error { return nil }
func (gapRepMgrNoStatus) RemovePolicy(_ string) error                { return nil }
func (gapRepMgrNoStatus) GetPolicy(_ string) (*common.ReplicationPolicy, error) {
	return nil, common.ErrPolicyNotFound
}
func (gapRepMgrNoStatus) GetPolicies() ([]common.ReplicationPolicy, error) { return nil, nil }
func (gapRepMgrNoStatus) SyncAll(_ context.Context) (*common.SyncResult, error) {
	return &common.SyncResult{}, nil
}
func (gapRepMgrNoStatus) SyncPolicy(_ context.Context, _ string) (*common.SyncResult, error) {
	return nil, common.ErrPolicyNotFound
}
func (gapRepMgrNoStatus) SyncAllParallel(_ context.Context, _ int) (*common.SyncResult, error) {
	return &common.SyncResult{}, nil
}
func (gapRepMgrNoStatus) SyncPolicyParallel(_ context.Context, _ string, _ int) (*common.SyncResult, error) {
	return nil, common.ErrPolicyNotFound
}
func (gapRepMgrNoStatus) SetBackendEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (gapRepMgrNoStatus) SetSourceEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (gapRepMgrNoStatus) SetDestinationEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (gapRepMgrNoStatus) Run(_ context.Context) {}

// gapStorageNoStatusReplication uses gapRepMgrNoStatus (no GetReplicationStatus).
type gapStorageNoStatusReplication struct {
	MockStorageWithReplication
}

func (g *gapStorageNoStatusReplication) GetReplicationManager() (common.ReplicationManager, error) {
	return gapRepMgrNoStatus{}, nil
}

func TestHandleGetReplicationStatusNoInterface(t *testing.T) {
	storage := &gapStorageNoStatusReplication{}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/status/some-policy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("GET /replication/status/ without interface = %d, want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleGetReplicationStatus — encode error path (handlers_replication.go:445)
// ---------------------------------------------------------------------------

func TestHandleGetReplicationStatusEncodeError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	// Pre-seed a replication status so the lookup succeeds.
	storage.repMgr.replicationStatuses["pol-status"] = &replicationPkg.ReplicationStatus{
		PolicyID: "pol-status",
	}
	initTestFacade(t, storage)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	bw := &gapWriteOnceWriter{}
	req := httptest.NewRequest(http.MethodGet, "/replication/status/pol-status", nil)
	h.handleGetReplicationStatus(bw, req)
}

// ---------------------------------------------------------------------------
// ServeHTTP — panic AFTER header written (handlers.go:171-173).
// We need a reader that panics during io.Copy (after WriteHeader at line 456).
// The defer recover in ServeHTTP catches the panic; since rw.wroteHeader is
// true it re-panics with http.ErrAbortHandler. httptest.ResponseRecorder
// does not call http.ErrAbortHandler itself, so we wrap ServeHTTP in a
// recover to catch the re-panic and verify the path was hit.
// ---------------------------------------------------------------------------

// gapPanicReader always panics on Read — used to panic INSIDE io.Copy after
// WriteHeader has been called, hitting the "panic after header" branch.
type gapPanicReader struct{}

func (g *gapPanicReader) Read(_ []byte) (int, error) {
	panic("panic inside io.Copy for coverage")
}

func (g *gapPanicReader) Close() error { return nil }

// gapPanicAfterHeaderStorage returns valid metadata so handleGet proceeds to
// WriteHeader, then provides a panicking reader so io.Copy panics.
type gapPanicAfterHeaderStorage struct {
	gapNilMetaStorage
}

func (g *gapPanicAfterHeaderStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{ContentType: "text/plain", Size: 10}, nil
}

func (g *gapPanicAfterHeaderStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return &gapPanicReader{}, nil
}

func (g *gapPanicAfterHeaderStorage) Get(_ string) (io.ReadCloser, error) {
	return &gapPanicReader{}, nil
}

func TestServeHTTPPanicRecoveryAfterHeaderWritten(t *testing.T) {
	st := &gapPanicAfterHeaderStorage{}
	initTestFacade(t, st)
	h, err := NewHandler("", 1024*1024, 5*time.Second, 5*time.Second,
		adapters.NewNoOpLogger(), adapters.NewNoOpAuthenticator(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/somekey", nil)
	w := httptest.NewRecorder()

	// ServeHTTP re-panics with http.ErrAbortHandler after the header is written.
	// Wrap the call in a recover so the test itself does not fail from the panic.
	var caught interface{}
	func() {
		defer func() { caught = recover() }()
		h.ServeHTTP(w, req)
	}()

	if caught != http.ErrAbortHandler {
		t.Errorf("expected re-panic with http.ErrAbortHandler, got %v", caught)
	}
}

// ---------------------------------------------------------------------------
// server.go:108 — ListenUDP error path.
// ResolveUDPAddr succeeds for any syntactically valid address, but
// ListenUDP fails when the host IP is not assigned to any local interface.
// 192.168.255.254:0 resolves fine but cannot be bound on most systems.
// ---------------------------------------------------------------------------

func TestStartListenUDPError(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr("192.168.255.254:0").
		WithTLSConfig(tlsConfig)

	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if startErr := srv.Start(); startErr == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		t.Error("expected Start to fail for non-routable address, got nil")
	}
}

// server.go:126 — error inside Start goroutine (when Serve returns error
// while server is still marked running). This is triggered when the http3
// server's internal Serve call fails, which happens naturally when the UDP
// connection is closed underneath it — but the goroutine only logs if
// s.running.Load() is true at the time. We test this via TestServerStartStop
// which already covers the goroutine; the specific error-logging line requires
// timing that's non-deterministic. We leave the existing coverage here as-is
// rather than introducing a flaky sleep-based test.
// ---------------------------------------------------------------------------
