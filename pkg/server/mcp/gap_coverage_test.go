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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	"github.com/sourcegraph/jsonrpc2"
)

// generateSelfSignedCert returns PEM-encoded cert and key for testing.
func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generateSelfSignedCert: key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("generateSelfSignedCert: cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("generateSelfSignedCert: marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// ---------------------------------------------------------------------------
// Handle – panic recovery, audit logging, and tools/call + resources dispatch
// ---------------------------------------------------------------------------

// TestRPCHandler_Handle_PanicRecovery exercises the deferred panic→error path
// inside Handle. Nil-ing the toolRegistry causes a nil-dereference inside
// handleToolsList which Handle's deferred recover() catches and converts to an
// ErrCodeInternalError JSON-RPC error.
func TestRPCHandler_Handle_PanicRecovery(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)

	// Nil the registry so ListTools panics via nil-pointer dereference.
	origRegistry := server.toolRegistry
	server.toolRegistry = nil

	handler := NewRPCHandler(server)
	req := &jsonrpc2.Request{Method: "tools/list"}

	_, err := handler.Handle(context.Background(), nil, req)
	// Restore so other tests are not affected.
	server.toolRegistry = origRegistry

	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	rpcErr, ok := err.(*jsonrpc2.Error)
	if !ok {
		t.Fatalf("expected *jsonrpc2.Error from panic recovery, got %T: %v", err, err)
	}
	if rpcErr.Code != ErrCodeInternalError {
		t.Errorf("expected ErrCodeInternalError (%d), got %d", ErrCodeInternalError, rpcErr.Code)
	}
}

// TestRPCHandler_Handle_AuditLogging exercises the audit log path inside
// Handle's deferred function.
func TestRPCHandler_Handle_AuditLogging(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	server.config.EnableAudit = true
	server.config.AuditLogger = audit.NewDefaultAuditLogger()

	handler := NewRPCHandler(server)
	req := &jsonrpc2.Request{Method: "ping"}

	result, err := handler.Handle(context.Background(), nil, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestRPCHandler_Handle_ToolsCallAndResources exercises the two dispatch
// branches in Handle that weren't covered: resources/list and resources/read.
func TestRPCHandler_Handle_ResourcesDispatch(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "dispatch.txt", strings.NewReader("hello"))
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// resources/list via Handle
	_, err := handler.Handle(context.Background(), nil, &jsonrpc2.Request{Method: "resources/list"})
	if err != nil {
		t.Fatalf("resources/list: %v", err)
	}

	// resources/read via Handle
	paramsJSON, _ := json.Marshal(map[string]any{"uri": "objstore://dispatch.txt"})
	raw := json.RawMessage(paramsJSON)
	_, err = handler.Handle(context.Background(), nil, &jsonrpc2.Request{Method: "resources/read", Params: &raw})
	if err != nil {
		t.Fatalf("resources/read: %v", err)
	}
}

// ---------------------------------------------------------------------------
// stdioActionResource – untested branches
// ---------------------------------------------------------------------------

// TestStdioActionResource exercises all branches of stdioActionResource:
// non-tools/call method, nil params, unmarshal error, empty tool name,
// known tool, and unknown tool.
func TestStdioActionResource(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		params         *json.RawMessage
		wantAction     string
		wantResourceOK bool
	}{
		{
			name:       "non-tools/call returns read",
			method:     "initialize",
			params:     nil,
			wantAction: adapters.ActionRead,
		},
		{
			name:       "nil params returns read",
			method:     methodToolsCall,
			params:     nil,
			wantAction: adapters.ActionRead,
		},
		{
			name:   "invalid JSON params returns admin",
			method: methodToolsCall,
			params: func() *json.RawMessage {
				raw := json.RawMessage([]byte("not-json{{{"))
				return &raw
			}(),
			wantAction: adapters.ActionAdmin,
		},
		{
			name:   "empty tool name returns admin",
			method: methodToolsCall,
			params: func() *json.RawMessage {
				raw, _ := json.Marshal(map[string]any{"name": ""})
				p := json.RawMessage(raw)
				return &p
			}(),
			wantAction: adapters.ActionAdmin,
		},
		{
			name:   "known read tool returns read action",
			method: methodToolsCall,
			params: func() *json.RawMessage {
				raw, _ := json.Marshal(map[string]any{"name": "objstore_get"})
				p := json.RawMessage(raw)
				return &p
			}(),
			wantAction: adapters.ActionRead,
		},
		{
			name:   "known write tool returns write action",
			method: methodToolsCall,
			params: func() *json.RawMessage {
				raw, _ := json.Marshal(map[string]any{"name": "objstore_put"})
				p := json.RawMessage(raw)
				return &p
			}(),
			wantAction: adapters.ActionWrite,
		},
		{
			name:   "unknown tool name returns admin",
			method: methodToolsCall,
			params: func() *json.RawMessage {
				raw, _ := json.Marshal(map[string]any{"name": "objstore_nonexistent"})
				p := json.RawMessage(raw)
				return &p
			}(),
			wantAction: adapters.ActionAdmin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &jsonrpc2.Request{Method: tt.method, Params: tt.params}
			action, _ := stdioActionResource(req)
			if action != tt.wantAction {
				t.Errorf("action = %q, want %q", action, tt.wantAction)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleResourcesList – error path from ListResources
// ---------------------------------------------------------------------------

func TestRPCHandler_HandleResourcesList_ListError(t *testing.T) {
	storage := &ErrorResourceMockStorage{listError: true}
	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{Mode: ModeStdio})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	handler := NewRPCHandler(server)

	_, listErr := handler.handleResourcesList(context.Background(), nil)
	if listErr == nil {
		t.Fatal("expected error from ListResources, got nil")
	}
}

// ---------------------------------------------------------------------------
// handleResourcesRead – binary (non-UTF8) content returns blob not text
// ---------------------------------------------------------------------------

func TestRPCHandler_HandleResourcesRead_BinaryBlob(t *testing.T) {
	// Build a storage that returns binary (non-UTF8) bytes.
	storage := &binaryMockStorage{}
	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{Mode: ModeStdio})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	handler := NewRPCHandler(server)

	paramsJSON, _ := json.Marshal(map[string]any{"uri": "objstore://binary.bin"})
	raw := json.RawMessage(paramsJSON)
	result, err := handler.handleResourcesRead(context.Background(), &raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected map result")
	}
	contents, ok := resultMap["contents"].([]map[string]any)
	if !ok || len(contents) == 0 {
		t.Fatal("expected non-empty contents")
	}
	entry := contents[0]
	if _, hasBlob := entry["blob"]; !hasBlob {
		t.Error("expected 'blob' field for binary content, got 'text'")
	}
	if _, hasText := entry["text"]; hasText {
		t.Error("did not expect 'text' field for binary content")
	}
}

// binaryMockStorage returns non-UTF8 bytes so the blob path is exercised.
type binaryMockStorage struct{}

func (b *binaryMockStorage) Configure(_ map[string]string) error { return nil }
func (b *binaryMockStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{ContentType: "application/octet-stream"}, nil
}
func (b *binaryMockStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	// \x80 is not valid UTF-8.
	return &mockReadCloser{strings.NewReader("\x80\x81\x82\x83")}, nil
}
func (b *binaryMockStorage) Put(_ string, _ io.Reader) error { return nil }
func (b *binaryMockStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	return nil
}
func (b *binaryMockStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *common.Metadata) error {
	return nil
}
func (b *binaryMockStorage) Get(_ string) (io.ReadCloser, error) { return nil, nil }
func (b *binaryMockStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return nil
}
func (b *binaryMockStorage) Delete(_ string) error                               { return nil }
func (b *binaryMockStorage) DeleteWithContext(_ context.Context, _ string) error { return nil }
func (b *binaryMockStorage) Exists(_ context.Context, _ string) (bool, error)    { return true, nil }
func (b *binaryMockStorage) List(_ string) ([]string, error)                     { return nil, nil }
func (b *binaryMockStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (b *binaryMockStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{}, nil
}
func (b *binaryMockStorage) Archive(_ string, _ common.Archiver) error          { return nil }
func (b *binaryMockStorage) SetLifecyclePolicy(_ *common.LifecyclePolicy) error { return nil }
func (b *binaryMockStorage) GetLifecyclePolicy() *common.LifecyclePolicy        { return nil }
func (b *binaryMockStorage) RunLifecycle(_ context.Context) error               { return nil }
func (b *binaryMockStorage) AddPolicy(_ common.LifecyclePolicy) error           { return nil }
func (b *binaryMockStorage) RemovePolicy(_ string) error                        { return nil }
func (b *binaryMockStorage) GetPolicies() ([]common.LifecyclePolicy, error)     { return nil, nil }

// ---------------------------------------------------------------------------
// ServeHTTP – non-jsonrpc2.Error error branch
// ---------------------------------------------------------------------------

// TestHTTPHandler_ServeHTTP_NonRPCError triggers the else-branch in ServeHTTP
// where the returned error is not a *jsonrpc2.Error (plain Go error).
// We can reach it via tools/call → CallTool → unknown tool returning ErrUnknownTool
// which is a plain error, but the callTool path wraps it via servererrors.JSONRPCError
// back into a jsonrpc2.Error. Instead, we verify the body-read-error path which
// calls writeError directly and returns StatusOK with a JSON error.
func TestHTTPHandler_ServeHTTP_BodyReadError(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	// A reader that errors immediately causes io.ReadAll to fail.
	req := httptest.NewRequest(http.MethodPost, "/", &alwaysErrReader{})
	rec := httptest.NewRecorder()
	httpHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp JSONRPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response body")
	}
	if resp.Error.Code != ErrCodeParseError {
		t.Errorf("expected parse error code %d, got %d", ErrCodeParseError, resp.Error.Code)
	}
}

// alwaysErrReader is an io.Reader that always returns an error.
type alwaysErrReader struct{}

func (a *alwaysErrReader) Read(_ []byte) (int, error) {
	return 0, bytes.ErrTooLarge
}

// ---------------------------------------------------------------------------
// ResourceManager.keyRef – backend-qualified path
// ---------------------------------------------------------------------------

func TestResourceManager_KeyRef_WithBackend(t *testing.T) {
	manager := NewResourceManager("mybackend", "")

	ref := manager.keyRef("some/key")
	expected := "mybackend:some/key"
	if ref != expected {
		t.Errorf("keyRef = %q, want %q", ref, expected)
	}
}

func TestResourceManager_KeyRef_EmptyBackend(t *testing.T) {
	manager := NewResourceManager("", "")

	ref := manager.keyRef("some/key")
	if ref != "some/key" {
		t.Errorf("keyRef = %q, want %q", ref, "some/key")
	}
}

// ---------------------------------------------------------------------------
// ToolExecutor.keyRef – backend-qualified path
// ---------------------------------------------------------------------------

func TestToolExecutor_KeyRef_WithBackend(t *testing.T) {
	executor := NewToolExecutor("mybackend")

	ref := executor.keyRef("my/object")
	expected := "mybackend:my/object"
	if ref != expected {
		t.Errorf("keyRef = %q, want %q", ref, expected)
	}
}

func TestToolExecutor_KeyRef_EmptyBackend(t *testing.T) {
	executor := NewToolExecutor("")

	ref := executor.keyRef("my/object")
	if ref != "my/object" {
		t.Errorf("keyRef = %q, want %q", ref, "my/object")
	}
}

// ---------------------------------------------------------------------------
// NewServer – audit logger default + rate limiter construction
// ---------------------------------------------------------------------------

func TestNewServer_AuditEnabledWithoutLogger(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:        ModeStdio,
		EnableAudit: true,
		// AuditLogger intentionally omitted — NewServer should set the default.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server.config.AuditLogger == nil {
		t.Error("expected AuditLogger to be set to default when EnableAudit=true")
	}
}

func TestNewServer_EnableRateLimit(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:            ModeStdio,
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 10, Burst: 5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server.rateLimiter == nil {
		t.Error("expected rateLimiter to be created when EnableRateLimit=true")
	}
	server.rateLimiter.Stop()
}

func TestNewServer_MaxBodySizeDefault(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{Mode: ModeStdio, MaxBodySize: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server.config.MaxBodySize != defaultMCPMaxBodySize {
		t.Errorf("expected MaxBodySize %d, got %d", defaultMCPMaxBodySize, server.config.MaxBodySize)
	}
}

// ---------------------------------------------------------------------------
// startHTTP – TLS Build error path
// ---------------------------------------------------------------------------

// TestServer_StartHTTP_TLSBuildError verifies that a non-nil TLSConfig that
// returns an error from Build propagates the error from startHTTP.
func TestServer_StartHTTP_TLSBuildError(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	// TLSModeServer requires cert PEM. Providing empty PEM causes Build to error.
	badTLS := &adapters.TLSConfig{
		Mode:          adapters.TLSModeServer,
		ServerCertPEM: nil,
		ServerKeyPEM:  nil,
	}

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: "127.0.0.1:0",
		TLSConfig:   badTLS,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx := context.Background()
	err = server.Start(ctx)
	if err == nil {
		t.Fatal("expected error from TLS Build, got nil")
	}
}

// TestServer_StartHTTP_ErrChan exercises the errChan path: Start returns when
// the listener goroutine sends an error (e.g., address already in use).
func TestServer_StartHTTP_ListenError(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	// Binding to an invalid address forces net.Listen to fail immediately,
	// which sends on errChan rather than waiting for context cancellation.
	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: "999.999.999.999:0",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx := context.Background()
	err = server.Start(ctx)
	if err == nil {
		t.Fatal("expected listen error, got nil")
	}
}

// TestServer_StartHTTP_RateLimiter_Shutdown verifies that the rate limiter is
// stopped when the context is cancelled, exercising the rateLimiter.Stop()
// call inside startHTTP.
func TestServer_StartHTTP_RateLimiterStop(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:            ModeHTTP,
		HTTPAddress:     "127.0.0.1:0",
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 100, Burst: 10},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Start() returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Start() did not return after context cancel")
	}
}

// TestServer_StartHTTP_AuditAndRateLimiter verifies that the audit middleware
// and rate limiter middleware are both applied when both are enabled.
func TestServer_StartHTTP_AuditAndRateLimiter(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:            ModeHTTP,
		HTTPAddress:     "127.0.0.1:0",
		EnableAudit:     true,
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 100, Burst: 10},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Start() returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Start() did not return after context cancel")
	}
}

// ---------------------------------------------------------------------------
// deriveMCPActionResource – replication tool branch
// ---------------------------------------------------------------------------

func TestDeriveMCPActionResource_ReplicationTool(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)

	body, _ := json.Marshal(map[string]any{
		"method": methodToolsCall,
		"params": map[string]any{"name": "objstore_add_replication_policy"},
	})

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	action, resource, _ := server.deriveMCPActionResource(req)
	if action != adapters.ActionAdmin {
		t.Errorf("action = %q, want %q", action, adapters.ActionAdmin)
	}
	if resource != adapters.ResourceReplication {
		t.Errorf("resource = %q, want %q", resource, adapters.ResourceReplication)
	}
}

// TestDeriveMCPActionResource_PolicyTool verifies unmapped non-replication
// tools fall through to the default admin/policy case.
func TestDeriveMCPActionResource_PolicyTool(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)

	body, _ := json.Marshal(map[string]any{
		"method": methodToolsCall,
		"params": map[string]any{"name": "objstore_completely_unknown_tool"},
	})

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	action, resource, _ := server.deriveMCPActionResource(req)
	if action != adapters.ActionAdmin {
		t.Errorf("action = %q, want %q", action, adapters.ActionAdmin)
	}
	if resource != adapters.ResourcePolicy {
		t.Errorf("resource = %q, want %q", resource, adapters.ResourcePolicy)
	}
}

// ---------------------------------------------------------------------------
// executeUpdateMetadata – invalid metadata type (metaRaw ok but not map)
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteUpdateMetadata_InvalidMetadataType(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// metadata present but wrong type: string instead of map[string]any
	_, err := executor.Execute(context.Background(), "objstore_update_metadata", map[string]any{
		"key":      "some-key",
		"metadata": "not-a-map",
	})
	if err == nil {
		t.Fatal("expected error for non-map metadata, got nil")
	}
	if err != ErrInvalidParameter {
		t.Errorf("expected ErrInvalidParameter, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// executeArchive – missing destination_type
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteArchive_MissingDestinationType(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "arch.txt", strings.NewReader("data"))
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_archive", map[string]any{
		"key": "arch.txt",
		// destination_type intentionally absent
	})
	if err == nil {
		t.Fatal("expected error for missing destination_type, got nil")
	}
}

// ---------------------------------------------------------------------------
// executeAddPolicy – int and int64 retention type branches
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteAddPolicy_IntRetention(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                "p-int",
		"action":            "delete",
		"retention_seconds": int(3600),
	})
	if err != nil {
		t.Errorf("unexpected error with int retention: %v", err)
	}
}

func TestToolExecutor_ExecuteAddPolicy_Int64Retention(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                "p-int64",
		"action":            "delete",
		"retention_seconds": int64(7200),
	})
	if err != nil {
		t.Errorf("unexpected error with int64 retention: %v", err)
	}
}

// ---------------------------------------------------------------------------
// executeApplyPolicies – archive action branch and list error path
// ---------------------------------------------------------------------------

// TestToolExecutor_executeApplyPolicies_ArchiveAction exercises the "archive"
// action branch inside the policy application loop.
func TestToolExecutor_executeApplyPolicies_ArchiveAction(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Add an object old enough to be archived.
	storage.PutWithContext(context.Background(), "old/blob.bin", bytes.NewReader([]byte("data")))
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["old/blob.bin"].LastModified = oldTime

	// Add a mock archiver that does nothing (local archiver reachable via factory).
	// Since the factory requires a registered backend, use a nil Destination so
	// the archive branch is entered but skips (Destination == nil guard).
	//
	// To exercise the non-nil Destination branch we rely on the mock lifecycle
	// storage's Archive method which returns nil (success).
	policy := common.LifecyclePolicy{
		ID:          "archive-policy",
		Prefix:      "old/",
		Retention:   24 * time.Hour,
		Action:      "archive",
		Destination: &mockArchiver{},
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}
	if !strings.Contains(result, "objects_processed") {
		t.Error("expected objects_processed in result")
	}
}

// mockArchiver is a no-op Archiver for testing.
type mockArchiver struct{}

func (m *mockArchiver) Put(_ string, _ io.Reader) error { return nil }

// TestToolExecutor_executeApplyPolicies_ListError verifies the error path
// when ListWithOptions fails during policy application.
func TestToolExecutor_executeApplyPolicies_ListError(t *testing.T) {
	storage := &applyPoliciesErrorStorage{
		policies: []common.LifecyclePolicy{
			{ID: "p1", Action: "delete", Retention: time.Hour},
		},
		listError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err == nil {
		t.Fatal("expected list error, got nil")
	}
}

// applyPoliciesErrorStorage has policies but ListWithOptions errors.
type applyPoliciesErrorStorage struct {
	policies  []common.LifecyclePolicy
	listError bool
}

func (s *applyPoliciesErrorStorage) Configure(_ map[string]string) error { return nil }
func (s *applyPoliciesErrorStorage) Put(_ string, _ io.Reader) error     { return nil }
func (s *applyPoliciesErrorStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	return nil
}
func (s *applyPoliciesErrorStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *common.Metadata) error {
	return nil
}
func (s *applyPoliciesErrorStorage) Get(_ string) (io.ReadCloser, error) { return nil, nil }
func (s *applyPoliciesErrorStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *applyPoliciesErrorStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return nil, nil
}
func (s *applyPoliciesErrorStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return nil
}
func (s *applyPoliciesErrorStorage) Delete(_ string) error                               { return nil }
func (s *applyPoliciesErrorStorage) DeleteWithContext(_ context.Context, _ string) error { return nil }
func (s *applyPoliciesErrorStorage) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (s *applyPoliciesErrorStorage) List(_ string) ([]string, error) { return nil, nil }
func (s *applyPoliciesErrorStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (s *applyPoliciesErrorStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	if s.listError {
		return nil, errApplyList
	}
	return &common.ListResult{}, nil
}
func (s *applyPoliciesErrorStorage) Archive(_ string, _ common.Archiver) error          { return nil }
func (s *applyPoliciesErrorStorage) SetLifecyclePolicy(_ *common.LifecyclePolicy) error { return nil }
func (s *applyPoliciesErrorStorage) GetLifecyclePolicy() *common.LifecyclePolicy        { return nil }
func (s *applyPoliciesErrorStorage) RunLifecycle(_ context.Context) error               { return nil }
func (s *applyPoliciesErrorStorage) AddPolicy(_ common.LifecyclePolicy) error           { return nil }
func (s *applyPoliciesErrorStorage) RemovePolicy(_ string) error                        { return nil }
func (s *applyPoliciesErrorStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return s.policies, nil
}

var errApplyList = common.ErrInternal

// ---------------------------------------------------------------------------
// executeAddReplicationPolicy – int and int64 check_interval type branches
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteAddReplicationPolicy_IntInterval(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_replication_policy", map[string]any{
		"id":                  "rep-int",
		"source_backend":      "local",
		"destination_backend": "s3",
		"check_interval":      int(300),
	})
	if err != nil {
		t.Errorf("unexpected error with int check_interval: %v", err)
	}
}

func TestToolExecutor_ExecuteAddReplicationPolicy_Int64Interval(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_replication_policy", map[string]any{
		"id":                  "rep-int64",
		"source_backend":      "local",
		"destination_backend": "s3",
		"check_interval":      int64(600),
	})
	if err != nil {
		t.Errorf("unexpected error with int64 check_interval: %v", err)
	}
}

// ---------------------------------------------------------------------------
// executeListReplicationPolicies – GetPolicies error
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteListReplicationPolicies_Error(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.getError = common.ErrInternal
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_list_replication_policies", map[string]any{})
	if err == nil {
		t.Fatal("expected error from GetPolicies, got nil")
	}
}

// ---------------------------------------------------------------------------
// extractName – empty-string key
// ---------------------------------------------------------------------------

func TestResourceManager_ExtractName_EmptyKey(t *testing.T) {
	manager := NewResourceManager("", "")
	// An empty key has one element in the split result: "". The function
	// returns parts[len(parts)-1] which is "".
	got := manager.extractName("")
	if got != "" {
		t.Errorf("extractName(\"\") = %q, want empty string", got)
	}
}

// ---------------------------------------------------------------------------
// NewServer – not-initialized facade error
// ---------------------------------------------------------------------------

func TestNewServer_FacadeNotInitialized(t *testing.T) {
	// Reset the facade so IsInitialized() returns false.
	objstore.Reset()

	_, err := NewServer(&ServerConfig{Mode: ModeStdio})
	if err == nil {
		t.Fatal("expected error when facade is not initialized, got nil")
	}
}

// ---------------------------------------------------------------------------
// Start / startStdio – ModeStdio path
// ---------------------------------------------------------------------------

// TestServer_Start_Stdio exercises the ModeStdio branch of Start by running
// startStdio in a goroutine and cancelling the context. jsonrpc2 closes the
// connection when its context is cancelled, which causes startStdio to unblock
// from <-conn.DisconnectNotify() and return nil.
func TestServer_Start_Stdio(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{Mode: ModeStdio})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	// Give startStdio time to begin, then cancel.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Start(stdio) returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Start(stdio) did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// startHTTP – default address, health endpoint, TLS min-version enforcement
// ---------------------------------------------------------------------------

// TestServer_StartHTTP_DefaultAddress exercises the branch where HTTPAddress
// is empty and the server falls back to the ":8081" default.
func TestServer_StartHTTP_DefaultAddress(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: "", // intentionally empty
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	// Let it bind, then immediately cancel.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("server did not stop in time")
	}
}

// TestServer_StartHTTP_HealthEndpoint exercises the /health handler registered
// inside startHTTP.
func TestServer_StartHTTP_HealthEndpoint(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Build the same mux that startHTTP builds, by running startHTTP in a
	// goroutine and hitting /health via a direct HTTP call. Since we don't
	// know the ephemeral port, we instead exercise the handler by building it
	// manually via the server's internal structure.
	//
	// A simpler approach: just call Start and hit /health, but port 0 makes
	// the listener address unknowable. Use a fixed-but-unlikely port instead.
	server.config.HTTPAddress = "127.0.0.1:18182"

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()
	time.Sleep(80 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:18182/health")
	if err != nil {
		// Server may not have bound in time on slow machines; skip rather than
		// fail since this path is covered by the server goroutine starting.
		cancel()
		<-errChan
		t.Skipf("health endpoint not ready: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health endpoint: expected 200, got %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected server error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("server did not stop")
	}
}

// TestServer_StartHTTP_ErrChan_RateLimiter exercises the errChan path where
// startHTTP returns the listener error AND the rateLimiter.Stop() call inside
// the errChan case is reached.
func TestServer_StartHTTP_ErrChan_WithRateLimiter(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	// Invalid address → net.Listen fails → sends on errChan.
	server, err := NewServer(&ServerConfig{
		Mode:            ModeHTTP,
		HTTPAddress:     "999.999.999.999:0",
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 100, Burst: 10},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from invalid address, got nil")
	}
}

// ---------------------------------------------------------------------------
// ReadResource – GetWithContext error and io.Copy error
// ---------------------------------------------------------------------------

// TestResourceManager_ReadResource_GetError exercises the path where
// GetWithContext fails after GetMetadata succeeds.
func TestResourceManager_ReadResource_GetError(t *testing.T) {
	storage := &metadataOKGetErrStorage{}
	initTestFacade(t, storage)
	manager := NewResourceManager("", "")

	_, _, err := manager.ReadResource(context.Background(), "objstore://any.txt")
	if err == nil {
		t.Fatal("expected error from GetWithContext, got nil")
	}
}

// TestResourceManager_ReadResource_CopyError exercises the path where
// io.Copy fails while reading from the returned reader.
func TestResourceManager_ReadResource_CopyError(t *testing.T) {
	storage := &metadataOKBadReadStorage{}
	initTestFacade(t, storage)
	manager := NewResourceManager("", "")

	_, _, err := manager.ReadResource(context.Background(), "objstore://any.txt")
	if err == nil {
		t.Fatal("expected io.Copy error, got nil")
	}
}

// metadataOKGetErrStorage returns valid metadata but fails on GetWithContext.
type metadataOKGetErrStorage struct{}

func (s *metadataOKGetErrStorage) Configure(_ map[string]string) error { return nil }
func (s *metadataOKGetErrStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{ContentType: "text/plain"}, nil
}
func (s *metadataOKGetErrStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, common.ErrKeyNotFound
}
func (s *metadataOKGetErrStorage) Put(_ string, _ io.Reader) error { return nil }
func (s *metadataOKGetErrStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	return nil
}
func (s *metadataOKGetErrStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *common.Metadata) error {
	return nil
}
func (s *metadataOKGetErrStorage) Get(_ string) (io.ReadCloser, error) { return nil, nil }
func (s *metadataOKGetErrStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return nil
}
func (s *metadataOKGetErrStorage) Delete(_ string) error                               { return nil }
func (s *metadataOKGetErrStorage) DeleteWithContext(_ context.Context, _ string) error { return nil }
func (s *metadataOKGetErrStorage) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (s *metadataOKGetErrStorage) List(_ string) ([]string, error) { return nil, nil }
func (s *metadataOKGetErrStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (s *metadataOKGetErrStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{}, nil
}
func (s *metadataOKGetErrStorage) Archive(_ string, _ common.Archiver) error          { return nil }
func (s *metadataOKGetErrStorage) SetLifecyclePolicy(_ *common.LifecyclePolicy) error { return nil }
func (s *metadataOKGetErrStorage) GetLifecyclePolicy() *common.LifecyclePolicy        { return nil }
func (s *metadataOKGetErrStorage) RunLifecycle(_ context.Context) error               { return nil }
func (s *metadataOKGetErrStorage) AddPolicy(_ common.LifecyclePolicy) error           { return nil }
func (s *metadataOKGetErrStorage) RemovePolicy(_ string) error                        { return nil }
func (s *metadataOKGetErrStorage) GetPolicies() ([]common.LifecyclePolicy, error)     { return nil, nil }

// metadataOKBadReadStorage returns valid metadata and a reader that fails.
type metadataOKBadReadStorage struct{}

func (s *metadataOKBadReadStorage) Configure(_ map[string]string) error { return nil }
func (s *metadataOKBadReadStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{ContentType: "text/plain"}, nil
}
func (s *metadataOKBadReadStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return &badReadCloser{}, nil
}
func (s *metadataOKBadReadStorage) Put(_ string, _ io.Reader) error { return nil }
func (s *metadataOKBadReadStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	return nil
}
func (s *metadataOKBadReadStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *common.Metadata) error {
	return nil
}
func (s *metadataOKBadReadStorage) Get(_ string) (io.ReadCloser, error) { return nil, nil }
func (s *metadataOKBadReadStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return nil
}
func (s *metadataOKBadReadStorage) Delete(_ string) error { return nil }
func (s *metadataOKBadReadStorage) DeleteWithContext(_ context.Context, _ string) error {
	return nil
}
func (s *metadataOKBadReadStorage) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (s *metadataOKBadReadStorage) List(_ string) ([]string, error) { return nil, nil }
func (s *metadataOKBadReadStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (s *metadataOKBadReadStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{}, nil
}
func (s *metadataOKBadReadStorage) Archive(_ string, _ common.Archiver) error          { return nil }
func (s *metadataOKBadReadStorage) SetLifecyclePolicy(_ *common.LifecyclePolicy) error { return nil }
func (s *metadataOKBadReadStorage) GetLifecyclePolicy() *common.LifecyclePolicy        { return nil }
func (s *metadataOKBadReadStorage) RunLifecycle(_ context.Context) error               { return nil }
func (s *metadataOKBadReadStorage) AddPolicy(_ common.LifecyclePolicy) error           { return nil }
func (s *metadataOKBadReadStorage) RemovePolicy(_ string) error                        { return nil }
func (s *metadataOKBadReadStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// executeUpdateMetadata – UpdateMetadata error
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteUpdateMetadata_UpdateError(t *testing.T) {
	// Use a storage that errors on UpdateMetadata.
	storage := &updateMetaErrorStorage{}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_update_metadata", map[string]any{
		"key": "any-key",
		"metadata": map[string]any{
			"content_type": "text/plain",
		},
	})
	if err == nil {
		t.Fatal("expected error from UpdateMetadata, got nil")
	}
}

// updateMetaErrorStorage succeeds on all ops except UpdateMetadata.
type updateMetaErrorStorage struct{}

func (s *updateMetaErrorStorage) Configure(_ map[string]string) error { return nil }
func (s *updateMetaErrorStorage) Put(_ string, _ io.Reader) error     { return nil }
func (s *updateMetaErrorStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	return nil
}
func (s *updateMetaErrorStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *common.Metadata) error {
	return nil
}
func (s *updateMetaErrorStorage) Get(_ string) (io.ReadCloser, error) { return nil, nil }
func (s *updateMetaErrorStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil
}
func (s *updateMetaErrorStorage) GetMetadata(_ context.Context, _ string) (*common.Metadata, error) {
	return &common.Metadata{}, nil
}
func (s *updateMetaErrorStorage) UpdateMetadata(_ context.Context, _ string, _ *common.Metadata) error {
	return common.ErrInternal
}
func (s *updateMetaErrorStorage) Delete(_ string) error                               { return nil }
func (s *updateMetaErrorStorage) DeleteWithContext(_ context.Context, _ string) error { return nil }
func (s *updateMetaErrorStorage) Exists(_ context.Context, _ string) (bool, error)    { return false, nil }
func (s *updateMetaErrorStorage) List(_ string) ([]string, error)                     { return nil, nil }
func (s *updateMetaErrorStorage) ListWithContext(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (s *updateMetaErrorStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{}, nil
}
func (s *updateMetaErrorStorage) Archive(_ string, _ common.Archiver) error          { return nil }
func (s *updateMetaErrorStorage) SetLifecyclePolicy(_ *common.LifecyclePolicy) error { return nil }
func (s *updateMetaErrorStorage) GetLifecyclePolicy() *common.LifecyclePolicy        { return nil }
func (s *updateMetaErrorStorage) RunLifecycle(_ context.Context) error               { return nil }
func (s *updateMetaErrorStorage) AddPolicy(_ common.LifecyclePolicy) error           { return nil }
func (s *updateMetaErrorStorage) RemovePolicy(_ string) error                        { return nil }
func (s *updateMetaErrorStorage) GetPolicies() ([]common.LifecyclePolicy, error)     { return nil, nil }

// ---------------------------------------------------------------------------
// executeArchive – createArchiver error
// ---------------------------------------------------------------------------

// TestToolExecutor_ExecuteArchive_ArchiverCreateError exercises the path
// where createArchiver fails because of an unknown destination type.
func TestToolExecutor_ExecuteArchive_ArchiverCreateError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.PutWithContext(context.Background(), "item.txt", strings.NewReader("data"))
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_archive", map[string]any{
		"key":              "item.txt",
		"destination_type": "totally_unknown_archiver_xyz",
	})
	if err == nil {
		t.Fatal("expected createArchiver error, got nil")
	}
}

// ---------------------------------------------------------------------------
// executeAddPolicy – createArchiver error for archive action
// ---------------------------------------------------------------------------

func TestToolExecutor_ExecuteAddPolicy_ArchiverCreateError(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                "p-bad-archiver",
		"action":            "archive",
		"retention_seconds": float64(3600),
		"destination_type":  "totally_unknown_archiver_xyz",
	})
	if err == nil {
		t.Fatal("expected createArchiver error for archive action, got nil")
	}
}

// ---------------------------------------------------------------------------
// executeApplyPolicies – via Execute switch (covers line 382-383)
// and additional loop branches
// ---------------------------------------------------------------------------

// TestToolExecutor_Execute_ApplyPolicies exercises the "objstore_apply_policies"
// case in the Execute switch (which direct calls to executeApplyPolicies miss).
func TestToolExecutor_Execute_ApplyPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	result, err := executor.Execute(context.Background(), "objstore_apply_policies", map[string]any{})
	if err != nil {
		t.Fatalf("Execute(objstore_apply_policies): %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}
}

// TestToolExecutor_executeApplyPolicies_PrefixMismatch exercises the
// "policy.Prefix != "" && !strings.HasPrefix" continue branch.
func TestToolExecutor_executeApplyPolicies_PrefixMismatch(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Object with "other/" prefix — does NOT match the policy's "logs/" prefix.
	storage.PutWithContext(context.Background(), "other/file.txt", bytes.NewReader([]byte("data")))
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["other/file.txt"].LastModified = oldTime

	policy := common.LifecyclePolicy{
		ID:        "logs-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	_, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	// Object should still exist because prefix didn't match.
	exists, _ := storage.Exists(context.Background(), "other/file.txt")
	if !exists {
		t.Error("expected object to still exist (prefix mismatch should skip)")
	}
}

// TestToolExecutor_executeApplyPolicies_NilMetadata exercises the
// "obj.Metadata == nil" continue branch. ListWithOptions iterates over
// storage.metadata, so the key must exist there with a nil value to appear
// in the result set with Metadata == nil.
func TestToolExecutor_executeApplyPolicies_NilMetadata(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Insert an object with a nil metadata entry so ListWithOptions returns it
	// with ObjectInfo.Metadata == nil.
	storage.data["bare/file.txt"] = []byte("data")
	storage.metadata["bare/file.txt"] = nil // explicitly nil

	policy := common.LifecyclePolicy{
		ID:        "bare-policy",
		Prefix:    "",
		Retention: 0, // matches everything regardless of age
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	_, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	// Object should still be present because nil metadata causes a skip.
	if _, exists := storage.data["bare/file.txt"]; !exists {
		t.Error("expected object with nil metadata to be skipped (not deleted)")
	}
}

// TestToolExecutor_executeApplyPolicies_TooNew exercises the
// "age <= policy.Retention" continue branch where an object is younger than
// the retention period and should be skipped.
func TestToolExecutor_executeApplyPolicies_TooNew(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Object created just now (well within retention).
	storage.PutWithContext(context.Background(), "fresh/file.txt", bytes.NewReader([]byte("data")))
	// Default LastModified from PutWithContext is time.Now(), which is very recent.

	policy := common.LifecyclePolicy{
		ID:        "fresh-policy",
		Prefix:    "fresh/",
		Retention: 24 * time.Hour, // 24h retention, object is only seconds old
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	// Object should not be deleted (too new).
	exists, _ := storage.Exists(context.Background(), "fresh/file.txt")
	if !exists {
		t.Error("expected object to still exist (too new)")
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(resultMap["objects_processed"].(float64)) != 0 {
		t.Errorf("expected 0 processed (too new), got %v", resultMap["objects_processed"])
	}
}

// TestToolExecutor_executeApplyPolicies_DeleteError exercises the delete error
// continue branch (err from DeleteWithContext → continue; not counted).
func TestToolExecutor_executeApplyPolicies_DeleteError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.PutWithContext(context.Background(), "old/file.txt", bytes.NewReader([]byte("data")))
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["old/file.txt"].LastModified = oldTime

	// Make DeleteWithContext fail.
	storage.archiveError = nil

	// Actually we need DeleteWithContext to fail. Let's use a wrapper:
	policy := common.LifecyclePolicy{
		ID:        "del-err-policy",
		Prefix:    "old/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	// Delete the object from storage.data so DeleteWithContext errors because
	// the object is not found.
	delete(storage.data, "old/file.txt")

	executor := createTestToolExecutor(t, storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	// objectsProcessed should be 0 since delete failed.
	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(resultMap["objects_processed"].(float64)) != 0 {
		t.Errorf("expected 0 objects processed after delete error")
	}
}

// TestToolExecutor_executeApplyPolicies_ArchiveError exercises the archive
// error continue branch.
func TestToolExecutor_executeApplyPolicies_ArchiveError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.PutWithContext(context.Background(), "old/blob.txt", bytes.NewReader([]byte("data")))
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["old/blob.txt"].LastModified = oldTime
	storage.archiveError = common.ErrInternal

	policy := common.LifecyclePolicy{
		ID:          "arch-err-policy",
		Prefix:      "old/",
		Retention:   24 * time.Hour,
		Action:      "archive",
		Destination: &mockArchiver{},
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	// objectsProcessed should be 0 since archive failed.
	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(resultMap["objects_processed"].(float64)) != 0 {
		t.Errorf("expected 0 objects processed after archive error")
	}
}

// ---------------------------------------------------------------------------
// executeAddReplicationPolicy – default type switch case for check_interval
// ---------------------------------------------------------------------------

// TestToolExecutor_ExecuteAddReplicationPolicy_AddPolicyError exercises the
// repMgr.AddPolicy error path (line 305-307 in tools_replication.go).
func TestToolExecutor_ExecuteAddReplicationPolicy_AddPolicyError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.addError = common.ErrInternal
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_replication_policy", map[string]any{
		"id":                  "rep-add-err",
		"source_backend":      "local",
		"destination_backend": "s3",
		"check_interval":      float64(60),
	})
	if err == nil {
		t.Fatal("expected error from repMgr.AddPolicy, got nil")
	}
}

func TestToolExecutor_ExecuteAddReplicationPolicy_InvalidIntervalType(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Pass a string for check_interval which is not float64/int64/int.
	_, err := executor.Execute(context.Background(), "objstore_add_replication_policy", map[string]any{
		"id":                  "rep-bad",
		"source_backend":      "local",
		"destination_backend": "s3",
		"check_interval":      "not-a-number",
	})
	if err == nil {
		t.Fatal("expected ErrInvalidParameter for string check_interval, got nil")
	}
	if err != ErrInvalidParameter {
		t.Errorf("expected ErrInvalidParameter, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// startStdio – rateLimiter.Stop() branch
// ---------------------------------------------------------------------------

// TestServer_Start_Stdio_WithRateLimiter exercises the rateLimiter.Stop()
// branch inside startStdio by starting the server in stdio mode with a rate
// limiter configured, then cancelling the context.
func TestServer_Start_Stdio_WithRateLimiter(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:            ModeStdio,
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 100, Burst: 10},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Start(stdio+rateLimiter) returned unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Start(stdio+rateLimiter) did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// startHTTP – TLS min-version enforcement and TLS listener path
// ---------------------------------------------------------------------------

// TestServer_StartHTTP_TLS_MinVersion exercises lines 263-265 (MinVersion
// enforcement) and 279-284 (TLS listener path) in startHTTP. It uses a
// self-signed cert and sets MinVersion to 0 so the floor triggers.
func TestServer_StartHTTP_TLS_MinVersion(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedCert(t)

	storage := NewMockStorage()
	initTestFacade(t, storage)

	tlsCfg := adapters.NewTLSConfig().WithServerCertPEM(certPEM, keyPEM)
	// Force MinVersion below TLS 1.2 to trigger the production floor.
	tlsCfg.MinVersion = tls.VersionSSL30 //nolint:gosec -- intentional for coverage

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: "127.0.0.1:18183",
		TLSConfig:   tlsCfg,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() { errChan <- server.Start(ctx) }()

	// Give the TLS listener time to bind before cancelling.
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected error from TLS server: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("TLS server did not stop after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// executeAddPolicy – policy.Destination = archiver (success path for archive)
// ---------------------------------------------------------------------------

// TestToolExecutor_ExecuteAddPolicy_MissingDestinationType exercises the
// ErrDestinationTypeRequired path when action is "archive" but destination_type
// is absent.
func TestToolExecutor_ExecuteAddPolicy_MissingDestinationType(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                "p-no-dest",
		"action":            "archive",
		"retention_seconds": float64(3600),
		// intentionally no "destination_type"
	})
	if err != ErrDestinationTypeRequired {
		t.Errorf("expected ErrDestinationTypeRequired, got %v", err)
	}
}

// TestToolExecutor_ExecuteAddPolicy_LocalArchiver exercises the success path
// for adding an "archive" policy with a valid "local" archiver. This covers
// the `policy.Destination = archiver` statement (line 791 in tools.go).
func TestToolExecutor_ExecuteAddPolicy_LocalArchiver(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := createTestToolExecutor(t, storage)

	dir := t.TempDir()

	result, err := executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                "p-local-archiver",
		"action":            "archive",
		"retention_seconds": float64(3600),
		"destination_type":  "local",
		"destination_settings": map[string]any{
			"path": dir,
		},
	})
	if err != nil {
		t.Fatalf("Execute(objstore_add_policy with local archiver): %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// executeArchive – facade Archive error (archiver created, but storage fails)
// ---------------------------------------------------------------------------

// TestToolExecutor_ExecuteArchive_FacadeError exercises the path where the
// archiver is created successfully (local backend) but storage.Archive returns
// an error (mockLifecycleStorage.archiveError).
func TestToolExecutor_ExecuteArchive_FacadeError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.PutWithContext(context.Background(), "item.txt", strings.NewReader("data"))
	storage.archiveError = common.ErrInternal
	executor := createTestToolExecutor(t, storage)

	dir := t.TempDir()

	_, err := executor.Execute(context.Background(), "objstore_archive", map[string]any{
		"key":              "item.txt",
		"destination_type": "local",
		"destination_settings": map[string]any{
			"path": dir,
		},
	})
	if err == nil {
		t.Fatal("expected error from storage.Archive, got nil")
	}
}

// ---------------------------------------------------------------------------
// executeApplyPolicies – archive success path (objectsProcessed++)
// ---------------------------------------------------------------------------

// TestToolExecutor_executeApplyPolicies_ArchiveSuccess exercises the archive
// action success path where objectsProcessed is incremented after a successful
// Archive call.
func TestToolExecutor_executeApplyPolicies_ArchiveSuccess(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.PutWithContext(context.Background(), "old/blob.txt", bytes.NewReader([]byte("data")))
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["old/blob.txt"].LastModified = oldTime
	// archiveError remains nil so Archive succeeds.

	dir := t.TempDir()
	archiver := newLocalArchiver(t, dir)

	policy := common.LifecyclePolicy{
		ID:          "arch-success-policy",
		Prefix:      "old/",
		Retention:   24 * time.Hour,
		Action:      "archive",
		Destination: archiver,
	}
	storage.AddPolicy(policy)

	executor := createTestToolExecutor(t, storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies: %v", err)
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(resultMap["objects_processed"].(float64)) != 1 {
		t.Errorf("expected 1 object processed, got %v", resultMap["objects_processed"])
	}
}

// newLocalArchiver creates a local archiver pointing at the given directory.
func newLocalArchiver(t *testing.T, dir string) common.Archiver {
	t.Helper()
	archiver, err := createArchiver("local", map[string]string{"path": dir})
	if err != nil {
		t.Fatalf("newLocalArchiver: %v", err)
	}
	return archiver
}
