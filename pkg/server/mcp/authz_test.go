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
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	"github.com/sourcegraph/jsonrpc2"
	"google.golang.org/grpc/metadata"
)

// readerOnlyAuth authenticates every request as a principal with only the
// "reader" role.
type readerOnlyAuth struct{}

func (readerOnlyAuth) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerOnlyAuth) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerOnlyAuth) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

// newMCPHandlerWithAuthz builds the authenticated + authorized HTTP handler
// chain (the same chain wired by startHTTP) for the given auth/authz.
func newMCPHandlerWithAuthz(t *testing.T, auth adapters.Authenticator, authz adapters.Authorizer) http.Handler {
	t.Helper()
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	server.config.Authenticator = auth
	server.config.Authorizer = authz
	return server.authenticationMiddleware(NewHTTPHandler(server))
}

func mcpToolCall(tool string) string {
	return `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + tool + `","arguments":{"key":"k1","data":"hello","prefix":""}}}`
}

// TestMCPAuthorizationBackwardCompat verifies the default (NoOp) authorizer
// allows tool calls.
func TestMCPAuthorizationBackwardCompat(t *testing.T) {
	handler := newMCPHandlerWithAuthz(t, adapters.NewNoOpAuthenticator(), adapters.NewNoOpAuthorizer())

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(mcpToolCall("objstore_put")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code == http.StatusForbidden {
		t.Errorf("default authorizer forbade objstore_put, got 403")
	}
}

// TestMCPAuthorizationEnforced verifies a restrictive RBAC authorizer permits
// read-ish tools and forbids mutating/admin tools with HTTP 403.
func TestMCPAuthorizationEnforced(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	handler := newMCPHandlerWithAuthz(t, readerOnlyAuth{}, authz)

	do := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w.Code
	}

	t.Run("allowed read tool", func(t *testing.T) {
		if code := do(mcpToolCall("objstore_get")); code == http.StatusForbidden {
			t.Errorf("objstore_get should be allowed for reader, got 403")
		}
	})

	t.Run("allowed list tool", func(t *testing.T) {
		if code := do(mcpToolCall("objstore_list")); code == http.StatusForbidden {
			t.Errorf("objstore_list should be allowed for reader, got 403")
		}
	})

	t.Run("denied write tool", func(t *testing.T) {
		if code := do(mcpToolCall("objstore_put")); code != http.StatusForbidden {
			t.Errorf("objstore_put should be forbidden for reader, got %d", code)
		}
	})

	t.Run("denied delete tool", func(t *testing.T) {
		if code := do(mcpToolCall("objstore_delete")); code != http.StatusForbidden {
			t.Errorf("objstore_delete should be forbidden for reader, got %d", code)
		}
	})

	t.Run("denied admin tool", func(t *testing.T) {
		if code := do(mcpToolCall("objstore_add_policy")); code != http.StatusForbidden {
			t.Errorf("objstore_add_policy should be forbidden for reader, got %d", code)
		}
	})
}

// TestMCPStdioRateLimit verifies stdio requests are rejected with the shared
// rate-limited code once the bucket is exhausted.
func TestMCPStdioRateLimit(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	server.config.EnableRateLimit = true
	server.config.RateLimitConfig = &middleware.RateLimitConfig{RequestsPerSecond: 1, Burst: 1}
	server.rateLimiter = middleware.NewRateLimiter(server.config.RateLimitConfig, server.config.Logger)
	defer server.rateLimiter.Stop()
	handler := NewRPCHandler(server)

	req := &jsonrpc2.Request{Method: "ping"}

	if _, err := handler.Handle(context.Background(), nil, req); err != nil {
		t.Fatalf("first request should pass: %v", err)
	}

	_, err := handler.Handle(context.Background(), nil, req)
	rpcErr, ok := err.(*jsonrpc2.Error)
	if !ok || rpcErr.Code != jsonrpc.CodeRateLimited {
		t.Errorf("second request should be rate limited with %d, got %v", jsonrpc.CodeRateLimited, err)
	}
}

// TestMCPStdioAuthzDenialUsesForbiddenCode verifies that stdio-mode
// authorization denials surface as the implementation-defined forbidden code
// (-32001), not as a malformed-request error (-32600).
func TestMCPStdioAuthzDenialUsesForbiddenCode(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	server.config.EnforceStdioAuthz = true
	// Deny everything: RBAC map with no role grants.
	server.config.Authorizer = adapters.NewRBACAuthorizer(map[string][]string{})
	handler := NewRPCHandler(server)

	params := json.RawMessage(`{"name":"objstore_put","arguments":{"key":"k1","data":"hello"}}`)
	req := &jsonrpc2.Request{Method: methodToolsCall, Params: &params}

	_, err := handler.Handle(context.Background(), nil, req)
	if err == nil {
		t.Fatal("expected authorization denial")
	}
	rpcErr, ok := err.(*jsonrpc2.Error)
	if !ok {
		t.Fatalf("expected *jsonrpc2.Error, got %T: %v", err, err)
	}
	if rpcErr.Code != ErrCodeForbidden {
		t.Errorf("authz denial code = %d, want %d (ErrCodeForbidden)", rpcErr.Code, ErrCodeForbidden)
	}
}
