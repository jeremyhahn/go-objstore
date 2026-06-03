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

package unix

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc/metadata"
)

// readerAuthenticator authenticates every request as a principal holding only
// the "reader" role. Unix transport carries no credentials, so the principal is
// returned unconditionally from the HTTP entrypoint used by the handler.
type readerAuthenticator struct{}

func (readerAuthenticator) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerAuthenticator) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerAuthenticator) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

// TestUnixAuthorizationBackwardCompat verifies the default (NoOp) authorizer
// allows all operations, preserving prior behavior.
func TestUnixAuthorizationBackwardCompat(t *testing.T) {
	// createTestHandler initializes the facade and uses NoOp auth/authz.
	handler := createTestHandler(t, NewMockStorage())

	resp := handler.Handle(context.Background(), &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodList,
		ID:      1,
	})
	if resp.Error != nil {
		t.Errorf("default authorizer denied list: %+v", resp.Error)
	}
}

// TestUnixAuthorizationEnforced verifies a restrictive RBAC authorizer allows
// read/list and forbids write/delete/admin with the forbidden JSON-RPC code.
func TestUnixAuthorizationEnforced(t *testing.T) {
	// Initialize the facade via the shared helper, then build a handler with a
	// restrictive authorizer.
	_ = createTestHandler(t, NewMockStorage())
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	handler := NewHandler("", &mockLogger{}, readerAuthenticator{}, authz)

	allowed := func(t *testing.T, method string) {
		resp := handler.Handle(context.Background(), &Request{JSONRPC: jsonRPCVersion, Method: method, ID: 1})
		if resp.Error != nil && resp.Error.Code == ErrCodeForbidden {
			t.Errorf("%s should be allowed, got forbidden", method)
		}
	}
	denied := func(t *testing.T, method string) {
		resp := handler.Handle(context.Background(), &Request{JSONRPC: jsonRPCVersion, Method: method, ID: 1})
		if resp.Error == nil || resp.Error.Code != ErrCodeForbidden {
			t.Errorf("%s should be forbidden, got %+v", method, resp.Error)
		}
	}

	t.Run("allowed list", func(t *testing.T) { allowed(t, MethodList) })
	t.Run("denied delete", func(t *testing.T) { denied(t, MethodDelete) })
	t.Run("denied add_policy", func(t *testing.T) { denied(t, MethodAddPolicy) })
	t.Run("denied trigger_replication", func(t *testing.T) { denied(t, MethodTriggerRepl) })

	t.Run("health public", func(t *testing.T) {
		resp := handler.Handle(context.Background(), &Request{JSONRPC: jsonRPCVersion, Method: MethodHealth, ID: 1})
		if resp.Error != nil {
			t.Errorf("health must remain public, got %+v", resp.Error)
		}
	})
}
