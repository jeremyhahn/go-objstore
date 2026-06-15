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

package grpc

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ctxWithPrincipal(p adapters.Principal) context.Context {
	return context.WithValue(context.Background(), principalContextKey, p)
}

// TestAuthorizationUnaryInterceptor_NoOpAllowsAll verifies the default NoOp
// authorizer permits every method (backward compatible).
func TestAuthorizationUnaryInterceptor_NoOpAllowsAll(t *testing.T) {
	interceptor := AuthorizationUnaryInterceptor(adapters.NewNoOpAuthorizer(), adapters.NewNoOpLogger())
	ctx := ctxWithPrincipal(adapters.Principal{ID: "u"})

	called := false
	handler := func(context.Context, any) (any, error) { called = true; return "ok", nil }

	for _, method := range []string{
		"/objstore.ObjectStore/Get",
		"/objstore.ObjectStore/Put",
		"/objstore.ObjectStore/Delete",
		"/objstore.ObjectStore/AddPolicy",
	} {
		called = false
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		if err != nil {
			t.Errorf("NoOp authorizer denied %s: %v", method, err)
		}
		if !called {
			t.Errorf("handler not called for %s", method)
		}
	}
}

// TestAuthorizationUnaryInterceptor_RBACEnforced verifies a reader-only RBAC
// authorizer permits read methods and denies others with PermissionDenied.
func TestAuthorizationUnaryInterceptor_RBACEnforced(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	interceptor := AuthorizationUnaryInterceptor(authz, adapters.NewNoOpLogger())
	ctx := ctxWithPrincipal(adapters.Principal{ID: "r", Roles: []string{"reader"}})
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	t.Run("allowed read", func(t *testing.T) {
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/objstore.ObjectStore/Get"}, handler)
		if err != nil {
			t.Errorf("Get should be allowed: %v", err)
		}
	})

	t.Run("denied write", func(t *testing.T) {
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/objstore.ObjectStore/Put"}, handler)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("Put = %v, want PermissionDenied", err)
		}
	})

	t.Run("denied delete", func(t *testing.T) {
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/objstore.ObjectStore/Delete"}, handler)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("Delete = %v, want PermissionDenied", err)
		}
	})

	t.Run("denied admin", func(t *testing.T) {
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/objstore.ObjectStore/AddPolicy"}, handler)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("AddPolicy = %v, want PermissionDenied", err)
		}
	})

	t.Run("missing principal denied", func(t *testing.T) {
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/objstore.ObjectStore/Get"}, handler)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("missing principal = %v, want PermissionDenied", err)
		}
	})

	t.Run("health method public", func(t *testing.T) {
		called := false
		h := func(context.Context, any) (any, error) { called = true; return nil, nil }
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, h)
		if err != nil || !called {
			t.Errorf("health should bypass authz, err=%v called=%v", err, called)
		}
	})
}

// TestAuthorizationStreamInterceptor_RBACEnforced verifies stream authorization.
func TestAuthorizationStreamInterceptor_RBACEnforced(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	interceptor := AuthorizationStreamInterceptor(authz, adapters.NewNoOpLogger())
	ctx := ctxWithPrincipal(adapters.Principal{ID: "r", Roles: []string{"reader"}})
	handler := func(any, grpc.ServerStream) error { return nil }

	t.Run("allowed list", func(t *testing.T) {
		err := interceptor(nil, &mockServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/objstore.ObjectStore/List"}, handler)
		if err != nil {
			t.Errorf("List should be allowed: %v", err)
		}
	})

	t.Run("denied write", func(t *testing.T) {
		err := interceptor(nil, &mockServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/objstore.ObjectStore/Put"}, handler)
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("Put stream = %v, want PermissionDenied", err)
		}
	})
}
