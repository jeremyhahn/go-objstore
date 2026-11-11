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

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
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

// TestAuthorization_ObjstoreHealthAnyPrincipal covers #239: the objstore
// Health RPC is open to any authenticated principal, even one with no roles,
// while every other method, known or unknown, still goes through the
// authorizer.
func TestAuthorization_ObjstoreHealthAnyPrincipal(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	unary := AuthorizationUnaryInterceptor(authz, adapters.NewNoOpLogger())
	stream := AuthorizationStreamInterceptor(authz, adapters.NewNoOpLogger())
	noRoles := ctxWithPrincipal(adapters.Principal{ID: "nobody"})
	reader := ctxWithPrincipal(adapters.Principal{ID: "r", Roles: []string{"reader"}})
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	streamHandler := func(any, grpc.ServerStream) error { return nil }
	health := objstorepb.ObjectStore_Health_FullMethodName

	unaryCall := func(ctx context.Context, method string) error {
		_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}
	streamCall := func(ctx context.Context, method string) error {
		return stream(nil, &wrappedServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, streamHandler)
	}

	for name, call := range map[string]func(context.Context, string) error{"unary": unaryCall, "stream": streamCall} {
		t.Run(name+" no roles may call Health", func(t *testing.T) {
			if err := call(noRoles, health); err != nil {
				t.Errorf("Health with no roles = %v, want nil", err)
			}
		})
		t.Run(name+" no roles refused Put", func(t *testing.T) {
			if err := call(noRoles, objstorepb.ObjectStore_Put_FullMethodName); status.Code(err) != codes.PermissionDenied {
				t.Errorf("Put with no roles = %v, want PermissionDenied", err)
			}
		})
		t.Run(name+" no principal refused Health", func(t *testing.T) {
			if err := call(context.Background(), health); status.Code(err) != codes.PermissionDenied {
				t.Errorf("Health without principal = %v, want PermissionDenied", err)
			}
		})
		t.Run(name+" unknown method needs admin", func(t *testing.T) {
			if err := call(reader, "/objstore.v1.ObjectStore/NoSuchMethod"); status.Code(err) != codes.PermissionDenied {
				t.Errorf("unknown method = %v, want PermissionDenied", err)
			}
		})
		t.Run(name+" Health lookalikes still need admin", func(t *testing.T) {
			for _, m := range []string{
				"/other.v1.ObjectStore/Health",
				"/objstore.v1.ObjectStore/HealthCheck",
				"/evil/objstore.v1.ObjectStore/Health",
			} {
				if err := call(reader, m); status.Code(err) != codes.PermissionDenied {
					t.Errorf("%s = %v, want PermissionDenied", m, err)
				}
			}
		})
	}
}

// TestIsPublicAuthzMethod_ExactServicePrefix checks that only the standard
// health and reflection services skip authorization, matched by service
// prefix rather than by substring (#239).
func TestIsPublicAuthzMethod_ExactServicePrefix(t *testing.T) {
	cases := map[string]bool{
		"/grpc.health.v1.Health/Check":                                   true,
		"/grpc.health.v1.Health/Watch":                                   true,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      true,
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": true,
		"/objstore.v1.ObjectStore/Health":                                false,
		"/evil.grpc.health.v1.Health/Check":                              false,
		"/x/grpc.health.v1.Health/Check":                                 false,
		"/evil.grpc.reflection.v1.ServerReflection/ServerReflectionInfo": false,
		"/grpc.health.v1.HealthX/Check":                                  false,
		"/grpc.reflection.v1.Other/Method":                               false,
		"/grpc.reflection.foo/Bar":                                       false,
		"/grpc.reflection.v1.ServerReflectionX/ServerReflectionInfo":     false,
	}
	for m, want := range cases {
		if got := isPublicAuthzMethod(m); got != want {
			t.Errorf("isPublicAuthzMethod(%q) = %v, want %v", m, got, want)
		}
	}
}
