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
	"sync"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
)

// TestAuthenticationUnaryInterceptorConcurrentNoBadLoggerRace fires many
// goroutines through the single interceptor closure returned by
// AuthenticationUnaryInterceptor to prove that the per-request logger enrichment
// no longer mutates the shared captured logger variable. Run with -race; must
// produce zero data-race reports.
func TestAuthenticationUnaryInterceptorConcurrentNoBadLoggerRace(t *testing.T) {
	authenticator := adapters.NewNoOpAuthenticator()
	logger := adapters.NewNoOpLogger()

	// interceptor is the single closure shared across all goroutines — the exact
	// scenario that triggered the race before the fix.
	interceptor := AuthenticationUnaryInterceptor(authenticator, logger)

	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Get"}
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = interceptor(context.Background(), "req", info, handler)
		}()
	}
	wg.Wait()
}

// TestAuthenticationStreamInterceptorConcurrentNoBadLoggerRace fires many
// goroutines through the single stream interceptor closure returned by
// AuthenticationStreamInterceptor to prove that the per-request logger enrichment
// no longer mutates the shared captured logger variable. Run with -race; must
// produce zero data-race reports.
func TestAuthenticationStreamInterceptorConcurrentNoBadLoggerRace(t *testing.T) {
	authenticator := adapters.NewNoOpAuthenticator()
	logger := adapters.NewNoOpLogger()

	interceptor := AuthenticationStreamInterceptor(authenticator, logger)

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/List"}
	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = interceptor(nil, &mockServerStream{ctx: context.Background()}, info, handler)
		}()
	}
	wg.Wait()
}
