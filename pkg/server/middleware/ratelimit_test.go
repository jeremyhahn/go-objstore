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

package middleware

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestGinMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("allows requests within limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 10,
			Burst:             20,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		router := gin.New()
		router.Use(limiter.GinMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		// Make a few requests - should all succeed
		for i := 0; i < 5; i++ {
			req := httptest.NewRequest("GET", "/test", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
		}
	})

	t.Run("blocks requests exceeding limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		router := gin.New()
		router.Use(limiter.GinMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		// First 2 requests should succeed (burst)
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest("GET", "/test", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
		}

		// Next request should be rate limited
		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.Contains(t, w.Header().Get("Retry-After"), "1")
	})

	t.Run("per-IP rate limiting", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             1,
			PerIP:             true,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		router := gin.New()
		router.Use(limiter.GinMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		// Request from first IP
		req1 := httptest.NewRequest("GET", "/test", nil)
		req1.RemoteAddr = "192.168.1.1:1234"
		w1 := httptest.NewRecorder()
		router.ServeHTTP(w1, req1)
		assert.Equal(t, http.StatusOK, w1.Code)

		// Second request from same IP should be rate limited
		req2 := httptest.NewRequest("GET", "/test", nil)
		req2.RemoteAddr = "192.168.1.1:1234"
		w2 := httptest.NewRecorder()
		router.ServeHTTP(w2, req2)
		assert.Equal(t, http.StatusTooManyRequests, w2.Code)

		// Request from different IP should succeed
		req3 := httptest.NewRequest("GET", "/test", nil)
		req3.RemoteAddr = "192.168.1.2:1234"
		w3 := httptest.NewRecorder()
		router.ServeHTTP(w3, req3)
		assert.Equal(t, http.StatusOK, w3.Code)
	})

	t.Run("uses default config when nil", func(t *testing.T) {
		limiter := NewRateLimiter(nil, adapters.NewDefaultLogger())
		defer limiter.Stop()

		router := gin.New()
		router.Use(limiter.GinMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestUnaryInterceptor(t *testing.T) {
	t.Run("allows requests within limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 100,
			Burst:             200,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		interceptor := limiter.UnaryInterceptor()

		handler := func(ctx context.Context, req any) (any, error) {
			return "success", nil
		}

		info := &grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Method",
		}

		// Make a few requests - should all succeed
		for i := 0; i < 5; i++ {
			resp, err := interceptor(context.Background(), nil, info, handler)
			assert.NoError(t, err)
			assert.Equal(t, "success", resp)
		}
	})

	t.Run("blocks requests exceeding limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		interceptor := limiter.UnaryInterceptor()

		handler := func(ctx context.Context, req any) (any, error) {
			return "success", nil
		}

		info := &grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Method",
		}

		// First 2 requests should succeed
		for i := 0; i < 2; i++ {
			resp, err := interceptor(context.Background(), nil, info, handler)
			assert.NoError(t, err)
			assert.Equal(t, "success", resp)
		}

		// Next request should be rate limited
		_, err := interceptor(context.Background(), nil, info, handler)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Contains(t, st.Message(), "rate limit exceeded")
	})
}

func TestStreamInterceptor(t *testing.T) {
	t.Run("allows streams within limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 100,
			Burst:             200,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		interceptor := limiter.StreamInterceptor()

		handler := func(srv any, stream grpc.ServerStream) error {
			return nil
		}

		info := &grpc.StreamServerInfo{
			FullMethod: "/test.Service/StreamMethod",
		}

		mockStream := &mockServerStream{ctx: context.Background()}

		// Make a few requests - should all succeed
		for i := 0; i < 5; i++ {
			err := interceptor(nil, mockStream, info, handler)
			assert.NoError(t, err)
		}
	})

	t.Run("blocks streams exceeding limit", func(t *testing.T) {
		limiter := NewRateLimiter(&RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}, adapters.NewDefaultLogger())
		defer limiter.Stop()

		interceptor := limiter.StreamInterceptor()

		handler := func(srv any, stream grpc.ServerStream) error {
			return nil
		}

		info := &grpc.StreamServerInfo{
			FullMethod: "/test.Service/StreamMethod",
		}

		mockStream := &mockServerStream{ctx: context.Background()}

		// First 2 requests should succeed
		for i := 0; i < 2; i++ {
			err := interceptor(nil, mockStream, info, handler)
			assert.NoError(t, err)
		}

		// Next request should be rate limited
		err := interceptor(nil, mockStream, info, handler)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
	})
}

// TestDeprecatedRateLimitShims verifies the deprecated 0.1.0 constructors
// (RateLimitMiddleware, RateLimitUnaryInterceptor, RateLimitStreamInterceptor)
// keep enforcing limits for backward compatibility.
func TestDeprecatedRateLimitShims(t *testing.T) {
	gin.SetMode(gin.TestMode)
	config := &RateLimitConfig{RequestsPerSecond: 1, Burst: 1, PerIP: false}
	logger := adapters.NewDefaultLogger()

	t.Run("RateLimitMiddleware", func(t *testing.T) {
		router := gin.New()
		router.Use(RateLimitMiddleware(config, logger))
		router.GET("/test", func(c *gin.Context) { c.JSON(200, gin.H{"message": "success"}) })

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("GET", "/test", nil))
		assert.Equal(t, http.StatusOK, w.Code)

		w = httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("GET", "/test", nil))
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
	})

	t.Run("RateLimitUnaryInterceptor", func(t *testing.T) {
		interceptor := RateLimitUnaryInterceptor(config, logger)
		handler := func(ctx context.Context, req any) (any, error) { return "success", nil }
		info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

		resp, err := interceptor(context.Background(), nil, info, handler)
		assert.NoError(t, err)
		assert.Equal(t, "success", resp)

		_, err = interceptor(context.Background(), nil, info, handler)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("RateLimitStreamInterceptor", func(t *testing.T) {
		interceptor := RateLimitStreamInterceptor(config, logger)
		handler := func(srv any, stream grpc.ServerStream) error { return nil }
		info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}
		stream := &mockServerStream{ctx: context.Background()}

		assert.NoError(t, interceptor(nil, stream, info, handler))
		assert.Equal(t, codes.ResourceExhausted, status.Code(interceptor(nil, stream, info, handler)))
	})
}

func TestDefaultRateLimitConfig(t *testing.T) {
	config := DefaultRateLimitConfig()
	assert.NotNil(t, config)
	assert.Equal(t, float64(100), config.RequestsPerSecond)
	assert.Equal(t, 200, config.Burst)
	assert.False(t, config.PerIP)
}

// mockServerStream is a mock implementation of grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

// TestRateLimiterStopTerminatesSweep verifies that Stop terminates the
// background eviction goroutine and is safe to call repeatedly.
func TestRateLimiterStopTerminatesSweep(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 10,
		Burst:             10,
		PerIP:             true,
	}, adapters.NewDefaultLogger())

	done := make(chan struct{})
	go func() {
		// sweepLoop selects on stopCh; after Stop it must return promptly.
		limiter.sweepLoop()
		close(done)
	}()

	limiter.Stop()
	limiter.Stop() // idempotent

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweepLoop did not terminate after Stop()")
	}
}

// TestRateLimiterEvictIdleAtomicTimestamps verifies eviction works with the
// atomic last-seen timestamps and that active clients are retained.
func TestRateLimiterEvictIdleAtomicTimestamps(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 10,
		Burst:             10,
		PerIP:             true,
	}, adapters.NewDefaultLogger())
	defer limiter.Stop()

	limiter.AllowKey("stale-client")
	limiter.AllowKey("fresh-client")

	// Age the stale client past the TTL.
	limiter.mu.RLock()
	limiter.clients["stale-client"].lastSeen.Store(time.Now().Add(-2 * idleClientTTL).UnixNano())
	limiter.mu.RUnlock()

	limiter.evictIdle()

	limiter.mu.RLock()
	defer limiter.mu.RUnlock()
	if _, ok := limiter.clients["stale-client"]; ok {
		t.Error("stale client should have been evicted")
	}
	if _, ok := limiter.clients["fresh-client"]; !ok {
		t.Error("fresh client should have been retained")
	}
}

// TestRateLimiterConcurrentAccess exercises the hot path under -race.
func TestRateLimiterConcurrentAccess(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 1000,
		Burst:             1000,
		PerIP:             true,
	}, adapters.NewDefaultLogger())
	defer limiter.Stop()

	var wg sync.WaitGroup
	keys := []string{"a", "b", "c"}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				limiter.AllowKey(keys[(i+j)%len(keys)])
			}
		}(i)
	}
	// Concurrent eviction sweeps.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			limiter.evictIdle()
		}
	}()
	wg.Wait()
}

// TestGRPCRateLimitPerPeer verifies that per-IP mode keys gRPC requests by the
// peer address, so distinct peers get independent buckets.
func TestGRPCRateLimitPerPeer(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
	}, adapters.NewDefaultLogger())
	defer limiter.Stop()

	interceptor := limiter.UnaryInterceptor()
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/objstore.v1.ObjectStore/Get"}

	peerCtx := func(addr string) context.Context {
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			t.Fatalf("resolve %s: %v", addr, err)
		}
		return peer.NewContext(context.Background(), &peer.Peer{Addr: tcpAddr})
	}

	ctxA := peerCtx("10.0.0.1:1111")
	ctxB := peerCtx("10.0.0.2:2222")

	// Peer A exhausts its burst of 1.
	if _, err := interceptor(ctxA, nil, info, handler); err != nil {
		t.Fatalf("first request from peer A should pass: %v", err)
	}
	if _, err := interceptor(ctxA, nil, info, handler); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("second request from peer A should be rate limited, got %v", err)
	}

	// Peer B has its own bucket and must still pass.
	if _, err := interceptor(ctxB, nil, info, handler); err != nil {
		t.Errorf("first request from peer B should pass despite peer A exhaustion: %v", err)
	}
}

// TestUnaryAndStreamShareBuckets verifies that interceptors from one
// RateLimiter draw from the same buckets.
func TestUnaryAndStreamShareBuckets(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             false,
	}, adapters.NewDefaultLogger())
	defer limiter.Stop()

	unary := limiter.UnaryInterceptor()
	stream := limiter.StreamInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/objstore.v1.ObjectStore/Get"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	if _, err := unary(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("first unary request should pass: %v", err)
	}

	// The shared global bucket is now exhausted; the stream interceptor must deny.
	streamErr := stream(nil, &mockServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/objstore.v1.ObjectStore/List"},
		func(srv any, ss grpc.ServerStream) error { return nil })
	if status.Code(streamErr) != codes.ResourceExhausted {
		t.Errorf("stream request should share the exhausted bucket, got %v", streamErr)
	}
}
