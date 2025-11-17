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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRateLimitMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("allows requests within limit", func(t *testing.T) {
		config := &RateLimitConfig{
			RequestsPerSecond: 10,
			Burst:             20,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		router := gin.New()
		router.Use(RateLimitMiddleware(config, logger))
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
		config := &RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		router := gin.New()
		router.Use(RateLimitMiddleware(config, logger))
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
		config := &RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             1,
			PerIP:             true,
		}
		logger := adapters.NewDefaultLogger()

		router := gin.New()
		router.Use(RateLimitMiddleware(config, logger))
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
		logger := adapters.NewDefaultLogger()

		router := gin.New()
		router.Use(RateLimitMiddleware(nil, logger))
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestRateLimitUnaryInterceptor(t *testing.T) {
	t.Run("allows requests within limit", func(t *testing.T) {
		config := &RateLimitConfig{
			RequestsPerSecond: 100,
			Burst:             200,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		interceptor := RateLimitUnaryInterceptor(config, logger)

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
		config := &RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		interceptor := RateLimitUnaryInterceptor(config, logger)

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

func TestRateLimitStreamInterceptor(t *testing.T) {
	t.Run("allows streams within limit", func(t *testing.T) {
		config := &RateLimitConfig{
			RequestsPerSecond: 100,
			Burst:             200,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		interceptor := RateLimitStreamInterceptor(config, logger)

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
		config := &RateLimitConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			PerIP:             false,
		}
		logger := adapters.NewDefaultLogger()

		interceptor := RateLimitStreamInterceptor(config, logger)

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
