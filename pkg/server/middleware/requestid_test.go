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
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestRequestIDMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("generates request ID when not present", func(t *testing.T) {
		router := gin.New()
		router.Use(RequestIDMiddleware())
		router.GET("/test", func(c *gin.Context) {
			requestID := GetRequestIDFromGinContext(c)
			assert.NotEmpty(t, requestID)
			c.JSON(200, gin.H{"request_id": requestID})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.NotEmpty(t, w.Header().Get(RequestIDHeader))
	})

	t.Run("uses existing request ID from header", func(t *testing.T) {
		existingID := "test-request-id-12345"

		router := gin.New()
		router.Use(RequestIDMiddleware())
		router.GET("/test", func(c *gin.Context) {
			requestID := GetRequestIDFromGinContext(c)
			assert.Equal(t, existingID, requestID)
			c.JSON(200, gin.H{"request_id": requestID})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set(RequestIDHeader, existingID)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, existingID, w.Header().Get(RequestIDHeader))
	})

	t.Run("adds request ID to response headers", func(t *testing.T) {
		router := gin.New()
		router.Use(RequestIDMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "success"})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		requestID := w.Header().Get(RequestIDHeader)
		assert.NotEmpty(t, requestID)
		assert.Greater(t, len(requestID), 10) // Should be a reasonably long ID
	})

	t.Run("adds request ID to context", func(t *testing.T) {
		router := gin.New()
		router.Use(RequestIDMiddleware())
		router.GET("/test", func(c *gin.Context) {
			requestIDFromGin := GetRequestIDFromGinContext(c)
			requestIDFromCtx := GetRequestIDFromContext(c.Request.Context())
			assert.NotEmpty(t, requestIDFromGin)
			assert.NotEmpty(t, requestIDFromCtx)
			assert.Equal(t, requestIDFromGin, requestIDFromCtx)
			c.JSON(200, gin.H{"message": "success"})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestGetRequestIDFromGinContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("returns empty string when request ID not set", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		requestID := GetRequestIDFromGinContext(c)
		assert.Empty(t, requestID)
	})

	t.Run("returns request ID when set", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		expectedID := "test-id-123"
		c.Set(RequestIDContextKey, expectedID)
		requestID := GetRequestIDFromGinContext(c)
		assert.Equal(t, expectedID, requestID)
	})

	t.Run("returns empty string for non-string value", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Set(RequestIDContextKey, 12345) // Not a string
		requestID := GetRequestIDFromGinContext(c)
		assert.Empty(t, requestID)
	})
}

func TestGetRequestIDFromContext(t *testing.T) {
	t.Run("returns empty string when request ID not set", func(t *testing.T) {
		ctx := context.Background()
		requestID := GetRequestIDFromContext(ctx)
		assert.Empty(t, requestID)
	})

	t.Run("returns request ID when set", func(t *testing.T) {
		expectedID := "test-id-456"
		ctx := context.WithValue(context.Background(), RequestIDContextKey, expectedID)
		requestID := GetRequestIDFromContext(ctx)
		assert.Equal(t, expectedID, requestID)
	})

	t.Run("returns empty string for non-string value", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), RequestIDContextKey, 12345)
		requestID := GetRequestIDFromContext(ctx)
		assert.Empty(t, requestID)
	})
}

func TestRequestIDUnaryInterceptor(t *testing.T) {
	t.Run("generates request ID when not present", func(t *testing.T) {
		interceptor := RequestIDUnaryInterceptor()

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			requestID := GetRequestIDFromContext(ctx)
			assert.NotEmpty(t, requestID)
			return "success", nil
		}

		info := &grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Method",
		}

		resp, err := interceptor(context.Background(), nil, info, handler)
		assert.NoError(t, err)
		assert.Equal(t, "success", resp)
		assert.True(t, handlerCalled)
	})

	t.Run("uses existing request ID from metadata", func(t *testing.T) {
		existingID := "grpc-test-id-12345"
		md := metadata.Pairs(GRPCRequestIDKey, existingID)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		interceptor := RequestIDUnaryInterceptor()

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			requestID := GetRequestIDFromContext(ctx)
			assert.Equal(t, existingID, requestID)
			return "success", nil
		}

		info := &grpc.UnaryServerInfo{
			FullMethod: "/test.Service/Method",
		}

		resp, err := interceptor(ctx, nil, info, handler)
		assert.NoError(t, err)
		assert.Equal(t, "success", resp)
		assert.True(t, handlerCalled)
	})
}

func TestRequestIDStreamInterceptor(t *testing.T) {
	t.Run("generates request ID when not present", func(t *testing.T) {
		interceptor := RequestIDStreamInterceptor()

		handlerCalled := false
		handler := func(srv any, stream grpc.ServerStream) error {
			handlerCalled = true
			requestID := GetRequestIDFromContext(stream.Context())
			assert.NotEmpty(t, requestID)
			return nil
		}

		info := &grpc.StreamServerInfo{
			FullMethod: "/test.Service/StreamMethod",
		}

		mockStream := &mockServerStream{ctx: context.Background()}
		err := interceptor(nil, mockStream, info, handler)
		assert.NoError(t, err)
		assert.True(t, handlerCalled)
	})

	t.Run("uses existing request ID from metadata", func(t *testing.T) {
		existingID := "grpc-stream-id-67890"
		md := metadata.Pairs(GRPCRequestIDKey, existingID)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		interceptor := RequestIDStreamInterceptor()

		handlerCalled := false
		handler := func(srv any, stream grpc.ServerStream) error {
			handlerCalled = true
			requestID := GetRequestIDFromContext(stream.Context())
			assert.Equal(t, existingID, requestID)
			return nil
		}

		info := &grpc.StreamServerInfo{
			FullMethod: "/test.Service/StreamMethod",
		}

		mockStream := &mockServerStream{ctx: ctx}
		err := interceptor(nil, mockStream, info, handler)
		assert.NoError(t, err)
		assert.True(t, handlerCalled)
	})
}

func TestGenerateRequestID(t *testing.T) {
	t.Run("generates unique IDs", func(t *testing.T) {
		id1 := generateRequestID()
		id2 := generateRequestID()
		assert.NotEmpty(t, id1)
		assert.NotEmpty(t, id2)
		assert.NotEqual(t, id1, id2)
	})

	t.Run("generates IDs of reasonable length", func(t *testing.T) {
		id := generateRequestID()
		assert.Greater(t, len(id), 10)
	})
}

func TestExtractRequestIDFromMetadata(t *testing.T) {
	t.Run("returns empty string when metadata not present", func(t *testing.T) {
		ctx := context.Background()
		requestID := extractRequestIDFromMetadata(ctx)
		assert.Empty(t, requestID)
	})

	t.Run("returns empty string when request ID not in metadata", func(t *testing.T) {
		md := metadata.Pairs("other-key", "other-value")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		requestID := extractRequestIDFromMetadata(ctx)
		assert.Empty(t, requestID)
	})

	t.Run("returns request ID from metadata", func(t *testing.T) {
		expectedID := "metadata-test-id"
		md := metadata.Pairs(GRPCRequestIDKey, expectedID)
		ctx := metadata.NewIncomingContext(context.Background(), md)
		requestID := extractRequestIDFromMetadata(ctx)
		assert.Equal(t, expectedID, requestID)
	})

	t.Run("returns first value when multiple IDs in metadata", func(t *testing.T) {
		md := metadata.Pairs(GRPCRequestIDKey, "id1", GRPCRequestIDKey, "id2")
		ctx := metadata.NewIncomingContext(context.Background(), md)
		requestID := extractRequestIDFromMetadata(ctx)
		assert.Equal(t, "id1", requestID)
	})
}

func TestRequestIDServerStream(t *testing.T) {
	t.Run("wraps context correctly", func(t *testing.T) {
		expectedID := "wrapped-stream-id"
		ctx := context.WithValue(context.Background(), RequestIDContextKey, expectedID)
		mockStream := &mockServerStream{ctx: context.Background()}

		wrappedStream := &requestIDServerStream{
			ServerStream: mockStream,
			ctx:          ctx,
		}

		assert.Equal(t, ctx, wrappedStream.Context())
		requestID := GetRequestIDFromContext(wrappedStream.Context())
		assert.Equal(t, expectedID, requestID)
	})
}
