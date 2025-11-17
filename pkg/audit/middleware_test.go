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

package audit

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestAuditMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}
	auditLogger := NewAuditLogger(config)

	tests := []struct {
		name       string
		method     string
		path       string
		setupFunc  func(*gin.Context)
		statusCode int
		wantLogged bool
	}{
		{
			name:       "successful GET request",
			method:     http.MethodGet,
			path:       "/objects/test-key",
			statusCode: http.StatusOK,
			wantLogged: true,
		},
		{
			name:       "successful PUT request",
			method:     http.MethodPut,
			path:       "/objects/test-key",
			statusCode: http.StatusCreated,
			wantLogged: true,
		},
		{
			name:       "DELETE request",
			method:     http.MethodDelete,
			path:       "/objects/test-key",
			statusCode: http.StatusOK,
			wantLogged: true,
		},
		{
			name:       "failed request",
			method:     http.MethodGet,
			path:       "/objects/missing",
			statusCode: http.StatusNotFound,
			wantLogged: true,
		},
		{
			name:       "health check not logged",
			method:     http.MethodGet,
			path:       "/health",
			statusCode: http.StatusOK,
			wantLogged: false,
		},
		{
			name:   "request with principal",
			method: http.MethodGet,
			path:   "/objects/test-key",
			setupFunc: func(c *gin.Context) {
				c.Set("principal", adapters.Principal{
					ID:   "user123",
					Name: "john.doe",
				})
			},
			statusCode: http.StatusOK,
			wantLogged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			router := gin.New()
			router.Use(AuditMiddleware(auditLogger))

			router.Any(tt.path, func(c *gin.Context) {
				if tt.setupFunc != nil {
					tt.setupFunc(c)
				}
				c.Status(tt.statusCode)
			})

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			output := buf.String()
			if tt.wantLogged && output == "" {
				t.Error("Expected audit log output, got none")
			}
			if !tt.wantLogged && output != "" {
				t.Errorf("Expected no audit log output, got: %s", output)
			}
		})
	}
}

func TestGetAuditLogger(t *testing.T) {
	tests := []struct {
		name       string
		setupCtx   func() context.Context
		expectType string
	}{
		{
			name: "logger in context",
			setupCtx: func() context.Context {
				logger := NewDefaultAuditLogger()
				return context.WithValue(context.Background(), AuditLoggerKey, logger)
			},
			expectType: "*audit.DefaultAuditLogger",
		},
		{
			name: "no logger in context",
			setupCtx: func() context.Context {
				return context.Background()
			},
			expectType: "*audit.NoOpAuditLogger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			logger := GetAuditLogger(ctx)
			if logger == nil {
				t.Fatal("Expected non-nil logger")
			}
		})
	}
}

func TestGetRequestID(t *testing.T) {
	tests := []struct {
		name       string
		setupCtx   func() context.Context
		expectedID string
	}{
		{
			name: "request ID in context",
			setupCtx: func() context.Context {
				return context.WithValue(context.Background(), RequestIDKey, "req-123")
			},
			expectedID: "req-123",
		},
		{
			name: "no request ID in context",
			setupCtx: func() context.Context {
				return context.Background()
			},
			expectedID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			requestID := GetRequestID(ctx)
			if requestID != tt.expectedID {
				t.Errorf("Expected request ID %q, got %q", tt.expectedID, requestID)
			}
		})
	}
}

func TestAuditUnaryInterceptor(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}
	auditLogger := NewAuditLogger(config)

	interceptor := AuditUnaryInterceptor(auditLogger)

	tests := []struct {
		name       string
		method     string
		setupCtx   func(context.Context) context.Context
		handler    grpc.UnaryHandler
		wantLogged bool
		wantError  bool
	}{
		{
			name:   "successful request",
			method: "/ObjectStore/Put",
			setupCtx: func(ctx context.Context) context.Context {
				return ctx
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return "success", nil
			},
			wantLogged: true,
			wantError:  false,
		},
		{
			name:   "failed request",
			method: "/ObjectStore/Get",
			setupCtx: func(ctx context.Context) context.Context {
				return ctx
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return nil, grpc.Errorf(5, "not found")
			},
			wantLogged: true,
			wantError:  true,
		},
		{
			name:   "health check",
			method: "/ObjectStore/Health",
			setupCtx: func(ctx context.Context) context.Context {
				return ctx
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return "healthy", nil
			},
			wantLogged: false,
			wantError:  false,
		},
		{
			name:   "with principal",
			method: "/ObjectStore/Delete",
			setupCtx: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, "principal", adapters.Principal{
					ID:   "user123",
					Name: "john.doe",
				})
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return "deleted", nil
			},
			wantLogged: true,
			wantError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			ctx := context.Background()
			ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
			if tt.setupCtx != nil {
				ctx = tt.setupCtx(ctx)
			}

			info := &grpc.UnaryServerInfo{
				FullMethod: tt.method,
			}

			resp, err := interceptor(ctx, nil, info, tt.handler)

			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			output := buf.String()
			if tt.wantLogged && output == "" {
				t.Error("Expected audit log output, got none")
			}
			if !tt.wantLogged && output != "" {
				t.Errorf("Expected no audit log output, got: %s", output)
			}

			if !tt.wantError && resp == nil {
				t.Error("Expected response, got nil")
			}
		})
	}
}

func TestAuditStreamInterceptor(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}
	auditLogger := NewAuditLogger(config)

	interceptor := AuditStreamInterceptor(auditLogger)

	tests := []struct {
		name       string
		method     string
		handler    grpc.StreamHandler
		wantLogged bool
		wantError  bool
	}{
		{
			name:   "successful stream",
			method: "/ObjectStore/GetStream",
			handler: func(srv any, stream grpc.ServerStream) error {
				return nil
			},
			wantLogged: true,
			wantError:  false,
		},
		{
			name:   "failed stream",
			method: "/ObjectStore/PutStream",
			handler: func(srv any, stream grpc.ServerStream) error {
				return grpc.Errorf(13, "internal error")
			},
			wantLogged: true,
			wantError:  true,
		},
		{
			name:   "health check stream",
			method: "/ObjectStore/HealthStream",
			handler: func(srv any, stream grpc.ServerStream) error {
				return nil
			},
			wantLogged: false,
			wantError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()

			ctx := context.Background()
			ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))

			mockStream := &mockServerStream{ctx: ctx}

			info := &grpc.StreamServerInfo{
				FullMethod: tt.method,
			}

			err := interceptor(nil, mockStream, info, tt.handler)

			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			output := buf.String()
			if tt.wantLogged && output == "" {
				t.Error("Expected audit log output, got none")
			}
			if !tt.wantLogged && output != "" {
				t.Errorf("Expected no audit log output, got: %s", output)
			}
		})
	}
}

func TestDetermineEventType(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		expected EventType
	}{
		{"GET object", "GET", "/objects/key", EventObjectAccessed},
		{"GET metadata", "GET", "/metadata/key", EventObjectAccessed},
		{"GET list", "GET", "/objects?prefix=test", EventListObjects},
		{"PUT object", "PUT", "/objects/key", EventObjectCreated},
		{"PUT metadata", "PUT", "/metadata/key", EventObjectMetadataUpdated},
		{"PUT policy", "PUT", "/lifecycle/policy", EventPolicyChanged},
		{"POST object", "POST", "/objects", EventObjectCreated},
		{"DELETE object", "DELETE", "/objects/key", EventObjectDeleted},
		{"DELETE bucket", "DELETE", "/bucket/mybucket", EventBucketDeleted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineEventType(tt.method, tt.path)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDetermineGRPCEventType(t *testing.T) {
	tests := []struct {
		name       string
		fullMethod string
		expected   EventType
	}{
		{"Get", "/ObjectStore/Get", EventObjectAccessed},
		{"GetMetadata", "/ObjectStore/GetMetadata", EventObjectAccessed},
		{"Put", "/ObjectStore/Put", EventObjectCreated},
		{"Delete", "/ObjectStore/Delete", EventObjectDeleted},
		{"List", "/ObjectStore/List", EventListObjects},
		{"UpdateMetadata", "/ObjectStore/UpdateMetadata", EventObjectMetadataUpdated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineGRPCEventType(tt.fullMethod)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractClientIP(t *testing.T) {
	tests := []struct {
		name     string
		setupCtx func() context.Context
		expected string
	}{
		{
			name: "from peer",
			setupCtx: func() context.Context {
				p := &peer.Peer{
					Addr: &mockAddr{addr: "192.168.1.1:1234"},
				}
				return peer.NewContext(context.Background(), p)
			},
			expected: "192.168.1.1:1234",
		},
		{
			name: "from x-forwarded-for",
			setupCtx: func() context.Context {
				md := metadata.New(map[string]string{
					"x-forwarded-for": "10.0.0.1",
				})
				return metadata.NewIncomingContext(context.Background(), md)
			},
			expected: "10.0.0.1",
		},
		{
			name: "from x-real-ip",
			setupCtx: func() context.Context {
				md := metadata.New(map[string]string{
					"x-real-ip": "172.16.0.1",
				})
				return metadata.NewIncomingContext(context.Background(), md)
			},
			expected: "172.16.0.1",
		},
		{
			name: "unknown",
			setupCtx: func() context.Context {
				return context.Background()
			},
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			ip := extractClientIP(ctx)
			if ip != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, ip)
			}
		})
	}
}

func TestShouldAuditRequest(t *testing.T) {
	tests := []struct {
		path     string
		method   string
		expected bool
	}{
		{"/health", "GET", false},
		{"/metrics", "GET", false},
		{"/ping", "GET", false},
		{"/objects/key", "GET", true},
		{"/objects/key", "PUT", true},
		{"/objects/key", "DELETE", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := shouldAuditRequest(tt.path, tt.method)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestShouldAuditGRPCMethod(t *testing.T) {
	tests := []struct {
		fullMethod string
		expected   bool
	}{
		{"/ObjectStore/Health", false},
		{"/ObjectStore/Get", true},
		{"/ObjectStore/Put", true},
		{"/ObjectStore/Delete", true},
		{"/ObjectStore/List", true},
	}

	for _, tt := range tests {
		t.Run(tt.fullMethod, func(t *testing.T) {
			result := shouldAuditGRPCMethod(tt.fullMethod)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// Mock types for testing

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

type mockAddr struct {
	addr string
}

func (m *mockAddr) Network() string {
	return "tcp"
}

func (m *mockAddr) String() string {
	return m.addr
}

type mockKeyRequest struct {
	Key string
}

func (m *mockKeyRequest) GetKey() string {
	return m.Key
}

func TestExtractResourceInfo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		path           string
		queryBucket    string
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "with key",
			path:           "/objects/:key",
			queryBucket:    "",
			expectedBucket: "default",
			expectedKey:    "test-key",
		},
		{
			name:           "with bucket query",
			path:           "/objects/:key",
			queryBucket:    "my-bucket",
			expectedBucket: "my-bucket",
			expectedKey:    "test-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.GET(tt.path, func(c *gin.Context) {
				c.Param("key")
				bucket, key := extractResourceInfo(c)
				if bucket != tt.expectedBucket {
					t.Errorf("Expected bucket %q, got %q", tt.expectedBucket, bucket)
				}
				if key != tt.expectedKey {
					t.Errorf("Expected key %q, got %q", tt.expectedKey, key)
				}
			})

			url := "/objects/test-key"
			if tt.queryBucket != "" {
				url += "?bucket=" + tt.queryBucket
			}

			req := httptest.NewRequest("GET", url, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
		})
	}
}

func TestExtractGRPCResourceInfo(t *testing.T) {
	tests := []struct {
		name           string
		req            any
		expectedBucket string
		expectedKey    string
	}{
		{
			name:           "with key getter",
			req:            &mockKeyRequest{Key: "my-key"},
			expectedBucket: "default",
			expectedKey:    "my-key",
		},
		{
			name:           "without key getter",
			req:            "not a key getter",
			expectedBucket: "default",
			expectedKey:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key := extractGRPCResourceInfo(tt.req)
			if bucket != tt.expectedBucket {
				t.Errorf("Expected bucket %q, got %q", tt.expectedBucket, bucket)
			}
			if key != tt.expectedKey {
				t.Errorf("Expected key %q, got %q", tt.expectedKey, key)
			}
		})
	}
}
