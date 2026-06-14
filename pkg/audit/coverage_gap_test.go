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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// --- NewAuditLogger: nil Output path -----------------------------------------

// TestNewAuditLogger_NilOutput exercises the branch that defaults Output to
// os.Stdout when config.Output is nil.
func TestNewAuditLogger_NilOutput(t *testing.T) {
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  nil, // must fall back to os.Stdout
	}
	logger := NewAuditLogger(config)
	if logger == nil {
		t.Fatal("expected non-nil logger when Output is nil")
	}
	// Smoke-test: logging must not panic.
	ctx := context.Background()
	if err := logger.LogEvent(ctx, &AuditEvent{Action: "test", Result: ResultSuccess}); err != nil {
		t.Fatalf("LogEvent: %v", err)
	}
}

// TestNewAuditLogger_UnknownFormat exercises the default (JSON handler) branch
// for an unrecognised OutputFormat value.
func TestNewAuditLogger_UnknownFormat(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  OutputFormat("xml"), // not JSON or Text
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}
	logger := NewAuditLogger(config)
	if logger == nil {
		t.Fatal("expected non-nil logger for unknown format")
	}
	_ = logger.LogEvent(context.Background(), &AuditEvent{Action: "smoke", Result: ResultSuccess})
}

// --- LogEvent: optional field branches ---------------------------------------

// TestLogEvent_ResourceField exercises the event.Resource branch in LogEvent.
func TestLogEvent_ResourceField(t *testing.T) {
	var buf bytes.Buffer
	logger := NewAuditLogger(&Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	})

	event := &AuditEvent{
		EventType: EventObjectAccessed,
		Resource:  "bucket/key",
		Action:    "get_object",
		Result:    ResultSuccess,
	}
	if err := logger.LogEvent(context.Background(), event); err != nil {
		t.Fatalf("LogEvent: %v", err)
	}
	if !strings.Contains(buf.String(), "bucket/key") {
		t.Error("expected resource to appear in log output")
	}
}

// TestLogEvent_NilEvent exercises the early-return for a nil event.
func TestLogEvent_NilEvent(t *testing.T) {
	logger := NewDefaultAuditLogger()
	if err := logger.LogEvent(context.Background(), nil); err != nil {
		t.Fatalf("LogEvent(nil): %v", err)
	}
}

// TestLogEvent_ZeroTimestamp exercises the branch that sets Timestamp when it
// is the zero value.
func TestLogEvent_ZeroTimestamp(t *testing.T) {
	var buf bytes.Buffer
	logger := NewAuditLogger(&Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	})
	event := &AuditEvent{
		// Timestamp intentionally left as zero value.
		EventType: EventObjectCreated,
		Action:    "put",
		Result:    ResultSuccess,
	}
	if err := logger.LogEvent(context.Background(), event); err != nil {
		t.Fatalf("LogEvent: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected log output for zero-timestamp event")
	}
}

// --- LogPolicyChange: error path ---------------------------------------------

// TestLogPolicyChange_WithError exercises the branch that sets ErrorMessage.
func TestLogPolicyChange_WithError(t *testing.T) {
	var buf bytes.Buffer
	logger := NewAuditLogger(&Config{
		Enabled:         true,
		Format:          FormatJSON,
		Level:           adapters.InfoLevel,
		Output:          &mockFile{Buffer: &buf},
		IncludeMetadata: true,
	})

	err := logger.LogPolicyChange(
		context.Background(),
		"user1", "alice", "my-bucket", "pol-001",
		"10.0.0.1", "req-xyz",
		ResultFailure, errors.New("permission denied"),
	)
	if err != nil {
		t.Fatalf("LogPolicyChange: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "permission denied") {
		t.Error("expected error message in log output")
	}
	if !strings.Contains(output, "POLICY_CHANGED") {
		t.Error("expected POLICY_CHANGED in log output")
	}
}

// --- NoOpAuditLogger SetLevel / GetLevel -------------------------------------

// TestNoOpAuditLogger_SetLevelGetLevel exercises the two 0%-covered methods
// on NoOpAuditLogger.
func TestNoOpAuditLogger_SetLevelGetLevel(t *testing.T) {
	noop := &NoOpAuditLogger{level: adapters.InfoLevel}

	noop.SetLevel(adapters.DebugLevel)
	if noop.GetLevel() != adapters.DebugLevel {
		t.Errorf("GetLevel = %v, want DebugLevel", noop.GetLevel())
	}

	noop.SetLevel(adapters.ErrorLevel)
	if noop.GetLevel() != adapters.ErrorLevel {
		t.Errorf("GetLevel = %v, want ErrorLevel", noop.GetLevel())
	}
}

// --- sanitizeRequestID branches ----------------------------------------------

// TestSanitizeRequestID_TooLong exercises the len > requestIDMaxLen branch.
func TestSanitizeRequestID_TooLong(t *testing.T) {
	id := strings.Repeat("a", requestIDMaxLen+1)
	if got := sanitizeRequestID(id); got != "" {
		t.Errorf("expected empty string for oversized ID, got %q", got)
	}
}

// TestSanitizeRequestID_InvalidChars exercises the !requestIDPattern.MatchString
// branch.
func TestSanitizeRequestID_InvalidChars(t *testing.T) {
	id := "bad id with spaces"
	if got := sanitizeRequestID(id); got != "" {
		t.Errorf("expected empty string for ID with spaces, got %q", got)
	}
}

// TestSanitizeRequestID_Valid exercises the happy path.
func TestSanitizeRequestID_Valid(t *testing.T) {
	id := "valid-request-ID_123.abc"
	if got := sanitizeRequestID(id); got != id {
		t.Errorf("expected %q unchanged, got %q", id, got)
	}
}

// TestSanitizeRequestID_Empty exercises the empty string branch.
func TestSanitizeRequestID_Empty(t *testing.T) {
	if got := sanitizeRequestID(""); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

// --- auditServerStream.Context -----------------------------------------------

// TestAuditServerStream_Context exercises the Context() method on
// auditServerStream, which is at 0% coverage.
func TestAuditServerStream_Context(t *testing.T) {
	inner := &mockServerStream{ctx: context.Background()}
	wrapped := &auditServerStream{ServerStream: inner, ctx: context.Background()}

	ctx := wrapped.Context()
	if ctx == nil {
		t.Fatal("expected non-nil context from auditServerStream.Context()")
	}
}

// --- AuditStreamInterceptor: principal extraction & metadata requestID -------

// TestAuditStreamInterceptor_PrincipalPointer exercises the *adapters.Principal
// branch in AuditStreamInterceptor.
func TestAuditStreamInterceptor_PrincipalPointer(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditStreamInterceptor(auditLogger)

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(nil))
	// Inject *adapters.Principal via the typed key.
	ctx = context.WithValue(ctx, adapters.PrincipalContextKey{}, &adapters.Principal{
		ID:   "stream-user",
		Name: "stream-name",
	})

	mockStream := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/ObjectStore/PutStream"}

	handler := func(srv any, ss grpc.ServerStream) error {
		// Also exercise Context() on the wrapped stream.
		_ = ss.Context()
		return nil
	}

	err := interceptor(nil, mockStream, info, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected audit log output")
	}
}

// TestAuditStreamInterceptor_PrincipalValue exercises the adapters.Principal
// value (non-pointer) branch in AuditStreamInterceptor.
func TestAuditStreamInterceptor_PrincipalValue(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditStreamInterceptor(auditLogger)

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(nil))
	ctx = context.WithValue(ctx, adapters.PrincipalContextKey{}, adapters.Principal{
		ID:   "val-user",
		Name: "val-name",
	})

	mockStream := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/ObjectStore/GetStream"}

	err := interceptor(nil, mockStream, info, func(srv any, ss grpc.ServerStream) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestAuditStreamInterceptor_MetadataRequestID exercises the metadata
// x-request-id extraction branch in AuditStreamInterceptor.
func TestAuditStreamInterceptor_MetadataRequestID(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditStreamInterceptor(auditLogger)

	const inboundID = "stream-req-id-abc"
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", inboundID))

	mockStream := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/ObjectStore/DeleteStream"}

	err := interceptor(nil, mockStream, info, func(srv any, ss grpc.ServerStream) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), inboundID) {
		t.Errorf("expected inbound request ID %q in audit record", inboundID)
	}
}

// --- AuditUnaryInterceptor: request-ID from metadata & value principal -------

// TestAuditUnaryInterceptor_MetadataRequestID exercises the branch that reads
// the x-request-id from incoming gRPC metadata.
func TestAuditUnaryInterceptor_MetadataRequestID(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditUnaryInterceptor(auditLogger)

	const inboundID = "unary-req-id-xyz"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", inboundID))

	info := &grpc.UnaryServerInfo{FullMethod: "/ObjectStore/Put"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	resp, err := interceptor(ctx, nil, info, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if !strings.Contains(buf.String(), inboundID) {
		t.Errorf("expected inbound request ID %q in audit record", inboundID)
	}
}

// TestAuditUnaryInterceptor_PrincipalValue exercises the adapters.Principal
// value form in AuditUnaryInterceptor.
func TestAuditUnaryInterceptor_PrincipalValue(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditUnaryInterceptor(auditLogger)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	ctx = context.WithValue(ctx, adapters.PrincipalContextKey{}, adapters.Principal{
		ID:   "val-uid",
		Name: "val-principal",
	})

	info := &grpc.UnaryServerInfo{FullMethod: "/ObjectStore/Delete"}
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	resp, err := interceptor(ctx, nil, info, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

// --- AuditMiddleware: adapters.Principal value form (gin context) ------------

// TestAuditMiddleware_PrincipalValue exercises the adapters.Principal (value,
// not pointer) branch in AuditMiddleware.
func TestAuditMiddleware_PrincipalValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)

	router := gin.New()
	router.Use(AuditMiddleware(auditLogger))
	router.GET("/objects/val-test", func(c *gin.Context) {
		// Store value type (not pointer).
		c.Set("principal", adapters.Principal{
			ID:   "val-id",
			Name: "val-user",
		})
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/objects/val-test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if buf.Len() == 0 {
		t.Error("expected audit log output")
	}
}

// --- AuditMiddleware: inbound X-Request-ID that passes sanitisation ----------

// TestAuditMiddleware_InboundSanitisedRequestID exercises the branch where
// the inbound header passes sanitizeRequestID (without RequestIDMiddleware).
func TestAuditMiddleware_InboundSanitisedRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)

	router := gin.New()
	router.Use(AuditMiddleware(auditLogger))
	router.GET("/objects/sancheck", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	const inboundID = "client-id-42"
	req := httptest.NewRequest(http.MethodGet, "/objects/sancheck", nil)
	req.Header.Set("X-Request-ID", inboundID)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if !strings.Contains(buf.String(), inboundID) {
		t.Errorf("expected audit record to contain sanitised request ID %q", inboundID)
	}
}

// --- AuditMiddleware: error on 4xx with gin error populated ------------------

// TestAuditMiddleware_ErrorMessage exercises the c.Errors branch where the
// error message is extracted when statusCode >= 400.
func TestAuditMiddleware_ErrorMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)

	router := gin.New()
	router.Use(AuditMiddleware(auditLogger))
	router.GET("/objects/errkey", func(c *gin.Context) {
		_ = c.Error(errors.New("object not found"))
		c.Status(http.StatusNotFound)
	})

	req := httptest.NewRequest(http.MethodGet, "/objects/errkey", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if buf.Len() == 0 {
		t.Error("expected audit log output for 404")
	}
}

// --- extractResourceInfo: key with leading slash -----------------------------

// TestExtractResourceInfo_KeyWithLeadingSlash exercises the branch that strips
// the leading '/' from the :key parameter value.
func TestExtractResourceInfo_KeyWithLeadingSlash(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	var capturedKey string
	router.GET("/objects/*key", func(c *gin.Context) {
		_, key := extractResourceInfo(c)
		capturedKey = key
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/objects/mykey", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Gin's wildcard parameter includes the leading slash; extractResourceInfo
	// must strip it.
	if strings.HasPrefix(capturedKey, "/") {
		t.Errorf("extractResourceInfo should strip leading slash, got %q", capturedKey)
	}
	if capturedKey != "mykey" {
		t.Errorf("expected key 'mykey', got %q", capturedKey)
	}
}

// --- determineEventType: GET /objects (root list path) -----------------------

// TestDetermineEventType_GetObjectsRoot exercises the path == "/objects" branch.
func TestDetermineEventType_GetObjectsRoot(t *testing.T) {
	got := determineEventType("GET", "/objects")
	if got != EventListObjects {
		t.Errorf("expected EventListObjects, got %v", got)
	}
}

// --- determineEventType: unknown method default branch -----------------------

// TestDetermineEventType_UnknownMethod exercises the default return.
func TestDetermineEventType_UnknownMethod(t *testing.T) {
	got := determineEventType("PATCH", "/objects/key")
	if got != EventObjectAccessed {
		t.Errorf("expected EventObjectAccessed for PATCH, got %v", got)
	}
}

// --- AuditUnaryInterceptor: gRPC status code extraction ---------------------

// TestAuditUnaryInterceptor_GRPCStatusCode exercises the status.FromError
// code extraction in AuditUnaryInterceptor.
func TestAuditUnaryInterceptor_GRPCStatusCode(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditUnaryInterceptor(auditLogger)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	info := &grpc.UnaryServerInfo{FullMethod: "/ObjectStore/Get"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, grpc.Errorf(7, "permission denied") //nolint:staticcheck // grpc.Errorf is fine in tests
	}

	_, err := interceptor(ctx, nil, info, handler)
	if err == nil {
		t.Fatal("expected error")
	}
	if buf.Len() == 0 {
		t.Error("expected audit log output for gRPC error")
	}
}

// --- AuditStreamInterceptor: gRPC status code extraction --------------------

// TestAuditStreamInterceptor_GRPCStatusCode exercises the status.FromError
// code extraction in AuditStreamInterceptor.
func TestAuditStreamInterceptor_GRPCStatusCode(t *testing.T) {
	var buf bytes.Buffer
	auditLogger := newBufferedAuditLogger(&buf)
	interceptor := AuditStreamInterceptor(auditLogger)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	mockStream := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/ObjectStore/PutStream"}

	err := interceptor(nil, mockStream, info, func(srv any, ss grpc.ServerStream) error {
		return grpc.Errorf(13, "internal error") //nolint:staticcheck // grpc.Errorf is fine in tests
	})
	if err == nil {
		t.Fatal("expected error from stream handler")
	}
	if buf.Len() == 0 {
		t.Error("expected audit log output")
	}
}
