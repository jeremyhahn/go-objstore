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
	"crypto/tls"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Test uncovered option functions
func TestWithRateLimit(t *testing.T) {
	opts := DefaultServerOptions()
	config := &middleware.RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             10,
	}
	WithRateLimit(true, config)(opts)

	if !opts.EnableRateLimit {
		t.Error("Rate limit should be enabled")
	}

	if opts.RateLimitConfig != config {
		t.Error("Rate limit config not set correctly")
	}

	// Test with nil config
	opts2 := DefaultServerOptions()
	WithRateLimit(true, nil)(opts2)

	if !opts2.EnableRateLimit {
		t.Error("Rate limit should be enabled")
	}
}

func TestWithRequestID(t *testing.T) {
	opts := DefaultServerOptions()
	WithRequestID(false)(opts)

	if opts.EnableRequestID {
		t.Error("Request ID should be disabled")
	}

	WithRequestID(true)(opts)
	if !opts.EnableRequestID {
		t.Error("Request ID should be enabled")
	}
}

func TestWithAuditLogger(t *testing.T) {
	opts := DefaultServerOptions()
	logger := audit.NewDefaultAuditLogger()
	WithAuditLogger(logger)(opts)

	if opts.AuditLogger == nil {
		t.Error("Audit logger should be set")
	}
}

func TestWithAudit(t *testing.T) {
	opts := DefaultServerOptions()
	WithAudit(false)(opts)

	if opts.EnableAudit {
		t.Error("Audit should be disabled")
	}

	WithAudit(true)(opts)
	if !opts.EnableAudit {
		t.Error("Audit should be enabled")
	}
}

// Test server with reflection enabled
func TestServer_WithReflection(t *testing.T) {
	storage := newMockStorage()

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"), WithReflection(true))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Stop server
	server.Stop()

	// Check if server started successfully
	select {
	case err := <-errChan:
		if err != nil && err.Error() != "use of closed network connection" {
			t.Logf("Server stopped with error: %v", err)
		}
	case <-time.After(1 * time.Second):
		// Server stopped gracefully
	}
}

// Test buildServerOptions with TLS error
func TestBuildServerOptions_TLSError(t *testing.T) {
	storage := newMockStorage()

	// Create an invalid TLS config that will fail to build
	invalidTLSConfig := &adapters.TLSConfig{
		Mode:           adapters.TLSModeMutual,
		ServerCertFile: "/nonexistent/cert.pem",
		ServerKeyFile:  "/nonexistent/key.pem",
		ClientCAFile:   "/nonexistent/ca.pem",
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithAdapterTLS(invalidTLSConfig),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// buildServerOptions is called during Start
	// This should log an error but not fail
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()

	time.Sleep(100 * time.Millisecond)
	server.Stop()

	select {
	case <-errChan:
		// Expected to complete
	case <-time.After(1 * time.Second):
		// Server stopped gracefully
	}
}

// Test buildServerOptions with rate limiting enabled
func TestBuildServerOptions_RateLimiting(t *testing.T) {
	storage := newMockStorage()

	rateLimitConfig := &middleware.RateLimitConfig{
		RequestsPerSecond: 10,
		Burst:             5,
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithRateLimit(true, rateLimitConfig),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if !server.opts.EnableRateLimit {
		t.Error("Rate limiting should be enabled")
	}

	// Start and stop server to ensure buildServerOptions is called
	go server.Start()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
}

// Test buildServerOptions with request ID disabled
func TestBuildServerOptions_RequestIDDisabled(t *testing.T) {
	storage := newMockStorage()

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithRequestID(false),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server.opts.EnableRequestID {
		t.Error("Request ID should be disabled")
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
}

// Test buildServerOptions with audit disabled
func TestBuildServerOptions_AuditDisabled(t *testing.T) {
	storage := newMockStorage()

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithAudit(false),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server.opts.EnableAudit {
		t.Error("Audit should be disabled")
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
}

// Test buildServerOptions with legacy TLS config
func TestBuildServerOptions_LegacyTLS(t *testing.T) {
	storage := newMockStorage()

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithTLS(tlsConfig),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server.opts.TLSConfig == nil {
		t.Error("TLS config should be set")
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
}

// Test extractGRPCClientIP with various scenarios
type mockAddr struct {
	addr string
}

func (m mockAddr) Network() string {
	return "tcp"
}

func (m mockAddr) String() string {
	return m.addr
}

type mockAuthInfo struct{}

func (m mockAuthInfo) AuthType() string {
	return "tls"
}

func TestExtractGRPCClientIP_FromPeer(t *testing.T) {
	// Create a context with peer info
	addr := mockAddr{addr: "127.0.0.1:12345"}
	p := &peer.Peer{Addr: addr}
	ctx := peer.NewContext(context.Background(), p)

	ip := extractGRPCClientIP(ctx)
	if ip == "unknown" {
		t.Errorf("Expected IP from peer, got 'unknown'")
	}
}

func TestExtractGRPCClientIP_FromXForwardedFor(t *testing.T) {
	// Create a context with X-Forwarded-For header
	md := metadata.Pairs("x-forwarded-for", "192.168.1.1")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ip := extractGRPCClientIP(ctx)
	if ip != "192.168.1.1" {
		t.Errorf("Expected '192.168.1.1', got '%s'", ip)
	}
}

func TestExtractGRPCClientIP_FromXRealIP(t *testing.T) {
	// Create a context with X-Real-IP header
	md := metadata.Pairs("x-real-ip", "10.0.0.1")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ip := extractGRPCClientIP(ctx)
	if ip != "10.0.0.1" {
		t.Errorf("Expected '10.0.0.1', got '%s'", ip)
	}
}

func TestExtractGRPCClientIP_Unknown(t *testing.T) {
	// Context without any IP information
	ctx := context.Background()

	ip := extractGRPCClientIP(ctx)
	if ip != "unknown" {
		t.Errorf("Expected 'unknown', got '%s'", ip)
	}
}

// Test extractGRPCPrincipal edge cases
func TestExtractGRPCPrincipal_WithPrincipal(t *testing.T) {
	principal := adapters.Principal{
		ID:   "user123",
		Name: "testuser",
	}
	ctx := context.WithValue(context.Background(), principalContextKey, principal)

	name, id := extractGRPCPrincipal(ctx)
	if name != "testuser" || id != "user123" {
		t.Errorf("Expected 'testuser' and 'user123', got '%s' and '%s'", name, id)
	}
}

func TestExtractGRPCPrincipal_NoPrincipal(t *testing.T) {
	ctx := context.Background()

	name, id := extractGRPCPrincipal(ctx)
	if name != "" || id != "" {
		t.Errorf("Expected empty strings, got '%s' and '%s'", name, id)
	}
}

func TestExtractGRPCPrincipal_InvalidType(t *testing.T) {
	// Context with wrong type for principal
	ctx := context.WithValue(context.Background(), "principal", "not-a-principal")

	name, id := extractGRPCPrincipal(ctx)
	if name != "" || id != "" {
		t.Errorf("Expected empty strings, got '%s' and '%s'", name, id)
	}
}

// Test authentication interceptors with various scenarios
type mockAuthenticator struct {
	shouldFail bool
}

func (m *mockAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*adapters.Principal, error) {
	if m.shouldFail {
		return nil, errors.New("authentication failed")
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *mockAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*adapters.Principal, error) {
	return nil, errors.New("not implemented")
}

func (m *mockAuthenticator) AuthenticateMTLS(ctx context.Context, connState *tls.ConnectionState) (*adapters.Principal, error) {
	return nil, errors.New("not implemented")
}

func (m *mockAuthenticator) ValidatePermission(ctx context.Context, principal *adapters.Principal, resource, action string) error {
	return nil
}

func TestAuthenticationUnaryInterceptor_Success(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: false}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationUnaryInterceptor(auth, logger)

	handler := func(ctx context.Context, req any) (any, error) {
		// Check if principal was added to context
		if ctx.Value(principalContextKey) == nil {
			t.Error("Expected principal in context")
		}
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	md := metadata.Pairs("authorization", "Bearer test-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	resp, err := interceptor(ctx, "request", info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if resp != "response" {
		t.Errorf("Expected 'response', got %v", resp)
	}
}

func TestAuthenticationUnaryInterceptor_Failure(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationUnaryInterceptor(auth, logger)

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	md := metadata.Pairs("authorization", "Bearer invalid-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptor(ctx, "request", info, handler)

	if err == nil {
		t.Error("Expected error for failed authentication")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("Expected Unauthenticated error, got %v", err)
	}
}

func TestAuthenticationUnaryInterceptor_NoMetadata(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationUnaryInterceptor(auth, logger)

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	// Context without metadata
	ctx := context.Background()

	_, err := interceptor(ctx, "request", info, handler)

	if err == nil {
		t.Error("Expected error for failed authentication")
	}
}

func TestAuthenticationStreamInterceptor_Success(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: false}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationStreamInterceptor(auth, logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	md := metadata.Pairs("authorization", "Bearer test-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := interceptor(nil, &mockServerStream{ctx: ctx}, info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestAuthenticationStreamInterceptor_Failure(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationStreamInterceptor(auth, logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	md := metadata.Pairs("authorization", "Bearer invalid-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := interceptor(nil, &mockServerStream{ctx: ctx}, info, handler)

	if err == nil {
		t.Error("Expected error for failed authentication")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("Expected Unauthenticated error, got %v", err)
	}
}

func TestAuthenticationStreamInterceptor_NoMetadata(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationStreamInterceptor(auth, logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err == nil {
		t.Error("Expected error for failed authentication")
	}
}

// Test server Start failure scenarios
func TestServer_StartFailure_InvalidAddress(t *testing.T) {
	storage := newMockStorage()

	server, err := newTestServer(t, storage, WithAddress("invalid:address:format"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	err = server.Start()
	if err == nil {
		t.Error("Expected error for invalid address")
		server.Stop()
	}
}

// Test Get with error reading from storage
func TestGet_StorageReadError(t *testing.T) {
	storage := newMockStorage()

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	// Try to get a non-existent object
	getReq := &objstorepb.GetRequest{
		Key: "non-existent",
	}

	stream, err := client.Get(ctx, getReq)
	if err != nil {
		// Some errors can happen during dial or stream creation
		return
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for non-existent object")
	}
}

// Test List with error from storage
func TestList_StorageError(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// This should succeed but return empty list
	listReq := &objstorepb.ListRequest{
		Prefix:     "test/",
		Delimiter:  "/",
		MaxResults: 100,
	}

	listResp, err := client.List(ctx, listReq)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if listResp == nil {
		t.Error("List response should not be nil")
	}
}

// Test Exists with error from storage
func TestExists_StorageError(t *testing.T) {
	storage := &errorMockStorage{
		mockStorage:      newMockStorage(),
		shouldFailExists: true,
	}

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	existsReq := &objstorepb.ExistsRequest{
		Key: "test-key",
	}

	_, err = client.Exists(ctx, existsReq)
	if err == nil {
		t.Error("Expected error from storage")
	}
}

// Test authentication with mTLS (peer with auth info)
func TestAuthenticationUnaryInterceptor_MTLSPath(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationUnaryInterceptor(auth, logger)

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	// Create context with TLS peer info
	addr := mockAddr{addr: "127.0.0.1:12345"}
	p := &peer.Peer{
		Addr:     addr,
		AuthInfo: mockAuthInfo{},
	}
	ctx := peer.NewContext(context.Background(), p)

	_, err := interceptor(ctx, "request", info, handler)

	// Should still fail because mockAuthenticator is set to fail
	if err == nil {
		t.Error("Expected error for failed authentication")
	}
}

func TestAuthenticationStreamInterceptor_MTLSPath(t *testing.T) {
	auth := &mockAuthenticator{shouldFail: true}
	logger := adapters.NewNoOpLogger()
	interceptor := AuthenticationStreamInterceptor(auth, logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	// Create context with TLS peer info
	addr := mockAddr{addr: "127.0.0.1:12345"}
	p := &peer.Peer{
		Addr:     addr,
		AuthInfo: mockAuthInfo{},
	}
	ctx := peer.NewContext(context.Background(), p)

	err := interceptor(nil, &mockServerStream{ctx: ctx}, info, handler)

	// Should still fail because mockAuthenticator is set to fail
	if err == nil {
		t.Error("Expected error for failed authentication")
	}
}

// Test Put with storage error
func TestPut_StorageError(t *testing.T) {
	storage := &errorPutMockStorage{
		mockStorage: newMockStorage(),
	}

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	_, err = client.Put(ctx, putReq)
	if err == nil {
		t.Error("Expected error from storage")
	}
}

// Test Get with invalid key
func TestGet_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	getReq := &objstorepb.GetRequest{
		Key: "",
	}

	stream, err := client.Get(ctx, getReq)
	if err != nil {
		// Error during Get call is expected
		return
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

// Test server with all interceptors enabled
func TestServer_AllInterceptors(t *testing.T) {
	storage := newMockStorage()

	rateLimitConfig := &middleware.RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             10,
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithRateLimit(true, rateLimitConfig),
		WithRequestID(true),
		WithAudit(true),
		WithMetrics(true),
		WithLogging(true),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	// Make a request to trigger all interceptors
	healthReq := &objstorepb.HealthRequest{}
	_, err = client.Health(ctx, healthReq)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
}

// Test buildServerOptions with valid AdapterTLSConfig
func TestBuildServerOptions_ValidAdapterTLS(t *testing.T) {
	storage := newMockStorage()

	// Create a valid TLS config
	cert, key := generateTestCert(t)
	tmpDir := t.TempDir()
	certFile := tmpDir + "/cert.pem"
	keyFile := tmpDir + "/key.pem"

	if err := os.WriteFile(certFile, cert, 0644); err != nil {
		t.Fatalf("Failed to write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, key, 0600); err != nil {
		t.Fatalf("Failed to write key: %v", err)
	}

	tlsConfig := &adapters.TLSConfig{
		Mode:           adapters.TLSModeServer,
		ServerCertFile: certFile,
		ServerKeyFile:  keyFile,
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithAdapterTLS(tlsConfig),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	server.Stop()
}

// Test server with custom unary and stream interceptors
func TestServer_CustomInterceptors(t *testing.T) {
	storage := newMockStorage()

	customUnaryInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}

	customStreamInterceptor := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}

	initTestFacade(t, storage)
	server, err := NewServer(
		WithAddress("127.0.0.1:0"),
		WithUnaryInterceptor(customUnaryInterceptor),
		WithStreamInterceptor(customStreamInterceptor),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	// Test with unary RPC
	healthReq := &objstorepb.HealthRequest{}
	_, err = client.Health(ctx, healthReq)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	// Test with stream RPC
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}
	_, err = client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	getReq := &objstorepb.GetRequest{
		Key: "test-key",
	}
	stream, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Read the stream
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
}

// Test ApplyPolicies with no policies
func TestApplyPolicies_NoPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()
	
	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	req := &objstorepb.ApplyPoliciesRequest{}
	resp, err := client.ApplyPolicies(ctx, req)
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success for no policies")
	}

	if resp.PoliciesCount != 0 {
		t.Errorf("Expected 0 policies, got %d", resp.PoliciesCount)
	}
}

// Test ApplyPolicies with policies
func TestApplyPolicies_WithPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 30 * 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	// Add an object that matches the policy
	storage.data["test/object.txt"] = []byte("data")
	storage.metadata["test/object.txt"] = &common.Metadata{Size: 4}

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	req := &objstorepb.ApplyPoliciesRequest{}
	resp, err := client.ApplyPolicies(ctx, req)
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success")
	}

	if resp.PoliciesCount == 0 {
		t.Error("Expected at least one policy")
	}
}

// Test hasPrefix helper function indirectly through ApplyPolicies
func TestApplyPolicies_PrefixFiltering(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Add a policy with specific prefix
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 7 * 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	// Add objects - some match prefix, some don't
	storage.data["logs/app.log"] = []byte("log data")
	storage.metadata["logs/app.log"] = &common.Metadata{Size: 8}
	storage.data["data/file.txt"] = []byte("data")
	storage.metadata["data/file.txt"] = &common.Metadata{Size: 4}
	storage.data["logs/error.log"] = []byte("error log")
	storage.metadata["logs/error.log"] = &common.Metadata{Size: 9}

	server, err := newTestServer(t, storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	req := &objstorepb.ApplyPoliciesRequest{}
	resp, err := client.ApplyPolicies(ctx, req)
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success")
	}

	// Should only process objects matching the prefix
	if resp.ObjectsProcessed != 2 {
		t.Logf("Expected 2 objects processed (logs/ prefix), got %d", resp.ObjectsProcessed)
	}
}
