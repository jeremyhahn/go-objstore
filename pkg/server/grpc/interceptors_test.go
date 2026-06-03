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
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net/http"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()

	if collector == nil {
		t.Fatal("Collector is nil")
	}

	// Test initial state
	metrics := collector.GetMetrics()
	if metrics["total_requests"].(uint64) != 0 {
		t.Error("Expected total_requests to be 0")
	}

	// Increment counters
	collector.totalRequests.Add(5)
	collector.successfulRequests.Add(3)
	collector.failedRequests.Add(2)

	metrics = collector.GetMetrics()
	if metrics["total_requests"].(uint64) != 5 {
		t.Errorf("Expected total_requests to be 5, got %d", metrics["total_requests"].(uint64))
	}

	if metrics["successful_requests"].(uint64) != 3 {
		t.Errorf("Expected successful_requests to be 3, got %d", metrics["successful_requests"].(uint64))
	}

	if metrics["failed_requests"].(uint64) != 2 {
		t.Errorf("Expected failed_requests to be 2, got %d", metrics["failed_requests"].(uint64))
	}
}

func TestMetricsCollector_AverageLatency(t *testing.T) {
	collector := NewMetricsCollector()

	// Add some latency data (in nanoseconds)
	collector.totalLatencyNanos.Add(1000000000) // 1 second
	collector.requestCount.Add(1)

	metrics := collector.GetMetrics()
	avgLatency := metrics["avg_latency_ms"].(float64)

	// Should be 1000ms
	if avgLatency < 999 || avgLatency > 1001 {
		t.Errorf("Expected avg_latency_ms to be ~1000, got %f", avgLatency)
	}
}

func TestMetricsCollector_ActiveStreams(t *testing.T) {
	collector := NewMetricsCollector()

	collector.activeStreams.Add(1)
	collector.activeStreams.Add(1)

	metrics := collector.GetMetrics()
	activeStreams := metrics["active_streams"].(int32)

	if activeStreams != 2 {
		t.Errorf("Expected active_streams to be 2, got %d", activeStreams)
	}

	collector.activeStreams.Add(-1)
	metrics = collector.GetMetrics()
	activeStreams = metrics["active_streams"].(int32)

	if activeStreams != 1 {
		t.Errorf("Expected active_streams to be 1, got %d", activeStreams)
	}
}

func TestLoggingUnaryInterceptor(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	interceptor := LoggingUnaryInterceptor(logger)

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	resp, err := interceptor(context.Background(), "request", info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !called {
		t.Error("Handler was not called")
	}

	if resp != "response" {
		t.Errorf("Expected 'response', got %v", resp)
	}
}

func TestLoggingUnaryInterceptor_WithError(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	interceptor := LoggingUnaryInterceptor(logger)

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, errors.New("test error")
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	_, err := interceptor(context.Background(), "request", info, handler)

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestMetricsUnaryInterceptor(t *testing.T) {
	collector := NewMetricsCollector()
	interceptor := MetricsUnaryInterceptor(collector)

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	_, err := interceptor(context.Background(), "request", info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	metrics := collector.GetMetrics()
	if metrics["total_requests"].(uint64) != 1 {
		t.Error("Expected total_requests to be 1")
	}

	if metrics["successful_requests"].(uint64) != 1 {
		t.Error("Expected successful_requests to be 1")
	}
}

func TestMetricsUnaryInterceptor_WithError(t *testing.T) {
	collector := NewMetricsCollector()
	interceptor := MetricsUnaryInterceptor(collector)

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, errors.New("test error")
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	_, err := interceptor(context.Background(), "request", info, handler)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	metrics := collector.GetMetrics()
	if metrics["failed_requests"].(uint64) != 1 {
		t.Error("Expected failed_requests to be 1")
	}
}

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func TestLoggingStreamInterceptor(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	interceptor := LoggingStreamInterceptor(logger)

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !called {
		t.Error("Handler was not called")
	}
}

func TestLoggingStreamInterceptor_WithError(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	interceptor := LoggingStreamInterceptor(logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		return errors.New("test error")
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestMetricsStreamInterceptor(t *testing.T) {
	collector := NewMetricsCollector()
	interceptor := MetricsStreamInterceptor(collector)

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	metrics := collector.GetMetrics()
	if metrics["total_requests"].(uint64) != 1 {
		t.Error("Expected total_requests to be 1")
	}

	if metrics["successful_requests"].(uint64) != 1 {
		t.Error("Expected successful_requests to be 1")
	}

	// Active streams should be 0 after completion
	if metrics["active_streams"].(int32) != 0 {
		t.Error("Expected active_streams to be 0 after completion")
	}
}

func TestMetricsStreamInterceptor_WithError(t *testing.T) {
	collector := NewMetricsCollector()
	interceptor := MetricsStreamInterceptor(collector)

	handler := func(srv any, stream grpc.ServerStream) error {
		return errors.New("test error")
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	metrics := collector.GetMetrics()
	if metrics["failed_requests"].(uint64) != 1 {
		t.Error("Expected failed_requests to be 1")
	}
}

func TestRecoveryUnaryInterceptor(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor()

	handler := func(ctx context.Context, req any) (any, error) {
		panic("test panic")
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	_, err := interceptor(context.Background(), "request", info, handler)

	if err == nil {
		t.Error("Expected error after panic, got nil")
	}
}

func TestRecoveryUnaryInterceptor_NoPanic(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor()

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	resp, err := interceptor(context.Background(), "request", info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if resp != "response" {
		t.Errorf("Expected 'response', got %v", resp)
	}
}

func TestRecoveryStreamInterceptor(t *testing.T) {
	interceptor := RecoveryStreamInterceptor()

	handler := func(srv any, stream grpc.ServerStream) error {
		panic("test panic")
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err == nil {
		t.Error("Expected error after panic, got nil")
	}
}

func TestRecoveryStreamInterceptor_NoPanic(t *testing.T) {
	interceptor := RecoveryStreamInterceptor()

	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := interceptor(nil, &mockServerStream{}, info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestChainUnaryInterceptors(t *testing.T) {
	var callOrder []int

	interceptor1 := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		callOrder = append(callOrder, 1)
		return handler(ctx, req)
	}

	interceptor2 := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		callOrder = append(callOrder, 2)
		return handler(ctx, req)
	}

	handler := func(ctx context.Context, req any) (any, error) {
		callOrder = append(callOrder, 3)
		return "response", nil
	}

	chained := ChainUnaryInterceptors(interceptor1, interceptor2)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Method",
	}

	_, err := chained(context.Background(), "request", info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(callOrder) != 3 {
		t.Errorf("Expected 3 calls, got %d", len(callOrder))
	}

	if callOrder[0] != 1 || callOrder[1] != 2 || callOrder[2] != 3 {
		t.Errorf("Unexpected call order: %v", callOrder)
	}
}

func TestChainStreamInterceptors(t *testing.T) {
	var callOrder []int

	interceptor1 := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		callOrder = append(callOrder, 1)
		return handler(srv, ss)
	}

	interceptor2 := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		callOrder = append(callOrder, 2)
		return handler(srv, ss)
	}

	handler := func(srv any, ss grpc.ServerStream) error {
		callOrder = append(callOrder, 3)
		return nil
	}

	chained := ChainStreamInterceptors(interceptor1, interceptor2)

	info := &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamMethod",
	}

	err := chained(nil, &mockServerStream{}, info, handler)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(callOrder) != 3 {
		t.Errorf("Expected 3 calls, got %d", len(callOrder))
	}

	if callOrder[0] != 1 || callOrder[1] != 2 || callOrder[2] != 3 {
		t.Errorf("Unexpected call order: %v", callOrder)
	}
}

// captureMTLSAuthenticator fails metadata authentication so the interceptors
// fall back to mTLS, and captures the *tls.ConnectionState passed to
// AuthenticateMTLS for inspection.
type captureMTLSAuthenticator struct {
	capturedState *tls.ConnectionState
}

func (c *captureMTLSAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*adapters.Principal, error) {
	return nil, errors.New("not implemented")
}

func (c *captureMTLSAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*adapters.Principal, error) {
	return nil, errors.New("metadata auth failed")
}

func (c *captureMTLSAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*adapters.Principal, error) {
	c.capturedState = state
	return &adapters.Principal{ID: "mtls-user", Name: "MTLS User"}, nil
}

// newTLSPeerContext returns a context carrying gRPC peer info with the given
// TLS connection state.
func newTLSPeerContext(state tls.ConnectionState) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: state},
	})
}

// testTLSConnectionState builds a connection state with both PeerCertificates
// and VerifiedChains populated, mirroring what the TLS handshake produces
// under RequireAndVerifyClientCert.
func testTLSConnectionState() tls.ConnectionState {
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "client"}}
	ca := &x509.Certificate{Subject: pkix.Name{CommonName: "ca"}}
	return tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf, ca}},
	}
}

func TestAuthenticationUnaryInterceptor_MTLSPropagatesVerifiedChains(t *testing.T) {
	auth := &captureMTLSAuthenticator{}
	interceptor := AuthenticationUnaryInterceptor(auth, adapters.NewNoOpLogger())

	state := testTLSConnectionState()
	ctx := newTLSPeerContext(state)
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	handler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}

	if _, err := interceptor(ctx, "request", info, handler); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if auth.capturedState == nil {
		t.Fatal("AuthenticateMTLS was not called")
	}
	if len(auth.capturedState.VerifiedChains) != 1 {
		t.Fatalf("VerifiedChains not propagated: got %d chains, want 1", len(auth.capturedState.VerifiedChains))
	}
	if len(auth.capturedState.VerifiedChains[0]) != 2 {
		t.Errorf("VerifiedChains[0] length = %d, want 2", len(auth.capturedState.VerifiedChains[0]))
	}
	if len(auth.capturedState.PeerCertificates) != 1 {
		t.Errorf("PeerCertificates length = %d, want 1", len(auth.capturedState.PeerCertificates))
	}
}

func TestAuthenticationStreamInterceptor_MTLSPropagatesVerifiedChains(t *testing.T) {
	auth := &captureMTLSAuthenticator{}
	interceptor := AuthenticationStreamInterceptor(auth, adapters.NewNoOpLogger())

	state := testTLSConnectionState()
	ctx := newTLSPeerContext(state)
	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}

	handler := func(srv any, ss grpc.ServerStream) error {
		return nil
	}

	if err := interceptor(nil, &mockServerStream{ctx: ctx}, info, handler); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if auth.capturedState == nil {
		t.Fatal("AuthenticateMTLS was not called")
	}
	if len(auth.capturedState.VerifiedChains) != 1 {
		t.Fatalf("VerifiedChains not propagated: got %d chains, want 1", len(auth.capturedState.VerifiedChains))
	}
	if len(auth.capturedState.PeerCertificates) != 1 {
		t.Errorf("PeerCertificates length = %d, want 1", len(auth.capturedState.PeerCertificates))
	}
}
