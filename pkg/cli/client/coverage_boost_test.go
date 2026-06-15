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

package client

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// NewUnixSocketClient
// ---------------------------------------------------------------------------

func TestNewUnixSocketClient_MissingSocket(t *testing.T) {
	_, err := NewUnixSocketClient(&Config{UnixSocket: ""})
	if err == nil {
		t.Fatal("expected error for empty unix socket path")
	}
	if err != ErrUnixSocketRequired {
		t.Errorf("expected ErrUnixSocketRequired, got %v", err)
	}
}

func TestNewUnixSocketClient_WithSocket(t *testing.T) {
	// Use a temp path; we only verify client construction, not a live connection.
	client, err := NewUnixSocketClient(&Config{UnixSocket: "/tmp/test.sock"})
	if err != nil {
		t.Fatalf("NewUnixSocketClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.baseURL != "http://localhost" {
		t.Errorf("expected baseURL http://localhost, got %s", client.baseURL)
	}
}

func TestNewUnixSocketClient_WithServerURL(t *testing.T) {
	client, err := NewUnixSocketClient(&Config{
		UnixSocket: "/tmp/test.sock",
		ServerURL:  "http://myserver",
	})
	if err != nil {
		t.Fatalf("NewUnixSocketClient failed: %v", err)
	}
	if client.baseURL != "http://myserver" {
		t.Errorf("expected baseURL http://myserver, got %s", client.baseURL)
	}
}

// Exercise the Unix socket transport by making a real request over a socket.
func TestNewUnixSocketClient_RoundTrip(t *testing.T) {
	// Create a temporary unix socket path.
	tmpDir := t.TempDir()
	sockPath := tmpDir + "/objstore.sock"

	// Start an HTTP server listening on the unix socket.
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv := &httptest.Server{
		Listener: ln,
		Config: &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		},
	}
	srv.Start()
	defer srv.Close()

	client, err := NewUnixSocketClient(&Config{
		UnixSocket: sockPath,
		ServerURL:  "http://localhost",
	})
	if err != nil {
		t.Fatalf("NewUnixSocketClient: %v", err)
	}

	err = client.Health(context.Background())
	if err != nil {
		t.Errorf("Health over unix socket failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// NewClient – unix protocol branch (factory.go:41 90.9%)
// ---------------------------------------------------------------------------

func TestNewClient_Unix(t *testing.T) {
	client, err := NewClient(&Config{
		Protocol:   "unix",
		UnixSocket: "/tmp/test.sock",
	})
	if err != nil {
		t.Fatalf("NewClient unix failed: %v", err)
	}
	if _, ok := client.(*RESTClient); !ok {
		t.Errorf("expected *RESTClient for unix protocol, got %T", client)
	}
	client.Close()
}

func TestNewClient_Unix_MissingSocket(t *testing.T) {
	_, err := NewClient(&Config{
		Protocol:   "unix",
		UnixSocket: "",
	})
	if err == nil {
		t.Fatal("expected error for unix without socket path")
	}
}

// ---------------------------------------------------------------------------
// REST GetReplicationStatus (rest.go:650 – 0%)
// ---------------------------------------------------------------------------

func TestRESTClient_GetReplicationStatus_Success(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	payload := map[string]interface{}{
		"policy_id":            "pol1",
		"source_backend":       "local",
		"destination_backend":  "s3",
		"enabled":              true,
		"total_objects_synced": 100,
		"total_bytes_synced":   4096,
		"last_sync_time":       now.Format(time.RFC3339),
	}
	body, _ := json.Marshal(payload)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/replication/status/pol1") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("NewRESTClient: %v", err)
	}

	status, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err != nil {
		t.Fatalf("GetReplicationStatus failed: %v", err)
	}
	if status.PolicyID != "pol1" {
		t.Errorf("expected PolicyID pol1, got %s", status.PolicyID)
	}
	if status.SourceBackend != "local" {
		t.Errorf("expected source_backend local, got %s", status.SourceBackend)
	}
	if status.TotalObjectsSynced != 100 {
		t.Errorf("expected 100 synced, got %d", status.TotalObjectsSynced)
	}
}

func TestRESTClient_GetReplicationStatus_NonOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationStatus(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected body in error, got %v", err)
	}
}

func TestRESTClient_GetReplicationStatus_NonOK_EmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error for 500")
	}
}

func TestRESTClient_GetReplicationStatus_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{bad json`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestRESTClient_GetReplicationStatus_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from connection failure")
	}
}

// ---------------------------------------------------------------------------
// QUIC GetReplicationStatus (quic.go:721 – 0%)
// ---------------------------------------------------------------------------

func TestQUICClient_GetReplicationStatus_Success(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	payload := map[string]interface{}{
		"policy_id":            "pol1",
		"source_backend":       "local",
		"destination_backend":  "s3",
		"enabled":              true,
		"total_objects_synced": 50,
		"last_sync_time":       now.Format(time.RFC3339),
	}
	body, _ := json.Marshal(payload)

	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/replication/status/pol1") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	status, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err != nil {
		t.Fatalf("GetReplicationStatus failed: %v", err)
	}
	if status.PolicyID != "pol1" {
		t.Errorf("expected PolicyID pol1, got %s", status.PolicyID)
	}
	if status.TotalObjectsSynced != 50 {
		t.Errorf("expected 50 synced, got %d", status.TotalObjectsSynced)
	}
}

func TestQUICClient_GetReplicationStatus_NonOK(t *testing.T) {
	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	_, err := client.GetReplicationStatus(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected body in error, got %v", err)
	}
}

func TestQUICClient_GetReplicationStatus_NonOK_EmptyBody(t *testing.T) {
	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error for 500")
	}
}

func TestQUICClient_GetReplicationStatus_InvalidJSON(t *testing.T) {
	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{bad json`))
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestQUICClient_GetReplicationStatus_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from connection failure")
	}
}

// ---------------------------------------------------------------------------
// GRPC GetReplicationStatus (grpc.go:434 – 0%)
// ---------------------------------------------------------------------------

// mockGetReplicationStatusServer implements GetReplicationStatus on the mock gRPC server.
type mockGetReplicationStatusServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockGetReplicationStatusServer) GetReplicationStatus(
	ctx context.Context,
	req *objstorepb.GetReplicationStatusRequest,
) (*objstorepb.GetReplicationStatusResponse, error) {
	return &objstorepb.GetReplicationStatusResponse{
		Status: &objstorepb.ReplicationStatus{
			PolicyId:              req.Id,
			SourceBackend:         "local",
			DestinationBackend:    "s3",
			Enabled:               true,
			TotalObjectsSynced:    200,
			TotalObjectsDeleted:   5,
			TotalBytesSynced:      8192,
			TotalErrors:           1,
			LastSyncTime:          timestamppb.Now(),
			AverageSyncDurationMs: 300,
			SyncCount:             10,
		},
	}, nil
}

func setupGetReplicationStatusGRPCServer(t *testing.T) (func(), *grpc.ClientConn) {
	t.Helper()
	l := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockGetReplicationStatusServer{})
	go func() {
		if err := s.Serve(l); err != nil {
			t.Logf("server exited: %v", err)
		}
	}()
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.Dial() }),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	return func() { s.Stop(); l.Close() }, conn
}

func TestGRPCClient_GetReplicationStatus_Success(t *testing.T) {
	cleanup, conn := setupGetReplicationStatusGRPCServer(t)
	defer cleanup()
	defer conn.Close()

	c := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	status, err := c.GetReplicationStatus(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("GetReplicationStatus failed: %v", err)
	}
	if status.PolicyID != "test-policy" {
		t.Errorf("expected PolicyID test-policy, got %s", status.PolicyID)
	}
	if status.SourceBackend != "local" {
		t.Errorf("expected source local, got %s", status.SourceBackend)
	}
	if status.TotalObjectsSynced != 200 {
		t.Errorf("expected 200 synced, got %d", status.TotalObjectsSynced)
	}
	if status.AverageSyncDuration != 300*time.Millisecond {
		t.Errorf("expected 300ms average, got %v", status.AverageSyncDuration)
	}
	if status.SyncCount != 10 {
		t.Errorf("expected 10 sync count, got %d", status.SyncCount)
	}
}

// mockNilStatusGRPCServer returns a nil Status in the response.
type mockNilStatusGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockNilStatusGRPCServer) GetReplicationStatus(
	ctx context.Context,
	req *objstorepb.GetReplicationStatusRequest,
) (*objstorepb.GetReplicationStatusResponse, error) {
	return &objstorepb.GetReplicationStatusResponse{Status: nil}, nil
}

func TestGRPCClient_GetReplicationStatus_NilStatus(t *testing.T) {
	l := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockNilStatusGRPCServer{})
	go func() { _ = s.Serve(l) }()
	defer s.Stop()
	defer l.Close()

	dial := func(ctx context.Context, _ string) (net.Conn, error) { return l.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dial), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := &GRPCClient{conn: conn, client: objstorepb.NewObjectStoreClient(conn)}
	_, err = c.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error for nil status")
	}
	if err != ErrNoStatus {
		t.Errorf("expected ErrNoStatus, got %v", err)
	}
}

func TestGRPCClient_GetReplicationStatus_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.GetReplicationStatus(ctx, "pol1")
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

// ---------------------------------------------------------------------------
// QUIC Close – nil transport branch (quic.go:562, 66.7%)
// The existing TestQUICClient_Close covers the non-nil transport path.
// Here we exercise the nil-transport branch directly.
// ---------------------------------------------------------------------------

func TestQUICClient_Close_NilTransport(t *testing.T) {
	c := &QUICClient{transport: nil}
	if err := c.Close(); err != nil {
		t.Errorf("Close with nil transport should return nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Additional GRPCClient branch coverage
// ---------------------------------------------------------------------------

// TestGRPCClient_NewGRPCClient_WithValidURL verifies the non-empty URL happy
// path of NewGRPCClient (covers the early-return error branch already tested
// elsewhere; this ensures the normal code path is exercised too).
func TestGRPCClient_NewGRPCClient_WithValidURL(t *testing.T) {
	c, err := NewGRPCClient(&Config{ServerURL: "localhost:50051"})
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	c.Close()
}

// ---------------------------------------------------------------------------
// REST/QUIC – remaining partial-coverage helpers
// ---------------------------------------------------------------------------

// TestRESTClient_GetReplicationStatus_RoundTrip verifies the full happy path
// including that JSON fields map to the domain type correctly.
func TestRESTClient_GetReplicationStatus_Fields(t *testing.T) {
	payload := replication.ReplicationStatus{
		PolicyID:            "rp1",
		SourceBackend:       "local",
		DestinationBackend:  "gcs",
		Enabled:             false,
		TotalObjectsSynced:  999,
		TotalObjectsDeleted: 7,
		TotalBytesSynced:    65536,
		TotalErrors:         3,
		LastSyncTime:        time.Now().Truncate(time.Second),
		AverageSyncDuration: 500 * time.Millisecond,
		SyncCount:           42,
	}
	body, _ := json.Marshal(payload)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	status, err := client.GetReplicationStatus(context.Background(), "rp1")
	if err != nil {
		t.Fatalf("GetReplicationStatus: %v", err)
	}
	if status.TotalObjectsDeleted != 7 {
		t.Errorf("expected 7 deleted, got %d", status.TotalObjectsDeleted)
	}
	if status.TotalErrors != 3 {
		t.Errorf("expected 3 errors, got %d", status.TotalErrors)
	}
	if status.SyncCount != 42 {
		t.Errorf("expected 42 sync count, got %d", status.SyncCount)
	}
}

// TestQUICClient_GetReplicationStatus_Fields exercises the same path via QUIC.
func TestQUICClient_GetReplicationStatus_Fields(t *testing.T) {
	payload := replication.ReplicationStatus{
		PolicyID:            "rp2",
		SourceBackend:       "s3",
		DestinationBackend:  "local",
		Enabled:             true,
		TotalObjectsSynced:  15,
		TotalObjectsDeleted: 2,
		TotalBytesSynced:    1024,
		TotalErrors:         0,
		SyncCount:           5,
	}
	body, _ := json.Marshal(payload)

	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	status, err := client.GetReplicationStatus(context.Background(), "rp2")
	if err != nil {
		t.Fatalf("GetReplicationStatus: %v", err)
	}
	if status.DestinationBackend != "local" {
		t.Errorf("expected destination local, got %s", status.DestinationBackend)
	}
	if status.SyncCount != 5 {
		t.Errorf("expected 5 sync count, got %d", status.SyncCount)
	}
}

// ---------------------------------------------------------------------------
// Error sentinel values (ErrUnixSocketRequired is in unix.go, used in tests
// above; verify the others are accessible and correctly typed).
// ---------------------------------------------------------------------------

func TestErrorSentinels(t *testing.T) {
	sentinels := []error{
		ErrConfigRequired,
		ErrUnsupportedProtocol,
		ErrServerURLRequired,
		ErrMaxResultsOverflow,
		ErrServerNotServing,
		ErrNoSyncResult,
		ErrNoStatus,
		ErrServerError,
		ErrUnixSocketRequired,
	}
	for _, e := range sentinels {
		if e == nil {
			t.Errorf("sentinel error must not be nil")
		}
		if e.Error() == "" {
			t.Errorf("sentinel error must have a message")
		}
	}
}

// ---------------------------------------------------------------------------
// NewClient unix – exercises the os.Stat branch in the socket dial via
// a live socket so the transport closure is covered at invocation time too.
// ---------------------------------------------------------------------------

func TestNewClient_Unix_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := tmpDir + "/test.sock"

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv := &httptest.Server{
		Listener: ln,
		Config: &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		},
	}
	srv.Start()
	defer srv.Close()

	client, err := NewClient(&Config{
		Protocol:   "unix",
		UnixSocket: sockPath,
		ServerURL:  "http://localhost",
	})
	if err != nil {
		t.Fatalf("NewClient unix: %v", err)
	}
	defer client.Close()

	if err := client.Health(context.Background()); err != nil {
		t.Errorf("Health over unix socket: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Additional GRPCClient coverage – uncovered branches in GetReplicationPolicy,
// GetReplicationPolicies, and TriggerReplication (83–88%).
// The existing tests cover the happy path; these cover the cancelled-context
// error return on each.
// ---------------------------------------------------------------------------

func TestGRPCClient_GetReplicationPolicy_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.GetReplicationPolicy(ctx, "pol1")
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestGRPCClient_GetReplicationPolicies_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.GetReplicationPolicies(ctx)
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestGRPCClient_TriggerReplication_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.TriggerReplication(ctx, "pol1")
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestGRPCClient_AddReplicationPolicy_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.AddReplicationPolicy(ctx, newTestReplicationPolicy())
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestGRPCClient_RemoveReplicationPolicy_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.RemoveReplicationPolicy(ctx, "pol1")
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

// ---------------------------------------------------------------------------
// GRPCClient – NewGRPCClient uncovered success path (grpc.go:37, 85.7%)
// ---------------------------------------------------------------------------

func TestGRPCClient_NewGRPCClient_DialFailure(t *testing.T) {
	// Connecting to an unreachable address is fine for construction; gRPC dials
	// lazily, so no error is expected here.
	c, err := NewGRPCClient(&Config{ServerURL: "localhost:1"})
	if err != nil {
		t.Fatalf("NewGRPCClient with unreachable address should not fail construction: %v", err)
	}
	c.Close()
}

// ---------------------------------------------------------------------------
// REST – UpdateMetadata nil metadata path (rest.go:280 89.5%)
// ---------------------------------------------------------------------------

func TestRESTClient_UpdateMetadata_NilMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	// Passing nil metadata should not panic and should succeed (no headers set).
	err := client.UpdateMetadata(context.Background(), "test.txt", nil)
	if err != nil {
		t.Errorf("UpdateMetadata with nil metadata failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// QUIC – QUICClient Close with active transport (quic.go:562, 66.7%)
// newQUICTestClient constructs a client with a non-nil transport so calling
// Close() exercises the non-nil branch (line 564).
// ---------------------------------------------------------------------------

func TestQUICClient_Close_NonNilTransport(t *testing.T) {
	server := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newQUICTestClient(t, server.URL)
	if err := client.Close(); err != nil {
		// The transport.Close() on a live QUIC transport should succeed.
		t.Errorf("Close non-nil transport: %v", err)
	}
}

// ---------------------------------------------------------------------------
// REST – AddReplicationPolicy marshal/HTTP path (rest.go:494 84.2%)
// Cover the "invalid ServerURL that causes NewRequest to fail" branch by
// injecting a URL with a control character which http.NewRequestWithContext
// will reject.
// ---------------------------------------------------------------------------

func TestRESTClient_AddReplicationPolicy_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	err := client.AddReplicationPolicy(context.Background(), newTestReplicationPolicy())
	if err == nil {
		t.Fatal("expected error from bad URL in AddReplicationPolicy")
	}
}

func TestRESTClient_RemoveReplicationPolicy_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	err := client.RemoveReplicationPolicy(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from bad URL in RemoveReplicationPolicy")
	}
}

func TestRESTClient_GetReplicationPolicy_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	_, err := client.GetReplicationPolicy(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationPolicy")
	}
}

func TestRESTClient_GetReplicationPolicies_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationPolicies")
	}
}

func TestRESTClient_TriggerReplication_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	_, err := client.TriggerReplication(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from bad URL in TriggerReplication")
	}
}

func TestRESTClient_GetReplicationStatus_BadURL(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://\x00invalid",
		httpClient: &http.Client{},
	}
	_, err := client.GetReplicationStatus(context.Background(), "pol1")
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationStatus")
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newTestReplicationPolicy() common.ReplicationPolicy {
	return common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"bucket": "my-bucket"},
		CheckInterval:       time.Hour,
	}
}

// Ensure common.ReplicationPolicy is also importable (it is an alias / same
// type in the common package used throughout).
var _ = os.DevNull // keep os import used
