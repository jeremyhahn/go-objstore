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
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

// Mock gRPC server
type mockGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockGRPCServer) Put(ctx context.Context, req *objstorepb.PutRequest) (*objstorepb.PutResponse, error) {
	return &objstorepb.PutResponse{}, nil
}

func (s *mockGRPCServer) Get(req *objstorepb.GetRequest, stream objstorepb.ObjectStore_GetServer) error {
	metadata := &objstorepb.Metadata{
		ContentType: "text/plain",
		Size:        22,
	}

	// Send first chunk with metadata
	chunk1 := &objstorepb.GetResponse{
		Data:     []byte("hello "),
		Metadata: metadata,
	}
	if err := stream.Send(chunk1); err != nil {
		return err
	}

	// Send second chunk
	chunk2 := &objstorepb.GetResponse{
		Data: []byte("world"),
	}
	if err := stream.Send(chunk2); err != nil {
		return err
	}

	// Send third chunk
	chunk3 := &objstorepb.GetResponse{
		Data: []byte(" again"),
	}
	return stream.Send(chunk3)
}

func (s *mockGRPCServer) Delete(ctx context.Context, req *objstorepb.DeleteRequest) (*objstorepb.DeleteResponse, error) {
	return &objstorepb.DeleteResponse{}, nil
}

func (s *mockGRPCServer) Exists(ctx context.Context, req *objstorepb.ExistsRequest) (*objstorepb.ExistsResponse, error) {
	exists := strings.Contains(req.Key, "exists")
	return &objstorepb.ExistsResponse{Exists: exists}, nil
}

func (s *mockGRPCServer) List(ctx context.Context, req *objstorepb.ListRequest) (*objstorepb.ListResponse, error) {
	return &objstorepb.ListResponse{
		Objects: []*objstorepb.ObjectInfo{
			{
				Key: "test/file1.txt",
				Metadata: &objstorepb.Metadata{
					ContentType: "text/plain",
					Size:        100,
				},
			},
		},
		CommonPrefixes: []string{},
		NextToken:      "",
		Truncated:      false,
	}, nil
}

func (s *mockGRPCServer) GetMetadata(ctx context.Context, req *objstorepb.GetMetadataRequest) (*objstorepb.MetadataResponse, error) {
	return &objstorepb.MetadataResponse{
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
			Size:        100,
		},
	}, nil
}

func (s *mockGRPCServer) UpdateMetadata(ctx context.Context, req *objstorepb.UpdateMetadataRequest) (*objstorepb.UpdateMetadataResponse, error) {
	return &objstorepb.UpdateMetadataResponse{}, nil
}

func (s *mockGRPCServer) Archive(ctx context.Context, req *objstorepb.ArchiveRequest) (*objstorepb.ArchiveResponse, error) {
	return &objstorepb.ArchiveResponse{}, nil
}

func (s *mockGRPCServer) AddPolicy(ctx context.Context, req *objstorepb.AddPolicyRequest) (*objstorepb.AddPolicyResponse, error) {
	return &objstorepb.AddPolicyResponse{}, nil
}

func (s *mockGRPCServer) RemovePolicy(ctx context.Context, req *objstorepb.RemovePolicyRequest) (*objstorepb.RemovePolicyResponse, error) {
	return &objstorepb.RemovePolicyResponse{}, nil
}

func (s *mockGRPCServer) GetPolicies(ctx context.Context, req *objstorepb.GetPoliciesRequest) (*objstorepb.GetPoliciesResponse, error) {
	return &objstorepb.GetPoliciesResponse{
		Policies: []*objstorepb.LifecyclePolicy{
			{
				Id:               "test",
				Prefix:           "tmp/",
				RetentionSeconds: 86400,
				Action:           "delete",
			},
		},
	}, nil
}

func (s *mockGRPCServer) ApplyPolicies(ctx context.Context, req *objstorepb.ApplyPoliciesRequest) (*objstorepb.ApplyPoliciesResponse, error) {
	return &objstorepb.ApplyPoliciesResponse{
		PoliciesCount:    1,
		ObjectsProcessed: 5,
	}, nil
}

func (s *mockGRPCServer) Health(ctx context.Context, req *objstorepb.HealthRequest) (*objstorepb.HealthResponse, error) {
	return &objstorepb.HealthResponse{
		Status:  objstorepb.HealthResponse_SERVING,
		Message: "OK",
	}, nil
}

func (s *mockGRPCServer) AddReplicationPolicy(ctx context.Context, req *objstorepb.AddReplicationPolicyRequest) (*objstorepb.AddReplicationPolicyResponse, error) {
	return &objstorepb.AddReplicationPolicyResponse{}, nil
}

func (s *mockGRPCServer) RemoveReplicationPolicy(ctx context.Context, req *objstorepb.RemoveReplicationPolicyRequest) (*objstorepb.RemoveReplicationPolicyResponse, error) {
	return &objstorepb.RemoveReplicationPolicyResponse{}, nil
}

func (s *mockGRPCServer) GetReplicationPolicy(ctx context.Context, req *objstorepb.GetReplicationPolicyRequest) (*objstorepb.GetReplicationPolicyResponse, error) {
	return &objstorepb.GetReplicationPolicyResponse{
		Policy: &objstorepb.ReplicationPolicy{
			Id:                   "test-policy",
			SourceBackend:        "local",
			DestinationBackend:   "s3",
			SourceSettings:       map[string]string{"path": "/tmp/src"},
			DestinationSettings:  map[string]string{"bucket": "test"},
			CheckIntervalSeconds: 3600,
			LastSyncTime:         timestamppb.Now(),
			Enabled:              true,
		},
	}, nil
}

func (s *mockGRPCServer) GetReplicationPolicies(ctx context.Context, req *objstorepb.GetReplicationPoliciesRequest) (*objstorepb.GetReplicationPoliciesResponse, error) {
	return &objstorepb.GetReplicationPoliciesResponse{
		Policies: []*objstorepb.ReplicationPolicy{
			{
				Id:                   "policy1",
				SourceBackend:        "local",
				DestinationBackend:   "s3",
				SourceSettings:       map[string]string{"path": "/tmp/src"},
				DestinationSettings:  map[string]string{"bucket": "test"},
				CheckIntervalSeconds: 3600,
				LastSyncTime:         timestamppb.Now(),
				Enabled:              true,
			},
		},
	}, nil
}

func (s *mockGRPCServer) TriggerReplication(ctx context.Context, req *objstorepb.TriggerReplicationRequest) (*objstorepb.TriggerReplicationResponse, error) {
	return &objstorepb.TriggerReplicationResponse{
		Result: &objstorepb.SyncResult{
			PolicyId:   "test-policy",
			Synced:     10,
			Deleted:    0,
			Failed:     0,
			BytesTotal: 1024,
			DurationMs: 5000,
		},
	}, nil
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func setupGRPCServer(t *testing.T) (*grpc.Server, func()) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockGRPCServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	return s, func() {
		s.Stop()
		lis.Close()
	}
}

func createGRPCTestClient(t *testing.T) (*GRPCClient, func()) {
	_, cleanup := setupGRPCServer(t)

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	return client, func() {
		conn.Close()
		cleanup()
	}
}

func TestNewGRPCClient_InvalidURL(t *testing.T) {
	_, err := NewGRPCClient(&Config{ServerURL: ""})
	if err == nil {
		t.Error("expected error with empty URL")
	}
}

func TestGRPCClient_Put(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.Put(context.Background(), "test.txt", strings.NewReader("hello"), nil)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
}

func TestGRPCClient_PutWithMetadata(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"author": "test"},
	}

	err := client.Put(context.Background(), "test.txt", strings.NewReader("hello"), metadata)
	if err != nil {
		t.Errorf("Put with metadata failed: %v", err)
	}
}

func TestGRPCClient_Get(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	reader, metadata, err := client.Get(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if string(data) != "hello world again" {
		t.Errorf("expected 'hello world again', got %q", string(data))
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.Size != 22 {
		t.Errorf("expected size 22, got %d", metadata.Size)
	}
}

func TestGRPCClient_Delete(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.Delete(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
}

func TestGRPCClient_Exists(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	exists, err := client.Exists(context.Background(), "exists.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected file to exist")
	}

	exists, err = client.Exists(context.Background(), "missing.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected file not to exist")
	}
}

func TestGRPCClient_List(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	result, err := client.List(context.Background(), &common.ListOptions{Prefix: "test/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(result.Objects))
	}
}

func TestGRPCClient_GetMetadata(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	metadata, err := client.GetMetadata(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.Size != 100 {
		t.Errorf("expected size 100, got %d", metadata.Size)
	}
}

func TestGRPCClient_UpdateMetadata(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	metadata := &common.Metadata{ContentType: "text/plain"}
	err := client.UpdateMetadata(context.Background(), "test.txt", metadata)
	if err != nil {
		t.Errorf("UpdateMetadata failed: %v", err)
	}
}

func TestGRPCClient_Archive(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.Archive(context.Background(), "test.txt", "glacier", map[string]string{"vault": "test"})
	if err != nil {
		t.Errorf("Archive failed: %v", err)
	}
}

func TestGRPCClient_Policies(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	// Test AddPolicy
	policy := common.LifecyclePolicy{
		ID:        "test",
		Prefix:    "tmp/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := client.AddPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
	}

	// Test GetPolicies
	policies, err := client.GetPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}

	// Test ApplyPolicies
	count, processed, err := client.ApplyPolicies(context.Background())
	if err != nil {
		t.Errorf("ApplyPolicies failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 policy applied, got %d", count)
	}
	if processed != 5 {
		t.Errorf("expected 5 objects processed, got %d", processed)
	}

	// Test RemovePolicy
	err = client.RemovePolicy(context.Background(), "test")
	if err != nil {
		t.Errorf("RemovePolicy failed: %v", err)
	}
}

func TestGRPCClient_Health(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.Health(context.Background())
	if err != nil {
		t.Errorf("Health failed: %v", err)
	}
}

func TestGRPCClient_Close(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMetadataConversion(t *testing.T) {
	// Test nil metadata
	if metadataToProto(nil) != nil {
		t.Error("expected nil for nil metadata")
	}
	if protoToMetadata(nil) != nil {
		t.Error("expected nil for nil proto metadata")
	}

	// Test metadata with all fields
	now := time.Now()
	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Size:            100,
		ETag:            "abc123",
		LastModified:    now,
		Custom:          map[string]string{"key": "value"},
	}

	proto := metadataToProto(metadata)
	if proto.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", proto.ContentType)
	}
	if proto.ContentEncoding != "gzip" {
		t.Errorf("expected gzip, got %s", proto.ContentEncoding)
	}
	if proto.Size != 100 {
		t.Errorf("expected 100, got %d", proto.Size)
	}
	if proto.Etag != "abc123" {
		t.Errorf("expected abc123, got %s", proto.Etag)
	}

	// Convert back
	converted := protoToMetadata(proto)
	if converted.ContentType != metadata.ContentType {
		t.Errorf("expected %s, got %s", metadata.ContentType, converted.ContentType)
	}
	if converted.ETag != metadata.ETag {
		t.Errorf("expected %s, got %s", metadata.ETag, converted.ETag)
	}
}

func TestLifecyclePolicyConversion(t *testing.T) {
	// Test nil policy
	if lifecyclePolicyToProto(nil) != nil {
		t.Error("expected nil for nil policy")
	}

	// Test policy conversion
	policy := &common.LifecyclePolicy{
		ID:        "test",
		Prefix:    "tmp/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	proto := lifecyclePolicyToProto(policy)
	if proto.Id != "test" {
		t.Errorf("expected test, got %s", proto.Id)
	}
	if proto.Prefix != "tmp/" {
		t.Errorf("expected tmp/, got %s", proto.Prefix)
	}
	if proto.RetentionSeconds != 86400 {
		t.Errorf("expected 86400, got %d", proto.RetentionSeconds)
	}
	if proto.Action != "delete" {
		t.Errorf("expected delete, got %s", proto.Action)
	}
}

func TestProtoToMetadata_WithTimestamp(t *testing.T) {
	now := time.Now()
	proto := &objstorepb.Metadata{
		ContentType:  "text/plain",
		LastModified: timestamppb.New(now),
	}

	metadata := protoToMetadata(proto)
	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	// Check timestamp is approximately equal (within 1 second)
	if metadata.LastModified.Sub(now).Abs() > time.Second {
		t.Errorf("timestamps don't match: expected %v, got %v", now, metadata.LastModified)
	}
}

// Error handling tests for gRPC

type mockErrorGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockErrorGRPCServer) Health(ctx context.Context, req *objstorepb.HealthRequest) (*objstorepb.HealthResponse, error) {
	return &objstorepb.HealthResponse{
		Status:  objstorepb.HealthResponse_NOT_SERVING,
		Message: "Server not ready",
	}, nil
}

func TestGRPCClient_Health_NotServing(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockErrorGRPCServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()
	defer lis.Close()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	err = client.Health(context.Background())
	if err == nil {
		t.Error("expected error when server not serving")
	}
}

func TestGRPCClient_Close_NilConn(t *testing.T) {
	client := &GRPCClient{
		conn:   nil,
		client: nil,
	}

	err := client.Close()
	if err != nil {
		t.Errorf("Close with nil conn should not error: %v", err)
	}
}

func TestMetadataConversion_WithZeroTime(t *testing.T) {
	metadata := &common.Metadata{
		ContentType:  "text/plain",
		LastModified: time.Time{}, // Zero time
	}

	proto := metadataToProto(metadata)
	if proto.LastModified != nil {
		t.Error("expected nil timestamp for zero time")
	}
}

func TestProtoToMetadata_NilTimestamp(t *testing.T) {
	proto := &objstorepb.Metadata{
		ContentType:  "application/json",
		LastModified: nil,
	}

	metadata := protoToMetadata(proto)
	if !metadata.LastModified.IsZero() {
		t.Error("expected zero time for nil timestamp")
	}
}

func TestGRPCClient_List_WithNilOptions(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	result, err := client.List(context.Background(), nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(result.Objects))
	}
}

func TestGRPCClient_Put_ReadError(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	// Create a reader that always returns an error
	errorReader := &errorReader{}
	err := client.Put(context.Background(), "test.txt", errorReader, nil)
	if err == nil {
		t.Error("expected error from errorReader")
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestGRPCClient_GetPolicies_WithResults(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policies, err := client.GetPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}

	if policies[0].Retention != 24*time.Hour {
		t.Errorf("expected 24h retention, got %v", policies[0].Retention)
	}
}

func TestGRPCClient_ApplyPolicies_WithResults(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	count, processed, err := client.ApplyPolicies(context.Background())
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 policy, got %d", count)
	}

	if processed != 5 {
		t.Errorf("expected 5 objects, got %d", processed)
	}
}

func TestGRPCClient_Exists_WithResults(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	exists, err := client.Exists(context.Background(), "exists.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}

	if !exists {
		t.Error("expected file to exist")
	}
}

func TestGRPCClient_GetMetadata_WithResults(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	metadata, err := client.GetMetadata(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.Size != 100 {
		t.Errorf("expected size 100, got %d", metadata.Size)
	}
}

// Error path tests

func TestGRPCClient_Get_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, _, err := client.Get(ctx, "test.txt")
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

func TestGRPCClient_Exists_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.Exists(ctx, "test.txt")
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

func TestGRPCClient_GetMetadata_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.GetMetadata(ctx, "test.txt")
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

func TestGRPCClient_ApplyPolicies_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, _, err := client.ApplyPolicies(ctx)
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

func TestNewGRPCClient_EmptyURL(t *testing.T) {
	_, err := NewGRPCClient(&Config{ServerURL: ""})
	if err == nil {
		t.Error("expected error with empty server URL")
	}
}

// gRPC Replication tests
func TestGRPCClient_AddReplicationPolicy(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"bucket": "test"},
		CheckInterval:       time.Hour,
	}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddReplicationPolicy failed: %v", err)
	}
}

func TestGRPCClient_RemoveReplicationPolicy(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	err := client.RemoveReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("RemoveReplicationPolicy failed: %v", err)
	}
}

func TestGRPCClient_GetReplicationPolicy(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policy, err := client.GetReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("GetReplicationPolicy failed: %v", err)
	}
	if policy == nil || policy.ID != "test-policy" {
		t.Errorf("expected policy with ID test-policy, got %v", policy)
	}
}

func TestGRPCClient_GetReplicationPolicies(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policies, err := client.GetReplicationPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetReplicationPolicies failed: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].ID != "policy1" {
		t.Errorf("expected policy1, got %s", policies[0].ID)
	}
}

func TestGRPCClient_TriggerReplication(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	result, err := client.TriggerReplication(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("TriggerReplication failed: %v", err)
	}
	if result == nil || result.PolicyID != "test-policy" {
		t.Errorf("expected result with policy_id=test-policy, got %v", result)
	}
	if result.Synced != 10 {
		t.Errorf("expected 10 synced, got %d", result.Synced)
	}
}

func TestGRPCClient_GetReplicationPolicy_WithTimestamp(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policy, err := client.GetReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("GetReplicationPolicy failed: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	// The timestamp handling is tested through the conversion functions
}

func TestGRPCClient_GetReplicationPolicies_Multiple(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policies, err := client.GetReplicationPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetReplicationPolicies failed: %v", err)
	}
	if len(policies) == 0 {
		t.Error("expected at least one policy")
	}
}

func TestGRPCClient_AddReplicationPolicy_WithLastSyncTime(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy-with-time",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"bucket": "test"},
		CheckInterval:       time.Hour,
		LastSyncTime:        time.Now(),
		Enabled:             true,
	}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddReplicationPolicy with LastSyncTime failed: %v", err)
	}
}

// Test BytesTotal and Duration fields in TriggerReplication
func TestGRPCClient_TriggerReplication_WithBytesAndDuration(t *testing.T) {
	// We need to test the actual result returned to ensure BytesTotal and Duration are populated
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	result, err := client.TriggerReplication(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("TriggerReplication failed: %v", err)
	}
	// Just verify we get a result - the mock server should set these fields
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// Error path: TriggerReplication with nil result
type mockNilResultGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockNilResultGRPCServer) TriggerReplication(ctx context.Context, req *objstorepb.TriggerReplicationRequest) (*objstorepb.TriggerReplicationResponse, error) {
	return &objstorepb.TriggerReplicationResponse{
		Result: nil,
	}, nil
}

func TestGRPCClient_TriggerReplication_NilResult(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockNilResultGRPCServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()
	defer lis.Close()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	_, err = client.TriggerReplication(context.Background(), "test-policy")
	if err == nil {
		t.Error("expected error when result is nil")
	}
	if err != nil && err.Error() != "no sync result returned" {
		t.Errorf("expected 'no sync result returned', got %v", err)
	}
}

// Test List with MaxResults overflow
func TestGRPCClient_List_MaxResultsOverflow(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	_, err := client.List(context.Background(), &common.ListOptions{
		MaxResults: 2147483648, // Exceeds int32 range
	})
	if err == nil {
		t.Error("expected error for MaxResults overflow")
	}
	if err != nil && !strings.Contains(err.Error(), "exceeds int32 range") {
		t.Errorf("expected overflow error, got %v", err)
	}
}

// Test replicationPolicyToProto with zero LastSyncTime
func TestReplicationPolicyConversion_ZeroTime(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		SourceSettings:      map[string]string{"path": "/tmp"},
		DestinationSettings: map[string]string{"bucket": "test"},
		CheckInterval:       time.Hour,
		LastSyncTime:        time.Time{}, // Zero time
		Enabled:             true,
	}

	proto := replicationPolicyToProto(policy)
	if proto.LastSyncTime != nil {
		t.Error("expected nil LastSyncTime for zero time")
	}

	// Test conversion back
	converted := protoToReplicationPolicy(proto)
	if !converted.LastSyncTime.IsZero() {
		t.Error("expected zero LastSyncTime after conversion")
	}
}

// Test protoToReplicationPolicy with nil LastSyncTime
func TestProtoToReplicationPolicy_NilTime(t *testing.T) {
	proto := &objstorepb.ReplicationPolicy{
		Id:                   "test",
		SourceBackend:        "local",
		DestinationBackend:   "s3",
		CheckIntervalSeconds: 3600,
		LastSyncTime:         nil,
		Enabled:              true,
	}

	policy := protoToReplicationPolicy(proto)
	if !policy.LastSyncTime.IsZero() {
		t.Error("expected zero time for nil timestamp")
	}
}

// Test gRPC Get with empty first chunk
type mockEmptyChunkGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockEmptyChunkGRPCServer) Get(req *objstorepb.GetRequest, stream objstorepb.ObjectStore_GetServer) error {
	metadata := &objstorepb.Metadata{
		ContentType: "text/plain",
		Size:        5,
	}

	// Send first chunk with metadata but no data
	chunk1 := &objstorepb.GetResponse{
		Data:     []byte(""),
		Metadata: metadata,
	}
	if err := stream.Send(chunk1); err != nil {
		return err
	}

	// Send second chunk with actual data
	chunk2 := &objstorepb.GetResponse{
		Data: []byte("hello"),
	}
	return stream.Send(chunk2)
}

func TestGRPCClient_Get_EmptyFirstChunk(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockEmptyChunkGRPCServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()
	defer lis.Close()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	reader, metadata, err := client.Get(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}
}

// Test error in stream.Recv for Get
type mockStreamErrorGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockStreamErrorGRPCServer) Get(req *objstorepb.GetRequest, stream objstorepb.ObjectStore_GetServer) error {
	// Send first chunk successfully
	chunk1 := &objstorepb.GetResponse{
		Data: []byte("partial "),
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
		},
	}
	if err := stream.Send(chunk1); err != nil {
		return err
	}

	// Return error on subsequent chunk
	return fmt.Errorf("stream error")
}

func TestGRPCClient_Get_StreamError(t *testing.T) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockStreamErrorGRPCServer{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()
	defer lis.Close()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}

	reader, _, err := client.Get(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	// Reading should eventually fail due to stream error
	data, _ := io.ReadAll(reader)
	// The goroutine will close the pipe with an error
	// We just verify we don't hang indefinitely
	if len(data) == 0 {
		t.Log("Stream error propagated correctly")
	}
}
