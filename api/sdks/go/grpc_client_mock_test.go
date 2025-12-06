// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MockObjectStoreClient is a mock for the gRPC client
type MockObjectStoreClient struct {
	mock.Mock
}

func (m *MockObjectStoreClient) Put(ctx context.Context, in *objstorepb.PutRequest, opts ...grpc.CallOption) (*objstorepb.PutResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.PutResponse), args.Error(1)
}

func (m *MockObjectStoreClient) Get(ctx context.Context, in *objstorepb.GetRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[objstorepb.GetResponse], error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(grpc.ServerStreamingClient[objstorepb.GetResponse]), args.Error(1)
}

func (m *MockObjectStoreClient) Delete(ctx context.Context, in *objstorepb.DeleteRequest, opts ...grpc.CallOption) (*objstorepb.DeleteResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.DeleteResponse), args.Error(1)
}

func (m *MockObjectStoreClient) List(ctx context.Context, in *objstorepb.ListRequest, opts ...grpc.CallOption) (*objstorepb.ListResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.ListResponse), args.Error(1)
}

func (m *MockObjectStoreClient) Exists(ctx context.Context, in *objstorepb.ExistsRequest, opts ...grpc.CallOption) (*objstorepb.ExistsResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.ExistsResponse), args.Error(1)
}

func (m *MockObjectStoreClient) GetMetadata(ctx context.Context, in *objstorepb.GetMetadataRequest, opts ...grpc.CallOption) (*objstorepb.MetadataResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.MetadataResponse), args.Error(1)
}

func (m *MockObjectStoreClient) UpdateMetadata(ctx context.Context, in *objstorepb.UpdateMetadataRequest, opts ...grpc.CallOption) (*objstorepb.UpdateMetadataResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.UpdateMetadataResponse), args.Error(1)
}

func (m *MockObjectStoreClient) Health(ctx context.Context, in *objstorepb.HealthRequest, opts ...grpc.CallOption) (*objstorepb.HealthResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.HealthResponse), args.Error(1)
}

func (m *MockObjectStoreClient) Archive(ctx context.Context, in *objstorepb.ArchiveRequest, opts ...grpc.CallOption) (*objstorepb.ArchiveResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.ArchiveResponse), args.Error(1)
}

func (m *MockObjectStoreClient) AddPolicy(ctx context.Context, in *objstorepb.AddPolicyRequest, opts ...grpc.CallOption) (*objstorepb.AddPolicyResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.AddPolicyResponse), args.Error(1)
}

func (m *MockObjectStoreClient) RemovePolicy(ctx context.Context, in *objstorepb.RemovePolicyRequest, opts ...grpc.CallOption) (*objstorepb.RemovePolicyResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.RemovePolicyResponse), args.Error(1)
}

func (m *MockObjectStoreClient) GetPolicies(ctx context.Context, in *objstorepb.GetPoliciesRequest, opts ...grpc.CallOption) (*objstorepb.GetPoliciesResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.GetPoliciesResponse), args.Error(1)
}

func (m *MockObjectStoreClient) ApplyPolicies(ctx context.Context, in *objstorepb.ApplyPoliciesRequest, opts ...grpc.CallOption) (*objstorepb.ApplyPoliciesResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.ApplyPoliciesResponse), args.Error(1)
}

func (m *MockObjectStoreClient) AddReplicationPolicy(ctx context.Context, in *objstorepb.AddReplicationPolicyRequest, opts ...grpc.CallOption) (*objstorepb.AddReplicationPolicyResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.AddReplicationPolicyResponse), args.Error(1)
}

func (m *MockObjectStoreClient) RemoveReplicationPolicy(ctx context.Context, in *objstorepb.RemoveReplicationPolicyRequest, opts ...grpc.CallOption) (*objstorepb.RemoveReplicationPolicyResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.RemoveReplicationPolicyResponse), args.Error(1)
}

func (m *MockObjectStoreClient) GetReplicationPolicies(ctx context.Context, in *objstorepb.GetReplicationPoliciesRequest, opts ...grpc.CallOption) (*objstorepb.GetReplicationPoliciesResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.GetReplicationPoliciesResponse), args.Error(1)
}

func (m *MockObjectStoreClient) GetReplicationPolicy(ctx context.Context, in *objstorepb.GetReplicationPolicyRequest, opts ...grpc.CallOption) (*objstorepb.GetReplicationPolicyResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.GetReplicationPolicyResponse), args.Error(1)
}

func (m *MockObjectStoreClient) TriggerReplication(ctx context.Context, in *objstorepb.TriggerReplicationRequest, opts ...grpc.CallOption) (*objstorepb.TriggerReplicationResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.TriggerReplicationResponse), args.Error(1)
}

func (m *MockObjectStoreClient) GetReplicationStatus(ctx context.Context, in *objstorepb.GetReplicationStatusRequest, opts ...grpc.CallOption) (*objstorepb.GetReplicationStatusResponse, error) {
	args := m.Called(ctx, in)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*objstorepb.GetReplicationStatusResponse), args.Error(1)
}

// MockGetStream is a mock for the Get stream that implements grpc.ServerStreamingClient
type MockGetStream struct {
	chunks []*objstorepb.GetResponse
	index  int
}

func (m *MockGetStream) Recv() (*objstorepb.GetResponse, error) {
	if m.index >= len(m.chunks) {
		return nil, io.EOF
	}
	chunk := m.chunks[m.index]
	m.index++
	return chunk, nil
}

func (m *MockGetStream) Header() (metadata.MD, error)   { return nil, nil }
func (m *MockGetStream) Trailer() metadata.MD           { return nil }
func (m *MockGetStream) CloseSend() error               { return nil }
func (m *MockGetStream) Context() context.Context       { return context.Background() }
func (m *MockGetStream) SendMsg(msg interface{}) error  { return nil }
func (m *MockGetStream) RecvMsg(msg interface{}) error  { return nil }

// Tests using mocks
func TestGRPCClient_Put_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	key := "test-key"
	data := []byte("test-data")
	metadata := &Metadata{
		ContentType: "text/plain",
	}

	mockClient.On("Put", ctx, mock.MatchedBy(func(req *objstorepb.PutRequest) bool {
		return req.Key == key && string(req.Data) == string(data)
	})).Return(&objstorepb.PutResponse{
		Success: true,
		Message: "Object stored successfully",
		Etag:    "abc123",
	}, nil)

	result, err := client.Put(ctx, key, data, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Success)
	assert.Equal(t, "abc123", result.ETag)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Put_Error(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	testErr := errors.New("server error")

	mockClient.On("Put", ctx, mock.Anything).Return(nil, testErr)

	_, err := client.Put(ctx, "key", []byte("data"), nil)
	assert.Error(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Get_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	mockStream := &MockGetStream{
		chunks: []*objstorepb.GetResponse{
			{
				Data: []byte("test"),
				Metadata: &objstorepb.Metadata{
					ContentType: "text/plain",
					Size:        4,
				},
			},
			{
				Data: []byte("-data"),
			},
		},
	}

	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Get", ctx, mock.Anything).Return(mockStream, nil)

	result, err := client.Get(ctx, "test-key")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, []byte("test-data"), result.Data)
	assert.NotNil(t, result.Metadata)
	assert.Equal(t, "text/plain", result.Metadata.ContentType)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Get_StreamError(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	testErr := errors.New("stream error")

	mockClient.On("Get", ctx, mock.Anything).Return(nil, testErr)

	_, err := client.Get(ctx, "test-key")
	assert.Error(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Delete_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Delete", ctx, mock.Anything).Return(&objstorepb.DeleteResponse{
		Success: true,
		Message: "Deleted",
	}, nil)

	err := client.Delete(ctx, "test-key")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Delete_OperationFailed(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Delete", ctx, mock.Anything).Return(&objstorepb.DeleteResponse{
		Success: false,
		Message: "Not found",
	}, nil)

	err := client.Delete(ctx, "test-key")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrOperationFailed)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_List_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("List", ctx, mock.Anything).Return(&objstorepb.ListResponse{
		Objects: []*objstorepb.ObjectInfo{
			{
				Key: "key1",
				Metadata: &objstorepb.Metadata{
					Size: 100,
				},
			},
			{
				Key: "key2",
				Metadata: &objstorepb.Metadata{
					Size: 200,
				},
			},
		},
		CommonPrefixes: []string{"prefix1/"},
		NextToken:      "token123",
		Truncated:      true,
	}, nil)

	result, err := client.List(ctx, &ListOptions{Prefix: "test/"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Objects, 2)
	assert.Equal(t, "key1", result.Objects[0].Key)
	assert.Equal(t, int64(100), result.Objects[0].Metadata.Size)
	assert.Len(t, result.CommonPrefixes, 1)
	assert.Equal(t, "token123", result.NextToken)
	assert.True(t, result.Truncated)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Exists_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Exists", ctx, mock.Anything).Return(&objstorepb.ExistsResponse{
		Exists: true,
	}, nil)

	exists, err := client.Exists(ctx, "test-key")
	assert.NoError(t, err)
	assert.True(t, exists)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_GetMetadata_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("GetMetadata", ctx, mock.Anything).Return(&objstorepb.MetadataResponse{
		Success: true,
		Metadata: &objstorepb.Metadata{
			ContentType: "application/json",
			Size:        1024,
			Etag:        "xyz789",
		},
	}, nil)

	metadata, err := client.GetMetadata(ctx, "test-key")
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, "application/json", metadata.ContentType)
	assert.Equal(t, int64(1024), metadata.Size)
	assert.Equal(t, "xyz789", metadata.ETag)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_UpdateMetadata_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("UpdateMetadata", ctx, mock.Anything).Return(&objstorepb.UpdateMetadataResponse{
		Success: true,
		Message: "Metadata updated",
	}, nil)

	err := client.UpdateMetadata(ctx, "test-key", &Metadata{ContentType: "text/html"})
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Health_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Health", ctx, mock.Anything).Return(&objstorepb.HealthResponse{
		Status:  objstorepb.HealthResponse_SERVING,
		Message: "Healthy",
	}, nil)

	status, err := client.Health(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, "SERVING", status.Status)
	assert.Equal(t, "Healthy", status.Message)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Archive_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("Archive", ctx, mock.Anything).Return(&objstorepb.ArchiveResponse{
		Success: true,
		Message: "Archived",
	}, nil)

	err := client.Archive(ctx, "test-key", "glacier", map[string]string{"vault": "my-vault"})
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_AddPolicy_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("AddPolicy", ctx, mock.Anything).Return(&objstorepb.AddPolicyResponse{
		Success: true,
		Message: "Policy added",
	}, nil)

	err := client.AddPolicy(ctx, &LifecyclePolicy{
		ID:               "policy1",
		Prefix:           "test/",
		RetentionSeconds: 3600,
		Action:           "archive",
	})
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_RemovePolicy_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("RemovePolicy", ctx, mock.Anything).Return(&objstorepb.RemovePolicyResponse{
		Success: true,
		Message: "Policy removed",
	}, nil)

	err := client.RemovePolicy(ctx, "policy1")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_GetPolicies_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("GetPolicies", ctx, mock.Anything).Return(&objstorepb.GetPoliciesResponse{
		Success: true,
		Policies: []*objstorepb.LifecyclePolicy{
			{
				Id:               "policy1",
				Prefix:           "test/",
				RetentionSeconds: 3600,
				Action:           "archive",
			},
		},
	}, nil)

	policies, err := client.GetPolicies(ctx, "test/")
	assert.NoError(t, err)
	assert.Len(t, policies, 1)
	assert.Equal(t, "policy1", policies[0].ID)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_ApplyPolicies_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("ApplyPolicies", ctx, mock.Anything).Return(&objstorepb.ApplyPoliciesResponse{
		Success:          true,
		PoliciesCount:    5,
		ObjectsProcessed: 100,
		Message:          "Policies applied",
	}, nil)

	result, err := client.ApplyPolicies(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Success)
	assert.Equal(t, int32(5), result.PoliciesCount)
	assert.Equal(t, int32(100), result.ObjectsProcessed)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_AddReplicationPolicy_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("AddReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.AddReplicationPolicyResponse{
		Success: true,
		Message: "Replication policy added",
	}, nil)

	err := client.AddReplicationPolicy(ctx, &ReplicationPolicy{
		ID:                   "repl1",
		SourceBackend:        "local",
		DestinationBackend:   "s3",
		CheckIntervalSeconds: 60,
		Enabled:              true,
	})
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_RemoveReplicationPolicy_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("RemoveReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.RemoveReplicationPolicyResponse{
		Success: true,
		Message: "Policy removed",
	}, nil)

	err := client.RemoveReplicationPolicy(ctx, "repl1")
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_GetReplicationPolicies_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	now := time.Now()
	mockClient.On("GetReplicationPolicies", ctx, mock.Anything).Return(&objstorepb.GetReplicationPoliciesResponse{
		Policies: []*objstorepb.ReplicationPolicy{
			{
				Id:                   "repl1",
				SourceBackend:        "local",
				DestinationBackend:   "s3",
				CheckIntervalSeconds: 60,
				Enabled:              true,
				LastSyncTime:         timestamppb.New(now),
			},
		},
	}, nil)

	policies, err := client.GetReplicationPolicies(ctx)
	assert.NoError(t, err)
	assert.Len(t, policies, 1)
	assert.Equal(t, "repl1", policies[0].ID)
	assert.Equal(t, "local", policies[0].SourceBackend)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_GetReplicationPolicy_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	now := time.Now()
	mockClient.On("GetReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.GetReplicationPolicyResponse{
		Policy: &objstorepb.ReplicationPolicy{
			Id:                   "repl1",
			SourceBackend:        "local",
			DestinationBackend:   "s3",
			CheckIntervalSeconds: 60,
			Enabled:              true,
			LastSyncTime:         timestamppb.New(now),
		},
	}, nil)

	policy, err := client.GetReplicationPolicy(ctx, "repl1")
	assert.NoError(t, err)
	assert.NotNil(t, policy)
	assert.Equal(t, "repl1", policy.ID)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_TriggerReplication_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	mockClient.On("TriggerReplication", ctx, mock.Anything).Return(&objstorepb.TriggerReplicationResponse{
		Success: true,
		Message: "Replication triggered",
		Result: &objstorepb.SyncResult{
			PolicyId:   "repl1",
			Synced:     10,
			Deleted:    2,
			Failed:     0,
			BytesTotal: 1024000,
			DurationMs: 5000,
		},
	}, nil)

	result, err := client.TriggerReplication(ctx, &TriggerReplicationOptions{
		PolicyID:    "repl1",
		Parallel:    true,
		WorkerCount: 4,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "repl1", result.PolicyID)
	assert.Equal(t, int32(10), result.Synced)
	assert.Equal(t, int32(2), result.Deleted)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_GetReplicationStatus_Success(t *testing.T) {
	mockClient := new(MockObjectStoreClient)
	client := &GRPCClient{
		client: mockClient,
		config: &ClientConfig{},
	}

	ctx := context.Background()
	now := time.Now()
	mockClient.On("GetReplicationStatus", ctx, mock.Anything).Return(&objstorepb.GetReplicationStatusResponse{
		Success: true,
		Status: &objstorepb.ReplicationStatus{
			PolicyId:              "repl1",
			SourceBackend:         "local",
			DestinationBackend:    "s3",
			Enabled:               true,
			TotalObjectsSynced:    1000,
			TotalObjectsDeleted:   50,
			TotalBytesSynced:      50000000,
			TotalErrors:           5,
			LastSyncTime:          timestamppb.New(now),
			AverageSyncDurationMs: 4500,
			SyncCount:             10,
		},
	}, nil)

	status, err := client.GetReplicationStatus(ctx, "repl1")
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, "repl1", status.PolicyID)
	assert.Equal(t, int64(1000), status.TotalObjectsSynced)
	assert.True(t, status.Enabled)
	mockClient.AssertExpectations(t)
}

func TestGRPCClient_Close_Success(t *testing.T) {
	client := &GRPCClient{
		conn:   nil,
		config: &ClientConfig{},
	}

	err := client.Close()
	assert.NoError(t, err)
}

func TestTimestampToProto(t *testing.T) {
	now := time.Now()
	proto := timestampToProto(now)
	assert.NotNil(t, proto)
	assert.Equal(t, now.Unix(), proto.AsTime().Unix())
}

func TestTimestampToProto_Zero(t *testing.T) {
	proto := timestampToProto(time.Time{})
	assert.Nil(t, proto)
}
