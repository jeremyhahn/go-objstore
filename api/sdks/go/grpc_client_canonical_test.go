// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// This file implements the canonical SDK unit-test matrix for the gRPC
// protocol client. Each of the 19 operations gets a success and an error
// case; nine operations additionally get a not_found case; and the protocol
// gets metadata_round_trip and validation_empty_key cross-cutting cases.
//
// The transport is mocked with MockObjectStoreClient (a testify mock of the
// generated objstorepb.ObjectStoreClient). gRPC errors are modeled with
// status.Error(codes.NotFound, ...) / codes.Internal so the tests exercise the
// real error-propagation paths in grpc_client.go.

import (
	"context"
	"io"
	"testing"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MockObjectStoreClient is a testify mock of objstorepb.ObjectStoreClient. It
// is the single mocked stub used by every canonical gRPC test.
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

// MockGetStream is a server-streaming client returning a fixed chunk sequence,
// used for Get success and metadata_round_trip.
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

func (m *MockGetStream) Header() (metadata.MD, error)  { return nil, nil }
func (m *MockGetStream) Trailer() metadata.MD          { return nil }
func (m *MockGetStream) CloseSend() error              { return nil }
func (m *MockGetStream) Context() context.Context      { return context.Background() }
func (m *MockGetStream) SendMsg(msg interface{}) error { return nil }
func (m *MockGetStream) RecvMsg(msg interface{}) error { return nil }

// newGRPCMock returns a GRPCClient backed by a fresh MockObjectStoreClient.
func newGRPCMock() (*GRPCClient, *MockObjectStoreClient) {
	mc := new(MockObjectStoreClient)
	return &GRPCClient{client: mc, config: &ClientConfig{}}, mc
}

// grpcNotFound is the gRPC status error servers return for missing resources.
func grpcNotFound() error { return status.Error(codes.NotFound, "not found") }

// grpcServerErr is a generic transport/server error.
func grpcServerErr() error { return status.Error(codes.Internal, "server error") }

func TestGRPCClientCanonical(t *testing.T) {
	ctx := context.Background()

	// ---------------------------------------------------------------- success

	t.Run("grpc_put_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Put", ctx, mock.Anything).Return(&objstorepb.PutResponse{Success: true, Etag: "e1"}, nil)
		res, err := c.Put(ctx, "k", []byte("d"), nil)
		require.NoError(t, err)
		assert.Equal(t, "e1", res.ETag)
	})

	t.Run("grpc_get_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Get", ctx, mock.Anything).Return(&MockGetStream{chunks: []*objstorepb.GetResponse{
			{Data: []byte("he"), Metadata: &objstorepb.Metadata{ContentType: "text/plain"}},
			{Data: []byte("llo")},
		}}, nil)
		res, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), res.Data)
		assert.Equal(t, "text/plain", res.Metadata.ContentType)
	})

	t.Run("grpc_delete_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(&objstorepb.DeleteResponse{Success: true}, nil)
		assert.NoError(t, c.Delete(ctx, "k"))
	})

	t.Run("grpc_list_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("List", ctx, mock.Anything).Return(&objstorepb.ListResponse{
			Objects:   []*objstorepb.ObjectInfo{{Key: "a"}, {Key: "b"}},
			NextToken: "t", Truncated: true,
		}, nil)
		res, err := c.List(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, res.Objects, 2)
		assert.Equal(t, "t", res.NextToken)
	})

	t.Run("grpc_exists_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Exists", ctx, mock.Anything).Return(&objstorepb.ExistsResponse{Exists: true}, nil)
		ok, err := c.Exists(ctx, "k")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("grpc_get_metadata_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetMetadata", ctx, mock.Anything).Return(&objstorepb.MetadataResponse{
			Success: true, Metadata: &objstorepb.Metadata{ContentType: "application/json", Size: 12},
		}, nil)
		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "application/json", md.ContentType)
		assert.Equal(t, int64(12), md.Size)
	})

	t.Run("grpc_update_metadata_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("UpdateMetadata", ctx, mock.Anything).Return(&objstorepb.UpdateMetadataResponse{Success: true}, nil)
		assert.NoError(t, c.UpdateMetadata(ctx, "k", &Metadata{ContentType: "text/html"}))
	})

	t.Run("grpc_health_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Health", ctx, mock.Anything).Return(&objstorepb.HealthResponse{
			Status: objstorepb.HealthResponse_SERVING, Message: "ok",
		}, nil)
		st, err := c.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SERVING", st.Status)
	})

	t.Run("grpc_archive_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Archive", ctx, mock.Anything).Return(&objstorepb.ArchiveResponse{Success: true}, nil)
		assert.NoError(t, c.Archive(ctx, "k", "glacier", map[string]string{"vault": "v"}))
	})

	t.Run("grpc_add_policy_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddPolicy", ctx, mock.Anything).Return(&objstorepb.AddPolicyResponse{Success: true}, nil)
		assert.NoError(t, c.AddPolicy(ctx, &LifecyclePolicy{ID: "p1"}))
	})

	t.Run("grpc_remove_policy_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemovePolicy", ctx, mock.Anything).Return(&objstorepb.RemovePolicyResponse{Success: true}, nil)
		assert.NoError(t, c.RemovePolicy(ctx, "p1"))
	})

	t.Run("grpc_get_policies_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetPolicies", ctx, mock.Anything).Return(&objstorepb.GetPoliciesResponse{
			Success: true, Policies: []*objstorepb.LifecyclePolicy{{Id: "p1", Prefix: "x/"}},
		}, nil)
		ps, err := c.GetPolicies(ctx, "x/")
		require.NoError(t, err)
		assert.Len(t, ps, 1)
		assert.Equal(t, "p1", ps[0].ID)
	})

	t.Run("grpc_apply_policies_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("ApplyPolicies", ctx, mock.Anything).Return(&objstorepb.ApplyPoliciesResponse{
			Success: true, PoliciesCount: 3, ObjectsProcessed: 9,
		}, nil)
		res, err := c.ApplyPolicies(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(3), res.PoliciesCount)
	})

	t.Run("grpc_add_replication_policy_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.AddReplicationPolicyResponse{Success: true}, nil)
		assert.NoError(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1"}))
	})

	t.Run("grpc_remove_replication_policy_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemoveReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.RemoveReplicationPolicyResponse{Success: true}, nil)
		assert.NoError(t, c.RemoveReplicationPolicy(ctx, "r1"))
	})

	t.Run("grpc_get_replication_policies_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationPolicies", ctx, mock.Anything).Return(&objstorepb.GetReplicationPoliciesResponse{
			Policies: []*objstorepb.ReplicationPolicy{{Id: "r1", SourceBackend: "local"}},
		}, nil)
		ps, err := c.GetReplicationPolicies(ctx)
		require.NoError(t, err)
		assert.Len(t, ps, 1)
		assert.Equal(t, "r1", ps[0].ID)
	})

	t.Run("grpc_get_replication_policy_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.GetReplicationPolicyResponse{
			Policy: &objstorepb.ReplicationPolicy{Id: "r1", SourceBackend: "local"},
		}, nil)
		p, err := c.GetReplicationPolicy(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, "r1", p.ID)
	})

	t.Run("grpc_trigger_replication_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("TriggerReplication", ctx, mock.Anything).Return(&objstorepb.TriggerReplicationResponse{
			Success: true, Result: &objstorepb.SyncResult{PolicyId: "r1", Synced: 5},
		}, nil)
		res, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		require.NoError(t, err)
		assert.Equal(t, int32(5), res.Synced)
	})

	t.Run("grpc_get_replication_status_success", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationStatus", ctx, mock.Anything).Return(&objstorepb.GetReplicationStatusResponse{
			Success: true, Status: &objstorepb.ReplicationStatus{PolicyId: "r1", TotalObjectsSynced: 100},
		}, nil)
		st, err := c.GetReplicationStatus(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, int64(100), st.TotalObjectsSynced)
	})

	// ------------------------------------------------------------------ error

	t.Run("grpc_put_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Put", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.Put(ctx, "k", []byte("d"), nil)
		assert.Error(t, err)
	})
	t.Run("grpc_get_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Get", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.Get(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("grpc_delete_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.Delete(ctx, "k"))
	})
	t.Run("grpc_list_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("List", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.List(ctx, nil)
		assert.Error(t, err)
	})
	t.Run("grpc_exists_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Exists", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.Exists(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("grpc_get_metadata_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetMetadata", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.GetMetadata(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("grpc_update_metadata_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("UpdateMetadata", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.UpdateMetadata(ctx, "k", &Metadata{}))
	})
	t.Run("grpc_health_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Health", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.Health(ctx)
		assert.Error(t, err)
	})
	t.Run("grpc_archive_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Archive", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.Archive(ctx, "k", "glacier", nil))
	})
	t.Run("grpc_add_policy_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddPolicy", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.AddPolicy(ctx, &LifecyclePolicy{ID: "p1"}))
	})
	t.Run("grpc_remove_policy_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemovePolicy", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.RemovePolicy(ctx, "p1"))
	})
	t.Run("grpc_get_policies_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetPolicies", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.GetPolicies(ctx, "")
		assert.Error(t, err)
	})
	t.Run("grpc_apply_policies_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("ApplyPolicies", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.ApplyPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("grpc_add_replication_policy_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddReplicationPolicy", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1"}))
	})
	t.Run("grpc_remove_replication_policy_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemoveReplicationPolicy", ctx, mock.Anything).Return(nil, grpcServerErr())
		assert.Error(t, c.RemoveReplicationPolicy(ctx, "r1"))
	})
	t.Run("grpc_get_replication_policies_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationPolicies", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.GetReplicationPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("grpc_get_replication_policy_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationPolicy", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.GetReplicationPolicy(ctx, "r1")
		assert.Error(t, err)
	})
	t.Run("grpc_trigger_replication_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("TriggerReplication", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		assert.Error(t, err)
	})
	t.Run("grpc_get_replication_status_error", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationStatus", ctx, mock.Anything).Return(nil, grpcServerErr())
		_, err := c.GetReplicationStatus(ctx, "r1")
		assert.Error(t, err)
	})

	// -------------------------------------------------------------- not_found
	// gRPC NOT_FOUND surfaces as a non-nil error for read/delete ops; for
	// exists it is expressed as Exists:false with no error.

	t.Run("grpc_get_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Get", ctx, mock.Anything).Return(nil, grpcNotFound())
		_, err := c.Get(ctx, "k")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_delete_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(nil, grpcNotFound())
		err := c.Delete(ctx, "k")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_exists_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Exists", ctx, mock.Anything).Return(&objstorepb.ExistsResponse{Exists: false}, nil)
		ok, err := c.Exists(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok)
	})
	t.Run("grpc_get_metadata_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetMetadata", ctx, mock.Anything).Return(nil, grpcNotFound())
		_, err := c.GetMetadata(ctx, "k")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_update_metadata_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("UpdateMetadata", ctx, mock.Anything).Return(nil, grpcNotFound())
		err := c.UpdateMetadata(ctx, "k", &Metadata{})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_remove_policy_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemovePolicy", ctx, mock.Anything).Return(nil, grpcNotFound())
		err := c.RemovePolicy(ctx, "p1")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_get_replication_policy_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationPolicy", ctx, mock.Anything).Return(nil, grpcNotFound())
		_, err := c.GetReplicationPolicy(ctx, "r1")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_get_replication_status_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationStatus", ctx, mock.Anything).Return(nil, grpcNotFound())
		_, err := c.GetReplicationStatus(ctx, "r1")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("grpc_remove_replication_policy_not_found", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemoveReplicationPolicy", ctx, mock.Anything).Return(nil, grpcNotFound())
		err := c.RemoveReplicationPolicy(ctx, "r1")
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})

	// --------------------------------------------------- canonical sentinels

	t.Run("grpc_error_sentinel_mapping", func(t *testing.T) {
		// Every row of the canonical gRPC code table must surface as the
		// matching SDK sentinel via errors.Is, with the status code preserved.
		cases := []struct {
			code     codes.Code
			sentinel error
		}{
			{codes.InvalidArgument, ErrInvalidArgument},
			{codes.Unauthenticated, ErrUnauthenticated},
			{codes.PermissionDenied, ErrPermissionDenied},
			{codes.NotFound, ErrObjectNotFound},
			{codes.AlreadyExists, ErrAlreadyExists},
			{codes.ResourceExhausted, ErrRateLimited},
		}
		for _, tc := range cases {
			c, mc := newGRPCMock()
			mc.On("Delete", ctx, mock.Anything).Return(nil, status.Error(tc.code, tc.code.String()))
			err := c.Delete(ctx, "k")
			require.Error(t, err, "code %s", tc.code)
			assert.Equal(t, tc.code, status.Code(err))
			assert.ErrorIs(t, err, tc.sentinel, "code %s", tc.code)
		}

		// Rate limiting keeps the retryable temporary-failure contract.
		c, mc := newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(nil, status.Error(codes.ResourceExhausted, "throttled"))
		assert.ErrorIs(t, c.Delete(ctx, "k"), ErrTemporaryFailure)

		// Internal stays a plain server error with no sentinel attached.
		c, mc = newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(nil, grpcServerErr())
		err := c.Delete(ctx, "k")
		require.Error(t, err)
		for _, tc := range cases {
			assert.NotErrorIs(t, err, tc.sentinel)
		}
	})

	// --------------------------------------------------------- cross-cutting

	t.Run("grpc_metadata_round_trip", func(t *testing.T) {
		c, mc := newGRPCMock()
		want := &objstorepb.Metadata{
			ContentType:     "text/plain",
			ContentEncoding: "gzip",
			Custom:          map[string]string{"author": "alice"},
		}
		// Put carries metadata in the proto request fields.
		mc.On("Put", ctx, mock.MatchedBy(func(req *objstorepb.PutRequest) bool {
			return req.Metadata != nil &&
				req.Metadata.ContentType == "text/plain" &&
				req.Metadata.ContentEncoding == "gzip" &&
				req.Metadata.Custom["author"] == "alice"
		})).Return(&objstorepb.PutResponse{Success: true}, nil)
		mc.On("Get", ctx, mock.Anything).Return(&MockGetStream{chunks: []*objstorepb.GetResponse{
			{Data: []byte("body"), Metadata: want},
		}}, nil)
		mc.On("GetMetadata", ctx, mock.Anything).Return(&objstorepb.MetadataResponse{
			Success: true, Metadata: want,
		}, nil)

		_, err := c.Put(ctx, "k", []byte("body"), &Metadata{
			ContentType:     "text/plain",
			ContentEncoding: "gzip",
			Custom:          map[string]string{"author": "alice"},
		})
		require.NoError(t, err)

		got, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "text/plain", got.Metadata.ContentType)
		assert.Equal(t, "gzip", got.Metadata.ContentEncoding)
		assert.Equal(t, "alice", got.Metadata.Custom["author"])

		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "text/plain", md.ContentType)
		assert.Equal(t, "gzip", md.ContentEncoding)
		assert.Equal(t, "alice", md.Custom["author"])
	})

	t.Run("grpc_validation_empty_key", func(t *testing.T) {
		c, mc := newGRPCMock()
		// No mock expectations are set: a rejected empty key must not reach
		// the transport.
		_, err := c.Put(ctx, "", []byte("d"), nil)
		assert.ErrorIs(t, err, ErrInvalidKey)
		mc.AssertNotCalled(t, "Put", mock.Anything, mock.Anything)
	})
}
