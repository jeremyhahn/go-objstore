// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// Language-specific gRPC extras beyond the canonical matrix: client-side
// validation rejection for every validated op, the operation-failed (Success
// false) response path, NOT_SERVING health mapping, and the timestamp helper.
// These exercise Go-specific branches in grpc_client.go that the canonical
// success/error/not_found cells do not reach.

import (
	"context"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGRPCClientValidationRejected(t *testing.T) {
	ctx := context.Background()
	c, mc := newGRPCMock()

	cases := []struct {
		name string
		call func() error
		want error
	}{
		{"put_empty_key", func() error { _, e := c.Put(ctx, "", []byte("d"), nil); return e }, ErrInvalidKey},
		{"put_nil_data", func() error { _, e := c.Put(ctx, "k", nil, nil); return e }, ErrInvalidData},
		{"get_empty_key", func() error { _, e := c.Get(ctx, ""); return e }, ErrInvalidKey},
		{"delete_empty_key", func() error { return c.Delete(ctx, "") }, ErrInvalidKey},
		{"exists_empty_key", func() error { _, e := c.Exists(ctx, ""); return e }, ErrInvalidKey},
		{"get_metadata_empty_key", func() error { _, e := c.GetMetadata(ctx, ""); return e }, ErrInvalidKey},
		{"update_metadata_empty_key", func() error { return c.UpdateMetadata(ctx, "", &Metadata{}) }, ErrInvalidKey},
		{"update_metadata_nil", func() error { return c.UpdateMetadata(ctx, "k", nil) }, ErrInvalidMetadata},
		{"add_policy_nil", func() error { return c.AddPolicy(ctx, nil) }, ErrInvalidPolicy},
		{"add_policy_empty_id", func() error { return c.AddPolicy(ctx, &LifecyclePolicy{}) }, ErrInvalidPolicyID},
		{"remove_policy_empty", func() error { return c.RemovePolicy(ctx, "") }, ErrInvalidPolicyID},
		{"add_repl_nil", func() error { return c.AddReplicationPolicy(ctx, nil) }, ErrInvalidPolicy},
		{"add_repl_empty_id", func() error { return c.AddReplicationPolicy(ctx, &ReplicationPolicy{}) }, ErrInvalidPolicyID},
		{"remove_repl_empty", func() error { return c.RemoveReplicationPolicy(ctx, "") }, ErrInvalidPolicyID},
		{"get_repl_policy_empty", func() error { _, e := c.GetReplicationPolicy(ctx, ""); return e }, ErrInvalidPolicyID},
		{"get_repl_status_empty", func() error { _, e := c.GetReplicationStatus(ctx, ""); return e }, ErrInvalidPolicyID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.call(), tc.want)
		})
	}
	// None of the rejected calls should have reached the transport.
	mc.AssertExpectations(t)
}

func TestGRPCClientOperationFailed(t *testing.T) {
	ctx := context.Background()

	t.Run("delete", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Delete", ctx, mock.Anything).Return(&objstorepb.DeleteResponse{Success: false, Message: "x"}, nil)
		assert.ErrorIs(t, c.Delete(ctx, "k"), ErrOperationFailed)
	})
	t.Run("get_metadata", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetMetadata", ctx, mock.Anything).Return(&objstorepb.MetadataResponse{Success: false}, nil)
		_, err := c.GetMetadata(ctx, "k")
		assert.ErrorIs(t, err, ErrOperationFailed)
	})
	t.Run("update_metadata", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("UpdateMetadata", ctx, mock.Anything).Return(&objstorepb.UpdateMetadataResponse{Success: false}, nil)
		assert.ErrorIs(t, c.UpdateMetadata(ctx, "k", &Metadata{}), ErrOperationFailed)
	})
	t.Run("archive", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("Archive", ctx, mock.Anything).Return(&objstorepb.ArchiveResponse{Success: false}, nil)
		assert.ErrorIs(t, c.Archive(ctx, "k", "g", nil), ErrOperationFailed)
	})
	t.Run("add_policy", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddPolicy", ctx, mock.Anything).Return(&objstorepb.AddPolicyResponse{Success: false}, nil)
		assert.ErrorIs(t, c.AddPolicy(ctx, &LifecyclePolicy{ID: "p"}), ErrOperationFailed)
	})
	t.Run("remove_policy", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemovePolicy", ctx, mock.Anything).Return(&objstorepb.RemovePolicyResponse{Success: false}, nil)
		assert.ErrorIs(t, c.RemovePolicy(ctx, "p"), ErrOperationFailed)
	})
	t.Run("get_policies", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetPolicies", ctx, mock.Anything).Return(&objstorepb.GetPoliciesResponse{Success: false}, nil)
		_, err := c.GetPolicies(ctx, "")
		assert.ErrorIs(t, err, ErrOperationFailed)
	})
	t.Run("add_repl", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("AddReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.AddReplicationPolicyResponse{Success: false}, nil)
		assert.ErrorIs(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r"}), ErrOperationFailed)
	})
	t.Run("remove_repl", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("RemoveReplicationPolicy", ctx, mock.Anything).Return(&objstorepb.RemoveReplicationPolicyResponse{Success: false}, nil)
		assert.ErrorIs(t, c.RemoveReplicationPolicy(ctx, "r"), ErrOperationFailed)
	})
	t.Run("trigger_repl", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("TriggerReplication", ctx, mock.Anything).Return(&objstorepb.TriggerReplicationResponse{Success: false}, nil)
		_, err := c.TriggerReplication(ctx, nil)
		assert.ErrorIs(t, err, ErrOperationFailed)
	})
	t.Run("get_repl_status", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationStatus", ctx, mock.Anything).Return(&objstorepb.GetReplicationStatusResponse{Success: false}, nil)
		_, err := c.GetReplicationStatus(ctx, "r")
		assert.ErrorIs(t, err, ErrOperationFailed)
	})
}

func TestGRPCClientHealthNotServing(t *testing.T) {
	ctx := context.Background()
	c, mc := newGRPCMock()
	mc.On("Health", ctx, mock.Anything).Return(&objstorepb.HealthResponse{Status: objstorepb.HealthResponse_NOT_SERVING}, nil)
	st, err := c.Health(ctx)
	require.NoError(t, err)
	assert.Equal(t, "NOT_SERVING", st.Status)
}

func TestGRPCClientTriggerAndStatusNilSubmessages(t *testing.T) {
	ctx := context.Background()
	t.Run("trigger_nil_result", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("TriggerReplication", ctx, mock.Anything).Return(&objstorepb.TriggerReplicationResponse{Success: true}, nil)
		res, err := c.TriggerReplication(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, int32(0), res.Synced)
	})
	t.Run("status_nil_status", func(t *testing.T) {
		c, mc := newGRPCMock()
		mc.On("GetReplicationStatus", ctx, mock.Anything).Return(&objstorepb.GetReplicationStatusResponse{Success: true}, nil)
		st, err := c.GetReplicationStatus(ctx, "r")
		require.NoError(t, err)
		assert.Equal(t, int64(0), st.TotalObjectsSynced)
	})
}

func TestTimestampToProtoHelper(t *testing.T) {
	assert.Nil(t, timestampToProto(time.Time{}))
	now := time.Now()
	assert.Equal(t, now.Unix(), timestampToProto(now).AsTime().Unix())
}
