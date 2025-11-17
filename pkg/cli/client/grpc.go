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
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCClient implements the Client interface for gRPC servers
type GRPCClient struct {
	conn   *grpc.ClientConn
	client objstorepb.ObjectStoreClient
}

// NewGRPCClient creates a new gRPC client
func NewGRPCClient(config *Config) (*GRPCClient, error) {
	if config.ServerURL == "" {
		return nil, fmt.Errorf("server URL is required")
	}

	// Note: TLS configuration can be added via DialOption parameters
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.Dial(config.ServerURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
	}, nil
}

// Put uploads an object
func (c *GRPCClient) Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	req := &objstorepb.PutRequest{
		Key:  key,
		Data: data,
	}

	if metadata != nil {
		req.Metadata = metadataToProto(metadata)
	}

	_, err = c.client.Put(ctx, req)
	return err
}

// Get retrieves an object
func (c *GRPCClient) Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error) {
	req := &objstorepb.GetRequest{
		Key: key,
	}

	stream, err := c.client.Get(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	// Receive first chunk to get metadata
	firstChunk, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}

	metadata := protoToMetadata(firstChunk.Metadata)

	// Create a pipe to stream the data
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		// Write first chunk
		if len(firstChunk.Data) > 0 {
			if _, err := pw.Write(firstChunk.Data); err != nil {
				return
			}
		}

		// Stream remaining chunks
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(chunk.Data); err != nil {
				return
			}
		}
	}()

	return pr, metadata, nil
}

// Delete removes an object
func (c *GRPCClient) Delete(ctx context.Context, key string) error {
	req := &objstorepb.DeleteRequest{
		Key: key,
	}

	_, err := c.client.Delete(ctx, req)
	return err
}

// Exists checks if an object exists
func (c *GRPCClient) Exists(ctx context.Context, key string) (bool, error) {
	req := &objstorepb.ExistsRequest{
		Key: key,
	}

	resp, err := c.client.Exists(ctx, req)
	if err != nil {
		return false, err
	}

	return resp.Exists, nil
}

// List lists objects with optional filters
func (c *GRPCClient) List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	req := &objstorepb.ListRequest{}

	if opts != nil {
		req.Prefix = opts.Prefix
		req.Delimiter = opts.Delimiter
		// Safe conversion with overflow check
		if opts.MaxResults > 2147483647 {
			return nil, fmt.Errorf("MaxResults exceeds int32 range")
		}
		req.MaxResults = int32(opts.MaxResults) // #nosec G115 -- overflow checked above
		req.ContinueFrom = opts.ContinueFrom
	}

	resp, err := c.client.List(ctx, req)
	if err != nil {
		return nil, err
	}

	result := &common.ListResult{
		CommonPrefixes: resp.CommonPrefixes,
		NextToken:      resp.NextToken,
		Truncated:      resp.Truncated,
	}

	for _, obj := range resp.Objects {
		result.Objects = append(result.Objects, &common.ObjectInfo{
			Key:      obj.Key,
			Metadata: protoToMetadata(obj.Metadata),
		})
	}

	return result, nil
}

// GetMetadata retrieves object metadata
func (c *GRPCClient) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	req := &objstorepb.GetMetadataRequest{
		Key: key,
	}

	resp, err := c.client.GetMetadata(ctx, req)
	if err != nil {
		return nil, err
	}

	return protoToMetadata(resp.Metadata), nil
}

// UpdateMetadata updates object metadata
func (c *GRPCClient) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	req := &objstorepb.UpdateMetadataRequest{
		Key:      key,
		Metadata: metadataToProto(metadata),
	}

	_, err := c.client.UpdateMetadata(ctx, req)
	return err
}

// Archive archives an object
func (c *GRPCClient) Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error {
	req := &objstorepb.ArchiveRequest{
		Key:                 key,
		DestinationType:     destinationType,
		DestinationSettings: destinationSettings,
	}

	_, err := c.client.Archive(ctx, req)
	return err
}

// AddPolicy adds a lifecycle policy
func (c *GRPCClient) AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error {
	req := &objstorepb.AddPolicyRequest{
		Policy: lifecyclePolicyToProto(&policy),
	}

	_, err := c.client.AddPolicy(ctx, req)
	return err
}

// RemovePolicy removes a lifecycle policy
func (c *GRPCClient) RemovePolicy(ctx context.Context, policyID string) error {
	req := &objstorepb.RemovePolicyRequest{
		Id: policyID,
	}

	_, err := c.client.RemovePolicy(ctx, req)
	return err
}

// GetPolicies retrieves all lifecycle policies
func (c *GRPCClient) GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error) {
	req := &objstorepb.GetPoliciesRequest{}

	resp, err := c.client.GetPolicies(ctx, req)
	if err != nil {
		return nil, err
	}

	policies := make([]common.LifecyclePolicy, len(resp.Policies))
	for i, p := range resp.Policies {
		policies[i] = common.LifecyclePolicy{
			ID:        p.Id,
			Prefix:    p.Prefix,
			Retention: time.Duration(p.RetentionSeconds) * time.Second,
			Action:    p.Action,
		}
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies
func (c *GRPCClient) ApplyPolicies(ctx context.Context) (policiesCount int, objectsProcessed int, err error) {
	req := &objstorepb.ApplyPoliciesRequest{}

	resp, err := c.client.ApplyPolicies(ctx, req)
	if err != nil {
		return 0, 0, err
	}

	return int(resp.PoliciesCount), int(resp.ObjectsProcessed), nil
}

// Health checks server health
func (c *GRPCClient) Health(ctx context.Context) error {
	req := &objstorepb.HealthRequest{}

	resp, err := c.client.Health(ctx, req)
	if err != nil {
		return err
	}

	if resp.Status != objstorepb.HealthResponse_SERVING {
		return fmt.Errorf("server not serving: %s", resp.Message)
	}

	return nil
}

// Close closes the gRPC connection
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Helper functions for converting between proto and common types

func metadataToProto(m *common.Metadata) *objstorepb.Metadata {
	if m == nil {
		return nil
	}

	proto := &objstorepb.Metadata{
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		Size:            m.Size,
		Etag:            m.ETag,
		Custom:          m.Custom,
	}

	if !m.LastModified.IsZero() {
		proto.LastModified = timestamppb.New(m.LastModified)
	}

	return proto
}

func protoToMetadata(p *objstorepb.Metadata) *common.Metadata {
	if p == nil {
		return nil
	}

	m := &common.Metadata{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		Size:            p.Size,
		ETag:            p.Etag,
		Custom:          p.Custom,
	}

	if p.LastModified != nil {
		m.LastModified = p.LastModified.AsTime()
	}

	return m
}

func lifecyclePolicyToProto(p *common.LifecyclePolicy) *objstorepb.LifecyclePolicy {
	if p == nil {
		return nil
	}

	return &objstorepb.LifecyclePolicy{
		Id:               p.ID,
		Prefix:           p.Prefix,
		RetentionSeconds: int64(p.Retention.Seconds()),
		Action:           p.Action,
		// Note: Destination cannot be serialized, needs to be handled separately
	}
}

// Replication operations
func (c *GRPCClient) AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error {
	req := &objstorepb.AddReplicationPolicyRequest{
		Policy: replicationPolicyToProto(policy),
	}

	_, err := c.client.AddReplicationPolicy(ctx, req)
	return err
}

func (c *GRPCClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	req := &objstorepb.RemoveReplicationPolicyRequest{
		Id: policyID,
	}

	_, err := c.client.RemoveReplicationPolicy(ctx, req)
	return err
}

func (c *GRPCClient) GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error) {
	req := &objstorepb.GetReplicationPolicyRequest{
		Id: policyID,
	}

	resp, err := c.client.GetReplicationPolicy(ctx, req)
	if err != nil {
		return nil, err
	}

	policy := protoToReplicationPolicy(resp.Policy)
	return &policy, nil
}

func (c *GRPCClient) GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error) {
	req := &objstorepb.GetReplicationPoliciesRequest{}

	resp, err := c.client.GetReplicationPolicies(ctx, req)
	if err != nil {
		return nil, err
	}

	policies := make([]common.ReplicationPolicy, len(resp.Policies))
	for i, p := range resp.Policies {
		policies[i] = protoToReplicationPolicy(p)
	}

	return policies, nil
}

func (c *GRPCClient) TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error) {
	req := &objstorepb.TriggerReplicationRequest{
		PolicyId: policyID,
	}

	resp, err := c.client.TriggerReplication(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Result == nil {
		return nil, fmt.Errorf("no sync result returned")
	}

	return &common.SyncResult{
		PolicyID:   resp.Result.PolicyId,
		Synced:     int(resp.Result.Synced),
		Deleted:    int(resp.Result.Deleted),
		Failed:     int(resp.Result.Failed),
		BytesTotal: resp.Result.BytesTotal,
		Duration:   time.Duration(resp.Result.DurationMs) * time.Millisecond,
	}, nil
}

func (c *GRPCClient) GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error) {
	req := &objstorepb.GetReplicationStatusRequest{
		Id: policyID,
	}

	resp, err := c.client.GetReplicationStatus(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Status == nil {
		return nil, fmt.Errorf("no status returned")
	}

	return &replication.ReplicationStatus{
		PolicyID:            resp.Status.PolicyId,
		SourceBackend:       resp.Status.SourceBackend,
		DestinationBackend:  resp.Status.DestinationBackend,
		Enabled:             resp.Status.Enabled,
		TotalObjectsSynced:  resp.Status.TotalObjectsSynced,
		TotalObjectsDeleted: resp.Status.TotalObjectsDeleted,
		TotalBytesSynced:    resp.Status.TotalBytesSynced,
		TotalErrors:         resp.Status.TotalErrors,
		LastSyncTime:        resp.Status.LastSyncTime.AsTime(),
		AverageSyncDuration: time.Duration(resp.Status.AverageSyncDurationMs) * time.Millisecond,
		SyncCount:           resp.Status.SyncCount,
	}, nil
}

// Helper functions for replication policy conversion
func replicationPolicyToProto(policy common.ReplicationPolicy) *objstorepb.ReplicationPolicy {
	var lastSyncTime *timestamppb.Timestamp
	if !policy.LastSyncTime.IsZero() {
		lastSyncTime = timestamppb.New(policy.LastSyncTime)
	}

	return &objstorepb.ReplicationPolicy{
		Id:                   policy.ID,
		SourceBackend:        policy.SourceBackend,
		SourceSettings:       policy.SourceSettings,
		SourcePrefix:         policy.SourcePrefix,
		DestinationBackend:   policy.DestinationBackend,
		DestinationSettings:  policy.DestinationSettings,
		CheckIntervalSeconds: int64(policy.CheckInterval.Seconds()),
		LastSyncTime:         lastSyncTime,
		Enabled:              policy.Enabled,
	}
}

func protoToReplicationPolicy(pb *objstorepb.ReplicationPolicy) common.ReplicationPolicy {
	var lastSyncTime time.Time
	if pb.LastSyncTime != nil {
		lastSyncTime = pb.LastSyncTime.AsTime()
	}

	return common.ReplicationPolicy{
		ID:                  pb.Id,
		SourceBackend:       pb.SourceBackend,
		SourceSettings:      pb.SourceSettings,
		SourcePrefix:        pb.SourcePrefix,
		DestinationBackend:  pb.DestinationBackend,
		DestinationSettings: pb.DestinationSettings,
		CheckInterval:       time.Duration(pb.CheckIntervalSeconds) * time.Second,
		LastSyncTime:        lastSyncTime,
		Enabled:             pb.Enabled,
	}
}
