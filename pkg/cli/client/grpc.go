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
	"errors"
	"fmt"
	"io"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
		return nil, ErrServerURLRequired
	}

	// Note: TLS configuration can be added via DialOption parameters
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(config.ServerURL, opts...)
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
	header := &objstorepb.PutStreamHeader{Key: key}
	if metadata != nil {
		header.Metadata = metadataToProto(metadata)
	}

	// Canceling the stream on an early return (a read error) aborts the
	// put on the server, which then stores nothing.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.client.PutStream(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&objstorepb.PutStreamRequest{
		Payload: &objstorepb.PutStreamRequest_Header{Header: header},
	}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	// The server sends its response headers once it accepts the header
	// message. Without them the stream already ended: a refusal, or a server
	// older than PutStream, which gets the unary Put. Nothing has been read
	// from the reader yet.
	if md, herr := stream.Header(); herr != nil || md == nil {
		_, err := stream.CloseAndRecv()
		if err == nil {
			err = herr
		}
		if status.Code(err) == codes.Unimplemented {
			return c.putUnary(ctx, key, reader, metadata)
		}
		return err
	}

	if err := sendPutChunks(reader, func(chunk []byte) error {
		return stream.Send(&objstorepb.PutStreamRequest{
			Payload: &objstorepb.PutStreamRequest_Chunk{Chunk: chunk},
		})
	}); err != nil {
		if errors.Is(err, io.EOF) {
			// The server ended the stream; CloseAndRecv carries its status.
			_, err = stream.CloseAndRecv()
		}
		return err
	}

	_, err = stream.CloseAndRecv()
	return err
}

// putStreamChunkSize is the size of the data chunks Put sends over PutStream:
// 1 MiB less 1 KiB of headroom for the message framing, so a chunk message
// fits the smallest receive limit the server accepts (1 MiB).
const putStreamChunkSize = 1<<20 - 1<<10

// sendPutChunks reads reader in chunks of putStreamChunkSize and passes each
// to send, holding one chunk in memory at a time. Each chunk is a fresh
// buffer, since gRPC forbids modifying a message after it is sent. A read
// error is returned wrapped; a send error is returned as is.
func sendPutChunks(reader io.Reader, send func([]byte) error) error {
	for {
		buf := make([]byte, putStreamChunkSize)
		n, rerr := fillPutChunk(reader, buf)
		if rerr != nil && rerr != io.EOF { // io.Reader returns io.EOF itself, unwrapped
			// The caller cancels the stream, so the server stores nothing.
			return fmt.Errorf("read object data: %w", rerr)
		}
		if n > 0 {
			if err := send(buf[:n]); err != nil {
				return err
			}
		}
		if rerr != nil {
			return nil
		}
	}
}

// fillPutChunk reads from r until buf is full or r returns an error. Only
// io.EOF ends the data: it is returned with the bytes read before it. Any
// other error, io.ErrUnexpectedEOF from the source included, is returned
// as is, so the put is aborted instead of storing a short object.
func fillPutChunk(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// putUnary stores the object with the unary Put RPC, for a server that has
// no PutStream. It holds the whole object in memory, so it is subject to the
// server's message size limit.
func (c *GRPCClient) putUnary(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
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
		defer func() { _ = pw.Close() }()

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
			return nil, ErrMaxResultsOverflow
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
		return fmt.Errorf("%w: %s", ErrServerNotServing, resp.Message)
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
		return nil, ErrNoSyncResult
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
		return nil, ErrNoStatus
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
