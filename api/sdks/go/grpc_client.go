// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCClient implements the Client interface using gRPC protocol.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client objstorepb.ObjectStoreClient
	config *ClientConfig
}

// newGRPCClient creates a new gRPC client.
func newGRPCClient(config *ClientConfig) (*GRPCClient, error) {
	if config == nil {
		return nil, ErrInvalidConfig
	}

	var opts []grpc.DialOption

	// Configure TLS
	if config.UseTLS {
		tlsConfig, err := buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set message size limits
	if config.MaxRecvMsgSize > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(config.MaxRecvMsgSize)))
	}
	if config.MaxSendMsgSize > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(config.MaxSendMsgSize)))
	}

	// Connect to server using grpc.NewClient (replaces deprecated grpc.DialContext)
	conn, err := grpc.NewClient(config.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	return &GRPCClient{
		conn:   conn,
		client: objstorepb.NewObjectStoreClient(conn),
		config: config,
	}, nil
}

// Put stores an object.
func (c *GRPCClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateData(data); err != nil {
		return nil, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*PutResult, error) {
		req := &objstorepb.PutRequest{
			Key:      key,
			Data:     data,
			Metadata: metadataToProto(metadata),
		}

		resp, err := c.client.Put(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("put operation failed: %w", err)
		}

		return &PutResult{
			Success: resp.Success,
			Message: resp.Message,
			ETag:    resp.Etag,
		}, nil
	})
}

// Get retrieves an object.
func (c *GRPCClient) Get(ctx context.Context, key string) (*GetResult, error) {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return nil, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*GetResult, error) {
		req := &objstorepb.GetRequest{
			Key: key,
		}

		stream, err := c.client.Get(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("get operation failed: %w", err)
		}

		var data bytes.Buffer
		var metadata *Metadata

		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("stream receive failed: %w", err)
			}

			if chunk.Metadata != nil && metadata == nil {
				metadata = metadataFromProto(chunk.Metadata)
			}

			data.Write(chunk.Data)
		}

		return &GetResult{
			Data:     data.Bytes(),
			Metadata: metadata,
		}, nil
	})
}

// Delete removes an object.
func (c *GRPCClient) Delete(ctx context.Context, key string) error {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.DeleteRequest{
			Key: key,
		}

		resp, err := c.client.Delete(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("delete operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// List returns a list of objects.
func (c *GRPCClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	if opts == nil {
		opts = &ListOptions{}
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*ListResult, error) {
		req := &objstorepb.ListRequest{
			Prefix:       opts.Prefix,
			Delimiter:    opts.Delimiter,
			MaxResults:   opts.MaxResults,
			ContinueFrom: opts.ContinueFrom,
		}

		resp, err := c.client.List(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("list operation failed: %w", err)
		}

		objects := make([]*ObjectInfo, len(resp.Objects))
		for i, obj := range resp.Objects {
			objects[i] = &ObjectInfo{
				Key:      obj.Key,
				Metadata: metadataFromProto(obj.Metadata),
			}
		}

		return &ListResult{
			Objects:        objects,
			CommonPrefixes: resp.CommonPrefixes,
			NextToken:      resp.NextToken,
			Truncated:      resp.Truncated,
		}, nil
	})
}

// Exists checks if an object exists.
func (c *GRPCClient) Exists(ctx context.Context, key string) (bool, error) {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return false, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (bool, error) {
		req := &objstorepb.ExistsRequest{
			Key: key,
		}

		resp, err := c.client.Exists(ctx, req)
		if err != nil {
			return false, fmt.Errorf("exists operation failed: %w", err)
		}

		return resp.Exists, nil
	})
}

// GetMetadata retrieves object metadata.
func (c *GRPCClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return nil, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*Metadata, error) {
		req := &objstorepb.GetMetadataRequest{
			Key: key,
		}

		resp, err := c.client.GetMetadata(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("get metadata operation failed: %w", err)
		}

		if !resp.Success {
			return nil, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return metadataFromProto(resp.Metadata), nil
	})
}

// UpdateMetadata updates object metadata.
func (c *GRPCClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	// Validate inputs
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateMetadata(metadata); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.UpdateMetadataRequest{
			Key:      key,
			Metadata: metadataToProto(metadata),
		}

		resp, err := c.client.UpdateMetadata(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("update metadata operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// Health performs a health check.
func (c *GRPCClient) Health(ctx context.Context) (*HealthStatus, error) {
	req := &objstorepb.HealthRequest{}

	resp, err := c.client.Health(ctx, req)
	if err != nil {
		return nil, err
	}

	status := "UNKNOWN"
	switch resp.Status {
	case objstorepb.HealthResponse_SERVING:
		status = "SERVING"
	case objstorepb.HealthResponse_NOT_SERVING:
		status = "NOT_SERVING"
	}

	return &HealthStatus{
		Status:  status,
		Message: resp.Message,
	}, nil
}

// Archive copies an object to archival storage.
func (c *GRPCClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	req := &objstorepb.ArchiveRequest{
		Key:                 key,
		DestinationType:     destinationType,
		DestinationSettings: settings,
	}

	resp, err := c.client.Archive(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
	}

	return nil
}

// AddPolicy adds a lifecycle policy.
func (c *GRPCClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	// Validate inputs
	if err := validateLifecyclePolicy(policy); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.AddPolicyRequest{
			Policy: lifecyclePolicyToProto(policy),
		}

		resp, err := c.client.AddPolicy(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("add policy operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// RemovePolicy removes a lifecycle policy.
func (c *GRPCClient) RemovePolicy(ctx context.Context, policyID string) error {
	// Validate inputs
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.RemovePolicyRequest{
			Id: policyID,
		}

		resp, err := c.client.RemovePolicy(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("remove policy operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// GetPolicies retrieves lifecycle policies.
func (c *GRPCClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	req := &objstorepb.GetPoliciesRequest{
		Prefix: prefix,
	}

	resp, err := c.client.GetPolicies(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
	}

	policies := make([]*LifecyclePolicy, len(resp.Policies))
	for i, p := range resp.Policies {
		policies[i] = lifecyclePolicyFromProto(p)
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies.
func (c *GRPCClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	req := &objstorepb.ApplyPoliciesRequest{}

	resp, err := c.client.ApplyPolicies(ctx, req)
	if err != nil {
		return nil, err
	}

	return &ApplyPoliciesResult{
		Success:          resp.Success,
		PoliciesCount:    resp.PoliciesCount,
		ObjectsProcessed: resp.ObjectsProcessed,
		Message:          resp.Message,
	}, nil
}

// AddReplicationPolicy adds a replication policy.
func (c *GRPCClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	// Validate inputs
	if err := validateReplicationPolicy(policy); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.AddReplicationPolicyRequest{
			Policy: replicationPolicyToProto(policy),
		}

		resp, err := c.client.AddReplicationPolicy(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("add replication policy operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// RemoveReplicationPolicy removes a replication policy.
func (c *GRPCClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	// Validate inputs
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	// Execute with retry logic
	_, err := retryWrapper(ctx, c.config.Retry, func() (struct{}, error) {
		req := &objstorepb.RemoveReplicationPolicyRequest{
			Id: policyID,
		}

		resp, err := c.client.RemoveReplicationPolicy(ctx, req)
		if err != nil {
			return struct{}{}, fmt.Errorf("remove replication policy operation failed: %w", err)
		}

		if !resp.Success {
			return struct{}{}, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		return struct{}{}, nil
	})
	return err
}

// GetReplicationPolicies retrieves all replication policies.
func (c *GRPCClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	req := &objstorepb.GetReplicationPoliciesRequest{}

	resp, err := c.client.GetReplicationPolicies(ctx, req)
	if err != nil {
		return nil, err
	}

	policies := make([]*ReplicationPolicy, len(resp.Policies))
	for i, p := range resp.Policies {
		policies[i] = replicationPolicyFromProto(p)
		if p.LastSyncTime != nil {
			policies[i].LastSyncTime = p.LastSyncTime.AsTime()
		}
	}

	return policies, nil
}

// GetReplicationPolicy retrieves a specific replication policy.
func (c *GRPCClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	// Validate inputs
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*ReplicationPolicy, error) {
		req := &objstorepb.GetReplicationPolicyRequest{
			Id: policyID,
		}

		resp, err := c.client.GetReplicationPolicy(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("get replication policy operation failed: %w", err)
		}

		policy := replicationPolicyFromProto(resp.Policy)
		if resp.Policy.LastSyncTime != nil {
			policy.LastSyncTime = resp.Policy.LastSyncTime.AsTime()
		}

		return policy, nil
	})
}

// TriggerReplication triggers replication synchronization.
func (c *GRPCClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &TriggerReplicationOptions{}
	}

	req := &objstorepb.TriggerReplicationRequest{
		PolicyId:    opts.PolicyID,
		Parallel:    opts.Parallel,
		WorkerCount: opts.WorkerCount,
	}

	resp, err := c.client.TriggerReplication(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
	}

	result := &SyncResult{}
	if resp.Result != nil {
		result.PolicyID = resp.Result.PolicyId
		result.Synced = resp.Result.Synced
		result.Deleted = resp.Result.Deleted
		result.Failed = resp.Result.Failed
		result.BytesTotal = resp.Result.BytesTotal
		result.DurationMs = resp.Result.DurationMs
		result.Errors = resp.Result.Errors
	}

	return result, nil
}

// GetReplicationStatus retrieves replication status and metrics.
func (c *GRPCClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	// Validate inputs
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	// Execute with retry logic
	return retryWrapper(ctx, c.config.Retry, func() (*ReplicationStatus, error) {
		req := &objstorepb.GetReplicationStatusRequest{
			Id: policyID,
		}

		resp, err := c.client.GetReplicationStatus(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("get replication status operation failed: %w", err)
		}

		if !resp.Success {
			return nil, fmt.Errorf("%w: %s", ErrOperationFailed, resp.Message)
		}

		status := &ReplicationStatus{}
		if resp.Status != nil {
			status.PolicyID = resp.Status.PolicyId
			status.SourceBackend = resp.Status.SourceBackend
			status.DestinationBackend = resp.Status.DestinationBackend
			status.Enabled = resp.Status.Enabled
			status.TotalObjectsSynced = resp.Status.TotalObjectsSynced
			status.TotalObjectsDeleted = resp.Status.TotalObjectsDeleted
			status.TotalBytesSynced = resp.Status.TotalBytesSynced
			status.TotalErrors = resp.Status.TotalErrors
			status.AverageSyncDurationMs = resp.Status.AverageSyncDurationMs
			status.SyncCount = resp.Status.SyncCount
			if resp.Status.LastSyncTime != nil {
				status.LastSyncTime = resp.Status.LastSyncTime.AsTime()
			}
		}

		return status, nil
	})
}

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// buildTLSConfig creates a TLS configuration from the client config.
func buildTLSConfig(config *ClientConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	// Load CA certificate if provided
	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate if provided
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Ensure GRPCClient implements Client interface
var _ Client = (*GRPCClient)(nil)

// Helper to convert timestamp
func timestampToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
