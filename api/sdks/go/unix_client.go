// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// UnixClient implements the Client interface using a Unix domain socket with
// newline-delimited JSON-RPC 2.0 messages.  Auth is handled server-side via
// OS peercred; the client simply dials and sends requests.
type UnixClient struct {
	socketPath string
	config     *ClientConfig
	conn       net.Conn
	reader     *bufio.Reader // wraps conn; recreated together with it in dial
	closed     bool
	mu         sync.Mutex // serialises request/response pairs on the single conn
	nextID     atomic.Int64
}

// newUnixClient creates a new Unix-socket client.
func newUnixClient(config *ClientConfig) (*UnixClient, error) {
	if config == nil {
		return nil, ErrInvalidConfig
	}
	if config.Address == "" {
		return nil, fmt.Errorf("%w: socket path (Address) is required for unix protocol", ErrInvalidConfig)
	}

	c := &UnixClient{
		socketPath: config.Address,
		config:     config,
	}

	if err := c.dial(); err != nil {
		return nil, err
	}

	return c, nil
}

// dial establishes (or re-establishes) the connection.
func (c *UnixClient) dial() error {
	timeout := 10 * time.Second
	if c.config.ConnectionTimeout > 0 {
		timeout = c.config.ConnectionTimeout
	}

	conn, err := net.DialTimeout("unix", c.socketPath, timeout)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return nil
}

// dropConn closes the current connection and marks it nil so the next call
// re-dials lazily.  The server closes idle connections (30s read deadline),
// so any write/read failure or protocol violation must not poison the client
// permanently.  Callers must hold c.mu.
func (c *UnixClient) dropConn() {
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
	c.reader = nil
}

// call sends a single JSON-RPC request and decodes the result into dest.
// The connection is locked for the duration of each call so multiple
// goroutines can share one UnixClient safely.
func (c *UnixClient) call(ctx context.Context, method string, params any, dest any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("%w: client is closed", ErrConnectionFailed)
	}

	// Re-dial lazily after a previous failure dropped the connection.
	if c.conn == nil {
		if err := c.dial(); err != nil {
			return err
		}
	}

	id := c.nextID.Add(1)

	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	data = append(data, '\n')

	// Apply the ctx deadline or configured request timeout; clear any stale
	// deadline from a previous call otherwise. A failed SetDeadline would
	// leave the request able to hang indefinitely, so surface it instead of
	// ignoring it.
	deadline := time.Time{}
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else if c.config.RequestTimeout > 0 {
		deadline = time.Now().Add(c.config.RequestTimeout)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		c.dropConn()
		return fmt.Errorf("set deadline: %w", err)
	}

	if _, err = c.conn.Write(data); err != nil {
		c.dropConn()
		return fmt.Errorf("write request: %w", err)
	}

	// Read exactly one newline-delimited response.
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		c.dropConn()
		return fmt.Errorf("read response: %w", err)
	}

	var resp rpcResponse
	if err = json.Unmarshal(line, &resp); err != nil {
		c.dropConn()
		return fmt.Errorf("unmarshal response: %w", err)
	}

	// A response carrying a different ID means the connection is desynced
	// (e.g. a stale response from a timed-out call); it is poisoned and must
	// not be reused.
	if respID, ok := resp.ID.(float64); !ok || int64(respID) != id {
		c.dropConn()
		return fmt.Errorf("response id %v does not match request id %d", resp.ID, id)
	}

	if resp.Error != nil {
		return resp.Error.asSDKError("rpc")
	}

	if dest != nil && len(resp.Result) > 0 {
		if err = json.Unmarshal(resp.Result, dest); err != nil {
			return fmt.Errorf("unmarshal result: %w", err)
		}
	}

	return nil
}

// Put stores an object.
func (c *UnixClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateData(data); err != nil {
		return nil, err
	}

	params := map[string]any{
		"key":  key,
		"data": base64.StdEncoding.EncodeToString(data),
	}
	if metadata != nil {
		params["metadata"] = rpcMetadataParams(metadata)
	}

	// The Unix server returns a generic success/message map for put; the
	// payload carries nothing the SDK surfaces, so it is discarded.
	if err := c.call(ctx, "put", params, nil); err != nil {
		return nil, err
	}

	return &PutResult{
		Success: true,
	}, nil
}

// Get retrieves an object.
func (c *UnixClient) Get(ctx context.Context, key string) (*GetResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	params := map[string]string{"key": key}

	var result struct {
		Data     string `json:"data"`
		Metadata *struct {
			ContentType     string            `json:"content_type"`
			ContentEncoding string            `json:"content_encoding"`
			Custom          map[string]string `json:"custom"`
		} `json:"metadata"`
	}

	if err := c.call(ctx, "get", params, &result); err != nil {
		return nil, err
	}

	raw, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		return nil, fmt.Errorf("decode object data: %w", err)
	}

	var meta *Metadata
	if result.Metadata != nil {
		meta = &Metadata{
			ContentType:     result.Metadata.ContentType,
			ContentEncoding: result.Metadata.ContentEncoding,
			Custom:          result.Metadata.Custom,
		}
	}

	return &GetResult{
		Data:     raw,
		Metadata: meta,
	}, nil
}

// Delete removes an object.
func (c *UnixClient) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	params := map[string]string{"key": key}
	return c.call(ctx, "delete", params, nil)
}

// List returns a list of objects.
func (c *UnixClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	if opts == nil {
		opts = &ListOptions{}
	}

	params := map[string]any{
		"prefix":        opts.Prefix,
		"delimiter":     opts.Delimiter,
		"max_results":   int(opts.MaxResults),
		"continue_from": opts.ContinueFrom,
	}

	var result struct {
		Objects []struct {
			Key          string `json:"key"`
			Size         int64  `json:"size"`
			LastModified string `json:"last_modified"`
			ETag         string `json:"etag"`
		} `json:"objects"`
		NextCursor  string `json:"next_cursor"`
		IsTruncated bool   `json:"is_truncated"`
	}

	if err := c.call(ctx, "list", params, &result); err != nil {
		return nil, err
	}

	objects := make([]*ObjectInfo, len(result.Objects))
	for i, obj := range result.Objects {
		meta := &Metadata{
			Size: obj.Size,
			ETag: obj.ETag,
		}
		if obj.LastModified != "" {
			if t, err := time.Parse(time.RFC3339, obj.LastModified); err == nil {
				meta.LastModified = t
			}
		}
		objects[i] = &ObjectInfo{
			Key:      obj.Key,
			Metadata: meta,
		}
	}

	return &ListResult{
		Objects:   objects,
		NextToken: result.NextCursor,
		Truncated: result.IsTruncated,
	}, nil
}

// Exists checks if an object exists.
func (c *UnixClient) Exists(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	params := map[string]string{"key": key}

	var result struct {
		Exists bool `json:"exists"`
	}

	if err := c.call(ctx, "exists", params, &result); err != nil {
		return false, err
	}

	return result.Exists, nil
}

// GetMetadata retrieves object metadata.
func (c *UnixClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	params := map[string]string{"key": key}

	var result struct {
		ContentType     string            `json:"content_type"`
		ContentEncoding string            `json:"content_encoding"`
		Custom          map[string]string `json:"custom"`
	}

	if err := c.call(ctx, "get_metadata", params, &result); err != nil {
		return nil, err
	}

	return &Metadata{
		ContentType:     result.ContentType,
		ContentEncoding: result.ContentEncoding,
		Custom:          result.Custom,
	}, nil
}

// UpdateMetadata updates object metadata.
func (c *UnixClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateMetadata(metadata); err != nil {
		return err
	}

	params := map[string]any{
		"key":      key,
		"metadata": rpcMetadataParams(metadata),
	}

	return c.call(ctx, "update_metadata", params, nil)
}

// Health performs a health check.
func (c *UnixClient) Health(ctx context.Context) (*HealthStatus, error) {
	var result struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}

	if err := c.call(ctx, "health", map[string]any{}, &result); err != nil {
		return nil, err
	}

	return &HealthStatus{
		Status:  result.Status,
		Message: result.Version,
	}, nil
}

// Archive copies an object to archival storage.
func (c *UnixClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	params := map[string]any{
		"key":                  key,
		"destination_type":     destinationType,
		"destination_settings": settings,
	}

	return c.call(ctx, "archive", params, nil)
}

// AddPolicy adds a lifecycle policy.
func (c *UnixClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	if err := validateLifecyclePolicy(policy); err != nil {
		return err
	}

	// retention_seconds carries the exact retention and takes precedence
	// server-side; after_days is sent rounded down for older servers.
	params := map[string]any{
		"id":                policy.ID,
		"prefix":            policy.Prefix,
		"action":            policy.Action,
		"after_days":        policy.RetentionSeconds / 86400,
		"retention_seconds": policy.RetentionSeconds,
	}

	return c.call(ctx, "add_policy", params, nil)
}

// RemovePolicy removes a lifecycle policy.
func (c *UnixClient) RemovePolicy(ctx context.Context, policyID string) error {
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	params := map[string]string{"id": policyID}
	return c.call(ctx, "remove_policy", params, nil)
}

// GetPolicies retrieves lifecycle policies.
func (c *UnixClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	// The unix server returns policies as a BARE JSON array, and does not
	// filter by prefix server-side; filter client-side.
	var result []unixLifecyclePolicy
	if err := c.call(ctx, "get_policies", map[string]any{}, &result); err != nil {
		return nil, err
	}

	policies := make([]*LifecyclePolicy, 0, len(result))
	for i := range result {
		if prefix != "" && result[i].Prefix != prefix {
			continue
		}
		policies = append(policies, result[i].toModel())
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies.
func (c *UnixClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	var result struct {
		PoliciesCount    int `json:"policies_count"`
		ObjectsProcessed int `json:"objects_processed"`
	}

	if err := c.call(ctx, "apply_policies", map[string]any{}, &result); err != nil {
		return nil, err
	}

	return &ApplyPoliciesResult{
		Success:          true,
		PoliciesCount:    int32(result.PoliciesCount),
		ObjectsProcessed: int32(result.ObjectsProcessed),
	}, nil
}

// AddReplicationPolicy adds a replication policy.
func (c *UnixClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	if err := validateReplicationPolicy(policy); err != nil {
		return err
	}

	mode := "transparent"
	if policy.ReplicationMode == ReplicationModeOpaque {
		mode = "opaque"
	}

	params := map[string]any{
		"id":               policy.ID,
		"source_prefix":    policy.SourcePrefix,
		"destination_type": policy.DestinationBackend,
		"destination":      policy.DestinationSettings,
		"enabled":          policy.Enabled,
		"replication_mode": mode,
	}

	return c.call(ctx, "add_replication_policy", params, nil)
}

// RemoveReplicationPolicy removes a replication policy.
func (c *UnixClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	params := map[string]string{"id": policyID}
	return c.call(ctx, "remove_replication_policy", params, nil)
}

// GetReplicationPolicies retrieves all replication policies.
func (c *UnixClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	// Returned as a BARE JSON array (see pkg/server/unix/handlers.go).
	var result []unixReplicationPolicy
	if err := c.call(ctx, "get_replication_policies", map[string]any{}, &result); err != nil {
		return nil, err
	}

	policies := make([]*ReplicationPolicy, len(result))
	for i := range result {
		policies[i] = result[i].toModel()
	}

	return policies, nil
}

// GetReplicationPolicy retrieves a specific replication policy.
func (c *UnixClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	params := map[string]string{"id": policyID}

	var result unixReplicationPolicy
	if err := c.call(ctx, "get_replication_policy", params, &result); err != nil {
		return nil, err
	}

	return result.toModel(), nil
}

// TriggerReplication triggers replication synchronization.
func (c *UnixClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &TriggerReplicationOptions{}
	}

	// The unix protocol identifies the policy with "id" (see
	// pkg/server/unix/protocol.go ReplicationPolicyIDParams); an empty id
	// triggers all policies.
	params := map[string]any{
		"id": opts.PolicyID,
	}

	var result struct {
		ObjectsSynced    int      `json:"objects_synced"`
		ObjectsFailed    int      `json:"objects_failed"`
		BytesTransferred int64    `json:"bytes_transferred"`
		Errors           []string `json:"errors"`
	}

	if err := c.call(ctx, "trigger_replication", params, &result); err != nil {
		return nil, err
	}

	return &SyncResult{
		PolicyID:   opts.PolicyID,
		Synced:     int32(result.ObjectsSynced),
		Failed:     int32(result.ObjectsFailed),
		BytesTotal: result.BytesTransferred,
		Errors:     result.Errors,
	}, nil
}

// GetReplicationStatus retrieves replication status.
func (c *UnixClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	params := map[string]string{"id": policyID}

	var result struct {
		PolicyID       string `json:"policy_id"`
		Status         string `json:"status"`
		LastSyncTime   string `json:"last_sync_time"`
		ObjectsSynced  int    `json:"objects_synced"`
		ObjectsPending int    `json:"objects_pending"`
		ObjectsFailed  int    `json:"objects_failed"`
	}

	if err := c.call(ctx, "get_replication_status", params, &result); err != nil {
		return nil, err
	}

	status := &ReplicationStatus{
		PolicyID:           result.PolicyID,
		TotalObjectsSynced: int64(result.ObjectsSynced),
		TotalErrors:        int64(result.ObjectsFailed),
	}
	if result.LastSyncTime != "" {
		if t, err := time.Parse(time.RFC3339, result.LastSyncTime); err == nil {
			status.LastSyncTime = t
		}
	}

	return status, nil
}

// Close closes the underlying connection and marks the client closed;
// subsequent calls fail instead of re-dialling.  Idempotent.
func (c *UnixClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.reader = nil
		return err
	}

	return nil
}

// unixLifecyclePolicy matches the unix protocol PolicyParams shape returned
// by get_policies.
type unixLifecyclePolicy struct {
	ID               string `json:"id"`
	Prefix           string `json:"prefix"`
	Action           string `json:"action"`
	AfterDays        int64  `json:"after_days"`
	RetentionSeconds int64  `json:"retention_seconds"`
}

// toModel converts the unix wire representation into the SDK LifecyclePolicy
// type, preferring the exact retention_seconds over the rounded after_days.
func (p *unixLifecyclePolicy) toModel() *LifecyclePolicy {
	retention := p.RetentionSeconds
	if retention == 0 {
		retention = p.AfterDays * 86400
	}
	return &LifecyclePolicy{
		ID:               p.ID,
		Prefix:           p.Prefix,
		RetentionSeconds: retention,
		Action:           p.Action,
	}
}

// unixReplicationPolicy matches the unix protocol ReplicationPolicyParams
// shape returned by get_replication_policy / get_replication_policies.
type unixReplicationPolicy struct {
	ID              string            `json:"id"`
	SourcePrefix    string            `json:"source_prefix"`
	DestinationType string            `json:"destination_type"`
	Destination     map[string]string `json:"destination"`
	Enabled         bool              `json:"enabled"`
	ReplicationMode string            `json:"replication_mode"`
}

// toModel converts the unix wire representation into the SDK ReplicationPolicy type.
func (p *unixReplicationPolicy) toModel() *ReplicationPolicy {
	mode := ReplicationModeTransparent
	if p.ReplicationMode == "opaque" {
		mode = ReplicationModeOpaque
	}
	return &ReplicationPolicy{
		ID:                  p.ID,
		SourcePrefix:        p.SourcePrefix,
		DestinationBackend:  p.DestinationType,
		DestinationSettings: p.Destination,
		Enabled:             p.Enabled,
		ReplicationMode:     mode,
	}
}

// Ensure UnixClient implements Client interface.
var _ Client = (*UnixClient)(nil)
