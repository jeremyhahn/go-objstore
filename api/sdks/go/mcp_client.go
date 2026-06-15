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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// MCPClient implements the Client interface using the MCP JSON-RPC 2.0 over HTTP
// protocol.  Every operation maps to a "tools/call" request with a tool name of
// the form "objstore_<op>" and arguments per the server's tool registry.
type MCPClient struct {
	baseURL    string
	httpClient *http.Client
	config     *ClientConfig
	nextID     atomic.Int64
}

// mcpToolsCallParams is the params shape for the "tools/call" method.
type mcpToolsCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// mcpToolsCallResult is the result shape returned by the MCP server for a
// "tools/call" invocation.  The textual result is nested in content[0].text.
type mcpToolsCallResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

// newMCPClient creates a new MCP HTTP client.
func newMCPClient(config *ClientConfig) (*MCPClient, error) {
	if config == nil {
		return nil, ErrInvalidConfig
	}

	scheme := "http"
	if config.UseTLS {
		scheme = "https"
	}

	baseURL := fmt.Sprintf("%s://%s", scheme, config.Address)

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	if config.UseTLS {
		tlsConfig, err := buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		transport.TLSClientConfig = tlsConfig
	}

	timeout := 30 * time.Second
	if config.RequestTimeout > 0 {
		timeout = config.RequestTimeout
	}

	return &MCPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
		config: config,
	}, nil
}

// callTool invokes an MCP tool and unmarshals the JSON result text into dest.
// dest may be nil when the caller does not need the result payload.
func (c *MCPClient) callTool(ctx context.Context, toolName string, args map[string]any, dest any) error {
	id := c.nextID.Add(1)

	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: mcpToolsCallParams{
			Name:      toolName,
			Arguments: args,
		},
		ID: id,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	applyAuthHeaders(httpReq, c.config)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	// HTTP-level failures (auth middleware, rate limiting, routing) never
	// carry a JSON-RPC envelope; map them through the shared HTTP table.
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		return httpStatusError("mcp "+toolName, httpResp.StatusCode)
	}

	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var resp rpcResponse
	if err = json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	if resp.Error != nil {
		return resp.Error.asSDKError("mcp")
	}

	if dest == nil {
		return nil
	}

	// Unwrap the result: result.content[0].text contains a JSON string.
	var toolResult mcpToolsCallResult
	if err = json.Unmarshal(resp.Result, &toolResult); err != nil {
		return fmt.Errorf("unmarshal tool result: %w", err)
	}

	if len(toolResult.Content) == 0 || toolResult.Content[0].Text == "" {
		return nil
	}

	if err = json.Unmarshal([]byte(toolResult.Content[0].Text), dest); err != nil {
		return fmt.Errorf("unmarshal tool text payload: %w", err)
	}

	return nil
}

// Put stores an object.
func (c *MCPClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateData(data); err != nil {
		return nil, err
	}

	args := map[string]any{
		"key":  key,
		"data": base64.StdEncoding.EncodeToString(data),
	}
	if metadata != nil {
		args["metadata"] = rpcMetadataParams(metadata)
	}

	// The put tool result carries nothing the SDK surfaces, so it is discarded.
	if err := c.callTool(ctx, "objstore_put", args, nil); err != nil {
		return nil, err
	}

	return &PutResult{Success: true}, nil
}

// Get retrieves an object.
func (c *MCPClient) Get(ctx context.Context, key string) (*GetResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	args := map[string]any{"key": key}

	var result struct {
		Data string `json:"data"`
		Size int64  `json:"size"`
	}

	if err := c.callTool(ctx, "objstore_get", args, &result); err != nil {
		return nil, err
	}

	// Object data is base64-encoded on the MCP transport; anything else is a
	// protocol violation and must surface as an error, not be passed through.
	raw, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 data in MCP response: %w", err)
	}

	return &GetResult{
		Data: raw,
		Metadata: &Metadata{
			Size: result.Size,
		},
	}, nil
}

// Delete removes an object.
func (c *MCPClient) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	args := map[string]any{"key": key}
	return c.callTool(ctx, "objstore_delete", args, nil)
}

// List returns a list of objects.
func (c *MCPClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	if opts == nil {
		opts = &ListOptions{}
	}

	args := map[string]any{
		"prefix":        opts.Prefix,
		"max_results":   int(opts.MaxResults),
		"continue_from": opts.ContinueFrom,
	}

	var result struct {
		Keys      []string `json:"keys"`
		Truncated bool     `json:"truncated"`
		NextToken string   `json:"next_token"`
	}

	if err := c.callTool(ctx, "objstore_list", args, &result); err != nil {
		return nil, err
	}

	objects := make([]*ObjectInfo, len(result.Keys))
	for i, k := range result.Keys {
		objects[i] = &ObjectInfo{Key: k}
	}

	return &ListResult{
		Objects:   objects,
		NextToken: result.NextToken,
		Truncated: result.Truncated,
	}, nil
}

// Exists checks if an object exists.
func (c *MCPClient) Exists(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	args := map[string]any{"key": key}

	var result struct {
		Exists bool `json:"exists"`
	}

	if err := c.callTool(ctx, "objstore_exists", args, &result); err != nil {
		return false, err
	}

	return result.Exists, nil
}

// GetMetadata retrieves object metadata.
func (c *MCPClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	args := map[string]any{"key": key}

	var result struct {
		Size            int64             `json:"size"`
		ContentType     string            `json:"content_type"`
		ContentEncoding string            `json:"content_encoding"`
		LastModified    string            `json:"last_modified"`
		ETag            string            `json:"etag"`
		Custom          map[string]string `json:"custom"`
	}

	if err := c.callTool(ctx, "objstore_get_metadata", args, &result); err != nil {
		return nil, err
	}

	meta := &Metadata{
		Size:            result.Size,
		ContentType:     result.ContentType,
		ContentEncoding: result.ContentEncoding,
		ETag:            result.ETag,
		Custom:          result.Custom,
	}
	if result.LastModified != "" {
		if t, err := time.Parse(time.RFC3339, result.LastModified); err == nil {
			meta.LastModified = t
		}
	}

	return meta, nil
}

// UpdateMetadata updates object metadata.
func (c *MCPClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateMetadata(metadata); err != nil {
		return err
	}

	args := map[string]any{
		"key":      key,
		"metadata": rpcMetadataParams(metadata),
	}

	return c.callTool(ctx, "objstore_update_metadata", args, nil)
}

// Health performs a health check.
func (c *MCPClient) Health(ctx context.Context) (*HealthStatus, error) {
	var result struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}

	if err := c.callTool(ctx, "objstore_health", map[string]any{}, &result); err != nil {
		return nil, err
	}

	return &HealthStatus{
		Status:  result.Status,
		Message: result.Version,
	}, nil
}

// Archive copies an object to archival storage.
func (c *MCPClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	args := map[string]any{
		"key":                  key,
		"destination_type":     destinationType,
		"destination_settings": settings,
	}

	return c.callTool(ctx, "objstore_archive", args, nil)
}

// AddPolicy adds a lifecycle policy.
func (c *MCPClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	if err := validateLifecyclePolicy(policy); err != nil {
		return err
	}

	args := map[string]any{
		"id":                policy.ID,
		"prefix":            policy.Prefix,
		"retention_seconds": policy.RetentionSeconds,
		"action":            policy.Action,
	}
	if policy.DestinationType != "" {
		args["destination_type"] = policy.DestinationType
	}
	if len(policy.DestinationSettings) > 0 {
		args["destination_settings"] = policy.DestinationSettings
	}

	return c.callTool(ctx, "objstore_add_policy", args, nil)
}

// RemovePolicy removes a lifecycle policy.
func (c *MCPClient) RemovePolicy(ctx context.Context, policyID string) error {
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	args := map[string]any{"id": policyID}
	return c.callTool(ctx, "objstore_remove_policy", args, nil)
}

// GetPolicies retrieves lifecycle policies.
func (c *MCPClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	args := map[string]any{"prefix": prefix}

	var result struct {
		Policies []struct {
			ID               string `json:"id"`
			Prefix           string `json:"prefix"`
			RetentionSeconds int64  `json:"retention_seconds"`
			Action           string `json:"action"`
			DestinationType  string `json:"destination_type"`
		} `json:"policies"`
	}

	if err := c.callTool(ctx, "objstore_get_policies", args, &result); err != nil {
		return nil, err
	}

	policies := make([]*LifecyclePolicy, len(result.Policies))
	for i, p := range result.Policies {
		policies[i] = &LifecyclePolicy{
			ID:               p.ID,
			Prefix:           p.Prefix,
			RetentionSeconds: p.RetentionSeconds,
			Action:           p.Action,
			DestinationType:  p.DestinationType,
		}
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies.
func (c *MCPClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	var result struct {
		PoliciesCount    int32  `json:"policies_count"`
		ObjectsProcessed int32  `json:"objects_processed"`
		Message          string `json:"message"`
	}

	if err := c.callTool(ctx, "objstore_apply_policies", map[string]any{}, &result); err != nil {
		return nil, err
	}

	return &ApplyPoliciesResult{
		Success:          true,
		PoliciesCount:    result.PoliciesCount,
		ObjectsProcessed: result.ObjectsProcessed,
		Message:          result.Message,
	}, nil
}

// AddReplicationPolicy adds a replication policy.
func (c *MCPClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	if err := validateReplicationPolicy(policy); err != nil {
		return err
	}

	mode := "transparent"
	if policy.ReplicationMode == ReplicationModeOpaque {
		mode = "opaque"
	}

	args := map[string]any{
		"id":                  policy.ID,
		"source_backend":      policy.SourceBackend,
		"destination_backend": policy.DestinationBackend,
		"check_interval":      policy.CheckIntervalSeconds,
		"enabled":             policy.Enabled,
		"replication_mode":    mode,
	}
	if len(policy.SourceSettings) > 0 {
		args["source_settings"] = policy.SourceSettings
	}
	if policy.SourcePrefix != "" {
		args["source_prefix"] = policy.SourcePrefix
	}
	if len(policy.DestinationSettings) > 0 {
		args["destination_settings"] = policy.DestinationSettings
	}

	return c.callTool(ctx, "objstore_add_replication_policy", args, nil)
}

// RemoveReplicationPolicy removes a replication policy.
func (c *MCPClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	if err := validatePolicyID(policyID); err != nil {
		return err
	}

	args := map[string]any{"id": policyID}
	return c.callTool(ctx, "objstore_remove_replication_policy", args, nil)
}

// GetReplicationPolicies retrieves all replication policies.
func (c *MCPClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	var result struct {
		Policies []mcpReplicationPolicy `json:"policies"`
	}

	if err := c.callTool(ctx, "objstore_list_replication_policies", map[string]any{}, &result); err != nil {
		return nil, err
	}

	policies := make([]*ReplicationPolicy, len(result.Policies))
	for i := range result.Policies {
		policies[i] = result.Policies[i].toModel()
	}

	return policies, nil
}

// GetReplicationPolicy retrieves a specific replication policy.
func (c *MCPClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	args := map[string]any{"id": policyID}

	var result mcpReplicationPolicy
	if err := c.callTool(ctx, "objstore_get_replication_policy", args, &result); err != nil {
		return nil, err
	}

	return result.toModel(), nil
}

// TriggerReplication triggers replication synchronization.
func (c *MCPClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &TriggerReplicationOptions{}
	}

	args := map[string]any{
		"policy_id": opts.PolicyID,
	}

	var result struct {
		Result *struct {
			PolicyID   string   `json:"policy_id"`
			Synced     int32    `json:"synced"`
			Deleted    int32    `json:"deleted"`
			Failed     int32    `json:"failed"`
			BytesTotal int64    `json:"bytes_total"`
			Duration   string   `json:"duration"`
			Errors     []string `json:"errors"`
		} `json:"result"`
	}

	if err := c.callTool(ctx, "objstore_trigger_replication", args, &result); err != nil {
		return nil, err
	}

	syncResult := &SyncResult{}
	if result.Result != nil {
		syncResult.PolicyID = result.Result.PolicyID
		syncResult.Synced = result.Result.Synced
		syncResult.Deleted = result.Result.Deleted
		syncResult.Failed = result.Result.Failed
		syncResult.BytesTotal = result.Result.BytesTotal
		syncResult.Errors = result.Result.Errors
		if d, err := time.ParseDuration(result.Result.Duration); err == nil {
			syncResult.DurationMs = d.Milliseconds()
		}
	}

	return syncResult, nil
}

// GetReplicationStatus retrieves replication status.
func (c *MCPClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	if err := validatePolicyID(policyID); err != nil {
		return nil, err
	}

	args := map[string]any{"policy_id": policyID}

	var result struct {
		PolicyID            string `json:"policy_id"`
		SourceBackend       string `json:"source_backend"`
		DestinationBackend  string `json:"destination_backend"`
		Enabled             bool   `json:"enabled"`
		TotalObjectsSynced  int64  `json:"total_objects_synced"`
		TotalObjectsDeleted int64  `json:"total_objects_deleted"`
		TotalBytesSynced    int64  `json:"total_bytes_synced"`
		TotalErrors         int64  `json:"total_errors"`
		LastSyncTime        string `json:"last_sync_time"`
		AverageSyncDuration string `json:"average_sync_duration"`
		SyncCount           int64  `json:"sync_count"`
	}

	if err := c.callTool(ctx, "objstore_get_replication_status", args, &result); err != nil {
		return nil, err
	}

	status := &ReplicationStatus{
		PolicyID:            result.PolicyID,
		SourceBackend:       result.SourceBackend,
		DestinationBackend:  result.DestinationBackend,
		Enabled:             result.Enabled,
		TotalObjectsSynced:  result.TotalObjectsSynced,
		TotalObjectsDeleted: result.TotalObjectsDeleted,
		TotalBytesSynced:    result.TotalBytesSynced,
		TotalErrors:         result.TotalErrors,
		SyncCount:           result.SyncCount,
	}
	if d, err := time.ParseDuration(result.AverageSyncDuration); err == nil {
		status.AverageSyncDurationMs = d.Milliseconds()
	}
	if result.LastSyncTime != "" {
		if t, err := time.Parse(time.RFC3339, result.LastSyncTime); err == nil {
			status.LastSyncTime = t
		}
	}

	return status, nil
}

// Close closes any idle connections held by the HTTP client.
func (c *MCPClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// mcpReplicationPolicy matches the JSON shape returned by the MCP server for
// replication policy tool results.
type mcpReplicationPolicy struct {
	ID                  string            `json:"id"`
	SourceBackend       string            `json:"source_backend"`
	SourceSettings      map[string]string `json:"source_settings,omitempty"`
	SourcePrefix        string            `json:"source_prefix,omitempty"`
	DestinationBackend  string            `json:"destination_backend"`
	DestinationSettings map[string]string `json:"destination_settings,omitempty"`
	CheckInterval       int64             `json:"check_interval"`
	LastSyncTime        string            `json:"last_sync_time,omitempty"`
	Enabled             bool              `json:"enabled"`
	ReplicationMode     string            `json:"replication_mode"`
}

// toModel converts the MCP wire representation into the SDK ReplicationPolicy type.
func (p *mcpReplicationPolicy) toModel() *ReplicationPolicy {
	policy := &ReplicationPolicy{
		ID:                   p.ID,
		SourceBackend:        p.SourceBackend,
		SourceSettings:       p.SourceSettings,
		SourcePrefix:         p.SourcePrefix,
		DestinationBackend:   p.DestinationBackend,
		DestinationSettings:  p.DestinationSettings,
		CheckIntervalSeconds: p.CheckInterval,
		Enabled:              p.Enabled,
	}
	if p.ReplicationMode == "opaque" {
		policy.ReplicationMode = ReplicationModeOpaque
	}
	if p.LastSyncTime != "" {
		if t, err := time.Parse(time.RFC3339, p.LastSyncTime); err == nil {
			policy.LastSyncTime = t
		}
	}
	return policy
}

// Ensure MCPClient implements Client interface.
var _ Client = (*MCPClient)(nil)
