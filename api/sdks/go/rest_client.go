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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RESTClient implements the Client interface using REST/HTTP protocol.
type RESTClient struct {
	baseURL    string
	httpClient *http.Client
	config     *ClientConfig
}

// newRESTClient creates a new REST client.
func newRESTClient(config *ClientConfig) (*RESTClient, error) {
	if config == nil {
		return nil, ErrInvalidConfig
	}

	scheme := "http"
	if config.UseTLS {
		scheme = "https"
	}

	baseURL := fmt.Sprintf("%s://%s", scheme, config.Address)

	// Build HTTP transport
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	// Configure TLS
	if config.UseTLS {
		tlsConfig, err := buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		transport.TLSClientConfig = tlsConfig
	}

	// Create HTTP client with timeout
	timeout := 30 * time.Second
	if config.RequestTimeout > 0 {
		timeout = config.RequestTimeout
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	return &RESTClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		config:     config,
	}, nil
}

// Put stores an object.
func (c *RESTClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// Set metadata headers if provided
	if metadata != nil {
		if metadata.ContentType != "" {
			req.Header.Set("Content-Type", metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			req.Header.Set("Content-Encoding", metadata.ContentEncoding)
		}
		// Custom metadata as JSON in header
		if len(metadata.Custom) > 0 {
			customJSON, _ := json.Marshal(metadata.Custom)
			req.Header.Set("X-Object-Metadata", string(customJSON))
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PUT failed with status %d", resp.StatusCode)
	}

	var result struct {
		Message string                 `json:"message"`
		Data    map[string]interface{} `json:"data,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Try to get ETag from response header first
	etag := resp.Header.Get("ETag")

	// If not in header, check if it's in the response body data
	if etag == "" && result.Data != nil {
		if etagVal, ok := result.Data["etag"].(string); ok {
			etag = etagVal
		}
	}

	return &PutResult{
		Success: true,
		Message: result.Message,
		ETag:    etag,
	}, nil
}

// Get retrieves an object.
func (c *RESTClient) Get(ctx context.Context, key string) (*GetResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET failed with status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse metadata from headers
	metadata := &Metadata{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		ETag:            resp.Header.Get("ETag"),
	}

	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
			metadata.Size = size
		}
	}

	if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
		if t, err := http.ParseTime(lastModified); err == nil {
			metadata.LastModified = t
		}
	}

	// Parse custom metadata from header
	if customMetadata := resp.Header.Get("X-Object-Metadata"); customMetadata != "" {
		var custom map[string]string
		if err := json.Unmarshal([]byte(customMetadata), &custom); err == nil {
			metadata.Custom = custom
		}
	}

	return &GetResult{
		Data:     data,
		Metadata: metadata,
	}, nil
}

// Delete removes an object.
func (c *RESTClient) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE failed with status %d", resp.StatusCode)
	}

	return nil
}

// List returns a list of objects.
func (c *RESTClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	if opts == nil {
		opts = &ListOptions{}
	}

	listURL := fmt.Sprintf("%s/objects", c.baseURL)
	params := url.Values{}

	if opts.Prefix != "" {
		params.Add("prefix", opts.Prefix)
	}
	if opts.Delimiter != "" {
		params.Add("delimiter", opts.Delimiter)
	}
	if opts.MaxResults > 0 {
		params.Add("limit", strconv.Itoa(int(opts.MaxResults)))
	}
	if opts.ContinueFrom != "" {
		params.Add("token", opts.ContinueFrom)
	}

	if len(params) > 0 {
		listURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("LIST failed with status %d", resp.StatusCode)
	}

	var result struct {
		Objects        []restObjectInfo `json:"objects"`
		CommonPrefixes []string         `json:"common_prefixes,omitempty"`
		NextToken      string           `json:"next_token,omitempty"`
		Truncated      bool             `json:"truncated"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	objects := make([]*ObjectInfo, len(result.Objects))
	for i, obj := range result.Objects {
		metadata := &Metadata{
			Size: obj.Size,
			ETag: obj.ETag,
		}

		if obj.Modified != "" {
			if t, err := time.Parse(time.RFC3339, obj.Modified); err == nil {
				metadata.LastModified = t
			}
		}

		objects[i] = &ObjectInfo{
			Key:      obj.Key,
			Metadata: metadata,
		}
	}

	return &ListResult{
		Objects:        objects,
		CommonPrefixes: result.CommonPrefixes,
		NextToken:      result.NextToken,
		Truncated:      result.Truncated,
	}, nil
}

// Exists checks if an object exists.
func (c *RESTClient) Exists(ctx context.Context, key string) (bool, error) {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("HEAD failed with status %d", resp.StatusCode)
	}

	return true, nil
}

// GetMetadata retrieves object metadata using HEAD request.
func (c *RESTClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET metadata failed with status %d", resp.StatusCode)
	}

	// Parse metadata from headers
	metadata := &Metadata{
		ETag:        resp.Header.Get("ETag"),
		ContentType: resp.Header.Get("Content-Type"),
	}

	// Parse content length
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
			metadata.Size = size
		}
	}

	// Parse Last-Modified
	if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
		if t, err := time.Parse(time.RFC1123, lastMod); err == nil {
			metadata.LastModified = t
		}
	}

	// Parse custom metadata from X-Object-Metadata header if present
	if customMetadata := resp.Header.Get("X-Object-Metadata"); customMetadata != "" {
		var custom map[string]string
		if err := json.Unmarshal([]byte(customMetadata), &custom); err == nil {
			metadata.Custom = custom
		}
	}

	return metadata, nil
}

// UpdateMetadata updates object metadata via PUT /metadata/{key}.
func (c *RESTClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	reqURL := fmt.Sprintf("%s/metadata/%s", c.baseURL, url.PathEscape(key))

	body, err := json.Marshal(metadataToJSON(metadata))
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("UPDATE metadata failed with status %d", resp.StatusCode)
	}

	return nil
}

// Health performs a health check.
func (c *RESTClient) Health(ctx context.Context) (*HealthStatus, error) {
	url := fmt.Sprintf("%s/health", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Status  string `json:"status"`
		Version string `json:"version,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &HealthStatus{
		Status:  strings.ToUpper(result.Status),
		Message: result.Version,
	}, nil
}

// Archive copies an object to archival storage via POST /archive.
func (c *RESTClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	reqBody := map[string]interface{}{
		"key":                  key,
		"destination_type":     destinationType,
		"destination_settings": settings,
	}

	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/archive", reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ARCHIVE failed with status %d", resp.StatusCode)
	}

	return nil
}

// AddPolicy adds a lifecycle policy via POST /policies.
func (c *RESTClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	if policy == nil {
		return ErrInvalidConfig
	}

	reqBody := map[string]interface{}{
		"id":                policy.ID,
		"prefix":            policy.Prefix,
		"retention_seconds": policy.RetentionSeconds,
		"action":            policy.Action,
	}
	if policy.DestinationType != "" {
		reqBody["destination_type"] = policy.DestinationType
	}
	if len(policy.DestinationSettings) > 0 {
		reqBody["destination_settings"] = policy.DestinationSettings
	}

	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/policies", reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ADD policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// RemovePolicy removes a lifecycle policy via DELETE /policies/{id}.
func (c *RESTClient) RemovePolicy(ctx context.Context, policyID string) error {
	reqURL := fmt.Sprintf("%s/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REMOVE policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetPolicies retrieves lifecycle policies via GET /policies.
func (c *RESTClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	reqURL := c.baseURL + "/policies"
	if prefix != "" {
		params := url.Values{}
		params.Add("prefix", prefix)
		reqURL += "?" + params.Encode()
	}

	resp, err := c.doJSON(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET policies failed with status %d", resp.StatusCode)
	}

	var result struct {
		Policies []struct {
			ID               string `json:"id"`
			Prefix           string `json:"prefix"`
			RetentionSeconds int64  `json:"retention_seconds"`
			Action           string `json:"action"`
			DestinationType  string `json:"destination_type"`
		} `json:"policies"`
		Count int `json:"count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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

// ApplyPolicies executes all lifecycle policies via POST /policies/apply.
func (c *RESTClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/policies/apply", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("APPLY policies failed with status %d", resp.StatusCode)
	}

	var result struct {
		Message          string `json:"message"`
		PoliciesCount    int32  `json:"policies_count"`
		ObjectsProcessed int32  `json:"objects_processed"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &ApplyPoliciesResult{
		Success:          true,
		PoliciesCount:    result.PoliciesCount,
		ObjectsProcessed: result.ObjectsProcessed,
		Message:          result.Message,
	}, nil
}

// AddReplicationPolicy adds a replication policy via POST /replication/policies.
func (c *RESTClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	if policy == nil {
		return ErrInvalidConfig
	}

	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/replication/policies", replicationPolicyToJSON(policy))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ADD replication policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// RemoveReplicationPolicy removes a replication policy via DELETE /replication/policies/{id}.
func (c *RESTClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	reqURL := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REMOVE replication policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetReplicationPolicies retrieves all replication policies via GET /replication/policies.
func (c *RESTClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	resp, err := c.doJSON(ctx, "GET", c.baseURL+"/replication/policies", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET replication policies failed with status %d", resp.StatusCode)
	}

	var result struct {
		Policies []restReplicationPolicy `json:"policies"`
		Count    int                     `json:"count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	policies := make([]*ReplicationPolicy, len(result.Policies))
	for i := range result.Policies {
		policies[i] = result.Policies[i].toModel()
	}

	return policies, nil
}

// GetReplicationPolicy retrieves a specific replication policy via GET /replication/policies/{id}.
func (c *RESTClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	reqURL := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET replication policy failed with status %d", resp.StatusCode)
	}

	var policy restReplicationPolicy
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		return nil, err
	}

	return policy.toModel(), nil
}

// TriggerReplication triggers replication synchronization via POST /replication/trigger.
func (c *RESTClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &TriggerReplicationOptions{}
	}

	reqBody := map[string]interface{}{
		"policy_id":    opts.PolicyID,
		"parallel":     opts.Parallel,
		"worker_count": opts.WorkerCount,
	}

	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/replication/trigger", reqBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TRIGGER replication failed with status %d", resp.StatusCode)
	}

	var result struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Result  *struct {
			PolicyID   string   `json:"policy_id"`
			Synced     int32    `json:"synced"`
			Deleted    int32    `json:"deleted"`
			Failed     int32    `json:"failed"`
			BytesTotal int64    `json:"bytes_total"`
			Duration   string   `json:"duration"`
			Errors     []string `json:"errors,omitempty"`
		} `json:"result,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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

// GetReplicationStatus retrieves replication status and metrics via GET /replication/status/{id}.
func (c *RESTClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	reqURL := fmt.Sprintf("%s/replication/status/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET replication status failed with status %d", resp.StatusCode)
	}

	var result struct {
		PolicyID            string `json:"policy_id"`
		SourceBackend       string `json:"source_backend"`
		DestinationBackend  string `json:"destination_backend"`
		Enabled             bool   `json:"enabled"`
		TotalObjectsSynced  int64  `json:"total_objects_synced"`
		TotalObjectsDeleted int64  `json:"total_objects_deleted"`
		TotalBytesSynced    int64  `json:"total_bytes_synced"`
		TotalErrors         int64  `json:"total_errors"`
		LastSyncTime        string `json:"last_sync_time,omitempty"`
		AverageSyncDuration string `json:"average_sync_duration"`
		SyncCount           int64  `json:"sync_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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

// Close closes any resources held by the client.
func (c *RESTClient) Close() error {
	// HTTP client doesn't need explicit closing, but we close idle connections
	c.httpClient.CloseIdleConnections()
	return nil
}

// doJSON performs an HTTP request with an optional JSON body and returns the response.
// The caller is responsible for closing the response body.
func (c *RESTClient) doJSON(ctx context.Context, method, reqURL string, body interface{}) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.httpClient.Do(req)
}

// metadataToJSON converts a Metadata value into the server's common.Metadata JSON shape.
func metadataToJSON(m *Metadata) map[string]interface{} {
	out := map[string]interface{}{}
	if m == nil {
		return out
	}
	if m.ContentType != "" {
		out["content_type"] = m.ContentType
	}
	if m.ContentEncoding != "" {
		out["content_encoding"] = m.ContentEncoding
	}
	if m.Size != 0 {
		out["size"] = m.Size
	}
	if len(m.Custom) > 0 {
		out["custom"] = m.Custom
	}
	return out
}

// replicationPolicyToJSON converts a ReplicationPolicy into the REST request shape.
func replicationPolicyToJSON(p *ReplicationPolicy) map[string]interface{} {
	out := map[string]interface{}{
		"id":                     p.ID,
		"source_backend":         p.SourceBackend,
		"destination_backend":    p.DestinationBackend,
		"check_interval_seconds": p.CheckIntervalSeconds,
		"enabled":                p.Enabled,
	}
	if len(p.SourceSettings) > 0 {
		out["source_settings"] = p.SourceSettings
	}
	if p.SourcePrefix != "" {
		out["source_prefix"] = p.SourcePrefix
	}
	if len(p.DestinationSettings) > 0 {
		out["destination_settings"] = p.DestinationSettings
	}
	switch p.ReplicationMode {
	case ReplicationModeOpaque:
		out["replication_mode"] = "opaque"
	case ReplicationModeTransparent:
		out["replication_mode"] = "transparent"
	}
	return out
}

// restObjectInfo matches the REST API's ObjectResponse schema.
type restObjectInfo struct {
	Key      string            `json:"key"`
	Size     int64             `json:"size"`
	Modified string            `json:"modified,omitempty"`
	ETag     string            `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// restReplicationPolicy matches the REST API's ReplicationPolicyResponse schema.
type restReplicationPolicy struct {
	ID                   string            `json:"id"`
	SourceBackend        string            `json:"source_backend"`
	SourceSettings       map[string]string `json:"source_settings,omitempty"`
	SourcePrefix         string            `json:"source_prefix,omitempty"`
	DestinationBackend   string            `json:"destination_backend"`
	DestinationSettings  map[string]string `json:"destination_settings,omitempty"`
	CheckIntervalSeconds int64             `json:"check_interval_seconds"`
	LastSyncTime         string            `json:"last_sync_time,omitempty"`
	Enabled              bool              `json:"enabled"`
	ReplicationMode      string            `json:"replication_mode"`
}

// toModel converts the wire representation into the SDK ReplicationPolicy type.
func (p *restReplicationPolicy) toModel() *ReplicationPolicy {
	policy := &ReplicationPolicy{
		ID:                   p.ID,
		SourceBackend:        p.SourceBackend,
		SourceSettings:       p.SourceSettings,
		SourcePrefix:         p.SourcePrefix,
		DestinationBackend:   p.DestinationBackend,
		DestinationSettings:  p.DestinationSettings,
		CheckIntervalSeconds: p.CheckIntervalSeconds,
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

// Ensure RESTClient implements Client interface
var _ Client = (*RESTClient)(nil)
