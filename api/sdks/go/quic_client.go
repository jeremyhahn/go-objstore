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

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// QUICClient implements the Client interface using QUIC/HTTP3 protocol.
type QUICClient struct {
	baseURL    string
	httpClient *http.Client
	config     *ClientConfig
	transport  *http3.Transport
}

// newQUICClient creates a new QUIC client.
func newQUICClient(config *ClientConfig) (*QUICClient, error) {
	if config == nil {
		return nil, ErrInvalidConfig
	}

	scheme := "https" // QUIC always uses TLS
	baseURL := fmt.Sprintf("%s://%s", scheme, config.Address)

	// Build TLS configuration
	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	// QUIC configuration
	quicConfig := &quic.Config{
		MaxIdleTimeout:  config.ConnectionTimeout,
		KeepAlivePeriod: config.ConnectionTimeout / 2,
	}

	if config.MaxStreams > 0 {
		quicConfig.MaxIncomingStreams = int64(config.MaxStreams)
		quicConfig.MaxIncomingUniStreams = int64(config.MaxStreams)
	}

	// Create HTTP3 transport
	transport := &http3.Transport{
		TLSClientConfig: tlsConfig,
		QUICConfig:      quicConfig,
	}

	// Create HTTP client
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   config.RequestTimeout,
	}

	return &QUICClient{
		baseURL:    baseURL,
		httpClient: httpClient,
		config:     config,
		transport:  transport,
	}, nil
}

// The QUIC client speaks HTTP/3 to the server's bare RESTful routes (no /api/v1 prefix).
// Object data is transferred as raw request/response bodies; metadata travels in headers.

// Put stores an object via PUT /objects/{key} with the raw body and X-Meta-* headers.
func (c *QUICClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	reqURL := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "PUT", reqURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	if metadata != nil {
		if metadata.ContentType != "" {
			req.Header.Set("Content-Type", metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			req.Header.Set("Content-Encoding", metadata.ContentEncoding)
		}
		for k, v := range metadata.Custom {
			req.Header.Set("X-Meta-"+k, v)
		}
	}
	c.applyAuthHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("PUT", resp.StatusCode)
	}

	var result struct {
		Key     string `json:"key"`
		Message string `json:"message"`
	}
	// The body has no etag; ignore decode errors so a missing/extra body never masks success.
	_ = json.NewDecoder(resp.Body).Decode(&result)

	return &PutResult{
		Success: true,
		Message: result.Message,
		ETag:    resp.Header.Get("ETag"),
	}, nil
}

// Get retrieves an object via GET /objects/{key}.
func (c *QUICClient) Get(ctx context.Context, key string) (*GetResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	reqURL := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	c.applyAuthHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("GET", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Data:     data,
		Metadata: metadataFromHeaders(resp.Header),
	}, nil
}

// Delete removes an object via DELETE /objects/{key}.
func (c *QUICClient) Delete(ctx context.Context, key string) error {
	reqURL := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return httpStatusError("DELETE", resp.StatusCode)
	}

	return nil
}

// List returns a list of objects via GET /objects with continue/max query params.
func (c *QUICClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	if opts == nil {
		opts = &ListOptions{}
	}

	reqURL := c.baseURL + "/objects"
	params := url.Values{}
	if opts.Prefix != "" {
		params.Add("prefix", opts.Prefix)
	}
	if opts.Delimiter != "" {
		params.Add("delimiter", opts.Delimiter)
	}
	if opts.MaxResults > 0 {
		params.Add("max", strconv.Itoa(int(opts.MaxResults)))
	}
	if opts.ContinueFrom != "" {
		params.Add("continue", opts.ContinueFrom)
	}
	if len(params) > 0 {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("LIST", resp.StatusCode)
	}

	var result struct {
		Objects []struct {
			Key      string `json:"key"`
			Metadata *struct {
				ContentType     string            `json:"content_type"`
				ContentEncoding string            `json:"content_encoding"`
				Size            int64             `json:"size"`
				LastModified    time.Time         `json:"last_modified"`
				ETag            string            `json:"etag"`
				Custom          map[string]string `json:"custom"`
			} `json:"metadata,omitempty"`
		} `json:"objects"`
		Prefixes  []string `json:"prefixes,omitempty"`
		NextToken string   `json:"next_token,omitempty"`
		Truncated bool     `json:"truncated"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	objects := make([]*ObjectInfo, len(result.Objects))
	for i, obj := range result.Objects {
		info := &ObjectInfo{Key: obj.Key}
		if obj.Metadata != nil {
			info.Metadata = &Metadata{
				ContentType:     obj.Metadata.ContentType,
				ContentEncoding: obj.Metadata.ContentEncoding,
				Size:            obj.Metadata.Size,
				LastModified:    obj.Metadata.LastModified,
				ETag:            obj.Metadata.ETag,
				Custom:          obj.Metadata.Custom,
			}
		}
		objects[i] = info
	}

	return &ListResult{
		Objects:        objects,
		CommonPrefixes: result.Prefixes,
		NextToken:      result.NextToken,
		Truncated:      result.Truncated,
	}, nil
}

// Exists checks if an object exists via GET /objects/{key}?exists=1.
func (c *QUICClient) Exists(ctx context.Context, key string) (bool, error) {
	reqURL := fmt.Sprintf("%s/objects/%s?exists=1", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, httpStatusError("EXISTS", resp.StatusCode)
	}

	var result struct {
		Exists bool `json:"exists"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.Exists, nil
}

// GetMetadata retrieves object metadata via HEAD /objects/{key}, reading headers.
func (c *QUICClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	reqURL := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "HEAD", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("GET metadata", resp.StatusCode)
	}

	return metadataFromHeaders(resp.Header), nil
}

// UpdateMetadata updates object metadata via PATCH /objects/{key}.
func (c *QUICClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	reqURL := fmt.Sprintf("%s/objects/%s", c.baseURL, url.PathEscape(key))

	body, err := json.Marshal(metadataToJSON(metadata))
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PATCH", reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpStatusError("UPDATE metadata", resp.StatusCode)
	}

	return nil
}

// Health performs a health check via GET /health.
func (c *QUICClient) Health(ctx context.Context) (*HealthStatus, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/health", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("HEALTH", resp.StatusCode)
	}

	var result struct {
		Status   string `json:"status"`
		Protocol string `json:"protocol"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &HealthStatus{
		Status:  strings.ToUpper(result.Status),
		Message: result.Protocol,
	}, nil
}

// Archive copies an object to archival storage via POST /archive.
func (c *QUICClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
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
		return httpStatusError("ARCHIVE", resp.StatusCode)
	}

	return nil
}

// AddPolicy adds a lifecycle policy via POST /policies.
func (c *QUICClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
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
		return httpStatusError("ADD policy", resp.StatusCode)
	}

	return nil
}

// RemovePolicy removes a lifecycle policy via DELETE /policies/{id}.
func (c *QUICClient) RemovePolicy(ctx context.Context, policyID string) error {
	reqURL := fmt.Sprintf("%s/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpStatusError("REMOVE policy", resp.StatusCode)
	}

	return nil
}

// GetPolicies retrieves lifecycle policies via GET /policies.
func (c *QUICClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
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
		return nil, httpStatusError("GET policies", resp.StatusCode)
	}

	var result struct {
		Policies []struct {
			ID               string `json:"id"`
			Prefix           string `json:"prefix"`
			RetentionSeconds int64  `json:"retention_seconds"`
			Action           string `json:"action"`
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
		}
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies via POST /policies/apply.
func (c *QUICClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/policies/apply", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("APPLY policies", resp.StatusCode)
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
func (c *QUICClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	if policy == nil {
		return ErrInvalidConfig
	}

	reqBody := map[string]interface{}{
		"id":                  policy.ID,
		"source_backend":      policy.SourceBackend,
		"destination_backend": policy.DestinationBackend,
		"check_interval":      policy.CheckIntervalSeconds,
		"enabled":             policy.Enabled,
	}
	if len(policy.SourceSettings) > 0 {
		reqBody["source_settings"] = policy.SourceSettings
	}
	if policy.SourcePrefix != "" {
		reqBody["source_prefix"] = policy.SourcePrefix
	}
	if len(policy.DestinationSettings) > 0 {
		reqBody["destination_settings"] = policy.DestinationSettings
	}
	switch policy.ReplicationMode {
	case ReplicationModeOpaque:
		reqBody["replication_mode"] = "opaque"
	case ReplicationModeTransparent:
		reqBody["replication_mode"] = "transparent"
	}

	resp, err := c.doJSON(ctx, "POST", c.baseURL+"/replication/policies", reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return httpStatusError("ADD replication policy", resp.StatusCode)
	}

	return nil
}

// RemoveReplicationPolicy removes a replication policy via DELETE /replication/policies/{id}.
func (c *QUICClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	reqURL := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpStatusError("REMOVE replication policy", resp.StatusCode)
	}

	return nil
}

// GetReplicationPolicies retrieves all replication policies via GET /replication/policies.
func (c *QUICClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	resp, err := c.doJSON(ctx, "GET", c.baseURL+"/replication/policies", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("GET replication policies", resp.StatusCode)
	}

	var result struct {
		Success  bool                    `json:"success"`
		Policies []quicReplicationPolicy `json:"policies"`
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
func (c *QUICClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	reqURL := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("GET replication policy", resp.StatusCode)
	}

	var policy quicReplicationPolicy
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		return nil, err
	}

	return policy.toModel(), nil
}

// TriggerReplication triggers replication via POST /replication/trigger?policy_id=.
func (c *QUICClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &TriggerReplicationOptions{}
	}

	reqURL := c.baseURL + "/replication/trigger"
	if opts.PolicyID != "" {
		params := url.Values{}
		params.Add("policy_id", opts.PolicyID)
		reqURL += "?" + params.Encode()
	}

	resp, err := c.doJSON(ctx, "POST", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("TRIGGER replication", resp.StatusCode)
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

// GetReplicationStatus retrieves replication status via GET /replication/status/{id}.
func (c *QUICClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	reqURL := fmt.Sprintf("%s/replication/status/%s", c.baseURL, url.PathEscape(policyID))

	resp, err := c.doJSON(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("GET replication status", resp.StatusCode)
	}

	var result struct {
		Success             bool   `json:"success"`
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

// Close closes the QUIC connection.
func (c *QUICClient) Close() error {
	if c.transport != nil {
		c.transport.Close()
	}
	return nil
}

// applyAuthHeaders adds Authorization, custom headers, and X-Tenant-ID to req
// when they are configured.
func (c *QUICClient) applyAuthHeaders(req *http.Request) {
	applyAuthHeaders(req, c.config)
}

// doJSON performs an HTTP/3 request with an optional JSON body and returns the response.
// The caller is responsible for closing the response body.
func (c *QUICClient) doJSON(ctx context.Context, method, reqURL string, body interface{}) (*http.Response, error) {
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
	c.applyAuthHeaders(req)

	return c.httpClient.Do(req)
}

// GetStream retrieves an object as a streaming io.ReadCloser.
// The caller must close the returned reader when done.
func (c *QUICClient) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return httpGetStream(ctx, c.httpClient, c.baseURL, key, c.config, metadataFromHeaders)
}

// PutStream stores an object from an io.Reader.
// size is the Content-Length hint; pass -1 when unknown.
func (c *QUICClient) PutStream(ctx context.Context, key string, r io.Reader, size int64, metadata *Metadata) (*PutResult, error) {
	// QUIC carries custom metadata as per-key X-Meta-<key> headers.
	return httpPutStream(ctx, c.httpClient, c.baseURL, key, r, size, metadata, c.config, func(req *http.Request, custom map[string]string) {
		for k, v := range custom {
			req.Header.Set("X-Meta-"+k, v)
		}
	})
}

// metadataFromHeaders reconstructs object metadata from QUIC response headers.
func metadataFromHeaders(header http.Header) *Metadata {
	metadata := &Metadata{
		ContentType:     header.Get("Content-Type"),
		ContentEncoding: header.Get("Content-Encoding"),
		ETag:            header.Get("ETag"),
	}

	if contentLength := header.Get("Content-Length"); contentLength != "" {
		if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
			metadata.Size = size
		}
	}

	if lastModified := header.Get("Last-Modified"); lastModified != "" {
		if t, err := http.ParseTime(lastModified); err == nil {
			metadata.LastModified = t
		}
	}

	custom := make(map[string]string)
	for name, values := range header {
		if strings.HasPrefix(name, "X-Meta-") && len(values) > 0 {
			// Go canonicalises header names (e.g. "X-Meta-Version"), so we
			// lowercase the extracted key to match what was stored.
			custom[strings.ToLower(strings.TrimPrefix(name, "X-Meta-"))] = values[0]
		}
	}
	if len(custom) > 0 {
		metadata.Custom = custom
	}

	return metadata
}

// quicReplicationPolicy matches the QUIC server's replication policy JSON shape
// (note: check_interval, not check_interval_seconds).
type quicReplicationPolicy struct {
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

// toModel converts the QUIC wire representation into the SDK ReplicationPolicy type.
func (p *quicReplicationPolicy) toModel() *ReplicationPolicy {
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

// Ensure QUICClient implements Client interface
var _ Client = (*QUICClient)(nil)
