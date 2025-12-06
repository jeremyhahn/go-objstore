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

	return resp.StatusCode == http.StatusOK, nil
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

// UpdateMetadata updates object metadata.
// Note: REST API requires re-uploading the object to update metadata.
// This is a limitation of the REST protocol compared to gRPC.
func (c *RESTClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	// REST doesn't have a dedicated metadata update endpoint that works like gRPC.
	// The server's /objects/{key}/metadata endpoint doesn't work as expected.
	// To properly update metadata, you need to GET the object, then PUT it back with new metadata.
	// For now, we'll return ErrNotSupported to indicate this limitation.
	return ErrNotSupported
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

// Archive copies an object to archival storage.
func (c *RESTClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	return ErrStreamingNotSupported // REST API doesn't support archive in the OpenAPI spec
}

// AddPolicy adds a lifecycle policy.
func (c *RESTClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	return ErrStreamingNotSupported // REST API doesn't support lifecycle policies in the OpenAPI spec
}

// RemovePolicy removes a lifecycle policy.
func (c *RESTClient) RemovePolicy(ctx context.Context, policyID string) error {
	return ErrStreamingNotSupported // REST API doesn't support lifecycle policies in the OpenAPI spec
}

// GetPolicies retrieves lifecycle policies.
func (c *RESTClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support lifecycle policies in the OpenAPI spec
}

// ApplyPolicies executes all lifecycle policies.
func (c *RESTClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support lifecycle policies in the OpenAPI spec
}

// AddReplicationPolicy adds a replication policy.
func (c *RESTClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	return ErrStreamingNotSupported // REST API doesn't support replication policies in the OpenAPI spec
}

// RemoveReplicationPolicy removes a replication policy.
func (c *RESTClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	return ErrStreamingNotSupported // REST API doesn't support replication policies in the OpenAPI spec
}

// GetReplicationPolicies retrieves all replication policies.
func (c *RESTClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support replication policies in the OpenAPI spec
}

// GetReplicationPolicy retrieves a specific replication policy.
func (c *RESTClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support replication policies in the OpenAPI spec
}

// TriggerReplication triggers replication synchronization.
func (c *RESTClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support replication in the OpenAPI spec
}

// GetReplicationStatus retrieves replication status and metrics.
func (c *RESTClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	return nil, ErrStreamingNotSupported // REST API doesn't support replication in the OpenAPI spec
}

// Close closes any resources held by the client.
func (c *RESTClient) Close() error {
	// HTTP client doesn't need explicit closing, but we close idle connections
	c.httpClient.CloseIdleConnections()
	return nil
}

// restObjectInfo matches the REST API's ObjectResponse schema.
type restObjectInfo struct {
	Key      string            `json:"key"`
	Size     int64             `json:"size"`
	Modified string            `json:"modified,omitempty"`
	ETag     string            `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Ensure RESTClient implements Client interface
var _ Client = (*RESTClient)(nil)
