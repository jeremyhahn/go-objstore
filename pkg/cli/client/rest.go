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

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// RESTClient implements the Client interface for REST API servers
type RESTClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewRESTClient creates a new REST client
func NewRESTClient(config *Config) (*RESTClient, error) {
	if config.ServerURL == "" {
		return nil, ErrServerURLRequired
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Note: TLS configuration can be added via http.Client customization

	return &RESTClient{
		baseURL:    strings.TrimSuffix(config.ServerURL, "/"),
		httpClient: httpClient,
	}, nil
}

// Put uploads an object
func (c *RESTClient) Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
	url := fmt.Sprintf("%s/api/v1/objects/%s", c.baseURL, key)

	// Read all data
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	// Add metadata as headers if provided
	if metadata != nil {
		if metadata.ContentType != "" {
			req.Header.Set("Content-Type", metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			req.Header.Set("Content-Encoding", metadata.ContentEncoding)
		}
		// Add custom metadata as X-Custom-* headers
		for k, v := range metadata.Custom {
			req.Header.Set(fmt.Sprintf("X-Custom-%s", k), v)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// Get retrieves an object
func (c *RESTClient) Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error) {
	url := fmt.Sprintf("%s/api/v1/objects/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	// Extract metadata from headers
	metadata := &common.Metadata{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		ETag:            resp.Header.Get("ETag"),
		Custom:          make(map[string]string),
	}

	if sizeStr := resp.Header.Get("Content-Length"); sizeStr != "" {
		if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			metadata.Size = size
		}
	}

	// Extract custom metadata from X-Custom-* headers
	for k, v := range resp.Header {
		if strings.HasPrefix(k, "X-Custom-") {
			customKey := strings.TrimPrefix(k, "X-Custom-")
			if len(v) > 0 {
				metadata.Custom[customKey] = v[0]
			}
		}
	}

	return resp.Body, metadata, nil
}

// Delete removes an object
func (c *RESTClient) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/api/v1/objects/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// Exists checks if an object exists
func (c *RESTClient) Exists(ctx context.Context, key string) (bool, error) {
	url := fmt.Sprintf("%s/api/v1/exists/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, http.NoBody)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode == http.StatusOK, nil
}

// List lists objects with optional filters
func (c *RESTClient) List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	urlStr := fmt.Sprintf("%s/api/v1/objects", c.baseURL)

	// Add query parameters
	params := url.Values{}
	if opts != nil {
		if opts.Prefix != "" {
			params.Set("prefix", opts.Prefix)
		}
		if opts.Delimiter != "" {
			params.Set("delimiter", opts.Delimiter)
		}
		if opts.MaxResults > 0 {
			params.Set("max_results", strconv.Itoa(opts.MaxResults))
		}
		if opts.ContinueFrom != "" {
			params.Set("continue_from", opts.ContinueFrom)
		}
	}

	if len(params) > 0 {
		urlStr += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var result common.ListResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// GetMetadata retrieves object metadata
func (c *RESTClient) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	url := fmt.Sprintf("%s/api/v1/metadata/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var metadata common.Metadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// UpdateMetadata updates object metadata
func (c *RESTClient) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	url := fmt.Sprintf("%s/api/v1/metadata/%s", c.baseURL, key)

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// Archive archives an object
func (c *RESTClient) Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error {
	url := fmt.Sprintf("%s/api/v1/archive", c.baseURL)

	payload := map[string]any{
		"key":                  key,
		"destination_type":     destinationType,
		"destination_settings": destinationSettings,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// AddPolicy adds a lifecycle policy
func (c *RESTClient) AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error {
	url := fmt.Sprintf("%s/api/v1/policies", c.baseURL)

	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// RemovePolicy removes a lifecycle policy
func (c *RESTClient) RemovePolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/api/v1/policies/%s", c.baseURL, policyID)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// GetPolicies retrieves all lifecycle policies
func (c *RESTClient) GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error) {
	url := fmt.Sprintf("%s/api/v1/policies", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var policies []common.LifecyclePolicy
	if err := json.NewDecoder(resp.Body).Decode(&policies); err != nil {
		return nil, err
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies
func (c *RESTClient) ApplyPolicies(ctx context.Context) (policiesCount int, objectsProcessed int, err error) {
	url := fmt.Sprintf("%s/api/v1/policies/apply", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return 0, 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return 0, 0, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return 0, 0, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var result struct {
		PoliciesCount    int `json:"policies_count"`
		ObjectsProcessed int `json:"objects_processed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, 0, err
	}

	return result.PoliciesCount, result.ObjectsProcessed, nil
}

// AddReplicationPolicy adds a replication policy
func (c *RESTClient) AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error {
	url := fmt.Sprintf("%s/api/v1/replication/policies", c.baseURL)

	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// RemoveReplicationPolicy removes a replication policy
func (c *RESTClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/api/v1/replication/policies/%s", c.baseURL, policyID)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// GetReplicationPolicy retrieves a specific replication policy
func (c *RESTClient) GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/api/v1/replication/policies/%s", c.baseURL, policyID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var policy common.ReplicationPolicy
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		return nil, err
	}

	return &policy, nil
}

// GetReplicationPolicies retrieves all replication policies
func (c *RESTClient) GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/api/v1/replication/policies", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var policies []common.ReplicationPolicy
	if err := json.NewDecoder(resp.Body).Decode(&policies); err != nil {
		return nil, err
	}

	return policies, nil
}

// TriggerReplication triggers a replication sync
func (c *RESTClient) TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error) {
	urlStr := fmt.Sprintf("%s/api/v1/replication/trigger", c.baseURL)

	// Add policy_id as query param if provided
	if policyID != "" {
		urlStr += "?policy_id=" + url.QueryEscape(policyID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var result common.SyncResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// GetReplicationStatus retrieves replication status for a specific policy
func (c *RESTClient) GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error) {
	urlStr := fmt.Sprintf("%s/api/v1/replication/status/%s", c.baseURL, policyID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return nil, fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	var status replication.ReplicationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

// Health checks server health
func (c *RESTClient) Health(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) > 0 {
			return fmt.Errorf("%w %d: %s", ErrServerError, resp.StatusCode, string(body))
		}
		return fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// Close closes the client
func (c *RESTClient) Close() error {
	// HTTP client doesn't need explicit closing
	return nil
}
