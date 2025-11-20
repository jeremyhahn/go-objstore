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

// QUICClient implements the Client interface for QUIC (HTTP/3) servers
// Note: For now, this uses standard HTTP client. Full HTTP/3 support requires
// the server to advertise HTTP/3 via Alt-Svc headers or connection upgrade.
type QUICClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewQUICClient creates a new QUIC client
func NewQUICClient(config *Config) (*QUICClient, error) {
	if config.ServerURL == "" {
		return nil, ErrServerURLRequired
	}

	// Note: Using http.Client for compatibility; native HTTP/3 transport coming in future quic-go releases
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	return &QUICClient{
		baseURL:    strings.TrimSuffix(config.ServerURL, "/"),
		httpClient: httpClient,
	}, nil
}

// Put uploads an object
func (c *QUICClient) Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)

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
func (c *QUICClient) Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error) {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)

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
func (c *QUICClient) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)

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
func (c *QUICClient) Exists(ctx context.Context, key string) (bool, error) {
	url := fmt.Sprintf("%s/exists/%s", c.baseURL, key)

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
func (c *QUICClient) List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	urlStr := fmt.Sprintf("%s/objects", c.baseURL)

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
func (c *QUICClient) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w %d", ErrServerError, resp.StatusCode)
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

	return metadata, nil
}

// UpdateMetadata updates object metadata
func (c *QUICClient) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, http.NoBody)
	if err != nil {
		return err
	}

	// Set metadata as headers
	if metadata != nil {
		if metadata.ContentType != "" {
			req.Header.Set("Content-Type", metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			req.Header.Set("Content-Encoding", metadata.ContentEncoding)
		}
		for k, v := range metadata.Custom {
			req.Header.Set(fmt.Sprintf("X-Custom-%s", k), v)
		}
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

// Archive archives an object
func (c *QUICClient) Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error {
	url := fmt.Sprintf("%s/archive", c.baseURL)

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
func (c *QUICClient) AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error {
	url := fmt.Sprintf("%s/policies", c.baseURL)

	payload := map[string]any{
		"id":                policy.ID,
		"prefix":            policy.Prefix,
		"retention_seconds": int64(policy.Retention.Seconds()),
		"action":            policy.Action,
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
func (c *QUICClient) RemovePolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/policies/%s", c.baseURL, policyID)

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
func (c *QUICClient) GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error) {
	url := fmt.Sprintf("%s/policies", c.baseURL)

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

	var result struct {
		Policies []struct {
			ID               string `json:"id"`
			Prefix           string `json:"prefix"`
			RetentionSeconds int64  `json:"retention_seconds"`
			Action           string `json:"action"`
		} `json:"policies"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	policies := make([]common.LifecyclePolicy, len(result.Policies))
	for i, p := range result.Policies {
		policies[i] = common.LifecyclePolicy{
			ID:        p.ID,
			Prefix:    p.Prefix,
			Retention: time.Duration(p.RetentionSeconds) * time.Second,
			Action:    p.Action,
		}
	}

	return policies, nil
}

// ApplyPolicies executes all lifecycle policies
func (c *QUICClient) ApplyPolicies(ctx context.Context) (policiesCount int, objectsProcessed int, err error) {
	url := fmt.Sprintf("%s/policies/apply", c.baseURL)

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

// Health checks server health
func (c *QUICClient) Health(ctx context.Context) error {
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

// Close closes the QUIC client
func (c *QUICClient) Close() error {
	// HTTP/3 client doesn't need explicit closing
	// The transport will close connections automatically
	return nil
}

// Replication operations
func (c *QUICClient) AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error {
	url := fmt.Sprintf("%s/replication/policies", c.baseURL)

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

func (c *QUICClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, policyID)

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

func (c *QUICClient) GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/replication/policies/%s", c.baseURL, policyID)

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

func (c *QUICClient) GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/replication/policies", c.baseURL)

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

func (c *QUICClient) TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error) {
	urlStr := fmt.Sprintf("%s/replication/trigger", c.baseURL)

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

func (c *QUICClient) GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error) {
	urlStr := fmt.Sprintf("%s/replication/status/%s", c.baseURL, policyID)

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
