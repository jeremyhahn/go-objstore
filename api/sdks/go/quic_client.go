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

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
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

// The QUIC client uses HTTP3, so we can leverage the gRPC-over-HTTP3 or use REST-like HTTP3.
// For simplicity, we'll implement it using HTTP3 with a REST-like interface similar to RESTClient,
// but we could also use gRPC-over-HTTP3. Let's use a hybrid approach where we use HTTP3 for
// transport but gRPC-like message encoding for better type safety.

// For this implementation, we'll use HTTP3 with JSON encoding similar to REST.

// Put stores an object.
func (c *QUICClient) Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error) {
	// Create request body with metadata
	reqBody := struct {
		Key      string    `json:"key"`
		Data     []byte    `json:"data"`
		Metadata *Metadata `json:"metadata,omitempty"`
	}{
		Key:      key,
		Data:     data,
		Metadata: metadata,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v1/put", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result PutResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// Get retrieves an object.
func (c *QUICClient) Get(ctx context.Context, key string) (*GetResult, error) {
	url := fmt.Sprintf("%s/api/v1/get?key=%s", c.baseURL, key)
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

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data     []byte    `json:"data"`
		Metadata *Metadata `json:"metadata,omitempty"`
	}

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return &GetResult{
		Data:     result.Data,
		Metadata: result.Metadata,
	}, nil
}

// Delete removes an object.
func (c *QUICClient) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/api/v1/delete?key=%s", c.baseURL, key)
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
func (c *QUICClient) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	reqBody, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v1/list", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ListResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// Exists checks if an object exists.
func (c *QUICClient) Exists(ctx context.Context, key string) (bool, error) {
	url := fmt.Sprintf("%s/api/v1/exists?key=%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var result struct {
		Exists bool `json:"exists"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.Exists, nil
}

// GetMetadata retrieves object metadata.
func (c *QUICClient) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	url := fmt.Sprintf("%s/api/v1/metadata?key=%s", c.baseURL, key)
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

	var metadata Metadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// UpdateMetadata updates object metadata.
func (c *QUICClient) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	reqBody := struct {
		Key      string    `json:"key"`
		Metadata *Metadata `json:"metadata"`
	}{
		Key:      key,
		Metadata: metadata,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/metadata", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("UPDATE metadata failed with status %d", resp.StatusCode)
	}

	return nil
}

// Health performs a health check.
func (c *QUICClient) Health(ctx context.Context) (*HealthStatus, error) {
	url := fmt.Sprintf("%s/api/v1/health", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

// Archive copies an object to archival storage.
func (c *QUICClient) Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error {
	reqBody := map[string]interface{}{
		"key":                  key,
		"destination_type":     destinationType,
		"destination_settings": settings,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/archive", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
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
		return fmt.Errorf("ARCHIVE failed with status %d", resp.StatusCode)
	}

	return nil
}

// AddPolicy adds a lifecycle policy.
func (c *QUICClient) AddPolicy(ctx context.Context, policy *LifecyclePolicy) error {
	body, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/policies", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("ADD policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// RemovePolicy removes a lifecycle policy.
func (c *QUICClient) RemovePolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/api/v1/policies/%s", c.baseURL, policyID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REMOVE policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetPolicies retrieves lifecycle policies.
func (c *QUICClient) GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error) {
	url := fmt.Sprintf("%s/api/v1/policies?prefix=%s", c.baseURL, prefix)
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
		Policies []*LifecyclePolicy `json:"policies"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Policies, nil
}

// ApplyPolicies executes all lifecycle policies.
func (c *QUICClient) ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error) {
	url := fmt.Sprintf("%s/api/v1/policies/apply", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ApplyPoliciesResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// AddReplicationPolicy adds a replication policy.
func (c *QUICClient) AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error {
	body, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/replication/policies", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("ADD replication policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// RemoveReplicationPolicy removes a replication policy.
func (c *QUICClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	url := fmt.Sprintf("%s/api/v1/replication/policies/%s", c.baseURL, policyID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REMOVE replication policy failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetReplicationPolicies retrieves all replication policies.
func (c *QUICClient) GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/api/v1/replication/policies", c.baseURL)
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
		Policies []*ReplicationPolicy `json:"policies"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Policies, nil
}

// GetReplicationPolicy retrieves a specific replication policy.
func (c *QUICClient) GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error) {
	url := fmt.Sprintf("%s/api/v1/replication/policies/%s", c.baseURL, policyID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var policy ReplicationPolicy
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		return nil, err
	}

	return &policy, nil
}

// TriggerReplication triggers replication synchronization.
func (c *QUICClient) TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v1/replication/trigger", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result SyncResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// GetReplicationStatus retrieves replication status and metrics.
func (c *QUICClient) GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error) {
	url := fmt.Sprintf("%s/api/v1/replication/status/%s", c.baseURL, policyID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status ReplicationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

// Close closes the QUIC connection.
func (c *QUICClient) Close() error {
	if c.transport != nil {
		c.transport.Close()
	}
	return nil
}

// Ensure QUICClient implements Client interface
var _ Client = (*QUICClient)(nil)

// Conversion helpers for protobuf types (reuse from grpc_client)
func protoToMetadata(m *objstorepb.Metadata) *Metadata {
	return metadataFromProto(m)
}
