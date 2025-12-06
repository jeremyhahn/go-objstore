// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGRPCClient_NewClient_InvalidConfig(t *testing.T) {
	_, err := newGRPCClient(nil)
	assert.Equal(t, ErrInvalidConfig, err)
}

func TestGRPCClient_BuildTLSConfig(t *testing.T) {
	config := &ClientConfig{
		Protocol:           ProtocolGRPC,
		Address:            "localhost:50051",
		UseTLS:             true,
		InsecureSkipVerify: true,
	}

	tlsConfig, err := buildTLSConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, tlsConfig)
	assert.True(t, tlsConfig.InsecureSkipVerify)
}

func TestGRPCClient_BuildTLSConfig_WithInvalidCA(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolGRPC,
		Address:  "localhost:50051",
		UseTLS:   true,
		CAFile:   "/nonexistent/ca.pem",
	}

	_, err := buildTLSConfig(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read CA file")
}

func TestGRPCClient_BuildTLSConfig_WithInvalidCert(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolGRPC,
		Address:  "localhost:50051",
		UseTLS:   true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}

	_, err := buildTLSConfig(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
}

func TestGRPCClient_Interface(t *testing.T) {
	// Verify GRPCClient implements Client interface at compile time
	var _ Client = (*GRPCClient)(nil)
}

func TestGRPCClient_Put_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	tests := []struct {
		name    string
		key     string
		data    []byte
		wantErr error
	}{
		{
			name:    "empty key",
			key:     "",
			data:    []byte("data"),
			wantErr: ErrInvalidKey,
		},
		{
			name:    "nil data",
			key:     "key",
			data:    nil,
			wantErr: ErrInvalidData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Put(nil, tt.key, tt.data, nil)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGRPCClient_Get_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	_, err := client.Get(nil, "")
	assert.ErrorIs(t, err, ErrInvalidKey)
}

func TestGRPCClient_Delete_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	err := client.Delete(nil, "")
	assert.ErrorIs(t, err, ErrInvalidKey)
}

func TestGRPCClient_Exists_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	_, err := client.Exists(nil, "")
	assert.ErrorIs(t, err, ErrInvalidKey)
}

func TestGRPCClient_GetMetadata_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	_, err := client.GetMetadata(nil, "")
	assert.ErrorIs(t, err, ErrInvalidKey)
}

func TestGRPCClient_UpdateMetadata_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	tests := []struct {
		name     string
		key      string
		metadata *Metadata
		wantErr  error
	}{
		{
			name:     "empty key",
			key:      "",
			metadata: &Metadata{},
			wantErr:  ErrInvalidKey,
		},
		{
			name:     "nil metadata",
			key:      "key",
			metadata: nil,
			wantErr:  ErrInvalidMetadata,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.UpdateMetadata(nil, tt.key, tt.metadata)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGRPCClient_AddPolicy_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	tests := []struct {
		name    string
		policy  *LifecyclePolicy
		wantErr error
	}{
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: ErrInvalidPolicy,
		},
		{
			name: "empty policy ID",
			policy: &LifecyclePolicy{
				ID: "",
			},
			wantErr: ErrInvalidPolicyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.AddPolicy(nil, tt.policy)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGRPCClient_RemovePolicy_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	err := client.RemovePolicy(nil, "")
	assert.ErrorIs(t, err, ErrInvalidPolicyID)
}

func TestGRPCClient_AddReplicationPolicy_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	tests := []struct {
		name    string
		policy  *ReplicationPolicy
		wantErr error
	}{
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: ErrInvalidPolicy,
		},
		{
			name: "empty policy ID",
			policy: &ReplicationPolicy{
				ID: "",
			},
			wantErr: ErrInvalidPolicyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.AddReplicationPolicy(nil, tt.policy)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGRPCClient_RemoveReplicationPolicy_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	err := client.RemoveReplicationPolicy(nil, "")
	assert.ErrorIs(t, err, ErrInvalidPolicyID)
}

func TestGRPCClient_GetReplicationPolicy_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	_, err := client.GetReplicationPolicy(nil, "")
	assert.ErrorIs(t, err, ErrInvalidPolicyID)
}

func TestGRPCClient_GetReplicationStatus_ValidationErrors(t *testing.T) {
	client := &GRPCClient{
		config: &ClientConfig{},
	}

	_, err := client.GetReplicationStatus(nil, "")
	assert.ErrorIs(t, err, ErrInvalidPolicyID)
}
