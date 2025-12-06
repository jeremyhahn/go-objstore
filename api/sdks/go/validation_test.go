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

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{
			name:    "valid key",
			key:     "test-key",
			wantErr: nil,
		},
		{
			name:    "valid key with path",
			key:     "path/to/key",
			wantErr: nil,
		},
		{
			name:    "empty key",
			key:     "",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "whitespace only key",
			key:     "   ",
			wantErr: ErrInvalidKey,
		},
		{
			name:    "key with leading/trailing spaces is valid",
			key:     " key ",
			wantErr: nil, // Will be trimmed and considered valid if not empty after trim
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateKey(tt.key)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateData(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "valid data",
			data:    []byte("test data"),
			wantErr: nil,
		},
		{
			name:    "empty data slice is valid",
			data:    []byte{},
			wantErr: nil,
		},
		{
			name:    "nil data",
			data:    nil,
			wantErr: ErrInvalidData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateData(tt.data)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePolicyID(t *testing.T) {
	tests := []struct {
		name     string
		policyID string
		wantErr  error
	}{
		{
			name:     "valid policy ID",
			policyID: "policy-123",
			wantErr:  nil,
		},
		{
			name:     "empty policy ID",
			policyID: "",
			wantErr:  ErrInvalidPolicyID,
		},
		{
			name:     "whitespace only policy ID",
			policyID: "   ",
			wantErr:  ErrInvalidPolicyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePolicyID(tt.policyID)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateLifecyclePolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  *LifecyclePolicy
		wantErr error
	}{
		{
			name: "valid policy",
			policy: &LifecyclePolicy{
				ID:               "policy-1",
				Prefix:           "test/",
				RetentionSeconds: 3600,
				Action:           "archive",
			},
			wantErr: nil,
		},
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: ErrInvalidPolicy,
		},
		{
			name: "empty policy ID",
			policy: &LifecyclePolicy{
				ID:               "",
				Prefix:           "test/",
				RetentionSeconds: 3600,
				Action:           "archive",
			},
			wantErr: ErrInvalidPolicyID,
		},
		{
			name: "whitespace policy ID",
			policy: &LifecyclePolicy{
				ID:               "   ",
				Prefix:           "test/",
				RetentionSeconds: 3600,
				Action:           "archive",
			},
			wantErr: ErrInvalidPolicyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLifecyclePolicy(tt.policy)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateReplicationPolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  *ReplicationPolicy
		wantErr error
	}{
		{
			name: "valid policy",
			policy: &ReplicationPolicy{
				ID:                   "repl-1",
				SourceBackend:        "local",
				DestinationBackend:   "s3",
				CheckIntervalSeconds: 60,
				Enabled:              true,
			},
			wantErr: nil,
		},
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: ErrInvalidPolicy,
		},
		{
			name: "empty policy ID",
			policy: &ReplicationPolicy{
				ID:                   "",
				SourceBackend:        "local",
				DestinationBackend:   "s3",
				CheckIntervalSeconds: 60,
			},
			wantErr: ErrInvalidPolicyID,
		},
		{
			name: "whitespace policy ID",
			policy: &ReplicationPolicy{
				ID:                   "   ",
				SourceBackend:        "local",
				DestinationBackend:   "s3",
				CheckIntervalSeconds: 60,
			},
			wantErr: ErrInvalidPolicyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReplicationPolicy(tt.policy)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata *Metadata
		wantErr  error
	}{
		{
			name: "valid metadata",
			metadata: &Metadata{
				ContentType: "application/json",
				Size:        1024,
			},
			wantErr: nil,
		},
		{
			name:     "empty metadata is valid",
			metadata: &Metadata{},
			wantErr:  nil,
		},
		{
			name:     "nil metadata",
			metadata: nil,
			wantErr:  ErrInvalidMetadata,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMetadata(tt.metadata)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
