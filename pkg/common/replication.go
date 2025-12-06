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

package common

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrReplicationNotSupported is returned when replication is not supported.
	ErrReplicationNotSupported = errors.New("replication not supported for this backend")
)

// ReplicationMode defines how replication handles encryption.
type ReplicationMode string

const (
	// ReplicationModeTransparent decrypts at source and re-encrypts at destination.
	// Use when: Different DEKs or need to process plaintext
	ReplicationModeTransparent ReplicationMode = "transparent"

	// ReplicationModeOpaque copies encrypted blobs as-is (no DEK operations).
	// Use when: Same DEKs, backup scenarios, performance critical
	ReplicationModeOpaque ReplicationMode = "opaque"
)

// EncryptionConfig specifies encryption settings for a replication layer.
type EncryptionConfig struct {
	Enabled    bool   `json:"enabled"`
	Provider   string `json:"provider"`    // "noop", "custom"
	DefaultKey string `json:"default_key"` // Provider-agnostic key ID
}

// EncryptionPolicy defines the three-layer encryption configuration.
type EncryptionPolicy struct {
	Backend     *EncryptionConfig `json:"backend,omitempty"`     // Layer 1: Backend at-rest encryption
	Source      *EncryptionConfig `json:"source,omitempty"`      // Layer 2: Client-side source DEK
	Destination *EncryptionConfig `json:"destination,omitempty"` // Layer 3: Client-side destination DEK
}

// ReplicationPolicy defines a replication configuration.
type ReplicationPolicy struct {
	ID                  string            `json:"id"`
	SourceBackend       string            `json:"source_backend"`
	SourceSettings      map[string]string `json:"source_settings"`
	SourcePrefix        string            `json:"source_prefix,omitempty"`
	DestinationBackend  string            `json:"destination_backend"`
	DestinationSettings map[string]string `json:"destination_settings"`
	CheckInterval       time.Duration     `json:"check_interval"`
	LastSyncTime        time.Time         `json:"last_sync_time"`
	Enabled             bool              `json:"enabled"`
	ReplicationMode     ReplicationMode   `json:"replication_mode"`
	Encryption          *EncryptionPolicy `json:"encryption,omitempty"`
}

// SyncResult contains the results of a sync operation.
type SyncResult struct {
	PolicyID   string        `json:"policy_id"`
	Synced     int           `json:"synced"`
	Deleted    int           `json:"deleted"`
	Failed     int           `json:"failed"`
	BytesTotal int64         `json:"bytes_total"`
	Duration   time.Duration `json:"duration"`
	Errors     []string      `json:"errors,omitempty"`
}

// ReplicationManager manages replication policies and sync operations.
type ReplicationManager interface {
	// AddPolicy adds a new replication policy.
	AddPolicy(policy ReplicationPolicy) error

	// RemovePolicy removes a replication policy.
	RemovePolicy(id string) error

	// GetPolicy retrieves a specific replication policy.
	GetPolicy(id string) (*ReplicationPolicy, error)

	// GetPolicies returns all replication policies.
	GetPolicies() ([]ReplicationPolicy, error)

	// SyncAll syncs all enabled policies.
	SyncAll(ctx context.Context) (*SyncResult, error)

	// SyncPolicy syncs a specific policy.
	SyncPolicy(ctx context.Context, policyID string) (*SyncResult, error)

	// SetBackendEncrypterFactory sets the backend at-rest encryption factory for a policy.
	SetBackendEncrypterFactory(policyID string, factory EncrypterFactory) error

	// SetSourceEncrypterFactory sets the source DEK encryption factory for a policy.
	SetSourceEncrypterFactory(policyID string, factory EncrypterFactory) error

	// SetDestinationEncrypterFactory sets the destination DEK encryption factory for a policy.
	SetDestinationEncrypterFactory(policyID string, factory EncrypterFactory) error

	// Run starts the background sync ticker.
	Run(ctx context.Context)
}
