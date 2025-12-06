// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"context"
	"io"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
)

// Client is the unified interface for interacting with go-objstore.
// It supports REST, gRPC, and QUIC/HTTP3 protocols.
type Client interface {
	// Object operations
	Put(ctx context.Context, key string, data []byte, metadata *Metadata) (*PutResult, error)
	Get(ctx context.Context, key string) (*GetResult, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, opts *ListOptions) (*ListResult, error)
	Exists(ctx context.Context, key string) (bool, error)

	// Metadata operations
	GetMetadata(ctx context.Context, key string) (*Metadata, error)
	UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error

	// Health check
	Health(ctx context.Context) (*HealthStatus, error)

	// Archive operations
	Archive(ctx context.Context, key string, destinationType string, settings map[string]string) error

	// Lifecycle policy operations
	AddPolicy(ctx context.Context, policy *LifecyclePolicy) error
	RemovePolicy(ctx context.Context, policyID string) error
	GetPolicies(ctx context.Context, prefix string) ([]*LifecyclePolicy, error)
	ApplyPolicies(ctx context.Context) (*ApplyPoliciesResult, error)

	// Replication policy operations
	AddReplicationPolicy(ctx context.Context, policy *ReplicationPolicy) error
	RemoveReplicationPolicy(ctx context.Context, policyID string) error
	GetReplicationPolicies(ctx context.Context) ([]*ReplicationPolicy, error)
	GetReplicationPolicy(ctx context.Context, policyID string) (*ReplicationPolicy, error)
	TriggerReplication(ctx context.Context, opts *TriggerReplicationOptions) (*SyncResult, error)
	GetReplicationStatus(ctx context.Context, policyID string) (*ReplicationStatus, error)

	// Close the client connection
	Close() error
}

// Metadata represents object metadata.
type Metadata struct {
	ContentType     string
	ContentEncoding string
	Size            int64
	LastModified    time.Time
	ETag            string
	Custom          map[string]string
}

// PutResult contains the response from a Put operation.
type PutResult struct {
	Success bool
	Message string
	ETag    string
}

// GetResult contains the response from a Get operation.
type GetResult struct {
	Data     []byte
	Metadata *Metadata
	Reader   io.ReadCloser // For streaming support
}

// ListOptions configures a List operation.
type ListOptions struct {
	Prefix       string
	Delimiter    string
	MaxResults   int32
	ContinueFrom string
}

// ListResult contains the response from a List operation.
type ListResult struct {
	Objects        []*ObjectInfo
	CommonPrefixes []string
	NextToken      string
	Truncated      bool
}

// ObjectInfo represents information about a stored object.
type ObjectInfo struct {
	Key      string
	Metadata *Metadata
}

// HealthStatus represents the health check response.
type HealthStatus struct {
	Status  string
	Message string
}

// LifecyclePolicy represents a lifecycle policy for objects.
type LifecyclePolicy struct {
	ID                  string
	Prefix              string
	RetentionSeconds    int64
	Action              string
	DestinationType     string
	DestinationSettings map[string]string
}

// ApplyPoliciesResult contains the results of applying lifecycle policies.
type ApplyPoliciesResult struct {
	Success          bool
	PoliciesCount    int32
	ObjectsProcessed int32
	Message          string
}

// EncryptionConfig defines encryption settings for a single layer.
type EncryptionConfig struct {
	Enabled    bool
	Provider   string
	DefaultKey string
}

// EncryptionPolicy defines encryption configuration for all three layers.
type EncryptionPolicy struct {
	Backend     *EncryptionConfig
	Source      *EncryptionConfig
	Destination *EncryptionConfig
}

// ReplicationMode specifies how replication should handle encrypted data.
type ReplicationMode int32

const (
	ReplicationModeTransparent ReplicationMode = 0 // TRANSPARENT decrypts at source and re-encrypts at destination
	ReplicationModeOpaque      ReplicationMode = 1 // OPAQUE copies encrypted blobs as-is
)

// ReplicationPolicy defines a replication configuration between storage backends.
type ReplicationPolicy struct {
	ID                     string
	SourceBackend          string
	SourceSettings         map[string]string
	SourcePrefix           string
	DestinationBackend     string
	DestinationSettings    map[string]string
	CheckIntervalSeconds   int64
	LastSyncTime           time.Time
	Enabled                bool
	Encryption             *EncryptionPolicy
	ReplicationMode        ReplicationMode
}

// TriggerReplicationOptions configures a replication trigger request.
type TriggerReplicationOptions struct {
	PolicyID    string
	Parallel    bool
	WorkerCount int32
}

// SyncResult contains the results of a replication sync operation.
type SyncResult struct {
	PolicyID   string
	Synced     int32
	Deleted    int32
	Failed     int32
	BytesTotal int64
	DurationMs int64
	Errors     []string
}

// ReplicationStatus contains policy information and metrics.
type ReplicationStatus struct {
	PolicyID              string
	SourceBackend         string
	DestinationBackend    string
	Enabled               bool
	TotalObjectsSynced    int64
	TotalObjectsDeleted   int64
	TotalBytesSynced      int64
	TotalErrors           int64
	LastSyncTime          time.Time
	AverageSyncDurationMs int64
	SyncCount             int64
}

// Protocol represents the supported protocol types.
type Protocol string

const (
	ProtocolREST Protocol = "rest"
	ProtocolGRPC Protocol = "grpc"
	ProtocolQUIC Protocol = "quic"
)

// RetryConfig defines retry behavior for transient failures.
type RetryConfig struct {
	// Enabled controls whether retries are enabled (default: false for backwards compatibility)
	Enabled bool

	// MaxRetries is the maximum number of retry attempts (default: 3)
	MaxRetries int

	// InitialBackoff is the initial backoff duration (default: 100ms)
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration (default: 5s)
	MaxBackoff time.Duration

	// RetryableErrors is a list of sentinel errors that should trigger retries.
	// If empty, a default set of transient errors will be used.
	RetryableErrors []error
}

// ClientConfig holds configuration for creating a client.
type ClientConfig struct {
	Protocol Protocol
	Address  string

	// TLS configuration
	UseTLS             bool
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool

	// Timeouts
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration

	// gRPC-specific
	MaxRecvMsgSize int
	MaxSendMsgSize int

	// QUIC-specific
	MaxStreams int

	// Retry configuration
	Retry *RetryConfig
}

// NewClient creates a new client with the specified configuration.
func NewClient(config *ClientConfig) (Client, error) {
	switch config.Protocol {
	case ProtocolREST:
		return newRESTClient(config)
	case ProtocolGRPC:
		return newGRPCClient(config)
	case ProtocolQUIC:
		return newQUICClient(config)
	default:
		return nil, ErrInvalidProtocol
	}
}

// Helper functions to convert between SDK types and protobuf types

func metadataToProto(m *Metadata) *objstorepb.Metadata {
	if m == nil {
		return nil
	}
	return &objstorepb.Metadata{
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		Size:            m.Size,
		Etag:            m.ETag,
		Custom:          m.Custom,
	}
}

func metadataFromProto(m *objstorepb.Metadata) *Metadata {
	if m == nil {
		return nil
	}
	return &Metadata{
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		Size:            m.Size,
		ETag:            m.Etag,
		Custom:          m.Custom,
	}
}

func lifecyclePolicyToProto(p *LifecyclePolicy) *objstorepb.LifecyclePolicy {
	if p == nil {
		return nil
	}
	return &objstorepb.LifecyclePolicy{
		Id:                  p.ID,
		Prefix:              p.Prefix,
		RetentionSeconds:    p.RetentionSeconds,
		Action:              p.Action,
		DestinationType:     p.DestinationType,
		DestinationSettings: p.DestinationSettings,
	}
}

func lifecyclePolicyFromProto(p *objstorepb.LifecyclePolicy) *LifecyclePolicy {
	if p == nil {
		return nil
	}
	return &LifecyclePolicy{
		ID:                  p.Id,
		Prefix:              p.Prefix,
		RetentionSeconds:    p.RetentionSeconds,
		Action:              p.Action,
		DestinationType:     p.DestinationType,
		DestinationSettings: p.DestinationSettings,
	}
}

func encryptionConfigToProto(e *EncryptionConfig) *objstorepb.EncryptionConfig {
	if e == nil {
		return nil
	}
	return &objstorepb.EncryptionConfig{
		Enabled:    e.Enabled,
		Provider:   e.Provider,
		DefaultKey: e.DefaultKey,
	}
}

func encryptionConfigFromProto(e *objstorepb.EncryptionConfig) *EncryptionConfig {
	if e == nil {
		return nil
	}
	return &EncryptionConfig{
		Enabled:    e.Enabled,
		Provider:   e.Provider,
		DefaultKey: e.DefaultKey,
	}
}

func encryptionPolicyToProto(e *EncryptionPolicy) *objstorepb.EncryptionPolicy {
	if e == nil {
		return nil
	}
	return &objstorepb.EncryptionPolicy{
		Backend:     encryptionConfigToProto(e.Backend),
		Source:      encryptionConfigToProto(e.Source),
		Destination: encryptionConfigToProto(e.Destination),
	}
}

func encryptionPolicyFromProto(e *objstorepb.EncryptionPolicy) *EncryptionPolicy {
	if e == nil {
		return nil
	}
	return &EncryptionPolicy{
		Backend:     encryptionConfigFromProto(e.Backend),
		Source:      encryptionConfigFromProto(e.Source),
		Destination: encryptionConfigFromProto(e.Destination),
	}
}

func replicationPolicyToProto(p *ReplicationPolicy) *objstorepb.ReplicationPolicy {
	if p == nil {
		return nil
	}
	return &objstorepb.ReplicationPolicy{
		Id:                   p.ID,
		SourceBackend:        p.SourceBackend,
		SourceSettings:       p.SourceSettings,
		SourcePrefix:         p.SourcePrefix,
		DestinationBackend:   p.DestinationBackend,
		DestinationSettings:  p.DestinationSettings,
		CheckIntervalSeconds: p.CheckIntervalSeconds,
		Enabled:              p.Enabled,
		Encryption:           encryptionPolicyToProto(p.Encryption),
		ReplicationMode:      objstorepb.ReplicationMode(p.ReplicationMode),
	}
}

func replicationPolicyFromProto(p *objstorepb.ReplicationPolicy) *ReplicationPolicy {
	if p == nil {
		return nil
	}
	return &ReplicationPolicy{
		ID:                   p.Id,
		SourceBackend:        p.SourceBackend,
		SourceSettings:       p.SourceSettings,
		SourcePrefix:         p.SourcePrefix,
		DestinationBackend:   p.DestinationBackend,
		DestinationSettings:  p.DestinationSettings,
		CheckIntervalSeconds: p.CheckIntervalSeconds,
		Enabled:              p.Enabled,
		Encryption:           encryptionPolicyFromProto(p.Encryption),
		ReplicationMode:      ReplicationMode(p.ReplicationMode),
	}
}
