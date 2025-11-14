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
	"context"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// Client defines the interface for remote object storage operations.
// Implementations provide access to REST, gRPC, and QUIC servers.
type Client interface {
	// Object operations
	Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error
	Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error)

	// Metadata operations
	GetMetadata(ctx context.Context, key string) (*common.Metadata, error)
	UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error

	// Archive operations
	Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error

	// Lifecycle policy operations
	AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error
	RemovePolicy(ctx context.Context, policyID string) error
	GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error)
	ApplyPolicies(ctx context.Context) (int, int, error) // Returns (policies_count, objects_processed, error)

	// Replication operations
	AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error
	RemoveReplicationPolicy(ctx context.Context, policyID string) error
	GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error)
	GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error)
	TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error)
	GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error)

	// Health check
	Health(ctx context.Context) error

	// Close the client connection
	Close() error
}

// Config holds configuration for creating a client
type Config struct {
	ServerURL string
	Protocol  string // rest, grpc, or quic
	TLSConfig *adapters.TLSConfig
}
