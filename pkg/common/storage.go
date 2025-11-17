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
	"io"
)

// Storage is the common interface for all storage backends.
type Storage interface {
	LifecycleManager

	// Configure sets up the backend with the necessary credentials and settings.
	Configure(settings map[string]string) error

	// Put stores an object in the backend.
	Put(key string, data io.Reader) error

	// PutWithContext stores an object in the backend with context support.
	PutWithContext(ctx context.Context, key string, data io.Reader) error

	// PutWithMetadata stores an object with associated metadata.
	PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *Metadata) error

	// Get retrieves an object from the backend.
	Get(key string) (io.ReadCloser, error)

	// GetWithContext retrieves an object from the backend with context support.
	GetWithContext(ctx context.Context, key string) (io.ReadCloser, error)

	// GetMetadata retrieves only the metadata for an object.
	GetMetadata(ctx context.Context, key string) (*Metadata, error)

	// UpdateMetadata updates the metadata for an existing object.
	UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error

	// Delete removes an object from the backend.
	Delete(key string) error

	// DeleteWithContext removes an object from the backend with context support.
	DeleteWithContext(ctx context.Context, key string) error

	// Exists checks if an object exists in the backend.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns a list of keys that start with the given prefix.
	List(prefix string) ([]string, error)

	// ListWithContext returns a list of keys with context support.
	ListWithContext(ctx context.Context, prefix string) ([]string, error)

	// ListWithOptions returns a paginated list of objects with full metadata.
	ListWithOptions(ctx context.Context, opts *ListOptions) (*ListResult, error)

	// Archive copies an object to another backend for archival.
	Archive(key string, destination Archiver) error
}

// ReplicationCapable extends Storage with replication capabilities.
type ReplicationCapable interface {
	Storage

	// GetReplicationManager returns the replication manager if supported.
	GetReplicationManager() (ReplicationManager, error)
}
