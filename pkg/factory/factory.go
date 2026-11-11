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

package factory

import (
	"errors"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// StorageCreator is a function that creates a storage backend.
type StorageCreator func(settings map[string]string, creds common.Credentials) (common.Storage, error)

// ArchiverCreator is a function that creates an archiver.
type ArchiverCreator func(settings map[string]string, creds common.Credentials) (common.Archiver, error)

var (
	storageRegistry  = make(map[string]StorageCreator)
	archiverRegistry = make(map[string]ArchiverCreator)
	archiveOnlyTypes = map[string]bool{
		"glacier":      true,
		"azurearchive": true,
	}
)

// RegisterStorage registers a storage backend creator.
func RegisterStorage(backendType string, creator StorageCreator) {
	storageRegistry[backendType] = creator
}

// RegisterArchiver registers an archiver creator.
//
// An archiver whose settings name a path on the server's filesystem must also
// be listed in pathArchiverTypes (wire_options.go), so CheckClientArchiverType
// refuses it from clients with ErrClientLocalArchiver unless the server sets
// archive.allow_client_local_destinations (#236). Registering such a type
// without listing it would let any client write to a server path.
func RegisterArchiver(backendType string, creator ArchiverCreator) {
	archiverRegistry[backendType] = creator
}

// NewStorage creates a new storage backend based on the given type.
func NewStorage(backendType string, settings map[string]string, creds common.Credentials) (common.Storage, error) {
	// Check if this is an archive-only backend
	if archiveOnlyTypes[backendType] {
		return nil, ErrArchiveOnlyBackend
	}

	creator, exists := storageRegistry[backendType]
	if !exists {
		return nil, ErrUnknownBackend
	}
	return creator(settings, creds)
}

// NewArchiver creates a new archiver based on the given type.
func NewArchiver(backendType string, settings map[string]string, creds common.Credentials) (common.Archiver, error) {
	creator, exists := archiverRegistry[backendType]
	if !exists {
		return nil, ErrUnknownArchiver
	}
	return creator(settings, creds)
}

// NewArchiverFromWire builds an archiver from destination settings received
// over a wire protocol (REST, gRPC, QUIC, unix socket, MCP). The credential
// entries are moved out of wire into a protected-memory handle before the
// backend sees them, and the handle is destroyed when the archiver is built.
// wire is left without credential entries. A request that carries no credential
// uses the SDK default chain.
//
// A filesystem-path archiver type (local) is refused with
// ErrClientLocalArchiver; a server that allows client-supplied local
// destinations calls NewArchiverFromWireOpts.
func NewArchiverFromWire(backendType string, wire map[string]string) (common.Archiver, error) {
	return NewArchiverFromWireOpts(backendType, wire, WireOptions{})
}

func newArchiverFromWire(backendType string, wire map[string]string) (common.Archiver, error) {
	settings, creds, release, err := common.ExtractCredentialSettings(wire)
	if err != nil {
		return nil, err
	}
	archiver, buildErr := NewArchiver(backendType, settings, creds)
	if err := errors.Join(buildErr, release()); err != nil {
		return nil, err
	}
	return archiver, nil
}

// ListStorageBackends returns a list of all registered storage backend types.
// Archive-only backends (glacier, azurearchive) are excluded from this list.
func ListStorageBackends() []string {
	backends := make([]string, 0, len(storageRegistry))
	for backendType := range storageRegistry {
		if !archiveOnlyTypes[backendType] {
			backends = append(backends, backendType)
		}
	}
	return backends
}

// ListArchivers returns a list of all registered archiver types.
func ListArchivers() []string {
	archivers := make([]string, 0, len(archiverRegistry))
	for archiverType := range archiverRegistry {
		archivers = append(archivers, archiverType)
	}
	return archivers
}

// IsStorageBackendRegistered checks if a storage backend type is registered.
func IsStorageBackendRegistered(backendType string) bool {
	_, exists := storageRegistry[backendType]
	return exists && !archiveOnlyTypes[backendType]
}

// IsArchiverRegistered checks if an archiver type is registered.
func IsArchiverRegistered(archiverType string) bool {
	_, exists := archiverRegistry[archiverType]
	return exists
}
