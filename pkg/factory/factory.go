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
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// StorageCreator is a function that creates a storage backend.
type StorageCreator func(settings map[string]string) (common.Storage, error)

// ArchiverCreator is a function that creates an archiver.
type ArchiverCreator func(settings map[string]string) (common.Archiver, error)

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
func RegisterArchiver(backendType string, creator ArchiverCreator) {
	archiverRegistry[backendType] = creator
}

// NewStorage creates a new storage backend based on the given type.
func NewStorage(backendType string, settings map[string]string) (common.Storage, error) {
	// Check if this is an archive-only backend
	if archiveOnlyTypes[backendType] {
		return nil, ErrArchiveOnlyBackend
	}

	creator, exists := storageRegistry[backendType]
	if !exists {
		return nil, ErrUnknownBackend
	}
	return creator(settings)
}

// NewArchiver creates a new archiver based on the given type.
func NewArchiver(backendType string, settings map[string]string) (common.Archiver, error) {
	creator, exists := archiverRegistry[backendType]
	if !exists {
		return nil, ErrUnknownArchiver
	}
	return creator(settings)
}
