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

// BucketManagerCreator creates a bucket manager from settings.
type BucketManagerCreator func(settings map[string]string) (common.BucketManager, error)

var bucketManagerRegistry = make(map[string]BucketManagerCreator)

// RegisterBucketManager registers a bucket manager creator for a backend type.
func RegisterBucketManager(backendType string, creator BucketManagerCreator) {
	bucketManagerRegistry[backendType] = creator
}

// NewBucketManager creates the bucket manager of a backend type. Archive-only
// backends return ErrArchiveOnlyBackend and unknown types ErrUnknownBackend.
// A registered backend that offers no bucket management returns
// common.ErrNotSupported.
func NewBucketManager(backendType string, settings map[string]string) (common.BucketManager, error) {
	if archiveOnlyTypes[backendType] {
		return nil, ErrArchiveOnlyBackend
	}
	if creator, exists := bucketManagerRegistry[backendType]; exists {
		return creator(settings)
	}
	if _, exists := storageRegistry[backendType]; exists {
		return nil, common.ErrNotSupported
	}
	return nil, ErrUnknownBackend
}
