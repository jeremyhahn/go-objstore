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

package replication

import (
	"context"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ChangeDetector detects changes between source and destination storage backends.
type ChangeDetector struct {
	source common.Storage
	dest   common.Storage
}

// NewChangeDetector creates a new ChangeDetector.
func NewChangeDetector(source, dest common.Storage) *ChangeDetector {
	return &ChangeDetector{
		source: source,
		dest:   dest,
	}
}

// DetectChanges compares source and destination to find objects that need syncing.
// It uses ETag and LastModified metadata for comparison.
// Returns a list of keys that have changed or are new.
func (cd *ChangeDetector) DetectChanges(ctx context.Context, prefix string) ([]string, error) {
	var changedKeys []string

	opts := &common.ListOptions{
		Prefix:     prefix,
		MaxResults: 1000,
	}

	for {
		result, err := cd.source.ListWithOptions(ctx, opts)
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			destMeta, err := cd.dest.GetMetadata(ctx, obj.Key)
			// If error occurs getting dest metadata, assume object doesn't exist or needs sync
			if err != nil || hasChanged(obj.Metadata, destMeta) {
				changedKeys = append(changedKeys, obj.Key)
			}
		}

		if !result.Truncated {
			break
		}
		opts.ContinueFrom = result.NextToken
	}

	return changedKeys, nil
}

// hasChanged compares source and destination metadata to determine if sync is needed.
func hasChanged(src, dest *common.Metadata) bool {
	if dest == nil {
		return true // Object doesn't exist at destination
	}

	// Compare ETags (most reliable)
	if src.ETag != "" && dest.ETag != "" && src.ETag != dest.ETag {
		return true
	}

	// Fall back to size comparison
	if src.Size != dest.Size {
		return true
	}

	// Compare modification time (source is newer)
	if src.LastModified.After(dest.LastModified) {
		return true
	}

	return false
}
