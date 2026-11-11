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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Custom metadata keys SyncObject writes on each destination copy: the ETag,
// size, precise modification time (Unix nanoseconds) and a digest of the
// content type, content encoding and user metadata of the source object it
// was copied from. Lowercase, as S3 returns user metadata keys lowercased.
// ETags alone are not enough: local and memory ETags are Unix seconds plus
// size, so a same-size rewrite in the same second keeps its ETag. They are
// common.ReplicationFingerprintKeys, which do not count toward the
// user-metadata entry limit.
const (
	MetaReplicationSourceETag     = "replication_source_etag"
	MetaReplicationSourceSize     = "replication_source_size"
	MetaReplicationSourceModified = "replication_source_modified"
	MetaReplicationSourceMeta     = "replication_source_meta"
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
			if err != nil {
				changedKeys = append(changedKeys, obj.Key)
				continue
			}
			srcMeta, err := cd.sourceMetadata(ctx, obj, destMeta)
			if err != nil {
				return nil, err
			}
			if hasChanged(srcMeta, destMeta) {
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

// sourceMetadata is the metadata hasChanged compares for obj. Against a
// recorded fingerprint it is the source's full metadata, the same call
// SyncObject records it from: a listing may carry no user metadata and
// a different modification time. When that call fails the listing's
// metadata is used.
func (cd *ChangeDetector) sourceMetadata(ctx context.Context, obj *common.ObjectInfo, dest *common.Metadata) (*common.Metadata, error) {
	// GCS and Azure listings carry no metadata: the source has to be
	// looked up whatever the destination holds, and a failed lookup is
	// reported rather than compared as nothing.
	if obj.Metadata == nil {
		md, err := cd.source.GetMetadata(ctx, obj.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to get source metadata for %s: %w", obj.Key, err)
		}
		if md == nil {
			md = &common.Metadata{}
		}
		return md, nil
	}
	if dest == nil || dest.Custom[MetaReplicationSourceETag] == "" {
		return obj.Metadata, nil
	}
	md, err := cd.source.GetMetadata(ctx, obj.Key)
	if err != nil || md == nil {
		return obj.Metadata, nil
	}
	return md, nil
}

// hasChanged compares source and destination metadata to determine if sync is needed.
func hasChanged(src, dest *common.Metadata) bool {
	if dest == nil {
		return true // Object doesn't exist at destination
	}

	// A copy made by SyncObject records the source fingerprint it was made
	// from. ETags are not comparable across backends (local is mtime-size,
	// S3 is an MD5), so when the record is there it decides alone. A record
	// missing any field (an older copy) counts as changed.
	if recorded, ok := dest.Custom[MetaReplicationSourceETag]; ok && src.ETag != "" {
		return recorded != src.ETag ||
			!recordIs(dest, MetaReplicationSourceSize, strconv.FormatInt(src.Size, 10)) ||
			!recordIs(dest, MetaReplicationSourceModified, modifiedStamp(src)) ||
			!recordIs(dest, MetaReplicationSourceMeta, metadataDigest(src))
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

// recordIs reports whether dest records key with value want.
func recordIs(dest *common.Metadata, key, want string) bool {
	got, ok := dest.Custom[key]
	return ok && got == want
}

// sourceFingerprint returns the metadata to write with the destination copy
// of an object: src with the source's ETag and size recorded under
// MetaReplicationSourceETag and MetaReplicationSourceSize. src is not
// modified.
func sourceFingerprint(src *common.Metadata) *common.Metadata {
	out := *src
	out.Custom = make(map[string]string, len(src.Custom)+4)
	for k, v := range src.Custom {
		if !isFingerprintKey(k) {
			out.Custom[k] = v
		}
	}
	// Without a source ETag nothing is recorded, and any record copied
	// from upstream is dropped above, so the fallback comparison applies.
	if src.ETag != "" {
		out.Custom[MetaReplicationSourceETag] = src.ETag
		out.Custom[MetaReplicationSourceSize] = strconv.FormatInt(src.Size, 10)
		out.Custom[MetaReplicationSourceModified] = modifiedStamp(src)
		out.Custom[MetaReplicationSourceMeta] = metadataDigest(src)
	}
	return &out
}

// isFingerprintKey reports whether k is one of the keys sourceFingerprint
// records.
func isFingerprintKey(k string) bool {
	return common.IsReplicationFingerprintKey(k)
}

// modifiedStamp is src's modification time in Unix nanoseconds, or "" when
// the backend reports none.
func modifiedStamp(src *common.Metadata) string {
	if src.LastModified.IsZero() {
		return ""
	}
	return strconv.FormatInt(src.LastModified.UnixNano(), 10)
}

// metadataDigest is a digest of src's content type, content encoding and
// user metadata (fingerprint keys excluded), so a metadata-only update is
// seen as a change.
func metadataDigest(src *common.Metadata) string {
	keys := make([]string, 0, len(src.Custom))
	for k := range src.Custom {
		if !isFingerprintKey(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(src.ContentType)
	b.WriteByte(0)
	b.WriteString(src.ContentEncoding)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(src.Custom[k])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}
