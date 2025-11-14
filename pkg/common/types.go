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
	"time"
)

// Metadata represents metadata associated with an object in storage.
type Metadata struct {
	// ContentType is the MIME type of the object (e.g., "application/json")
	ContentType string `json:"content_type,omitempty"`

	// ContentEncoding is the encoding applied to the object (e.g., "gzip")
	ContentEncoding string `json:"content_encoding,omitempty"`

	// Size is the size of the object in bytes
	Size int64 `json:"size"`

	// LastModified is the timestamp when the object was last modified
	LastModified time.Time `json:"last_modified"`

	// ETag is the entity tag for the object (used for versioning/caching)
	ETag string `json:"etag,omitempty"`

	// Custom is a map of custom metadata key-value pairs
	Custom map[string]string `json:"custom,omitempty"`
}

// ObjectInfo represents complete information about a stored object.
type ObjectInfo struct {
	// Key is the object's storage key/path
	Key string `json:"key"`

	// Metadata contains the object's metadata
	Metadata *Metadata `json:"metadata,omitempty"`
}

// ListOptions specifies options for listing objects.
type ListOptions struct {
	// Prefix filters objects to those starting with this prefix
	Prefix string

	// Delimiter is used for hierarchical listing (e.g., "/" for directories)
	// When set, common prefixes are returned separately
	Delimiter string

	// MaxResults specifies the maximum number of results per page
	// 0 means use backend default
	MaxResults int

	// ContinueFrom is a pagination token from a previous ListResult
	// Empty string means start from the beginning
	ContinueFrom string
}

// ListResult contains the results of a list operation.
type ListResult struct {
	// Objects contains the list of objects matching the criteria
	Objects []*ObjectInfo

	// CommonPrefixes contains common prefixes when using Delimiter
	// For example, with delimiter "/" and prefix "a/", this might contain
	// ["a/b/", "a/c/"] representing subdirectories
	CommonPrefixes []string

	// NextToken is the pagination token for the next page of results
	// Empty string means no more results available
	NextToken string

	// Truncated indicates whether more results are available
	Truncated bool
}
