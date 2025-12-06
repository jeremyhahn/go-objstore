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

package local

import "time"

// ChangeEvent represents a single change in the change log.
type ChangeEvent struct {
	Key       string          `json:"key"`
	Operation string          `json:"operation"` // "put", "delete"
	Timestamp time.Time       `json:"timestamp"`
	ETag      string          `json:"etag,omitempty"`
	Size      int64           `json:"size,omitempty"`
	Processed map[string]bool `json:"processed"` // policyID -> processed
}

// ChangeLog is the interface for tracking object changes.
// This allows the replication package to inject a change logger.
type ChangeLog interface {
	// RecordChange records a new change event.
	RecordChange(event ChangeEvent) error
}
