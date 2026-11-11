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
	"time"
)

// ConditionalDeleter is implemented by a backend that can delete an object
// only if it is still the revision a caller observed, checked and deleted
// under the backend's own mutation lock. Lifecycle passes that decide from a
// listing and act later use it so they never delete a replacement written
// in between.
type ConditionalDeleter interface {
	// DeleteIfUnmodified deletes key if its LastModified still equals
	// lastModified. It returns ErrObjectChanged when the object was
	// replaced or updated since, and ErrKeyNotFound when it is gone.
	DeleteIfUnmodified(ctx context.Context, key string, lastModified time.Time) error
}
