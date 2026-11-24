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

// PathAccessor is an optional interface of local-filesystem Storage for consumers that need a real OS path; cloud backends do not implement it.
type PathAccessor interface {
	// LocalPath returns the absolute local-filesystem path that backs this
	// Storage. The returned path must already exist and be writable by the
	// daemon process.
	LocalPath() string
}
