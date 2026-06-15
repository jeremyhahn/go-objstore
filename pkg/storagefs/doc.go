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

// Package storagefs adapts a Storage backend to the standard io/fs interfaces.
//
// It presents stored objects as a navigable file system, supporting Open,
// Create, ReadDir, Seek, and Stat so backends can be used wherever an fs.FS
// (or a read-write file tree) is expected.
package storagefs
