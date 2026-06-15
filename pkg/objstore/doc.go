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

// Package objstore provides the high-level facade for go-objstore.
//
// The facade routes object operations to one or more configured storage
// backends by key, presenting a single Storage-compatible API over them, and
// wires optional lifecycle and replication management on top.
package objstore
