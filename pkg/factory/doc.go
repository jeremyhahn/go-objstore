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

// Package factory constructs storage backends and archivers from configuration.
//
// Backends and archivers register themselves by name (often behind build tags
// so unused cloud SDKs are not linked) and are created on demand, keeping
// backend selection decoupled from the rest of the library.
package factory
