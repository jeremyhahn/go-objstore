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

// Package common defines the core types, interfaces, and errors shared across
// go-objstore.
//
// It declares the Storage and Archiver interfaces, object Metadata and list
// options, the canonical error sentinels and the Classify taxonomy that maps
// errors to a transport-independent class, key validation, encrypted-storage
// wrappers, and lifecycle policy types.
package common
