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

// Package azurearchive provides the azurearchive object-storage backend.
//
// The backend implementation is gated behind the "azurearchive" build tag so that
// builds which do not need it avoid linking its cloud SDK. Without the tag this
// package compiles to an empty stub and the backend is unregistered. Enable it
// with: go build -tags azurearchive   (Makefile: WITH_AZURE_ARCHIVE=1, which is the default).
package azurearchive
