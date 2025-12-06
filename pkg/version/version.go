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

package version

// Version is the application version.
// This should be set at build time using:
//
//	go build -ldflags "-X github.com/jeremyhahn/go-objstore/pkg/version.Version=1.0.0"
var Version = "0.1.0-alpha" // default version if not set at build time

// Get returns the application version string.
// The version can be overridden at build time using ldflags.
func Get() string {
	return Version
}
