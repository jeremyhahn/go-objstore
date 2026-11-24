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

// Package serverconfig is the configuration layer shared by the objstore
// server binaries (objstore-server and the single-transport servers).
//
// Every setting — including every listen host, port, address and socket
// path — resolves through one path with the company precedence:
//
//	flags > environment > config file > defaults
//
// Defaults come from Defaults (derived from the transport packages' own
// defaults), the optional YAML file is named by the -config flag or
// OBJSTORE_CONFIG, environment variables are OBJSTORE_<KEY> with nesting
// joined by "_" (rest.host → OBJSTORE_REST_HOST), and a flag is applied only
// when it was set on the command line.
package serverconfig
