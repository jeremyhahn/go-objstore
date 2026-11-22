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

// Package storagefs adapts a common.Storage backend to an os-like file system.
//
// New wraps a backend in a *StorageFS, which implements this package's Fs
// interface (Create, Open, OpenFile, Mkdir, MkdirAll, Remove, RemoveAll,
// Rename, Stat, Chmod, Chown, Chtimes). Open files implement File: the io
// reader, writer, seeker and closer interfaces plus Stat, Sync, Truncate,
// Readdir and Readdirnames. It does not implement io/fs.FS.
package storagefs
