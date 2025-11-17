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

package factory

import "errors"

var (
	// ErrArchiveOnlyBackend is returned when attempting to use an archive-only backend as a primary storage.
	ErrArchiveOnlyBackend = errors.New("archive-only backend")

	// ErrUnknownBackend is returned when an unknown backend type is specified.
	ErrUnknownBackend = errors.New("unknown backend type")

	// ErrUnknownArchiver is returned when an unknown archiver type is specified.
	ErrUnknownArchiver = errors.New("unknown archiver type")

	// ErrTypeAssertionFailed is returned when a type assertion fails.
	ErrTypeAssertionFailed = errors.New("type assertion failed")
)
