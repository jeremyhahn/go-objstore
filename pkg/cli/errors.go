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

package cli

import "errors"

var (
	// Configuration errors

	// ErrBackendPathRequired is returned when backend-path is required but not set.
	ErrBackendPathRequired = errors.New("backend-path is required for local backend")

	// ErrBackendBucketRequired is returned when backend-bucket is required but not set.
	ErrBackendBucketRequired = errors.New("backend-bucket is required")

	// ErrBackendRegionRequired is returned when backend-region is required but not set.
	ErrBackendRegionRequired = errors.New("backend-region is required")

	// ErrBackendURLRequired is returned when backend-url is required but not set.
	ErrBackendURLRequired = errors.New("backend-url is required")

	// ErrUnsupportedBackend is returned when an unsupported backend is specified.
	ErrUnsupportedBackend = errors.New("unsupported backend")

	// ErrUnsupportedOutputFormat is returned when an unsupported output format is specified.
	ErrUnsupportedOutputFormat = errors.New("unsupported output format")
)
