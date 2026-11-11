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

import (
	"fmt"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ErrClientLocalArchiver is returned when an archiver whose settings name a
// filesystem path (the local archiver) is built from settings a client sent,
// and the server does not allow client-supplied local destinations. It wraps
// common.ErrInvalidArgument, so every transport answers invalid argument.
var ErrClientLocalArchiver = fmt.Errorf("factory: client-supplied local archive destinations are not allowed: %w", common.ErrInvalidArgument)

// pathArchiverTypes lists the registered archiver types whose settings name a
// filesystem path on the server host. A client that picks one of them chooses
// a directory the service user writes into.
var pathArchiverTypes = map[string]struct{}{
	"local": {},
}

// WireOptions controls how NewArchiverFromWireOpts treats settings a client
// sent. The zero value is the safe default.
type WireOptions struct {
	// AllowLocalDestinations lets a client name a filesystem-path archiver
	// type (local). Off by default: such a client could write a stored
	// object into any directory the service user can write.
	AllowLocalDestinations bool
}

// IsPathArchiverType reports whether an archiver type's settings name a
// filesystem path on the server host.
func IsPathArchiverType(backendType string) bool {
	_, ok := pathArchiverTypes[backendType]
	return ok
}

// CheckClientArchiverType returns ErrClientLocalArchiver when backendType is a
// filesystem-path archiver type and opts does not allow it.
func CheckClientArchiverType(backendType string, opts WireOptions) error {
	if !opts.AllowLocalDestinations && IsPathArchiverType(backendType) {
		return fmt.Errorf("archiver type %q: %w", backendType, ErrClientLocalArchiver)
	}
	return nil
}

// NewArchiverFromWireOpts is NewArchiverFromWire with options. A
// filesystem-path archiver type is refused with ErrClientLocalArchiver unless
// opts.AllowLocalDestinations is set; the check runs before any setting is
// read, so a refused request leaves no credential handle behind.
func NewArchiverFromWireOpts(backendType string, wire map[string]string, opts WireOptions) (common.Archiver, error) {
	if err := CheckClientArchiverType(backendType, opts); err != nil {
		return nil, err
	}
	return newArchiverFromWire(backendType, wire)
}
