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

package rest

import (
	"errors"
	"strings"
)

var (
	// ErrStorageRequired is returned when storage backend is required but not provided.
	ErrStorageRequired = errors.New("storage backend is required")
)

// TrustedProxiesError is returned by NewServer when the configured trusted
// proxies cannot be applied to the router (for example an invalid CIDR).
type TrustedProxiesError struct {
	Proxies []string
	Err     error
}

func (e *TrustedProxiesError) Error() string {
	return "rest: cannot configure trusted proxies [" + strings.Join(e.Proxies, ", ") + "]: " + e.Err.Error()
}

func (e *TrustedProxiesError) Unwrap() error { return e.Err }

// HandlerInitError is returned by NewServer when the request handler cannot
// be created, for example because the objstore facade is not initialized.
type HandlerInitError struct {
	Err error
}

func (e *HandlerInitError) Error() string {
	return "rest: cannot create request handler: " + e.Err.Error()
}

func (e *HandlerInitError) Unwrap() error { return e.Err }
