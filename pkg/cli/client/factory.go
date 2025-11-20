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

package client

import (
	"errors"
	"fmt"
)

var (
	// ErrConfigRequired is returned when client config is nil
	ErrConfigRequired = errors.New("client config is required")
	// ErrUnsupportedProtocol is returned when an unsupported protocol is specified
	ErrUnsupportedProtocol = errors.New("unsupported protocol")
	// ErrServerURLRequired is returned when server URL is missing
	ErrServerURLRequired = errors.New("server URL is required")
	// ErrMaxResultsOverflow is returned when MaxResults exceeds int32 range
	ErrMaxResultsOverflow = errors.New("MaxResults exceeds int32 range")
	// ErrServerNotServing is returned when health check fails
	ErrServerNotServing = errors.New("server not serving")
	// ErrNoSyncResult is returned when sync result is nil
	ErrNoSyncResult = errors.New("no sync result returned")
	// ErrNoStatus is returned when status is nil
	ErrNoStatus = errors.New("no status returned")
	// ErrServerError is returned when server returns non-success status
	ErrServerError = errors.New("server returned error")
)

// NewClient creates a new client based on the protocol specified in the config
func NewClient(config *Config) (Client, error) {
	if config == nil {
		return nil, ErrConfigRequired
	}

	// Default to REST if no protocol specified
	protocol := config.Protocol
	if protocol == "" {
		protocol = "rest"
	}

	switch protocol {
	case "rest", "http", "https":
		return NewRESTClient(config)
	case "grpc":
		return NewGRPCClient(config)
	case "quic", "http3":
		return NewQUICClient(config)
	default:
		return nil, fmt.Errorf("%w: %s (supported: rest, grpc, quic)", ErrUnsupportedProtocol, config.Protocol)
	}
}
