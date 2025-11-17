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
	"fmt"
)

// NewClient creates a new client based on the protocol specified in the config
func NewClient(config *Config) (Client, error) {
	if config == nil {
		return nil, fmt.Errorf("client config is required")
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
		return nil, fmt.Errorf("unsupported protocol: %s (supported: rest, grpc, quic)", config.Protocol)
	}
}
