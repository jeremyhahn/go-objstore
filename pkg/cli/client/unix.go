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
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

var (
	// ErrUnixSocketRequired is returned when Unix socket path is missing
	ErrUnixSocketRequired = errors.New("unix socket path is required")
)

// NewUnixSocketClient creates a new REST client that connects via Unix socket
func NewUnixSocketClient(config *Config) (*RESTClient, error) {
	if config.UnixSocket == "" {
		return nil, ErrUnixSocketRequired
	}

	// Create HTTP client with custom transport for Unix socket
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", config.UnixSocket)
		},
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		MaxIdleConnsPerHost: 10,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// Use localhost as the base URL - the actual connection goes to the socket
	baseURL := "http://localhost"
	if config.ServerURL != "" {
		baseURL = config.ServerURL
	}

	return &RESTClient{
		baseURL:    baseURL,
		httpClient: httpClient,
	}, nil
}
