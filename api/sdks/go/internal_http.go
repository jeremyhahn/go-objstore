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

package objstore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// httpStatusError converts a non-success HTTP status code into an error that
// wraps the canonical SDK sentinel for that status: 400 ErrInvalidArgument,
// 401 ErrUnauthenticated, 403 ErrPermissionDenied, 404 ErrObjectNotFound,
// 409 ErrAlreadyExists, 429 ErrRateLimited. Any other status (notably 5xx)
// yields a plain server error with no sentinel. Shared by the REST, QUIC,
// and MCP clients.
func httpStatusError(op string, statusCode int) error {
	var sentinel error
	switch statusCode {
	case http.StatusBadRequest:
		sentinel = ErrInvalidArgument
	case http.StatusUnauthorized:
		sentinel = ErrUnauthenticated
	case http.StatusForbidden:
		sentinel = ErrPermissionDenied
	case http.StatusNotFound:
		sentinel = ErrObjectNotFound
	case http.StatusConflict:
		sentinel = ErrAlreadyExists
	case http.StatusTooManyRequests:
		sentinel = ErrRateLimited
	default:
		return fmt.Errorf("%s failed with status %d", op, statusCode)
	}
	return fmt.Errorf("%s failed with status %d: %w", op, statusCode, sentinel)
}

// applyAuthHeaders applies the configured auth headers (bearer token, custom
// headers, tenant ID) to an outgoing HTTP request. Shared by the REST, QUIC,
// and MCP clients.
func applyAuthHeaders(req *http.Request, config *ClientConfig) {
	if config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+config.Token)
	}
	for k, v := range config.Headers {
		req.Header.Set(k, v)
	}
	if config.TenantID != "" {
		req.Header.Set("X-Tenant-ID", config.TenantID)
	}
}

// httpGetStream issues a streaming GET against the standard /objects/<key>
// endpoint and returns the response body without buffering it. Shared by the
// REST and QUIC clients; parseMeta extracts transport-specific metadata from
// the response headers.
func httpGetStream(ctx context.Context, client *http.Client, baseURL, key string, config *ClientConfig, parseMeta func(http.Header) *Metadata) (io.ReadCloser, *Metadata, error) {
	if err := validateKey(key); err != nil {
		return nil, nil, err
	}
	reqURL := fmt.Sprintf("%s/objects/%s", baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, nil, err
	}
	applyAuthHeaders(req, config)

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, nil, httpStatusError("GET (stream)", resp.StatusCode)
	}

	return resp.Body, parseMeta(resp.Header), nil
}

// httpPutStream issues a streaming PUT against the standard /objects/<key>
// endpoint. size is the Content-Length hint; pass -1 when unknown. Shared by
// the REST and QUIC clients; setCustom applies the transport-specific custom
// metadata headers.
func httpPutStream(ctx context.Context, client *http.Client, baseURL, key string, r io.Reader, size int64, metadata *Metadata, config *ClientConfig, setCustom func(*http.Request, map[string]string)) (*PutResult, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	reqURL := fmt.Sprintf("%s/objects/%s", baseURL, url.PathEscape(key))

	req, err := http.NewRequestWithContext(ctx, "PUT", reqURL, r)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size

	if metadata != nil {
		if metadata.ContentType != "" {
			req.Header.Set("Content-Type", metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			req.Header.Set("Content-Encoding", metadata.ContentEncoding)
		}
		if len(metadata.Custom) > 0 {
			setCustom(req, metadata.Custom)
		}
	}
	applyAuthHeaders(req, config)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, httpStatusError("PUT (stream)", resp.StatusCode)
	}

	return &PutResult{
		Success: true,
		ETag:    resp.Header.Get("ETag"),
	}, nil
}
