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

// Package errors maps the canonical error taxonomy (common.Classify) to each
// transport's wire representation: HTTP status codes, gRPC status codes, and
// JSON-RPC error codes. Keeping the full matrix in one place guarantees that
// every transport reports the same class of failure for the same error.
package errors

import (
	"net/http"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTPStatus returns the HTTP status code and a sanitized client-safe message
// for an error. Used by the REST and QUIC transports.
func HTTPStatus(err error) (int, string) {
	if err == nil {
		return http.StatusOK, ""
	}
	switch common.Classify(err) {
	case common.CodeNotFound:
		return http.StatusNotFound, "object not found"
	case common.CodeAlreadyExists:
		return http.StatusConflict, "already exists"
	case common.CodeInvalidArgument:
		return http.StatusBadRequest, common.SanitizeErrorMessage(err)
	case common.CodePermissionDenied:
		return http.StatusForbidden, "forbidden"
	case common.CodeUnauthenticated:
		return http.StatusUnauthorized, "unauthorized"
	case common.CodeResourceExhausted:
		return http.StatusTooManyRequests, "rate limit exceeded"
	case common.CodeUnavailable:
		return http.StatusServiceUnavailable, "service unavailable"
	case common.CodeCanceled:
		// 499 Client Closed Request (nginx convention).
		return 499, "request canceled"
	case common.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout, "deadline exceeded"
	default:
		return http.StatusInternalServerError, common.SanitizeErrorMessage(err)
	}
}

// GRPCStatus returns a gRPC status error for an error. Used by the gRPC
// transport.
func GRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	switch common.Classify(err) {
	case common.CodeNotFound:
		return status.Error(codes.NotFound, "object not found")
	case common.CodeAlreadyExists:
		return status.Error(codes.AlreadyExists, "already exists")
	case common.CodeInvalidArgument:
		return status.Error(codes.InvalidArgument, common.SanitizeErrorMessage(err))
	case common.CodePermissionDenied:
		return status.Error(codes.PermissionDenied, "permission denied")
	case common.CodeUnauthenticated:
		return status.Error(codes.Unauthenticated, "unauthenticated")
	case common.CodeResourceExhausted:
		return status.Error(codes.ResourceExhausted, "rate limit exceeded")
	case common.CodeUnavailable:
		return status.Error(codes.Unavailable, "service unavailable")
	case common.CodeCanceled:
		return status.Error(codes.Canceled, "request canceled")
	case common.CodeDeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		return status.Error(codes.Internal, common.SanitizeErrorMessage(err))
	}
}

// JSONRPCError returns the JSON-RPC error code and a sanitized client-safe
// message for an error. Used by the unix-socket and MCP transports.
func JSONRPCError(err error) (int, string) {
	if err == nil {
		return jsonrpc.CodeInternal, ""
	}
	switch common.Classify(err) {
	case common.CodeNotFound:
		return jsonrpc.CodeNotFound, "object not found"
	case common.CodeAlreadyExists:
		return jsonrpc.CodeAlreadyExists, "already exists"
	case common.CodeInvalidArgument:
		return jsonrpc.CodeInvalidParams, common.SanitizeErrorMessage(err)
	case common.CodePermissionDenied:
		return jsonrpc.CodeForbidden, "forbidden"
	case common.CodeUnauthenticated:
		return jsonrpc.CodeUnauthenticated, "unauthenticated"
	case common.CodeResourceExhausted:
		return jsonrpc.CodeRateLimited, "rate limit exceeded"
	case common.CodeUnavailable:
		return jsonrpc.CodeUnavailable, "service unavailable"
	case common.CodeCanceled:
		return jsonrpc.CodeInternal, "request canceled"
	case common.CodeDeadlineExceeded:
		return jsonrpc.CodeInternal, "deadline exceeded"
	default:
		return jsonrpc.CodeInternal, common.SanitizeErrorMessage(err)
	}
}
