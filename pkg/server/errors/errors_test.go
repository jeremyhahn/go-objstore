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

package errors

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTaxonomyMatrix asserts the full sentinel × transport mapping in one
// table — the executable spec for cross-transport error consistency.
func TestTaxonomyMatrix(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantHTTP    int
		wantGRPC    codes.Code
		wantJSONRPC int
	}{
		{"key not found", common.ErrKeyNotFound, http.StatusNotFound, codes.NotFound, jsonrpc.CodeNotFound},
		{"wrapped key not found", fmt.Errorf("get %q: %w", "k", common.ErrKeyNotFound), http.StatusNotFound, codes.NotFound, jsonrpc.CodeNotFound},
		{"metadata not found", common.ErrMetadataNotFound, http.StatusNotFound, codes.NotFound, jsonrpc.CodeNotFound},
		{"policy not found", common.ErrPolicyNotFound, http.StatusNotFound, codes.NotFound, jsonrpc.CodeNotFound},
		{"already exists", common.ErrAlreadyExists, http.StatusConflict, codes.AlreadyExists, jsonrpc.CodeAlreadyExists},
		{"invalid argument", common.ErrInvalidArgument, http.StatusBadRequest, codes.InvalidArgument, jsonrpc.CodeInvalidParams},
		{"permission denied", common.ErrPermissionDenied, http.StatusForbidden, codes.PermissionDenied, jsonrpc.CodeForbidden},
		{"unauthenticated", common.ErrUnauthenticated, http.StatusUnauthorized, codes.Unauthenticated, jsonrpc.CodeUnauthenticated},
		{"resource exhausted", common.ErrResourceExhausted, http.StatusTooManyRequests, codes.ResourceExhausted, jsonrpc.CodeRateLimited},
		{"unavailable", common.ErrUnavailable, http.StatusServiceUnavailable, codes.Unavailable, jsonrpc.CodeUnavailable},
		{"canceled", context.Canceled, 499, codes.Canceled, jsonrpc.CodeInternal},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout, codes.DeadlineExceeded, jsonrpc.CodeInternal},
		{"unclassified", fmt.Errorf("disk on fire"), http.StatusInternalServerError, codes.Internal, jsonrpc.CodeInternal},
		{"raw fs not exist", fmt.Errorf("open: %w", fs.ErrNotExist), http.StatusNotFound, codes.NotFound, jsonrpc.CodeNotFound},
		// Bare strings no longer classify: producers must wrap sentinels.
		{"string only not found", fmt.Errorf("not found"), http.StatusInternalServerError, codes.Internal, jsonrpc.CodeInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHTTP, msg := HTTPStatus(tt.err)
			if gotHTTP != tt.wantHTTP {
				t.Errorf("HTTPStatus = %d, want %d", gotHTTP, tt.wantHTTP)
			}
			if msg == "" {
				t.Error("HTTPStatus message must not be empty")
			}

			grpcErr := GRPCStatus(tt.err)
			if got := status.Code(grpcErr); got != tt.wantGRPC {
				t.Errorf("GRPCStatus code = %v, want %v", got, tt.wantGRPC)
			}

			gotRPC, rpcMsg := JSONRPCError(tt.err)
			if gotRPC != tt.wantJSONRPC {
				t.Errorf("JSONRPCError code = %d, want %d", gotRPC, tt.wantJSONRPC)
			}
			if rpcMsg == "" {
				t.Error("JSONRPCError message must not be empty")
			}
		})
	}
}

// TestNilError verifies nil-error behavior of all three mappers.
func TestNilError(t *testing.T) {
	if code, _ := HTTPStatus(nil); code != http.StatusOK {
		t.Errorf("HTTPStatus(nil) = %d, want 200", code)
	}
	if err := GRPCStatus(nil); err != nil {
		t.Errorf("GRPCStatus(nil) = %v, want nil", err)
	}
}
