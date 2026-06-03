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

package common

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

// TestClassifyValidationError pins that backend-level validation failures
// classify as invalid arguments rather than internal errors.
func TestClassifyValidationError(t *testing.T) {
	err := ValidateKey("../bad")
	if err == nil {
		t.Fatal("ValidateKey(\"../bad\") = nil, want error")
	}
	if got := Classify(err); got != CodeInvalidArgument {
		t.Errorf("Classify(%v) = %v, want %v", err, got, CodeInvalidArgument)
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("errors.Is(%v, ErrInvalidArgument) = false, want true", err)
	}
}

// TestClassify pins the canonical error classification used by every
// transport mapper.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorCode
	}{
		{"nil", nil, CodeInternal},
		{"key not found", ErrKeyNotFound, CodeNotFound},
		{"wrapped key not found", fmt.Errorf("get %q: %w", "k", ErrKeyNotFound), CodeNotFound},
		{"metadata not found", ErrMetadataNotFound, CodeNotFound},
		{"policy not found", ErrPolicyNotFound, CodeNotFound},
		{"already exists", ErrAlreadyExists, CodeAlreadyExists},
		{"invalid argument", ErrInvalidArgument, CodeInvalidArgument},
		{"permission denied", ErrPermissionDenied, CodePermissionDenied},
		{"unauthenticated", ErrUnauthenticated, CodeUnauthenticated},
		{"resource exhausted", ErrResourceExhausted, CodeResourceExhausted},
		{"unavailable", ErrUnavailable, CodeUnavailable},
		{"canceled", context.Canceled, CodeCanceled},
		{"deadline", context.DeadlineExceeded, CodeDeadlineExceeded},
		{"wrapped deadline", fmt.Errorf("op: %w", context.DeadlineExceeded), CodeDeadlineExceeded},

		// Wrapped-sentinel classification. Producers wrap the canonical
		// sentinels (no string matching), so errors.Is must see through
		// arbitrary wrapping.
		{"raw fs not exist", fs.ErrNotExist, CodeNotFound},
		{"wrapped fs not exist", fmt.Errorf("open: %w", fs.ErrNotExist), CodeNotFound},
		{"raw fs permission", fs.ErrPermission, CodePermissionDenied},
		{"wrapped fs permission", fmt.Errorf("open: %w", fs.ErrPermission), CodePermissionDenied},
		{"wrapped already exists", fmt.Errorf("add policy: %w", ErrAlreadyExists), CodeAlreadyExists},
		{
			"doubly wrapped policy already exists",
			fmt.Errorf("ctx: %w", fmt.Errorf("policy already exists: %w", ErrAlreadyExists)),
			CodeAlreadyExists,
		},
		{"wrapped invalid argument", fmt.Errorf("put: %w", ErrInvalidArgument), CodeInvalidArgument},
		{"wrapped permission denied", fmt.Errorf("get: %w", ErrPermissionDenied), CodePermissionDenied},
		{"wrapped validation error", fmt.Errorf("put: %w", ValidateKey("../bad")), CodeInvalidArgument},

		// Bare strings no longer classify: producers must wrap sentinels.
		{"string only not found", errors.New("not found"), CodeInternal},
		{"string only already exists", errors.New("policy already exists"), CodeInternal},
		{"unclassified", errors.New("disk on fire"), CodeInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Errorf("Classify(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
