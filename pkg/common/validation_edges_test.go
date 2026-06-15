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

// Package common — additional edge-case tests to lift ValidateKey and
// SanitizeErrorMessage to full coverage.
package common_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestValidateKey_DotDotBackslashStart covers the branch at line 166:
//
//	key[2] == '\\'
//
// A key starting with "..\\" must be rejected as a path traversal sequence.
func TestValidateKey_DotDotBackslashStart(t *testing.T) {
	err := common.ValidateKey(`..\..\file.txt`)
	if err == nil {
		t.Fatal("ValidateKey() expected error for ..\\-prefixed key, got nil")
	}
	if !strings.Contains(err.Error(), "path traversal") {
		t.Errorf("ValidateKey() error = %v, want 'path traversal'", err)
	}
}

// TestSanitizeErrorMessage_ConnectionReset verifies the "connection reset"
// sanitization pattern (no path separator in the error string).
func TestSanitizeErrorMessage_ConnectionReset(t *testing.T) {
	err := errors.New("connection reset by peer")
	got := common.SanitizeErrorMessage(err)
	if got != "service unavailable" {
		t.Errorf("SanitizeErrorMessage() = %q, want %q", got, "service unavailable")
	}
}

// TestSanitizeErrorMessage_InvalidArgument verifies the "invalid argument"
// sanitization pattern.
func TestSanitizeErrorMessage_InvalidArgument(t *testing.T) {
	err := errors.New("invalid argument: bad value")
	got := common.SanitizeErrorMessage(err)
	if got != "invalid request" {
		t.Errorf("SanitizeErrorMessage() = %q, want %q", got, "invalid request")
	}
}

// TestSanitizeErrorMessage_AlreadyExistsWithPath verifies that an "already
// exists" error containing a path separator is caught by the inner
// path-aware branch (line 328-331 in validation.go) and not just the
// sanitizedPatterns map lookup below it.
func TestSanitizeErrorMessage_AlreadyExistsWithPath(t *testing.T) {
	err := errors.New("file /tmp/bucket/obj already exists")
	got := common.SanitizeErrorMessage(err)
	if got != "object already exists" {
		t.Errorf("SanitizeErrorMessage() = %q, want %q", got, "object already exists")
	}
}

// TestValidateKey_BareDotDot verifies that a key consisting only of ".."
// is rejected as a path traversal sequence (exercises the post-loop check
// at line 165 where keyLen == 2).
func TestValidateKey_BareDotDot(t *testing.T) {
	err := common.ValidateKey("..")
	if err == nil {
		t.Fatal("ValidateKey(\"..\") expected error, got nil")
	}
	if !strings.Contains(err.Error(), "path traversal") {
		t.Errorf("ValidateKey(\"..\") error = %v, want 'path traversal'", err)
	}
}
