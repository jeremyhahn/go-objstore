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

package main

import (
	"errors"
	"strings"
	"testing"
)

// The //export functions themselves cannot be called from here: `go test`
// rejects cgo in test files outright ("use of cgo in test ... not supported"),
// so nothing in a _test.go file can name C.int or *C.char. That is why the
// length guard is a plain-Go function taking plain-Go types — it is the whole
// of the decision, and it is reachable. The remaining untested surface is the
// one-line `if !checkLength(...)` in each wrapper, which is covered end-to-end
// from C by test_length_validation in examples/c_client/test_objstore.c.

func TestCheckLengthAcceptsNonNegative(t *testing.T) {
	for _, n := range []int{0, 1, 4096} {
		setLastError(errors.New("sentinel: must be cleared or left alone"))
		if !checkLength("dataLen", n) {
			t.Errorf("checkLength(%d) = false, want true", n)
		}
		// A valid length must not touch the error slot. Asserting the sentinel
		// survives catches a guard that reports success while still recording a
		// failure, which would strand a stale message for the next
		// ObjstoreGetLastError caller.
		if lastError == "" {
			t.Errorf("checkLength(%d) cleared lastError; the guard must not touch it on success", n)
		}
	}
}

func TestCheckLengthRejectsNegative(t *testing.T) {
	setLastError(nil)

	if checkLength("dataLen", -1) {
		t.Fatal("checkLength(-1) = true, want false")
	}

	// Assert the recorded state, not merely the return value: the return code
	// tells the C caller to look at ObjstoreGetLastError, so a guard that
	// returns false without recording anything sends it to an empty string.
	if lastError == "" {
		t.Fatal("checkLength(-1) returned false but recorded no error")
	}
	if !strings.Contains(lastError, "dataLen") {
		t.Errorf("lastError = %q, want it to name the offending parameter", lastError)
	}
	if !strings.Contains(lastError, "-1") {
		t.Errorf("lastError = %q, want it to report the offending value", lastError)
	}
}

func TestInvalidLengthErrorIsMatchable(t *testing.T) {
	err := error(&InvalidLengthError{Param: "bufferSize", Value: -7})

	// errors.Is, not string matching: the sentinel is what a caller branches
	// on, and the Is method is what makes that work for a typed error.
	if !errors.Is(err, ErrInvalidLength) {
		t.Error("errors.Is(err, ErrInvalidLength) = false; the type must match the sentinel")
	}
	if errors.Is(err, ErrBufferTooSmall) {
		t.Error("an invalid-length error must not match ErrBufferTooSmall; they send callers to different bugs")
	}

	// The fields stay reachable — the point of a typed error over fmt.Errorf is
	// that the parameter and value survive as data, not only as prose.
	var ile *InvalidLengthError
	if !errors.As(err, &ile) {
		t.Fatal("errors.As did not reach *InvalidLengthError")
	}
	if ile.Param != "bufferSize" || ile.Value != -7 {
		t.Errorf("recovered %+v, want Param=bufferSize Value=-7", ile)
	}

	for _, want := range []string{"bufferSize", "-7"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %q, want it to contain %q", err, want)
		}
	}
}

func TestSetLastErrorRoundTrip(t *testing.T) {
	setLastError(&InvalidLengthError{Param: "settingsCount", Value: -2})
	if lastError == "" {
		t.Fatal("setLastError stored nothing")
	}

	setLastError(nil)
	if lastError != "" {
		t.Errorf("setLastError(nil) = %q, want it to clear the slot", lastError)
	}
}
