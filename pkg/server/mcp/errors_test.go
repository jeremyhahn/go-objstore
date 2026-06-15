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

package mcp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestErrPolicyAlreadyExistsWrapsSentinel pins that the policy-conflict
// sentinel wraps common.ErrAlreadyExists so common.Classify maps it to
// CodeAlreadyExists even when further wrapped.
func TestErrPolicyAlreadyExistsWrapsSentinel(t *testing.T) {
	if !errors.Is(ErrPolicyAlreadyExists, common.ErrAlreadyExists) {
		t.Error("ErrPolicyAlreadyExists does not wrap common.ErrAlreadyExists")
	}

	if got := common.Classify(ErrPolicyAlreadyExists); got != common.CodeAlreadyExists {
		t.Errorf("Classify(ErrPolicyAlreadyExists) = %v, want %v", got, common.CodeAlreadyExists)
	}

	wrapped := fmt.Errorf("ctx: %w", ErrPolicyAlreadyExists)
	if got := common.Classify(wrapped); got != common.CodeAlreadyExists {
		t.Errorf("Classify(%v) = %v, want %v", wrapped, got, common.CodeAlreadyExists)
	}
}
