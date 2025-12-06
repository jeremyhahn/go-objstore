// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrInvalidProtocol", ErrInvalidProtocol, "invalid protocol"},
		{"ErrConnectionFailed", ErrConnectionFailed, "connection failed"},
		{"ErrObjectNotFound", ErrObjectNotFound, "object not found"},
		{"ErrInvalidConfig", ErrInvalidConfig, "invalid configuration"},
		{"ErrStreamingNotSupported", ErrStreamingNotSupported, "streaming not supported"},
		{"ErrPolicyNotFound", ErrPolicyNotFound, "policy not found"},
		{"ErrOperationFailed", ErrOperationFailed, "operation failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.msg, tt.err.Error())
			assert.True(t, errors.Is(tt.err, tt.err))
		})
	}
}
