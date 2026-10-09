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
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	errExec := errors.New("execute failed")
	errDestroy := errors.New("destroy failed")

	tests := []struct {
		name        string
		execErr     error
		destroyErr  error
		wantCode    int
		wantStderr  []string
		wantNoError bool
	}{
		{name: "success", wantCode: 0, wantNoError: true},
		{name: "execute fails", execErr: errExec, wantCode: 1, wantStderr: []string{"execute failed"}},
		{
			name: "destroy fails after success", destroyErr: errDestroy, wantCode: 1,
			wantStderr: []string{"failed to destroy backend credentials", "destroy failed"},
		},
		{
			name: "both fail", execErr: errExec, destroyErr: errDestroy, wantCode: 1,
			wantStderr: []string{"execute failed", "failed to destroy backend credentials", "destroy failed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destroyed := false
			var stderr bytes.Buffer

			code := run(
				func() error { return tt.execErr },
				func() error { destroyed = true; return tt.destroyErr },
				&stderr,
			)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !destroyed {
				t.Error("credentials were not destroyed")
			}
			if tt.wantNoError && stderr.Len() != 0 {
				t.Errorf("unexpected stderr output: %q", stderr.String())
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr %q does not contain %q", stderr.String(), want)
				}
			}
		})
	}
}
