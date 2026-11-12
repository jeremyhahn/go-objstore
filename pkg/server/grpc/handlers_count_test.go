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

package grpc

import (
	"errors"
	"math"
	"testing"
)

func TestPoliciesCountToInt32(t *testing.T) {
	tests := []struct {
		name    string
		in      int
		want    int32
		wantErr bool
	}{
		{name: "zero", in: 0, want: 0},
		{name: "in range", in: 7, want: 7},
		{name: "max int32", in: math.MaxInt32, want: math.MaxInt32},
		{name: "negative", in: -1, wantErr: true},
		{name: "over range", in: math.MaxInt32 + 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := policiesCountToInt32(tt.in)
			if tt.wantErr {
				if !errors.Is(err, ErrPoliciesCountExceedsRange) {
					t.Fatalf("policiesCountToInt32(%d) error = %v, want ErrPoliciesCountExceedsRange", tt.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("policiesCountToInt32(%d) unexpected error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("policiesCountToInt32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestClampToInt32(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int32
	}{
		{name: "negative", in: -5, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "in range", in: 42, want: 42},
		{name: "max int32", in: math.MaxInt32, want: math.MaxInt32},
		{name: "over range", in: math.MaxInt32 + 1, want: math.MaxInt32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clampToInt32(tt.in); got != tt.want {
				t.Errorf("clampToInt32(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
