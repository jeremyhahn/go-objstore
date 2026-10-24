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

package multipart

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestParseExpiry(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		want     time.Duration
		wantErr  bool
	}{
		{"nil settings", nil, DefaultExpiry, false},
		{"absent", map[string]string{"x": "y"}, DefaultExpiry, false},
		{"empty", map[string]string{ExpirySetting: ""}, DefaultExpiry, false},
		{"valid", map[string]string{ExpirySetting: "36h"}, 36 * time.Hour, false},
		{"not a duration", map[string]string{ExpirySetting: "soon"}, 0, true},
		{"zero", map[string]string{ExpirySetting: "0s"}, 0, true},
		{"negative", map[string]string{ExpirySetting: "-1h"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseExpiry(tt.settings)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, common.ErrInvalidArgument) {
				t.Errorf("error %v does not match ErrInvalidArgument", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
	if DefaultExpiry != 168*time.Hour {
		t.Errorf("DefaultExpiry = %v", DefaultExpiry)
	}
}

func TestExpired(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		initiated time.Time
		want      bool
	}{
		{"fresh", now.Add(-time.Hour), false},
		{"exactly at expiry", now.Add(-24 * time.Hour), false},
		{"past expiry", now.Add(-24*time.Hour - time.Second), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Expired(tt.initiated, 24*time.Hour, now); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUploadID(t *testing.T) {
	a, err := NewUploadID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewUploadID()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("two IDs are equal")
	}
	if !ValidUploadID(a) {
		t.Errorf("generated ID %q is not valid", a)
	}
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{"empty", "", false},
		{"short", "abcd", false},
		{"long", strings.Repeat("a", 33), false},
		{"uppercase", strings.Repeat("A", 32), false},
		{"non hex", strings.Repeat("g", 32), false},
		{"traversal", "../../../../etc/passwd/aaaaaaaaaa", false},
		{"separator", strings.Repeat("a", 15) + "/" + strings.Repeat("a", 16), false},
		{"valid", strings.Repeat("0f", 16), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidUploadID(tt.id); got != tt.want {
				t.Errorf("ValidUploadID(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestValidateInitiate(t *testing.T) {
	tests := []struct {
		name string
		key  string
		meta *common.Metadata
		want bool
	}{
		{"valid, nil metadata", "a/b", nil, false},
		{"valid metadata", "a", &common.Metadata{Custom: map[string]string{"k": "v"}}, false},
		{"empty key", "", nil, true},
		{"traversal key", "../x", nil, true},
		{"reserved key", common.MultipartPrefix + "x", nil, true},
		{"bad metadata", "a", &common.Metadata{Custom: map[string]string{"k": "v\n"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateInitiate(tt.key, tt.meta)
			if (err != nil) != tt.want {
				t.Fatalf("err = %v, want error %v", err, tt.want)
			}
			if err != nil && !errors.Is(err, common.ErrInvalidArgument) {
				t.Errorf("error %v does not match ErrInvalidArgument", err)
			}
		})
	}
}

func TestValidatePartNumber(t *testing.T) {
	tests := []struct {
		n    int
		want bool
	}{
		{-1, true}, {0, true}, {1, false}, {5000, false}, {common.MaxParts, false}, {common.MaxParts + 1, true},
	}
	for _, tt := range tests {
		err := ValidatePartNumber(tt.n)
		if (err != nil) != tt.want {
			t.Errorf("ValidatePartNumber(%d) = %v, want error %v", tt.n, err, tt.want)
		}
		if err != nil && !errors.Is(err, common.ErrInvalidPartNumber) {
			t.Errorf("error %v does not match ErrInvalidPartNumber", err)
		}
	}
}

func TestCloneMetadata(t *testing.T) {
	if got := CloneMetadata(nil); got == nil || got.Custom != nil {
		t.Errorf("CloneMetadata(nil) = %+v", got)
	}
	src := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"a": "1"}}
	c := CloneMetadata(src)
	c.Custom["a"] = "2"
	c.ContentType = "x"
	if src.Custom["a"] != "1" || src.ContentType != "text/plain" {
		t.Error("clone aliases the source")
	}
	if got := CloneMetadata(&common.Metadata{ContentType: "y"}); got.Custom != nil || got.ContentType != "y" {
		t.Errorf("clone without custom = %+v", got)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestCopyPart(t *testing.T) {
	t.Run("exact length", func(t *testing.T) {
		var buf bytes.Buffer
		etag, err := CopyPart(&buf, strings.NewReader("hello"), 5)
		if err != nil {
			t.Fatal(err)
		}
		if buf.String() != "hello" || etag == "" {
			t.Errorf("buf=%q etag=%q", buf.String(), etag)
		}
		again, _ := CopyPart(io.Discard, strings.NewReader("hello"), 5)
		other, _ := CopyPart(io.Discard, strings.NewReader("hellp"), 5)
		if again != etag || other == etag {
			t.Error("etag is not a function of the content")
		}
	})
	t.Run("zero length", func(t *testing.T) {
		if _, err := CopyPart(io.Discard, strings.NewReader(""), 0); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("short body", func(t *testing.T) {
		_, err := CopyPart(io.Discard, strings.NewReader("abc"), 5)
		if !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("long body", func(t *testing.T) {
		_, err := CopyPart(io.Discard, strings.NewReader("abcdef"), 5)
		if !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("negative size", func(t *testing.T) {
		_, err := CopyPart(io.Discard, strings.NewReader("a"), -1)
		if !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("too large is rejected before reading", func(t *testing.T) {
		_, err := CopyPart(io.Discard, iotest.ErrReader(errors.New("must not be read")), common.MaxPartSize+1)
		if !errors.Is(err, common.ErrPartTooLarge) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("read error", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := CopyPart(io.Discard, iotest.ErrReader(boom), 5)
		if !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("read error after full body", func(t *testing.T) {
		boom := errors.New("boom")
		r := io.MultiReader(strings.NewReader("abcde"), iotest.ErrReader(boom))
		_, err := CopyPart(io.Discard, r, 5)
		if !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("write error", func(t *testing.T) {
		_, err := CopyPart(failWriter{}, strings.NewReader("abcde"), 5)
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestCheckComplete(t *testing.T) {
	const big = common.MinPartSize
	stored := map[int]common.PartInfo{
		1: {PartNumber: 1, ETag: "e1", Size: big},
		2: {PartNumber: 2, ETag: "e2", Size: big},
		3: {PartNumber: 3, ETag: "e3", Size: 10},
	}
	lookup := func(n int) (common.PartInfo, bool) { p, ok := stored[n]; return p, ok }
	tests := []struct {
		name    string
		parts   []common.CompletedPart
		want    []int
		wantErr error
	}{
		{"all parts", []common.CompletedPart{{PartNumber: 1, ETag: "e1"}, {PartNumber: 2, ETag: "e2"}, {PartNumber: 3, ETag: "e3"}}, []int{1, 2, 3}, nil},
		{"small last part", []common.CompletedPart{{PartNumber: 1, ETag: "e1"}, {PartNumber: 3, ETag: "e3"}}, []int{1, 3}, nil},
		{"single small part", []common.CompletedPart{{PartNumber: 3, ETag: "e3"}}, []int{3}, nil},
		{"skipping a stored part is allowed", []common.CompletedPart{{PartNumber: 2, ETag: "e2"}}, []int{2}, nil},
		{"empty", nil, nil, common.ErrInvalidPartOrder},
		{"descending", []common.CompletedPart{{PartNumber: 2, ETag: "e2"}, {PartNumber: 1, ETag: "e1"}}, nil, common.ErrInvalidPartOrder},
		{"duplicate", []common.CompletedPart{{PartNumber: 1, ETag: "e1"}, {PartNumber: 1, ETag: "e1"}}, nil, common.ErrInvalidPartOrder},
		{"number zero", []common.CompletedPart{{PartNumber: 0, ETag: "e1"}}, nil, common.ErrInvalidPartNumber},
		{"number too big", []common.CompletedPart{{PartNumber: common.MaxParts + 1, ETag: "e"}}, nil, common.ErrInvalidPartNumber},
		{"not uploaded", []common.CompletedPart{{PartNumber: 1, ETag: "e1"}, {PartNumber: 4, ETag: "e4"}}, nil, common.ErrPartMismatch},
		{"etag mismatch", []common.CompletedPart{{PartNumber: 1, ETag: "nope"}}, nil, common.ErrPartMismatch},
		{"small non-final part", []common.CompletedPart{{PartNumber: 3, ETag: "e3"}, {PartNumber: 4, ETag: "e4"}}, nil, common.ErrPartTooSmall},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckComplete(tt.parts, lookup)
			if !errors.Is(err, tt.wantErr) && (tt.wantErr != nil || err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && !equalInts(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
