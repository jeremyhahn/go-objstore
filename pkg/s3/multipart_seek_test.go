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

//go:build awss3

package s3

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSizedSeeker(t *testing.T) {
	newSeeker := func(data string, off, size int64) *sizedSeeker {
		rs := strings.NewReader(data)
		if _, err := rs.Seek(off, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		return &sizedSeeker{sizedReader: sizedReader{r: rs, size: size}, rs: rs, start: off}
	}

	t.Run("reads from the start offset and rewinds to it", func(t *testing.T) {
		s := newSeeker("xxabcd", 2, 4)
		for i := 0; i < 2; i++ {
			got, err := io.ReadAll(s)
			if err != nil || string(got) != "abcd" {
				t.Fatalf("pass %d: read %q, %v", i, got, err)
			}
			if pos, err := s.Seek(0, io.SeekStart); err != nil || pos != 0 {
				t.Fatalf("Seek start = %d, %v", pos, err)
			}
		}
	})
	t.Run("whence", func(t *testing.T) {
		s := newSeeker("xxabcd", 2, 4)
		if pos, err := s.Seek(0, io.SeekEnd); err != nil || pos != 4 {
			t.Errorf("Seek end = %d, %v", pos, err)
		}
		if pos, err := s.Seek(-3, io.SeekEnd); err != nil || pos != 1 {
			t.Errorf("Seek end-3 = %d, %v", pos, err)
		}
		if pos, err := s.Seek(1, io.SeekCurrent); err != nil || pos != 2 {
			t.Errorf("Seek current+1 = %d, %v", pos, err)
		}
		got, _ := io.ReadAll(s)
		if string(got) != "cd" {
			t.Errorf("read %q after seeking to 2", got)
		}
	})
	t.Run("rejects positions outside the part", func(t *testing.T) {
		s := newSeeker("xxabcd", 2, 4)
		for _, c := range []struct {
			off    int64
			whence int
		}{{-1, io.SeekStart}, {5, io.SeekStart}, {1, io.SeekEnd}, {-1, io.SeekCurrent}, {0, 99}} {
			if _, err := s.Seek(c.off, c.whence); err == nil {
				t.Errorf("Seek(%d, %d) succeeded", c.off, c.whence)
			}
		}
	})
	t.Run("underlying seek failure", func(t *testing.T) {
		boom := errors.New("boom")
		s := &sizedSeeker{sizedReader: sizedReader{r: strings.NewReader("abcd"), size: 4}, rs: failingSeeker{boom}}
		if _, err := s.Seek(0, io.SeekStart); !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
	})
}

type failingSeeker struct{ err error }

func (f failingSeeker) Read([]byte) (int, error)       { return 0, f.err }
func (f failingSeeker) Seek(int64, int) (int64, error) { return 0, f.err }
