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

//go:build integration

package s3

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

// patternReader is size bytes of a repeating, offset-dependent pattern,
// produced on demand: it is seekable, so the S3 backend can sign and retry
// each part, and nothing but the current read buffer is held in memory.
type patternReader struct {
	size, off int64
}

func patternByte(off int64) byte { return byte(off*31 + off>>20) }

func (p *patternReader) Read(b []byte) (int, error) {
	if p.off >= p.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(b)), p.size-p.off))
	for i := range n {
		b[i] = patternByte(p.off + int64(i))
	}
	p.off += int64(n)
	return n, nil
}

func (p *patternReader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = p.off + offset
	case io.SeekEnd:
		target = p.size + offset
	}
	if target < 0 {
		return 0, errors.New("pattern reader: negative offset")
	}
	p.off = target
	return target, nil
}

// TestS3_LargeObject_6GiB puts a 6 GiB object, over the 5 GiB single-PUT
// limit, so the backend must switch to a multipart upload by itself, then
// reads it back and compares it byte for byte. Both directions stream.
func TestS3_LargeObject_6GiB(t *testing.T) {
	if testing.Short() {
		t.Skip("6 GiB transfer skipped in -short mode")
	}
	const size = int64(6) << 30
	bucket := "objstore-s3-large"
	testcommon.CreateS3Bucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://floci:4566",
		"forcePathStyle": "true",
		"storage_class":  "STANDARD",
	}, testcommon.Creds(t, "test", "test"))
	if err != nil {
		t.Fatal(err)
	}

	key := "large/6gib.bin"
	if err := st.Put(key, &patternReader{size: size}); err != nil {
		t.Fatalf("Put of 6 GiB failed: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(key) })

	rc, err := st.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	want := sha256.New()
	if _, err := io.Copy(want, &patternReader{size: size}); err != nil {
		t.Fatal(err)
	}
	got := sha256.New()
	n, err := io.Copy(got, rc)
	if err != nil {
		t.Fatalf("reading the object back failed after %d bytes: %v", n, err)
	}
	if n != size {
		t.Fatalf("read %d bytes, want %d", n, size)
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatal("object read back differs from the object written")
	}
}
