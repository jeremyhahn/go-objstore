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
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

// largeEnv opts in to TestS3_LargeObject_6GiB. Floci keeps every object in
// RAM, so a 6 GiB upload exhausts a CI runner; the test runs only where the
// host has the memory for it.
const largeEnv = "OBJSTORE_IT_LARGE"

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
// reads it back and compares it byte for byte. Both directions stream. It
// runs only with OBJSTORE_IT_LARGE=1 (see largeEnv).
func TestS3_LargeObject_6GiB(t *testing.T) {
	if testing.Short() {
		t.Skip("6 GiB transfer skipped in -short mode")
	}
	if os.Getenv(largeEnv) != "1" {
		t.Skipf("6 GiB transfer is opt-in: set %s=1 (Floci holds every object in RAM)", largeEnv)
	}
	const size = int64(6) << 30
	st := newPatternStore(t, "objstore-s3-large")

	key := "large/6gib.bin"
	if err := st.Put(key, &patternReader{size: size}); err != nil {
		t.Fatalf("Put of 6 GiB failed: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(key) })
	checkPattern(t, st, key, size)
}

// TestS3_Multipart_Small uploads a 13 MiB object in three parts (two of the
// 5 MiB minimum and a short last part) through the backend's multipart API,
// then reads it back and compares it byte for byte. It runs by default and
// keeps Floci's in-memory footprint small; the automatic switch to multipart
// above the threshold is pinned by the pkg/s3 unit tests and, against a real
// 6 GiB object, by TestS3_LargeObject_6GiB.
func TestS3_Multipart_Small(t *testing.T) {
	st := newPatternStore(t, "objstore-s3-multipart")
	mu, ok := st.(common.MultipartUploader)
	if !ok {
		t.Fatalf("s3 storage %T does not implement common.MultipartUploader", st)
	}

	const size = int64(2*common.MinPartSize + 3<<20)
	key := "multipart/13mib.bin"
	ctx := context.Background()
	id, err := mu.InitiateMultipartUpload(ctx, key, nil)
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}
	var parts []common.CompletedPart
	for n, off := 1, int64(0); off < size; n, off = n+1, off+common.MinPartSize {
		end := min(off+common.MinPartSize, size)
		buf, err := io.ReadAll(&patternReader{off: off, size: end})
		if err != nil {
			t.Fatal(err)
		}
		info, err := mu.UploadPart(ctx, key, id, n, bytes.NewReader(buf), end-off)
		if err != nil {
			_ = mu.AbortMultipartUpload(ctx, key, id)
			t.Fatalf("UploadPart %d failed: %v", n, err)
		}
		parts = append(parts, common.CompletedPart{PartNumber: info.PartNumber, ETag: info.ETag})
	}
	if err := mu.CompleteMultipartUpload(ctx, key, id, parts); err != nil {
		_ = mu.AbortMultipartUpload(ctx, key, id)
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(key) })
	checkPattern(t, st, key, size)
}

// newPatternStore creates bucket on Floci and returns an s3 storage on it.
func newPatternStore(t *testing.T, bucket string) common.Storage {
	t.Helper()
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
	return st
}

// checkPattern reads key back and checks it is the first size bytes of the
// pattern, streaming both sides.
func checkPattern(t *testing.T, st common.Storage, key string, size int64) {
	t.Helper()
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
