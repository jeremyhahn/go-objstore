package local

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A writer that replaces the key with same-length data after the upload is
// published must not be described by the upload's result or its sidecar.
func TestPutWithMetadataResult_IdentityFromWrittenFile(t *testing.T) {
	dir := t.TempDir()
	l := New().(*Local)
	if err := l.Configure(map[string]string{"path": dir}, noCreds); err != nil {
		t.Fatal(err)
	}

	replaced := time.Unix(1_000_000_000, 0)
	afterPublishHook = func(name string) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("BBBB"), 0o644); err != nil {
			t.Error(err)
		}
		if err := os.Chtimes(p, replaced, replaced); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterPublishHook = nil })

	got, err := l.PutWithMetadataResult(context.Background(), "k", bytes.NewBufferString("AAAA"), nil)
	if err != nil {
		t.Fatal(err)
	}
	replacement := fmt.Sprintf("%d-%d", replaced.UnixNano(), 4)
	if got == nil || got.ETag == "" || got.Size != 4 {
		t.Fatalf("result = %+v, want the upload's identity", got)
	}
	if got.ETag == replacement || got.LastModified.Equal(replaced) {
		t.Fatalf("result %+v describes the replacement", got)
	}

}
