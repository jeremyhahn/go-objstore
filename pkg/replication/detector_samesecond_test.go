package replication

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

// A same-length rewrite of the destination within the same second, carrying
// the copy's custom metadata, is seen as a change. No sleep: the rewrite
// follows the copy immediately.
func TestHasChanged_SameSecondSameSizeDestRewrite(t *testing.T) {
	backends := map[string]func(t *testing.T) common.Storage{
		"memory": func(*testing.T) common.Storage { return memory.New() },
		"local": func(t *testing.T) common.Storage {
			s := local.New()
			if err := s.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dest := mk(t)
			src := &common.Metadata{ETag: "src-1", Size: 4, LastModified: time.Unix(1_700_000_000, 0)}
			s := &Syncer{dest: dest, logger: &mockLogger{}, auditLog: &mockAuditLogger{}, metrics: NewReplicationMetrics()}

			fp := sourceFingerprint(src)
			stored, err := s.putDestination(ctx, "k", bytes.NewBufferString("AAAA"), fp)
			if err != nil {
				t.Fatal(err)
			}
			s.bindDestination(ctx, "k", fp, stored)

			copied, err := dest.GetMetadata(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if hasChanged(src, copied) {
				t.Fatalf("fresh copy reported changed: %+v", copied)
			}

			// Rewrite with the same length and the copy's own metadata.
			rewrite := *copied
			if err := dest.PutWithMetadata(ctx, "k", bytes.NewBufferString("BBBB"), &rewrite); err != nil {
				t.Fatal(err)
			}
			after, err := dest.GetMetadata(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if !hasChanged(src, after) {
				t.Fatalf("same-second rewrite not seen: copy %q, rewrite %q", copied.ETag, after.ETag)
			}
		})
	}
}
