package memory

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

type putOnlyArchiver struct{ puts int }

func (p *putOnlyArchiver) Put(string, io.Reader) error { p.puts++; return nil }

func putEncryptedObject(t *testing.T) common.Storage {
	t.Helper()
	src := New()
	_ = src.Configure(nil, noCreds)
	md := &common.Metadata{Custom: map[string]string{
		"encryption_algorithm": "AES-256-GCM",
		"encryption_key_id":    "dek-1",
	}}
	if err := src.PutWithMetadata(context.Background(), "obj", strings.NewReader("ciphertext"), md); err != nil {
		t.Fatalf("PutWithMetadata: %v", err)
	}
	return src
}

// TestArchiveKeepsEncryptionMetadata: the archived copy of a client-side
// encrypted object carries its encryption_* metadata (#243).
func TestArchiveKeepsEncryptionMetadata(t *testing.T) {
	src := putEncryptedObject(t)
	dst := New()
	_ = dst.Configure(nil, noCreds)

	if err := src.Archive("obj", dst); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	md, err := dst.GetMetadata(context.Background(), "obj")
	if err != nil {
		t.Fatalf("destination GetMetadata: %v", err)
	}
	if md.Custom["encryption_key_id"] != "dek-1" || md.Custom["encryption_algorithm"] != "AES-256-GCM" {
		t.Fatalf("archived metadata lost encryption_*: %v", md.Custom)
	}
}

// TestArchiveEncryptedRefusesMetadataLessDestination: a destination that
// cannot store metadata is refused with a clear error and nothing is written.
func TestArchiveEncryptedRefusesMetadataLessDestination(t *testing.T) {
	src := putEncryptedObject(t)
	dst := &putOnlyArchiver{}
	if err := src.Archive("obj", dst); !errors.Is(err, common.ErrArchiveMetadataUnsupported) {
		t.Fatalf("Archive err = %v, want ErrArchiveMetadataUnsupported", err)
	}
	if dst.puts != 0 {
		t.Fatalf("refused archive wrote %d objects", dst.puts)
	}
}
