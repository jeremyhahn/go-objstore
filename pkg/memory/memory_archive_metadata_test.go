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

package memory

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
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

// recordingArchiver records each archived copy's data and key ID.
type recordingArchiver struct {
	mu     sync.Mutex
	copies []string // "<data>/<encryption_key_id>"
}

func (r *recordingArchiver) Put(key string, data io.Reader) error {
	return r.PutWithMetadata(context.Background(), key, data, nil)
}

func (r *recordingArchiver) PutWithMetadata(_ context.Context, _ string, data io.Reader, md *common.Metadata) error {
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	var id string
	if md != nil {
		id = md.Custom["encryption_key_id"]
	}
	r.mu.Lock()
	r.copies = append(r.copies, string(b)+"/"+id)
	r.mu.Unlock()
	return nil
}

// An archive racing puts of one key copies the bytes and the key ID of one
// put, never the ciphertext of one with the key ID of another (#243).
func TestArchiveRacingPutsKeepsKeyIDWithData(t *testing.T) {
	src := New()
	_ = src.Configure(nil, noCreds)
	ctx := context.Background()
	put := func(id string) error {
		md := &common.Metadata{Custom: map[string]string{
			"encryption_algorithm": "AES-256-GCM",
			"encryption_key_id":    id,
		}}
		return src.PutWithMetadata(ctx, "obj", strings.NewReader(id), md)
	}
	if err := put("dek-a"); err != nil {
		t.Fatalf("put: %v", err)
	}

	stop := make(chan struct{})
	putErr := make(chan error, 1)
	go func() {
		defer close(putErr)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id := "dek-a"
			if i%2 == 1 {
				id = "dek-b"
			}
			if err := put(id); err != nil {
				putErr <- err
				return
			}
		}
	}()
	dst := &recordingArchiver{}
	for range 2000 {
		if err := src.Archive("obj", dst); err != nil {
			t.Fatalf("Archive: %v", err)
		}
	}
	close(stop)
	if err := <-putErr; err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, c := range dst.copies {
		data, id, _ := strings.Cut(c, "/")
		if data != id {
			t.Fatalf("archived the ciphertext of %q with key ID %q", data, id)
		}
	}
}
