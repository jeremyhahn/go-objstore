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

package common

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type putOnlyArchiver struct{ puts int }

func (p *putOnlyArchiver) Put(string, io.Reader) error { p.puts++; return nil }

type mdArchiver struct {
	putOnlyArchiver
	data string
	md   *Metadata
}

func (m *mdArchiver) PutWithMetadata(_ context.Context, _ string, r io.Reader, md *Metadata) error {
	b, err := io.ReadAll(r)
	m.data, m.md = string(b), md
	return err
}

func encryptedMD() *Metadata {
	return &Metadata{
		ContentType: "text/plain",
		Size:        42,
		ETag:        "src-etag",
		Custom: map[string]string{
			"encryption_algorithm":         "AES-256-GCM",
			"encryption_key_id":            "dek-1",
			"at_rest_encryption_algorithm": "AES-256-GCM",
			"at_rest_encryption_key_id":    "rest-1",
			"owner":                        "team",
		},
	}
}

func TestArchiveCopyKeepsEncryptionMetadata(t *testing.T) {
	dst := &mdArchiver{}
	if err := ArchiveCopy(context.Background(), "k", strings.NewReader("ct"), encryptedMD(), dst); err != nil {
		t.Fatalf("ArchiveCopy: %v", err)
	}
	if dst.puts != 0 || dst.data != "ct" || dst.md == nil {
		t.Fatalf("want one PutWithMetadata of %q, got puts=%d data=%q md=%v", "ct", dst.puts, dst.data, dst.md)
	}
	c := dst.md.Custom
	if c["encryption_algorithm"] != "AES-256-GCM" || c["encryption_key_id"] != "dek-1" || c["owner"] != "team" {
		t.Fatalf("custom metadata not carried: %v", c)
	}
	if _, ok := c["at_rest_encryption_key_id"]; ok {
		t.Fatalf("source at-rest key id carried to destination: %v", c)
	}
	if _, ok := c["at_rest_encryption_algorithm"]; ok {
		t.Fatalf("source at-rest algorithm carried to destination: %v", c)
	}
	if dst.md.ContentType != "text/plain" || dst.md.Size != 0 || dst.md.ETag != "" {
		t.Fatalf("descriptive fields: %+v", dst.md)
	}
}

func TestArchiveCopyRefusesMetadataLessDestination(t *testing.T) {
	dst := &putOnlyArchiver{}
	err := ArchiveCopy(context.Background(), "k", strings.NewReader("ct"), encryptedMD(), dst)
	if !errors.Is(err, ErrArchiveMetadataUnsupported) {
		t.Fatalf("err = %v, want ErrArchiveMetadataUnsupported", err)
	}
	if dst.puts != 0 {
		t.Fatalf("refused archive still wrote %d objects", dst.puts)
	}
}

func TestArchiveCopyPlainObjectUsesPut(t *testing.T) {
	for name, md := range map[string]*Metadata{
		"nil":     nil,
		"at-rest": {Custom: map[string]string{"at_rest_encryption_key_id": "rest-1"}},
	} {
		dst := &putOnlyArchiver{}
		if err := ArchiveCopy(context.Background(), "k", strings.NewReader("x"), md, dst); err != nil || dst.puts != 1 {
			t.Fatalf("%s: err=%v puts=%d, want plain Put", name, err, dst.puts)
		}
	}
}

func TestHasClientEncryptionCanonicalKeys(t *testing.T) {
	if !HasClientEncryption(&Metadata{Custom: map[string]string{"Encryption_key_id": "dek-1"}}) {
		t.Fatal("canonicalised S3 key Encryption_key_id not detected")
	}
	if HasClientEncryption(&Metadata{Custom: map[string]string{"At_rest_encryption_key_id": "r"}}) {
		t.Fatal("at-rest key mistaken for client-side encryption")
	}
}

func TestArchiveCopyNilDestination(t *testing.T) {
	if err := ArchiveCopy(context.Background(), "k", strings.NewReader("x"), nil, nil); !errors.Is(err, ErrArchiveDestinationNil) {
		t.Fatalf("err = %v", err)
	}
}
