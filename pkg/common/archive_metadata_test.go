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

// A source that returns the client encryption names canonicalised (S3
// "Encryption_key_id") gives an archived copy with the lowercase names the
// encryption wrapper reads, so the copy decrypts with its recorded key and
// not the default one (#243).
func TestArchiveCopyLowercasesEncryptionNames(t *testing.T) {
	ctx := context.Background()
	const plaintext = "secret written with key A"
	cipher, err := (&keyedEncrypter{keyID: "keyA"}).Encrypt(ctx, strings.NewReader(plaintext))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	src := &Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"Encryption_key_id":         "keyA",
			"Encryption_algorithm":      "test-keyed",
			"At_rest_encryption_key_id": "rest-1",
			"Owner":                     "team",
		},
	}

	dst := newMockUnderlyingStorage()
	if err := ArchiveCopy(ctx, "k", cipher, src, dst); err != nil {
		t.Fatalf("ArchiveCopy: %v", err)
	}
	md, err := dst.GetMetadata(ctx, "k")
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	want := map[string]string{
		"encryption_key_id":    "keyA",
		"encryption_algorithm": "test-keyed",
		"Owner":                "team",
	}
	if len(md.Custom) != len(want) {
		t.Fatalf("archived custom metadata = %v, want %v", md.Custom, want)
	}
	for k, v := range want {
		if md.Custom[k] != v {
			t.Errorf("Custom[%q] = %q, want %q", k, md.Custom[k], v)
		}
	}

	// The default key of the reader is B: the copy decrypts with key A.
	readerB := NewEncryptedStorage(dst, newKeyedFactory("keyB"))
	if got := readAllString(t, readerB, "k"); got != plaintext {
		t.Errorf("read archived copy = %q, want %q", got, plaintext)
	}

	// An exact lowercase entry wins over the same name in another case.
	both := archivedMetadata(&Metadata{Custom: map[string]string{
		"encryption_key_id": "keyA",
		"Encryption_key_id": "keyB",
	}})
	if len(both.Custom) != 1 || both.Custom["encryption_key_id"] != "keyA" {
		t.Errorf("archived custom metadata = %v, want encryption_key_id=keyA only", both.Custom)
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
