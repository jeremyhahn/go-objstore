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

package factory

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

// Test error variable
var errTestMockArchive = errors.New("mock archive error")

func TestLocal(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		// Local backend may not be available without build tag
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	key := "test-key"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r, err := storage.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	readData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected %s, got %s", data, readData)
	}

	err = storage.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storage.Get(key)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLocal_Configure_NoPath(t *testing.T) {
	// Test that local backend returns error when path is not set
	// This will either work (if local backend is compiled in) or return ErrUnknownBackend
	_, err := NewStorage("local", map[string]string{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Error can be either ErrUnknownBackend (no build tag) or configuration error (has build tag)
}

func TestLocal_Put_Error(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	// Create a file with the same name as the directory
	file, err := os.Create(tmpdir + "/test-key")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	key := "test-key/test-file"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLocal_Get_NotFound(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	_, err = storage.Get("non-existent-key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLocal_Delete_NotFound(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	err = storage.Delete("non-existent-key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLocal_Delete_PermissionError(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	key := "test-key"
	data := []byte("test-data")

	// Put the file first
	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// Make the directory read-only; in some environments, delete may still succeed.
	if err := os.Chmod(tmpdir, 0555); err != nil {
		t.Fatal(err)
	}
	// Attempt to delete the file; if it succeeds, skip as environment allows it.
	err = storage.Delete(key)
	if err == nil {
		t.Skip("delete succeeded under read-only dir; skipping permission error assertion")
	}
}

type mockErrorArchiver struct{}

func (m *mockErrorArchiver) Put(key string, data io.Reader) error {
	return errTestMockArchive
}

func TestLocal_Archive_DestinationError(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "objstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpdir) }()

	storage, err := NewStorage("local", map[string]string{"path": tmpdir})
	if err != nil {
		if errors.Is(err, ErrUnknownBackend) {
			t.Skip("local backend not available (requires -tags local)")
		}
		t.Fatal(err)
	}

	key := "test-key"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Archive(key, &mockErrorArchiver{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFactory_NewStorage_S3_Config(t *testing.T) {
	st, err := NewStorage("s3", map[string]string{"bucket": "b", "region": "us-east-1"})

	// Either the backend is available (success) or not compiled in (error)
	if err != nil {
		// If build tag not present, should get ErrUnknownBackend error
		if !errors.Is(err, ErrUnknownBackend) {
			// Got an error but not the expected one - this is a real failure
			t.Fatalf("expected ErrUnknownBackend or success, got: %v", err)
		}
		// Expected error due to missing build tag - test passes
		return
	}

	// Backend is available - verify it was created
	if st == nil {
		t.Fatalf("NewStorage succeeded but returned nil storage")
	}
}

func TestFactory_NewStorage_Azure_Config(t *testing.T) {
	storage, err := NewStorage("azure", map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
		"containerName": "testcontainer",
		"endpoint":      "http://127.0.0.1:10000/testaccount",
	})

	if err != nil {
		if !errors.Is(err, ErrUnknownBackend) {
			t.Fatalf("expected ErrUnknownBackend or success, got: %v", err)
		}
		return
	}

	if storage == nil {
		t.Fatalf("NewStorage succeeded but returned nil storage")
	}
}

func TestFactory_NewStorage_MinIO_Config(t *testing.T) {
	storage, err := NewStorage("minio", map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})

	if err != nil {
		if !errors.Is(err, ErrUnknownBackend) {
			t.Fatalf("expected ErrUnknownBackend or success, got: %v", err)
		}
		return
	}

	if storage == nil {
		t.Fatalf("NewStorage succeeded but returned nil storage")
	}
}

func TestS3(t *testing.T) {
	bucket := os.Getenv("AWS_BUCKET")
	region := os.Getenv("AWS_REGION")

	// Try to create S3 storage
	storage, err := NewStorage("s3", map[string]string{
		"bucket": bucket,
		"region": region,
	})

	// If S3 backend not compiled in, verify we get the expected error
	if err != nil {
		expectedErr := "unknown backend type: s3"
		if err.Error() == expectedErr {
			// Backend not compiled in - test passes
			return
		}
		// Some other error - could be missing credentials
		if bucket == "" || region == "" {
			// Missing environment variables - test passes (no credentials configured)
			return
		}
		t.Fatalf("unexpected error: %v", err)
	}

	// Backend is available - run integration test if credentials are set
	if bucket == "" || region == "" {
		// No credentials configured - can't run integration test
		t.Log("S3 backend available but no credentials configured (AWS_BUCKET/AWS_REGION not set)")
		return
	}

	key := "test-key"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r, err := storage.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	readData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected %s, got %s", data, readData)
	}

	err = storage.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storage.Get(key)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGCS(t *testing.T) {
	bucket := os.Getenv("GCS_BUCKET")

	storage, err := NewStorage("gcs", map[string]string{
		"bucket": bucket,
	})

	if err != nil {
		expectedErr := "unknown backend type: gcs"
		if err.Error() == expectedErr {
			return
		}
		if bucket == "" {
			return
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if bucket == "" {
		t.Log("GCS backend available but no credentials configured (GCS_BUCKET not set)")
		return
	}

	key := "test-key"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r, err := storage.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	readData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected %s, got %s", data, readData)
	}

	err = storage.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storage.Get(key)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestAzure(t *testing.T) {
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	containerName := os.Getenv("AZURE_CONTAINER_NAME")

	storage, err := NewStorage("azure", map[string]string{
		"accountName":   accountName,
		"accountKey":    accountKey,
		"containerName": containerName,
	})

	if err != nil {
		expectedErr := "unknown backend type: azure"
		if err.Error() == expectedErr {
			return
		}
		if accountName == "" || accountKey == "" || containerName == "" {
			return
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if accountName == "" || accountKey == "" || containerName == "" {
		t.Log("Azure backend available but no credentials configured")
		return
	}

	key := "test-key"
	data := []byte("test-data")

	err = storage.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	r, err := storage.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	readData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected %s, got %s", data, readData)
	}

	err = storage.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = storage.Get(key)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFactory_NewStorage_GCS_Config(t *testing.T) {
	storage, err := NewStorage("gcs", map[string]string{"bucket": "test-bucket", "skip_client": "true"})

	if err != nil {
		if !errors.Is(err, ErrUnknownBackend) {
			t.Fatalf("expected ErrUnknownBackend or success, got: %v", err)
		}
		return
	}

	if storage == nil {
		t.Fatalf("NewStorage succeeded but returned nil storage")
	}
}

func TestFactory_NewStorage_ArchiveOnlyError(t *testing.T) {
	tests := []struct {
		name        string
		backendType string
	}{
		{"glacier", "glacier"},
		{"azurearchive", "azurearchive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStorage(tt.backendType, map[string]string{})
			if err == nil {
				t.Fatal("expected error for archive-only backend, got nil")
			}
			if !errors.Is(err, ErrArchiveOnlyBackend) {
				t.Fatalf("expected ErrArchiveOnlyBackend, got %q", err.Error())
			}
		})
	}
}

func TestFactory_NewStorage_UnknownBackend(t *testing.T) {
	_, err := NewStorage("unknown", map[string]string{})
	if err == nil {
		t.Fatal("expected error for unknown backend, got nil")
	}
	if !errors.Is(err, ErrUnknownBackend) {
		t.Fatalf("expected ErrUnknownBackend, got %q", err.Error())
	}
}

func TestFactory_NewStorage_ConfigureErrors(t *testing.T) {
	tests := []struct {
		name        string
		backendType string
		settings    map[string]string
	}{
		{"s3 missing bucket", "s3", map[string]string{"region": "us-east-1"}},
		{"gcs missing bucket", "gcs", map[string]string{}},
		{"azure missing account", "azure", map[string]string{"containerName": "test"}},
		{"local missing path", "local", map[string]string{}},
		{"minio missing bucket", "minio", map[string]string{"endpoint": "http://localhost:9000", "accessKey": "minioadmin", "secretKey": "minioadmin"}},
		{"minio missing endpoint", "minio", map[string]string{"bucket": "test-bucket", "accessKey": "minioadmin", "secretKey": "minioadmin"}},
		{"minio missing accessKey", "minio", map[string]string{"bucket": "test-bucket", "endpoint": "http://localhost:9000", "secretKey": "minioadmin"}},
		{"minio missing secretKey", "minio", map[string]string{"bucket": "test-bucket", "endpoint": "http://localhost:9000", "accessKey": "minioadmin"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStorage(tt.backendType, tt.settings)
			if err == nil {
				t.Fatalf("expected error for %s with missing config, got nil", tt.backendType)
			}
		})
	}
}

func TestFactory_NewArchiver_Glacier(t *testing.T) {
	archiver, err := NewArchiver("glacier", map[string]string{
		"vaultName": "test-vault",
		"region":    "us-east-1",
	})

	if err != nil {
		if !errors.Is(err, ErrUnknownArchiver) {
			t.Fatalf("expected ErrUnknownArchiver or success, got: %v", err)
		}
		return
	}

	if archiver == nil {
		t.Fatalf("NewArchiver succeeded but returned nil archiver")
	}
}

func TestFactory_NewArchiver_AzureArchive(t *testing.T) {
	archiver, err := NewArchiver("azurearchive", map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
		"containerName": "testcontainer",
	})

	if err != nil {
		if !errors.Is(err, ErrUnknownArchiver) {
			t.Fatalf("expected ErrUnknownArchiver or success, got: %v", err)
		}
		return
	}

	if archiver == nil {
		t.Fatalf("NewArchiver succeeded but returned nil archiver")
	}
}

func TestFactory_NewArchiver_Local(t *testing.T) {
	archiver, err := NewArchiver("local", map[string]string{
		"path": "/tmp/test-archive",
	})
	// Either the backend is available (success) or not compiled in (error)
	if err != nil {
		// If build tag not present, should get ErrUnknownArchiver error
		if !errors.Is(err, ErrUnknownArchiver) {
			t.Fatalf("expected ErrUnknownArchiver or success, got: %v", err)
		}
		// Expected error due to missing build tag - test passes
		return
	}
	if archiver == nil {
		t.Fatal("expected local archiver instance")
	}
}

func TestFactory_NewArchiver_UnknownType(t *testing.T) {
	_, err := NewArchiver("unknown", map[string]string{})
	if err == nil {
		t.Fatal("expected error for unknown archiver type, got nil")
	}
	if !errors.Is(err, ErrUnknownArchiver) {
		t.Fatalf("expected ErrUnknownArchiver, got %q", err.Error())
	}
}

func TestFactory_NewArchiver_ConfigureErrors(t *testing.T) {
	tests := []struct {
		name     string
		archType string
		settings map[string]string
	}{
		{"local missing path", "local", map[string]string{}},
		{"glacier missing vault", "glacier", map[string]string{"region": "us-east-1"}},
		{"azurearchive missing account", "azurearchive", map[string]string{"containerName": "test"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewArchiver(tt.archType, tt.settings)
			if err == nil {
				t.Fatalf("expected error for %s with missing config, got nil", tt.archType)
			}
		})
	}
}
