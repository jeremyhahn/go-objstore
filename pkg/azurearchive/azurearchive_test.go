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

//go:build azurearchive

package azurearchive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// Mocks implementing the small interfaces for isolated unit tests.
type mockBlob struct {
	uploadErr error
	data      []byte
}

func (m *mockBlob) UploadFromReader(ctx context.Context, r io.Reader) error {
	if m.uploadErr != nil {
		return m.uploadErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data = data
	return nil
}

type mockContainer struct {
	b *mockBlob
}

func (m mockContainer) NewBlockBlob(_ string) blobUploader { return m.b }

func TestAzureArchive_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if _, ok := s.(*AzureArchive); !ok {
		t.Fatal("New() did not return *AzureArchive type")
	}
}

func TestAzureArchive_Configure_Success(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
	}{
		{
			name: "standard configuration",
			settings: map[string]string{
				"accountName":   "testaccount",
				"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
				"containerName": "testcontainer",
			},
		},
		{
			name: "configuration with custom endpoint",
			settings: map[string]string{
				"accountName":   "testaccount",
				"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
				"containerName": "testcontainer",
				"endpoint":      "http://127.0.0.1:10000/devstoreaccount1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AzureArchive{}
			err := a.Configure(tt.settings)
			if err != nil {
				t.Fatalf("Configure() error = %v", err)
			}
			if a.container == nil {
				t.Fatal("container not set after Configure()")
			}
		})
	}
}

func TestAzureArchive_Configure_Errors(t *testing.T) {
	tests := []struct {
		name        string
		settings    map[string]string
		expectedErr string
	}{
		{
			name:        "missing accountName",
			settings:    map[string]string{"accountKey": "key", "containerName": "container"},
			expectedErr: "accountName",
		},
		{
			name:        "missing accountKey",
			settings:    map[string]string{"accountName": "account", "containerName": "container"},
			expectedErr: "accountKey",
		},
		{
			name:        "missing containerName",
			settings:    map[string]string{"accountName": "account", "accountKey": "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA=="},
			expectedErr: "containerName",
		},
		{
			name:        "empty settings",
			settings:    map[string]string{},
			expectedErr: "accountName",
		},
		{
			name: "invalid accountKey",
			settings: map[string]string{
				"accountName":   "account",
				"accountKey":    "invalid-key",
				"containerName": "container",
			},
			expectedErr: "illegal base64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AzureArchive{}
			err := a.Configure(tt.settings)
			if err == nil {
				t.Fatal("Configure() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Configure() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzureArchive_Put_Success(t *testing.T) {
	tests := []struct {
		name string
		key  string
		data string
	}{
		{"simple put", "test.txt", "hello archive"},
		{"empty data", "empty.txt", ""},
		{"large data", "large.txt", strings.Repeat("x", 10000)},
		{"key with path", "archive/2024/file.txt", "archived file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := &mockBlob{}
			a := &AzureArchive{container: mockContainer{b: blob}}

			err := a.Put(tt.key, bytes.NewBufferString(tt.data))
			if err != nil {
				t.Fatalf("Put() error = %v", err)
			}

			// Verify data was uploaded
			if string(blob.data) != tt.data {
				t.Fatalf("Put() uploaded data = %q, want %q", string(blob.data), tt.data)
			}
		})
	}
}

func TestAzureArchive_Put_Errors(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *AzureArchive
		key         string
		expectedErr string
	}{
		{
			name: "not configured",
			setup: func() *AzureArchive {
				return &AzureArchive{}
			},
			key:         "test.txt",
			expectedErr: "not configured",
		},
		{
			name: "upload error",
			setup: func() *AzureArchive {
				blob := &mockBlob{uploadErr: errors.New("upload failed")}
				return &AzureArchive{container: mockContainer{b: blob}}
			},
			key:         "test.txt",
			expectedErr: "upload failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.setup()
			err := a.Put(tt.key, bytes.NewBufferString("data"))
			if err == nil {
				t.Fatal("Put() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Put() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

// Test that Put properly buffers data from reader
func TestAzureArchive_Put_BufferingBehavior(t *testing.T) {
	blob := &mockBlob{}
	a := &AzureArchive{container: mockContainer{b: blob}}

	// Create a custom reader that tracks if it was fully read
	data := []byte("test data for buffering")
	reader := bytes.NewReader(data)

	err := a.Put("test.txt", reader)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Verify reader was fully consumed
	remaining, _ := io.ReadAll(reader)
	if len(remaining) != 0 {
		t.Fatalf("Put() did not fully consume reader, remaining: %d bytes", len(remaining))
	}

	// Verify data was correctly uploaded
	if !bytes.Equal(blob.data, data) {
		t.Fatalf("Put() uploaded data mismatch")
	}
}

// Test error during buffering (ReadAll fails)
func TestAzureArchive_Put_ReadError(t *testing.T) {
	blob := &mockBlob{}
	a := &AzureArchive{container: mockContainer{b: blob}}

	// Create a reader that returns an error
	errReader := &errorReader{err: errors.New("read error")}

	err := a.Put("test.txt", errReader)
	if err == nil {
		t.Fatal("Put() expected error from reader, got nil")
	}
	if !strings.Contains(err.Error(), "read error") {
		t.Fatalf("Put() error = %v, want error containing %q", err, "read error")
	}
}

// errorReader is a reader that always returns an error
type errorReader struct {
	err error
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func TestAzureArchive_Configure_BadEndpoint(t *testing.T) {
	a := &AzureArchive{}
	settings := map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
		"containerName": "testcontainer",
		"endpoint":      "://bad url",
	}
	err := a.Configure(settings)
	if err == nil {
		t.Fatal("Configure() expected error for bad endpoint, got nil")
	}
}
