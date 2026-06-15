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

//go:build azureblob

package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Test error variables
var (
	errTestPutError      = errors.New("put error")
	errTestListError     = errors.New("list error")
	errTestBlobNotFound  = errors.New("blob not found")
	errTestGetPropsError = errors.New("get properties error")
)

// mockContainerEnhanced for enhanced testing
type mockContainerEnhanced struct {
	newBlockBlobFn  func(name string) BlobAPI
	listBlobsFlatFn func(ctx context.Context, prefix string) ([]string, error)
}

func (m *mockContainerEnhanced) NewBlockBlob(name string) BlobAPI {
	if m.newBlockBlobFn != nil {
		return m.newBlockBlobFn(name)
	}
	return &mockBlob{}
}

func (m *mockContainerEnhanced) ListBlobsFlat(ctx context.Context, prefix string) ([]string, error) {
	if m.listBlobsFlatFn != nil {
		return m.listBlobsFlatFn(ctx, prefix)
	}
	return []string{}, nil
}

// mockBlob for enhanced testing
type mockBlob struct {
	uploadFn         func(ctx context.Context, r io.Reader) error
	readFn           func(ctx context.Context) (io.ReadCloser, error)
	deleteFn         func(ctx context.Context) error
	getPropertiesFn  func(ctx context.Context) (*BlobProperties, error)
	setMetadataFn    func(ctx context.Context, metadata map[string]string) error
	setHTTPHeadersFn func(ctx context.Context, headers azblob.BlobHTTPHeaders) error
}

func (m *mockBlob) UploadFromReader(ctx context.Context, r io.Reader) error {
	if m.uploadFn != nil {
		return m.uploadFn(ctx, r)
	}
	return nil
}

func (m *mockBlob) NewReader(ctx context.Context) (io.ReadCloser, error) {
	if m.readFn != nil {
		return m.readFn(ctx)
	}
	return io.NopCloser(nil), nil
}

func (m *mockBlob) Delete(ctx context.Context) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx)
	}
	return nil
}

func (m *mockBlob) GetProperties(ctx context.Context) (*BlobProperties, error) {
	if m.getPropertiesFn != nil {
		return m.getPropertiesFn(ctx)
	}
	return &BlobProperties{}, nil
}

func (m *mockBlob) SetMetadata(ctx context.Context, metadata map[string]string) error {
	if m.setMetadataFn != nil {
		return m.setMetadataFn(ctx, metadata)
	}
	return nil
}

func (m *mockBlob) SetHTTPHeaders(ctx context.Context, headers azblob.BlobHTTPHeaders) error {
	if m.setHTTPHeadersFn != nil {
		return m.setHTTPHeadersFn(ctx, headers)
	}
	return nil
}

// fakeStorageError implements azblob.StorageError for not-found testing.
type fakeStorageError struct {
	code azblob.ServiceCodeType
}

func (f *fakeStorageError) Error() string                       { return string(f.code) }
func (f *fakeStorageError) Timeout() bool                       { return false }
func (f *fakeStorageError) Temporary() bool                     { return false }
func (f *fakeStorageError) Response() *http.Response            { return nil }
func (f *fakeStorageError) ServiceCode() azblob.ServiceCodeType { return f.code }

var _ azblob.StorageError = (*fakeStorageError)(nil)

// TestAzure_PutWithContext tests context-aware put operation
func TestAzure_PutWithContext(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				uploadFn: func(ctx context.Context, r io.Reader) error {
					return nil
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.PutWithContext(ctx, "key", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// TestAzure_PutWithMetadata tests put with metadata
func TestAzure_PutWithMetadata(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "application/octet-stream",
		Size:        1024,
	}

	err := a.PutWithMetadata(ctx, "key", nil, metadata)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// TestAzure_GetWithContext tests context-aware get operation
func TestAzure_GetWithContext(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				readFn: func(ctx context.Context) (io.ReadCloser, error) {
					return io.NopCloser(nil), nil
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	_, err := a.GetWithContext(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// TestAzure_DeleteWithContext tests context-aware delete operation
func TestAzure_DeleteWithContext(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				deleteFn: func(ctx context.Context) error {
					return nil
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.DeleteWithContext(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// TestAzure_GetMetadata tests metadata retrieval from blob properties.
func TestAzure_GetMetadata(t *testing.T) {
	lastModified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return &BlobProperties{
						Size:            1024,
						ContentType:     "application/json",
						ContentEncoding: "gzip",
						LastModified:    lastModified,
						ETag:            `"abc123"`,
						Metadata:        map[string]string{"author": "test"},
					}, nil
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	metadata, err := a.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// GetMetadata must return a non-nil struct so callers never dereference nil.
	if metadata == nil {
		t.Fatal("expected non-nil metadata, got nil")
	}
	if metadata.Size != 1024 {
		t.Errorf("expected size 1024, got %d", metadata.Size)
	}
	if metadata.ContentType != "application/json" {
		t.Errorf("expected content type 'application/json', got %q", metadata.ContentType)
	}
	if metadata.ContentEncoding != "gzip" {
		t.Errorf("expected content encoding 'gzip', got %q", metadata.ContentEncoding)
	}
	if !metadata.LastModified.Equal(lastModified) {
		t.Errorf("expected last modified %v, got %v", lastModified, metadata.LastModified)
	}
	if metadata.ETag != `"abc123"` {
		t.Errorf("expected etag '\"abc123\"', got %q", metadata.ETag)
	}
	if metadata.Custom["author"] != "test" {
		t.Errorf("expected custom metadata author=test, got %v", metadata.Custom)
	}
}

// TestAzure_GetMetadata_NotFound tests that a missing blob yields an error
// wrapping common.ErrKeyNotFound.
func TestAzure_GetMetadata_NotFound(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return nil, &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	_, err := a.GetMetadata(ctx, "missing")
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// TestAzure_GetMetadata_Error tests that non-not-found errors propagate unchanged.
func TestAzure_GetMetadata_Error(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return nil, errTestGetPropsError
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	_, err := a.GetMetadata(ctx, "key")
	if !errors.Is(err, errTestGetPropsError) {
		t.Fatalf("expected the underlying error to propagate, got %v", err)
	}
	if errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("non-not-found errors must not map to ErrKeyNotFound, got %v", err)
	}
}

// TestAzure_UpdateMetadata tests that metadata updates are applied via
// SetMetadata and SetHTTPHeaders with replace semantics.
func TestAzure_UpdateMetadata(t *testing.T) {
	var gotMetadata map[string]string
	var gotHeaders azblob.BlobHTTPHeaders
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setMetadataFn: func(ctx context.Context, metadata map[string]string) error {
					gotMetadata = metadata
					return nil
				},
				setHTTPHeadersFn: func(ctx context.Context, headers azblob.BlobHTTPHeaders) error {
					gotHeaders = headers
					return nil
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"updated": "true"},
	}

	err := a.UpdateMetadata(ctx, "key", metadata)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if gotMetadata["updated"] != "true" {
		t.Errorf("expected custom metadata to be applied, got %v", gotMetadata)
	}
	if gotHeaders.ContentType != "text/plain" {
		t.Errorf("expected content type 'text/plain', got %q", gotHeaders.ContentType)
	}
	if gotHeaders.ContentEncoding != "gzip" {
		t.Errorf("expected content encoding 'gzip', got %q", gotHeaders.ContentEncoding)
	}
}

// TestAzure_UpdateMetadata_NotFound tests that a missing blob yields an error
// wrapping common.ErrKeyNotFound.
func TestAzure_UpdateMetadata_NotFound(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setMetadataFn: func(ctx context.Context, metadata map[string]string) error {
					return &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.UpdateMetadata(ctx, "missing", &common.Metadata{ContentType: "text/plain"})
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// TestAzure_UpdateMetadata_HeadersNotFound tests not-found mapping on the
// SetHTTPHeaders call.
func TestAzure_UpdateMetadata_HeadersNotFound(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setHTTPHeadersFn: func(ctx context.Context, headers azblob.BlobHTTPHeaders) error {
					return &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.UpdateMetadata(ctx, "missing", &common.Metadata{ContentType: "text/plain"})
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// TestAzure_UpdateMetadata_NilMetadata tests that nil metadata clears custom
// metadata and headers (replace semantics).
func TestAzure_UpdateMetadata_NilMetadata(t *testing.T) {
	var gotMetadata map[string]string
	var gotHeaders azblob.BlobHTTPHeaders
	setMetadataCalled := false
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setMetadataFn: func(ctx context.Context, metadata map[string]string) error {
					setMetadataCalled = true
					gotMetadata = metadata
					return nil
				},
				setHTTPHeadersFn: func(ctx context.Context, headers azblob.BlobHTTPHeaders) error {
					gotHeaders = headers
					return nil
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.UpdateMetadata(ctx, "key", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !setMetadataCalled {
		t.Fatal("expected SetMetadata to be called")
	}
	if len(gotMetadata) != 0 {
		t.Errorf("expected empty custom metadata, got %v", gotMetadata)
	}
	if gotHeaders.ContentType != "" || gotHeaders.ContentEncoding != "" {
		t.Errorf("expected cleared headers, got %+v", gotHeaders)
	}
}

// TestAzure_Exists tests object existence check
func TestAzure_Exists(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	// The default mockBlob GetProperties succeeds, so the object exists.
	exists, err := a.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatal("expected object to exist")
	}
}

// TestAzure_Exists_NotFound tests that a BlobNotFound error maps to false, nil.
func TestAzure_Exists_NotFound(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return nil, &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	exists, err := a.Exists(ctx, "missing")
	if err != nil {
		t.Fatalf("expected no error for missing blob, got %v", err)
	}
	if exists {
		t.Fatal("expected exists=false for missing blob")
	}
}

// TestAzure_ListWithContext tests context-aware list
func TestAzure_ListWithContext(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		listBlobsFlatFn: func(ctx context.Context, prefix string) ([]string, error) {
			return []string{"file1.txt", "file2.txt"}, nil
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	keys, err := a.ListWithContext(ctx, "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

// TestAzure_ListWithOptions tests paginated list
func TestAzure_ListWithOptions(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	options := &common.ListOptions{
		Prefix:       "",
		MaxResults:   10,
		ContinueFrom: "",
	}

	result, err := a.ListWithOptions(ctx, options)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestAzure_PutWithContext_Error tests error handling
func TestAzure_PutWithContext_Error(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				uploadFn: func(ctx context.Context, r io.Reader) error {
					return errTestPutError
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	err := a.PutWithContext(ctx, "key", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestAzure_ListWithOptions_Error tests list error handling
func TestAzure_ListWithOptions_Error(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		listBlobsFlatFn: func(ctx context.Context, prefix string) ([]string, error) {
			return nil, errTestListError
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	result, err := a.ListWithOptions(ctx, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if result != nil {
		t.Fatalf("expected nil result on error, got %v", result)
	}
}

// TestAzure_ListWithOptions_Pagination tests pagination logic
func TestAzure_ListWithOptions_Pagination(t *testing.T) {
	tests := []struct {
		name          string
		keys          []string
		opts          *common.ListOptions
		expectedCount int
		expectedTrunc bool
		expectedNext  string
	}{
		{
			name:          "nil options uses defaults",
			keys:          []string{"a", "b", "c"},
			opts:          nil,
			expectedCount: 3,
			expectedTrunc: false,
			expectedNext:  "",
		},
		{
			name:          "max results limits output",
			keys:          []string{"a", "b", "c", "d", "e"},
			opts:          &common.ListOptions{MaxResults: 3},
			expectedCount: 3,
			expectedTrunc: true,
			expectedNext:  "c",
		},
		{
			name:          "continue from skips items",
			keys:          []string{"a", "b", "c", "d", "e"},
			opts:          &common.ListOptions{ContinueFrom: "b", MaxResults: 2},
			expectedCount: 2,
			expectedTrunc: true,
			expectedNext:  "d",
		},
		{
			name:          "continue from with remaining items",
			keys:          []string{"a", "b", "c"},
			opts:          &common.ListOptions{ContinueFrom: "a", MaxResults: 10},
			expectedCount: 2,
			expectedTrunc: false,
			expectedNext:  "",
		},
		{
			name:          "zero max results uses default 1000",
			keys:          make([]string, 100),
			opts:          &common.ListOptions{MaxResults: 0},
			expectedCount: 100,
			expectedTrunc: false,
			expectedNext:  "",
		},
		{
			name:          "negative max results uses default 1000",
			keys:          make([]string, 50),
			opts:          &common.ListOptions{MaxResults: -1},
			expectedCount: 50,
			expectedTrunc: false,
			expectedNext:  "",
		},
		{
			name:          "continue from not found starts at beginning",
			keys:          []string{"a", "b", "c"},
			opts:          &common.ListOptions{ContinueFrom: "nonexistent", MaxResults: 2},
			expectedCount: 2,
			expectedTrunc: true,
			expectedNext:  "b",
		},
		{
			name:          "max results larger than list",
			keys:          []string{"a", "b"},
			opts:          &common.ListOptions{MaxResults: 10},
			expectedCount: 2,
			expectedTrunc: false,
			expectedNext:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Populate keys for zero/negative test cases
			if tt.opts != nil && (tt.opts.MaxResults == 0 || tt.opts.MaxResults == -1) && len(tt.keys) > 0 && tt.keys[0] == "" {
				for i := 0; i < len(tt.keys); i++ {
					tt.keys[i] = fmt.Sprintf("key%d", i)
				}
			}

			mockCont := &mockContainerEnhanced{
				listBlobsFlatFn: func(ctx context.Context, prefix string) ([]string, error) {
					return tt.keys, nil
				},
			}

			a := &Azure{container: mockCont}
			ctx := context.Background()

			result, err := a.ListWithOptions(ctx, tt.opts)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if len(result.Objects) != tt.expectedCount {
				t.Errorf("expected %d objects, got %d", tt.expectedCount, len(result.Objects))
			}

			if result.Truncated != tt.expectedTrunc {
				t.Errorf("expected Truncated=%v, got %v", tt.expectedTrunc, result.Truncated)
			}

			if result.NextToken != tt.expectedNext {
				t.Errorf("expected NextToken=%q, got %q", tt.expectedNext, result.NextToken)
			}

			// Verify all objects have the key field populated
			for _, obj := range result.Objects {
				if obj.Key == "" {
					t.Error("object Key should not be empty")
				}
			}
		})
	}
}

// TestAzure_ListWithOptions_WithPrefix tests prefix filtering
func TestAzure_ListWithOptions_WithPrefix(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		listBlobsFlatFn: func(ctx context.Context, prefix string) ([]string, error) {
			if prefix != "test/" {
				t.Errorf("expected prefix 'test/', got %q", prefix)
			}
			return []string{"test/a.txt", "test/b.txt"}, nil
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	opts := &common.ListOptions{
		Prefix:     "test/",
		MaxResults: 10,
	}

	result, err := a.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(result.Objects))
	}
}

func TestAzure_Exists_NonStorageErrorPropagates(t *testing.T) {
	// A plain (non-azblob.StorageError) error must be propagated to the caller.
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return nil, errTestBlobNotFound
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	exists, err := a.Exists(ctx, "missing.txt")
	if err == nil {
		t.Fatal("Exists should propagate non-StorageError errors")
	}
	if exists {
		t.Error("expected exists=false on error")
	}
}
