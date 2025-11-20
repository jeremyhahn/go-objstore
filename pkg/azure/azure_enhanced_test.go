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
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Test error variables
var (
	errTestPutError     = errors.New("put error")
	errTestListError    = errors.New("list error")
	errTestBlobNotFound = errors.New("blob not found")
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
	uploadFn        func(ctx context.Context, r io.Reader) error
	readFn          func(ctx context.Context) (io.ReadCloser, error)
	deleteFn        func(ctx context.Context) error
	getPropertiesFn func(ctx context.Context) error
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

func (m *mockBlob) GetProperties(ctx context.Context) error {
	if m.getPropertiesFn != nil {
		return m.getPropertiesFn(ctx)
	}
	return nil
}

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

// TestAzure_GetMetadata tests metadata retrieval (stub implementation)
func TestAzure_GetMetadata(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	metadata, err := a.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Stub implementation returns nil
	if metadata != nil {
		t.Fatalf("expected nil metadata (stub), got %v", metadata)
	}
}

// TestAzure_UpdateMetadata tests metadata update (stub implementation)
func TestAzure_UpdateMetadata(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	metadata := &common.Metadata{ContentType: "test"}

	err := a.UpdateMetadata(ctx, "key", metadata)
	if err != nil {
		t.Fatalf("expected nil (stub), got error: %v", err)
	}
}

// TestAzure_Exists tests object existence check
func TestAzure_Exists(t *testing.T) {
	mockCont := &mockContainerEnhanced{}
	a := &Azure{container: mockCont}
	ctx := context.Background()

	// Since GetMetadata returns nil (stub), Exists will return true
	exists, err := a.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatal("expected object to exist (stub returns success)")
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

func TestAzure_Exists_ErrorPath(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) error {
					return errTestBlobNotFound
				},
			}
		},
	}

	a := &Azure{container: mockCont}
	ctx := context.Background()

	exists, err := a.Exists(ctx, "missing.txt")
	if err != nil {
		t.Fatalf("Exists should not return error for missing blob, got: %v", err)
	}
	if exists {
		t.Error("expected exists=false for missing blob")
	}
}
