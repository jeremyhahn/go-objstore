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

package unix

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestHandlePut(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name       string
		params     PutParams
		wantErr    bool
		errCode    int
		errContain string
	}{
		{
			name: "successful put",
			params: PutParams{
				Key:  "test/file.txt",
				Data: base64.StdEncoding.EncodeToString([]byte("Hello World")),
			},
			wantErr: false,
		},
		{
			name: "put with metadata",
			params: PutParams{
				Key:  "test/file2.txt",
				Data: base64.StdEncoding.EncodeToString([]byte("Content")),
				Metadata: &MetadataParams{
					ContentType: "text/plain",
					Custom:      map[string]string{"author": "test"},
				},
			},
			wantErr: false,
		},
		{
			name: "missing key",
			params: PutParams{
				Data: base64.StdEncoding.EncodeToString([]byte("data")),
			},
			wantErr:    true,
			errCode:    ErrCodeInvalidParams,
			errContain: "key is required",
		},
		{
			name: "invalid base64 data",
			params: PutParams{
				Key:  "test/invalid.txt",
				Data: "not-valid-base64!!!",
			},
			wantErr:    true,
			errCode:    ErrCodeInvalidParams,
			errContain: "invalid base64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodPut,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
					return
				}
				if resp.Error.Code != tt.errCode {
					t.Errorf("got error code %d, want %d", resp.Error.Code, tt.errCode)
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
				}
				if resp.Result == nil {
					t.Error("expected result but got nil")
				}
			}
		})
	}
}

func TestHandleGet(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/existing.txt"] = []byte("Hello World")
	storage.metadata["test/existing.txt"] = nil

	handler := createTestHandler(t, storage)

	tests := []struct {
		name       string
		params     GetParams
		wantErr    bool
		errCode    int
		wantData   string
	}{
		{
			name:     "successful get",
			params:   GetParams{Key: "test/existing.txt"},
			wantErr:  false,
			wantData: "Hello World",
		},
		{
			name:    "missing key parameter",
			params:  GetParams{Key: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "non-existent key",
			params:  GetParams{Key: "test/nonexistent.txt"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGet,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
					return
				}
				if resp.Error.Code != tt.errCode {
					t.Errorf("got error code %d, want %d", resp.Error.Code, tt.errCode)
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
					return
				}
				result, ok := resp.Result.(*GetResult)
				if !ok {
					t.Error("result is not a GetResult")
					return
				}
				decoded, _ := base64.StdEncoding.DecodeString(result.Data)
				if string(decoded) != tt.wantData {
					t.Errorf("got data %q, want %q", string(decoded), tt.wantData)
				}
			}
		})
	}
}

func TestHandleDelete(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/to-delete.txt"] = []byte("data")

	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  DeleteParams
		wantErr bool
		errCode int
	}{
		{
			name:    "successful delete",
			params:  DeleteParams{Key: "test/to-delete.txt"},
			wantErr: false,
		},
		{
			name:    "missing key",
			params:  DeleteParams{Key: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodDelete,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
				}
			}
		})
	}
}

func TestHandleExists(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/exists.txt"] = []byte("data")

	handler := createTestHandler(t, storage)

	tests := []struct {
		name       string
		params     ExistsParams
		wantErr    bool
		wantExists bool
	}{
		{
			name:       "existing key",
			params:     ExistsParams{Key: "test/exists.txt"},
			wantErr:    false,
			wantExists: true,
		},
		{
			name:       "non-existing key",
			params:     ExistsParams{Key: "test/nonexistent.txt"},
			wantErr:    false,
			wantExists: false,
		},
		{
			name:    "missing key parameter",
			params:  ExistsParams{Key: ""},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodExists,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
					return
				}
				result, ok := resp.Result.(*ExistsResult)
				if !ok {
					t.Error("result is not an ExistsResult")
					return
				}
				if result.Exists != tt.wantExists {
					t.Errorf("got exists %v, want %v", result.Exists, tt.wantExists)
				}
			}
		})
	}
}

func TestHandleList(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/file1.txt"] = []byte("data1")
	storage.objects["test/file2.txt"] = []byte("data2")
	storage.objects["other/file3.txt"] = []byte("data3")

	handler := createTestHandler(t, storage)

	tests := []struct {
		name      string
		params    ListParams
		wantErr   bool
		wantCount int
	}{
		{
			name:      "list all",
			params:    ListParams{},
			wantErr:   false,
			wantCount: 3,
		},
		{
			name:      "list with prefix",
			params:    ListParams{Prefix: "test/"},
			wantErr:   false,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodList,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
					return
				}
				result, ok := resp.Result.(*ListResult)
				if !ok {
					t.Error("result is not a ListResult")
					return
				}
				if len(result.Objects) != tt.wantCount {
					t.Errorf("got %d objects, want %d", len(result.Objects), tt.wantCount)
				}
			}
		})
	}
}

func TestHandleHealth(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	methods := []string{MethodHealth, MethodPing}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  method,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if resp.Error != nil {
				t.Errorf("unexpected error: %s", resp.Error.Message)
				return
			}

			result, ok := resp.Result.(*HealthResult)
			if !ok {
				t.Error("result is not a HealthResult")
				return
			}
			if result.Status != "ok" {
				t.Errorf("got status %q, want %q", result.Status, "ok")
			}
		})
	}
}

func TestHandleMethodNotFound(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  "unknown_method",
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for unknown method")
		return
	}

	if resp.Error.Code != ErrCodeMethodNotFound {
		t.Errorf("got error code %d, want %d", resp.Error.Code, ErrCodeMethodNotFound)
	}
}

func TestHandleInvalidParams(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	// Send invalid JSON params
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodPut,
		Params:  json.RawMessage(`{"invalid": json`),
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for invalid params")
		return
	}

	if resp.Error.Code != ErrCodeInvalidParams {
		t.Errorf("got error code %d, want %d", resp.Error.Code, ErrCodeInvalidParams)
	}
}

func TestHandleGetMetadata(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/file.txt"] = []byte("data")
	storage.metadata["test/file.txt"] = nil

	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  GetMetadataParams
		wantErr bool
		errCode int
	}{
		{
			name:    "get existing metadata",
			params:  GetMetadataParams{Key: "test/file.txt"},
			wantErr: false,
		},
		{
			name:    "missing key",
			params:  GetMetadataParams{Key: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "non-existent key",
			params:  GetMetadataParams{Key: "test/nonexistent.txt"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGetMetadata,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
				}
			}
		})
	}
}

func TestHandleUpdateMetadata(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/file.txt"] = []byte("data")
	storage.metadata["test/file.txt"] = nil

	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  UpdateMetadataParams
		wantErr bool
		errCode int
	}{
		{
			name: "update metadata",
			params: UpdateMetadataParams{
				Key: "test/file.txt",
				Metadata: &MetadataParams{
					ContentType: "text/plain",
				},
			},
			wantErr: false,
		},
		{
			name: "missing key",
			params: UpdateMetadataParams{
				Metadata: &MetadataParams{ContentType: "text/plain"},
			},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodUpdateMetadata,
				Params:  paramsJSON,
				ID:      1,
			}

			resp := handler.Handle(context.Background(), req)

			if tt.wantErr {
				if resp.Error == nil {
					t.Errorf("expected error but got success")
				}
			} else {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
				}
			}
		})
	}
}

func TestKeyRef(t *testing.T) {
	tests := []struct {
		backend string
		key     string
		want    string
	}{
		{"", "file.txt", "file.txt"},
		{"default", "file.txt", "default:file.txt"},
		{"s3", "path/to/file.txt", "s3:path/to/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.backend+":"+tt.key, func(t *testing.T) {
			handler := &Handler{backend: tt.backend}
			got := handler.keyRef(tt.key)
			if got != tt.want {
				t.Errorf("keyRef(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
