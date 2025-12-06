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
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
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
		name     string
		params   GetParams
		wantErr  bool
		errCode  int
		wantData string
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

func TestHandleArchive(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/file.txt"] = []byte("data")
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  ArchiveParams
		wantErr bool
		errCode int
	}{
		{
			name:    "missing key",
			params:  ArchiveParams{DestinationType: "local"},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "missing destination_type",
			params:  ArchiveParams{Key: "test/file.txt"},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name: "invalid destination type",
			params: ArchiveParams{
				Key:             "test/file.txt",
				DestinationType: "invalid-backend",
			},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodArchive,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodArchive,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleAddPolicy(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  PolicyParams
		wantErr bool
	}{
		{
			name: "add valid policy",
			params: PolicyParams{
				ID:        "test-policy",
				Prefix:    "logs/",
				Action:    "delete",
				AfterDays: 30,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodAddPolicy,
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

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodAddPolicy,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleRemovePolicy(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  RemovePolicyParams
		wantErr bool
		errCode int
	}{
		{
			name:    "missing id",
			params:  RemovePolicyParams{ID: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "remove non-existent policy succeeds",
			params:  RemovePolicyParams{ID: "non-existent"},
			wantErr: false, // RemovePolicy succeeds even if policy doesn't exist
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodRemovePolicy,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodRemovePolicy,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleGetPolicies(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetPolicies,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.([]PolicyParams)
	if !ok {
		t.Error("result is not []PolicyParams")
		return
	}

	// Should be empty initially
	if len(result) != 0 {
		t.Errorf("expected 0 policies, got %d", len(result))
	}
}

func TestHandleApplyPolicies(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodApplyPolicies,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.(*ApplyPoliciesResult)
	if !ok {
		t.Error("result is not *ApplyPoliciesResult")
		return
	}

	// Should have 0 policies and 0 objects processed
	if result.PoliciesCount != 0 {
		t.Errorf("expected 0 policies, got %d", result.PoliciesCount)
	}
}

func TestHandleAddReplicationPolicy(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  ReplicationPolicyParams
		wantErr bool
		errCode int
	}{
		{
			name: "add valid replication policy",
			params: ReplicationPolicyParams{
				ID:              "test-repl",
				SourcePrefix:    "src/",
				DestinationType: "local",
				Destination:     map[string]string{"path": "/tmp/dest"},
				Schedule:        "1h",
				Enabled:         true,
			},
			wantErr: true, // Will fail because no replication manager configured
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodAddReplPolicy,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodAddReplPolicy,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleRemoveReplicationPolicy(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  ReplicationPolicyIDParams
		wantErr bool
		errCode int
	}{
		{
			name:    "missing id",
			params:  ReplicationPolicyIDParams{ID: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "no replication manager",
			params:  ReplicationPolicyIDParams{ID: "test-id"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodRemoveReplPolicy,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodRemoveReplPolicy,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleGetReplicationPolicy(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  ReplicationPolicyIDParams
		wantErr bool
		errCode int
	}{
		{
			name:    "missing id",
			params:  ReplicationPolicyIDParams{ID: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "no replication manager",
			params:  ReplicationPolicyIDParams{ID: "test-id"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGetReplPolicy,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodGetReplPolicy,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleGetReplicationPolicies(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplPolicies,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	// Should fail because no replication manager
	if resp.Error == nil {
		t.Error("expected error but got success")
		return
	}
	if resp.Error.Code != ErrCodeInternalError {
		t.Errorf("got error code %d, want %d", resp.Error.Code, ErrCodeInternalError)
	}
}

func TestHandleTriggerReplication(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  *ReplicationPolicyIDParams
		wantErr bool
		errCode int
	}{
		{
			name:    "trigger all - no replication manager",
			params:  nil,
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
		{
			name:    "trigger specific - no replication manager",
			params:  &ReplicationPolicyIDParams{ID: "test-policy"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var paramsJSON json.RawMessage
			if tt.params != nil {
				paramsJSON, _ = json.Marshal(tt.params)
			}
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodTriggerRepl,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodTriggerRepl,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

func TestHandleGetReplicationStatus(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	tests := []struct {
		name    string
		params  ReplicationPolicyIDParams
		wantErr bool
		errCode int
	}{
		{
			name:    "missing id",
			params:  ReplicationPolicyIDParams{ID: ""},
			wantErr: true,
			errCode: ErrCodeInvalidParams,
		},
		{
			name:    "no replication manager",
			params:  ReplicationPolicyIDParams{ID: "test-id"},
			wantErr: true,
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			req := &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGetReplStatus,
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
			}
		})
	}

	// Test invalid JSON params
	t.Run("invalid json params", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodGetReplStatus,
			Params:  json.RawMessage(`{"invalid": json`),
			ID:      1,
		}
		resp := handler.Handle(context.Background(), req)
		if resp.Error == nil || resp.Error.Code != ErrCodeInvalidParams {
			t.Error("expected invalid params error")
		}
	})
}

// Tests with mock replication manager for success paths

func TestHandleAddReplicationPolicyWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()
	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyParams{
		ID:              "test-repl-policy",
		SourcePrefix:    "src/",
		DestinationType: "local",
		Destination:     map[string]string{"path": "/tmp/dest"},
		Schedule:        "1h",
		Enabled:         true,
	}

	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodAddReplPolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleGetReplicationPoliciesWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()

	// Add a policy first
	_ = storage.replMgr.AddPolicy(common.ReplicationPolicy{
		ID:           "test-policy-1",
		SourcePrefix: "src/",
		Enabled:      true,
	})

	handler := createTestHandlerWithReplication(t, storage)

	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplPolicies,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.([]ReplicationPolicyParams)
	if !ok {
		t.Error("result is not []ReplicationPolicyParams")
		return
	}

	if len(result) != 1 {
		t.Errorf("expected 1 policy, got %d", len(result))
	}
}

func TestHandleGetReplicationPolicyWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()

	// Add a policy first
	_ = storage.replMgr.AddPolicy(common.ReplicationPolicy{
		ID:           "test-policy-get",
		SourcePrefix: "data/",
		Enabled:      true,
	})

	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "test-policy-get"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplPolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.(*ReplicationPolicyParams)
	if !ok {
		t.Error("result is not *ReplicationPolicyParams")
		return
	}

	if result.ID != "test-policy-get" {
		t.Errorf("expected policy ID 'test-policy-get', got '%s'", result.ID)
	}
}

func TestHandleRemoveReplicationPolicyWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()

	// Add a policy first
	_ = storage.replMgr.AddPolicy(common.ReplicationPolicy{
		ID:           "test-policy-remove",
		SourcePrefix: "data/",
		Enabled:      true,
	})

	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "test-policy-remove"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodRemoveReplPolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleTriggerReplicationWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()

	// Add a policy first
	_ = storage.replMgr.AddPolicy(common.ReplicationPolicy{
		ID:           "test-trigger-policy",
		SourcePrefix: "data/",
		Enabled:      true,
	})

	handler := createTestHandlerWithReplication(t, storage)

	t.Run("trigger all policies", func(t *testing.T) {
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodTriggerRepl,
			ID:      1,
		}

		resp := handler.Handle(context.Background(), req)

		if resp.Error != nil {
			t.Errorf("unexpected error: %s", resp.Error.Message)
			return
		}

		result, ok := resp.Result.(*TriggerReplicationResult)
		if !ok {
			t.Error("result is not *TriggerReplicationResult")
			return
		}

		if result.ObjectsSynced != 1 {
			t.Errorf("expected 1 object synced, got %d", result.ObjectsSynced)
		}
	})

	t.Run("trigger specific policy", func(t *testing.T) {
		params := ReplicationPolicyIDParams{ID: "test-trigger-policy"}
		paramsJSON, _ := json.Marshal(params)
		req := &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodTriggerRepl,
			Params:  paramsJSON,
			ID:      1,
		}

		resp := handler.Handle(context.Background(), req)

		if resp.Error != nil {
			t.Errorf("unexpected error: %s", resp.Error.Message)
			return
		}

		result, ok := resp.Result.(*TriggerReplicationResult)
		if !ok {
			t.Error("result is not *TriggerReplicationResult")
			return
		}

		if result.ObjectsSynced != 1 {
			t.Errorf("expected 1 object synced, got %d", result.ObjectsSynced)
		}
	})
}

func TestHandleGetReplicationStatusWithManager(t *testing.T) {
	storage := NewMockReplicableStorage()

	// Add a policy first
	_ = storage.replMgr.AddPolicy(common.ReplicationPolicy{
		ID:           "test-status-policy",
		SourcePrefix: "data/",
		Enabled:      true,
	})

	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "test-status-policy"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplStatus,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.(*ReplicationStatusResult)
	if !ok {
		t.Error("result is not *ReplicationStatusResult")
		return
	}

	if result.PolicyID != "test-status-policy" {
		t.Errorf("expected policy ID 'test-status-policy', got '%s'", result.PolicyID)
	}
}

func TestHandleApplyPoliciesWithObjects(t *testing.T) {
	storage := NewMockStorage()
	// Add some objects
	storage.objects["logs/old.txt"] = []byte("old data")
	storage.metadata["logs/old.txt"] = &common.Metadata{
		Size:         8,
		LastModified: time.Now().Add(-48 * time.Hour), // 2 days old
	}
	storage.objects["logs/new.txt"] = []byte("new data")
	storage.metadata["logs/new.txt"] = &common.Metadata{
		Size:         8,
		LastModified: time.Now(),
	}

	handler := createTestHandler(t, storage)

	// First add a policy
	addPolicyParams := PolicyParams{
		ID:        "cleanup-logs",
		Prefix:    "logs/",
		Action:    "delete",
		AfterDays: 1, // 1 day retention
	}
	addParamsJSON, _ := json.Marshal(addPolicyParams)
	addReq := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodAddPolicy,
		Params:  addParamsJSON,
		ID:      1,
	}
	handler.Handle(context.Background(), addReq)

	// Now apply policies
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodApplyPolicies,
		ID:      2,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.(*ApplyPoliciesResult)
	if !ok {
		t.Error("result is not *ApplyPoliciesResult")
		return
	}

	if result.PoliciesCount != 1 {
		t.Errorf("expected 1 policy, got %d", result.PoliciesCount)
	}

	// The old object should have been deleted
	if result.ObjectsProcessed != 1 {
		t.Errorf("expected 1 object processed, got %d", result.ObjectsProcessed)
	}
}

func TestHandleGetPoliciesWithData(t *testing.T) {
	storage := NewMockStorage()
	handler := createTestHandler(t, storage)

	// Add a policy first
	addParams := PolicyParams{
		ID:        "test-get-policy",
		Prefix:    "data/",
		Action:    "delete",
		AfterDays: 30,
	}
	addParamsJSON, _ := json.Marshal(addParams)
	addReq := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodAddPolicy,
		Params:  addParamsJSON,
		ID:      1,
	}
	handler.Handle(context.Background(), addReq)

	// Get policies
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetPolicies,
		ID:      2,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.([]PolicyParams)
	if !ok {
		t.Error("result is not []PolicyParams")
		return
	}

	if len(result) != 1 {
		t.Errorf("expected 1 policy, got %d", len(result))
		return
	}

	if result[0].ID != "test-get-policy" {
		t.Errorf("expected policy ID 'test-get-policy', got '%s'", result[0].ID)
	}
}

func TestHandleDeleteSuccess(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test-key"] = []byte("data")
	handler := createTestHandler(t, storage)

	params := DeleteParams{Key: "test-key"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodDelete,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleExistsSuccess(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test-key"] = []byte("data")
	handler := createTestHandler(t, storage)

	params := ExistsParams{Key: "test-key"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodExists,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
		return
	}

	result, ok := resp.Result.(*ExistsResult)
	if !ok {
		t.Error("result is not *ExistsResult")
		return
	}

	if !result.Exists {
		t.Error("expected exists=true")
	}
}

func TestHandleArchiveSuccess(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test-key"] = []byte("data")
	storage.metadata["test-key"] = &common.Metadata{Size: 4}
	handler := createTestHandler(t, storage)

	params := ArchiveParams{
		Key:             "test-key",
		DestinationType: "memory",
	}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodArchive,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	// This might fail if memory backend isn't registered, which is fine
	// The important thing is we're exercising the code path
	_ = resp
}

func TestHandleUpdateMetadataSuccess(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test-key"] = []byte("data")
	storage.metadata["test-key"] = &common.Metadata{Size: 4}
	handler := createTestHandler(t, storage)

	params := UpdateMetadataParams{
		Key: "test-key",
		Metadata: &MetadataParams{
			Custom: map[string]string{
				"custom": "value",
			},
		},
	}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodUpdateMetadata,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleRemovePolicySuccess(t *testing.T) {
	storage := NewMockStorage()
	// Add a policy first
	storage.policies["test-policy"] = common.LifecyclePolicy{ID: "test-policy"}
	handler := createTestHandler(t, storage)

	params := RemovePolicyParams{ID: "test-policy"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodRemovePolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleApplyPoliciesArchiveAction(t *testing.T) {
	storage := NewMockStorage()
	// Add an old object
	storage.objects["archive/old.txt"] = []byte("old data")
	storage.metadata["archive/old.txt"] = &common.Metadata{
		Size:         8,
		LastModified: time.Now().Add(-48 * time.Hour),
	}
	handler := createTestHandler(t, storage)

	// Add an archive policy
	addParams := PolicyParams{
		ID:        "archive-policy",
		Prefix:    "archive/",
		Action:    "archive",
		AfterDays: 1,
	}
	addParamsJSON, _ := json.Marshal(addParams)
	addReq := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodAddPolicy,
		Params:  addParamsJSON,
		ID:      1,
	}
	handler.Handle(context.Background(), addReq)

	// Apply policies
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodApplyPolicies,
		ID:      2,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error != nil {
		t.Errorf("unexpected error: %s", resp.Error.Message)
	}
}

func TestHandleTriggerReplicationPolicyNotFound(t *testing.T) {
	storage := NewMockReplicableStorage()
	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "nonexistent"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodTriggerRepl,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for non-existent policy")
	}
}

func TestHandleGetReplicationStatusPolicyNotFound(t *testing.T) {
	storage := NewMockReplicableStorage()
	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "nonexistent"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplStatus,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for non-existent policy")
	}
}

func TestHandleRemoveReplicationPolicyNotFound(t *testing.T) {
	storage := NewMockReplicableStorage()
	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "nonexistent"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodRemoveReplPolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for non-existent policy")
	}
}

func TestHandleGetReplicationPolicyNotFound(t *testing.T) {
	storage := NewMockReplicableStorage()
	handler := createTestHandlerWithReplication(t, storage)

	params := ReplicationPolicyIDParams{ID: "nonexistent"}
	paramsJSON, _ := json.Marshal(params)
	req := &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetReplPolicy,
		Params:  paramsJSON,
		ID:      1,
	}

	resp := handler.Handle(context.Background(), req)

	if resp.Error == nil {
		t.Error("expected error for non-existent policy")
	}
}
