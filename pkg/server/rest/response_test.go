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

package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestRespondWithError(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		message  string
		wantCode int
		wantErr  string
	}{
		{
			name:     "bad request error",
			code:     http.StatusBadRequest,
			message:  "invalid input",
			wantCode: http.StatusBadRequest,
			wantErr:  "Bad Request",
		},
		{
			name:     "not found error",
			code:     http.StatusNotFound,
			message:  "object not found",
			wantCode: http.StatusNotFound,
			wantErr:  "Not Found",
		},
		{
			name:     "internal server error",
			code:     http.StatusInternalServerError,
			message:  "something went wrong",
			wantCode: http.StatusInternalServerError,
			wantErr:  "Internal Server Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			RespondWithError(c, tt.code, tt.message)

			if w.Code != tt.wantCode {
				t.Errorf("RespondWithError() status = %v, want %v", w.Code, tt.wantCode)
			}

			if !contains(w.Body.String(), tt.wantErr) {
				t.Errorf("RespondWithError() body = %v, want to contain %v", w.Body.String(), tt.wantErr)
			}

			if !contains(w.Body.String(), tt.message) {
				t.Errorf("RespondWithError() body = %v, want to contain %v", w.Body.String(), tt.message)
			}
		})
	}
}

func TestRespondWithSuccess(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		message  string
		data     any
		wantCode int
	}{
		{
			name:     "success with data",
			code:     http.StatusOK,
			message:  "operation successful",
			data:     map[string]string{"key": "value"},
			wantCode: http.StatusOK,
		},
		{
			name:     "success without data",
			code:     http.StatusCreated,
			message:  "resource created",
			data:     nil,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			RespondWithSuccess(c, tt.code, tt.message, tt.data)

			if w.Code != tt.wantCode {
				t.Errorf("RespondWithSuccess() status = %v, want %v", w.Code, tt.wantCode)
			}

			if !contains(w.Body.String(), tt.message) {
				t.Errorf("RespondWithSuccess() body = %v, want to contain %v", w.Body.String(), tt.message)
			}
		})
	}
}

func TestRespondWithObject(t *testing.T) {
	now := time.Now()
	metadata := &common.Metadata{
		Size:         1024,
		ETag:         "abc123",
		LastModified: now,
		Custom: map[string]string{
			"author": "test",
		},
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithObject(c, "test/key", metadata)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithObject() status = %v, want %v", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "test/key") {
		t.Errorf("RespondWithObject() body should contain key")
	}
	if !contains(body, "1024") {
		t.Errorf("RespondWithObject() body should contain size")
	}
	if !contains(body, "abc123") {
		t.Errorf("RespondWithObject() body should contain etag")
	}
}

func TestRespondWithListObjects(t *testing.T) {
	result := &common.ListResult{
		Objects: []*common.ObjectInfo{
			{
				Key: "obj1",
				Metadata: &common.Metadata{
					Size: 100,
					ETag: "etag1",
				},
			},
			{
				Key: "obj2",
				Metadata: &common.Metadata{
					Size: 200,
					ETag: "etag2",
				},
			},
		},
		CommonPrefixes: []string{"prefix1/", "prefix2/"},
		NextToken:      "token123",
		Truncated:      true,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithListObjects(c, result)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithListObjects() status = %v, want %v", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "obj1") {
		t.Errorf("RespondWithListObjects() body should contain obj1")
	}
	if !contains(body, "obj2") {
		t.Errorf("RespondWithListObjects() body should contain obj2")
	}
	if !contains(body, "token123") {
		t.Errorf("RespondWithListObjects() body should contain token")
	}
	if !contains(body, "true") {
		t.Errorf("RespondWithListObjects() body should contain truncated flag")
	}
}

func TestRespondWithListObjectsEmpty(t *testing.T) {
	result := &common.ListResult{
		Objects:        []*common.ObjectInfo{},
		CommonPrefixes: []string{},
		NextToken:      "",
		Truncated:      false,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithListObjects(c, result)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithListObjects() status = %v, want %v", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "[]") {
		t.Errorf("RespondWithListObjects() body should contain empty array")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
