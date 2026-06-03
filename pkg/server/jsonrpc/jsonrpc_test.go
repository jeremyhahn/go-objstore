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

package jsonrpc

import (
	"encoding/json"
	"testing"
)

func TestParseRequestValid(t *testing.T) {
	req, errResp := ParseRequest([]byte(`{"jsonrpc":"2.0","method":"get","params":{"key":"k"},"id":1}`))
	if errResp != nil {
		t.Fatalf("unexpected error response: %+v", errResp.Error)
	}
	if req.Method != "get" {
		t.Errorf("method = %q, want %q", req.Method, "get")
	}
	if req.ID != float64(1) {
		t.Errorf("id = %v, want 1", req.ID)
	}
}

func TestParseRequestInvalidJSON(t *testing.T) {
	req, errResp := ParseRequest([]byte(`{not json`))
	if req != nil {
		t.Error("request should be nil for invalid JSON")
	}
	if errResp == nil || errResp.Error == nil || errResp.Error.Code != CodeParseError {
		t.Errorf("expected parse error response, got %+v", errResp)
	}
}

func TestParseRequestWrongVersion(t *testing.T) {
	req, errResp := ParseRequest([]byte(`{"jsonrpc":"1.0","method":"get","id":7}`))
	if req != nil {
		t.Error("request should be nil for wrong version")
	}
	if errResp == nil || errResp.Error == nil || errResp.Error.Code != CodeInvalidRequest {
		t.Errorf("expected invalid-request response, got %+v", errResp)
	}
	if errResp.ID != float64(7) {
		t.Errorf("error response should echo the request id, got %v", errResp.ID)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	resp := NewResponse(42, map[string]string{"status": "ok"})
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Response
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.JSONRPC != Version {
		t.Errorf("version = %q, want %q", decoded.JSONRPC, Version)
	}
	if decoded.Error != nil {
		t.Error("success response must not carry an error")
	}
}

func TestNewError(t *testing.T) {
	resp := NewError("abc", CodeForbidden, "forbidden")
	if resp.Error == nil || resp.Error.Code != CodeForbidden || resp.Error.Message != "forbidden" {
		t.Errorf("unexpected error response: %+v", resp.Error)
	}
	if resp.ID != "abc" {
		t.Errorf("id = %v, want abc", resp.ID)
	}
}
