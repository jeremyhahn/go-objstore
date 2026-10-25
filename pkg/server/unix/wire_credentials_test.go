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
	"encoding/json"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func archiveOverUnix(t *testing.T, settings map[string]string) *Response {
	t.Helper()
	storage := NewMockStorage()
	storage.objects["test-key"] = []byte("data")
	storage.metadata["test-key"] = &common.Metadata{Size: 4}
	handler := createTestHandler(t, storage)

	paramsJSON, err := json.Marshal(ArchiveParams{Key: "test-key", DestinationType: wireprobe.Backend, DestinationSettings: settings})
	if err != nil {
		t.Fatal(err)
	}
	return handler.Handle(context.Background(), &Request{JSONRPC: jsonRPCVersion, Method: MethodArchive, Params: paramsJSON, ID: 1})
}

func TestArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	resp := archiveOverUnix(t, map[string]string{"accessKey": "AK-unix", "secretKey": "SK-unix", "region": "eu-1"})
	if resp.Error != nil {
		t.Fatalf("archive failed: code %d", resp.Error.Code)
	}
	probe.AssertCredential(t, "AK-unix", "SK-unix", map[string]string{"region": "eu-1"})
}

func TestArchiveRefusesTokenSettings(t *testing.T) {
	probe := wireprobe.Register(t)
	resp := archiveOverUnix(t, map[string]string{"sessionToken": "tok-unix"})
	if resp.Error == nil {
		t.Fatal("a token left in settings must be refused")
	}
	if probe.Calls() != 0 {
		t.Error("a refused settings map must not configure the backend")
	}
	if b, _ := json.Marshal(resp); strings.Contains(string(b), "tok-unix") {
		t.Error("response echoes the token")
	}
}
