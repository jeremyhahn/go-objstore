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
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// archiveLocalRequest builds an archive call naming a local destination.
func archiveLocalRequest(t *testing.T, dir string) *Request {
	t.Helper()
	params, err := json.Marshal(ArchiveParams{
		Key:                 "arch-key",
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dir},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &Request{JSONRPC: jsonRPCVersion, Method: MethodArchive, Params: params, ID: 1}
}

func seededStorage() *MockStorage {
	storage := NewMockStorage()
	storage.objects["arch-key"] = []byte("data")
	storage.metadata["arch-key"] = &common.Metadata{Size: 4}
	return storage
}

// TestArchive_LocalDestinationRefusedByDefault: a server built with the
// default config answers invalid params to a client-supplied local
// destination and writes nothing into the directory the client named.
func TestArchive_LocalDestinationRefusedByDefault(t *testing.T) {
	initTestFacade(t, seededStorage())
	dir := t.TempDir()

	srv, err := NewServer(&ServerConfig{SocketPath: filepath.Join(t.TempDir(), "s.sock")})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	resp := srv.handler.Handle(context.Background(), archiveLocalRequest(t, dir))
	if resp.Error == nil {
		t.Fatal("expected an error for a local archive destination")
	}
	if resp.Error.Code != ErrCodeInvalidParams {
		t.Errorf("code = %d, want %d (invalid params)", resp.Error.Code, ErrCodeInvalidParams)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("refused archive wrote %d entries into %s", len(entries), dir)
	}
}

// TestArchive_LocalDestinationAllowedWhenConfigured: with
// AllowClientLocalArchiveDestinations set, the call is not refused.
func TestArchive_LocalDestinationAllowedWhenConfigured(t *testing.T) {
	initTestFacade(t, seededStorage())
	dir := t.TempDir()

	srv, err := NewServer(&ServerConfig{
		SocketPath:                          filepath.Join(t.TempDir(), "s.sock"),
		AllowClientLocalArchiveDestinations: true,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if !srv.handler.wireOpts.AllowLocalDestinations {
		t.Fatal("handler did not receive AllowLocalDestinations")
	}
	resp := srv.handler.Handle(context.Background(), archiveLocalRequest(t, dir))
	if resp.Error != nil {
		t.Fatalf("archive refused with the setting on: %d %s", resp.Error.Code, resp.Error.Message)
	}
}
