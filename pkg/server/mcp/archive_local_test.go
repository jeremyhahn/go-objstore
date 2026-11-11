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

package mcp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	servererrors "github.com/jeremyhahn/go-objstore/pkg/server/errors"
)

// allowLocalArchive is the wire option of a server that allows client-supplied
// local archive destinations; tests that archive to local on purpose use it.
var allowLocalArchive = factory.WireOptions{AllowLocalDestinations: true}

func TestArchiveToolRefusesClientLocalDestinationByDefault(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
		t.Fatalf("configure storage: %v", err)
	}
	if err := storage.Put("obj.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	executor := createTestToolExecutor(t, storage)
	dest := filepath.Join(t.TempDir(), "outside")

	_, err := executor.Execute(context.Background(), "objstore_archive", map[string]any{
		"key":                  "obj.txt",
		"destination_type":     "local",
		"destination_settings": map[string]any{"path": dest},
	})
	if !errors.Is(err, factory.ErrClientLocalArchiver) {
		t.Fatalf("archive: err = %v, want ErrClientLocalArchiver", err)
	}

	_, err = executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                   "p1",
		"action":               "archive",
		"retention_seconds":    float64(60),
		"destination_type":     "local",
		"destination_settings": map[string]any{"path": dest},
	})
	if !errors.Is(err, factory.ErrClientLocalArchiver) {
		t.Fatalf("add policy: err = %v, want ErrClientLocalArchiver", err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("refused calls must not create %s (stat: %v)", dest, statErr)
	}
}

// TestArchiveToolRefusalMapsToInvalidParams checks that the refusal reaches an
// MCP client as the JSON-RPC invalid-params code: handleToolsCall maps a tool
// error through servererrors.JSONRPCError (#236).
func TestArchiveToolRefusalMapsToInvalidParams(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
		t.Fatalf("configure storage: %v", err)
	}
	if err := storage.Put("obj.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_archive", map[string]any{
		"key":                  "obj.txt",
		"destination_type":     "local",
		"destination_settings": map[string]any{"path": filepath.Join(t.TempDir(), "outside")},
	})
	if err == nil {
		t.Fatal("archive to local must be refused by default")
	}
	code, _ := servererrors.JSONRPCError(err)
	if int64(code) != int64(ErrCodeInvalidParams) {
		t.Fatalf("JSON-RPC code = %d, want %d (invalid params)", code, ErrCodeInvalidParams)
	}
}

func TestNewServerPassesAllowClientLocalArchiveDestinations(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.AllowClientLocalArchiveDestinations = true
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
		t.Fatalf("configure storage: %v", err)
	}
	initTestFacade(t, storage)
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if !srv.toolExecutor.wireOpts.AllowLocalDestinations {
		t.Fatal("the setting must reach the tool executor")
	}
}
