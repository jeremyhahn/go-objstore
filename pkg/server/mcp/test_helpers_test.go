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
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// initTestFacade initializes the objstore facade with a mock storage for testing.
// This must be called before creating tool executors or resource managers.
func initTestFacade(t *testing.T, storage common.Storage) {
	t.Helper()
	objstore.Reset()
	err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}
}

// createTestToolExecutor creates a ToolExecutor for testing after setting up the facade.
func createTestToolExecutor(t *testing.T, storage common.Storage) *ToolExecutor {
	t.Helper()
	initTestFacade(t, storage)
	return NewToolExecutor("")
}

// createTestResourceManager creates a ResourceManager for testing after setting up the facade.
func createTestResourceManager(t *testing.T, storage common.Storage, prefix string) *ResourceManager {
	t.Helper()
	initTestFacade(t, storage)
	return NewResourceManager("", prefix)
}

// createTestServer creates an MCP Server for testing after setting up the facade.
func createTestServer(t *testing.T, storage common.Storage, mode ServerMode) *Server {
	t.Helper()
	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{
		Mode:    mode,
		Backend: "",
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	return server
}
