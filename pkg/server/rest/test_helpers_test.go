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
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// initTestFacade initializes the objstore facade with a mock storage for testing.
// This must be called before creating handlers.
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

// newTestHandler creates a handler for testing after setting up the facade.
func newTestHandler(t *testing.T, storage common.Storage) *Handler {
	t.Helper()
	initTestFacade(t, storage)
	handler, err := NewHandler("")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	return handler
}
