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

package quic

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// TestServeHTTPConcurrentNoBadLoggerRace fires many goroutines through a single
// shared Handler.ServeHTTP to prove that the per-request logger enrichment no
// longer mutates the shared h.logger field. Run with -race; must produce zero
// data-race reports.
//
// The local filesystem backend is used because it is safe for concurrent
// access, unlike the in-memory test mocks that use unsynchronized maps.
// Each goroutine issues a GET /health request, which exercises the
// authentication and per-request logger enrichment paths in ServeHTTP without
// touching the backend, keeping the test focused on the logger race.
func TestServeHTTPConcurrentNoBadLoggerRace(t *testing.T) {
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}); err != nil {
		t.Fatalf("storage.Configure: %v", err)
	}
	initTestFacade(t, storage)

	handler, err := NewHandler(
		"",
		100*1024*1024,
		30*time.Second,
		30*time.Second,
		adapters.NewNoOpLogger(),
		adapters.NewNoOpAuthenticator(),
		adapters.NewNoOpAuthorizer(),
		nil,
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	// GET /objects/<key> on a non-existent object goes through authentication
	// and logger enrichment and then returns 404 — sufficient to trigger the
	// race if h.logger were still written concurrently.
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/objects/no-such-key", nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
		}()
	}
	wg.Wait()
}
