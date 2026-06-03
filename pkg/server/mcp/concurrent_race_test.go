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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// TestAuthMiddlewareConcurrentNoBadLoggerRace fires many goroutines through the
// shared authenticationMiddleware to prove that the per-request logger
// enrichment no longer mutates the shared s.config.Logger field. Run with
// -race; must produce zero data-race reports.
func TestAuthMiddlewareConcurrentNoBadLoggerRace(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	server.config.Authenticator = adapters.NewNoOpAuthenticator()
	server.config.Authorizer = adapters.NewNoOpAuthorizer()

	handler := server.authenticationMiddleware(NewHTTPHandler(server))

	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
		}()
	}
	wg.Wait()
}
