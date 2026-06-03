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
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// TestAuthenticationMiddlewareConcurrentNoBadLoggerRace fires many goroutines
// through a single shared gin engine that has AuthenticationMiddleware mounted
// to prove that the per-request logger enrichment no longer mutates the shared
// captured logger variable. Run with -race; must produce zero data-race reports.
func TestAuthenticationMiddlewareConcurrentNoBadLoggerRace(t *testing.T) {
	gin.SetMode(gin.TestMode)

	authenticator := &MockAuthenticator{
		shouldFail: false,
		principal: adapters.Principal{
			ID:   "user1",
			Name: "testuser",
		},
	}
	logger := adapters.NewNoOpLogger()

	// Build the engine once — the middleware closure is shared across all requests,
	// which is exactly the scenario that triggered the race before the fix.
	engine := gin.New()
	engine.Use(AuthenticationMiddleware(authenticator, logger, nil))
	engine.GET("/probe", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/probe", nil)
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
		}()
	}
	wg.Wait()
}
