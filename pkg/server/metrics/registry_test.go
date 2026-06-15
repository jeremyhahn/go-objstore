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

package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecordAndRender(t *testing.T) {
	r := New()
	r.RecordRequest(TransportREST, "200", 10*time.Millisecond)
	r.RecordRequest(TransportREST, "200", 30*time.Millisecond)
	r.RecordRequest(TransportGRPC, "OK", 5*time.Millisecond)
	r.RecordRequest(TransportUnix, "error", 0)

	var sb strings.Builder
	r.WritePrometheus(&sb)
	out := sb.String()

	for _, want := range []string{
		"# TYPE objstore_requests_total counter",
		`objstore_requests_total{transport="rest",code="200"} 2`,
		`objstore_requests_total{transport="grpc",code="OK"} 1`,
		`objstore_requests_total{transport="unix",code="error"} 1`,
		`objstore_request_duration_seconds_sum{transport="rest",code="200"} 0.04`,
		"go_goroutines ",
		"objstore_build_info{version=",
		"go_memstats_alloc_bytes ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

func TestHandler(t *testing.T) {
	Default.RecordRequest(TransportMCP, "200", time.Millisecond)
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain...", ct)
	}
}

func TestRecordRequestConcurrent(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.RecordRequest(TransportQUIC, "200", time.Microsecond)
			}
		}()
	}
	wg.Wait()

	var sb strings.Builder
	r.WritePrometheus(&sb)
	if !strings.Contains(sb.String(), `objstore_requests_total{transport="quic",code="200"} 5000`) {
		t.Errorf("expected 5000 quic requests, got:\n%s", sb.String())
	}
}
