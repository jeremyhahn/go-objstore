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

// Package metrics provides a small, dependency-free, transport-agnostic metrics
// registry and a Prometheus text-exposition renderer. Every server transport
// (REST, gRPC, QUIC, MCP, Unix) records request outcomes into the shared
// process-wide Default registry; the HTTP-based transports expose it at
// /metrics via Handler. No third-party metrics library is required.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/version"
)

// Transport label values identify which server transport recorded a request.
const (
	TransportREST = "rest"
	TransportGRPC = "grpc"
	TransportQUIC = "quic"
	TransportMCP  = "mcp"
	TransportUnix = "unix"
)

// reqKey identifies a request series by transport and status code.
type reqKey struct {
	transport string
	code      string
}

// reqStat holds the cumulative counters for one (transport, code) series.
type reqStat struct {
	count        uint64
	latencyNanos uint64
}

// Registry is a thread-safe, process-wide collector of request metrics. The
// zero value is not usable; construct one with New.
type Registry struct {
	mu      sync.Mutex
	series  map[reqKey]*reqStat
	start   time.Time
	version string
}

// New creates an empty Registry whose uptime is measured from now.
func New() *Registry {
	return &Registry{
		series:  make(map[reqKey]*reqStat),
		start:   time.Now(),
		version: version.Get(),
	}
}

// Default is the shared registry that all transports record into and that the
// /metrics handler renders.
var Default = New()

// RecordRequest records a single completed request for the given transport with
// the given status code (e.g. an HTTP status, a gRPC code name, or "ok"/"error"
// for the message transports) and its duration. It is safe for concurrent use.
func (r *Registry) RecordRequest(transport, code string, dur time.Duration) {
	key := reqKey{transport: transport, code: code}
	r.mu.Lock()
	stat, ok := r.series[key]
	if !ok {
		stat = &reqStat{}
		r.series[key] = stat
	}
	stat.count++
	if nanos := dur.Nanoseconds(); nanos >= 0 {
		stat.latencyNanos += uint64(nanos)
	}
	r.mu.Unlock()
}

// snapshot returns a stable, sorted copy of the recorded series so rendering
// holds the lock only briefly and produces deterministic output.
func (r *Registry) snapshot() []struct {
	key  reqKey
	stat reqStat
} {
	r.mu.Lock()
	out := make([]struct {
		key  reqKey
		stat reqStat
	}, 0, len(r.series))
	for k, s := range r.series {
		out = append(out, struct {
			key  reqKey
			stat reqStat
		}{key: k, stat: *s})
	}
	r.mu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].key.transport != out[j].key.transport {
			return out[i].key.transport < out[j].key.transport
		}
		return out[i].key.code < out[j].key.code
	})
	return out
}

// WritePrometheus renders the registry as Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Fprintf(w, "# HELP objstore_build_info Build information of the running server.\n")
	fmt.Fprintf(w, "# TYPE objstore_build_info gauge\n")
	fmt.Fprintf(w, "objstore_build_info{version=%q} 1\n", r.version)

	fmt.Fprintf(w, "# HELP objstore_uptime_seconds Seconds since the metrics registry was created.\n")
	fmt.Fprintf(w, "# TYPE objstore_uptime_seconds gauge\n")
	fmt.Fprintf(w, "objstore_uptime_seconds %g\n", time.Since(r.start).Seconds())

	fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines that currently exist.\n")
	fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
	fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
	fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", mem.Alloc)

	fmt.Fprintf(w, "# HELP go_memstats_heap_inuse_bytes Number of heap bytes in in-use spans.\n")
	fmt.Fprintf(w, "# TYPE go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(w, "go_memstats_heap_inuse_bytes %d\n", mem.HeapInuse)

	fmt.Fprintf(w, "# HELP go_memstats_sys_bytes Number of bytes obtained from the OS.\n")
	fmt.Fprintf(w, "# TYPE go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(w, "go_memstats_sys_bytes %d\n", mem.Sys)

	series := r.snapshot()

	fmt.Fprintf(w, "# HELP objstore_requests_total Total server requests by transport and status code.\n")
	fmt.Fprintf(w, "# TYPE objstore_requests_total counter\n")
	for _, s := range series {
		fmt.Fprintf(w, "objstore_requests_total{transport=%q,code=%q} %d\n", s.key.transport, s.key.code, s.stat.count)
	}

	fmt.Fprintf(w, "# HELP objstore_request_duration_seconds_sum Cumulative request duration in seconds by transport and code.\n")
	fmt.Fprintf(w, "# TYPE objstore_request_duration_seconds_sum counter\n")
	for _, s := range series {
		fmt.Fprintf(w, "objstore_request_duration_seconds_sum{transport=%q,code=%q} %g\n",
			s.key.transport, s.key.code, float64(s.stat.latencyNanos)/1e9)
	}
}

// Handler returns an http.Handler that renders the Default registry in
// Prometheus text-exposition format. Mount it at GET /metrics.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		Default.WritePrometheus(w)
	})
}
