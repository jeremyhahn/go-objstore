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

package audit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// recordingAuditLogger captures events for assertions. It embeds the no-op
// logger to satisfy the full AuditLogger interface and overrides LogEvent.
type recordingAuditLogger struct {
	AuditLogger
	mu     sync.Mutex
	events []*AuditEvent
}

func newRecordingAuditLogger() *recordingAuditLogger {
	return &recordingAuditLogger{AuditLogger: NewNoOpAuditLogger()}
}

func (r *recordingAuditLogger) LogEvent(_ context.Context, event *AuditEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recordingAuditLogger) last() *AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) == 0 {
		return nil
	}
	return r.events[len(r.events)-1]
}

func (r *recordingAuditLogger) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func TestAuditHTTPMiddleware(t *testing.T) {
	t.Run("records auditable requests", func(t *testing.T) {
		logger := newRecordingAuditLogger()
		h := AuditHTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}))

		req := httptest.NewRequest(http.MethodPut, "/objects/key1", nil)
		req.RemoteAddr = "10.1.2.3:5555"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		event := logger.last()
		if event == nil {
			t.Fatal("expected an audit event")
		}
		if event.EventType != EventObjectCreated {
			t.Errorf("event type = %v, want %v", event.EventType, EventObjectCreated)
		}
		if event.Result != ResultSuccess {
			t.Errorf("result = %v, want success", event.Result)
		}
		if event.StatusCode != http.StatusCreated {
			t.Errorf("status = %d, want 201", event.StatusCode)
		}
		if event.IPAddress != "10.1.2.3" {
			t.Errorf("ip = %q, want 10.1.2.3", event.IPAddress)
		}
		if event.RequestID == "" {
			t.Error("request ID must be set")
		}
	})

	t.Run("records failures", func(t *testing.T) {
		logger := newRecordingAuditLogger()
		h := AuditHTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusForbidden)
		}))

		req := httptest.NewRequest(http.MethodDelete, "/objects/key1", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		event := logger.last()
		if event == nil {
			t.Fatal("expected an audit event")
		}
		if event.Result != ResultFailure {
			t.Errorf("result = %v, want failure", event.Result)
		}
		if event.EventType != EventObjectDeleted {
			t.Errorf("event type = %v, want %v", event.EventType, EventObjectDeleted)
		}
	})

	t.Run("skips non-auditable paths", func(t *testing.T) {
		logger := newRecordingAuditLogger()
		h := AuditHTTPMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if logger.count() != 0 {
			t.Errorf("health checks must not be audited, got %d events", logger.count())
		}
	})
}

func TestLogRPC(t *testing.T) {
	logger := newRecordingAuditLogger()
	principal := &adapters.Principal{ID: "uid-1000", Name: "alice"}

	LogRPC(context.Background(), logger, "unix", "put", principal, time.Now(), nil)
	event := logger.last()
	if event == nil {
		t.Fatal("expected an audit event")
	}
	if event.EventType != EventObjectCreated {
		t.Errorf("event type = %v, want %v", event.EventType, EventObjectCreated)
	}
	if event.UserID != "uid-1000" || event.Principal != "alice" {
		t.Errorf("principal not recorded: %+v", event)
	}
	if event.Action != "unix put" {
		t.Errorf("action = %q", event.Action)
	}

	LogRPC(context.Background(), logger, "mcp-stdio", "objstore_delete", nil, time.Now(), errors.New("forbidden"))
	event = logger.last()
	if event.Result != ResultFailure || event.ErrorMessage != "forbidden" {
		t.Errorf("failure not recorded: %+v", event)
	}
	if event.EventType != EventObjectDeleted {
		t.Errorf("event type = %v, want %v", event.EventType, EventObjectDeleted)
	}

	// Nil logger is a safe no-op.
	LogRPC(context.Background(), nil, "unix", "get", nil, time.Now(), nil)
}

func TestDetermineRPCEventType(t *testing.T) {
	tests := []struct {
		method string
		want   EventType
	}{
		{"put", EventObjectCreated},
		{"objstore_put", EventObjectCreated},
		{"archive", EventObjectCreated},
		{"delete", EventObjectDeleted},
		{"remove_policy", EventObjectDeleted},
		{"update_metadata", EventObjectMetadataUpdated},
		{"list", EventListObjects},
		{"get_policies", EventPolicyChanged},
		{"trigger_replication", EventPolicyChanged},
		{"get", EventObjectAccessed},
		{"health", EventObjectAccessed},
	}
	for _, tt := range tests {
		if got := determineRPCEventType(tt.method); got != tt.want {
			t.Errorf("determineRPCEventType(%q) = %v, want %v", tt.method, got, tt.want)
		}
	}
}
