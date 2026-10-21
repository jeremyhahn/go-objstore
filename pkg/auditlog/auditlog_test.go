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

package auditlog

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

func TestNoOpAuditLogger_ImplementsAuditLogger(t *testing.T) {
	var _ AuditLogger = NewNoOpAuditLogger()
}

func TestNoOpAuditLogger_DiscardsEvents(t *testing.T) {
	logger := NewNoOpAuditLogger()
	ctx := context.Background()
	failure := errors.New("denied")

	checks := map[string]error{
		"LogEvent":          logger.LogEvent(ctx, &AuditEvent{Action: "a", Result: ResultSuccess}),
		"LogAuthFailure":    logger.LogAuthFailure(ctx, "u", "p", "ip", "req", "bad password"),
		"LogAuthSuccess":    logger.LogAuthSuccess(ctx, "u", "p", "ip", "req"),
		"LogObjectAccess":   logger.LogObjectAccess(ctx, "u", "p", "b", "k", "ip", "req", ResultFailure, failure),
		"LogObjectMutation": logger.LogObjectMutation(ctx, EventObjectCreated, "u", "p", "b", "k", "ip", "req", 1, ResultSuccess, nil),
		"LogPolicyChange":   logger.LogPolicyChange(ctx, "u", "p", "b", "pol", "ip", "req", ResultSuccess, nil),
	}
	for name, err := range checks {
		if err != nil {
			t.Errorf("%s returned %v, want nil", name, err)
		}
	}
}

func TestNoOpAuditLogger_SetLevelGetLevel(t *testing.T) {
	noop := &NoOpAuditLogger{level: adapters.InfoLevel}

	noop.SetLevel(adapters.DebugLevel)
	if noop.GetLevel() != adapters.DebugLevel {
		t.Errorf("GetLevel = %v, want DebugLevel", noop.GetLevel())
	}

	noop.SetLevel(adapters.ErrorLevel)
	if noop.GetLevel() != adapters.ErrorLevel {
		t.Errorf("GetLevel = %v, want ErrorLevel", noop.GetLevel())
	}
}

func TestNewNoOpAuditLogger_DefaultsToInfo(t *testing.T) {
	if got := NewNoOpAuditLogger().GetLevel(); got != adapters.InfoLevel {
		t.Errorf("default level = %v, want InfoLevel", got)
	}
}
