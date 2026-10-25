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
	"context"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
)

func addArchivePolicy(t *testing.T, settings map[string]any) (string, error) {
	t.Helper()
	executor := createTestToolExecutor(t, newMockLifecycleStorage())
	return executor.Execute(context.Background(), "objstore_add_policy", map[string]any{
		"id":                   "p-wire",
		"action":               "archive",
		"retention_seconds":    float64(3600),
		"destination_type":     wireprobe.Backend,
		"destination_settings": settings,
	})
}

func TestAddPolicyArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	result, err := addArchivePolicy(t, map[string]any{"accessKey": "AK-mcp", "secretKey": "SK-mcp", "region": "eu-1"})
	if err != nil {
		t.Fatalf("add policy: %v", err)
	}
	probe.AssertCredential(t, "AK-mcp", "SK-mcp", map[string]string{"region": "eu-1"})
	if strings.Contains(result, "SK-mcp") {
		t.Error("result echoes the credential")
	}
}

func TestAddPolicyArchiveRefusesTokenSettings(t *testing.T) {
	probe := wireprobe.Register(t)
	_, err := addArchivePolicy(t, map[string]any{"sessionToken": "tok-mcp"})
	if err == nil {
		t.Fatal("a token left in settings must be refused")
	}
	if probe.Calls() != 0 || strings.Contains(err.Error(), "tok-mcp") {
		t.Errorf("configured %d times, or the error echoes the token", probe.Calls())
	}
}
