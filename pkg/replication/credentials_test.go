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

package replication

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func credentialPolicy(id string, source, destination map[string]string) common.ReplicationPolicy {
	return common.ReplicationPolicy{
		ID:                  id,
		SourceBackend:       wireprobe.Backend,
		SourceSettings:      source,
		DestinationBackend:  wireprobe.Backend,
		DestinationSettings: destination,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}
}

func TestAddPolicyRefusesCredentialInSettings(t *testing.T) {
	cases := map[string]common.ReplicationPolicy{
		"source":      credentialPolicy("p", map[string]string{"secretKey": "SK-repl"}, map[string]string{"region": "eu-1"}),
		"destination": credentialPolicy("p", map[string]string{"region": "eu-1"}, map[string]string{"access_key": "AK-repl"}),
		"token":       credentialPolicy("p", nil, map[string]string{"Session-Token": "tok-repl"}),
	}
	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			fs := newMockFileSystem()
			mgr, err := newLocalTestManager(fs, "test-policies.json", 5*time.Minute, adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())
			if err != nil {
				t.Fatalf("create manager: %v", err)
			}

			err = mgr.AddPolicy(policy)

			var settingErr *common.CredentialSettingError
			if !errors.As(err, &settingErr) || !errors.Is(err, common.ErrCredentialInSettings) {
				t.Fatalf("want *CredentialSettingError, got %v", err)
			}
			for _, secret := range []string{"SK-repl", "AK-repl", "tok-repl"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error echoes the credential %q", secret)
				}
			}
			if _, getErr := mgr.GetPolicy("p"); getErr == nil {
				t.Error("a refused policy must not be held")
			}
			if fs.fileExists("test-policies.json") {
				data, _ := fs.readFile("test-policies.json")
				for _, secret := range []string{"SK-repl", "AK-repl", "tok-repl"} {
					if strings.Contains(string(data), secret) {
						t.Errorf("credential %q was persisted", secret)
					}
				}
			}
		})
	}
}

func TestSyncerConfiguresBackendsOnDefaultChain(t *testing.T) {
	probe := wireprobe.RegisterStorage(t)
	policy := credentialPolicy("p", map[string]string{"region": "eu-1"}, map[string]string{"region": "eu-2"})

	if _, err := NewSyncer(policy, nil, nil, nil, adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger()); err != nil {
		t.Fatalf("NewSyncer: %v", err)
	}

	if _, _, hasKey, hasSecret := probe.Credential(); hasKey || hasSecret || probe.Calls() != 2 {
		t.Errorf("replication supplies no credential: key handle %t, secret handle %t, calls %d", hasKey, hasSecret, probe.Calls())
	}
}
