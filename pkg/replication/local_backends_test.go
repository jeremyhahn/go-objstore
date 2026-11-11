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

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// newLocalTestManager builds a manager that allows filesystem backends, for
// the tests that add "local" policies on purpose.
func newLocalTestManager(
	fs FileSystem,
	policyFile string,
	interval time.Duration,
	logger adapters.Logger,
	auditLog auditlog.AuditLogger,
	opts ...ManagerOption,
) (*PersistentReplicationManager, error) {
	return NewPersistentReplicationManager(fs, policyFile, interval, logger, auditLog,
		append([]ManagerOption{WithClientLocalBackends(true)}, opts...)...)
}

func localPolicy(id, src, dst string) common.ReplicationPolicy {
	return common.ReplicationPolicy{
		ID:                  id,
		SourceBackend:       src,
		SourceSettings:      map[string]string{"path": "/etc"},
		DestinationBackend:  dst,
		DestinationSettings: map[string]string{"path": "/tmp/out"},
		CheckInterval:       time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}
}

func TestAddPolicy_RefusesLocalBackendsByDefault(t *testing.T) {
	cases := []struct{ name, src, dst string }{
		{"local source", "local", "memory"},
		{"local destination", "memory", "local"},
		{"both local", "local", "local"},
		{"case and space", " LOCAL ", "memory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			mgr, err := NewPersistentReplicationManager(fs, "p.json", time.Minute, nil, nil)
			if err != nil {
				t.Fatalf("new manager: %v", err)
			}
			err = mgr.AddPolicy(localPolicy("p1", tc.src, tc.dst))
			if !errors.Is(err, ErrClientLocalBackend) || !errors.Is(err, common.ErrInvalidArgument) {
				t.Fatalf("AddPolicy err = %v, want ErrClientLocalBackend wrapping ErrInvalidArgument", err)
			}
			if _, err := mgr.GetPolicy("p1"); err == nil {
				t.Fatal("refused policy was stored")
			}
			if _, ok := fs.files["p.json"]; ok {
				t.Fatal("refused policy was written to the policy file")
			}
		})
	}
}

func TestAddPolicy_NonPathBackendsAllowedByDefault(t *testing.T) {
	mgr, err := NewPersistentReplicationManager(newMockFileSystem(), "p.json", time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	p := localPolicy("p1", "memory", "memory")
	p.SourceSettings, p.DestinationSettings = nil, nil
	if err := mgr.AddPolicy(p); err != nil {
		t.Fatalf("AddPolicy(memory->memory) = %v", err)
	}
}

func TestAddPolicy_AllowsLocalBackendsWhenEnabled(t *testing.T) {
	mgr, err := NewPersistentReplicationManager(newMockFileSystem(), "p.json", time.Minute, nil, nil,
		WithClientLocalBackends(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.AddPolicy(localPolicy("p1", "local", "local")); err != nil {
		t.Fatalf("AddPolicy = %v, want nil with WithClientLocalBackends(true)", err)
	}
}

func TestLoad_RefusesPolicyFileWithLocalBackendByDefault(t *testing.T) {
	fs := newMockFileSystem()
	writer, err := newLocalTestManager(fs, "p.json", time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := writer.AddPolicy(localPolicy("held-local", "memory", "local")); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}

	_, err = NewPersistentReplicationManager(fs, "p.json", time.Minute, nil, nil)
	if !errors.Is(err, ErrClientLocalBackend) {
		t.Fatalf("load err = %v, want ErrClientLocalBackend", err)
	}
	if !strings.Contains(err.Error(), `"held-local"`) {
		t.Fatalf("load err %q does not name the policy ID", err)
	}

	if _, err := newLocalTestManager(fs, "p.json", time.Minute, nil, nil); err != nil {
		t.Fatalf("load with local backends allowed = %v", err)
	}
}

func TestIsPathBackendType(t *testing.T) {
	for typ, want := range map[string]bool{
		"local": true, "Local": true, "memory": false, "s3": false, "gcs": false, "azure": false, "": false,
	} {
		if got := IsPathBackendType(typ); got != want {
			t.Errorf("IsPathBackendType(%q) = %v, want %v", typ, got, want)
		}
	}
}
