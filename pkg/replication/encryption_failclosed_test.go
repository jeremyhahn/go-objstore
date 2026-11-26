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

//go:build local

package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// failClosedXorEncrypter is a reversible test encrypter whose output never
// equals its input, so a test can tell ciphertext from plaintext on disk.
type failClosedXorEncrypter struct{}

func failClosedXor(r io.Reader) (io.ReadCloser, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	for i := range data {
		data[i] ^= 0x5A
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (failClosedXorEncrypter) Encrypt(_ context.Context, r io.Reader) (io.ReadCloser, error) {
	return failClosedXor(r)
}

func (failClosedXorEncrypter) Decrypt(_ context.Context, r io.Reader) (io.ReadCloser, error) {
	return failClosedXor(r)
}

func (failClosedXorEncrypter) Algorithm() string { return "test-xor" }
func (failClosedXorEncrypter) KeyID() string     { return "test-key" }

type failClosedXorFactory struct{}

func (failClosedXorFactory) GetEncrypter(string) (common.Encrypter, error) {
	return failClosedXorEncrypter{}, nil
}
func (failClosedXorFactory) DefaultKeyID() string { return "test-key" }
func (failClosedXorFactory) Close() error         { return nil }

func failClosedPolicy(t *testing.T, srcBackend, dstBackend string, mode common.ReplicationMode, enc *common.EncryptionPolicy) common.ReplicationPolicy {
	t.Helper()
	settings := func(backend string) map[string]string {
		if backend == backendLocal {
			return map[string]string{"path": t.TempDir()}
		}
		return map[string]string{}
	}
	return common.ReplicationPolicy{
		ID:                  "fail-closed",
		SourceBackend:       srcBackend,
		SourceSettings:      settings(srcBackend),
		DestinationBackend:  dstBackend,
		DestinationSettings: settings(dstBackend),
		ReplicationMode:     mode,
		Encryption:          enc,
		Enabled:             true,
	}
}

func enabledLayer() *common.EncryptionConfig {
	return &common.EncryptionConfig{Enabled: true, Provider: "test"}
}

// F33: backend at-rest encryption on a side that cannot take it must fail,
// not replicate unencrypted.
func TestNewSyncer_BackendEncryptionUnsupportedBackend(t *testing.T) {
	cases := []struct{ name, src, dst string }{
		{"memory destination", backendLocal, "memory"},
		{"memory source", "memory", backendLocal},
		{"memory both", "memory", "memory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := failClosedPolicy(t, tc.src, tc.dst, common.ReplicationModeTransparent,
				&common.EncryptionPolicy{Backend: enabledLayer()})
			s, err := NewSyncer(policy, failClosedXorFactory{}, nil, nil,
				adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())
			if !errors.Is(err, ErrBackendEncryptionUnsupported) {
				t.Fatalf("NewSyncer error = %v, want ErrBackendEncryptionUnsupported", err)
			}
			if s != nil {
				t.Fatal("NewSyncer returned a syncer with an unsupported backend encryption layer")
			}
		})
	}
}

func TestNewSyncer_BackendEncryptionLocalWithFactory(t *testing.T) {
	policy := failClosedPolicy(t, backendLocal, backendLocal, common.ReplicationModeTransparent,
		&common.EncryptionPolicy{Backend: enabledLayer()})
	s, err := NewSyncer(policy, failClosedXorFactory{}, nil, nil,
		adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())
	if err != nil {
		t.Fatalf("NewSyncer: %v", err)
	}
	if s == nil {
		t.Fatal("NewSyncer returned no syncer")
	}
}

// F34: an enabled layer with no factory must fail, not fall back to a
// passthrough.
func TestNewSyncer_EnabledLayerWithoutFactory(t *testing.T) {
	cases := []struct {
		name    string
		mode    common.ReplicationMode
		enc     *common.EncryptionPolicy
		backend common.EncrypterFactory
		source  common.EncrypterFactory
		dest    common.EncrypterFactory
		layer   string
	}{
		{"backend transparent", common.ReplicationModeTransparent,
			&common.EncryptionPolicy{Backend: enabledLayer()}, nil, failClosedXorFactory{}, failClosedXorFactory{}, "backend"},
		{"backend opaque", common.ReplicationModeOpaque,
			&common.EncryptionPolicy{Backend: enabledLayer()}, nil, failClosedXorFactory{}, failClosedXorFactory{}, "backend"},
		{"source transparent", common.ReplicationModeTransparent,
			&common.EncryptionPolicy{Source: enabledLayer()}, failClosedXorFactory{}, nil, failClosedXorFactory{}, "source"},
		{"destination transparent", common.ReplicationModeTransparent,
			&common.EncryptionPolicy{Destination: enabledLayer()}, failClosedXorFactory{}, failClosedXorFactory{}, nil, "destination"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := failClosedPolicy(t, backendLocal, backendLocal, tc.mode, tc.enc)
			s, err := NewSyncer(policy, tc.backend, tc.source, tc.dest,
				adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())
			if !errors.Is(err, ErrEncrypterFactoryMissing) {
				t.Fatalf("NewSyncer error = %v, want ErrEncrypterFactoryMissing", err)
			}
			if !strings.Contains(err.Error(), tc.layer) {
				t.Errorf("error %q does not name the %s layer", err, tc.layer)
			}
			if s != nil {
				t.Fatal("NewSyncer returned a syncer for a layer with no factory")
			}
		})
	}
}

// Opaque mode skips the source and destination DEK layers, so their
// factories are not needed there.
func TestNewSyncer_OpaqueModeIgnoresDEKFactories(t *testing.T) {
	policy := failClosedPolicy(t, backendLocal, backendLocal, common.ReplicationModeOpaque,
		&common.EncryptionPolicy{Source: enabledLayer(), Destination: enabledLayer()})
	s, err := NewSyncer(policy, nil, nil, nil,
		adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger())
	if err != nil || s == nil {
		t.Fatalf("NewSyncer = (%v, %v), want a syncer", s, err)
	}
}

func failClosedManager(t *testing.T) *PersistentReplicationManager {
	t.Helper()
	mgr, err := NewPersistentReplicationManager(nil, filepath.Join(t.TempDir(), "policies.json"),
		time.Minute, nil, nil, WithClientLocalBackends(true))
	if err != nil {
		t.Fatalf("NewPersistentReplicationManager: %v", err)
	}
	t.Cleanup(mgr.Stop)
	return mgr
}

func failClosedSeed(t *testing.T, dir, key string, data []byte) {
	t.Helper()
	s := local.New()
	if err := s.Configure(map[string]string{"path": dir}, common.Credentials{}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutWithContext(context.Background(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// filesContaining returns the regular files under dir whose content
// contains needle.
func filesContaining(t *testing.T, dir string, needle []byte) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path) // #nosec G304 -- test temp dir
		if err != nil {
			return err
		}
		if bytes.Contains(data, needle) {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPersistentManager_EncryptedPolicyWithoutFactoryFails(t *testing.T) {
	for _, layer := range []string{"backend", "source", "destination"} {
		t.Run(layer, func(t *testing.T) {
			mgr := failClosedManager(t)
			enc := &common.EncryptionPolicy{}
			switch layer {
			case "backend":
				enc.Backend = enabledLayer()
			case "source":
				enc.Source = enabledLayer()
			case "destination":
				enc.Destination = enabledLayer()
			}
			policy := failClosedPolicy(t, backendLocal, backendLocal, common.ReplicationModeTransparent, enc)
			failClosedSeed(t, policy.SourceSettings["path"], "secret.txt", []byte("plaintext secret"))
			if err := mgr.AddPolicy(policy); err != nil {
				t.Fatalf("AddPolicy: %v", err)
			}

			_, err := mgr.SyncPolicy(context.Background(), policy.ID)
			if !errors.Is(err, ErrEncrypterFactoryMissing) {
				t.Fatalf("SyncPolicy error = %v, want ErrEncrypterFactoryMissing", err)
			}
			if _, err := mgr.SyncPolicyParallel(context.Background(), policy.ID, 2); !errors.Is(err, ErrEncrypterFactoryMissing) {
				t.Fatalf("SyncPolicyParallel error = %v, want ErrEncrypterFactoryMissing", err)
			}
			if n := countFiles(t, policy.DestinationSettings["path"]); n != 0 {
				t.Fatalf("destination has %d files after a refused sync, want 0", n)
			}
		})
	}
}

func TestPersistentManager_BackendEncryptionWithFactoryEncryptsAtRest(t *testing.T) {
	mgr := failClosedManager(t)
	policy := failClosedPolicy(t, backendLocal, backendLocal, common.ReplicationModeTransparent,
		&common.EncryptionPolicy{Backend: enabledLayer()})
	plaintext := []byte("plaintext secret that must not reach the disk")
	if err := mgr.AddPolicy(policy); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}
	if err := mgr.SetBackendEncrypterFactory(policy.ID, failClosedXorFactory{}); err != nil {
		t.Fatal(err)
	}
	// Seed through an encrypting source backend, as the policy declares.
	src := local.New()
	if err := src.Configure(map[string]string{"path": policy.SourceSettings["path"]}, common.Credentials{}); err != nil {
		t.Fatal(err)
	}
	src.(*local.Local).SetAtRestEncrypterFactory(failClosedXorFactory{})
	if err := src.PutWithContext(context.Background(), "secret.txt", bytes.NewReader(plaintext)); err != nil {
		t.Fatal(err)
	}

	result, err := mgr.SyncPolicy(context.Background(), policy.ID)
	if err != nil {
		t.Fatalf("SyncPolicy: %v", err)
	}
	if result.Synced != 1 {
		t.Fatalf("Synced = %d, want 1 (errors %v)", result.Synced, result.Errors)
	}
	if files := filesContaining(t, policy.DestinationSettings["path"], plaintext); len(files) != 0 {
		t.Fatalf("destination stores plaintext in %v", files)
	}
}
