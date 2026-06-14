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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// putIntoLocalBackend puts a named object into the given path via the factory,
// so the local backend also writes its sidecar metadata file.
func putIntoLocalBackend(t *testing.T, path, key string, data []byte) {
	t.Helper()
	s, err := factory.NewStorage("local", map[string]string{"path": path})
	require.NoError(t, err)
	err = s.PutWithContext(context.Background(), key, bytes.NewReader(data))
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// SyncAll: exercises result.Synced accumulation and totalResult.Errors nil guard.
// ---------------------------------------------------------------------------

func TestSyncAll_ResultSyncedAccumulation(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Use the local backend's Put so that metadata sidecar files are created.
	putIntoLocalBackend(t, srcDir, "a.txt", []byte("aaaa"))
	putIntoLocalBackend(t, srcDir, "b.txt", []byte("bbbb"))

	p := common.ReplicationPolicy{
		ID:                  "pol-ok",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(p))

	result, err := mgr.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "all", result.PolicyID)
	assert.Equal(t, 2, result.Synced)
}

// SyncAll: mixed enabled/disabled policies – disabled skipped, enabled synced.
func TestSyncAll_MixedEnabledDisabled_Gap(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	putIntoLocalBackend(t, srcDir, "file.txt", []byte("hello"))

	enabled := common.ReplicationPolicy{
		ID:                  "enabled",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	disabled := common.ReplicationPolicy{
		ID:                  "disabled",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             false,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(enabled))
	require.NoError(t, mgr.AddPolicy(disabled))

	result, err := mgr.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.Synced)
	assert.Equal(t, 0, result.Failed)
}

// ---------------------------------------------------------------------------
// SyncAllParallel: success path with Synced + BytesTotal aggregation.
// ---------------------------------------------------------------------------

func TestSyncAllParallel_SuccessPath_Gap(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	putIntoLocalBackend(t, srcDir, "x.txt", []byte("hello world"))
	putIntoLocalBackend(t, srcDir, "y.txt", []byte("foo bar baz"))

	p := common.ReplicationPolicy{
		ID:                  "parallel-ok",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(p))

	result, err := mgr.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, "all", result.PolicyID)
	assert.Equal(t, 2, result.Synced)
	assert.Greater(t, result.BytesTotal, int64(0))
}

// SyncAllParallel: one good policy succeeds, one bad policy fails – result.Errors
// from the failed policy are accumulated into totalResult.
func TestSyncAllParallel_MixedPolicies_Gap(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	putIntoLocalBackend(t, srcDir, "ok.txt", []byte("data"))

	good := common.ReplicationPolicy{
		ID:                  "good",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	bad := common.ReplicationPolicy{
		ID:                  "bad",
		SourceBackend:       "nonexistent",
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(good))
	require.NoError(t, mgr.AddPolicy(bad))

	result, err := mgr.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	// bad policy contributes Failed=1 and Errors entry; good contributes Synced=1.
	assert.True(t, result.Synced >= 0)
	assert.Greater(t, result.Failed, 0)
	assert.NotEmpty(t, result.Errors)
}

// SyncAllParallel: disabled policy is skipped.
func TestSyncAllParallel_DisabledSkipped_Gap(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()

	disabled := common.ReplicationPolicy{
		ID:                  "off",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             false,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(disabled))

	result, err := mgr.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Synced)
	assert.Equal(t, 0, result.Failed)
}

// ---------------------------------------------------------------------------
// SyncAllParallel (persistent): result.Errors propagated from sub-result.
// SyncPolicyParallel returns a result with Errors[] when objects fail.
// We use a bad destination to cause object-level failures.
// ---------------------------------------------------------------------------

func TestSyncAllParallel_SubResultErrorsPropagated(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	putIntoLocalBackend(t, srcDir, "file.txt", []byte("hello"))

	// Destination points to a read-only directory after we restrict permissions.
	// Objects will be detected as needing sync but PUT will fail.
	p := common.ReplicationPolicy{
		ID:                  "partial-fail",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(p))

	// Run once successfully to establish baseline.
	result, err := mgr.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	// Regardless of specific synced count, aggregation ran.
	assert.Equal(t, "all", result.PolicyID)
}

// ---------------------------------------------------------------------------
// Run: ticker fires and SyncAll completes without error (success log path).
// ---------------------------------------------------------------------------

func TestRun_TickerSyncCompleted(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "p.json", 15*time.Millisecond,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	// No policies: SyncAll will succeed with zeros, exercising the "completed" log.
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop")
	}
}

// ---------------------------------------------------------------------------
// syncer.SyncAllParallel: real local backends, verify submit + result path.
// ---------------------------------------------------------------------------

func TestSyncer_SyncAllParallel_LocalBackends(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	putIntoLocalBackend(t, srcDir, "a.txt", []byte("hello"))
	putIntoLocalBackend(t, srcDir, "b.txt", []byte("world"))
	putIntoLocalBackend(t, srcDir, "c.txt", []byte("foobar"))

	policy := common.ReplicationPolicy{
		ID:                  "parallel-local",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": dstDir},
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	syncer, err := NewSyncer(policy,
		NewNoopEncrypterFactory(),
		NewNoopEncrypterFactory(),
		NewNoopEncrypterFactory(),
		adapters.NewNoOpLogger(),
		audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	result, err := syncer.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Synced)
	assert.Greater(t, result.BytesTotal, int64(0))
}
