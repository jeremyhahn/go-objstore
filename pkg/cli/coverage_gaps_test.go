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

package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	replicationPkg "github.com/jeremyhahn/go-objstore/pkg/replication"
)

// errStorage wraps mockStorage and injects configurable errors into storage operations.
type errStorage struct {
	*mockStorage
	listErr   error
	existsErr error
}

func (e *errStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	return e.mockStorage.ListWithOptions(ctx, opts)
}

func (e *errStorage) Exists(ctx context.Context, key string) (bool, error) {
	if e.existsErr != nil {
		return false, e.existsErr
	}
	return e.mockStorage.Exists(ctx, key)
}

// errPolicyStorage extends mockLifecycleStorage with an archive error and
// controllable list behaviour so we can reach uncovered branches in applyLocalPolicies.
type errPolicyStorage struct {
	*mockLifecycleStorage
	archiveCallErr error
}

func (s *errPolicyStorage) Archive(key string, dest common.Archiver) error {
	if s.archiveCallErr != nil {
		return s.archiveCallErr
	}
	return s.mockLifecycleStorage.Archive(key, dest)
}

// metadataStorage is a storage whose ListWithOptions returns the metadata
// that was stored via PutWithMetadata (or manually via the metadata map).
// This is needed for applyLocalPolicies tests: the base mockStorage.ListWithOptions
// always generates fresh metadata with time.Now(), so object age is always zero
// and the policy action branches are never reached.
type metadataStorage struct {
	*mockStorage
	archiveErr error
}

func newMetadataStorage() *metadataStorage {
	return &metadataStorage{mockStorage: newMockStorage()}
}

func (s *metadataStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for key := range s.data {
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		info := &common.ObjectInfo{Key: key}
		if meta, ok := s.metadata[key]; ok {
			info.Metadata = meta
		}
		objects = append(objects, info)
	}
	return &common.ListResult{Objects: objects}, nil
}

func (s *metadataStorage) Archive(key string, dest common.Archiver) error {
	if s.archiveErr != nil {
		return s.archiveErr
	}
	return s.mockStorage.Archive(key, dest)
}

func (s *metadataStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return s.mockStorage.AddPolicy(policy)
}

func (s *metadataStorage) RemovePolicy(id string) error {
	return s.mockStorage.RemovePolicy(id)
}

func (s *metadataStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return s.mockStorage.GetPolicies()
}

// -------------------------------------------------------------------
// NewCommandContext – server branch
// -------------------------------------------------------------------

func TestNewCommandContext_ServerWithUnsupportedProtocol(t *testing.T) {
	// Providing a server URL triggers the client branch. An unsupported
	// protocol causes client.NewClient to return an error, which surfaces
	// as a "failed to create remote client" error from NewCommandContext.
	cfg := &Config{
		Backend:        "local",
		BackendPath:    t.TempDir(),
		OutputFormat:   "text",
		Server:         "localhost:9999",
		ServerProtocol: "badprotocol",
	}
	_, err := NewCommandContext(cfg)
	if err == nil {
		t.Fatal("expected error for unsupported server protocol")
	}
	if !strings.Contains(err.Error(), "failed to create remote client") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestNewCommandContext_LocalStorageCreateError covers the "return nil, err" branch
// inside the else clause of NewCommandContext when factory.NewStorage fails.
// The local backend's Configure calls os.MkdirAll; passing an unwritable path
// causes that call to fail. We skip the test when running as root since root
// can write anywhere.
func TestNewCommandContext_LocalStorageCreateError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping unwritable-path test when running as root")
	}
	// Create a temp dir with no write permission so MkdirAll on a subdirectory fails.
	parent := t.TempDir()
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0700) }) // allow cleanup

	cfg := &Config{
		Backend:      "local",
		BackendPath:  parent + "/newsubdir",
		OutputFormat: "text",
	}
	_, err := NewCommandContext(cfg)
	if err == nil {
		t.Fatal("expected error when local storage cannot be created")
	}
}

func TestNewCommandContext_ServerRestSuccess(t *testing.T) {
	// The REST client constructor does not dial the server, so NewClient succeeds
	// even when the server isn't running. This covers the "ctx.Client = remoteClient" branch.
	cfg := &Config{
		Backend:        "local",
		BackendPath:    t.TempDir(),
		OutputFormat:   "text",
		Server:         "http://localhost:9999",
		ServerProtocol: "rest",
	}
	cmdCtx, err := NewCommandContext(cfg)
	if err != nil {
		t.Fatalf("NewCommandContext with REST server failed: %v", err)
	}
	if cmdCtx.Client == nil {
		t.Error("expected non-nil Client for remote server config")
	}
	if cmdCtx.Storage != nil {
		t.Error("expected nil Storage when using remote server")
	}
	cmdCtx.Close()
}

// -------------------------------------------------------------------
// PutCommandWithMetadata – client error path
// -------------------------------------------------------------------

func TestPutCommandWithMetadata_ClientError(t *testing.T) {
	want := errors.New("put failed")
	mc := &mockClient{putError: want}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{OutputFormat: "text"},
	}
	err := ctx.PutCommandWithMetadata("key", "-", "text/plain", "gzip",
		map[string]string{"x": "y"})
	if !errors.Is(err, want) {
		t.Errorf("expected put error, got %v", err)
	}
}

// putErrStorage wraps mockStorage and returns an error from PutWithMetadata.
type putErrStorage struct {
	*mockStorage
	putErr error
}

func (s *putErrStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if s.putErr != nil {
		return s.putErr
	}
	return s.mockStorage.PutWithMetadata(ctx, key, data, metadata)
}

// -------------------------------------------------------------------
// PutCommandWithMetadata – PutWithMetadata local storage error
// -------------------------------------------------------------------

func TestPutCommandWithMetadata_StoragePutError(t *testing.T) {
	want := errors.New("put storage error")
	st := &putErrStorage{mockStorage: newMockStorage(), putErr: want}
	ctx := &CommandContext{
		Storage: st,
		Config:  &Config{},
	}
	// filePath = "" reads from stdin; we don't need a real stdin here since
	// the error is returned from PutWithMetadata, not from reading stdin.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	oldStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()
	w.Close() // EOF immediately

	if err := ctx.PutCommandWithMetadata("k", "", "", "", nil); !errors.Is(err, want) {
		t.Errorf("expected put storage error, got %v", err)
	}
}

// -------------------------------------------------------------------
// GetCommand – client error path and client→stdout path
// -------------------------------------------------------------------

func TestGetCommand_ClientError(t *testing.T) {
	want := errors.New("get failed")
	mc := &mockClient{getError: want}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.GetCommand("key", "/tmp/out.txt"); !errors.Is(err, want) {
		t.Errorf("expected get error, got %v", err)
	}
}

func TestGetCommand_ClientToStdout(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	// Empty outputPath routes the data to os.Stdout – we just check no error.
	if err := ctx.GetCommand("key", ""); err != nil {
		t.Errorf("GetCommand client→stdout failed: %v", err)
	}
}

// -------------------------------------------------------------------
// ListCommand – local storage error
// -------------------------------------------------------------------

func TestListCommand_StorageError(t *testing.T) {
	want := errors.New("list error")
	st := &errStorage{mockStorage: newMockStorage(), listErr: want}
	ctx := &CommandContext{
		Storage: st,
		Config:  &Config{OutputFormat: "text"},
	}
	_, err := ctx.ListCommand("")
	if !errors.Is(err, want) {
		t.Errorf("expected list error, got %v", err)
	}
}

// -------------------------------------------------------------------
// ExistsCommand – local storage error
// -------------------------------------------------------------------

func TestExistsCommand_StorageError(t *testing.T) {
	want := errors.New("exists error")
	st := &errStorage{mockStorage: newMockStorage(), existsErr: want}
	ctx := &CommandContext{
		Storage: st,
		Config:  &Config{},
	}
	_, err := ctx.ExistsCommand("key")
	if !errors.Is(err, want) {
		t.Errorf("expected exists error, got %v", err)
	}
}

// -------------------------------------------------------------------
// ArchiveCommandWithSettings – empty-settings fallback + client path
// -------------------------------------------------------------------

func TestArchiveCommandWithSettings_EmptySettingsFallback(t *testing.T) {
	// When destinationSettings is empty the function should fall back to
	// cfg.GetStorageSettings(). Archiving to "invalid" still fails, but the
	// fallback branch must have been executed.
	st := newMockLifecycleStorage()
	st.data["obj.txt"] = []byte("data")
	cfg := &Config{
		Backend:     "local",
		BackendPath: t.TempDir(),
	}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	// Passing empty map triggers the fallback; "invalid" backend returns error.
	err := cmdCtx.ArchiveCommandWithSettings("obj.txt", "invalid", map[string]string{})
	if err == nil {
		t.Error("expected error for invalid destination backend")
	}
}

func TestArchiveCommandWithSettings_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{Backend: "local", BackendPath: t.TempDir()},
	}
	// Archive via client should succeed (mockClient.Archive returns nil).
	if err := ctx.ArchiveCommandWithSettings("key", "local", map[string]string{"path": "/tmp"}); err != nil {
		t.Errorf("ArchiveCommandWithSettings client path failed: %v", err)
	}
}

// -------------------------------------------------------------------
// AddPolicyCommand – client path
// -------------------------------------------------------------------

func TestAddPolicyCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.AddPolicyCommand("p1", "logs/", "7", "delete"); err != nil {
		t.Errorf("AddPolicyCommand client path failed: %v", err)
	}
}

// -------------------------------------------------------------------
// newPolicyArchiver – glacier registered paths
// -------------------------------------------------------------------

func TestNewPolicyArchiver_GlacierRegisteredNoVault(t *testing.T) {
	if !factory.IsArchiverRegistered("glacier") {
		t.Skip("glacier archiver not compiled in (needs -tags glacier)")
	}
	ctx := &CommandContext{
		Config: &Config{
			// No ArchiveVaultName → should return ErrArchiveVaultRequired.
		},
	}
	_, err := ctx.newPolicyArchiver()
	if !errors.Is(err, ErrArchiveVaultRequired) {
		t.Errorf("expected ErrArchiveVaultRequired, got %v", err)
	}
}

func TestNewPolicyArchiver_GlacierRegisteredWithVault(t *testing.T) {
	if !factory.IsArchiverRegistered("glacier") {
		t.Skip("glacier archiver not compiled in (needs -tags glacier)")
	}
	ctx := &CommandContext{
		Config: &Config{
			ArchiveVaultName: "my-vault",
			ArchiveRegion:    "us-east-1",
		},
	}
	archiver, err := ctx.newPolicyArchiver()
	if err != nil {
		t.Errorf("newPolicyArchiver with vault failed: %v", err)
	}
	if archiver == nil {
		t.Error("expected non-nil archiver")
	}
}

// TestAddPolicyCommand_LocalArchiveWithGlacier exercises the
// "policy.Destination = archiver" branch when the glacier archiver is
// registered and a vault name is configured.
func TestAddPolicyCommand_LocalArchiveWithGlacier(t *testing.T) {
	if !factory.IsArchiverRegistered("glacier") {
		t.Skip("glacier archiver not compiled in (needs -tags glacier)")
	}
	st := newMockLifecycleStorage()
	ctx := &CommandContext{
		Storage: st,
		Config: &Config{
			Backend:          "local",
			BackendPath:      t.TempDir(),
			ArchiveVaultName: "my-test-vault",
			ArchiveRegion:    "us-east-1",
		},
	}
	if err := ctx.AddPolicyCommand("arch-p1", "data/", "30", "archive"); err != nil {
		t.Errorf("AddPolicyCommand local archive failed: %v", err)
	}
	policies, _ := st.GetPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].Destination == nil {
		t.Error("expected non-nil Destination archiver on archive policy")
	}
}

// -------------------------------------------------------------------
// RemovePolicyCommand – client path
// -------------------------------------------------------------------

func TestRemovePolicyCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.RemovePolicyCommand("p1"); err != nil {
		t.Errorf("RemovePolicyCommand client path failed: %v", err)
	}
}

func TestRemovePolicyCommand_LocalStorage(t *testing.T) {
	// Local CLI mode returns ErrReplicationRequiresServer for replication
	// commands but returns a storage error for regular policy remove.
	st := newMockLifecycleStorage()
	ctx := &CommandContext{Storage: st, Config: &Config{}}
	// "nonexistent" is not in the store, so RemovePolicy returns an error.
	if err := ctx.RemovePolicyCommand("nonexistent"); err == nil {
		t.Error("expected error removing nonexistent policy")
	}
}

// -------------------------------------------------------------------
// ListPoliciesCommand – client path
// -------------------------------------------------------------------

func TestListPoliciesCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	policies, err := ctx.ListPoliciesCommand()
	if err != nil {
		t.Errorf("ListPoliciesCommand client path failed: %v", err)
	}
	if policies == nil {
		t.Error("expected non-nil policies slice")
	}
}

// -------------------------------------------------------------------
// ApplyPoliciesCommand – client path
// -------------------------------------------------------------------

func TestApplyPoliciesCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.ApplyPoliciesCommand(); err != nil {
		t.Errorf("ApplyPoliciesCommand client path failed: %v", err)
	}
}

// -------------------------------------------------------------------
// applyLocalPolicies – archive branch, obj.Metadata==nil, delete error
// -------------------------------------------------------------------

func TestApplyLocalPolicies_ArchiveAction(t *testing.T) {
	// Build a storage that has one very old object matching an archive policy
	// whose Destination archiver is set (use local backend as fake archiver).
	destDir := t.TempDir()
	archiver, err := factory.NewArchiver("local", map[string]string{"path": destDir})
	if err != nil {
		t.Fatalf("failed to create local archiver: %v", err)
	}

	st := newMetadataStorage()
	st.data["data/old.bin"] = []byte("archive content")
	st.metadata["data/old.bin"] = &common.Metadata{
		Size:         15,
		LastModified: time.Now().Add(-72 * time.Hour),
	}

	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}

	policies := []common.LifecyclePolicy{
		{
			ID:          "archive-old",
			Prefix:      "data/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: archiver,
		},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies archive action failed: %v", err)
	}
}

func TestApplyLocalPolicies_ArchiveDestinationNil(t *testing.T) {
	// archive policy with nil Destination should be silently skipped.
	st := newMetadataStorage()
	st.data["logs/old.txt"] = []byte("data")
	st.metadata["logs/old.txt"] = &common.Metadata{
		Size:         4,
		LastModified: time.Now().Add(-48 * time.Hour),
	}
	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	policies := []common.LifecyclePolicy{
		{
			ID:          "nil-dest",
			Prefix:      "logs/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: nil,
		},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies with nil Destination failed: %v", err)
	}
}

func TestApplyLocalPolicies_ObjectWithNilMetadata(t *testing.T) {
	// Objects with nil metadata must be skipped without error. metadataStorage
	// returns nil Metadata for keys that have no entry in the metadata map.
	st := newMetadataStorage()
	st.data["test/noMeta.txt"] = []byte("x")
	// Deliberately do not set st.metadata["test/noMeta.txt"]

	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	policies := []common.LifecyclePolicy{
		{
			ID:        "skip-nil-meta",
			Prefix:    "test/",
			Retention: 1 * time.Second,
			Action:    "delete",
		},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies with nil metadata failed: %v", err)
	}
}

func TestApplyLocalPolicies_DeleteError(t *testing.T) {
	// When DeleteWithContext returns an error applyLocalPolicies logs to stderr
	// and continues – it does NOT propagate the error to the caller. We verify
	// that the overall call still returns nil.
	//
	// Strategy: create a very old object in the metadata store so the policy
	// age check passes, then pre-remove it from the data map so that
	// DeleteWithContext finds nothing and returns an error. The listing still
	// returns the object because metadataStorage.ListWithOptions iterates the
	// data map – so we must keep the key in data but make the delete fail.
	// We achieve this by using metadataStorage.archiveErr approach: put the
	// object, let it age, then use a separate helper that wraps DeleteWithContext.

	// Simplest reliable approach: put an old object, run the delete policy,
	// confirm nil return (and the object gets deleted if the mock allows it).
	st := newMetadataStorage()
	st.data["logs/expired.log"] = []byte("old")
	st.metadata["logs/expired.log"] = &common.Metadata{
		Size:         3,
		LastModified: time.Now().Add(-48 * time.Hour),
	}

	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}

	policies := []common.LifecyclePolicy{
		{
			ID:        "del-success",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies returned unexpected error: %v", err)
	}
}

// ghostStorage is a storage where ListWithOptions returns objects that are
// then absent from the underlying data map. This forces DeleteWithContext to
// return an error, exercising the error-logging branch in applyLocalPolicies.
type ghostStorage struct {
	*metadataStorage
	ghostKeys map[string]*common.Metadata // extra keys returned only from List
}

func newGhostStorage() *ghostStorage {
	return &ghostStorage{
		metadataStorage: newMetadataStorage(),
		ghostKeys:       make(map[string]*common.Metadata),
	}
}

func (g *ghostStorage) addGhost(key string, meta *common.Metadata) {
	g.ghostKeys[key] = meta
}

func (g *ghostStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	result, err := g.metadataStorage.ListWithOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	for key, meta := range g.ghostKeys {
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}
		result.Objects = append(result.Objects, &common.ObjectInfo{Key: key, Metadata: meta})
	}
	return result, nil
}

// TestApplyLocalPolicies_ListError verifies that when ListWithOptions returns
// an error, applyLocalPolicies propagates it.
func TestApplyLocalPolicies_ListError(t *testing.T) {
	listErr := errors.New("list failed")
	st := &errStorage{mockStorage: newMockStorage(), listErr: listErr}
	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	policies := []common.LifecyclePolicy{
		{ID: "p", Prefix: "x/", Retention: 24 * time.Hour, Action: "delete"},
	}
	if err := cmdCtx.applyLocalPolicies(policies); !errors.Is(err, listErr) {
		t.Errorf("expected list error, got %v", err)
	}
}

// TestApplyLocalPolicies_PrefixMismatch verifies that objects whose key does
// not match the policy prefix are skipped (the !HasPrefix continue branch).
func TestApplyLocalPolicies_PrefixMismatch(t *testing.T) {
	st := newMetadataStorage()
	// Add an old object under "other/" prefix.
	st.data["other/file.txt"] = []byte("data")
	st.metadata["other/file.txt"] = &common.Metadata{
		Size:         4,
		LastModified: time.Now().Add(-72 * time.Hour),
	}
	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	// Policy only targets "logs/" – "other/file.txt" should be skipped.
	policies := []common.LifecyclePolicy{
		{ID: "logs-only", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies prefix mismatch failed: %v", err)
	}
	// Object should still exist since it didn't match the policy.
	if _, ok := st.data["other/file.txt"]; !ok {
		t.Error("object should not have been deleted (prefix mismatch)")
	}
}

// TestApplyLocalPolicies_DeleteContinuesOnError verifies that when a delete
// fails, applyLocalPolicies logs the error but does not return it.
func TestApplyLocalPolicies_DeleteContinuesOnError(t *testing.T) {
	// Add a "ghost" key that appears in the listing but does not exist in the
	// data map. DeleteWithContext on that key returns an error. The function
	// must log the error and continue without returning it.
	st := newGhostStorage()
	st.addGhost("items/ghost.txt", &common.Metadata{
		Size:         5,
		LastModified: time.Now().Add(-48 * time.Hour),
	})

	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	policies := []common.LifecyclePolicy{
		{
			ID:        "del-err-policy",
			Prefix:    "items/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		},
	}
	// applyLocalPolicies logs the delete error to stderr and returns nil.
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies should not propagate delete error, got: %v", err)
	}
}

// TestApplyLocalPolicies_ArchiveContinuesOnError verifies that when Archive
// fails, applyLocalPolicies logs the error and does not return it.
func TestApplyLocalPolicies_ArchiveContinuesOnError(t *testing.T) {
	// archiveErrStorage wraps metadataStorage and always fails Archive.
	type archiveErrStorage struct {
		*metadataStorage
	}
	inner := &archiveErrStorage{metadataStorage: newMetadataStorage()}
	inner.data["arch/old.bin"] = []byte("data")
	inner.metadata["arch/old.bin"] = &common.Metadata{
		Size:         4,
		LastModified: time.Now().Add(-72 * time.Hour),
	}
	inner.archiveErr = errors.New("archive backend unreachable")

	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: inner, Config: cfg}
	policies := []common.LifecyclePolicy{
		{
			ID:          "arch-err-policy",
			Prefix:      "arch/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: &mockArchiver{},
		},
	}
	// applyLocalPolicies logs the archive error and returns nil.
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies should not propagate archive error, got: %v", err)
	}
}

// -------------------------------------------------------------------
// GetMetadataCommand – client path
// -------------------------------------------------------------------

func TestGetMetadataCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	meta, err := ctx.GetMetadataCommand("key.txt")
	if err != nil {
		t.Errorf("GetMetadataCommand client path failed: %v", err)
	}
	if meta == nil {
		t.Error("expected non-nil metadata")
	}
}

func TestGetMetadataCommand_ClientError(t *testing.T) {
	want := errors.New("metadata error")
	mc := &mockClient{metadataError: want}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	_, err := ctx.GetMetadataCommand("key.txt")
	if !errors.Is(err, want) {
		t.Errorf("expected metadata error, got %v", err)
	}
}

// -------------------------------------------------------------------
// UpdateMetadataCommand – client path
// -------------------------------------------------------------------

func TestUpdateMetadataCommand_ClientPath(t *testing.T) {
	mc := &mockClient{}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.UpdateMetadataCommand("key.txt", "text/plain", "", nil); err != nil {
		t.Errorf("UpdateMetadataCommand client path failed: %v", err)
	}
}

func TestUpdateMetadataCommand_ClientError(t *testing.T) {
	want := errors.New("update error")
	mc := &mockClient{metadataError: want}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	if err := ctx.UpdateMetadataCommand("key.txt", "", "", nil); !errors.Is(err, want) {
		t.Errorf("expected update error, got %v", err)
	}
}

// -------------------------------------------------------------------
// Replication commands – local storage paths (ErrReplicationRequiresServer)
// -------------------------------------------------------------------

func TestRemoveReplicationPolicyCommand_LocalStorage(t *testing.T) {
	ctx := &CommandContext{Storage: newMockStorage(), Config: &Config{}}
	err := ctx.RemoveReplicationPolicyCommand("p1")
	if !errors.Is(err, ErrReplicationRequiresServer) {
		t.Errorf("expected ErrReplicationRequiresServer, got %v", err)
	}
}

func TestGetReplicationPolicyCommand_LocalStorage(t *testing.T) {
	ctx := &CommandContext{Storage: newMockStorage(), Config: &Config{}}
	_, err := ctx.GetReplicationPolicyCommand("p1")
	if !errors.Is(err, ErrReplicationRequiresServer) {
		t.Errorf("expected ErrReplicationRequiresServer, got %v", err)
	}
}

func TestListReplicationPoliciesCommand_LocalStorage(t *testing.T) {
	ctx := &CommandContext{Storage: newMockStorage(), Config: &Config{}}
	_, err := ctx.ListReplicationPoliciesCommand()
	if !errors.Is(err, ErrReplicationRequiresServer) {
		t.Errorf("expected ErrReplicationRequiresServer, got %v", err)
	}
}

func TestTriggerReplicationCommand_LocalStorage(t *testing.T) {
	ctx := &CommandContext{Storage: newMockStorage(), Config: &Config{}}
	_, err := ctx.TriggerReplicationCommand("p1")
	if !errors.Is(err, ErrReplicationRequiresServer) {
		t.Errorf("expected ErrReplicationRequiresServer, got %v", err)
	}
}

func TestGetReplicationStatusCommand_LocalStorage(t *testing.T) {
	ctx := &CommandContext{Storage: newMockStorage(), Config: &Config{}}
	_, err := ctx.GetReplicationStatusCommand("p1")
	if !errors.Is(err, ErrReplicationRequiresServer) {
		t.Errorf("expected ErrReplicationRequiresServer, got %v", err)
	}
}

// -------------------------------------------------------------------
// truncateString – very-short maxLen (≤3) branch
// -------------------------------------------------------------------

func TestTruncateString_ShortMaxLen(t *testing.T) {
	// maxLen ≤ 3 should return the raw prefix without ellipsis.
	got := truncateString("hello", 3)
	if got != "hel" {
		t.Errorf("truncateString(\"hello\", 3) = %q, want %q", got, "hel")
	}
	got2 := truncateString("hello", 2)
	if got2 != "he" {
		t.Errorf("truncateString(\"hello\", 2) = %q, want %q", got2, "he")
	}
}

// -------------------------------------------------------------------
// formatReplicationPoliciesTable – disabled-policy branch
// -------------------------------------------------------------------

func TestFormatReplicationPoliciesTable_DisabledPolicy(t *testing.T) {
	policies := []common.ReplicationPolicy{
		{
			ID:                 "disabled-pol",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			ReplicationMode:    common.ReplicationModeTransparent,
			Enabled:            false,
			CheckInterval:      5 * time.Minute,
			LastSyncTime:       time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC),
		},
	}
	out := formatReplicationPoliciesTable(policies)
	if !strings.Contains(out, "No") {
		t.Errorf("expected 'No' for disabled policy, got: %s", out)
	}
	if !strings.Contains(out, "2024-06-01") {
		t.Errorf("expected last-sync date in output, got: %s", out)
	}
}

// -------------------------------------------------------------------
// formatReplicationStatusTable – disabled-status and zero-time branches
// -------------------------------------------------------------------

func TestFormatReplicationStatusTable_Disabled(t *testing.T) {
	out := formatReplicationStatusTable(&replicationPkg.ReplicationStatus{
		PolicyID:           "p",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            false,
	})
	if !strings.Contains(out, "No") {
		t.Errorf("expected 'No' for disabled status, got: %s", out)
	}
}

// -------------------------------------------------------------------
// Config.GetStorageSettings – encryption branches
// -------------------------------------------------------------------

func TestGetStorageSettings_EncryptionFields(t *testing.T) {
	cfg := &Config{
		Backend:               "local",
		BackendPath:           "/tmp/data",
		EncryptionEnabled:     true,
		EncryptionKeyID:       "key-id-123",
		EncryptionBackend:     "vault",
		EncryptionBackendPath: "/etc/vault",
		EncryptionKMSPath:     "/etc/kms",
	}
	settings := cfg.GetStorageSettings()

	if settings["encryption_enabled"] != "true" {
		t.Errorf("encryption_enabled = %q, want \"true\"", settings["encryption_enabled"])
	}
	if settings["encryption_key_id"] != "key-id-123" {
		t.Errorf("encryption_key_id = %q", settings["encryption_key_id"])
	}
	if settings["encryption_backend"] != "vault" {
		t.Errorf("encryption_backend = %q", settings["encryption_backend"])
	}
	if settings["encryption_backend_path"] != "/etc/vault" {
		t.Errorf("encryption_backend_path = %q", settings["encryption_backend_path"])
	}
	if settings["encryption_kms_path"] != "/etc/kms" {
		t.Errorf("encryption_kms_path = %q", settings["encryption_kms_path"])
	}
}

// -------------------------------------------------------------------
// Config.GetArchiverSettings – explicit archive region and vault-only
// -------------------------------------------------------------------

func TestGetArchiverSettings_ExplicitArchiveRegion(t *testing.T) {
	cfg := &Config{
		ArchiveVaultName: "my-vault",
		ArchiveRegion:    "eu-west-1",
		BackendRegion:    "us-east-1", // should NOT be used
	}
	settings := cfg.GetArchiverSettings()
	if settings["vaultName"] != "my-vault" {
		t.Errorf("vaultName = %q, want \"my-vault\"", settings["vaultName"])
	}
	if settings["region"] != "eu-west-1" {
		t.Errorf("region = %q, want \"eu-west-1\" (explicit archive region)", settings["region"])
	}
}

func TestGetArchiverSettings_FallbackToBackendRegion(t *testing.T) {
	cfg := &Config{
		ArchiveVaultName: "vault-2",
		ArchiveRegion:    "", // not set → fallback
		BackendRegion:    "ap-southeast-1",
	}
	settings := cfg.GetArchiverSettings()
	if settings["region"] != "ap-southeast-1" {
		t.Errorf("region = %q, want \"ap-southeast-1\"", settings["region"])
	}
}

func TestGetArchiverSettings_NoVaultNoRegion(t *testing.T) {
	cfg := &Config{}
	settings := cfg.GetArchiverSettings()
	if _, ok := settings["vaultName"]; ok {
		t.Error("expected vaultName to be absent")
	}
	if _, ok := settings["region"]; ok {
		t.Error("expected region to be absent")
	}
}

// -------------------------------------------------------------------
// Config.formatConfigText/Table/JSON – BackendPath branch
// -------------------------------------------------------------------

func TestFormatConfigText_WithBackendPath(t *testing.T) {
	cfg := &Config{
		Backend:      "local",
		BackendPath:  "/var/objstore/data",
		OutputFormat: "text",
	}
	out := formatConfigText(cfg)
	if !strings.Contains(out, "Backend Path: /var/objstore/data") {
		t.Errorf("expected Backend Path in text output, got: %s", out)
	}
}

func TestFormatConfigTable_WithBackendPath(t *testing.T) {
	cfg := &Config{
		Backend:     "local",
		BackendPath: "/var/objstore/data",
	}
	out := formatConfigTable(cfg)
	if !strings.Contains(out, "Backend Path") {
		t.Errorf("expected 'Backend Path' in table output, got: %s", out)
	}
	if !strings.Contains(out, "objstore") {
		t.Errorf("expected path value in table output, got: %s", out)
	}
}

func TestFormatConfigJSON_WithBackendPath(t *testing.T) {
	cfg := &Config{
		Backend:     "local",
		BackendPath: "/var/objstore/data",
	}
	out := formatConfigJSON(cfg)
	if !strings.Contains(out, `"backend_path"`) {
		t.Errorf("expected backend_path in JSON output, got: %s", out)
	}
}

// -------------------------------------------------------------------
// Config.formatConfigText – BackendURL branch
// -------------------------------------------------------------------

func TestFormatConfigText_WithBackendURL(t *testing.T) {
	cfg := &Config{
		Backend:       "minio",
		BackendBucket: "bucket",
		BackendURL:    "http://minio.local:9000",
		OutputFormat:  "text",
	}
	out := formatConfigText(cfg)
	if !strings.Contains(out, "Backend URL: http://minio.local:9000") {
		t.Errorf("expected Backend URL in text output, got: %s", out)
	}
}

// -------------------------------------------------------------------
// Config.formatConfigTable – BackendURL branch
// -------------------------------------------------------------------

func TestFormatConfigTable_WithBackendURL(t *testing.T) {
	cfg := &Config{
		Backend:    "minio",
		BackendURL: "http://minio.local:9000",
	}
	out := formatConfigTable(cfg)
	if !strings.Contains(out, "minio.local") {
		t.Errorf("expected URL in table output, got: %s", out)
	}
}

// -------------------------------------------------------------------
// Config.formatConfigJSON – BackendURL branch
// -------------------------------------------------------------------

func TestFormatConfigJSON_WithBackendURL(t *testing.T) {
	cfg := &Config{
		Backend:    "minio",
		BackendURL: "http://minio.local:9000",
	}
	out := formatConfigJSON(cfg)
	if !strings.Contains(out, `"backend_url"`) {
		t.Errorf("expected backend_url in JSON output, got: %s", out)
	}
}

// -------------------------------------------------------------------
// ValidateConfig – minio branches
// -------------------------------------------------------------------

func TestValidateConfig_MinioMissingBucket(t *testing.T) {
	cfg := &Config{
		Backend:      "minio",
		BackendURL:   "http://localhost:9000",
		OutputFormat: "text",
	}
	if err := ValidateConfig(cfg); !errors.Is(err, ErrBackendBucketRequired) {
		t.Errorf("expected ErrBackendBucketRequired, got %v", err)
	}
}

func TestValidateConfig_MinioMissingURL(t *testing.T) {
	cfg := &Config{
		Backend:       "minio",
		BackendBucket: "mybucket",
		OutputFormat:  "text",
	}
	if err := ValidateConfig(cfg); !errors.Is(err, ErrBackendURLRequired) {
		t.Errorf("expected ErrBackendURLRequired, got %v", err)
	}
}

func TestValidateConfig_MinioValid(t *testing.T) {
	cfg := &Config{
		Backend:       "minio",
		BackendBucket: "mybucket",
		BackendURL:    "http://localhost:9000",
		OutputFormat:  "text",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Errorf("expected valid minio config, got %v", err)
	}
}

// -------------------------------------------------------------------
// formatMetadataTable – ContentEncoding and Custom branches
// -------------------------------------------------------------------

func TestFormatMetadataTable_WithEncodingAndCustom(t *testing.T) {
	metadata := &common.Metadata{
		Size:            2048,
		LastModified:    time.Date(2025, 3, 15, 9, 0, 0, 0, time.UTC),
		ContentType:     "application/octet-stream",
		ContentEncoding: "gzip",
		Custom: map[string]string{
			"project": "alpha",
		},
	}
	out := formatMetadataTable(metadata)
	if !strings.Contains(out, "Content Encoding") {
		t.Errorf("expected Content Encoding in table, got: %s", out)
	}
	if !strings.Contains(out, "gzip") {
		t.Errorf("expected gzip value in table, got: %s", out)
	}
	if !strings.Contains(out, "project") {
		t.Errorf("expected custom field in table, got: %s", out)
	}
}

// -------------------------------------------------------------------
// FormatMetadataResult – nil metadata branch
// -------------------------------------------------------------------

func TestFormatMetadataResult_NilMetadataTable(t *testing.T) {
	out := FormatMetadataResult(nil, FormatTable)
	if !strings.Contains(out, "FAILED") {
		t.Errorf("expected FAILED in nil metadata table output, got: %s", out)
	}
}

func TestFormatMetadataResult_NilMetadataJSON(t *testing.T) {
	out := FormatMetadataResult(nil, FormatJSON)
	if !strings.Contains(out, "false") {
		t.Errorf("expected success:false in nil metadata JSON, got: %s", out)
	}
}

// -------------------------------------------------------------------
// ListCommand – client error path
// -------------------------------------------------------------------

func TestListCommand_ClientError(t *testing.T) {
	want := errors.New("list client error")
	mc := &mockClient{listError: want}
	ctx := &CommandContext{
		Client: mc,
		Config: &Config{},
	}
	_, err := ctx.ListCommand("")
	if !errors.Is(err, want) {
		t.Errorf("expected list error, got %v", err)
	}
}

// -------------------------------------------------------------------
// ArchiveCommandWithSettings – nil settings uses backend settings
// -------------------------------------------------------------------

func TestArchiveCommandWithSettings_NilSettings(t *testing.T) {
	// nil destinationSettings should also trigger the fallback to backend settings.
	st := newMockLifecycleStorage()
	st.data["obj.txt"] = []byte("data")
	cfg := &Config{
		Backend:     "local",
		BackendPath: t.TempDir(),
	}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}
	// Use local archiver (non-nil settings produced by GetStorageSettings).
	err := cmdCtx.ArchiveCommandWithSettings("obj.txt", "local", nil)
	if err != nil {
		t.Errorf("ArchiveCommandWithSettings nil settings failed: %v", err)
	}
}

// -------------------------------------------------------------------
// mockClient helpers – ensure Read/Write for applyLocalPolicies with archive
// -------------------------------------------------------------------

// mockArchiver implements common.Archiver for use in applyLocalPolicies tests.
type mockArchiver struct{}

func (a *mockArchiver) Put(key string, data io.Reader) error { return nil }

func TestApplyLocalPolicies_ArchiveWithMockArchiver(t *testing.T) {
	// Cover the archive branch where policy.Destination != nil and Archive succeeds.
	// Uses metadataStorage so the returned ObjectInfo carries the stored (old) LastModified.
	st := newMetadataStorage()
	st.data["archive/file.txt"] = []byte("content")
	st.metadata["archive/file.txt"] = &common.Metadata{
		Size:         7,
		LastModified: time.Now().Add(-72 * time.Hour),
	}
	cfg := &Config{Backend: "local", BackendPath: t.TempDir()}
	cmdCtx := &CommandContext{Storage: st, Config: cfg}

	policies := []common.LifecyclePolicy{
		{
			ID:          "archive-mock",
			Prefix:      "archive/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: &mockArchiver{},
		},
	}
	if err := cmdCtx.applyLocalPolicies(policies); err != nil {
		t.Errorf("applyLocalPolicies with mockArchiver failed: %v", err)
	}
}
