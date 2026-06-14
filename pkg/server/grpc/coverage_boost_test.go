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

package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// options.go: WithAuthorizer and WithBackend (0% coverage)
// ---------------------------------------------------------------------------

func TestWithAuthorizer(t *testing.T) {
	opts := DefaultServerOptions()
	authz := adapters.NewNoOpAuthorizer()
	WithAuthorizer(authz)(opts)

	if opts.Authorizer == nil {
		t.Error("Authorizer should be set")
	}
}

func TestWithBackend(t *testing.T) {
	opts := DefaultServerOptions()
	WithBackend("mybackend")(opts)

	if opts.Backend != "mybackend" {
		t.Errorf("expected Backend %q, got %q", "mybackend", opts.Backend)
	}
}

// ---------------------------------------------------------------------------
// handlers.go: keyRef with non-empty backend (66.7%)
// ---------------------------------------------------------------------------

func TestKeyRef_WithBackend(t *testing.T) {
	storage := newMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(WithAddress(":0"), WithBackend("s3"))
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	got := server.keyRef("mykey")
	want := "s3:mykey"
	if got != want {
		t.Errorf("keyRef = %q, want %q", got, want)
	}
}

func TestKeyRef_EmptyBackend(t *testing.T) {
	storage := newMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(WithAddress(":0"))
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	got := server.keyRef("mykey")
	if got != "mykey" {
		t.Errorf("keyRef = %q, want %q", "mykey", got)
	}
}

// ---------------------------------------------------------------------------
// handlers.go: List error path (87.5%)
// ---------------------------------------------------------------------------

// listErrorStorage wraps mockStorage and fails ListWithOptions.
type listErrorStorage struct {
	*mockStorage
}

func (s *listErrorStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, common.ErrKeyNotFound
}

func TestList_Error(t *testing.T) {
	storage := &listErrorStorage{mockStorage: newMockStorage()}
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.List(context.Background(), &objstorepb.ListRequest{Prefix: "x/"})
	if err == nil {
		t.Fatal("expected error from List")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Errorf("List error code = %v, want NotFound", st.Code())
	}
}

// ---------------------------------------------------------------------------
// handlers.go: GetMetadata nil metadata → NotFound (87.5%)
// ---------------------------------------------------------------------------

// nilMetadataStorage returns nil metadata for any key without an error.
type nilMetadataStorage struct {
	*mockStorage
}

func (s *nilMetadataStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, nil
}

func TestGetMetadata_NilMetadata(t *testing.T) {
	storage := &nilMetadataStorage{mockStorage: newMockStorage()}
	// Store the key so Get doesn't fail before metadata call.
	storage.data["mykey"] = []byte("data")

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetMetadata(context.Background(), &objstorepb.GetMetadataRequest{Key: "mykey"})
	if err == nil {
		t.Fatal("expected error for nil metadata")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Errorf("GetMetadata nil metadata = %v, want NotFound", err)
	}
}

// ---------------------------------------------------------------------------
// handlers.go: ApplyPolicies branches (63.6%)
//   - GetPolicies error
//   - ListWithOptions error (after policies found)
//   - archive action with Destination
//   - archive action with nil Destination (no-op)
//   - delete error (continue)
// ---------------------------------------------------------------------------

// applyPoliciesListErrorStorage: has policies, but ListWithOptions fails.
type applyPoliciesListErrorStorage struct {
	*mockLifecycleStorage
}

func (s *applyPoliciesListErrorStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, errors.New("list failed")
}

func TestApplyPolicies_ListError(t *testing.T) {
	base := newMockLifecycleStorage()
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:        "p1",
		Action:    "delete",
		Retention: time.Millisecond,
	})
	storage := &applyPoliciesListErrorStorage{mockLifecycleStorage: base}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err == nil {
		t.Fatal("expected error from ListWithOptions in ApplyPolicies")
	}
}

func TestApplyPolicies_GetPoliciesError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.getPoliciesError = errors.New("db error")

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err == nil {
		t.Fatal("expected error from GetPolicies in ApplyPolicies")
	}
}

// archiveMockStorage succeeds both Archive and ListWithOptions.
type archiveMockStorage struct {
	*mockLifecycleStorage
	archived []string
}

func (s *archiveMockStorage) Archive(key string, dst common.Archiver) error {
	s.archived = append(s.archived, key)
	return nil
}

func TestApplyPolicies_ArchiveAction(t *testing.T) {
	base := newMockLifecycleStorage()
	arcStorage := &archiveMockStorage{mockLifecycleStorage: base}

	// Add an archive policy whose retention is already exceeded.
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:          "arch-policy",
		Action:      "archive",
		Retention:   time.Millisecond, // immediately exceeded
		Destination: &mockArchiver{},
	})

	// Add an object older than retention.
	arcStorage.data["docs/a.txt"] = []byte("hello")
	arcStorage.metadata["docs/a.txt"] = &common.Metadata{
		Size:         5,
		LastModified: time.Now().Add(-time.Hour),
	}

	server, err := newTestServer(t, arcStorage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}
	if resp.ObjectsProcessed == 0 {
		t.Error("expected at least 1 object archived")
	}
}

func TestApplyPolicies_ArchiveAction_NilDestination(t *testing.T) {
	base := newMockLifecycleStorage()

	// Archive policy with nil Destination — should be a no-op (not counted).
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:          "no-dest",
		Action:      "archive",
		Retention:   time.Millisecond,
		Destination: nil, // deliberately nil
	})

	base.data["x.txt"] = []byte("x")
	base.metadata["x.txt"] = &common.Metadata{
		LastModified: time.Now().Add(-time.Hour),
	}

	server, err := newTestServer(t, base)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}
	if resp.ObjectsProcessed != 0 {
		t.Errorf("expected 0 objects processed for nil destination, got %d", resp.ObjectsProcessed)
	}
}

func TestApplyPolicies_DeleteError_Continues(t *testing.T) {
	base := newMockLifecycleStorage()

	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:        "del-policy",
		Action:    "delete",
		Retention: time.Millisecond,
	})

	// Object matches but deletion will fail because mockStorage.DeleteWithContext
	// returns ErrKeyNotFound for keys not in data (we put it only in metadata).
	base.metadata["orphan.txt"] = &common.Metadata{
		LastModified: time.Now().Add(-time.Hour),
	}
	// Note: intentionally NOT in base.data so DeleteWithContext returns not-found.

	server, err := newTestServer(t, base)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	// Should succeed overall (errors are logged and skipped).
	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies should not return error on delete failure: %v", err)
	}
	if resp.ObjectsProcessed != 0 {
		t.Errorf("expected 0 processed (delete failed), got %d", resp.ObjectsProcessed)
	}
}

// mockArchiver is a no-op Archiver for testing.
type mockArchiver struct{}

func (m *mockArchiver) Put(key string, data io.Reader) error { return nil }

// ---------------------------------------------------------------------------
// handlers.go: protoToLifecyclePolicy with invalid archiver (90.9%)
// ---------------------------------------------------------------------------

func TestProtoToLifecyclePolicy_InvalidArchiverType(t *testing.T) {
	proto := &objstorepb.LifecyclePolicy{
		Id:               "p1",
		Action:           "archive",
		DestinationType:  "unknown-backend-xyz",
		RetentionSeconds: 60,
	}
	_, err := protoToLifecyclePolicy(proto)
	if err == nil {
		t.Error("expected error for unknown archiver type")
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: generic error path (non-ErrReplicationNotSupported)
//   covers the mapError(err) fallback in all five replication handlers (75-88%)
// ---------------------------------------------------------------------------

// replicationManagerErrStorage returns a generic error from GetReplicationManager.
type replicationManagerErrStorage struct {
	*mockStorage
	managerErr error
}

func (s *replicationManagerErrStorage) GetReplicationManager() (common.ReplicationManager, error) {
	return nil, s.managerErr
}

func newReplicationManagerErrStorage(err error) *replicationManagerErrStorage {
	return &replicationManagerErrStorage{
		mockStorage: newMockStorage(),
		managerErr:  err,
	}
}

func TestAddReplicationPolicy_ManagerError(t *testing.T) {
	storage := newReplicationManagerErrStorage(errors.New("db down"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.AddReplicationPolicy(context.Background(), &objstorepb.AddReplicationPolicyRequest{
		Policy: &objstorepb.ReplicationPolicy{Id: "p1", SourceBackend: "a", DestinationBackend: "b"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() == codes.Unimplemented {
		t.Error("should not return Unimplemented for generic manager error")
	}
}

func TestRemoveReplicationPolicy_ManagerError(t *testing.T) {
	storage := newReplicationManagerErrStorage(errors.New("db down"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.RemoveReplicationPolicy(context.Background(), &objstorepb.RemoveReplicationPolicyRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() == codes.Unimplemented {
		t.Error("should not return Unimplemented for generic manager error")
	}
}

func TestGetReplicationPolicies_ManagerError(t *testing.T) {
	storage := newReplicationManagerErrStorage(errors.New("db down"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationPolicies(context.Background(), &objstorepb.GetReplicationPoliciesRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetReplicationPolicy_ManagerError(t *testing.T) {
	storage := newReplicationManagerErrStorage(errors.New("db down"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationPolicy(context.Background(), &objstorepb.GetReplicationPolicyRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTriggerReplication_ManagerError(t *testing.T) {
	storage := newReplicationManagerErrStorage(errors.New("db down"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.TriggerReplication(context.Background(), &objstorepb.TriggerReplicationRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: GetReplicationStatus (0% → 100%)
// ---------------------------------------------------------------------------

// replicationStatusProvider extends mockReplicationManager to satisfy the
// GetReplicationStatus interface checked inside GetReplicationStatus handler.
type replicationStatusProvider struct {
	*mockReplicationManager
	status    *replication.ReplicationStatus
	statusErr error
}

func (r *replicationStatusProvider) GetReplicationStatus(id string) (*replication.ReplicationStatus, error) {
	if r.statusErr != nil {
		return nil, r.statusErr
	}
	return r.status, nil
}

// mockReplicationStatusStorage wraps mockStorage and returns a replicationStatusProvider.
type mockReplicationStatusStorage struct {
	*mockStorage
	provider *replicationStatusProvider
	mgrErr   error
}

func (s *mockReplicationStatusStorage) GetReplicationManager() (common.ReplicationManager, error) {
	if s.mgrErr != nil {
		return nil, s.mgrErr
	}
	return s.provider, nil
}

func newReplicationStatusStorage(status *replication.ReplicationStatus, statusErr error) *mockReplicationStatusStorage {
	return &mockReplicationStatusStorage{
		mockStorage: newMockStorage(),
		provider: &replicationStatusProvider{
			mockReplicationManager: newMockReplicationManager(),
			status:                 status,
			statusErr:              statusErr,
		},
	}
}

func TestGetReplicationStatus_EmptyID(t *testing.T) {
	storage := newReplicationStatusStorage(&replication.ReplicationStatus{PolicyID: "p1"}, nil)
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: ""})
	if err == nil {
		t.Fatal("expected error for empty ID")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestGetReplicationStatus_ReplicationNotSupported(t *testing.T) {
	// Plain mockStorage does not implement ReplicationCapable.
	storage := newMockStorage()
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

func TestGetReplicationStatus_ManagerError(t *testing.T) {
	storage := &mockReplicationStatusStorage{
		mockStorage: newMockStorage(),
		mgrErr:      errors.New("manager unavailable"),
	}
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// mockReplicationManagerNoStatus does NOT implement GetReplicationStatus to
// exercise the "not supported by this backend" path inside the handler.
type mockReplicationManagerNoStatus struct {
	*mockReplicationManager
}

type noStatusReplicationStorage struct {
	*mockStorage
	mgr *mockReplicationManagerNoStatus
}

func (s *noStatusReplicationStorage) GetReplicationManager() (common.ReplicationManager, error) {
	return s.mgr, nil
}

func TestGetReplicationStatus_NotSupportedByManager(t *testing.T) {
	storage := &noStatusReplicationStorage{
		mockStorage: newMockStorage(),
		mgr:         &mockReplicationManagerNoStatus{newMockReplicationManager()},
	}
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

func TestGetReplicationStatus_StatusError(t *testing.T) {
	storage := newReplicationStatusStorage(nil, errors.New("not found"))
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected error from GetReplicationStatus")
	}
}

func TestGetReplicationStatus_Success(t *testing.T) {
	now := time.Now()
	repStatus := &replication.ReplicationStatus{
		PolicyID:            "p1",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		Enabled:             true,
		TotalObjectsSynced:  42,
		TotalObjectsDeleted: 3,
		TotalBytesSynced:    1024,
		TotalErrors:         0,
		LastSyncTime:        now,
		AverageSyncDuration: 100 * time.Millisecond,
		SyncCount:           7,
	}
	storage := newReplicationStatusStorage(repStatus, nil)
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	resp, err := server.GetReplicationStatus(context.Background(), &objstorepb.GetReplicationStatusRequest{Id: "p1"})
	if err != nil {
		t.Fatalf("GetReplicationStatus failed: %v", err)
	}
	if !resp.Success {
		t.Error("expected success")
	}
	if resp.Status == nil {
		t.Fatal("expected status in response")
	}
	if resp.Status.PolicyId != "p1" {
		t.Errorf("PolicyId = %q, want %q", resp.Status.PolicyId, "p1")
	}
	if resp.Status.TotalObjectsSynced != 42 {
		t.Errorf("TotalObjectsSynced = %d, want 42", resp.Status.TotalObjectsSynced)
	}
	if resp.Status.SyncCount != 7 {
		t.Errorf("SyncCount = %d, want 7", resp.Status.SyncCount)
	}
	if resp.Status.AverageSyncDurationMs != 100 {
		t.Errorf("AverageSyncDurationMs = %d, want 100", resp.Status.AverageSyncDurationMs)
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: logReplicationAudit with nil logger (88.9%)
// ---------------------------------------------------------------------------

func TestLogReplicationAudit_NilLogger(t *testing.T) {
	// Should not panic when auditLogger is nil.
	logReplicationAudit(context.Background(), nil, "TEST_EVENT", "policy-1", nil)
	logReplicationAudit(context.Background(), nil, "TEST_EVENT", "policy-1", errors.New("err"))
}

func TestLogReplicationAudit_WithLogger(t *testing.T) {
	logger := audit.NewDefaultAuditLogger()
	logReplicationAudit(context.Background(), logger, "TEST_EVENT", "policy-1", nil)
	logReplicationAudit(context.Background(), logger, "TEST_EVENT", "policy-1", errors.New("err"))
}

// ---------------------------------------------------------------------------
// handlers_replication.go: encryption policy coverage
//   protoToEncryptionPolicy — source and destination branches (80%)
//   encryptionPolicyToProto — source and destination branches (80%)
// ---------------------------------------------------------------------------

func TestProtoToEncryptionPolicy_AllBranches(t *testing.T) {
	proto := &objstorepb.EncryptionPolicy{
		Backend: &objstorepb.EncryptionConfig{
			Enabled:    true,
			Provider:   "kms",
			DefaultKey: "key-backend",
		},
		Source: &objstorepb.EncryptionConfig{
			Enabled:    true,
			Provider:   "vault",
			DefaultKey: "key-source",
		},
		Destination: &objstorepb.EncryptionConfig{
			Enabled:    false,
			Provider:   "custom",
			DefaultKey: "key-dest",
		},
	}

	got := protoToEncryptionPolicy(proto)
	if got == nil {
		t.Fatal("expected non-nil policy")
	}
	if got.Backend == nil || got.Backend.Provider != "kms" {
		t.Errorf("Backend not converted correctly: %+v", got.Backend)
	}
	if got.Source == nil || got.Source.Provider != "vault" {
		t.Errorf("Source not converted correctly: %+v", got.Source)
	}
	if got.Destination == nil || got.Destination.Provider != "custom" {
		t.Errorf("Destination not converted correctly: %+v", got.Destination)
	}
}

func TestEncryptionPolicyToProto_AllBranches(t *testing.T) {
	domain := &common.EncryptionPolicy{
		Backend: &common.EncryptionConfig{
			Enabled:    true,
			Provider:   "kms",
			DefaultKey: "key-backend",
		},
		Source: &common.EncryptionConfig{
			Enabled:    true,
			Provider:   "vault",
			DefaultKey: "key-source",
		},
		Destination: &common.EncryptionConfig{
			Enabled:    false,
			Provider:   "custom",
			DefaultKey: "key-dest",
		},
	}

	got := encryptionPolicyToProto(domain)
	if got == nil {
		t.Fatal("expected non-nil proto")
	}
	if got.Backend == nil || got.Backend.Provider != "kms" {
		t.Errorf("Backend not converted correctly: %+v", got.Backend)
	}
	if got.Source == nil || got.Source.Provider != "vault" {
		t.Errorf("Source not converted correctly: %+v", got.Source)
	}
	if got.Destination == nil || got.Destination.Provider != "custom" {
		t.Errorf("Destination not converted correctly: %+v", got.Destination)
	}
}

func TestReplicationPolicyToProto_NoEncryption(t *testing.T) {
	domain := &common.ReplicationPolicy{
		ID:                 "p1",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeOpaque,
		Encryption:         nil, // explicitly nil
	}

	got := replicationPolicyToProto(domain)
	if got == nil {
		t.Fatal("expected non-nil proto")
	}
	if got.Encryption != nil {
		t.Errorf("expected nil Encryption in proto, got %+v", got.Encryption)
	}
	if got.ReplicationMode != objstorepb.ReplicationMode_OPAQUE {
		t.Errorf("expected OPAQUE mode, got %v", got.ReplicationMode)
	}
}

func TestProtoToReplicationPolicy_OpaqueMode(t *testing.T) {
	proto := &objstorepb.ReplicationPolicy{
		Id:              "p1",
		SourceBackend:   "local",
		ReplicationMode: objstorepb.ReplicationMode_OPAQUE,
	}

	domain, err := protoToReplicationPolicy(proto)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if domain.ReplicationMode != common.ReplicationModeOpaque {
		t.Errorf("ReplicationMode = %v, want Opaque", domain.ReplicationMode)
	}
}

// ---------------------------------------------------------------------------
// interceptors.go: AuthorizationStreamInterceptor — public method bypass,
//   and missing-principal path (83.3%)
// ---------------------------------------------------------------------------

func TestAuthorizationStreamInterceptor_PublicMethodBypass(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{})
	interceptor := AuthorizationStreamInterceptor(authz, adapters.NewNoOpLogger())

	called := false
	handler := func(srv any, ss grpc.ServerStream) error {
		called = true
		return nil
	}

	// Health check method is public — must bypass authz even with no principal.
	err := interceptor(nil,
		&mockServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		handler,
	)
	if err != nil {
		t.Errorf("public method should pass: %v", err)
	}
	if !called {
		t.Error("handler should have been called for public method")
	}
}

func TestAuthorizationStreamInterceptor_MissingPrincipal(t *testing.T) {
	authz := adapters.NewNoOpAuthorizer()
	interceptor := AuthorizationStreamInterceptor(authz, adapters.NewNoOpLogger())

	handler := func(srv any, ss grpc.ServerStream) error { return nil }

	// Context has no principal → PermissionDenied.
	err := interceptor(nil,
		&mockServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/objstore.ObjectStore/Put"},
		handler,
	)
	if err == nil {
		t.Fatal("expected PermissionDenied when principal is missing")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// server.go: ForceStop when rateLimiter is set (90%)
// ---------------------------------------------------------------------------

func TestServer_ForceStop_WithRateLimiter(t *testing.T) {
	storage := newMockStorage()
	rateCfg := &middleware.RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             10,
	}

	server, err := newTestServer(t, storage,
		WithAddress("127.0.0.1:0"),
		WithRateLimit(true, rateCfg),
	)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	go server.Start()
	// Wait for server to bind.
	for i := 0; i < 200; i++ {
		if addr := server.GetAddress(); addr != "" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// ForceStop should stop both the rate limiter and the gRPC server.
	server.ForceStop()
}

// ---------------------------------------------------------------------------
// server.go: Stop when rateLimiter is set (completes the 90% branch)
// ---------------------------------------------------------------------------

func TestServer_Stop_WithRateLimiter(t *testing.T) {
	storage := newMockStorage()
	rateCfg := &middleware.RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             10,
	}

	server, err := newTestServer(t, storage,
		WithAddress("127.0.0.1:0"),
		WithRateLimit(true, rateCfg),
	)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	go server.Start()
	for i := 0; i < 200; i++ {
		if addr := server.GetAddress(); addr != "" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	server.Stop()
}

// ---------------------------------------------------------------------------
// handlers.go: Get — stream.Send error paths and canceled-context path
// ---------------------------------------------------------------------------

// mockGetStream implements objstorepb.ObjectStore_GetServer (which is
// grpc.ServerStreamingServer[GetResponse]) for unit-testing the Get handler.
type mockGetStream struct {
	grpc.ServerStream
	ctx      context.Context
	sendErr  error
	received []*objstorepb.GetResponse
}

func (m *mockGetStream) Send(resp *objstorepb.GetResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.received = append(m.received, resp)
	return nil
}

func (m *mockGetStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func TestGet_SendErrorOnData(t *testing.T) {
	storage := newMockStorage()
	storage.data["testkey"] = []byte("hello")
	storage.metadata["testkey"] = &common.Metadata{Size: 5}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	stream := &mockGetStream{
		ctx:     context.Background(),
		sendErr: errors.New("send failed"),
	}

	err = server.Get(&objstorepb.GetRequest{Key: "testkey"}, stream)
	if err == nil {
		t.Fatal("expected error when stream.Send fails")
	}
}

func TestGet_SendErrorOnFinalMessage(t *testing.T) {
	storage := newMockStorage()
	// Empty object: reader.Read returns (0, io.EOF) immediately so the
	// first Send is the final message, not a data chunk.
	storage.data["emptykey"] = []byte{}
	storage.metadata["emptykey"] = &common.Metadata{Size: 0}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	sendCount := 0
	stream := &mockGetStream{
		ctx: context.Background(),
	}
	// Override Send to fail on first call (the final message for empty object).
	stream.sendErr = errors.New("final send failed")

	_ = sendCount

	err = server.Get(&objstorepb.GetRequest{Key: "emptykey"}, stream)
	if err == nil {
		t.Fatal("expected error when final stream.Send fails for empty object")
	}
}

func TestGet_CanceledContext(t *testing.T) {
	storage := newMockStorage()
	storage.data["testkey"] = []byte("hello world")
	storage.metadata["testkey"] = &common.Metadata{Size: 11}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	// Pre-canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &mockGetStream{ctx: ctx}

	err = server.Get(&objstorepb.GetRequest{Key: "testkey"}, stream)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Canceled {
		t.Errorf("expected Canceled, got %v", err)
	}
}

// errorReader returns a read error after some initial data.
type errorReader struct {
	data     []byte
	offset   int
	readOnce bool
}

func (r *errorReader) Read(p []byte) (int, error) {
	if !r.readOnce && len(r.data) > 0 {
		n := copy(p, r.data[r.offset:])
		r.offset += n
		r.readOnce = true
		return n, nil
	}
	return 0, errors.New("read error from storage")
}

func (r *errorReader) Close() error { return nil }

// errorReaderStorage returns an errorReader that fails after the first Read.
type errorReaderStorage struct {
	*mockStorage
}

func (s *errorReaderStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if _, ok := s.data[key]; !ok {
		return nil, common.ErrKeyNotFound
	}
	return &errorReader{data: s.data[key]}, nil
}

// TestGet_ReaderError covers the reader.Read non-EOF error path.
func TestGet_ReaderError(t *testing.T) {
	base := newMockStorage()
	base.data["testkey"] = []byte("hello world")
	base.metadata["testkey"] = &common.Metadata{Size: 11}
	storage := &errorReaderStorage{mockStorage: base}

	// Use chunk size smaller than data so we get at least one partial read before
	// the error — but actually errorReader will succeed on first call then error.
	server, err := newTestServer(t, storage, WithChunkSize(3))
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	stream := &mockGetStream{ctx: context.Background()}
	err = server.Get(&objstorepb.GetRequest{Key: "testkey"}, stream)
	if err == nil {
		t.Fatal("expected error from reader")
	}
}

// statefulGetStream fails on the Nth Send call (1-indexed).
type statefulGetStream struct {
	grpc.ServerStream
	ctx      context.Context
	callNum  int
	failOn   int
	received []*objstorepb.GetResponse
}

func (m *statefulGetStream) Send(resp *objstorepb.GetResponse) error {
	m.callNum++
	if m.callNum == m.failOn {
		return errors.New("send failed on call " + string(rune('0'+m.callNum)))
	}
	m.received = append(m.received, resp)
	return nil
}

func (m *statefulGetStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// TestGet_SendErrorOnFinalAfterData sends data chunks successfully then fails
// on the final (IsLast=true) message, covering the second stream.Send error path.
func TestGet_SendErrorOnFinalAfterData(t *testing.T) {
	storage := newMockStorage()
	// Non-empty object with small chunk size → first Send is data, second is final.
	storage.data["testkey"] = []byte("hi")
	storage.metadata["testkey"] = &common.Metadata{Size: 2}

	// Use a very large chunk size so the entire content fits in one read,
	// meaning first Send = data chunk (succeeds), second Send = final (fails).
	server, err := newTestServer(t, storage, WithChunkSize(64*1024))
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	stream := &statefulGetStream{
		ctx:    context.Background(),
		failOn: 2, // succeed on call 1 (data), fail on call 2 (final)
	}

	err = server.Get(&objstorepb.GetRequest{Key: "testkey"}, stream)
	if err == nil {
		t.Fatal("expected error when final stream.Send fails after data")
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: protoToReplicationPolicy nil input → ErrPolicyCannotBeNil
// ---------------------------------------------------------------------------

func TestProtoToReplicationPolicy_Nil(t *testing.T) {
	_, err := protoToReplicationPolicy(nil)
	if err == nil {
		t.Fatal("expected error for nil proto")
	}
	if !errors.Is(err, ErrPolicyCannotBeNil) {
		t.Errorf("expected ErrPolicyCannotBeNil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: default replication mode branches
// ---------------------------------------------------------------------------

func TestProtoToReplicationPolicy_DefaultMode(t *testing.T) {
	// ReplicationMode value 2 is not defined by the proto (only 0=TRANSPARENT
	// and 1=OPAQUE exist), so the switch default fires and maps to Transparent.
	proto := &objstorepb.ReplicationPolicy{
		Id:              "p1",
		SourceBackend:   "local",
		ReplicationMode: objstorepb.ReplicationMode(2),
	}

	domain, err := protoToReplicationPolicy(proto)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if domain.ReplicationMode != common.ReplicationModeTransparent {
		t.Errorf("default mode = %v, want Transparent", domain.ReplicationMode)
	}
}

func TestReplicationPolicyToProto_DefaultMode(t *testing.T) {
	// Use an unrecognized domain mode (not Transparent or Opaque) to trigger
	// the default branch, which maps to TRANSPARENT.
	domain := &common.ReplicationPolicy{
		ID:              "p1",
		SourceBackend:   "local",
		ReplicationMode: common.ReplicationMode("unknown-mode"),
	}

	proto := replicationPolicyToProto(domain)
	if proto == nil {
		t.Fatal("expected non-nil proto")
	}
	if proto.ReplicationMode != objstorepb.ReplicationMode_TRANSPARENT {
		t.Errorf("default mode = %v, want TRANSPARENT", proto.ReplicationMode)
	}
}

// ---------------------------------------------------------------------------
// handlers_replication.go: ErrReplicationNotSupported path for handlers where
//   only the generic-error path was hit previously (complete coverage).
// ---------------------------------------------------------------------------

// The plain mockStorage does not implement ReplicationCapable, which causes
// GetReplicationManager to return ErrReplicationNotSupported. Only
// AddReplicationPolicy's not-supported path is exercised in the existing
// handlers_replication_test.go. The remaining four handlers are tested below.

func TestTriggerReplication_NotSupported(t *testing.T) {
	storage := newMockStorage()
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.TriggerReplication(context.Background(), &objstorepb.TriggerReplicationRequest{})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

func TestRemoveReplicationPolicy_NotSupported(t *testing.T) {
	storage := newMockStorage()
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.RemoveReplicationPolicy(context.Background(), &objstorepb.RemoveReplicationPolicyRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

func TestGetReplicationPolicies_NotSupported(t *testing.T) {
	storage := newMockStorage()
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationPolicies(context.Background(), &objstorepb.GetReplicationPoliciesRequest{})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

func TestGetReplicationPolicy_NotSupported(t *testing.T) {
	storage := newMockStorage()
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	_, err = server.GetReplicationPolicy(context.Background(), &objstorepb.GetReplicationPolicyRequest{Id: "p1"})
	if err == nil {
		t.Fatal("expected Unimplemented error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", st.Code())
	}
}

// ---------------------------------------------------------------------------
// handlers.go: Get — GetMetadata returns error after successful object read
// ---------------------------------------------------------------------------

// getMetadataErrStorage: Get succeeds, GetMetadata returns an error.
type getMetadataErrStorage struct {
	*mockStorage
}

func (s *getMetadataErrStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, common.ErrMetadataNotFound
}

func TestGet_MetadataFetchError(t *testing.T) {
	storage := &getMetadataErrStorage{mockStorage: newMockStorage()}
	storage.data["testkey"] = []byte("hello")
	// Note: no entry in metadata map, but GetMetadata is overridden to return error.

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	stream := &mockGetStream{ctx: context.Background()}
	err = server.Get(&objstorepb.GetRequest{Key: "testkey"}, stream)
	if err == nil {
		t.Fatal("expected error when GetMetadata fails")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// handlers.go: ApplyPolicies — additional branch coverage
// ---------------------------------------------------------------------------

// TestApplyPolicies_ObjectWithNilMetadataSkipped covers the obj.Metadata == nil
// continue branch. The mockStorage.ListWithOptions always sets metadata, so
// we embed a custom ListWithOptions that returns an ObjectInfo with nil metadata.
type nilMetadataListStorage struct {
	*mockLifecycleStorage
}

func (s *nilMetadataListStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{
		Objects: []*common.ObjectInfo{
			{Key: "orphan.txt", Metadata: nil}, // nil metadata — should be skipped
		},
	}, nil
}

func TestApplyPolicies_ObjectWithNilMetadataSkipped(t *testing.T) {
	base := newMockLifecycleStorage()
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:        "p1",
		Action:    "delete",
		Retention: time.Millisecond,
	})
	storage := &nilMetadataListStorage{mockLifecycleStorage: base}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}
	if resp.ObjectsProcessed != 0 {
		t.Errorf("expected 0 processed (nil metadata), got %d", resp.ObjectsProcessed)
	}
}

func TestApplyPolicies_ObjectWithinRetention(t *testing.T) {
	base := newMockLifecycleStorage()
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:        "p1",
		Action:    "delete",
		Retention: 24 * time.Hour, // long retention
	})

	// Object was modified just now — within retention.
	base.data["recent.txt"] = []byte("data")
	base.metadata["recent.txt"] = &common.Metadata{
		LastModified: time.Now(), // brand new
	}

	server, err := newTestServer(t, base)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}
	if resp.ObjectsProcessed != 0 {
		t.Errorf("expected 0 processed (within retention), got %d", resp.ObjectsProcessed)
	}
}

// archiveErrorStorage extends mockLifecycleStorage so that its Archive method
// returns an error, covering the "failed to archive" continue branch in
// ApplyPolicies.
type archiveErrorStorage struct {
	*mockLifecycleStorage
}

func (s *archiveErrorStorage) Archive(key string, dst common.Archiver) error {
	return errors.New("archive backend unavailable")
}

func TestApplyPolicies_ArchiveError_Continues(t *testing.T) {
	base := newMockLifecycleStorage()
	storage := &archiveErrorStorage{mockLifecycleStorage: base}

	// Archive policy; Destination must be non-nil for the branch to be entered.
	_ = base.AddPolicy(common.LifecyclePolicy{
		ID:          "arch-err",
		Action:      "archive",
		Retention:   time.Millisecond,
		Destination: &mockArchiver{},
	})

	base.data["docs/b.txt"] = []byte("data")
	base.metadata["docs/b.txt"] = &common.Metadata{
		LastModified: time.Now().Add(-time.Hour),
	}

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	// Should succeed overall (error logged and skipped).
	resp, err := server.ApplyPolicies(context.Background(), &objstorepb.ApplyPoliciesRequest{})
	if err != nil {
		t.Fatalf("ApplyPolicies should not fail on archive error: %v", err)
	}
	if resp.ObjectsProcessed != 0 {
		t.Errorf("expected 0 processed (archive error), got %d", resp.ObjectsProcessed)
	}
}

// ---------------------------------------------------------------------------
// server.go: buildServerOptions nil-authorizer fallback
// ---------------------------------------------------------------------------

// TestBuildServerOptions_NilAuthorizer exercises the branch that replaces a
// nil authorizer with the NoOp default inside buildServerOptions.
func TestBuildServerOptions_NilAuthorizer(t *testing.T) {
	storage := newMockStorage()
	initTestFacade(t, storage)

	// Manually build options with a nil Authorizer and call buildServerOptions.
	opts := DefaultServerOptions()
	opts.Authorizer = nil
	opts.Address = "127.0.0.1:0"

	server := &Server{
		opts:    opts,
		metrics: NewMetricsCollector(),
	}

	serverOpts := server.buildServerOptions()
	// If it didn't panic and returned options, the nil-authorizer branch fired.
	if len(serverOpts) == 0 {
		t.Error("expected non-empty server options")
	}
}
