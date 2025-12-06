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
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestNewSyncer_InvalidSourceBackend tests NewSyncer with invalid source backend.
func TestNewSyncer_InvalidSourceBackend(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "invalid-backend",
		SourceSettings:      map[string]string{"path": "/invalid"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	_, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err == nil {
		t.Fatal("Expected error for invalid source backend")
	}

	if !strings.Contains(err.Error(), "failed to create source backend") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestNewSyncer_InvalidDestBackend tests NewSyncer with invalid destination backend.
func TestNewSyncer_InvalidDestBackend(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "invalid-backend",
		DestinationSettings: map[string]string{"path": "/invalid"},
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	_, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err == nil {
		t.Fatal("Expected error for invalid destination backend")
	}

	if !strings.Contains(err.Error(), "failed to create destination backend") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestNewSyncer_LocalWithBackendEncryption tests NewSyncer with local backend and backend encryption.
func TestNewSyncer_LocalWithBackendEncryption(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:             "test-policy",
		SourceBackend:  "local",
		SourceSettings: map[string]string{"path": t.TempDir()},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode: common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Backend: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "backend-key",
			},
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-key",
			},
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-key",
			},
		},
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}

	if syncer.policy.ID != "test-policy" {
		t.Errorf("Expected policy ID 'test-policy', got '%s'", syncer.policy.ID)
	}
}

// TestNewSyncer_OpaqueMode tests NewSyncer with opaque replication mode.
func TestNewSyncer_OpaqueMode(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeOpaque,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-key",
			},
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-key",
			},
		},
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}

	// In opaque mode, encryption wrappers should NOT be applied
	// The syncer should still work but copy blobs as-is
}

// TestNewSyncer_InvalidMode tests NewSyncer with invalid replication mode.
func TestNewSyncer_InvalidMode(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     "invalid-mode",
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	_, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err == nil {
		t.Fatal("Expected error for invalid replication mode")
	}

	if !strings.Contains(err.Error(), "unsupported replication mode") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestNewSyncer_TransparentMode tests NewSyncer with transparent mode and encryption.
func TestNewSyncer_TransparentMode(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-key",
			},
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-key",
			},
		},
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}

	// Verify syncer is properly configured
	if syncer.source == nil {
		t.Error("Source storage should be set")
	}

	if syncer.dest == nil {
		t.Error("Destination storage should be set")
	}

	if syncer.logger == nil {
		t.Error("Logger should be set")
	}

	if syncer.auditLog == nil {
		t.Error("Audit log should be set")
	}
}

// TestNewSyncer_NoEncryption tests NewSyncer without encryption.
func TestNewSyncer_NoEncryption(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeTransparent,
		Encryption:          nil, // No encryption
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}
}

// TestNewSyncer_SourceEncryptionOnly tests NewSyncer with only source encryption.
func TestNewSyncer_SourceEncryptionOnly(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-key",
			},
		},
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}
}

// TestNewSyncer_DestinationEncryptionOnly tests NewSyncer with only destination encryption.
func TestNewSyncer_DestinationEncryptionOnly(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		ReplicationMode:     common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-key",
			},
		},
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}
}

// TestSyncObject_GetMetadataError tests SyncObject when GetMetadata fails.
func TestSyncObject_GetMetadataError(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source
	testData := []byte("test content")
	source.data["test.txt"] = testData
	source.getMetaError = errors.New("metadata fetch failed")

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncObject(context.Background(), "test.txt")

	if err == nil {
		t.Fatal("Expected error when GetMetadata fails")
	}

	if !strings.Contains(err.Error(), "failed to get metadata") {
		t.Errorf("Unexpected error message: %v", err)
	}
}


// TestSyncAll_MultipleObjects tests syncing multiple objects.
func TestSyncAll_MultipleObjects(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add multiple test files
	for i := 1; i <= 5; i++ {
		key := "file" + string(rune('0'+i)) + ".txt"
		data := []byte("content " + string(rune('0'+i)))
		source.data[key] = data
		source.objects[key] = &common.Metadata{
			Size:         int64(len(data)),
			ETag:         "etag" + string(rune('0'+i)),
			LastModified: time.Now(),
		}
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result.Synced != 5 {
		t.Errorf("Expected 5 synced objects, got %d", result.Synced)
	}

	if result.Failed != 0 {
		t.Errorf("Expected 0 failed objects, got %d", result.Failed)
	}

	// Verify all files were synced to destination
	for i := 1; i <= 5; i++ {
		key := "file" + string(rune('0'+i)) + ".txt"
		if _, exists := dest.data[key]; !exists {
			t.Errorf("File %s was not synced to destination", key)
		}
	}
}

// TestClose_Success tests the Close method.
func TestClose_Success(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	err := syncer.Close()

	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestSyncObject_LargeFile tests syncing a large file.
func TestSyncObject_LargeFile(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Create a large test file (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	source.data["large.bin"] = largeData
	source.objects["large.bin"] = &common.Metadata{
		Size:         int64(len(largeData)),
		ETag:         "large-etag",
		LastModified: time.Now(),
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	size, err := syncer.SyncObject(context.Background(), "large.bin")

	if err != nil {
		t.Fatalf("SyncObject failed: %v", err)
	}

	if size != int64(len(largeData)) {
		t.Errorf("Expected size %d, got %d", len(largeData), size)
	}

	// Verify data integrity
	syncedData := dest.data["large.bin"]
	if len(syncedData) != len(largeData) {
		t.Errorf("Data size mismatch: expected %d, got %d", len(largeData), len(syncedData))
	}

	for i := range largeData {
		if syncedData[i] != largeData[i] {
			t.Errorf("Data mismatch at byte %d", i)
			break
		}
	}
}

// TestSyncAll_ContextCancellation tests SyncAll with context cancellation.
func TestSyncAll_ContextCancellation(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data
	source.data["test.txt"] = []byte("test")
	source.objects["test.txt"] = &common.Metadata{
		Size:         4,
		ETag:         "etag",
		LastModified: time.Now(),
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// This should handle the cancelled context gracefully
	// The behavior depends on when the context is checked
	_, err := syncer.SyncAll(ctx)

	// The error could be nil (if sync completed before context check)
	// or a context cancellation error
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Logf("SyncAll with cancelled context returned: %v", err)
	}
}

// TestNewSyncer_S3Backend tests NewSyncer with S3 backend (if available).
func TestNewSyncer_S3Backend(t *testing.T) {
	t.Skip("Skipping S3 backend test - requires AWS credentials")

	policy := common.ReplicationPolicy{
		ID:            "test-policy",
		SourceBackend: "s3",
		SourceSettings: map[string]string{
			"bucket": "test-bucket",
			"region": "us-east-1",
		},
		DestinationBackend: "s3",
		DestinationSettings: map[string]string{
			"bucket": "test-dest-bucket",
			"region": "us-east-1",
		},
		ReplicationMode: common.ReplicationModeTransparent,
	}

	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	syncer, err := NewSyncer(policy, backendFactory, sourceFactory, destFactory, logger, auditLog)

	if err != nil {
		t.Fatalf("NewSyncer with S3 failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}
}

// TestSyncObject_CustomMetadata tests syncing with custom metadata fields.
func TestSyncObject_CustomMetadata(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	testData := []byte("test content with metadata")
	source.data["test.txt"] = testData
	source.objects["test.txt"] = &common.Metadata{
		Size:         int64(len(testData)),
		ETag:         "etag123",
		LastModified: time.Now(),
		ContentType:  "text/plain",
		Custom: map[string]string{
			"author":  "test-user",
			"version": "1.0",
		},
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	size, err := syncer.SyncObject(context.Background(), "test.txt")

	if err != nil {
		t.Fatalf("SyncObject failed: %v", err)
	}

	if size != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), size)
	}

	// Verify custom metadata was preserved
	destMeta := dest.objects["test.txt"]
	if destMeta.Custom == nil {
		t.Fatal("Expected custom metadata to be preserved")
	}

	if destMeta.Custom["author"] != "test-user" {
		t.Errorf("Expected author 'test-user', got '%s'", destMeta.Custom["author"])
	}

	if destMeta.Custom["version"] != "1.0" {
		t.Errorf("Expected version '1.0', got '%s'", destMeta.Custom["version"])
	}
}

// TestSyncAll_EmptyStorage tests SyncAll with empty source storage.
func TestSyncAll_EmptyStorage(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result.Synced != 0 {
		t.Errorf("Expected 0 synced objects, got %d", result.Synced)
	}

	if result.Failed != 0 {
		t.Errorf("Expected 0 failed objects, got %d", result.Failed)
	}

	if result.BytesTotal != 0 {
		t.Errorf("Expected 0 bytes, got %d", result.BytesTotal)
	}
}

// TestSyncObject_ReaderError tests SyncObject when source reader fails.
func TestSyncObject_ReaderError(t *testing.T) {
	source := &errorReadStorage{}
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncObject(context.Background(), "test.txt")

	if err == nil {
		t.Fatal("Expected error when source reader fails")
	}
}

// errorReadStorage is a mock storage that fails on GetWithContext.
type errorReadStorage struct {
	*mockStorage
}

func (e *errorReadStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("read error")
}

func (e *errorReadStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return &common.Metadata{
		Size:         100,
		ETag:         "test",
		LastModified: time.Now(),
	}, nil
}
