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
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestChangeDetectorError tests change detection when destination list fails
func TestChangeDetectorError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add data to source
	source.data["test"] = []byte("data")
	source.objects["test"] = &common.Metadata{
		Size:         4,
		LastModified: time.Now(),
	}

	// Make source list fail
	source.listError = common.ErrInternal

	detector := NewChangeDetector(source, dest)

	// Should fail on source list
	_, err := detector.DetectChanges(ctx, "")
	if err == nil {
		t.Error("Expected error when source list fails")
	}
}

// TestSyncObjectAuditLogging tests that audit logs are written
func TestSyncObjectAuditLogging(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	testKey := "audit-test"
	testData := []byte("audit-data")
	source.data[testKey] = testData
	source.objects[testKey] = &common.Metadata{
		Size:         int64(len(testData)),
		LastModified: time.Now(),
	}

	auditLog := &capturingAuditLogger{}

	policy := common.ReplicationPolicy{
		ID:              "audit-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: auditLog,
		metrics:  NewReplicationMetrics(),
	}

	// Sync object
	_, err := syncer.SyncObject(ctx, testKey)
	if err != nil {
		t.Fatalf("SyncObject failed: %v", err)
	}

	// Verify audit log was called
	if !auditLog.called {
		t.Error("Expected audit log to be called")
	}
}

// TestSyncObjectPutError tests that put errors are audited
func TestSyncObjectPutError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	testKey := "put-error-test"
	testData := []byte("data")
	source.data[testKey] = testData
	source.objects[testKey] = &common.Metadata{
		Size:         int64(len(testData)),
		LastModified: time.Now(),
	}

	// Make put fail
	dest.putError = common.ErrInternal

	auditLog := &capturingAuditLogger{}

	policy := common.ReplicationPolicy{
		ID:              "put-error-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: auditLog,
		metrics:  NewReplicationMetrics(),
	}

	// Sync object - should fail
	_, err := syncer.SyncObject(ctx, testKey)
	if err == nil {
		t.Error("Expected error when put fails")
	}

	// Audit log should still be called for failure
	if !auditLog.called {
		t.Error("Expected audit log to be called even on failure")
	}
}

// capturingAuditLogger captures whether audit methods were called
type capturingAuditLogger struct {
	called bool
}

func (c *capturingAuditLogger) LogEvent(ctx context.Context, event *audit.AuditEvent) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result audit.Result, err error) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) LogObjectMutation(ctx context.Context, eventType audit.EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result audit.Result, err error) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result audit.Result, err error) error {
	c.called = true
	return nil
}

func (c *capturingAuditLogger) SetLevel(level adapters.LogLevel) {
}

func (c *capturingAuditLogger) GetLevel() adapters.LogLevel {
	return adapters.InfoLevel
}

// TestNoopEncrypter tests the noop encrypter implementation
func TestNoopEncrypter(t *testing.T) {
	ctx := context.Background()
	factory := NewNoopEncrypterFactory()

	// Test GetEncrypter
	enc, err := factory.GetEncrypter("test-key")
	if err != nil {
		t.Fatalf("GetEncrypter failed: %v", err)
	}

	if enc == nil {
		t.Fatal("Expected non-nil encrypter")
	}

	// Test Algorithm
	if enc.Algorithm() != "none" {
		t.Errorf("Expected algorithm 'none', got %s", enc.Algorithm())
	}

	// Test KeyID
	if enc.KeyID() != "" {
		t.Errorf("Expected keyID '', got %s", enc.KeyID())
	}

	// Test Encrypt (should pass through)
	plaintext := []byte("test data")
	encrypted, err := enc.Encrypt(ctx, io.NopCloser(bytes.NewReader(plaintext)))
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	result, err := io.ReadAll(encrypted)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(result) != string(plaintext) {
		t.Errorf("Expected passthrough encryption, got different data")
	}

	encrypted.Close()

	// Test Decrypt (should pass through)
	decrypted, err := enc.Decrypt(ctx, io.NopCloser(bytes.NewReader(plaintext)))
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	result, err = io.ReadAll(decrypted)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(result) != string(plaintext) {
		t.Errorf("Expected passthrough decryption, got different data")
	}

	decrypted.Close()

	// Test DefaultKeyID
	if factory.DefaultKeyID() != "" {
		t.Errorf("Expected default key ID '', got %s", factory.DefaultKeyID())
	}

	// Test Close
	err = factory.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
