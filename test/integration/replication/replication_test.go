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

//go:build integration && local

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// TestHelper provides common test setup and teardown functionality
type TestHelper struct {
	t            *testing.T
	testDir      string
	sourceDir    string
	destDir      string
	sourceStore  common.Storage
	destStore    common.Storage
	logger       adapters.Logger
	auditLog     audit.AuditLogger
	cleanupFuncs []func()
}

// NewTestHelper creates a new test helper with isolated storage directories
func NewTestHelper(t *testing.T) *TestHelper {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("replication-test-%d", time.Now().UnixNano()))
	sourceDir := filepath.Join(testDir, "source")
	destDir := filepath.Join(testDir, "dest")

	// Create directories
	require.NoError(t, os.MkdirAll(sourceDir, 0755))
	require.NoError(t, os.MkdirAll(destDir, 0755))

	// Create storage backends
	sourceStore, err := factory.NewStorage("local", map[string]string{"path": sourceDir})
	require.NoError(t, err)

	destStore, err := factory.NewStorage("local", map[string]string{"path": destDir})
	require.NoError(t, err)

	return &TestHelper{
		t:           t,
		testDir:     testDir,
		sourceDir:   sourceDir,
		destDir:     destDir,
		sourceStore: sourceStore,
		destStore:   destStore,
		logger:      &mockLogger{},
		auditLog:    &mockAuditLogger{},
	}
}

// Cleanup removes all test data and releases resources
func (h *TestHelper) Cleanup() {
	for _, fn := range h.cleanupFuncs {
		fn()
	}
	os.RemoveAll(h.testDir)
}

// CreateTestFile creates a test file in the source storage
func (h *TestHelper) CreateTestFile(key string, content []byte) {
	h.t.Helper()
	err := h.sourceStore.Put(key, bytes.NewReader(content))
	require.NoError(h.t, err, "Failed to create test file: %s", key)
}

// VerifyFileExists verifies that a file exists in destination with expected content
func (h *TestHelper) VerifyFileExists(key string, expectedContent []byte) {
	h.t.Helper()

	reader, err := h.destStore.GetWithContext(context.Background(), key)
	require.NoError(h.t, err, "File should exist in destination: %s", key)
	defer reader.Close()

	actualContent, err := io.ReadAll(reader)
	require.NoError(h.t, err)
	assert.Equal(h.t, expectedContent, actualContent, "Content mismatch for key: %s", key)
}

// VerifyFileNotExists verifies that a file does not exist in destination
func (h *TestHelper) VerifyFileNotExists(key string) {
	h.t.Helper()

	_, err := h.destStore.GetWithContext(context.Background(), key)
	assert.Error(h.t, err, "File should not exist in destination: %s", key)
}

// VerifyMetadata verifies that metadata is preserved in destination
func (h *TestHelper) VerifyMetadata(key string) {
	h.t.Helper()

	srcMeta, err := h.sourceStore.GetMetadata(context.Background(), key)
	require.NoError(h.t, err)

	destMeta, err := h.destStore.GetMetadata(context.Background(), key)
	require.NoError(h.t, err)

	assert.Equal(h.t, srcMeta.Size, destMeta.Size, "Size mismatch")
	assert.Equal(h.t, srcMeta.ContentType, destMeta.ContentType, "ContentType mismatch")
}

// VerifyEncryptedAtRest verifies that a file is encrypted on disk
func (h *TestHelper) VerifyEncryptedAtRest(dir, key string, originalContent []byte) {
	h.t.Helper()

	filePath := filepath.Join(dir, key)
	encryptedData, err := os.ReadFile(filePath)
	require.NoError(h.t, err)

	// Encrypted data should not match original content
	assert.NotEqual(h.t, originalContent, encryptedData,
		"File should be encrypted at rest: %s", key)
}

// CreateEncrypterFactory creates a test encryption factory with the given key
func (h *TestHelper) CreateEncrypterFactory(keyID string) common.EncrypterFactory {
	return &testEncrypterFactory{
		keyID:     keyID,
		encrypter: &testEncrypter{keyID: keyID},
	}
}

// testEncrypter is a simple test encrypter that adds/removes a prefix
type testEncrypter struct {
	keyID string
}

func (e *testEncrypter) Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error) {
	data, err := io.ReadAll(plaintext)
	if err != nil {
		return nil, err
	}
	// Simple encryption: prepend key ID and reverse bytes
	prefix := []byte(fmt.Sprintf("[ENC:%s]", e.keyID))
	encrypted := append(prefix, reverseBytes(data)...)
	return io.NopCloser(bytes.NewReader(encrypted)), nil
}

func (e *testEncrypter) Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error) {
	data, err := io.ReadAll(ciphertext)
	if err != nil {
		return nil, err
	}
	// Simple decryption: remove prefix and reverse bytes
	prefix := []byte(fmt.Sprintf("[ENC:%s]", e.keyID))
	if len(data) < len(prefix) {
		return nil, fmt.Errorf("invalid encrypted data")
	}
	decrypted := reverseBytes(data[len(prefix):])
	return io.NopCloser(bytes.NewReader(decrypted)), nil
}

func (e *testEncrypter) Algorithm() string {
	return "TEST-ENCRYPTION"
}

func (e *testEncrypter) KeyID() string {
	return e.keyID
}

func reverseBytes(data []byte) []byte {
	result := make([]byte, len(data))
	for i, b := range data {
		result[len(data)-1-i] = b
	}
	return result
}

// testEncrypterFactory creates test encrypters
type testEncrypterFactory struct {
	keyID     string
	encrypter *testEncrypter
}

func (f *testEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	if keyID == "" {
		keyID = f.keyID
	}
	return &testEncrypter{keyID: keyID}, nil
}

func (f *testEncrypterFactory) DefaultKeyID() string {
	return f.keyID
}

func (f *testEncrypterFactory) Close() error {
	return nil
}

// Mock implementations - matching actual interfaces
type mockLogger struct{}

func (m *mockLogger) Debug(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) Info(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Warn(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Error(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) WithFields(fields ...adapters.Field) adapters.Logger              { return m }
func (m *mockLogger) WithContext(ctx context.Context) adapters.Logger                  { return m }
func (m *mockLogger) SetLevel(level adapters.LogLevel)                                 {}
func (m *mockLogger) GetLevel() adapters.LogLevel                                      { return adapters.InfoLevel }

type mockAuditLogger struct{}

func (m *mockAuditLogger) LogEvent(ctx context.Context, event *audit.AuditEvent) error {
	return nil
}
func (m *mockAuditLogger) LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error {
	return nil
}
func (m *mockAuditLogger) LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error {
	return nil
}
func (m *mockAuditLogger) LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result audit.Result, err error) error {
	return nil
}
func (m *mockAuditLogger) LogObjectMutation(ctx context.Context, eventType audit.EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result audit.Result, err error) error {
	return nil
}
func (m *mockAuditLogger) LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result audit.Result, err error) error {
	return nil
}
func (m *mockAuditLogger) SetLevel(level adapters.LogLevel) {}
func (m *mockAuditLogger) GetLevel() adapters.LogLevel      { return adapters.InfoLevel }

// Test Cases

// TestReplication_LocalToLocal_NoEncryption tests basic replication without encryption
func TestReplication_LocalToLocal_NoEncryption(t *testing.T) {
	h := NewTestHelper(t)
	defer h.Cleanup()

	// Create test files in source
	testFiles := map[string][]byte{
		"file1.txt":        []byte("content of file 1"),
		"file2.txt":        []byte("content of file 2"),
		"subdir/file3.txt": []byte("content of file 3 in subdir"),
	}

	for key, content := range testFiles {
		h.CreateTestFile(key, content)
	}

	// Create replication policy
	policy := common.ReplicationPolicy{
		ID:                  "test-local-to-local",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": h.sourceDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": h.destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
	}

	// Create syncer with noop encryption
	noopFactory := replication.NewNoopEncrypterFactory()
	syncer, err := replication.NewSyncer(
		policy,
		noopFactory,
		noopFactory,
		noopFactory,
		h.logger,
		h.auditLog,
	)
	require.NoError(t, err)
	defer syncer.Close()

	// Execute sync
	result, err := syncer.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, len(testFiles), result.Synced, "Should sync all files")
	assert.Equal(t, 0, result.Failed, "Should have no failures")

	// Verify all files were synced correctly
	for key, content := range testFiles {
		h.VerifyFileExists(key, content)
		h.VerifyMetadata(key)
	}
}

// TestReplication_LocalToLocal_WithPrefixFilter tests replication with prefix filtering
func TestReplication_LocalToLocal_WithPrefixFilter(t *testing.T) {
	h := NewTestHelper(t)
	defer h.Cleanup()

	// Create test files in source
	testFiles := map[string][]byte{
		"logs/app.log":     []byte("application logs"),
		"logs/error.log":   []byte("error logs"),
		"data/file1.txt":   []byte("data file 1"),
		"data/file2.txt":   []byte("data file 2"),
		"config/app.yaml":  []byte("config data"),
	}

	for key, content := range testFiles {
		h.CreateTestFile(key, content)
	}

	// Create replication policy with prefix filter for "logs/" only
	policy := common.ReplicationPolicy{
		ID:                  "test-prefix-filter",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": h.sourceDir},
		SourcePrefix:        "logs/",
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": h.destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
	}

	// Create syncer
	noopFactory := replication.NewNoopEncrypterFactory()
	syncer, err := replication.NewSyncer(
		policy,
		noopFactory,
		noopFactory,
		noopFactory,
		h.logger,
		h.auditLog,
	)
	require.NoError(t, err)
	defer syncer.Close()

	// Execute sync
	result, err := syncer.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, result.Synced, "Should sync only logs/ files")

	// Verify only logs/ files were synced
	h.VerifyFileExists("logs/app.log", testFiles["logs/app.log"])
	h.VerifyFileExists("logs/error.log", testFiles["logs/error.log"])

	// Verify other files were NOT synced
	h.VerifyFileNotExists("data/file1.txt")
	h.VerifyFileNotExists("data/file2.txt")
	h.VerifyFileNotExists("config/app.yaml")
}

// TestReplication_LocalToLocal_BackendEncryption tests replication with backend at-rest encryption
func TestReplication_LocalToLocal_BackendEncryption(t *testing.T) {
	h := NewTestHelper(t)
	defer h.Cleanup()

	// Create backend encryption factory FIRST
	backendFactory := h.CreateEncrypterFactory("backend-key")

	// Set encryption factory on source store so files are written encrypted
	if localBackend, ok := h.sourceStore.(*local.Local); ok {
		localBackend.SetAtRestEncrypterFactory(backendFactory)
	}

	// Create test files (now written encrypted)
	testFiles := map[string][]byte{
		"file1.txt": []byte("sensitive data 1"),
		"file2.txt": []byte("sensitive data 2"),
	}

	for key, content := range testFiles {
		h.CreateTestFile(key, content)
	}

	// Create replication policy with backend encryption enabled
	policy := common.ReplicationPolicy{
		ID:                  "test-backend-encryption",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": h.sourceDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": h.destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		Encryption: &common.EncryptionPolicy{
			Backend: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "backend-key",
			},
		},
	}

	// Create syncer with backend encryption
	noopFactory := replication.NewNoopEncrypterFactory()
	syncer, err := replication.NewSyncer(
		policy,
		backendFactory,
		noopFactory,
		noopFactory,
		h.logger,
		h.auditLog,
	)
	require.NoError(t, err)
	defer syncer.Close()

	// Execute sync
	result, err := syncer.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, len(testFiles), result.Synced)

	// Set up dest store with backend encryption for verification
	if localBackend, ok := h.destStore.(*local.Local); ok {
		localBackend.SetAtRestEncrypterFactory(backendFactory)
	}

	// Verify files are encrypted at rest in destination
	for key, content := range testFiles {
		// Verify content can be read and decrypted correctly
		reader, err := h.destStore.GetWithContext(context.Background(), key)
		require.NoError(t, err, "File should exist in destination: %s", key)
		actualContent, err := io.ReadAll(reader)
		reader.Close()
		require.NoError(t, err)
		assert.Equal(t, content, actualContent, "Content mismatch for key: %s", key)

		h.VerifyEncryptedAtRest(h.destDir, key, content)
	}
}

// TestReplication_LocalToLocal_ThreeLayerEncryption tests all three encryption layers
func TestReplication_LocalToLocal_ThreeLayerEncryption(t *testing.T) {
	h := NewTestHelper(t)
	defer h.Cleanup()

	// Create three different encryption factories
	backendFactory := h.CreateEncrypterFactory("backend-key")
	sourceFactory := h.CreateEncrypterFactory("source-dek")
	destFactory := h.CreateEncrypterFactory("dest-dek")

	// Set up source store with backend encryption AND source DEK encryption
	// so that files are written with the same encryption the syncer expects
	if localBackend, ok := h.sourceStore.(*local.Local); ok {
		localBackend.SetAtRestEncrypterFactory(backendFactory)
	}
	// Wrap with source DEK encryption for client-side layer
	encryptedSourceStore := common.NewEncryptedStorage(h.sourceStore, sourceFactory)

	// Create test file using the encrypted source store
	testKey := "secret.txt"
	testContent := []byte("triple encrypted content")
	err := encryptedSourceStore.Put(testKey, bytes.NewReader(testContent))
	require.NoError(t, err, "Failed to create test file")

	// Create policy with all three encryption layers
	policy := common.ReplicationPolicy{
		ID:                  "test-three-layer",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": h.sourceDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": h.destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		Encryption: &common.EncryptionPolicy{
			Backend: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "backend-key",
			},
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-dek",
			},
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-dek",
			},
		},
	}

	// Create syncer with all three encryption layers
	syncer, err := replication.NewSyncer(
		policy,
		backendFactory,
		sourceFactory,
		destFactory,
		h.logger,
		h.auditLog,
	)
	require.NoError(t, err)
	defer syncer.Close()

	// Execute sync
	result, err := syncer.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.Synced)
	assert.Equal(t, 0, result.Failed)

	// Set up dest store with backend encryption for reading
	if localBackend, ok := h.destStore.(*local.Local); ok {
		localBackend.SetAtRestEncrypterFactory(backendFactory)
	}

	// Read from dest with backend decryption
	backendDecryptedReader, err := h.destStore.GetWithContext(context.Background(), testKey)
	require.NoError(t, err, "File should exist in destination")

	// Manually decrypt with dest DEK (the outer encryption layer)
	destDecrypter, err := destFactory.GetEncrypter("dest-dek")
	require.NoError(t, err)
	decryptedReader, err := destDecrypter.Decrypt(context.Background(), backendDecryptedReader)
	backendDecryptedReader.Close()
	require.NoError(t, err)
	defer decryptedReader.Close()

	actualContent, err := io.ReadAll(decryptedReader)
	require.NoError(t, err)
	assert.Equal(t, testContent, actualContent, "Content mismatch after decryption")

	// Verify it's encrypted at rest
	h.VerifyEncryptedAtRest(h.destDir, testKey, testContent)
}

// TestReplication_EmptySource tests syncing from an empty source
func TestReplication_EmptySource(t *testing.T) {
	h := NewTestHelper(t)
	defer h.Cleanup()

	// Don't create any files in source

	policy := common.ReplicationPolicy{
		ID:                  "test-empty-source",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": h.sourceDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": h.destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
	}

	noopFactory := replication.NewNoopEncrypterFactory()
	syncer, err := replication.NewSyncer(
		policy,
		noopFactory,
		noopFactory,
		noopFactory,
		h.logger,
		h.auditLog,
	)
	require.NoError(t, err)
	defer syncer.Close()

	result, err := syncer.SyncAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, result.Synced, "Should sync 0 files from empty source")
	assert.Equal(t, 0, result.Failed)
}
