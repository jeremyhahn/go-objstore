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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockReplicationClient is a mock implementation of the Client interface for replication testing
type MockReplicationClient struct {
	mock.Mock
}

// Object operations (required by Client interface)
func (m *MockReplicationClient) Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
	args := m.Called(ctx, key, reader, metadata)
	return args.Error(0)
}

func (m *MockReplicationClient) Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(io.ReadCloser), args.Get(1).(*common.Metadata), args.Error(2)
}

func (m *MockReplicationClient) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockReplicationClient) Exists(ctx context.Context, key string) (bool, error) {
	args := m.Called(ctx, key)
	return args.Bool(0), args.Error(1)
}

func (m *MockReplicationClient) List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	args := m.Called(ctx, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.ListResult), args.Error(1)
}

func (m *MockReplicationClient) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.Metadata), args.Error(1)
}

func (m *MockReplicationClient) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	args := m.Called(ctx, key, metadata)
	return args.Error(0)
}

func (m *MockReplicationClient) Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error {
	args := m.Called(ctx, key, destinationType, destinationSettings)
	return args.Error(0)
}

// Lifecycle policy operations
func (m *MockReplicationClient) AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error {
	args := m.Called(ctx, policy)
	return args.Error(0)
}

func (m *MockReplicationClient) RemovePolicy(ctx context.Context, policyID string) error {
	args := m.Called(ctx, policyID)
	return args.Error(0)
}

func (m *MockReplicationClient) GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.LifecyclePolicy), args.Error(1)
}

func (m *MockReplicationClient) ApplyPolicies(ctx context.Context) (int, int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Int(1), args.Error(2)
}

// Replication operations
func (m *MockReplicationClient) AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error {
	args := m.Called(ctx, policy)
	return args.Error(0)
}

func (m *MockReplicationClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	args := m.Called(ctx, policyID)
	return args.Error(0)
}

func (m *MockReplicationClient) GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error) {
	args := m.Called(ctx, policyID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.ReplicationPolicy), args.Error(1)
}

func (m *MockReplicationClient) GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]common.ReplicationPolicy), args.Error(1)
}

func (m *MockReplicationClient) TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error) {
	args := m.Called(ctx, policyID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.SyncResult), args.Error(1)
}

func (m *MockReplicationClient) GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error) {
	args := m.Called(ctx, policyID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*replication.ReplicationStatus), args.Error(1)
}

func (m *MockReplicationClient) Health(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockReplicationClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestParseSettings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected map[string]string
	}{
		{
			name:     "empty input",
			input:    []string{},
			expected: map[string]string{},
		},
		{
			name:  "single setting",
			input: []string{"key1=value1"},
			expected: map[string]string{
				"key1": "value1",
			},
		},
		{
			name:  "multiple settings",
			input: []string{"key1=value1", "key2=value2", "key3=value3"},
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			name:  "value with equals sign",
			input: []string{"key1=value=with=equals"},
			expected: map[string]string{
				"key1": "value=with=equals",
			},
		},
		{
			name:     "invalid setting (no equals)",
			input:    []string{"invalid"},
			expected: map[string]string{},
		},
		{
			name:  "mixed valid and invalid",
			input: []string{"valid=value", "invalid", "another=test"},
			expected: map[string]string{
				"valid":   "value",
				"another": "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSettings(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddReplicationPolicyCommand(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		sourceBackend  string
		destBackend    string
		sourceSettings map[string]string
		destSettings   map[string]string
		prefix         string
		interval       time.Duration
		mode           string
		backendKey     string
		sourceDEK      string
		destDEK        string
		setupMock      func(*MockReplicationClient)
		expectError    bool
		expectedError  error
	}{
		{
			name:           "successful add without encryption",
			id:             "test-policy",
			sourceBackend:  "local",
			destBackend:    "s3",
			sourceSettings: map[string]string{"path": "/data"},
			destSettings:   map[string]string{"bucket": "backup"},
			prefix:         "logs/",
			interval:       5 * time.Minute,
			mode:           "transparent",
			setupMock: func(m *MockReplicationClient) {
				m.On("AddReplicationPolicy", mock.Anything, mock.MatchedBy(func(p common.ReplicationPolicy) bool {
					return p.ID == "test-policy" &&
						p.SourceBackend == "local" &&
						p.DestinationBackend == "s3" &&
						p.ReplicationMode == common.ReplicationModeTransparent &&
						p.Enabled == true
				})).Return(nil)
			},
			expectError: false,
		},
		{
			name:           "successful add with all encryption layers",
			id:             "encrypted-policy",
			sourceBackend:  "local",
			destBackend:    "s3",
			sourceSettings: map[string]string{"path": "/data"},
			destSettings:   map[string]string{"bucket": "backup"},
			prefix:         "",
			interval:       10 * time.Minute,
			mode:           "transparent",
			backendKey:     "backend-key-123",
			sourceDEK:      "source-dek-456",
			destDEK:        "dest-dek-789",
			setupMock: func(m *MockReplicationClient) {
				m.On("AddReplicationPolicy", mock.Anything, mock.MatchedBy(func(p common.ReplicationPolicy) bool {
					return p.ID == "encrypted-policy" &&
						p.Encryption != nil &&
						p.Encryption.Backend != nil &&
						p.Encryption.Backend.Enabled &&
						p.Encryption.Backend.DefaultKey == "backend-key-123" &&
						p.Encryption.Source != nil &&
						p.Encryption.Source.Enabled &&
						p.Encryption.Source.DefaultKey == "source-dek-456" &&
						p.Encryption.Destination != nil &&
						p.Encryption.Destination.Enabled &&
						p.Encryption.Destination.DefaultKey == "dest-dek-789"
				})).Return(nil)
			},
			expectError: false,
		},
		{
			name:           "opaque mode replication",
			id:             "opaque-policy",
			sourceBackend:  "s3",
			destBackend:    "s3",
			sourceSettings: map[string]string{"bucket": "source"},
			destSettings:   map[string]string{"bucket": "dest"},
			prefix:         "backups/",
			interval:       15 * time.Minute,
			mode:           "opaque",
			setupMock: func(m *MockReplicationClient) {
				m.On("AddReplicationPolicy", mock.Anything, mock.MatchedBy(func(p common.ReplicationPolicy) bool {
					return p.ReplicationMode == common.ReplicationModeOpaque
				})).Return(nil)
			},
			expectError: false,
		},
		{
			name:           "client returns error",
			id:             "fail-policy",
			sourceBackend:  "local",
			destBackend:    "s3",
			sourceSettings: map[string]string{"path": "/data"},
			destSettings:   map[string]string{"bucket": "backup"},
			interval:       5 * time.Minute,
			mode:           "transparent",
			setupMock: func(m *MockReplicationClient) {
				m.On("AddReplicationPolicy", mock.Anything, mock.Anything).
					Return(errors.New("policy already exists"))
			},
			expectError:   true,
			expectedError: errors.New("policy already exists"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			err := ctx.AddReplicationPolicyCommand(
				tt.id, tt.sourceBackend, tt.destBackend,
				tt.sourceSettings, tt.destSettings,
				tt.prefix, tt.interval, tt.mode,
				tt.backendKey, tt.sourceDEK, tt.destDEK,
			)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedError != nil {
					assert.EqualError(t, err, tt.expectedError.Error())
				}
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestAddReplicationPolicyCommand_LocalStorage(t *testing.T) {
	// Test with local storage (should return ErrReplicationNotSupported)
	ctx := &CommandContext{
		Storage: nil, // Local storage doesn't support replication via CLI yet
		Config:  &Config{Backend: "local"},
	}

	err := ctx.AddReplicationPolicyCommand(
		"test", "local", "s3",
		map[string]string{"path": "/data"},
		map[string]string{"bucket": "backup"},
		"", 5*time.Minute, "transparent",
		"", "", "",
	)

	assert.ErrorIs(t, err, common.ErrReplicationNotSupported)
}

func TestRemoveReplicationPolicyCommand(t *testing.T) {
	tests := []struct {
		name        string
		policyID    string
		setupMock   func(*MockReplicationClient)
		expectError bool
	}{
		{
			name:     "successful remove",
			policyID: "test-policy",
			setupMock: func(m *MockReplicationClient) {
				m.On("RemoveReplicationPolicy", mock.Anything, "test-policy").Return(nil)
			},
			expectError: false,
		},
		{
			name:     "policy not found",
			policyID: "nonexistent",
			setupMock: func(m *MockReplicationClient) {
				m.On("RemoveReplicationPolicy", mock.Anything, "nonexistent").
					Return(errors.New("policy not found"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			err := ctx.RemoveReplicationPolicyCommand(tt.policyID)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetReplicationPolicyCommand(t *testing.T) {
	testPolicy := &common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            true,
	}

	tests := []struct {
		name           string
		policyID       string
		setupMock      func(*MockReplicationClient)
		expectError    bool
		expectedPolicy *common.ReplicationPolicy
	}{
		{
			name:     "successful get",
			policyID: "test-policy",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationPolicy", mock.Anything, "test-policy").Return(testPolicy, nil)
			},
			expectError:    false,
			expectedPolicy: testPolicy,
		},
		{
			name:     "policy not found",
			policyID: "nonexistent",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationPolicy", mock.Anything, "nonexistent").
					Return((*common.ReplicationPolicy)(nil), errors.New("policy not found"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			policy, err := ctx.GetReplicationPolicyCommand(tt.policyID)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, policy)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPolicy, policy)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestListReplicationPoliciesCommand(t *testing.T) {
	testPolicies := []common.ReplicationPolicy{
		{
			ID:                 "policy1",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			Enabled:            true,
		},
		{
			ID:                 "policy2",
			SourceBackend:      "s3",
			DestinationBackend: "gcs",
			Enabled:            false,
		},
	}

	tests := []struct {
		name             string
		setupMock        func(*MockReplicationClient)
		expectError      bool
		expectedPolicies []common.ReplicationPolicy
	}{
		{
			name: "successful list",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationPolicies", mock.Anything).Return(testPolicies, nil)
			},
			expectError:      false,
			expectedPolicies: testPolicies,
		},
		{
			name: "empty list",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationPolicies", mock.Anything).Return([]common.ReplicationPolicy{}, nil)
			},
			expectError:      false,
			expectedPolicies: []common.ReplicationPolicy{},
		},
		{
			name: "error fetching policies",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationPolicies", mock.Anything).
					Return(([]common.ReplicationPolicy)(nil), errors.New("connection error"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			policies, err := ctx.ListReplicationPoliciesCommand()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPolicies, policies)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestTriggerReplicationCommand(t *testing.T) {
	testResult := &common.SyncResult{
		PolicyID:   "test-policy",
		Synced:     10,
		Deleted:    2,
		Failed:     1,
		BytesTotal: 1024000,
		Duration:   30 * time.Second,
		Errors:     []string{"error syncing object1"},
	}

	tests := []struct {
		name           string
		policyID       string
		setupMock      func(*MockReplicationClient)
		expectError    bool
		expectedResult *common.SyncResult
	}{
		{
			name:     "successful sync specific policy",
			policyID: "test-policy",
			setupMock: func(m *MockReplicationClient) {
				m.On("TriggerReplication", mock.Anything, "test-policy").Return(testResult, nil)
			},
			expectError:    false,
			expectedResult: testResult,
		},
		{
			name:     "successful sync all policies",
			policyID: "",
			setupMock: func(m *MockReplicationClient) {
				m.On("TriggerReplication", mock.Anything, "").Return(&common.SyncResult{
					Synced:     20,
					BytesTotal: 2048000,
					Duration:   60 * time.Second,
				}, nil)
			},
			expectError: false,
		},
		{
			name:     "sync error",
			policyID: "failing-policy",
			setupMock: func(m *MockReplicationClient) {
				m.On("TriggerReplication", mock.Anything, "failing-policy").
					Return((*common.SyncResult)(nil), errors.New("sync failed"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			result, err := ctx.TriggerReplicationCommand(tt.policyID)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.expectedResult != nil {
					assert.Equal(t, tt.expectedResult.PolicyID, result.PolicyID)
					assert.Equal(t, tt.expectedResult.Synced, result.Synced)
				}
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestFormatReplicationPoliciesResult(t *testing.T) {
	policies := []common.ReplicationPolicy{
		{
			ID:                 "policy1",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			SourcePrefix:       "logs/",
			ReplicationMode:    common.ReplicationModeTransparent,
			Enabled:            true,
			CheckInterval:      5 * time.Minute,
			LastSyncTime:       time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	tests := []struct {
		name     string
		format   OutputFormat
		policies []common.ReplicationPolicy
	}{
		{
			name:     "text format",
			format:   FormatText,
			policies: policies,
		},
		{
			name:     "json format",
			format:   FormatJSON,
			policies: policies,
		},
		{
			name:     "table format",
			format:   FormatTable,
			policies: policies,
		},
		{
			name:     "empty policies text",
			format:   FormatText,
			policies: []common.ReplicationPolicy{},
		},
		{
			name:     "empty policies table",
			format:   FormatTable,
			policies: []common.ReplicationPolicy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatReplicationPoliciesResult(tt.policies, tt.format)
			assert.NotEmpty(t, result)

			if len(tt.policies) == 0 {
				assert.Contains(t, result, "No replication policies")
			}
		})
	}
}

func TestFormatSyncResult(t *testing.T) {
	result := &common.SyncResult{
		PolicyID:   "test-policy",
		Synced:     100,
		Deleted:    5,
		Failed:     2,
		BytesTotal: 1024000,
		Duration:   45 * time.Second,
		Errors:     []string{"error1", "error2"},
	}

	tests := []struct {
		name   string
		format OutputFormat
	}{
		{
			name:   "text format",
			format: FormatText,
		},
		{
			name:   "json format",
			format: FormatJSON,
		},
		{
			name:   "table format",
			format: FormatTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := FormatSyncResult(result, tt.format)
			assert.NotEmpty(t, output)

			// Verify key information is present
			if tt.format == FormatText || tt.format == FormatTable {
				assert.Contains(t, output, "Synced")
				assert.Contains(t, output, "100")
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "no truncation needed",
			input:    "short",
			maxLen:   10,
			expected: "short",
		},
		{
			name:     "exact length",
			input:    "exactly10c",
			maxLen:   10,
			expected: "exactly10c",
		},
		{
			name:     "needs truncation",
			input:    "this is a very long string that needs truncation",
			maxLen:   20,
			expected: "this is a very lo...",
		},
		{
			name:     "very short max length",
			input:    "hello",
			maxLen:   3,
			expected: "...",
		},
		{
			name:     "empty string",
			input:    "",
			maxLen:   10,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncate(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
			assert.LessOrEqual(t, len(result), tt.maxLen)
		})
	}
}

func TestFormatReplicationPoliciesWithEncryption(t *testing.T) {
	policies := []common.ReplicationPolicy{
		{
			ID:                 "encrypted-policy",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			ReplicationMode:    common.ReplicationModeTransparent,
			Enabled:            true,
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
		},
	}

	result := FormatReplicationPoliciesResult(policies, FormatText)
	assert.Contains(t, result, "Backend Encryption")
	assert.Contains(t, result, "Source DEK")
	assert.Contains(t, result, "Destination DEK")
	assert.Contains(t, result, "backend-key")
	assert.Contains(t, result, "source-key")
	assert.Contains(t, result, "dest-key")
}

func TestFormatSyncResultWithErrors(t *testing.T) {
	result := &common.SyncResult{
		PolicyID: "test",
		Synced:   10,
		Failed:   2,
		Errors:   []string{"error syncing file1.txt", "error syncing file2.txt"},
	}

	output := FormatSyncResult(result, FormatText)
	assert.Contains(t, output, "Errors:")
	assert.Contains(t, output, "error syncing file1.txt")
	assert.Contains(t, output, "error syncing file2.txt")
}

func TestGetReplicationStatusCommand(t *testing.T) {
	testStatus := &replication.ReplicationStatus{
		PolicyID:            "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		Enabled:             true,
		TotalObjectsSynced:  1000,
		TotalObjectsDeleted: 50,
		TotalBytesSynced:    10485760,
		TotalErrors:         5,
		LastSyncTime:        time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		AverageSyncDuration: 30 * time.Second,
		SyncCount:           100,
	}

	tests := []struct {
		name           string
		policyID       string
		setupMock      func(*MockReplicationClient)
		expectError    bool
		expectedStatus *replication.ReplicationStatus
	}{
		{
			name:     "successful get status",
			policyID: "test-policy",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationStatus", mock.Anything, "test-policy").Return(testStatus, nil)
			},
			expectError:    false,
			expectedStatus: testStatus,
		},
		{
			name:     "status not found",
			policyID: "nonexistent",
			setupMock: func(m *MockReplicationClient) {
				m.On("GetReplicationStatus", mock.Anything, "nonexistent").
					Return((*replication.ReplicationStatus)(nil), errors.New("policy not found"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockReplicationClient)
			tt.setupMock(mockClient)

			ctx := &CommandContext{
				Client: mockClient,
				Config: &Config{},
			}

			status, err := ctx.GetReplicationStatusCommand(tt.policyID)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, status)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStatus, status)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestFormatReplicationStatus(t *testing.T) {
	status := &replication.ReplicationStatus{
		PolicyID:            "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		Enabled:             true,
		TotalObjectsSynced:  1000,
		TotalObjectsDeleted: 50,
		TotalBytesSynced:    10485760,
		TotalErrors:         5,
		LastSyncTime:        time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		AverageSyncDuration: 30 * time.Second,
		SyncCount:           100,
	}

	tests := []struct {
		name   string
		format OutputFormat
	}{
		{
			name:   "text format",
			format: FormatText,
		},
		{
			name:   "json format",
			format: FormatJSON,
		},
		{
			name:   "table format",
			format: FormatTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := FormatReplicationStatus(status, tt.format)
			assert.NotEmpty(t, output)

			// Verify key information is present
			if tt.format == FormatText || tt.format == FormatTable {
				assert.Contains(t, output, "test-policy")
				assert.Contains(t, output, "1000")
			}
		})
	}
}
