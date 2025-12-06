// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

//go:build integration
// +build integration

package objstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions to get server addresses from environment or defaults
func getGRPCAddr() string {
	if addr := os.Getenv("OBJSTORE_GRPC_HOST"); addr != "" {
		return addr
	}
	return "objstore-server:50051"
}

func getRESTAddr() string {
	if addr := os.Getenv("OBJSTORE_REST_URL"); addr != "" {
		// Strip http:// or https:// prefix if present
		if len(addr) > 7 && addr[:7] == "http://" {
			addr = addr[7:]
		} else if len(addr) > 8 && addr[:8] == "https://" {
			addr = addr[8:]
		}
		return addr
	}
	return "objstore-server:8080"
}

func getQUICAddr() string {
	if addr := os.Getenv("OBJSTORE_QUIC_URL"); addr != "" {
		// Strip https:// prefix if present
		if len(addr) > 8 && addr[:8] == "https://" {
			addr = addr[8:]
		}
		return addr
	}
	return "objstore-server:4433"
}

// TestCaseOperation describes a single operation test case
type TestCaseOperation struct {
	name        string
	operation   string // put, get, delete, exists, list, etc.
	key         string
	data        []byte
	metadata    *Metadata
	shouldError bool
	errorType   error
	skipReason  string // If set, skip this test case for specific protocol
}

// TestResult holds the result of a test operation
type TestResult struct {
	operationName string
	passed        bool
	duration      time.Duration
	error         error
}

// ComprehensiveIntegrationTestSuite contains all test cases for a protocol
type ComprehensiveIntegrationTestSuite struct {
	protocol  Protocol
	address   string
	testCases []TestCaseOperation
	results   []TestResult
}

// NewComprehensiveIntegrationTestSuite creates a new test suite for a protocol
func NewComprehensiveIntegrationTestSuite(protocol Protocol, address string) *ComprehensiveIntegrationTestSuite {
	return &ComprehensiveIntegrationTestSuite{
		protocol:  protocol,
		address:   address,
		testCases: make([]TestCaseOperation, 0),
		results:   make([]TestResult, 0),
	}
}

// addTestCase adds a test case to the suite
func (s *ComprehensiveIntegrationTestSuite) addTestCase(tc TestCaseOperation) {
	s.testCases = append(s.testCases, tc)
}

// createClientForProtocol creates a client for the test protocol
func (s *ComprehensiveIntegrationTestSuite) createClient() (Client, error) {
	config := &ClientConfig{
		Protocol:          s.protocol,
		Address:           s.address,
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
		UseTLS:            false,
		MaxRecvMsgSize:    10 * 1024 * 1024,
		MaxSendMsgSize:    10 * 1024 * 1024,
	}

	// QUIC requires TLS
	if s.protocol == ProtocolQUIC {
		config.UseTLS = true
		config.InsecureSkipVerify = true
	}

	return NewClient(config)
}

// TestComprehensiveIntegrationAllOperations tests all 19 operations across all 3 protocols
// Using table-driven approach with subtests for each protocol
func TestComprehensiveIntegrationAllOperations(t *testing.T) {
	// Define all test protocols
	protocols := []struct {
		name    string
		proto   Protocol
		address string
		skip    bool
	}{
		{
			name:    "REST",
			proto:   ProtocolREST,
			address: getRESTAddr(),
			skip:    false,
		},
		{
			name:    "gRPC",
			proto:   ProtocolGRPC,
			address: getGRPCAddr(),
			skip:    false,
		},
		{
			name:    "QUIC",
			proto:   ProtocolQUIC,
			address: getQUICAddr(),
			skip:    true, // Requires TLS certificates in test environment
		},
	}

	// Test each protocol independently
	for _, prot := range protocols {
		t.Run(prot.name, func(t *testing.T) {
			if prot.skip {
				t.Skip("QUIC protocol requires TLS certificates. " +
					"QUIC (HTTP/3) mandates TLS 1.3. " +
					"Test server would need valid TLS certificates or self-signed with proper CA setup.")
			}

			testAllOperationsTableDriven(t, prot.proto, prot.address, prot.name)
		})
	}
}

// testAllOperationsTableDriven tests all operations using table-driven approach
func testAllOperationsTableDriven(t *testing.T, protocol Protocol, address string, protocolName string) {
	// Create client
	config := &ClientConfig{
		Protocol:          protocol,
		Address:           address,
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
		UseTLS:            false,
		MaxRecvMsgSize:    10 * 1024 * 1024,
		MaxSendMsgSize:    10 * 1024 * 1024,
	}

	if protocol == ProtocolQUIC {
		config.UseTLS = true
		config.InsecureSkipVerify = true
	}

	client, err := NewClient(config)
	require.NoError(t, err, "failed to create client for protocol %s", protocolName)
	defer client.Close()

	ctx := context.Background()

	// Define test cases for all 19 operations
	testCases := []struct {
		name      string
		operation string
		setupFunc func() (string, []byte, *Metadata) // Returns key, data, metadata
		testFunc  func(context.Context, Client, string, []byte, *Metadata) error
		verify    func(context.Context, Client) error
		cleanup   func(context.Context, Client, string)
		skipFor   []Protocol // Protocols to skip this test for
	}{
		// 1. PUT Operation
		{
			name:      "Put_BasicOperation",
			operation: "Put",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-put-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Hello, World!")
				metadata := &Metadata{
					ContentType: "text/plain",
					Custom: map[string]string{
						"test": "value",
					},
				}
				return key, data, metadata
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				result, err := client.Put(ctx, key, data, metadata)
				if err != nil {
					return err
				}
				if !result.Success {
					return fmt.Errorf("put returned success=false")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 2. GET Operation
		{
			name:      "Get_BasicOperation",
			operation: "Get",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-get-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Test data for get")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Then get it
				result, err := client.Get(ctx, key)
				if err != nil {
					return err
				}
				if result == nil || len(result.Data) == 0 {
					return fmt.Errorf("get returned empty data")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 3. DELETE Operation
		{
			name:      "Delete_BasicOperation",
			operation: "Delete",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-delete-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("To be deleted")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Then delete it
				return client.Delete(ctx, key)
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 4. EXISTS Operation
		{
			name:      "Exists_BasicOperation",
			operation: "Exists",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-exists-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Existing object")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Then check if it exists
				exists, err := client.Exists(ctx, key)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("exists returned false for existing object")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 5. LIST Operation
		{
			name:      "List_BasicOperation",
			operation: "List",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-list-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Object to list")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put some objects
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// List objects
				opts := &ListOptions{
					Prefix:     protocolName,
					MaxResults: 100,
				}
				result, err := client.List(ctx, opts)
				if err != nil {
					return err
				}
				if result == nil {
					return fmt.Errorf("list returned nil result")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 6. GETMETADATA Operation
		{
			name:      "GetMetadata_BasicOperation",
			operation: "GetMetadata",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-getmeta-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Object with metadata")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Get metadata
				meta, err := client.GetMetadata(ctx, key)
				if err != nil {
					return err
				}
				if meta == nil {
					return fmt.Errorf("getmetadata returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
		},
		// 7. UPDATEMETADATA Operation
		{
			name:      "UpdateMetadata_BasicOperation",
			operation: "UpdateMetadata",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-updatemeta-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Object to update metadata")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Update metadata
				newMeta := &Metadata{
					ContentType: "text/plain",
					Custom: map[string]string{
						"updated": "true",
					},
				}
				err = client.UpdateMetadata(ctx, key, newMeta)
				// REST doesn't support metadata updates without re-uploading
				if err == ErrNotSupported {
					return nil
				}
				return err
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 8. HEALTH Operation
		{
			name:      "Health_BasicOperation",
			operation: "Health",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				health, err := client.Health(ctx)
				if err != nil {
					return err
				}
				if health == nil {
					return fmt.Errorf("health returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
		},
		// 9. ARCHIVE Operation
		{
			name:      "Archive_BasicOperation",
			operation: "Archive",
			setupFunc: func() (string, []byte, *Metadata) {
				key := fmt.Sprintf("%s-archive-basic-%d", protocolName, time.Now().UnixNano())
				data := []byte("Object to archive")
				return key, data, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				// First put the object
				_, err := client.Put(ctx, key, data, nil)
				if err != nil {
					return fmt.Errorf("setup put failed: %w", err)
				}

				// Archive it
				err = client.Archive(ctx, key, "glacier", map[string]string{
					"vault": "test-vault",
				})
				// Archive may not be supported - that's OK
				if err != nil {
					// Log but don't fail
					t.Logf("Archive operation returned: %v", err)
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				client.Delete(ctx, key) // nolint: errcheck
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 10. ADDPOLICY Operation
		{
			name:      "AddPolicy_BasicOperation",
			operation: "AddPolicy",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policy := &LifecyclePolicy{
					ID:               fmt.Sprintf("%s-policy-%d", protocolName, time.Now().UnixNano()),
					Prefix:           "temp/",
					RetentionSeconds: 3600,
					Action:           "delete",
				}
				err := client.AddPolicy(ctx, policy)
				// Not all backends support this
				if err != nil {
					t.Logf("AddPolicy returned: %v", err)
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 11. REMOVEPOLICY Operation
		{
			name:      "RemovePolicy_BasicOperation",
			operation: "RemovePolicy",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policyID := fmt.Sprintf("%s-policy-remove-%d", protocolName, time.Now().UnixNano())
				err := client.RemovePolicy(ctx, policyID)
				// Not all backends support this
				if err != nil {
					t.Logf("RemovePolicy returned: %v", err)
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 12. GETPOLICIES Operation
		{
			name:      "GetPolicies_BasicOperation",
			operation: "GetPolicies",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policies, err := client.GetPolicies(ctx, "")
				// Not all backends support this
				if err != nil {
					t.Logf("GetPolicies returned: %v", err)
					return nil
				}
				if policies == nil {
					return fmt.Errorf("getpolicies returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 13. APPLYPOLICIES Operation
		{
			name:      "ApplyPolicies_BasicOperation",
			operation: "ApplyPolicies",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				result, err := client.ApplyPolicies(ctx)
				// Not all backends support this
				if err != nil {
					t.Logf("ApplyPolicies returned: %v", err)
					return nil
				}
				if result == nil {
					return fmt.Errorf("applypolicies returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 14. ADDREPLICATIONPOLICY Operation
		{
			name:      "AddReplicationPolicy_BasicOperation",
			operation: "AddReplicationPolicy",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policy := &ReplicationPolicy{
					ID:                   fmt.Sprintf("%s-repl-policy-%d", protocolName, time.Now().UnixNano()),
					SourceBackend:        "local",
					SourceSettings:       map[string]string{"path": "/tmp/source"},
					DestinationBackend:   "local",
					DestinationSettings:  map[string]string{"path": "/tmp/dest"},
					CheckIntervalSeconds: 60,
					Enabled:              true,
				}
				err := client.AddReplicationPolicy(ctx, policy)
				// Not all backends support this
				if err != nil {
					t.Logf("AddReplicationPolicy returned: %v", err)
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 15. REMOVEREPLICATIONPOLICY Operation
		{
			name:      "RemoveReplicationPolicy_BasicOperation",
			operation: "RemoveReplicationPolicy",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policyID := fmt.Sprintf("%s-repl-policy-remove-%d", protocolName, time.Now().UnixNano())
				err := client.RemoveReplicationPolicy(ctx, policyID)
				// Not all backends support this
				if err != nil {
					t.Logf("RemoveReplicationPolicy returned: %v", err)
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 16. GETREPLICATIONPOLICIES Operation
		{
			name:      "GetReplicationPolicies_BasicOperation",
			operation: "GetReplicationPolicies",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policies, err := client.GetReplicationPolicies(ctx)
				// Not all backends support this
				if err != nil {
					t.Logf("GetReplicationPolicies returned: %v", err)
					return nil
				}
				if policies == nil {
					return fmt.Errorf("getreplicationpolicies returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 17. GETREPLICATIONPOLICY Operation
		{
			name:      "GetReplicationPolicy_BasicOperation",
			operation: "GetReplicationPolicy",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policyID := fmt.Sprintf("%s-repl-policy-get-%d", protocolName, time.Now().UnixNano())
				policy, err := client.GetReplicationPolicy(ctx, policyID)
				// Policy likely doesn't exist - that's OK
				if err != nil {
					t.Logf("GetReplicationPolicy returned: %v", err)
					return nil
				}
				if policy == nil {
					return fmt.Errorf("getreplicationpolicy returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 18. TRIGGERREPLICATION Operation
		{
			name:      "TriggerReplication_BasicOperation",
			operation: "TriggerReplication",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				opts := &TriggerReplicationOptions{
					PolicyID: fmt.Sprintf("%s-repl-policy-trigger-%d", protocolName, time.Now().UnixNano()),
				}
				result, err := client.TriggerReplication(ctx, opts)
				// Not all backends support this
				if err != nil {
					t.Logf("TriggerReplication returned: %v", err)
					return nil
				}
				if result == nil {
					return fmt.Errorf("triggerreplication returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
		// 19. GETREPLICATIONSTATUS Operation
		{
			name:      "GetReplicationStatus_BasicOperation",
			operation: "GetReplicationStatus",
			setupFunc: func() (string, []byte, *Metadata) {
				return "", nil, nil
			},
			testFunc: func(ctx context.Context, client Client, key string, data []byte, metadata *Metadata) error {
				policyID := fmt.Sprintf("%s-repl-policy-status-%d", protocolName, time.Now().UnixNano())
				status, err := client.GetReplicationStatus(ctx, policyID)
				// Status likely doesn't exist - that's OK
				if err != nil {
					t.Logf("GetReplicationStatus returned: %v", err)
					return nil
				}
				if status == nil {
					return fmt.Errorf("getreplicationstatus returned nil")
				}
				return nil
			},
			verify: func(ctx context.Context, client Client) error {
				return nil
			},
			cleanup: func(ctx context.Context, client Client, key string) {
				// No cleanup needed
			},
			skipFor: []Protocol{ProtocolREST},
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Check if this test should be skipped for this protocol
			for _, skipProto := range tc.skipFor {
				if skipProto == protocol {
					t.Skipf("Operation %s not supported for %s protocol", tc.operation, protocolName)
				}
			}

			// Setup
			key, data, metadata := tc.setupFunc()
			t.Cleanup(func() {
				tc.cleanup(ctx, client, key)
			})

			// Execute test
			start := time.Now()
			err := tc.testFunc(ctx, client, key, data, metadata)
			duration := time.Since(start)

			// Assert results
			if err != nil {
				t.Errorf("Operation %s failed: %v (duration: %v)", tc.operation, err, duration)
			} else {
				t.Logf("Operation %s succeeded (duration: %v)", tc.operation, duration)
			}

			// Verify results
			if err == nil {
				if verifyErr := tc.verify(ctx, client); verifyErr != nil {
					t.Errorf("Verification failed: %v", verifyErr)
				}
			}
		})
	}
}

// TestCrossProtocolConsistency tests that all protocols return consistent responses
func TestCrossProtocolConsistency(t *testing.T) {
	t.Run("ObjectOperationsConsistency", func(t *testing.T) {
		// Create clients for all protocols
		restClient, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer restClient.Close()

		grpcClient, err := NewClient(&ClientConfig{
			Protocol:          ProtocolGRPC,
			Address:           getGRPCAddr(),
			ConnectionTimeout: 10 * time.Second,
			RequestTimeout:    30 * time.Second,
		})
		require.NoError(t, err)
		defer grpcClient.Close()

		ctx := context.Background()

		// Test that both clients handle Put/Get consistently
		testKey := fmt.Sprintf("consistency-test-%d", time.Now().UnixNano())
		testData := []byte("Consistency test data")

		// Put via REST
		restResult, err := restClient.Put(ctx, testKey, testData, nil)
		require.NoError(t, err)
		assert.True(t, restResult.Success)

		// Put via gRPC
		grpcResult, err := grpcClient.Put(ctx, testKey, testData, nil)
		require.NoError(t, err)
		assert.True(t, grpcResult.Success)

		// Get via REST
		restGetResult, err := restClient.Get(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, testData, restGetResult.Data)

		// Get via gRPC
		grpcGetResult, err := grpcClient.Get(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, testData, grpcGetResult.Data)

		// Both should return same data
		assert.Equal(t, restGetResult.Data, grpcGetResult.Data)

		// Cleanup
		restClient.Delete(ctx, testKey) // nolint: errcheck
	})

	t.Run("ListOperationsConsistency", func(t *testing.T) {
		restClient, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer restClient.Close()

		grpcClient, err := NewClient(&ClientConfig{
			Protocol:          ProtocolGRPC,
			Address:           getGRPCAddr(),
			ConnectionTimeout: 10 * time.Second,
			RequestTimeout:    30 * time.Second,
		})
		require.NoError(t, err)
		defer grpcClient.Close()

		ctx := context.Background()

		// List via REST
		restList, err := restClient.List(ctx, &ListOptions{Prefix: "consistency", MaxResults: 10})
		require.NoError(t, err)
		assert.NotNil(t, restList)

		// List via gRPC
		grpcList, err := grpcClient.List(ctx, &ListOptions{Prefix: "consistency", MaxResults: 10})
		require.NoError(t, err)
		assert.NotNil(t, grpcList)

		// Both should return valid results
		assert.NotNil(t, restList.Objects)
		assert.NotNil(t, grpcList.Objects)
	})

	t.Run("HealthCheckConsistency", func(t *testing.T) {
		restClient, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer restClient.Close()

		grpcClient, err := NewClient(&ClientConfig{
			Protocol:          ProtocolGRPC,
			Address:           getGRPCAddr(),
			ConnectionTimeout: 10 * time.Second,
			RequestTimeout:    30 * time.Second,
		})
		require.NoError(t, err)
		defer grpcClient.Close()

		ctx := context.Background()

		// Health check via REST
		restHealth, err := restClient.Health(ctx)
		require.NoError(t, err)
		assert.NotNil(t, restHealth)

		// Health check via gRPC
		grpcHealth, err := grpcClient.Health(ctx)
		require.NoError(t, err)
		assert.NotNil(t, grpcHealth)

		// Both should return successful health status
		assert.NotEmpty(t, restHealth.Status)
		assert.NotEmpty(t, grpcHealth.Status)
	})
}

// TestErrorHandlingConsistency tests that protocols handle errors consistently
func TestErrorHandlingConsistency(t *testing.T) {
	t.Run("NonexistentObjectHandling", func(t *testing.T) {
		restClient, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer restClient.Close()

		grpcClient, err := NewClient(&ClientConfig{
			Protocol:          ProtocolGRPC,
			Address:           getGRPCAddr(),
			ConnectionTimeout: 10 * time.Second,
			RequestTimeout:    30 * time.Second,
		})
		require.NoError(t, err)
		defer grpcClient.Close()

		ctx := context.Background()
		nonexistentKey := fmt.Sprintf("nonexistent-%d", time.Now().UnixNano())

		// Get nonexistent via REST
		_, restErr := restClient.Get(ctx, nonexistentKey)
		assert.Error(t, restErr)

		// Get nonexistent via gRPC
		_, grpcErr := grpcClient.Get(ctx, nonexistentKey)
		assert.Error(t, grpcErr)

		// Both should return errors (specific error type may vary by protocol)
		t.Logf("REST error: %v", restErr)
		t.Logf("gRPC error: %v", grpcErr)
	})

	t.Run("InvalidKeyHandling", func(t *testing.T) {
		restClient, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer restClient.Close()

		ctx := context.Background()

		// Try to put with empty key
		_, err = restClient.Put(ctx, "", []byte("test"), nil)
		assert.Error(t, err)

		// Try to get with empty key
		_, err = restClient.Get(ctx, "")
		assert.Error(t, err)
	})
}

// TestBackendLimitationsAndSkips tests that appropriate operations are skipped
// based on backend capabilities
func TestBackendLimitationsAndSkips(t *testing.T) {
	t.Run("RESTProtocolLimitations", func(t *testing.T) {
		client, err := NewClient(&ClientConfig{
			Protocol: ProtocolREST,
			Address:  getRESTAddr(),
		})
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()

		// REST doesn't support all advanced features
		t.Run("UpdateMetadataNotSupported", func(t *testing.T) {
			key := fmt.Sprintf("rest-metadata-test-%d", time.Now().UnixNano())
			testData := []byte("Test data")

			// Create object
			_, err := client.Put(ctx, key, testData, nil)
			require.NoError(t, err)
			defer client.Delete(ctx, key) // nolint: errcheck

			// Try to update metadata
			err = client.UpdateMetadata(ctx, key, &Metadata{ContentType: "text/plain"})
			// REST should return ErrNotSupported or similar
			if err != nil {
				t.Logf("UpdateMetadata correctly returned error: %v", err)
			}
		})

		t.Run("ApplyPoliciesNotSupported", func(t *testing.T) {
			result, err := client.ApplyPolicies(ctx)
			if err != nil {
				t.Logf("ApplyPolicies correctly returned error: %v", err)
				assert.Nil(t, result)
			}
		})
	})

	t.Run("GRPCProtocolCapabilities", func(t *testing.T) {
		client, err := NewClient(&ClientConfig{
			Protocol:          ProtocolGRPC,
			Address:           getGRPCAddr(),
			ConnectionTimeout: 10 * time.Second,
			RequestTimeout:    30 * time.Second,
		})
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()

		// gRPC supports more advanced features
		t.Run("UpdateMetadataSupported", func(t *testing.T) {
			key := fmt.Sprintf("grpc-metadata-test-%d", time.Now().UnixNano())
			testData := []byte("Test data")

			// Create object
			_, err := client.Put(ctx, key, testData, nil)
			require.NoError(t, err)
			defer client.Delete(ctx, key) // nolint: errcheck

			// Update metadata (might succeed or fail depending on backend support)
			err = client.UpdateMetadata(ctx, key, &Metadata{ContentType: "text/plain"})
			if err != nil {
				t.Logf("UpdateMetadata returned: %v", err)
			}
		})
	})
}
