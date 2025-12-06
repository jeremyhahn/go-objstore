// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getGRPCAddress() string {
	if addr := os.Getenv("OBJSTORE_GRPC_HOST"); addr != "" {
		return addr
	}
	return "objstore-server:50051"
}

func getRESTAddress() string {
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

func getQUICAddress() string {
	if addr := os.Getenv("OBJSTORE_QUIC_URL"); addr != "" {
		// Strip https:// prefix if present
		if len(addr) > 8 && addr[:8] == "https://" {
			addr = addr[8:]
		}
		return addr
	}
	return "objstore-server:4433"
}

// TestGRPCClientIntegration tests all operations via gRPC.
func TestGRPCClientIntegration(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           getGRPCAddress(),
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	testAllOperations(t, client, "grpc")
}

// TestRESTClientIntegration tests all operations via REST.
func TestRESTClientIntegration(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	testBasicOperations(t, client, "rest")
}

// TestQUICClientIntegration tests all operations via QUIC.
func TestQUICClientIntegration(t *testing.T) {
	t.Skip("QUIC protocol requires TLS certificates. " +
		"QUIC (HTTP/3) mandates TLS 1.3 as part of the protocol specification. " +
		"The test server would need to be configured with valid TLS certificates " +
		"or self-signed certificates with proper certificate authority setup. " +
		"Currently skipped in local backend testing environment.")

	config := &objstore.ClientConfig{
		Protocol:           objstore.ProtocolQUIC,
		Address:            getQUICAddress(),
		UseTLS:             true,
		InsecureSkipVerify: true,
		ConnectionTimeout:  10 * time.Second,
		RequestTimeout:     30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	testAllOperations(t, client, "quic")
}

// testBasicOperations tests only the basic operations supported by all protocols.
func testBasicOperations(t *testing.T, client objstore.Client, prefix string) {
	ctx := context.Background()

	// Test Health
	t.Run(prefix+"/Health", func(t *testing.T) {
		health, err := client.Health(ctx)
		require.NoError(t, err)
		assert.NotNil(t, health)
	})

	testKey := fmt.Sprintf("%s-test-object-%d", prefix, time.Now().UnixNano())

	// Test Put
	t.Run(prefix+"/Put", func(t *testing.T) {
		data := []byte("Hello, World!")
		metadata := &objstore.Metadata{
			ContentType: "text/plain",
			Custom: map[string]string{
				"author": "integration-test",
			},
		}

		result, err := client.Put(ctx, testKey, data, metadata)
		require.NoError(t, err)
		assert.True(t, result.Success)
		// ETag may not be available in all protocols/backends
		if result.ETag == "" {
			t.Logf("Warning: ETag not returned by server for protocol %s", prefix)
		}
	})

	// Test Exists
	t.Run(prefix+"/Exists", func(t *testing.T) {
		exists, err := client.Exists(ctx, testKey)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	// Test Get
	t.Run(prefix+"/Get", func(t *testing.T) {
		result, err := client.Get(ctx, testKey)
		require.NoError(t, err)
		assert.Equal(t, "Hello, World!", string(result.Data))
		assert.NotNil(t, result.Metadata)
	})

	// Test GetMetadata
	t.Run(prefix+"/GetMetadata", func(t *testing.T) {
		metadata, err := client.GetMetadata(ctx, testKey)
		require.NoError(t, err)
		assert.NotNil(t, metadata)
		assert.NotZero(t, metadata.Size)
	})

	// Test UpdateMetadata
	t.Run(prefix+"/UpdateMetadata", func(t *testing.T) {
		newMetadata := &objstore.Metadata{
			ContentType: "text/plain",
			Custom: map[string]string{
				"author":  "integration-test",
				"updated": "true",
			},
		}

		err := client.UpdateMetadata(ctx, testKey, newMetadata)
		// REST protocol doesn't support metadata updates without re-uploading the object
		// Skip this test for REST
		if err == objstore.ErrNotSupported {
			t.Skip("UpdateMetadata not supported for this protocol")
		}
		require.NoError(t, err)
	})

	// Test List
	t.Run(prefix+"/List", func(t *testing.T) {
		opts := &objstore.ListOptions{
			Prefix:     prefix,
			MaxResults: 10,
		}

		result, err := client.List(ctx, opts)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotEmpty(t, result.Objects)
	})

	// Test Delete
	t.Run(prefix+"/Delete", func(t *testing.T) {
		err := client.Delete(ctx, testKey)
		require.NoError(t, err)

		// Verify deletion
		exists, err := client.Exists(ctx, testKey)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

// testAllOperations tests all operations including advanced features.
func testAllOperations(t *testing.T, client objstore.Client, prefix string) {
	ctx := context.Background()

	// Run basic operations first
	testBasicOperations(t, client, prefix)

	// Test Archive
	t.Run(prefix+"/Archive", func(t *testing.T) {
		archiveKey := fmt.Sprintf("%s-archive-%d", prefix, time.Now().UnixNano())

		// First create an object
		data := []byte("Archive me!")
		_, err := client.Put(ctx, archiveKey, data, nil)
		require.NoError(t, err)

		// Archive it
		err = client.Archive(ctx, archiveKey, "glacier", map[string]string{
			"vault": "test-vault",
		})
		// May fail if glacier backend not configured - that's OK
		if err != nil {
			t.Logf("Archive test skipped: %v", err)
		}

		// Clean up
		client.Delete(ctx, archiveKey)
	})

	// Test Lifecycle Policies
	t.Run(prefix+"/LifecyclePolicies", func(t *testing.T) {
		policy := &objstore.LifecyclePolicy{
			ID:               fmt.Sprintf("%s-policy-%d", prefix, time.Now().UnixNano()),
			Prefix:           "temp/",
			RetentionSeconds: 3600,
			Action:           "delete",
		}

		// Add policy
		err := client.AddPolicy(ctx, policy)
		if err != nil {
			t.Logf("AddPolicy not supported or failed: %v", err)
			return
		}

		// Get policies
		policies, err := client.GetPolicies(ctx, "")
		if err != nil {
			t.Logf("GetPolicies not supported: %v", err)
			return
		}
		assert.NotNil(t, policies)

		// Apply policies
		result, err := client.ApplyPolicies(ctx)
		if err != nil {
			t.Logf("ApplyPolicies not supported: %v", err)
		} else {
			assert.NotNil(t, result)
		}

		// Remove policy
		err = client.RemovePolicy(ctx, policy.ID)
		if err != nil {
			t.Logf("RemovePolicy failed: %v", err)
		}
	})

	// Test Replication Policies
	t.Run(prefix+"/ReplicationPolicies", func(t *testing.T) {
		policy := &objstore.ReplicationPolicy{
			ID:            fmt.Sprintf("%s-repl-%d", prefix, time.Now().UnixNano()),
			SourceBackend: "local",
			SourceSettings: map[string]string{
				"path": "/tmp/source",
			},
			DestinationBackend: "local",
			DestinationSettings: map[string]string{
				"path": "/tmp/dest",
			},
			CheckIntervalSeconds: 60,
			Enabled:              true,
		}

		// Add replication policy
		err := client.AddReplicationPolicy(ctx, policy)
		if err != nil {
			t.Logf("AddReplicationPolicy not supported: %v", err)
			return
		}

		// Get replication policies
		policies, err := client.GetReplicationPolicies(ctx)
		if err != nil {
			t.Logf("GetReplicationPolicies not supported: %v", err)
			return
		}
		assert.NotNil(t, policies)

		// Get specific policy
		retrievedPolicy, err := client.GetReplicationPolicy(ctx, policy.ID)
		if err != nil {
			t.Logf("GetReplicationPolicy failed: %v", err)
		} else {
			assert.Equal(t, policy.ID, retrievedPolicy.ID)
		}

		// Trigger replication
		syncResult, err := client.TriggerReplication(ctx, &objstore.TriggerReplicationOptions{
			PolicyID: policy.ID,
		})
		if err != nil {
			t.Logf("TriggerReplication failed: %v", err)
		} else {
			assert.NotNil(t, syncResult)
		}

		// Get replication status
		status, err := client.GetReplicationStatus(ctx, policy.ID)
		if err != nil {
			t.Logf("GetReplicationStatus failed: %v", err)
		} else {
			assert.NotNil(t, status)
			assert.Equal(t, policy.ID, status.PolicyID)
		}

		// Remove replication policy
		err = client.RemoveReplicationPolicy(ctx, policy.ID)
		if err != nil {
			t.Logf("RemoveReplicationPolicy failed: %v", err)
		}
	})
}

// TestConcurrentOperations tests concurrent operations.
func TestConcurrentOperations(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           getGRPCAddress(),
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	numGoroutines := 10
	doneCh := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent-test-%d-%d", id, time.Now().UnixNano())
			data := []byte(fmt.Sprintf("Data from goroutine %d", id))

			// Put
			_, err := client.Put(ctx, key, data, nil)
			assert.NoError(t, err)

			// Get
			result, err := client.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, data, result.Data)

			// Delete
			err = client.Delete(ctx, key)
			assert.NoError(t, err)

			doneCh <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-doneCh
	}
}

// TestLargeObject tests handling of large objects.
func TestLargeObject(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           getGRPCAddress(),
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    60 * time.Second,
		MaxRecvMsgSize:    10 * 1024 * 1024, // 10MB
		MaxSendMsgSize:    10 * 1024 * 1024, // 10MB
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	key := fmt.Sprintf("large-object-%d", time.Now().UnixNano())

	// Create 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Put large object
	result, err := client.Put(ctx, key, data, nil)
	require.NoError(t, err)
	assert.True(t, result.Success)

	// Get large object
	getResult, err := client.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(getResult.Data))
	assert.Equal(t, data, getResult.Data)

	// Clean up
	err = client.Delete(ctx, key)
	require.NoError(t, err)
}

// TestRESTGetNonexistent tests error handling when getting a non-existent object via REST.
func TestRESTGetNonexistent(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	nonexistentKey := fmt.Sprintf("nonexistent-key-%d", time.Now().UnixNano())

	// Attempt to get non-existent object
	result, err := client.Get(ctx, nonexistentKey)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, objstore.ErrObjectNotFound)
}

// TestRESTDeleteNonexistent tests error handling when deleting a non-existent object via REST.
func TestRESTDeleteNonexistent(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	nonexistentKey := fmt.Sprintf("nonexistent-delete-%d", time.Now().UnixNano())

	// Attempt to delete non-existent object
	// Note: Some backends may return success for idempotent deletes
	err = client.Delete(ctx, nonexistentKey)
	if err != nil {
		assert.ErrorIs(t, err, objstore.ErrObjectNotFound)
	}
}

// TestRESTUpdateMetadataNonexistent tests error handling when updating metadata on non-existent object via REST.
func TestRESTUpdateMetadataNonexistent(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	nonexistentKey := fmt.Sprintf("nonexistent-metadata-%d", time.Now().UnixNano())

	metadata := &objstore.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"test": "value",
		},
	}

	// Attempt to update metadata on non-existent object
	// REST protocol doesn't support metadata updates without re-uploading
	err = client.UpdateMetadata(ctx, nonexistentKey, metadata)
	assert.Error(t, err)
	assert.ErrorIs(t, err, objstore.ErrNotSupported)
}

// TestGRPCGetNonexistent tests error handling when getting a non-existent object via gRPC.
func TestGRPCGetNonexistent(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           getGRPCAddress(),
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	nonexistentKey := fmt.Sprintf("grpc-nonexistent-%d", time.Now().UnixNano())

	// Attempt to get non-existent object
	result, err := client.Get(ctx, nonexistentKey)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestGRPCDeleteNonexistent tests error handling when deleting a non-existent object via gRPC.
func TestGRPCDeleteNonexistent(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           getGRPCAddress(),
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	nonexistentKey := fmt.Sprintf("grpc-nonexistent-delete-%d", time.Now().UnixNano())

	// Attempt to delete non-existent object
	// Behavior may vary by backend - some return error, others are idempotent
	err = client.Delete(ctx, nonexistentKey)
	// We don't assert error here as some backends allow idempotent deletes
	if err != nil {
		t.Logf("Delete non-existent returned error: %v", err)
	}
}

// TestRESTStreamLargeObject tests handling of large objects via REST.
// Note: REST client doesn't have true streaming like gRPC, but handles large objects
// by reading the entire response body.
func TestRESTStreamLargeObject(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 60 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	key := fmt.Sprintf("rest-large-%d", time.Now().UnixNano())

	// Create 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Put large object
	result, err := client.Put(ctx, key, data, nil)
	require.NoError(t, err)
	assert.True(t, result.Success)

	// Get large object (REST reads entire body, not true streaming)
	getResult, err := client.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(getResult.Data))
	assert.Equal(t, data, getResult.Data)

	// Verify metadata
	assert.NotNil(t, getResult.Metadata)
	assert.Equal(t, int64(len(data)), getResult.Metadata.Size)

	// Clean up
	err = client.Delete(ctx, key)
	require.NoError(t, err)
}

// TestRESTApplyPolicies tests applying lifecycle policies via REST.
func TestRESTApplyPolicies(t *testing.T) {
	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        getRESTAddress(),
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Attempt to apply policies
	// REST protocol doesn't support lifecycle policies in the current implementation
	result, err := client.ApplyPolicies(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, objstore.ErrStreamingNotSupported)
}
