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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockStorage implements the common.Storage interface for testing.
type mockStorage struct {
	common.LifecycleManager
	data     map[string][]byte
	metadata map[string]*common.Metadata
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
}

func (m *mockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, data)
	if err != nil {
		return err
	}
	m.data[key] = buf.Bytes()
	m.metadata[key] = &common.Metadata{
		Size:         int64(buf.Len()),
		LastModified: time.Now(),
		ETag:         "mock-etag",
	}
	return nil
}

func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, data)
	if err != nil {
		return err
	}
	m.data[key] = buf.Bytes()
	metadata.Size = int64(buf.Len())
	metadata.LastModified = time.Now()
	m.metadata[key] = metadata
	return nil
}

func (m *mockStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, &notFoundError{}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	meta, ok := m.metadata[key]
	if !ok {
		return nil, &notFoundError{}
	}
	return meta, nil
}

func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, ok := m.data[key]; !ok {
		return &notFoundError{}
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if _, ok := m.data[key]; !ok {
		return &notFoundError{}
	}
	delete(m.data, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range m.data {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for k, meta := range m.metadata {
		if len(opts.Prefix) == 0 || len(k) >= len(opts.Prefix) && k[:len(opts.Prefix)] == opts.Prefix {
			objects = append(objects, &common.ObjectInfo{
				Key:      k,
				Metadata: meta,
			})
		}
	}
	return &common.ListResult{
		Objects:        objects,
		CommonPrefixes: []string{},
		NextToken:      "",
		Truncated:      false,
	}, nil
}

func (m *mockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

type notFoundError struct{}

func (e *notFoundError) Error() string {
	return "not found"
}

// Test Server Creation
func TestNewServer(t *testing.T) {
	storage := newMockStorage()

	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server == nil {
		t.Fatal("Server is nil")
	}

	// Server now uses facade, no direct storage reference to check
}

func TestNewServer_FacadeNotInitialized(t *testing.T) {
	// Reset facade to ensure it's not initialized
	objstore.Reset()
	_, err := NewServer()
	if err == nil {
		t.Error("Expected error when facade not initialized, got nil")
	}
}

// Test Server Options
func TestServerOptions(t *testing.T) {
	opts := DefaultServerOptions()

	if opts.Address != ":50051" {
		t.Errorf("Expected default address :50051, got %s", opts.Address)
	}

	if opts.ChunkSize != 64*1024 {
		t.Errorf("Expected chunk size 64KB, got %d", opts.ChunkSize)
	}

	if !opts.EnableHealthCheck {
		t.Error("Expected health check to be enabled by default")
	}
}

func TestWithAddress(t *testing.T) {
	opts := DefaultServerOptions()
	WithAddress(":8080")(opts)

	if opts.Address != ":8080" {
		t.Errorf("Expected address :8080, got %s", opts.Address)
	}
}

func TestWithTLS(t *testing.T) {
	opts := DefaultServerOptions()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	WithTLS(tlsConfig)(opts)

	if opts.TLSConfig != tlsConfig {
		t.Error("TLS config not set correctly")
	}
}

func TestWithMaxConcurrentStreams(t *testing.T) {
	opts := DefaultServerOptions()
	WithMaxConcurrentStreams(200)(opts)

	if opts.MaxConcurrentStreams != 200 {
		t.Errorf("Expected 200 concurrent streams, got %d", opts.MaxConcurrentStreams)
	}
}

func TestWithMaxMessageSize(t *testing.T) {
	opts := DefaultServerOptions()
	WithMaxMessageSize(20 * 1024 * 1024)(opts)

	if opts.MaxReceiveMessageSize != 20*1024*1024 {
		t.Errorf("MaxReceiveMessageSize not set correctly")
	}

	if opts.MaxSendMessageSize != 20*1024*1024 {
		t.Errorf("MaxSendMessageSize not set correctly")
	}
}

func TestWithChunkSize(t *testing.T) {
	opts := DefaultServerOptions()
	WithChunkSize(128 * 1024)(opts)

	if opts.ChunkSize != 128*1024 {
		t.Errorf("Expected chunk size 128KB, got %d", opts.ChunkSize)
	}
}

func TestWithReflection(t *testing.T) {
	opts := DefaultServerOptions()
	WithReflection(true)(opts)

	if !opts.EnableReflection {
		t.Error("Expected reflection to be enabled")
	}
}

func TestWithMetrics(t *testing.T) {
	opts := DefaultServerOptions()
	WithMetrics(false)(opts)

	if opts.EnableMetrics {
		t.Error("Expected metrics to be disabled")
	}
}

func TestWithLogging(t *testing.T) {
	opts := DefaultServerOptions()
	WithLogging(false)(opts)

	if opts.EnableLogging {
		t.Error("Expected logging to be disabled")
	}
}

// Integration tests with actual gRPC client
func setupTestServer(t *testing.T, opts ...ServerOption) (*Server, objstorepb.ObjectStoreClient, func()) {
	storage := newMockStorage()

	// Use dynamic port allocation
	allOpts := append([]ServerOption{WithAddress("127.0.0.1:0")}, opts...)
	server, err := newTestServer(t, storage, allOpts...)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server in goroutine
	go func() {
		if err := server.Start(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}

	client := objstorepb.NewObjectStoreClient(conn)

	cleanup := func() {
		conn.Close()
		server.Stop()
	}

	return server, client, cleanup
}

func TestPutAndGet(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Test Put
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	putResp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !putResp.Success {
		t.Errorf("Put not successful: %s", putResp.Message)
	}

	// Test Get
	getReq := &objstorepb.GetRequest{
		Key: "test-key",
	}

	stream, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var receivedData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Stream receive failed: %v", err)
		}
		receivedData = append(receivedData, resp.Data...)
	}

	if string(receivedData) != "test-data" {
		t.Errorf("Expected 'test-data', got '%s'", string(receivedData))
	}
}

func TestPut_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	putReq := &objstorepb.PutRequest{
		Key:  "",
		Data: []byte("test-data"),
	}

	_, err := client.Put(ctx, putReq)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestGet_NotFound(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	getReq := &objstorepb.GetRequest{
		Key: "non-existent-key",
	}

	stream, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Error("Expected error for non-existent key, got nil")
	}
}

func TestDelete(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put an object first
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete the object
	delReq := &objstorepb.DeleteRequest{
		Key: "test-key",
	}

	delResp, err := client.Delete(ctx, delReq)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if !delResp.Success {
		t.Errorf("Delete not successful: %s", delResp.Message)
	}
}

func TestDelete_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	delReq := &objstorepb.DeleteRequest{
		Key: "",
	}

	_, err := client.Delete(ctx, delReq)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestExists(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put an object first
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check existence
	existsReq := &objstorepb.ExistsRequest{
		Key: "test-key",
	}

	existsResp, err := client.Exists(ctx, existsReq)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}

	if !existsResp.Exists {
		t.Error("Expected object to exist")
	}

	// Check non-existent object
	existsReq = &objstorepb.ExistsRequest{
		Key: "non-existent-key",
	}

	existsResp, err = client.Exists(ctx, existsReq)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}

	if existsResp.Exists {
		t.Error("Expected object to not exist")
	}
}

func TestExists_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	existsReq := &objstorepb.ExistsRequest{
		Key: "",
	}

	_, err := client.Exists(ctx, existsReq)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestList(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple objects
	keys := []string{"prefix/obj1", "prefix/obj2", "other/obj3"}
	for _, key := range keys {
		putReq := &objstorepb.PutRequest{
			Key:  key,
			Data: []byte("test-data"),
		}
		_, err := client.Put(ctx, putReq)
		if err != nil {
			t.Fatalf("Put failed for %s: %v", key, err)
		}
	}

	// List with prefix
	listReq := &objstorepb.ListRequest{
		Prefix: "prefix/",
	}

	listResp, err := client.List(ctx, listReq)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(listResp.Objects) != 2 {
		t.Errorf("Expected 2 objects, got %d", len(listResp.Objects))
	}
}

func TestGetMetadata(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put an object with metadata
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
		},
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get metadata
	metaReq := &objstorepb.GetMetadataRequest{
		Key: "test-key",
	}

	metaResp, err := client.GetMetadata(ctx, metaReq)
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if !metaResp.Success {
		t.Errorf("GetMetadata not successful: %s", metaResp.Message)
	}

	if metaResp.Metadata == nil {
		t.Fatal("Metadata is nil")
	}

	if metaResp.Metadata.ContentType != "text/plain" {
		t.Errorf("Expected ContentType 'text/plain', got '%s'", metaResp.Metadata.ContentType)
	}
}

func TestGetMetadata_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	metaReq := &objstorepb.GetMetadataRequest{
		Key: "",
	}

	_, err := client.GetMetadata(ctx, metaReq)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestUpdateMetadata(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put an object first
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Update metadata
	updateReq := &objstorepb.UpdateMetadataRequest{
		Key: "test-key",
		Metadata: &objstorepb.Metadata{
			ContentType: "application/json",
		},
	}

	updateResp, err := client.UpdateMetadata(ctx, updateReq)
	if err != nil {
		t.Fatalf("UpdateMetadata failed: %v", err)
	}

	if !updateResp.Success {
		t.Errorf("UpdateMetadata not successful: %s", updateResp.Message)
	}
}

func TestUpdateMetadata_InvalidKey(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	updateReq := &objstorepb.UpdateMetadataRequest{
		Key: "",
		Metadata: &objstorepb.Metadata{
			ContentType: "application/json",
		},
	}

	_, err := client.UpdateMetadata(ctx, updateReq)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
}

func TestUpdateMetadata_NilMetadata(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	updateReq := &objstorepb.UpdateMetadataRequest{
		Key:      "test-key",
		Metadata: nil,
	}

	_, err := client.UpdateMetadata(ctx, updateReq)
	if err == nil {
		t.Error("Expected error for nil metadata, got nil")
	}
}

func TestHealth(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	healthReq := &objstorepb.HealthRequest{}

	healthResp, err := client.Health(ctx, healthReq)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if healthResp.Status != objstorepb.HealthResponse_SERVING {
		t.Errorf("Expected SERVING status, got %v", healthResp.Status)
	}
}

func TestGetMetrics(t *testing.T) {
	server, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Make some requests
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get metrics
	metrics := server.GetMetrics()

	if metrics["total_requests"].(uint64) == 0 {
		t.Error("Expected total_requests > 0")
	}
}

func TestBytesReader(t *testing.T) {
	data := []byte("test data")
	reader := &bytesReader{data: data}

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != 4 {
		t.Errorf("Expected to read 4 bytes, got %d", n)
	}

	if string(buf) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(buf))
	}

	// Read rest of data
	buf = make([]byte, 10)
	n, err = reader.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}

	// Read at EOF
	n, err = reader.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestMetadataConversion(t *testing.T) {
	now := time.Now()
	commonMeta := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Size:            1024,
		LastModified:    now,
		ETag:            "test-etag",
		Custom: map[string]string{
			"key1": "value1",
		},
	}

	// Convert to proto
	protoMeta := metadataToProto(commonMeta)

	if protoMeta.ContentType != "text/plain" {
		t.Errorf("ContentType mismatch")
	}

	if protoMeta.Size != 1024 {
		t.Errorf("Size mismatch")
	}

	// Convert back to common
	convertedMeta := protoToMetadata(protoMeta)

	if convertedMeta.ContentType != commonMeta.ContentType {
		t.Errorf("ContentType mismatch after conversion")
	}

	if convertedMeta.Size != commonMeta.Size {
		t.Errorf("Size mismatch after conversion")
	}
}

func TestMetadataConversion_Nil(t *testing.T) {
	protoMeta := metadataToProto(nil)
	if protoMeta != nil {
		t.Error("Expected nil, got non-nil")
	}

	commonMeta := protoToMetadata(nil)
	if commonMeta != nil {
		t.Error("Expected nil, got non-nil")
	}
}

// Error mock storage types for testing error paths
type errorMockStorage struct {
	*mockStorage
	shouldFailExists bool
}

func (e *errorMockStorage) Exists(ctx context.Context, key string) (bool, error) {
	if e.shouldFailExists {
		return false, errors.New("storage error")
	}
	return e.mockStorage.Exists(ctx, key)
}

type errorPutMockStorage struct {
	*mockStorage
}

func (e *errorPutMockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return errors.New("storage write error")
}

func (e *errorPutMockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return errors.New("storage write error")
}
