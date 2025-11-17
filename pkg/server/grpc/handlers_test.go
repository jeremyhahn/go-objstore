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
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMapError_Various(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "not found",
			err:      errors.New("not found"),
			wantCode: codes.NotFound,
		},
		{
			name:     "key not found",
			err:      errors.New("key not found"),
			wantCode: codes.NotFound,
		},
		{
			name:     "already exists",
			err:      errors.New("already exists"),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "permission denied",
			err:      errors.New("permission denied"),
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "invalid argument",
			err:      errors.New("invalid argument"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid key",
			err:      errors.New("invalid key"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "deadline exceeded",
			err:      errors.New("deadline exceeded"),
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "context deadline exceeded",
			err:      errors.New("context deadline exceeded"),
			wantCode: codes.DeadlineExceeded,
		},
		{
			name:     "canceled",
			err:      errors.New("canceled"),
			wantCode: codes.Canceled,
		},
		{
			name:     "context canceled",
			err:      errors.New("context canceled"),
			wantCode: codes.Canceled,
		},
		{
			name:     "unknown error",
			err:      errors.New("some random error"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcErr := mapError(tt.err)
			st, ok := status.FromError(grpcErr)
			if !ok {
				t.Fatal("Expected gRPC status error")
			}

			if st.Code() != tt.wantCode {
				t.Errorf("Expected code %v, got %v", tt.wantCode, st.Code())
			}
		})
	}
}

func TestMapError_Nil(t *testing.T) {
	err := mapError(nil)
	if err != nil {
		t.Errorf("Expected nil for nil input, got %v", err)
	}
}

func TestGet_Streaming(t *testing.T) {
	_, client, cleanup := setupTestServer(t, WithChunkSize(4)) // Small chunk size for testing
	defer cleanup()

	ctx := context.Background()

	// Put a larger object
	data := []byte("this is a longer test data string for streaming")
	putReq := &objstorepb.PutRequest{
		Key:  "large-object",
		Data: data,
	}

	_, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the object with streaming
	getReq := &objstorepb.GetRequest{
		Key: "large-object",
	}

	stream, err := client.Get(ctx, getReq)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var receivedData []byte
	var hasMetadata bool

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Stream receive failed: %v", err)
		}

		if resp.Metadata != nil {
			hasMetadata = true
		}

		receivedData = append(receivedData, resp.Data...)
	}

	if !hasMetadata {
		t.Error("Expected metadata in first response")
	}

	if string(receivedData) != string(data) {
		t.Errorf("Data mismatch: expected %s, got %s", string(data), string(receivedData))
	}
}

func TestGet_ContextCancellation(t *testing.T) {
	storage := newMockStorage()
	server, err := NewServer(storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := grpc.Dial(
		server.GetAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)

	// Put an object
	ctx := context.Background()
	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
	}
	_, err = client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get with cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	getReq := &objstorepb.GetRequest{
		Key: "test-key",
	}

	stream, err := client.Get(cancelCtx, getReq)
	if err == nil {
		_, err = stream.Recv()
	}

	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestDelete_NotFound(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	delReq := &objstorepb.DeleteRequest{
		Key: "non-existent-key",
	}

	_, err := client.Delete(ctx, delReq)
	if err == nil {
		t.Error("Expected error for deleting non-existent key")
	}

	st, ok := status.FromError(err)
	if ok && st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound error, got %v", st.Code())
	}
}

func TestList_Empty(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	listReq := &objstorepb.ListRequest{
		Prefix: "non-existent/",
	}

	listResp, err := client.List(ctx, listReq)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(listResp.Objects) != 0 {
		t.Errorf("Expected 0 objects, got %d", len(listResp.Objects))
	}
}

func TestList_WithOptions(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Put some objects
	for i := 0; i < 5; i++ {
		putReq := &objstorepb.PutRequest{
			Key:  "obj" + string(rune('0'+i)),
			Data: []byte("data"),
		}
		_, err := client.Put(ctx, putReq)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// List with max results
	listReq := &objstorepb.ListRequest{
		MaxResults: 3,
	}

	listResp, err := client.List(ctx, listReq)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(listResp.Objects) == 0 {
		t.Error("Expected some objects")
	}
}

func TestUpdateMetadata_NotFound(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	updateReq := &objstorepb.UpdateMetadataRequest{
		Key: "non-existent-key",
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
		},
	}

	_, err := client.UpdateMetadata(ctx, updateReq)
	if err == nil {
		t.Error("Expected error for updating non-existent key")
	}
}

func TestGetMetadata_NotFound(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	metaReq := &objstorepb.GetMetadataRequest{
		Key: "non-existent-key",
	}

	_, err := client.GetMetadata(ctx, metaReq)
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
}

func TestPutWithMetadata(t *testing.T) {
	_, client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	putReq := &objstorepb.PutRequest{
		Key:  "test-key",
		Data: []byte("test-data"),
		Metadata: &objstorepb.Metadata{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			Custom: map[string]string{
				"author": "test",
			},
		},
	}

	putResp, err := client.Put(ctx, putReq)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !putResp.Success {
		t.Error("Put should be successful")
	}

	// Verify metadata was stored
	metaReq := &objstorepb.GetMetadataRequest{
		Key: "test-key",
	}

	metaResp, err := client.GetMetadata(ctx, metaReq)
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if metaResp.Metadata.ContentType != "application/json" {
		t.Errorf("ContentType mismatch")
	}

	if metaResp.Metadata.Custom["author"] != "test" {
		t.Errorf("Custom metadata mismatch")
	}
}

func TestServerOptions_DisableAll(t *testing.T) {
	storage := newMockStorage()

	server, err := NewServer(
		storage,
		WithAddress("127.0.0.1:0"),
		WithLogging(false),
		WithMetrics(false),
		WithHealthCheck(false),
		WithReflection(false),
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server.opts.EnableLogging {
		t.Error("Logging should be disabled")
	}

	if server.opts.EnableMetrics {
		t.Error("Metrics should be disabled")
	}

	if server.opts.EnableHealthCheck {
		t.Error("HealthCheck should be disabled")
	}

	if server.opts.EnableReflection {
		t.Error("Reflection should be disabled")
	}
}

func TestProtoToMetadata_NilLastModified(t *testing.T) {
	protoMeta := &objstorepb.Metadata{
		ContentType:  "text/plain",
		Size:         100,
		LastModified: nil,
	}

	meta := protoToMetadata(protoMeta)

	if meta == nil {
		t.Fatal("Metadata should not be nil")
	}

	if meta.ContentType != "text/plain" {
		t.Error("ContentType mismatch")
	}

	if meta.Size != 100 {
		t.Error("Size mismatch")
	}
}

func TestServer_GetAddress_BeforeStart(t *testing.T) {
	storage := newMockStorage()

	server, err := NewServer(storage, WithAddress(":12345"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	addr := server.GetAddress()
	if addr != ":12345" {
		t.Errorf("Expected :12345, got %s", addr)
	}
}

func TestServer_ForceStop(t *testing.T) {
	storage := newMockStorage()

	server, err := NewServer(storage, WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	go server.Start()
	time.Sleep(100 * time.Millisecond)

	// Force stop
	server.ForceStop()

	// Should not panic
}

func TestBytesReader_EmptyData(t *testing.T) {
	reader := &bytesReader{data: []byte{}}

	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}

	if n != 0 {
		t.Errorf("Expected 0 bytes read, got %d", n)
	}
}

func TestMetadataToProto_AllFields(t *testing.T) {
	now := time.Now()
	meta := &common.Metadata{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Size:            12345,
		LastModified:    now,
		ETag:            "test-etag",
		Custom: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	protoMeta := metadataToProto(meta)

	if protoMeta.ContentType != meta.ContentType {
		t.Error("ContentType mismatch")
	}

	if protoMeta.ContentEncoding != meta.ContentEncoding {
		t.Error("ContentEncoding mismatch")
	}

	if protoMeta.Size != meta.Size {
		t.Error("Size mismatch")
	}

	if protoMeta.Etag != meta.ETag {
		t.Error("ETag mismatch")
	}

	if len(protoMeta.Custom) != len(meta.Custom) {
		t.Error("Custom metadata length mismatch")
	}

	if protoMeta.Custom["key1"] != "value1" {
		t.Error("Custom metadata value mismatch")
	}
}
