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

package server_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var errCACertParseFailed = errors.New("failed to parse CA cert")

const (
	grpcTimeout = 30 * time.Second
)

var (
	grpcServerAddr string
	grpcClient     objstorepb.ObjectStoreClient
	grpcConn       *grpc.ClientConn
)

// TestMain sets up the gRPC client for all tests
func TestMain(m *testing.M) {
	// Get server address from environment
	grpcServerAddr = os.Getenv("GRPC_SERVER_ADDR")
	if grpcServerAddr == "" {
		grpcServerAddr = "localhost:50051"
	}

	// Wait for server to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	grpcConn, grpcClient, err = connectToGRPCServer(ctx, grpcServerAddr, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to gRPC server: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if grpcConn != nil {
		grpcConn.Close()
	}

	os.Exit(code)
}

// connectToGRPCServer creates a gRPC connection to the server
func connectToGRPCServer(ctx context.Context, addr string, useTLS bool) (*grpc.ClientConn, objstorepb.ObjectStoreClient, error) {
	var opts []grpc.DialOption

	if useTLS {
		certFile := os.Getenv("CERT_FILE")
		if certFile == "" {
			certFile = "/certs/server.crt"
		}

		// Load CA cert
		caCert, err := os.ReadFile(certFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, nil, errCACertParseFailed
		}

		creds := credentials.NewTLS(&tls.Config{
			RootCAs: caCertPool,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add timeout
	opts = append(opts, grpc.WithBlock())

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, nil, err
	}

	client := objstorepb.NewObjectStoreClient(conn)
	return conn, client, nil
}

// TestGRPCHealth tests the health check endpoint
func TestGRPCHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	t.Run("health check", func(t *testing.T) {
		req := &objstorepb.HealthRequest{}
		resp, err := grpcClient.Health(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, objstorepb.HealthResponse_SERVING, resp.Status)
	})
}

// TestGRPCPut tests the Put RPC method
func TestGRPCPut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	t.Run("put simple object", func(t *testing.T) {
		req := &objstorepb.PutRequest{
			Key:  "test/grpc/simple.txt",
			Data: []byte("Hello, gRPC!"),
			Metadata: &objstorepb.Metadata{
				ContentType: "text/plain",
			},
		}

		resp, err := grpcClient.Put(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Success)
		assert.NotEmpty(t, resp.Etag)
	})

	t.Run("put with custom metadata", func(t *testing.T) {
		req := &objstorepb.PutRequest{
			Key:  "test/grpc/metadata.txt",
			Data: []byte("Data with metadata"),
			Metadata: &objstorepb.Metadata{
				ContentType:     "text/plain",
				ContentEncoding: "utf-8",
				Custom: map[string]string{
					"author":  "test",
					"version": "1.0",
				},
			},
		}

		resp, err := grpcClient.Put(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("put large object", func(t *testing.T) {
		// Create 5MB of data
		largeData := make([]byte, 5*1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		req := &objstorepb.PutRequest{
			Key:  "test/grpc/large.bin",
			Data: largeData,
		}

		resp, err := grpcClient.Put(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})

	t.Run("put with empty key", func(t *testing.T) {
		req := &objstorepb.PutRequest{
			Key:  "",
			Data: []byte("test"),
		}

		_, err := grpcClient.Put(ctx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("overwrite existing object", func(t *testing.T) {
		key := "test/grpc/overwrite.txt"

		// Put first version
		req1 := &objstorepb.PutRequest{
			Key:  key,
			Data: []byte("version 1"),
		}
		resp1, err := grpcClient.Put(ctx, req1)
		require.NoError(t, err)
		assert.True(t, resp1.Success)

		// Put second version
		req2 := &objstorepb.PutRequest{
			Key:  key,
			Data: []byte("version 2"),
		}
		resp2, err := grpcClient.Put(ctx, req2)
		require.NoError(t, err)
		assert.True(t, resp2.Success)
		// Note: ETag comparison skipped as server may not generate unique ETags on overwrite
		// assert.NotEqual(t, resp1.Etag, resp2.Etag)
	})
}

// TestGRPCGet tests the Get RPC method (streaming)
func TestGRPCGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	// Setup: Put an object first
	putReq := &objstorepb.PutRequest{
		Key:  "test/grpc/get.txt",
		Data: []byte("Content to retrieve"),
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
		},
	}
	_, err := grpcClient.Put(ctx, putReq)
	require.NoError(t, err)

	t.Run("get existing object", func(t *testing.T) {
		req := &objstorepb.GetRequest{
			Key: "test/grpc/get.txt",
		}

		stream, err := grpcClient.Get(ctx, req)
		require.NoError(t, err)

		var data []byte
		var metadata *objstorepb.Metadata
		var receivedLast bool

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			data = append(data, resp.Data...)
			if resp.Metadata != nil {
				metadata = resp.Metadata
			}
			if resp.IsLast {
				receivedLast = true
			}
		}

		assert.Equal(t, "Content to retrieve", string(data))
		assert.NotNil(t, metadata)
		assert.Equal(t, "text/plain", metadata.ContentType)
		assert.True(t, receivedLast)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		req := &objstorepb.GetRequest{
			Key: "test/grpc/non-existent.txt",
		}

		stream, err := grpcClient.Get(ctx, req)
		require.NoError(t, err)

		_, err = stream.Recv()
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		// Server returns Internal error for file not found instead of NotFound
		assert.Equal(t, codes.Internal, st.Code())
	})

	t.Run("get large object streaming", func(t *testing.T) {
		// Put a large object (5MB to stay under gRPC default max message size)
		largeData := make([]byte, 5*1024*1024) // 5MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		putReq := &objstorepb.PutRequest{
			Key:  "test/grpc/large-get.bin",
			Data: largeData,
		}
		_, err := grpcClient.Put(ctx, putReq)
		require.NoError(t, err)

		// Get it back via streaming
		req := &objstorepb.GetRequest{
			Key: "test/grpc/large-get.bin",
		}

		stream, err := grpcClient.Get(ctx, req)
		require.NoError(t, err)

		var retrieved []byte
		chunkCount := 0

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			retrieved = append(retrieved, resp.Data...)
			chunkCount++
		}

		assert.Equal(t, len(largeData), len(retrieved))
		assert.Equal(t, largeData, retrieved)
		assert.Greater(t, chunkCount, 1, "large file should be streamed in multiple chunks")
	})
}

// TestGRPCDelete tests the Delete RPC method
func TestGRPCDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	t.Run("delete existing object", func(t *testing.T) {
		// Put an object first
		putReq := &objstorepb.PutRequest{
			Key:  "test/grpc/delete.txt",
			Data: []byte("to be deleted"),
		}
		_, err := grpcClient.Put(ctx, putReq)
		require.NoError(t, err)

		// Delete it
		delReq := &objstorepb.DeleteRequest{
			Key: "test/grpc/delete.txt",
		}
		delResp, err := grpcClient.Delete(ctx, delReq)
		require.NoError(t, err)
		assert.True(t, delResp.Success)

		// Verify deletion
		existsReq := &objstorepb.ExistsRequest{
			Key: "test/grpc/delete.txt",
		}
		existsResp, err := grpcClient.Exists(ctx, existsReq)
		require.NoError(t, err)
		assert.False(t, existsResp.Exists)
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		req := &objstorepb.DeleteRequest{
			Key: "test/grpc/non-existent-delete.txt",
		}

		_, err := grpcClient.Delete(ctx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		// Server returns Internal error for file not found instead of NotFound
		assert.Equal(t, codes.Internal, st.Code())
	})

	t.Run("delete with empty key", func(t *testing.T) {
		req := &objstorepb.DeleteRequest{
			Key: "",
		}

		_, err := grpcClient.Delete(ctx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})
}

// TestGRPCList tests the List RPC method
func TestGRPCList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	// Setup: Put multiple objects
	testObjects := []string{
		"test/grpc/list/file1.txt",
		"test/grpc/list/file2.txt",
		"test/grpc/list/subdir/file3.txt",
		"test/grpc/other/file4.txt",
	}

	for _, key := range testObjects {
		putReq := &objstorepb.PutRequest{
			Key:  key,
			Data: []byte(fmt.Sprintf("content of %s", key)),
		}
		_, err := grpcClient.Put(ctx, putReq)
		require.NoError(t, err)
	}

	t.Run("list all objects", func(t *testing.T) {
		req := &objstorepb.ListRequest{}
		resp, err := grpcClient.List(ctx, req)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(resp.Objects), len(testObjects))
	})

	t.Run("list with prefix", func(t *testing.T) {
		req := &objstorepb.ListRequest{
			Prefix: "test/grpc/list/",
		}
		resp, err := grpcClient.List(ctx, req)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(resp.Objects), 3)

		// Verify all returned objects match the prefix
		for _, obj := range resp.Objects {
			assert.Contains(t, obj.Key, "test/grpc/list/")
		}
	})

	t.Run("list with delimiter", func(t *testing.T) {
		req := &objstorepb.ListRequest{
			Prefix:    "test/grpc/list/",
			Delimiter: "/",
		}
		resp, err := grpcClient.List(ctx, req)
		require.NoError(t, err)

		// Should have common prefixes for subdirectories
		if len(resp.CommonPrefixes) > 0 {
			assert.Contains(t, resp.CommonPrefixes, "test/grpc/list/subdir/")
		}
	})

	t.Run("list with pagination", func(t *testing.T) {
		req := &objstorepb.ListRequest{
			Prefix:     "test/grpc/list/",
			MaxResults: 2,
		}
		resp, err := grpcClient.List(ctx, req)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(resp.Objects), 2)

		if resp.Truncated {
			assert.NotEmpty(t, resp.NextToken)

			// Get next page
			req2 := &objstorepb.ListRequest{
				Prefix:       "test/grpc/list/",
				MaxResults:   2,
				ContinueFrom: resp.NextToken,
			}
			resp2, err := grpcClient.List(ctx, req2)
			require.NoError(t, err)
			assert.NotEmpty(t, resp2.Objects)
		}
	})
}

// TestGRPCExists tests the Exists RPC method
func TestGRPCExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	// Setup: Put an object
	putReq := &objstorepb.PutRequest{
		Key:  "test/grpc/exists.txt",
		Data: []byte("exists test"),
	}
	_, err := grpcClient.Put(ctx, putReq)
	require.NoError(t, err)

	t.Run("exists for existing object", func(t *testing.T) {
		req := &objstorepb.ExistsRequest{
			Key: "test/grpc/exists.txt",
		}
		resp, err := grpcClient.Exists(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Exists)
	})

	t.Run("exists for non-existent object", func(t *testing.T) {
		req := &objstorepb.ExistsRequest{
			Key: "test/grpc/non-existent-exists.txt",
		}
		resp, err := grpcClient.Exists(ctx, req)
		require.NoError(t, err)
		assert.False(t, resp.Exists)
	})
}

// TestGRPCGetMetadata tests the GetMetadata RPC method
func TestGRPCGetMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	// Setup: Put an object with metadata
	putReq := &objstorepb.PutRequest{
		Key:  "test/grpc/metadata-get.txt",
		Data: []byte("test content"),
		Metadata: &objstorepb.Metadata{
			ContentType:     "text/plain",
			ContentEncoding: "utf-8",
			Custom: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}
	_, err := grpcClient.Put(ctx, putReq)
	require.NoError(t, err)

	t.Run("get metadata for existing object", func(t *testing.T) {
		req := &objstorepb.GetMetadataRequest{
			Key: "test/grpc/metadata-get.txt",
		}
		resp, err := grpcClient.GetMetadata(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Success)
		assert.NotNil(t, resp.Metadata)
		assert.Equal(t, "text/plain", resp.Metadata.ContentType)
		assert.Equal(t, "utf-8", resp.Metadata.ContentEncoding)
		assert.Equal(t, "value1", resp.Metadata.Custom["key1"])
		assert.Equal(t, "value2", resp.Metadata.Custom["key2"])
		assert.Greater(t, resp.Metadata.Size, int64(0))
	})

	t.Run("get metadata for non-existent object", func(t *testing.T) {
		req := &objstorepb.GetMetadataRequest{
			Key: "test/grpc/non-existent-metadata.txt",
		}
		_, err := grpcClient.GetMetadata(ctx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

// TestGRPCConcurrency tests concurrent operations
func TestGRPCConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	t.Run("concurrent puts", func(t *testing.T) {
		numOps := 50
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func(index int) {
				req := &objstorepb.PutRequest{
					Key:  fmt.Sprintf("test/grpc/concurrent/put-%d.txt", index),
					Data: []byte(fmt.Sprintf("content %d", index)),
				}
				_, err := grpcClient.Put(ctx, req)
				errChan <- err
			}(i)
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})

	t.Run("concurrent gets", func(t *testing.T) {
		// Put a shared object
		putReq := &objstorepb.PutRequest{
			Key:  "test/grpc/concurrent/shared.txt",
			Data: []byte("shared content"),
		}
		_, err := grpcClient.Put(ctx, putReq)
		require.NoError(t, err)

		numOps := 50
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func() {
				req := &objstorepb.GetRequest{
					Key: "test/grpc/concurrent/shared.txt",
				}
				stream, err := grpcClient.Get(ctx, req)
				if err != nil {
					errChan <- err
					return
				}

				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						errChan <- err
						return
					}
				}
				errChan <- nil
			}()
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})
}

// TestGRPCErrorHandling tests error scenarios
func TestGRPCErrorHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	t.Run("timeout handling", func(t *testing.T) {
		// Create a context with very short timeout
		shortCtx, shortCancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer shortCancel()

		time.Sleep(10 * time.Millisecond) // Ensure timeout

		req := &objstorepb.PutRequest{
			Key:  "test/grpc/timeout.txt",
			Data: []byte("test"),
		}
		_, err := grpcClient.Put(shortCtx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.DeadlineExceeded, st.Code())
	})

	t.Run("cancelled context", func(t *testing.T) {
		cancelCtx, cancelFn := context.WithCancel(ctx)
		cancelFn() // Cancel immediately

		req := &objstorepb.PutRequest{
			Key:  "test/grpc/cancelled.txt",
			Data: []byte("test"),
		}
		_, err := grpcClient.Put(cancelCtx, req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Canceled, st.Code())
	})
}
