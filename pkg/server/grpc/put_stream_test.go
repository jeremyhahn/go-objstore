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
	"errors"
	"io"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// signalStorage wraps mockStorage and reports every finished put, so a test
// can wait for the handler's backend call to return before it checks the
// store.
type signalStorage struct {
	*mockStorage
	done chan error
}

func (s *signalStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	err := s.mockStorage.PutWithContext(ctx, key, data)
	s.done <- err
	return err
}

func (s *signalStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, md *common.Metadata) error {
	err := s.mockStorage.PutWithMetadata(ctx, key, data, md)
	s.done <- err
	return err
}

const putStreamTestMaxRecv = 64 * 1024

func startPutStreamServer(t *testing.T, storage common.Storage) objstorepb.ObjectStoreClient {
	t.Helper()
	server, err := newTestServer(t, storage,
		WithAddress("127.0.0.1:0"),
		WithMaxReceiveMessageSize(putStreamTestMaxRecv))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = server.Start() }()
	var addr string
	for i := 0; i < 200; i++ {
		addr = server.GetAddress()
		if addr != "" && addr[len(addr)-2:] != ":0" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
	})
	return objstorepb.NewObjectStoreClient(conn)
}

func testPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func getAll(t *testing.T, client objstorepb.ObjectStoreClient, key string) []byte {
	t.Helper()
	stream, err := client.Get(context.Background(), &objstorepb.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var buf bytes.Buffer
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return buf.Bytes()
		}
		if err != nil {
			t.Fatalf("Get recv: %v", err)
		}
		buf.Write(resp.GetData())
	}
}

func exists(t *testing.T, client objstorepb.ObjectStoreClient, key string) bool {
	t.Helper()
	resp, err := client.Exists(context.Background(), &objstorepb.ExistsRequest{Key: key})
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	return resp.GetExists()
}

// TestPutStream_LargerThanMaxRecv stores an object several times the server's
// receive limit through PutStream and reads back the same bytes, while the
// unary Put of the same object is refused by the limit.
func TestPutStream_LargerThanMaxRecv(t *testing.T) {
	client := startPutStreamServer(t, newMockStorage())
	data := testPayload(16*putStreamTestMaxRecv + 123)

	if _, err := client.Put(context.Background(), &objstorepb.PutRequest{Key: "big", Data: data}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unary Put of %d bytes = %v, want ResourceExhausted", len(data), err)
	}

	stream, err := client.PutStream(context.Background())
	if err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if err := stream.Send(&objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Header{
		Header: &objstorepb.PutStreamHeader{Key: "big", Metadata: &objstorepb.Metadata{ContentType: "application/octet-stream"}},
	}}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	const chunk = 16 * 1024
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		if err := stream.Send(&objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Chunk{Chunk: data[off:end]}}); err != nil {
			t.Fatalf("send chunk at %d: %v", off, err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("PutStream not successful: %v", resp)
	}
	if got := getAll(t, client, "big"); !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, want %d identical bytes", len(got), len(data))
	}
}

// TestPutStream_NoKeyStoresNothing covers the header checks: no message at
// all, a chunk before the header, an empty key, and a second header.
func TestPutStream_NoKeyStoresNothing(t *testing.T) {
	client := startPutStreamServer(t, newMockStorage())
	chunk := &objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Chunk{Chunk: []byte("data")}}
	header := func(key string) *objstorepb.PutStreamRequest {
		return &objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Header{Header: &objstorepb.PutStreamHeader{Key: key}}}
	}

	cases := []struct {
		name string
		msgs []*objstorepb.PutStreamRequest
		key  string
	}{
		{"empty stream", nil, ""},
		{"chunk first", []*objstorepb.PutStreamRequest{chunk}, ""},
		{"empty key", []*objstorepb.PutStreamRequest{header(""), chunk}, ""},
		{"second header", []*objstorepb.PutStreamRequest{header("dup"), chunk, header("other")}, "dup"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := client.PutStream(context.Background())
			if err != nil {
				t.Fatalf("PutStream: %v", err)
			}
			for _, m := range tc.msgs {
				if err := stream.Send(m); err != nil {
					break // the server may already have refused the stream
				}
			}
			_, err = stream.CloseAndRecv()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CloseAndRecv = %v, want InvalidArgument", err)
			}
			if tc.key != "" && exists(t, client, tc.key) {
				t.Fatalf("object %q was stored", tc.key)
			}
		})
	}
}

// TestPutStream_CancelStoresNothing cancels the client stream after some
// chunks and checks the backend's put failed and no object exists.
func TestPutStream_CancelStoresNothing(t *testing.T) {
	storage := &signalStorage{mockStorage: newMockStorage(), done: make(chan error, 1)}
	client := startPutStreamServer(t, storage)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.PutStream(ctx)
	if err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if err := stream.Send(&objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Header{
		Header: &objstorepb.PutStreamHeader{Key: "cancelled"},
	}}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := stream.Send(&objstorepb.PutStreamRequest{Payload: &objstorepb.PutStreamRequest_Chunk{Chunk: testPayload(8 * 1024)}}); err != nil {
			t.Fatalf("send chunk %d: %v", i, err)
		}
	}
	cancel()

	select {
	case perr := <-storage.done:
		if perr == nil {
			t.Fatal("backend put succeeded after the client cancelled")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("backend put did not return after the client cancelled")
	}
	if exists(t, client, "cancelled") {
		t.Fatal("cancelled stream stored an object")
	}
}

// TestPutStream_AuthorizationMatchesPut checks an unauthorized principal is
// refused on PutStream exactly as on Put, and a writer is allowed on both.
func TestPutStream_AuthorizationMatchesPut(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
		"writer": {adapters.ActionWrite},
	})
	unary := AuthorizationUnaryInterceptor(authz, adapters.NewNoOpLogger())
	streamI := AuthorizationStreamInterceptor(authz, adapters.NewNoOpLogger())
	uh := func(context.Context, any) (any, error) { return nil, nil }
	sh := func(any, grpc.ServerStream) error { return nil }

	for _, role := range []string{"reader", "writer"} {
		ctx := ctxWithPrincipal(adapters.Principal{ID: role, Roles: []string{role}})
		_, putErr := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: objstorepb.ObjectStore_Put_FullMethodName}, uh)
		streamErr := streamI(nil, &mockServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: objstorepb.ObjectStore_PutStream_FullMethodName}, sh)
		if status.Code(putErr) != status.Code(streamErr) {
			t.Errorf("%s: Put = %v, PutStream = %v; want the same code", role, putErr, streamErr)
		}
		want := codes.OK
		if role == "reader" {
			want = codes.PermissionDenied
		}
		if status.Code(streamErr) != want {
			t.Errorf("%s: PutStream = %v, want %v", role, streamErr, want)
		}
	}
}
