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

package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// putStreamRecorder is a server with PutStream that records what it got.
type putStreamRecorder struct {
	objstorepb.UnimplementedObjectStoreServer

	firstChunk chan struct{} // closed when the first data chunk arrives
	done       chan struct{} // closed when the handler returns
	once       sync.Once

	mu       sync.Mutex
	key      string
	ctype    string
	chunks   int
	maxChunk int
	data     bytes.Buffer
	recvErr  error
}

func newPutStreamRecorder() *putStreamRecorder {
	return &putStreamRecorder{firstChunk: make(chan struct{}), done: make(chan struct{})}
}

func (s *putStreamRecorder) PutStream(stream objstorepb.ObjectStore_PutStreamServer) error {
	defer close(s.done)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader().GetKey() == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	s.mu.Lock()
	s.key = first.GetHeader().GetKey()
	s.ctype = first.GetHeader().GetMetadata().GetContentType()
	s.mu.Unlock()
	if err := stream.SendHeader(nil); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&objstorepb.PutResponse{Success: true})
		}
		if err != nil {
			s.mu.Lock()
			s.recvErr = err
			s.mu.Unlock()
			return err
		}
		s.mu.Lock()
		s.chunks++
		s.maxChunk = max(s.maxChunk, len(msg.GetChunk()))
		s.data.Write(msg.GetChunk())
		s.mu.Unlock()
		s.once.Do(func() { close(s.firstChunk) })
	}
}

// gatedReader serves data, but once it has handed out one full chunk it
// waits for the server to receive a chunk before it serves more. A Put that
// read the whole source before sending (io.ReadAll) never gets past the gate.
type gatedReader struct {
	data      []byte
	off       int
	gate      <-chan struct{}
	gateAfter int
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	if r.off >= r.gateAfter {
		select {
		case <-r.gate:
		case <-time.After(10 * time.Second):
			return 0, errors.New("source read past the first chunk before the server received any chunk")
		}
	}
	end := len(r.data)
	if r.off < r.gateAfter {
		end = min(end, r.gateAfter)
	}
	n := copy(p, r.data[r.off:end])
	r.off += n
	return n, nil
}

func newTestGRPCClient(t *testing.T, srv objstorepb.ObjectStoreServer) *GRPCClient {
	t.Helper()
	conn := serveGRPC(t, srv)
	t.Cleanup(func() { _ = conn.Close() })
	return &GRPCClient{conn: conn, client: objstorepb.NewObjectStoreClient(conn)}
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 253)
	}
	return b
}

// TestGRPCClient_Put_StreamsChunks checks Put sends a reader larger than the
// chunk size as several chunks of at most putStreamChunkSize, starting before
// the source is exhausted.
func TestGRPCClient_Put_StreamsChunks(t *testing.T) {
	srv := newPutStreamRecorder()
	c := newTestGRPCClient(t, srv)
	data := payload(2*putStreamChunkSize + putStreamChunkSize/2)
	r := &gatedReader{data: data, gate: srv.firstChunk, gateAfter: putStreamChunkSize}

	if err := c.Put(context.Background(), "big.bin", r, &common.Metadata{ContentType: "application/x-test"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	<-srv.done
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.key != "big.bin" || srv.ctype != "application/x-test" {
		t.Errorf("header = (%q, %q), want (big.bin, application/x-test)", srv.key, srv.ctype)
	}
	if srv.chunks < 2 {
		t.Errorf("server got %d chunks, want more than one", srv.chunks)
	}
	if srv.maxChunk > putStreamChunkSize {
		t.Errorf("largest chunk %d bytes, want at most %d", srv.maxChunk, putStreamChunkSize)
	}
	if !bytes.Equal(srv.data.Bytes(), data) {
		t.Errorf("server got %d bytes, want the %d bytes sent", srv.data.Len(), len(data))
	}
}

// TestGRPCClient_Put_ReadErrorCancelsStream checks a source that fails
// midway aborts the stream, so the server never completes the put.
func TestGRPCClient_Put_ReadErrorCancelsStream(t *testing.T) {
	srv := newPutStreamRecorder()
	c := newTestGRPCClient(t, srv)
	readErr := errors.New("disk on fire")
	r := io.MultiReader(bytes.NewReader(payload(putStreamChunkSize+10)), &failingReader{err: readErr})

	if err := c.Put(context.Background(), "partial.bin", r, nil); !errors.Is(err, readErr) {
		t.Fatalf("Put = %v, want the read error", err)
	}
	select {
	case <-srv.done:
	case <-time.After(10 * time.Second):
		t.Fatal("server handler did not end after the client aborted")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if status.Code(srv.recvErr) != codes.Canceled {
		t.Errorf("server Recv error = %v, want Canceled", srv.recvErr)
	}
}

type failingReader struct{ err error }

func (r *failingReader) Read([]byte) (int, error) { return 0, r.err }

// TestGRPCClient_Put_RefusedHeaderReadsNothing checks a refused header is
// returned before Put reads from the source.
func TestGRPCClient_Put_RefusedHeaderReadsNothing(t *testing.T) {
	c := newTestGRPCClient(t, newPutStreamRecorder())
	r := &failingReader{err: errors.New("source must not be read")}

	if err := c.Put(context.Background(), "", r, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put = %v, want InvalidArgument", err)
	}
}

// unaryOnlyServer is a server older than PutStream.
type unaryOnlyServer struct {
	objstorepb.UnimplementedObjectStoreServer
	mu   sync.Mutex
	key  string
	data []byte
}

func (s *unaryOnlyServer) Put(_ context.Context, req *objstorepb.PutRequest) (*objstorepb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.key, s.data = req.GetKey(), req.GetData()
	return &objstorepb.PutResponse{Success: true}, nil
}

// TestGRPCClient_Put_FallsBackToUnary checks Put still works against a
// server without PutStream.
func TestGRPCClient_Put_FallsBackToUnary(t *testing.T) {
	srv := &unaryOnlyServer{}
	c := newTestGRPCClient(t, srv)

	if err := c.Put(context.Background(), "old.txt", bytes.NewReader([]byte("hello")), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.key != "old.txt" || string(srv.data) != "hello" {
		t.Errorf("unary Put got (%q, %q), want (old.txt, hello)", srv.key, srv.data)
	}
}
