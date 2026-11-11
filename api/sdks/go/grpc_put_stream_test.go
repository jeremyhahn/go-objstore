package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// streamRecorder is a server with PutStream that records the chunks it got.
type streamRecorder struct {
	objstorepb.UnimplementedObjectStoreServer

	firstChunk chan struct{}
	done       chan struct{}
	once       sync.Once

	mu       sync.Mutex
	key      string
	chunks   int
	maxChunk int
	data     bytes.Buffer
	recvErr  error
}

func newStreamRecorder() *streamRecorder {
	return &streamRecorder{firstChunk: make(chan struct{}), done: make(chan struct{})}
}

func (s *streamRecorder) PutStream(stream objstorepb.ObjectStore_PutStreamServer) error {
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
	s.mu.Unlock()
	if err := stream.SendHeader(nil); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&objstorepb.PutResponse{Success: true, Etag: "e1"})
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

// unaryOnly is a server older than PutStream.
type unaryOnly struct {
	objstorepb.UnimplementedObjectStoreServer
	mu   sync.Mutex
	key  string
	data []byte
}

func (s *unaryOnly) Put(_ context.Context, req *objstorepb.PutRequest) (*objstorepb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.key, s.data = req.GetKey(), req.GetData()
	return &objstorepb.PutResponse{Success: true}, nil
}

func newBufconnGRPCClient(t *testing.T, srv objstorepb.ObjectStoreServer) *GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return &GRPCClient{conn: conn, client: objstorepb.NewObjectStoreClient(conn), config: &ClientConfig{}}
}

func sdkPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// gateReader hands out one full chunk, then waits for the server to receive
// a chunk before serving more; io.ReadAll never gets past the gate.
type gateReader struct {
	data      []byte
	off       int
	gate      <-chan struct{}
	gateAfter int
}

func (r *gateReader) Read(p []byte) (int, error) {
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

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func TestGRPCPutStream_SendsChunks(t *testing.T) {
	srv := newStreamRecorder()
	c := newBufconnGRPCClient(t, srv)
	data := sdkPayload(2*grpcPutChunkSize + 77)
	r := &gateReader{data: data, gate: srv.firstChunk, gateAfter: grpcPutChunkSize}

	res, err := c.PutStream(context.Background(), "big", r, int64(len(data)), nil)
	if err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if !res.Success || res.ETag != "e1" {
		t.Errorf("result = %+v, want success with etag e1", res)
	}
	<-srv.done
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.key != "big" || srv.chunks < 2 || srv.maxChunk > grpcPutChunkSize {
		t.Errorf("server got key %q, %d chunks, largest %d; want big, >1, <= %d", srv.key, srv.chunks, srv.maxChunk, grpcPutChunkSize)
	}
	if !bytes.Equal(srv.data.Bytes(), data) {
		t.Errorf("server got %d bytes, want the %d bytes sent", srv.data.Len(), len(data))
	}
}

func TestGRPCPutStream_ReadErrorCancels(t *testing.T) {
	srv := newStreamRecorder()
	c := newBufconnGRPCClient(t, srv)
	readErr := errors.New("boom")
	r := io.MultiReader(bytes.NewReader(sdkPayload(grpcPutChunkSize+1)), &errReader{err: readErr})

	if _, err := c.PutStream(context.Background(), "partial", r, -1, nil); !errors.Is(err, readErr) {
		t.Fatalf("PutStream = %v, want the read error", err)
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

// TestGRPCPutStream_SourceUnexpectedEOFCancels checks a source that returns
// io.ErrUnexpectedEOF mid-object aborts the put instead of being taken for
// the end of the data, so the server stores nothing.
func TestGRPCPutStream_SourceUnexpectedEOFCancels(t *testing.T) {
	srv := newStreamRecorder()
	c := newBufconnGRPCClient(t, srv)
	r := io.MultiReader(bytes.NewReader(sdkPayload(grpcPutChunkSize+1)), &errReader{err: io.ErrUnexpectedEOF})

	if _, err := c.PutStream(context.Background(), "truncated", r, -1, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("PutStream = %v, want io.ErrUnexpectedEOF", err)
	}
	select {
	case <-srv.done:
	case <-time.After(10 * time.Second):
		t.Fatal("server handler did not end after the client aborted")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if status.Code(srv.recvErr) != codes.Canceled {
		t.Errorf("server Recv error = %v, want Canceled (the put must not complete)", srv.recvErr)
	}
}

// smallReads hands out at most max bytes per Read.
type smallReads struct {
	r   io.Reader
	max int
}

func (s *smallReads) Read(p []byte) (int, error) {
	if len(p) > s.max {
		p = p[:s.max]
	}
	return s.r.Read(p)
}

// TestGRPCPutStream_ShortReadsSendWholeObject checks a source that returns
// fewer bytes per Read than a chunk still has the whole object sent.
func TestGRPCPutStream_ShortReadsSendWholeObject(t *testing.T) {
	srv := newStreamRecorder()
	c := newBufconnGRPCClient(t, srv)
	data := sdkPayload(2*grpcPutChunkSize + 777)
	r := &smallReads{r: bytes.NewReader(data), max: 4093}

	if _, err := c.PutStream(context.Background(), "short", r, -1, nil); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	<-srv.done
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if !bytes.Equal(srv.data.Bytes(), data) {
		t.Errorf("server got %d bytes, want the %d bytes sent", srv.data.Len(), len(data))
	}
	if srv.chunks != 3 {
		t.Errorf("server got %d chunks, want 3 full-size fills", srv.chunks)
	}
}

func TestGRPCPutStream_RefusedBeforeRead(t *testing.T) {
	c := newBufconnGRPCClient(t, newStreamRecorder())
	// validateKey refuses "" client-side; a server refusal of the header
	// must also come back without the source being read.
	c2 := newBufconnGRPCClient(t, &refuseHeader{})
	if _, err := c.PutStream(context.Background(), "", &errReader{err: errors.New("read")}, 0, nil); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, err := c2.PutStream(context.Background(), "k", &errReader{err: errors.New("source must not be read")}, 0, nil); !isCode(err, codes.PermissionDenied) {
		t.Fatalf("PutStream = %v, want PermissionDenied", err)
	}
}

type refuseHeader struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (refuseHeader) PutStream(objstorepb.ObjectStore_PutStreamServer) error {
	return status.Error(codes.PermissionDenied, "no")
}

func isCode(err error, code codes.Code) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if s, ok := status.FromError(e); ok && s.Code() == code {
			return true
		}
	}
	return false
}

func TestGRPCPutStream_FallsBackToUnary(t *testing.T) {
	srv := &unaryOnly{}
	c := newBufconnGRPCClient(t, srv)

	if _, err := c.PutStream(context.Background(), "old", bytes.NewReader([]byte("hello")), 5, nil); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.key != "old" || string(srv.data) != "hello" {
		t.Errorf("unary Put got (%q, %q), want (old, hello)", srv.key, srv.data)
	}
}
