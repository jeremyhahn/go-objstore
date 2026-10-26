package main

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
)

// grpcStore is an in-memory objstore gRPC server. Get sends an object whole,
// in one message, as a server is free to.
type grpcStore struct {
	objstorepb.UnimplementedObjectStoreServer

	mu      sync.Mutex
	objs    map[string][]byte
	largest int
}

func (s *grpcStore) Put(_ context.Context, req *objstorepb.PutRequest) (*objstorepb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objs[req.GetKey()] = slices.Clone(req.GetData())
	s.largest = max(s.largest, len(req.GetData()))
	return &objstorepb.PutResponse{Success: true}, nil
}

func (s *grpcStore) Get(req *objstorepb.GetRequest, stream objstorepb.ObjectStore_GetServer) error {
	s.mu.Lock()
	data, ok := s.objs[req.GetKey()]
	s.mu.Unlock()
	if !ok {
		return status.Error(codes.NotFound, "object not found")
	}
	return stream.Send(&objstorepb.GetResponse{Data: data, IsLast: true})
}

func (s *grpcStore) Delete(_ context.Context, req *objstorepb.DeleteRequest) (*objstorepb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objs[req.GetKey()]; !ok {
		return nil, status.Error(codes.NotFound, "object not found")
	}
	delete(s.objs, req.GetKey())
	return &objstorepb.DeleteResponse{Success: true}, nil
}

func (s *grpcStore) List(_ context.Context, req *objstorepb.ListRequest) (*objstorepb.ListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &objstorepb.ListResponse{}
	for key := range s.objs {
		if strings.HasPrefix(key, req.GetPrefix()) {
			resp.Objects = append(resp.Objects, &objstorepb.ObjectInfo{Key: key})
		}
	}
	return resp, nil
}

func (s *grpcStore) Exists(_ context.Context, req *objstorepb.ExistsRequest) (*objstorepb.ExistsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objs[req.GetKey()]
	return &objstorepb.ExistsResponse{Exists: ok}, nil
}

// TestGRPCLargePacks backs up, restores and verifies through a gRPC server a
// repository whose pack is larger than gRPC's default 4 MiB message limit.
func TestGRPCLargePacks(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &grpcStore{objs: map[string][]byte{}}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	objstorepb.RegisterObjectStoreServer(gs, srv)
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)

	h := newHarness(t)
	run := func(want int, args ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		e := &env{stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr, open: openSDK}
		args = append(args, "--address", l.Addr().String(), "--repo", "hosts/a", "--passphrase-file", h.pass)
		if code := runWith(context.Background(), e, args); code != want {
			t.Fatalf("%s = %d, want %d\nstderr: %s", args[0], code, want, stderr.String())
		}
		return stdout.String()
	}

	data := make([]byte, 12<<20)
	rng := rand.NewChaCha8([32]byte{1})
	_, _ = rng.Read(data)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "big"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	run(exitOK, "init")
	run(exitOK, "backup", src)
	if srv.largest <= 4<<20 {
		t.Fatalf("largest object %d bytes, not above gRPC's default limit", srv.largest)
	}
	id := strings.Fields(run(exitOK, "snapshots"))[6]
	target := filepath.Join(t.TempDir(), "out")
	run(exitOK, "restore", id, target)
	if got, err := os.ReadFile(filepath.Join(target, "big")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored %d bytes, %v", len(got), err)
	}
	if out := run(exitOK, "verify"); !strings.Contains(out, "no problems found") {
		t.Errorf("verify: %q", out)
	}
}
